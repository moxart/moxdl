"""End-to-end tests: run mox against a local HTTP server that misbehaves on request.

Server paths:
    /<file>           ranges, keep-alive, 8 MB/s per connection
    /norange/<file>   ignores Range headers
    /flaky/<file>     drops connections mid-stream at random
    /slowconn/<file>  about a third of requests crawl at 150 KB/s
    /busy/<file>      answers 503 when more than 4 requests are in flight
    /named/<file>     sends a UTF-8 Content-Disposition file name
    /gone/<file>      403, like an expired signed link
    /alt/<file>       the same file under another URL
    /short/<file>     closes every connection after 1 MB
    /stall/<file>     sends 256 KB per request, then goes silent for 15 seconds
    /index/           an Apache-style folder listing of the files; /index/<file> serves them

The SOCKS5 proxies know one name of their own, blocked.test, which they connect to 127.0.0.1.
Directly the name does not resolve, like a server that is only reachable through a proxy.

Every reply carries an ETag, and If-Range is honored, so a file replaced on disk counts as changed.

POST /bot<token>/sendMessage stands in for the Telegram Bot API and records what it gets.

There is also a small SOCKS5 proxy, with and without a password.
"""

import datetime
import hashlib
import importlib.util
import io
import json
import os
import random
import re
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import wave
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
from unittest import mock

MOX = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mox.py")
SIZE = 8 * 1024 * 1024
RATE = 8 * 1024 * 1024
DISPOSITION_NAME = "گزارش.bin"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    inflight = 0
    lock = threading.Lock()
    telegram: list = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        if re.fullmatch(r"/bot[^/]+/sendMessage", self.path):
            Handler.telegram.append(urllib.parse.parse_qs(body))
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_GET(self):
        with Handler.lock:
            Handler.inflight += 1
            busy = Handler.inflight > 4
        try:
            self.serve(busy)
        finally:
            with Handler.lock:
                Handler.inflight -= 1

    def serve(self, busy):
        if self.path.split("?")[0].rstrip("/") == "/index":
            self.listing()
            return
        parts = self.path.split("?")[0].strip("/").split("/")
        mode, name = (parts[0], parts[-1]) if len(parts) > 1 else ("", parts[-1])
        path = os.path.join(self.server.root, name)
        if not os.path.isfile(path):
            self.send_error(404)
            return
        if mode == "gone":
            self.send_response(403)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "busy" and busy:
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        rate = 150 * 1024 if mode == "slowconn" and random.random() < 0.3 else RATE
        info = os.stat(path)
        size = info.st_size
        etag = f'"{info.st_mtime_ns:x}-{size:x}"'
        start, end = 0, size - 1
        match = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
        if_range = self.headers.get("If-Range")
        if match and mode != "norange" and if_range in (None, etag):
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else size - 1
            if start >= size:
                self.send_response(416)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("ETag", etag)
        if mode == "named":
            self.send_header("Content-Disposition",
                             "attachment; filename*=UTF-8''%DA%AF%D8%B2%D8%A7%D8%B1%D8%B4.bin")
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            left, sent = end - start + 1, 0
            while left > 0:
                chunk = f.read(min(65536, left))
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    self.close_connection = True
                    return
                left -= len(chunk)
                sent += len(chunk)
                if mode == "flaky" and sent > 256 * 1024 and random.random() < 0.05:
                    self.close_connection = True
                    return
                if mode == "short" and sent >= 1024 * 1024:
                    self.close_connection = True
                    return
                if mode == "stall" and sent >= 256 * 1024:
                    time.sleep(15)
                    self.close_connection = True
                    return
                time.sleep(len(chunk) / rate)


    def listing(self):
        """What Apache shows for a folder: sort links, the parent, a subfolder, then the files."""
        files = sorted(os.listdir(self.server.root))
        body = ('<html><body><h1>Index of /index</h1><a href="?C=N;O=D">Name</a> <a href="?C=M;O=A">Date</a>'
                '<a href="/">Parent Directory</a><a href="sub/">sub/</a><a href="readme.html">readme</a>'
                + "".join(f'<a href="{urllib.parse.quote(name)}">{name}</a>' for name in files)
                + f'<a href="{files[0]}">again</a></body></html>').encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        pass  # mox drops connections on purpose; the resets are expected


class Socks5Handler(socketserver.BaseRequestHandler):
    def recv(self, n):
        data = b""
        while len(data) < n:
            chunk = self.request.recv(n - len(data))
            if not chunk:
                raise ConnectionError
            data += chunk
        return data

    def handle(self):
        try:
            self.negotiate()
        except (ConnectionError, OSError):
            pass

    def negotiate(self):
        client = self.request
        _, count = self.recv(2)
        methods = self.recv(count)
        credentials = self.server.credentials
        if credentials:
            if 2 not in methods:
                client.sendall(b"\x05\xff")
                return
            client.sendall(b"\x05\x02")
            self.recv(1)
            user = self.recv(self.recv(1)[0]).decode()
            password = self.recv(self.recv(1)[0]).decode()
            ok = (user, password) == credentials
            client.sendall(b"\x01" + (b"\x00" if ok else b"\x01"))
            if not ok:
                return
        else:
            client.sendall(b"\x05\x00")
        _, _, _, address_type = self.recv(4)
        if address_type == 3:
            host = self.recv(self.recv(1)[0]).decode()
        else:
            host = socket.inet_ntoa(self.recv(4))
        port = int.from_bytes(self.recv(2), "big")
        remote = socket.create_connection(("127.0.0.1" if host == "blocked.test" else host, port))
        with self.server.lock:
            self.server.tunnels += 1
        with remote:
            client.sendall(b"\x05\x00\x00\x01" + bytes(6))
            while True:
                readable, _, _ = select.select([client, remote], [], [], 30)
                if not readable:
                    return
                for sock in readable:
                    data = sock.recv(65536)
                    if not data:
                        return
                    (remote if sock is client else client).sendall(data)


class Socks5Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, credentials=None):
        super().__init__(("127.0.0.1", 0), Socks5Handler)
        self.credentials = credentials
        self.tunnels = 0
        self.lock = threading.Lock()

    def handle_error(self, request, client_address):
        pass


def load_mox():
    spec = importlib.util.spec_from_file_location("mox", MOX)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def clock_in(seconds):
    return (datetime.datetime.now() + datetime.timedelta(seconds=seconds)).strftime("%H:%M:%S")


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def replace_with_random(path, size=SIZE):
    """Write new content of the same size, the way a new release would replace a file."""
    with open(path + ".new", "wb") as f:
        f.write(os.urandom(size))
    os.replace(path + ".new", path)
    return sha256(path)


class MoxTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = tempfile.mkdtemp(prefix="mox-files-")
        with open(os.path.join(cls.root, "big.bin"), "wb") as f:
            f.write(os.urandom(SIZE))
        open(os.path.join(cls.root, "empty.bin"), "wb").close()
        cls.digest = sha256(os.path.join(cls.root, "big.bin"))
        cls.server = QuietServer(("127.0.0.1", 0), Handler)
        cls.server.root = cls.root
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.socks = Socks5Server()
        cls.socks_auth = Socks5Server(credentials=("mozart", "k525"))
        for server in (cls.server, cls.socks, cls.socks_auth):
            threading.Thread(target=server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        for server in (cls.server, cls.socks, cls.socks_auth):
            server.shutdown()
            server.server_close()
        shutil.rmtree(cls.root, ignore_errors=True)

    def setUp(self):
        self.out = self.temp_dir("mox-out-")
        # Keep the history these runs write away from the real one. Subprocesses inherit this.
        self.data = self.temp_dir("mox-data-")
        for name, value in (("XDG_DATA_HOME", self.data), ("APPDATA", self.data), ("XDG_CACHE_HOME", self.data),
                            ("XDG_CONFIG_HOME", self.data),
                            ("MOX_NO_HISTORY", None), ("MOX_SOUND", "0"),  # silent test runs
                            # A proxy app's system-wide setting would send the tests through it.
                            *((name, None) for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                                                        "http_proxy", "https_proxy", "all_proxy"))):
            self.addCleanup(restore_env, name, os.environ.get(name))
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def temp_dir(self, prefix):
        """A temporary folder that is deleted after the test, pass or fail."""
        path = tempfile.mkdtemp(prefix=prefix)
        self.addCleanup(shutil.rmtree, path, True)
        return path

    def mox(self, *args, timeout=60):
        return subprocess.run([sys.executable, MOX, "-d", self.out, *args],
                              capture_output=True, text=True, timeout=timeout)

    def run_mox(self, *args, timeout=60, env=None):
        """Like mox(), without the -d option, for subcommands."""
        return subprocess.run([sys.executable, MOX, *args], capture_output=True, text=True,
                              timeout=timeout, env=env)

    def wait_for(self, condition, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.2)
        self.fail("timed out waiting")

    def assertDownloaded(self, name="big.bin", digest=None):
        path = os.path.join(self.out, name)
        self.assertTrue(os.path.exists(path), f"{name} missing; folder has {os.listdir(self.out)}")
        self.assertEqual(sha256(path), digest or self.digest)
        leftovers = [n for n in os.listdir(self.out) if n.endswith((".part", ".mox"))]
        self.assertEqual(leftovers, [])

    def test_many_connections(self):
        result = self.mox(f"{self.base}/big.bin", "-c", "16")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_one_connection(self):
        result = self.mox(f"{self.base}/big.bin", "-c", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_server_without_ranges(self):
        result = self.mox(f"{self.base}/norange/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_dropped_connections_are_retried(self):
        result = self.mox(f"{self.base}/flaky/big.bin", "-r", "30")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_slow_connections_do_not_hold_up_the_file(self):
        started = time.monotonic()
        result = self.mox(f"{self.base}/slowconn/big.bin")
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()
        # A slow piece left alone would take about 512 KB / 150 KB/s, over 3 seconds.
        self.assertLess(elapsed, 3.5)

    def test_busy_server_gets_fewer_connections(self):
        result = self.mox(f"{self.base}/busy/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_pause_and_resume(self):
        proc = subprocess.Popen([sys.executable, MOX, "-d", self.out, "-l", "2M", f"{self.base}/big.bin"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.5)
        proc.send_signal(signal.SIGINT)
        self.assertEqual(proc.wait(timeout=10), 130)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin.mox")))
        result = self.mox(f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("resumed", result.stderr)
        self.assertDownloaded()

    def test_second_ctrl_c_quits_without_a_traceback(self):
        proc = subprocess.Popen([sys.executable, MOX, "-d", self.out, f"{self.base}/stall/big.bin"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(1.5)
        proc.send_signal(signal.SIGINT)
        time.sleep(0.5)
        self.assertIsNone(proc.poll())  # the stalled connections have not noticed the pause yet
        proc.send_signal(signal.SIGINT)
        _, stderr = proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 130, stderr)
        self.assertIn("Quit without waiting", stderr)
        self.assertNotIn("Traceback", stderr)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin.mox")))

    def test_name_from_content_disposition(self):
        result = self.mox(f"{self.base}/named/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded(DISPOSITION_NAME)

    def test_same_name_gets_a_suffix(self):
        result = self.mox(f"{self.base}/big.bin", f"{self.base}/norange/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded("big.bin")
        self.assertDownloaded("big (1).bin")

    def test_empty_file(self):
        result = self.mox(f"{self.base}/empty.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(os.path.getsize(os.path.join(self.out, "empty.bin")), 0)

    def test_missing_file_fails(self):
        result = self.mox(f"{self.base}/nope.bin")
        self.assertEqual(result.returncode, 1)
        self.assertIn("404", result.stderr)

    def test_checksum(self):
        good = self.mox(f"{self.base}/big.bin", "--sha256", self.digest)
        self.assertEqual(good.returncode, 0, good.stderr)
        self.assertIn("SHA-256 verified", good.stderr)
        bad = self.mox(f"{self.base}/big.bin", "-o", "copy.bin", "--sha256", "0" * 64)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("mismatch", bad.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.out, "copy.bin")))


    # -- proxy

    def test_socks5_proxy(self):
        before = self.socks.tunnels
        port = self.socks.server_address[1]
        result = self.mox(f"{self.base}/big.bin", "--proxy", f"socks5://127.0.0.1:{port}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()
        self.assertGreater(self.socks.tunnels - before, 1)

    def test_socks5_proxy_with_password(self):
        port = self.socks_auth.server_address[1]
        result = self.mox(f"{self.base}/big.bin", "--proxy", f"socks5h://mozart:k525@127.0.0.1:{port}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    def test_socks5_wrong_password(self):
        port = self.socks_auth.server_address[1]
        result = self.mox(f"{self.base}/big.bin", "--proxy", f"socks5://mozart:wrong@127.0.0.1:{port}")
        self.assertEqual(result.returncode, 1)
        self.assertIn("password", result.stderr)

    def test_bad_proxy_url(self):
        result = self.mox(f"{self.base}/big.bin", "--proxy", "ftp://127.0.0.1:21")
        self.assertEqual(result.returncode, 2)
        self.assertIn("unsupported proxy", result.stderr)

    # -- schedule

    def test_wait_for_start_time(self):
        started = time.monotonic()
        result = self.mox(f"{self.base}/big.bin", "--at", clock_in(3))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(time.monotonic() - started, 1.5)
        self.assertDownloaded()

    def test_stop_at_end_time_then_continue(self):
        result = self.mox(f"{self.base}/big.bin", "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("as scheduled", result.stderr)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin.mox")))
        result = self.mox(f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("resumed", result.stderr)
        self.assertDownloaded()

    def test_clock_math(self):
        mox = load_mox()
        night = datetime.datetime(2026, 10, 19, 23, 30)
        morning = datetime.datetime(2026, 10, 20, 3, 0)
        noon = datetime.datetime(2026, 10, 20, 12, 0)
        two, seven = datetime.time(2), datetime.time(7)
        self.assertEqual(mox.next_time(two, night), datetime.datetime(2026, 10, 20, 2, 0))
        self.assertEqual(mox.next_time(two, morning), datetime.datetime(2026, 10, 21, 2, 0))
        self.assertTrue(mox.in_window(morning, two, seven))
        self.assertFalse(mox.in_window(noon, two, seven))
        late, early = datetime.time(23), datetime.time(6)  # window across midnight
        self.assertTrue(mox.in_window(night, late, early))
        self.assertFalse(mox.in_window(datetime.datetime(2026, 10, 19, 22, 45), late, early))
        self.assertTrue(mox.in_window(morning, late, early))
        self.assertFalse(mox.in_window(noon, late, early))

    # -- expired links

    def test_expired_link_then_new_link(self):
        result = self.mox(f"{self.base}/big.bin", "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        expired = self.mox(f"{self.base}/gone/big.bin")
        self.assertEqual(expired.returncode, 1)
        self.assertIn("expired", expired.stderr)
        fresh = self.mox(f"{self.base}/alt/big.bin", "-o", "big.bin")
        self.assertEqual(fresh.returncode, 0, fresh.stderr)
        self.assertIn("resumed", fresh.stderr)
        self.assertDownloaded()

    def test_link_cut_by_the_shell(self):
        # What 'mox URL?md5=x&expires=y' without quotes sends: everything before the '&'.
        result = self.mox(f"{self.base}/gone/big.bin?md5=x")
        self.assertEqual(result.returncode, 1)
        self.assertIn("inside single quotes", result.stderr)
        self.assertNotIn("expired", result.stderr)

    def test_expired_link_with_query_while_resuming(self):
        result = self.mox(f"{self.base}/big.bin?token=a", "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        expired = self.mox(f"{self.base}/gone/big.bin?token=a")
        self.assertEqual(expired.returncode, 1)
        self.assertIn("expired", expired.stderr)
        self.assertNotIn("single quotes", expired.stderr)

    def test_file_changed_while_paused(self):
        served = os.path.join(self.root, "paused-change.bin")
        replace_with_random(served)
        result = self.mox(f"{self.base}/paused-change.bin", "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        new_digest = replace_with_random(served)
        result = self.mox(f"{self.base}/paused-change.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("changed on the server", result.stderr)
        self.assertDownloaded("paused-change.bin", new_digest)

    def test_file_changed_while_downloading(self):
        served = os.path.join(self.root, "live-change.bin")
        replace_with_random(served)
        proc = subprocess.Popen([sys.executable, MOX, "-d", self.out, "-c", "2", "-l", "2M",
                                 f"{self.base}/short/live-change.bin"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(1)
        new_digest = replace_with_random(served)
        _, stderr = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 1, stderr)
        self.assertIn("changed on the server", stderr)
        result = self.mox(f"{self.base}/short/live-change.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("started over", result.stderr)
        self.assertDownloaded("live-change.bin", new_digest)


    # -- piece map

    def test_coverage_map(self):
        mox = load_mox()
        # 100 bytes in 10 cells: first piece complete, second one half done.
        self.assertEqual(mox.coverage_map([(0, 50), (50, 25)], 100, 10), [3, 3, 3, 3, 3, 3, 3, 2, 0, 0])
        self.assertEqual(mox.coverage_map([(0, 3)], 100, 10), [1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(mox.coverage_map([], 0, 10), [])

    def test_display_draws_piece_map(self):
        mox = load_mox()

        class Terminal(io.StringIO):
            def isatty(self):
                return True

        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.status, d.total, d.done, d.name = "downloading", 1000, 300, "big.bin"
        d.segments = [{"start": 0, "end": 499, "done": 300, "busy": True},
                      {"start": 500, "end": 999, "done": 0, "busy": True}]
        screen = Terminal()
        with mock.patch("shutil.get_terminal_size", return_value=os.terminal_size((100, 24))):
            mox.Display([d], stream=screen).draw()
        output = screen.getvalue()
        lines = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", output).splitlines()
        self.assertEqual(len(lines), 2)  # one row per download, then the summary
        self.assertIn("big.bin", lines[0])
        self.assertIn("30.0%", lines[0])
        # The bar is the piece map: the first piece is 60% done, the second not started.
        self.assertRegex(output, "\x1b\\[38;5;173m━+\x1b\\[0m(\x1b\\[(2;)?38;5;166m━\x1b\\[0m)?\x1b\\[2;37m━+")

    def test_spinner(self):
        mox = load_mox()
        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        display = mox.Display([d], stream=io.StringIO())
        layout = display._layout([d], 100)

        def icon_at(moment):
            with mock.patch.object(mox.time, "monotonic", return_value=moment):
                return re.sub(r"\x1b\[[0-9;]*m", "", display._row(d, layout, 100))[0]

        d.status = "connecting"
        frames = [icon_at(i * mox.FRAME_INTERVAL) for i in range(len(mox.SPINNER))]
        self.assertEqual("".join(frames), mox.SPINNER)  # one frame per redraw, all of them
        d.status, d.total, d.message = "downloading", 1000, mox.OFFLINE_MESSAGE
        self.assertIn(icon_at(0), mox.SPINNER)
        d.message = ""
        self.assertEqual(icon_at(0), "↓")  # moving data shows the arrow, not the spinner

    def test_display_on_a_terminal_without_a_size(self):
        mox = load_mox()
        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.status, d.total, d.done = "downloading", 1000, 300
        display = mox.Display([d], stream=io.StringIO())
        display.tty = True
        with mock.patch("shutil.get_terminal_size", return_value=os.terminal_size((0, 0))):
            display.draw()
            display.draw()  # the second frame moves up over the first, which divided by the width
        self.assertIn("30.0%", display.stream.getvalue())

    def test_display_redraw_after_resize(self):
        mox = load_mox()

        class Terminal(io.StringIO):
            def isatty(self):
                return True

        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.status, d.total, d.done, d.name = "downloading", 1000, 300, "a-rather-long-file-name.bin"
        screen = Terminal()
        display = mox.Display([d], stream=screen)
        with mock.patch("shutil.get_terminal_size", return_value=os.terminal_size((120, 24))):
            display.draw()
        wide = list(display.drawn)
        with mock.patch("shutil.get_terminal_size", return_value=os.terminal_size((50, 24))):
            screen.truncate(0)
            screen.seek(0)
            display.draw()
        # The terminal rewrapped the 120-column frame to 50 columns, so moving to its top takes
        # more rows than it had lines.
        up = sum(-(-w // 50) for w in wide)
        self.assertGreater(up, len(wide))
        self.assertIn(f"\x1b[{up}F", screen.getvalue())
        self.assertTrue(all(w < 50 for w in display.drawn))

    # -- mox bench

    def test_bench(self):
        result = self.run_mox("bench", f"{self.base}/big.bin", "-s", "0.5", "-c", "4")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("supports range requests", result.stdout)
        self.assertRegex(result.stdout, r"4 connections .*×")
        self.assertEqual(os.listdir(self.out), [])

    def test_bench_without_ranges(self):
        result = self.run_mox("bench", f"{self.base}/norange/big.bin", "-s", "0.5")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("one piece", result.stdout)

    def test_bench_missing_file(self):
        result = self.run_mox("bench", f"{self.base}/nope.bin", "-s", "0.5")
        self.assertEqual(result.returncode, 1)
        self.assertIn("404", result.stderr)

    # -- mox resume

    def test_resume_command(self):
        result = self.mox(f"{self.base}/big.bin", "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        result = self.run_mox("resume", self.out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("resumed", result.stderr)
        self.assertDownloaded()

    def test_resume_with_nothing_to_do(self):
        result = self.run_mox("resume", self.out)
        self.assertEqual(result.returncode, 0)
        self.assertIn("No unfinished downloads", result.stderr)

    # -- --watch

    def test_links_in(self):
        mox = load_mox()
        text = ("see https://example.com/blog/post.html and https://example.com/files/a.zip, "
                "or (https://example.com/b.iso) https://example.com/")
        self.assertEqual(mox.links_in(text), ["https://example.com/files/a.zip", "https://example.com/b.iso"])

    def test_watch_clipboard(self):
        clipboard = os.path.join(self.temp_dir("mox-clipboard-"), "clipboard")
        with open(clipboard, "w") as f:
            f.write(f"{self.base}/named/big.bin")  # already there when mox starts: must be ignored
        env = {**os.environ, "MOX_CLIPBOARD_COMMAND": f"cat {clipboard}"}
        proc = subprocess.Popen([sys.executable, MOX, "-d", self.out, "--watch"], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            time.sleep(1.5)
            with open(clipboard, "w") as f:
                f.write(f"copied: {self.base}/big.bin and a page {self.base}/index.html")
            self.wait_for(lambda: os.path.exists(os.path.join(self.out, "big.bin")))
        finally:
            proc.send_signal(signal.SIGINT)
            _, stderr = proc.communicate(timeout=15)
        self.assertEqual(proc.returncode, 0, stderr)
        self.assertDownloaded()
        self.assertEqual(sorted(os.listdir(self.out)), ["big.bin"])

    def test_watch_without_clipboard_tool(self):
        env = {k: v for k, v in os.environ.items() if k not in ("MOX_CLIPBOARD_COMMAND", "WAYLAND_DISPLAY")}
        env["PATH"] = self.temp_dir("mox-empty-path-")
        result = self.run_mox("-d", self.out, "--watch", env=env)
        self.assertEqual(result.returncode, 2)
        self.assertIn("clipboard tool", result.stderr)

    # -- --notify

    def test_notify(self):
        tools = self.temp_dir("mox-tools-")
        log = os.path.join(tools, "notify.log")
        fake = os.path.join(tools, "notify-send")
        with open(fake, "w") as f:
            f.write(f'#!/bin/sh\nprintf "%s\\n" "$@" >> {log}\n')
        os.chmod(fake, 0o755)
        env = {**os.environ, "PATH": tools + os.pathsep + os.environ["PATH"],
               "MOX_TELEGRAM_TOKEN": "123:abc", "MOX_TELEGRAM_CHAT": "42", "MOX_TELEGRAM_API": self.base}
        Handler.telegram.clear()
        result = self.run_mox("-d", self.out, "--notify", f"{self.base}/big.bin", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(log) as f:
            desktop = f.read()
        self.assertIn("download finished", desktop)
        self.assertIn("big.bin", desktop)
        self.assertEqual(len(Handler.telegram), 1)
        self.assertEqual(Handler.telegram[0]["chat_id"], ["42"])
        self.assertIn("big.bin", Handler.telegram[0]["text"][0])

    # -- no internet

    def test_waits_for_the_internet(self):
        server = QuietServer(("127.0.0.1", 0), Handler)
        server.root = self.root
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, daemon=True).start()
        env = {**os.environ, "MOX_ONLINE_CHECK": f"127.0.0.1:{closed_port()}"}
        # -r 1 would give up within seconds if the outage counted against the retries.
        proc = subprocess.Popen([sys.executable, MOX, "-d", self.out, "-r", "1", "-c", "2", "-l", "2M",
                                 f"http://127.0.0.1:{port}/short/big.bin"],
                                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        self.addCleanup(proc.kill)
        time.sleep(1.5)
        server.shutdown()
        server.server_close()  # every new connection is refused now, like a dead internet
        time.sleep(8)
        if proc.poll() is not None:
            self.fail(f"mox gave up while the internet was down: {proc.communicate()[1]}")
        server = QuietServer(("127.0.0.1", port), Handler)
        server.root = self.root
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        _, stderr = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 0, stderr)
        self.assertDownloaded()

    def test_internet_down(self):
        mox = load_mox()
        with mock.patch.dict(os.environ, {"MOX_ONLINE_CHECK": f"127.0.0.1:{closed_port()}"}):
            self.assertTrue(mox.internet_down(f"http://127.0.0.1:{closed_port()}/x"))
            self.assertFalse(mox.internet_down(f"{self.base}/big.bin"))  # the server answers
        with mock.patch.dict(os.environ, {"MOX_ONLINE_CHECK": self.base.split("//")[1]}):
            self.assertFalse(mox.internet_down(f"http://127.0.0.1:{closed_port()}/y"))  # only the server is down

    # -- --sort, --quota, --then

    def test_sort_into_folders(self):
        self.assertEqual(self.mox("--sort", "-o", "clip.mp4", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(self.mox("--sort", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(sha256(os.path.join(self.out, "Videos", "clip.mp4")), self.digest)
        self.assertDownloaded()  # a file of no known kind stays in --dir

    def test_sort_series(self):
        for name in ("Breaking.Bad.S02E05.720p.mkv", "breaking.bad.s02e05.srt", "Breaking.Bad.S03E01.mkv"):
            result = self.mox("--sort", "-o", name, f"{self.base}/big.bin")
            self.assertEqual(result.returncode, 0, result.stderr)
        show = os.path.join(self.out, "Series", "Breaking Bad")
        self.assertEqual(sorted(os.listdir(os.path.join(show, "Season 02"))),
                         ["Breaking.Bad.S02E05.720p.mkv", "breaking.bad.s02e05.srt"])
        self.assertEqual(os.listdir(os.path.join(show, "Season 03")), ["Breaking.Bad.S03E01.mkv"])
        # A folder that differs only in case is reused rather than doubled.
        os.makedirs(os.path.join(self.out, "Series", "The office"))
        self.assertEqual(self.mox("--sort", "-o", "THE.OFFICE.S01E01.mkv", f"{self.base}/big.bin").returncode, 0)
        self.assertTrue(os.path.exists(os.path.join(self.out, "Series", "The office", "Season 01", "THE.OFFICE.S01E01.mkv")))
        # Spaces and dots do not make another show either: MrRobot joins Mr Robot.
        for name in ("Mr.Robot.S04E01.mkv", "MrRobot.S03E01.mkv", "Breaking_Bad.S05E01.mkv"):
            self.assertEqual(self.mox("--sort", "-o", name, f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(sorted(os.listdir(os.path.join(self.out, "Series"))), ["Breaking Bad", "Mr Robot", "The office"])
        robot = os.path.join(self.out, "Series", "Mr Robot")
        self.assertEqual(sorted(os.listdir(robot)), ["Season 03", "Season 04"])
        self.assertEqual(os.listdir(os.path.join(robot, "Season 03")), ["MrRobot.S03E01.mkv"])
        self.assertEqual(os.listdir(os.path.join(show, "Season 05")), ["Breaking_Bad.S05E01.mkv"])

    def test_episode_of(self):
        mox = load_mox()
        self.assertEqual(mox.episode_of("Breaking.Bad.S02E05.720p.BluRay.x264.mkv"), ("Breaking Bad", 2))
        self.assertEqual(mox.episode_of("breaking.bad.s2e5.mkv"), ("Breaking Bad", 2))
        self.assertEqual(mox.episode_of("[Group] Doctor.Who.2005.S01.E01.mkv"), ("Doctor Who 2005", 1))
        self.assertEqual(mox.episode_of("friends.1x02.avi"), ("Friends", 1))
        self.assertEqual(mox.episode_of("Dark Season 3 Episode 12.mkv"), ("Dark", 3))
        self.assertEqual(mox.episode_of("شهرزاد فصل ۲ قسمت ۵.mp4"), ("شهرزاد", 2))
        self.assertEqual(mox.episode_of("Shahrzad.Fasl.2.Ghesmat.05.mkv"), ("Shahrzad", 2))
        self.assertIsNone(mox.episode_of("Movie.2019.1920x1080.mkv"))
        self.assertIsNone(mox.episode_of("S01E01.mkv"))  # no show name to make a folder of

    def test_sort_then_resume(self):
        result = self.mox("--sort", "-o", "song.mp3", "-l", "2M", "--until", clock_in(2), f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertTrue(os.path.exists(os.path.join(self.out, "Music", "song.mp3.mox")))
        result = self.run_mox("resume", self.out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sha256(os.path.join(self.out, "Music", "song.mp3")), self.digest)
        self.assertEqual(os.listdir(os.path.join(self.out, "Music")), ["song.mp3"])
        result = self.mox("--sort", "-o", "Dark.S01E02.mkv", "-l", "2M", "--until", clock_in(2), f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertEqual(self.run_mox("resume", self.out).returncode, 0)
        self.assertEqual(sha256(os.path.join(self.out, "Series", "Dark", "Season 01", "Dark.S01E02.mkv")), self.digest)

    def test_quota(self):
        result = self.mox("--quota", "3M", "-l", "4M", f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 3, result.stderr)
        self.assertIn("--quota 3.0 MB", result.stderr)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin.mox")))
        result = self.mox(f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()
        # The history counts what both runs received, the unfinished first run included.
        self.assertRegex(self.run_mox("history").stdout, r"Received \d+\.\d MB today")
        self.assertEqual(self.mox("--quota", "lots", f"{self.base}/big.bin").returncode, 2)

    def test_then(self):
        mox = load_mox()
        mox.THEN_DELAY = 0
        tools = self.temp_dir("mox-tools-")
        called = os.path.join(tools, "called")
        script = os.path.join(tools, "systemctl")
        with open(script, "w") as f:
            f.write(f"#!/bin/sh\necho \"$@\" > {called}\n")
        os.chmod(script, 0o755)
        with mock.patch.object(mox.sys, "platform", "linux"), \
                mock.patch.dict(os.environ, {"PATH": tools}), mock.patch.object(mox.sys, "stderr", io.StringIO()):
            mox.power_off("sleep")
        with open(called) as f:
            self.assertEqual(f.read().strip(), "suspend")
        with mock.patch.object(mox.sys, "platform", "darwin"):
            self.assertEqual(mox.power_command("sleep"), ["pmset", "sleepnow"])
        with mock.patch.object(mox.sys, "platform", "win32"):
            self.assertEqual(mox.power_command("shutdown")[:2], ["shutdown", "/s"])
        self.assertEqual(self.mox("--watch", "--then", "shutdown").returncode, 2)

    # -- --sound

    def fake_player(self):
        """A pw-play stand-in that writes down what it was asked to play. Returns (env, record file)."""
        tools = self.temp_dir("mox-player-")
        record = os.path.join(tools, "played")
        script = os.path.join(tools, "pw-play")
        with open(script, "w") as f:
            f.write(f"#!/bin/sh\necho \"$@\" >> {record}\n")
        os.chmod(script, 0o755)
        return {**os.environ, "PATH": tools + os.pathsep + os.environ["PATH"]}, record

    def plays(self, *args, env_extra=None):
        env, record = self.fake_player()
        for name, value in (env_extra or {}).items():
            if value is None:
                env.pop(name, None)
            else:
                env[name] = value
        result = self.run_mox("-d", self.out, *args, f"{self.base}/big.bin", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        deadline = time.monotonic() + 3  # the player runs on its own, after mox may have exited
        while time.monotonic() < deadline and not os.path.exists(record):
            time.sleep(0.1)
        if not os.path.exists(record):
            return None
        with open(record) as f:
            return f.read().strip()

    def test_sound(self):
        played = self.plays(env_extra={"MOX_SOUND": None})  # on unless turned off
        self.assertEqual(played, os.path.join(self.data, "mox", "done.wav"))
        self.assertIsNone(self.plays("-o", "two.bin", "--no-sound", env_extra={"MOX_SOUND": None}))
        self.assertIsNone(self.plays("-o", "three.bin", env_extra={"MOX_SOUND": "0"}))
        self.assertIsNotNone(self.plays("-o", "four.bin", "--sound", env_extra={"MOX_SOUND": "0"}))

    def test_done_sound_is_gentle(self):
        mox = load_mox()
        with wave.open(io.BytesIO(mox.done_sound())) as w:
            self.assertEqual((w.getnchannels(), w.getsampwidth()), (1, 2))
            frames = w.readframes(w.getnframes())
            seconds = w.getnframes() / w.getframerate()
        samples = [int.from_bytes(frames[i:i + 2], "little", signed=True) for i in range(0, len(frames), 2)]
        self.assertLess(seconds, 0.6)
        self.assertLessEqual(max(map(abs, samples)), 0.08 * 32767)
        self.assertEqual((samples[0], samples[-1]), (0, 0))  # no click at either end

    # -- choosing what downloads first

    def test_scheduler_follows_the_order(self):
        mox = load_mox()
        downloads = [FakeDownload(name) for name in "abcd"]
        queue = mox.Scheduler(downloads, 2, None, threading.Event())
        self.addCleanup(release, queue)  # a failed assertion must not leave threads that block the exit

        def running():
            queue.tick()
            time.sleep(0.05)  # let paused threads end
            queue.tick()
            return sorted(d.name for d, _, _ in queue.running.values())

        self.assertEqual(running(), ["a", "b"])
        queue.move(3, queue.front())  # d to the front: it starts, and b, now third, makes room
        self.assertEqual(running(), ["a", "d"])
        self.assertEqual([d.name for d in downloads], ["d", "a", "b", "c"])
        self.assertEqual(downloads[2].status, "waiting")  # b, queued again rather than lost
        queue.toggle_focus(downloads[3])  # c alone
        self.assertEqual(running(), ["c"])
        queue.toggle_focus(downloads[3])
        self.assertEqual(running(), ["a", "d"])
        queue.stop.set()
        for _ in range(20):
            queue.tick()
            if queue.finished():
                break
            time.sleep(0.05)
        self.assertTrue(queue.finished())

    def test_key_names(self):
        mox = load_mox()
        self.assertEqual(mox.Keyboard.names("\x1b[A\x1b[B\x1bOA\r"), ["up", "down", "up", "enter"])
        self.assertEqual(mox.Keyboard.names("\x1b[1;2A]f\x1b[Cz"), ["move up", "move down", "focus"])
        self.assertEqual(mox.Keyboard.names("p xo"), ["pause", "pause", "remove", "open"])

    def test_display_marks_the_selected_row(self):
        mox = load_mox()
        downloads = [mox.Download(f"{self.base}/{n}.bin", argparse_namespace(self.out)) for n in "abc"]
        downloads[0].status, downloads[0].total = "downloading", 1000
        display = mox.Display(downloads, stream=io.StringIO())
        display.tty, display.selected = True, 2
        with mock.patch("shutil.get_terminal_size", return_value=os.terminal_size((100, 24))):
            display.draw()
        lines = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", display.stream.getvalue()).splitlines()
        self.assertTrue(lines[2].startswith("› "))
        self.assertIn("next", lines[1])
        self.assertIn("#2", lines[2])
        self.assertIn("Enter next", lines[3])

    @unittest.skipUnless(hasattr(os, "openpty"), "needs a pseudo-terminal")
    def test_keys_change_the_order(self):
        urls = [f"{self.base}/big.bin", f"{self.base}/alt/big.bin", f"{self.base}/short/big.bin"]
        master, slave = os.openpty()
        # mox reads keys from /dev/tty, so the pseudo-terminal must be its controlling terminal.
        # A small launcher claims it, then becomes mox: no fork from this threaded process.
        launcher = ("import fcntl, os, sys, termios; fcntl.ioctl(0, termios.TIOCSCTTY, 0); "
                    "os.execv(sys.executable, [sys.executable] + sys.argv[1:])")
        proc = subprocess.Popen([sys.executable, "-c", launcher, MOX, "-d", self.out, "-j", "1", "-l", "4M", *urls],
                                stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        os.close(slave)
        self.addCleanup(os.close, master)
        output = []
        threading.Thread(target=drain, args=(master, output), daemon=True).start()
        time.sleep(1.2)  # the first file is under way
        os.write(master, b"\x1b[B\x1b[B\r")  # down, down, Enter: the third file next
        try:
            code = proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.fail("mox did not finish: " + b"".join(output)[-2000:].decode(errors="replace"))
        self.assertEqual(code, 0, b"".join(output)[-2000:].decode(errors="replace"))
        finished = [line for line in self.history_lines()]
        self.assertEqual([re.search(r'"url": "([^"]+)"', line).group(1) for line in finished],
                         [urls[2], urls[0], urls[1]])
        names = sorted(n for n in os.listdir(self.out))
        self.assertEqual(names, ["big (1).bin", "big (2).bin", "big.bin"])  # nothing started over
        for name in names:
            self.assertEqual(sha256(os.path.join(self.out, name)), self.digest)

    # -- disk space

    def test_not_enough_disk_space(self):
        mox = load_mox()
        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.total, d.path = 10 * 1024 ** 3, os.path.join(self.out, "big.bin")
        with mock.patch("shutil.disk_usage", return_value=shutil._ntuple_diskusage(0, 0, 3 * 1024 ** 3)):
            with self.assertRaises(mox.FatalError) as caught:
                d._check_space()
        self.assertIn("not enough disk space: needs 10.0 GB, 3.0 GB free", str(caught.exception))
        # What the downloads under way still need counts too.
        d.total = 2 * 1024 ** 3
        other = mox.Download(f"{self.base}/alt/big.bin", argparse_namespace(self.out))
        other.total, other.path, other.device = 2 * 1024 ** 3, os.path.join(self.out, "other.bin"), os.stat(self.out).st_dev
        mox._writing.add(other)
        with mock.patch("shutil.disk_usage", return_value=shutil._ntuple_diskusage(0, 0, 3 * 1024 ** 3)):
            with self.assertRaises(mox.FatalError) as caught:
                d._check_space()
        self.assertIn("2.0 GB for the downloads under way", str(caught.exception))

    def test_disk_full_is_not_retried(self):
        mox = load_mox()
        full = OSError(28, "No space left on device")
        with mock.patch.object(mox, "write_at", side_effect=full):
            started = time.monotonic()
            with self.assertRaises(mox.DownloadError) as caught:
                mox.download(f"{self.base}/big.bin", self.out, retries=10)
        self.assertIn("No space left on device", str(caught.exception))
        self.assertLess(time.monotonic() - started, 5)  # not ten rounds of backoff per connection

    # -- --paste, links among text

    def test_links_copied_from_html(self):
        mox = load_mox()
        copied = "https://dl.example/a.mkv?md5=VdGX&amp;expires=1792876194"
        self.assertEqual(mox.urls_in(f"see {copied}."), ["https://dl.example/a.mkv?md5=VdGX&expires=1792876194"])
        self.assertEqual(mox.parse_args(["-d", self.out, copied]).urls, ["https://dl.example/a.mkv?md5=VdGX&expires=1792876194"])
        self.assertEqual(mox.urls_in("https://dl.example/a b"), ["https://dl.example/a"])  # nothing else changes

    def test_paste_and_links_among_text(self):
        clipboard = os.path.join(self.temp_dir("mox-clipboard-"), "clipboard")
        with open(clipboard, "w") as f:
            f.write(f"what do you suggest for these?{self.base}/big.bin and ({self.base}/empty.bin).\n")
        env = {**os.environ, "MOX_CLIPBOARD_COMMAND": f"cat {clipboard}"}
        result = self.run_mox("-d", self.out, "--paste", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()
        self.assertTrue(os.path.exists(os.path.join(self.out, "empty.bin")))
        # -i takes the same text, as from a file of links copied out of a chat.
        listing = os.path.join(self.temp_dir("mox-list-"), "links.txt")
        shutil.copy(clipboard, listing)
        out = self.temp_dir("mox-out2-")
        self.assertEqual(self.run_mox("-d", out, "-i", listing).returncode, 0)
        self.assertEqual(sorted(os.listdir(out)), ["big.bin", "empty.bin"])
        with open(clipboard, "w") as f:
            f.write("no links here")
        result = self.run_mox("-d", self.out, "--paste", env=env)
        self.assertEqual(result.returncode, 2)
        self.assertIn("no links on the clipboard", result.stderr)

    # -- --page

    def test_page(self):
        result = self.mox("--page", f"{self.base}/index/")
        self.assertEqual(result.returncode, 0, result.stderr)
        # Files only, once each: other tests add files to the server's folder, so compare with it.
        self.assertEqual(sorted(os.listdir(self.out)), sorted(os.listdir(self.root)))
        out = self.temp_dir("mox-out2-")
        result = self.run_mox("-d", out, "--page", f"{self.base}/index/", "--match", "BIG*")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(os.listdir(out), ["big.bin"])
        result = self.mox("--page", f"{self.base}/index/", "--match", "S09E01")
        self.assertEqual(result.returncode, 1)
        self.assertIn("links to no files matching 'S09E01'", result.stderr)
        self.assertEqual(self.mox("--match", "x", f"{self.base}/big.bin").returncode, 2)

    # -- --fallback-proxy

    def test_fallback_proxy(self):
        socks = f"socks5://127.0.0.1:{self.socks.server_address[1]}"
        before = self.socks.tunnels
        result = self.mox("--fallback-proxy", socks, f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.socks.tunnels, before)  # reachable directly, so it went direct
        port = self.server.server_address[1]
        result = self.mox("--fallback-proxy", socks, "-o", "blocked.bin", f"http://blocked.test:{port}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(self.socks.tunnels, before)
        self.assertEqual(sha256(os.path.join(self.out, "blocked.bin")), self.digest)
        self.assertEqual(self.mox("--fallback-proxy", "http://127.0.0.1:1", f"{self.base}/big.bin").returncode, 2)

    def test_find_local_proxy(self):
        mox = load_mox()
        port = self.socks.server_address[1]
        self.assertEqual(mox.find_local_proxy([closed_port(), port]), f"socks5h://127.0.0.1:{port}")
        self.assertIsNone(mox.find_local_proxy([closed_port(), self.server.server_address[1]]))  # HTTP is not SOCKS

    # -- --stream

    def test_in_order(self):
        mox = load_mox()
        mb = 1024 * 1024
        pieces = mox.in_order([{"start": 10 * mb, "end": 20 * mb - 1, "done": 3 * mb},
                               {"start": 0, "end": 10 * mb - 1, "done": 0}])
        self.assertEqual([(p["start"] // mb, (p["end"] + 1) // mb, p["done"] // mb) for p in pieces],
                         [(0, 4, 0), (4, 8, 0), (8, 10, 0), (10, 13, 3), (13, 17, 0), (17, 20, 0)])
        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.ranges, d.segments = True, pieces
        claimed = [d._next_segment()["start"] // mb for _ in range(3)]
        self.assertEqual(claimed, [0, 4, 8])  # from the start, in order

    def test_stream(self):
        result = self.mox("--stream", "-c", "4", f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertDownloaded()

    # -- fresh links for a whole list

    def test_fresh_links_resume_a_list(self):
        listing = os.path.join(self.temp_dir("mox-list-"), "links.txt")
        with open(listing, "w") as f:
            f.write(f"{self.base}/big.bin?md5=old&expires=1\n")
        result = self.mox("--sort", "-i", listing, "-l", "2M", "--until", clock_in(2))
        self.assertEqual(result.returncode, 3, result.stderr)
        with open(listing, "w") as f:  # the site's new links: another token, the same files
            f.write(f"{self.base}/alt/big.bin?md5=new&expires=2\n")
        result = self.mox("--sort", "-i", listing)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("resumed", result.stderr)
        self.assertDownloaded()  # big.bin, not big (1).bin started over

    # -- mox config

    def test_config(self):
        saved = self.temp_dir("mox-saved-")
        self.assertEqual(self.run_mox("config", "dir", saved).returncode, 0)
        self.assertEqual(self.run_mox("config", "sort", "on").returncode, 0)
        self.assertEqual(self.run_mox("config", "limit", "50M").returncode, 0)
        listing = self.run_mox("config").stdout
        self.assertRegex(listing, r"dir\s+" + re.escape(saved))
        self.assertRegex(listing, r"sort\s+on")
        self.assertEqual(self.run_mox("config", "sort").stdout.strip(), "on")
        # A plain run uses them: the saved folder, sorted.
        result = self.run_mox("-o", "clip.mp4", f"{self.base}/big.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sha256(os.path.join(saved, "Videos", "clip.mp4")), self.digest)
        # The command line still wins.
        self.assertEqual(self.mox(f"{self.base}/big.bin").returncode, 0)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin")))
        self.assertEqual(self.run_mox("config", "--unset", "sort").returncode, 0)
        self.assertNotIn("sort = ", open(os.path.join(self.data, "mox", "config")).read())
        bad = self.run_mox("config", "jobs", "lots")
        self.assertEqual(bad.returncode, 2)
        self.assertIn("whole number", bad.stderr)
        self.assertEqual(self.run_mox("config", "colour", "red").returncode, 2)
        # A bad line written by hand is reported and skipped, never fatal.
        with open(os.path.join(self.data, "mox", "config"), "a") as f:
            f.write("jobs = many\n")
        result = self.mox(f"{self.base}/alt/big.bin", "-o", "again.bin")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ignoring the setting jobs = many", result.stderr)

    # -- a plain 'mox'

    def run_on_terminal(self, args, typed, env=None, timeout=60):
        """Run mox on a pseudo-terminal, type `typed` (a list of (seconds to wait, bytes)), return (code, output)."""
        master, slave = os.openpty()
        launcher = ("import fcntl, os, sys, termios; fcntl.ioctl(0, termios.TIOCSCTTY, 0); "
                    "os.execv(sys.executable, [sys.executable] + sys.argv[1:])")
        proc = subprocess.Popen([sys.executable, "-c", launcher, MOX, *args], stdin=slave, stdout=slave,
                                stderr=slave, start_new_session=True, env=env)
        os.close(slave)
        self.addCleanup(os.close, master)
        output = []
        threading.Thread(target=drain, args=(master, output), daemon=True).start()
        for wait, text in typed:
            time.sleep(wait)
            os.write(master, text)
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            code = None
        time.sleep(0.2)
        return code, b"".join(output).decode(errors="replace")

    @unittest.skipUnless(hasattr(os, "openpty"), "needs a pseudo-terminal")
    def test_plain_mox_offers_the_clipboard(self):
        clipboard = os.path.join(self.temp_dir("mox-clipboard-"), "clipboard")
        with open(clipboard, "w") as f:
            f.write(f"here: {self.base}/big.bin")
        env = {**os.environ, "MOX_CLIPBOARD_COMMAND": f"cat {clipboard}"}
        code, output = self.run_on_terminal(["-d", self.out], [(1.5, b"\n")], env)
        self.assertEqual(code, 0, output)
        self.assertIn("1 link on the clipboard (big.bin). Download? [Y/n]", output)
        self.assertDownloaded()

    @unittest.skipUnless(hasattr(os, "openpty"), "needs a pseudo-terminal")
    def test_plain_mox_takes_pasted_links(self):
        env = {**os.environ, "MOX_CLIPBOARD_COMMAND": "true"}  # an empty clipboard
        code, output = self.run_on_terminal(["-d", self.out], [(1.5, f"{self.base}/big.bin\n\n".encode())], env)
        self.assertEqual(code, 0, output)
        self.assertIn("Paste the links", output)
        self.assertDownloaded()

    def test_plain_mox_in_a_script(self):
        result = self.run_mox("-d", self.out)  # no terminal: an error, not a question nobody answers
        self.assertEqual(result.returncode, 2)

    # -- p, x and o

    def test_pause_and_remove_one(self):
        mox = load_mox()
        downloads = [FakeDownload(name) for name in "abc"]
        queue = mox.Scheduler(downloads, 2, None, threading.Event())
        self.addCleanup(release, queue)  # a failed assertion must not leave threads that block the exit

        def running():
            queue.tick()
            time.sleep(0.05)
            queue.tick()
            return sorted(d.name for d, _, _ in queue.running.values())

        self.assertEqual(running(), ["a", "b"])
        queue.toggle_hold(downloads[0])  # a waits, c takes its place
        self.assertEqual(running(), ["b", "c"])
        self.assertTrue(downloads[0].held)
        queue.toggle_hold(downloads[0])  # a is first in the list again, so it runs, and c makes room
        self.assertEqual(running(), ["a", "b"])
        queue.remove(downloads[1])
        self.assertEqual(running(), ["a", "c"])
        self.assertEqual([d.name for d in downloads], ["a", "c"])
        queue.stop.set()
        while not queue.finished():
            queue.tick()
            time.sleep(0.05)

    def test_remove_needs_two_presses(self):
        mox = load_mox()
        downloads = [FakeDownload(name) for name in "ab"]
        queue = mox.Scheduler(downloads, 2, None, threading.Event())
        display = mox.Display(downloads, stream=io.StringIO(), queue=queue)
        display.selected = 1
        mox.handle_keys(["remove"], display, queue)
        self.assertEqual(len(downloads), 2)
        mox.handle_keys(["up", "remove", "down", "remove"], display, queue)  # moving away cancels it
        self.assertEqual(len(downloads), 2)
        mox.handle_keys(["remove", "remove"], display, queue)
        self.assertEqual([d.name for d in downloads], ["a"])
        self.assertEqual(display.selected, 0)

    def test_open_command(self):
        mox = load_mox()
        tools = self.temp_dir("mox-tools-")
        for tool in ("vlc", "xdg-open"):
            with open(os.path.join(tools, tool), "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(os.path.join(tools, tool), 0o755)
        with mock.patch.dict(os.environ, {"PATH": tools}), mock.patch.object(mox.sys, "platform", "linux"):
            self.assertEqual(mox.open_command("/x/a.mkv.part", True)[0], "vlc")  # a .part takes a player
            self.assertEqual(mox.open_command("/x/a.mkv", False), ["xdg-open", "/x/a.mkv"])
        with mock.patch.dict(os.environ, {"PATH": self.temp_dir("mox-empty-")}):
            self.assertIsNone(mox.open_command("/x/a.mkv.part", True))
        d = mox.Download(f"{self.base}/big.bin", argparse_namespace(self.out))
        d.status = "downloading"
        self.assertIn("press w", mox.open_download(d))

    # -- mox doctor

    def test_doctor(self):
        result = self.run_mox("doctor", timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr)
        for expected in ("mox ", "free in", "sound", "history"):
            self.assertIn(expected, result.stdout)
        self.assertIn("the sound when downloads finish is off", result.stdout)  # MOX_SOUND=0 in the tests
        self.assertRegex(result.stdout, r"\n(All good\.|\d+ things? to fix, above\.)\n$")

    # -- mox engine (the desktop app's link to mox)

    def start_engine(self, *args):
        proc = subprocess.Popen([sys.executable, MOX, "engine", "-d", self.out, *args], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(lambda: [proc.poll() is None and proc.kill(), proc.wait(),
                                 *(pipe.close() for pipe in (proc.stdin, proc.stdout, proc.stderr))])
        events = Queue()
        threading.Thread(target=lambda: [events.put(json.loads(line)) for line in proc.stdout] and None,
                         daemon=True).start()

        def send(**command):
            proc.stdin.write(json.dumps(command) + "\n")
            proc.stdin.flush()

        def wait_for(kind, test=lambda e: True, timeout=30):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    event = events.get(timeout=0.5)
                except Empty:
                    continue
                if event["type"] == kind and test(event):
                    return event
            self.fail(f"no {kind} event")

        return proc, send, wait_for

    def test_engine(self):
        proc, send, wait_for = self.start_engine("-c", "4")
        self.assertEqual(wait_for("hello")["version"], load_mox().VERSION)
        send(cmd="add", urls=[f"{self.base}/big.bin"])
        first = wait_for("added")["ids"][0]
        done = wait_for("finished", lambda e: e["id"] == first)
        self.assertEqual(done["status"], "done")
        self.assertDownloaded()
        state = wait_for("state")
        row = state["downloads"][0]
        self.assertEqual((row["id"], row["status"], row["done"], row["total"]), (first, "done", SIZE, SIZE))
        self.assertEqual(len(row["pieces"]), 120)

        # Queue control: one at a time, the second link moved ahead of the first.
        send(cmd="set", jobs=1, limit="2M")
        send(cmd="add", urls=[f"{self.base}/alt/big.bin", f"{self.base}/short/big.bin"], dir=self.temp_dir("mox-e-"))
        a, b = wait_for("added")["ids"]
        send(cmd="hold", id=a, on=True)
        held = wait_for("state", lambda e: any(d["id"] == a and d["status"] == "held" for d in e["downloads"]))
        self.assertEqual(held["jobs"], 1)
        send(cmd="set", limit=None)
        finished = wait_for("finished", lambda e: e["id"] == b)
        self.assertEqual(finished["status"], "done", finished["message"])  # b went first
        send(cmd="hold", id=a, on=False)
        self.assertEqual(wait_for("finished", lambda e: e["id"] == a)["status"], "done")

        send(cmd="add", text=f"two links:{self.base}/empty.bin\n# a comment\nand {self.base}/empty.bin again")
        self.assertEqual(len(wait_for("added")["ids"]), 1)  # found in the text, and only once
        send(cmd="nonsense")
        self.assertIn("unknown command", wait_for("error")["text"])
        send(cmd="config_set", key="sort", value="sideways")
        self.assertIn("on or off", wait_for("error")["text"])
        send(cmd="config_set", key="sort", value="on")
        self.assertEqual(wait_for("config")["settings"]["sort"], "on")
        send(cmd="history")
        self.assertGreaterEqual(len(wait_for("history")["entries"]), 3)
        send(cmd="quit")
        self.assertEqual(proc.wait(timeout=20), 0)

    def test_engine_keeps_its_list(self):
        state = os.path.join(self.temp_dir("mox-state-"), "queue.json")
        proc, send, wait_for = self.start_engine("-l", "2M", "--state", state)
        send(cmd="add", urls=[f"{self.base}/big.bin", f"{self.base}/alt/big.bin"], dir=self.out)
        wait_for("state", lambda e: any(d["done"] > 0 for d in e["downloads"]))
        second = wait_for("state")["downloads"][1]["id"]
        send(cmd="hold", id=second, on=True)
        wait_for("state", lambda e: e["downloads"][1]["status"] == "held")
        send(cmd="quit")
        self.assertEqual(proc.wait(timeout=20), 0)

        # Started again with no commands: the first carries on and finishes, the paused one waits.
        proc, send, wait_for = self.start_engine("--state", state)
        rows = wait_for("state")["downloads"]
        self.assertEqual([r["status"] for r in rows][1], "held")
        self.assertEqual(wait_for("finished")["status"], "done")
        self.assertEqual(sha256(os.path.join(self.out, "big.bin")), self.digest)
        send(cmd="hold_all", on=False)
        self.assertEqual(wait_for("finished")["status"], "done")
        self.assertDownloaded("big (1).bin")  # resumed under its own name, and nothing left over
        send(cmd="quit")
        self.assertEqual(proc.wait(timeout=20), 0)

        # Once more: both show as done, and nothing downloads or announces itself again.
        proc, send, wait_for = self.start_engine("--state", state)
        rows = wait_for("state")["downloads"]
        self.assertEqual([r["status"] for r in rows], ["done", "done"])
        send(cmd="clear")
        self.assertEqual(wait_for("state", lambda e: not e["downloads"])["downloads"], [])
        send(cmd="quit")
        self.assertEqual(proc.wait(timeout=20), 0)
        with open(state) as f:
            self.assertEqual(json.load(f)["downloads"], [])

    def test_engine_saves_progress_on_quit(self):
        proc, send, wait_for = self.start_engine("-l", "2M")
        send(cmd="add", urls=[f"{self.base}/big.bin"])
        first = wait_for("added")["ids"][0]
        wait_for("state", lambda e: e["downloads"][0]["done"] > 0)
        send(cmd="quit")
        self.assertEqual(proc.wait(timeout=20), 0)
        self.assertTrue(os.path.exists(os.path.join(self.out, "big.bin.mox")))  # resumable
        proc, send, wait_for = self.start_engine()
        send(cmd="add", urls=[f"{self.base}/big.bin"])
        self.assertEqual(wait_for("finished")["status"], "done")
        self.assertDownloaded()
        proc.stdin.close()  # the app went away: the engine exits by itself
        self.assertEqual(proc.wait(timeout=20), 0)

    # -- mox history

    def history_lines(self):
        path = os.path.join(self.data, "mox", "history.jsonl")
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as f:
            return [line for line in f.read().splitlines() if '"size"' in line]  # files, not traffic

    def test_history(self):
        self.assertIn("Nothing downloaded yet", self.run_mox("history").stdout)
        self.assertEqual(self.mox(f"{self.base}/big.bin?token=s3cret#part").returncode, 0)
        self.assertEqual(self.mox("-o", "clip.mp4", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(self.mox("-o", "song.mp3", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(self.mox("-o", "other.mp4", f"{self.base}/big.bin").returncode, 0)
        lines = self.history_lines()
        self.assertEqual(len(lines), 4)
        self.assertNotIn("s3cret", "".join(lines))  # a signed link's key stays out of the history
        self.assertIn(f'"url": "{self.base}/big.bin"', lines[0])

        result = self.run_mox("history", "-l")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("4 files", result.stdout)
        self.assertRegex(result.stdout, r"video\s+2\s+")
        self.assertRegex(result.stdout, r"audio\s+1\s+")
        self.assertRegex(result.stdout, r"other\s+1\s+")
        self.assertIn(os.path.join(self.out, "song.mp3"), result.stdout)
        self.assertNotIn("song.mp3", self.run_mox("history", "-k", "video", "-l").stdout)

        self.assertEqual(self.run_mox("history", "--clear").returncode, 0)
        self.assertEqual(self.history_lines(), [])
        self.assertIn("Nothing downloaded yet", self.run_mox("history").stdout)

    def test_history_turned_off(self):
        self.assertEqual(self.mox("--no-history", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(self.mox("-o", "again.bin", f"{self.base}/missing.bin").returncode, 1)
        os.environ["MOX_NO_HISTORY"] = "1"
        self.assertEqual(self.mox("-o", "third.bin", f"{self.base}/big.bin").returncode, 0)
        self.assertEqual(self.history_lines(), [])
        self.assertIn("MOX_NO_HISTORY is set", self.run_mox("history").stdout)

    def test_file_kind(self):
        mox = load_mox()
        self.assertEqual(mox.file_kind("Film.MKV"), "video")
        self.assertEqual(mox.file_kind("photo.jpeg"), "image")
        self.assertEqual(mox.file_kind("book.pdf"), "document")
        self.assertEqual(mox.file_kind("backup.tar.gz"), "archive")
        self.assertEqual(mox.file_kind("setup.exe"), "program")
        self.assertEqual(mox.file_kind("ubuntu.iso"), "disk image")
        self.assertEqual(mox.file_kind("download", "video/mp4; codecs=avc1"), "video")
        self.assertEqual(mox.file_kind("data.bin", "application/octet-stream"), "other")

    # -- library

    def test_library_download(self):
        mox = load_mox()
        seen = []
        path = mox.download(f"{self.base}/big.bin", self.out, progress=lambda done, total: seen.append(done))
        self.assertEqual(path, os.path.join(self.out, "big.bin"))
        self.assertDownloaded()
        self.assertTrue(seen)
        self.assertEqual(self.history_lines(), [])  # only the command keeps a history
        with self.assertRaises(mox.DownloadError):
            mox.download(f"{self.base}/nope.bin", self.out)
        with self.assertRaises(ValueError):
            mox.download(f"{self.base}/big.bin", self.out, limit="fast")


class FakeDownload:
    """Stands in for mox.Download in scheduler tests: 'downloads' until told to stop."""

    def __init__(self, name):
        self.name, self.status, self.path = name, "waiting", None
        self.held = self.removed = False

    def run(self, stop, limiter):
        self.status = "downloading"
        stop.wait()
        self.status = "paused"

    def requeue(self):
        fresh = FakeDownload(self.name)
        fresh.held = self.held
        return fresh


def release(queue):
    """Stop every thread a scheduler test started."""
    queue.stop.set()
    for _, _, halt in list(queue.running.values()):
        halt.set()


def drain(fd, into):
    """Read a pseudo-terminal until it closes, so the program writing to it never blocks."""
    while True:
        try:
            data = os.read(fd, 65536)
        except OSError:
            return
        if not data:
            return
        into.append(data)


def closed_port():
    """A local port nothing listens on, so connecting to it is refused."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def restore_env(name, value):
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def argparse_namespace(directory):
    import argparse
    return argparse.Namespace(dir=directory, connections=4, retries=1, timeout=5, headers={}, limit=None)


if __name__ == "__main__":
    unittest.main()
