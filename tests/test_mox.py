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

Every reply carries an ETag, and If-Range is honored, so a file replaced on disk counts as changed.

POST /bot<token>/sendMessage stands in for the Telegram Bot API and records what it gets.

There is also a small SOCKS5 proxy, with and without a password.
"""

import datetime
import hashlib
import importlib.util
import io
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
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
                time.sleep(len(chunk) / rate)


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
        remote = socket.create_connection((host, port))
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
        self.assertRegex(output, "\x1b\\[96m━+\x1b\\[0m(\x1b\\[(2;)?36m━\x1b\\[0m)?\x1b\\[2;37m━+")

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

    # -- library

    def test_library_download(self):
        mox = load_mox()
        seen = []
        path = mox.download(f"{self.base}/big.bin", self.out, progress=lambda done, total: seen.append(done))
        self.assertEqual(path, os.path.join(self.out, "big.bin"))
        self.assertDownloaded()
        self.assertTrue(seen)
        with self.assertRaises(mox.DownloadError):
            mox.download(f"{self.base}/nope.bin", self.out)
        with self.assertRaises(ValueError):
            mox.download(f"{self.base}/big.bin", self.out, limit="fast")


def argparse_namespace(directory):
    import argparse
    return argparse.Namespace(dir=directory, connections=4, retries=1, timeout=5, headers={}, limit=None)


if __name__ == "__main__":
    unittest.main()
