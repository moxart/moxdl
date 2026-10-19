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

There is also a small SOCKS5 proxy, with and without a password.
"""

import datetime
import hashlib
import importlib.util
import os
import random
import re
import select
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MOX = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mox.py")
SIZE = 8 * 1024 * 1024
RATE = 8 * 1024 * 1024
DISPOSITION_NAME = "گزارش.bin"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    inflight = 0
    lock = threading.Lock()

    def log_message(self, *args):
        pass

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
        size = os.path.getsize(path)
        start, end = 0, size - 1
        match = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
        if match and mode != "norange":
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

    def setUp(self):
        self.out = tempfile.mkdtemp(prefix="mox-out-")

    def mox(self, *args, timeout=60):
        return subprocess.run([sys.executable, MOX, "-d", self.out, *args],
                              capture_output=True, text=True, timeout=timeout)

    def assertDownloaded(self, name="big.bin"):
        path = os.path.join(self.out, name)
        self.assertTrue(os.path.exists(path), f"{name} missing; folder has {os.listdir(self.out)}")
        self.assertEqual(sha256(path), self.digest)
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


if __name__ == "__main__":
    unittest.main()
