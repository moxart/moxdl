#!/usr/bin/env python3
"""mox: a fast, resumable, multi-connection download manager for the terminal.

Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import datetime
import functools
import hashlib
import http.client
import json
import os
import re
import shlex
import shutil
import socket
import ssl
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor

VERSION = "1.2"
CHUNK = 256 * 1024
MIN_SPLIT = 256 * 1024  # smallest piece when first dividing a file between connections
MIN_STEAL = 64 * 1024   # smallest piece an idle connection takes over from a busy one
MIN_STEAL_ETA = 1.0     # seconds: only take over work from a piece that would take longer than this
IDLE_POLL = 0.2         # seconds an idle connection waits before looking for work again
MIN_ACTIVE = 2         # a busy server never makes a download drop below this many connections
DEFAULT_CONNECTIONS = 16
MAX_CONNECTIONS = 64
STATE_SUFFIX = ".mox"
PART_SUFFIX = ".part"
SAVE_INTERVAL = 1.0
FRAME_INTERVAL = 0.25
SPEED_WINDOW = 0.5      # seconds between updates of each piece's measured speed
MIN_MEASURE = 0.3       # seconds a request must run before its speed is trusted
STALL_TIME = 5.0        # seconds without a single byte before a request counts as stalled
SLOW_WINDOW = 1.5       # seconds between checks of a connection's speed
SLOW_RATIO = 0.3        # slower than this share of the average connection counts as slow
SLOW_RECONNECTS = 3     # reconnect a slow connection at most this many times per piece
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
BUSY_HTTP = {429, 503}  # the server wants fewer connections
EXPIRED_HTTP = {401, 403, 410}  # what an expired signed link usually returns
RETRYABLE_ERRORS = (urllib.error.URLError, OSError, http.client.HTTPException)


class FatalError(Exception):
    """A failure that retrying will not fix."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class ServerBusy(ConnectionError):
    """A retryable HTTP status (5xx, 429) on a keep-alive connection."""

    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


class SlowConnection(Exception):
    """This connection is much slower than its siblings; reconnecting may get a better route."""


class Retire(Exception):
    """The server limits connections: this worker steps aside and leaves its piece to the others."""


# ---------------------------------------------------------------- formatting

def human_size(n: float | None) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return "?"


def human_time(seconds: float | None) -> str:
    if seconds is None:
        return "--:--"
    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours}:{minutes:02}:{seconds:02}" if hours else f"{minutes:02}:{seconds:02}"


def parse_rate(text: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmg]?)b?\s*", text, re.I)
    if not match:
        raise argparse.ArgumentTypeError(f"invalid rate {text!r} (examples: 500K, 2M)")
    multiplier = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3}[match.group(2).lower()]
    return int(float(match.group(1)) * multiplier)


def char_width(ch: str) -> int:
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in "WF" else 1


def clip(text: str, width: int) -> str:
    """Cut or pad text to exactly `width` terminal columns."""
    out, used = [], 0
    for ch in text:
        w = char_width(ch)
        if used + w > width:
            if width >= 1 and out:
                while used > width - 1:
                    used -= char_width(out.pop())
                out.append("…")
                used += 1
            break
        out.append(ch)
        used += w
    return "".join(out) + " " * (width - used)


# ---------------------------------------------------------------- HTTP

def build_headers(extra, start=None, end=None) -> dict:
    headers = {"User-Agent": f"mox/{VERSION}", "Accept-Encoding": "identity", **extra}
    if start is not None:
        headers["Range"] = f"bytes={start}-" + ("" if end is None else str(end))
    return headers


class Proxy:
    """A proxy given with --proxy: socks5://, socks5h:// or http://, optionally with user:password@."""

    SCHEMES = ("socks5", "socks5h", "http")

    def __init__(self, url: str):
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in self.SCHEMES or not parts.hostname:
            raise ValueError(f"unsupported proxy {url!r}; use socks5://host:port or http://host:port")
        self.url = url
        self.socks = parts.scheme.startswith("socks")
        self.host = parts.hostname
        self.port = parts.port or (1080 if self.socks else 8080)
        self.username = urllib.parse.unquote(parts.username) if parts.username else None
        self.password = urllib.parse.unquote(parts.password) if parts.password else ""


_proxy: Proxy | None = None
_opener = urllib.request.build_opener()  # honours http_proxy / https_proxy from the environment

SOCKS_ERRORS = {
    1: "general failure", 2: "connection not allowed by ruleset", 3: "network unreachable",
    4: "host unreachable", 5: "connection refused", 6: "TTL expired",
    7: "command not supported", 8: "address type not supported",
}


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("SOCKS proxy closed the connection")
        data += chunk
    return data


def socks5_connect(proxy: Proxy, host: str, port: int, timeout) -> socket.socket:
    """Open a TCP connection to host:port through a SOCKS5 proxy (RFC 1928, auth per RFC 1929).

    The host name is sent to the proxy unresolved, so DNS happens on the proxy's side.
    """
    timeout = timeout if isinstance(timeout, (int, float)) else None
    sock = socket.create_connection((proxy.host, proxy.port), timeout)
    try:
        methods = b"\x00\x02" if proxy.username else b"\x00"
        sock.sendall(b"\x05" + bytes([len(methods)]) + methods)
        version, method = _recv_exact(sock, 2)
        if version != 5 or method == 0xFF:
            raise FatalError("SOCKS proxy refused every login method we offered")
        if method == 2:
            user, password = proxy.username.encode(), proxy.password.encode()
            sock.sendall(b"\x01" + bytes([len(user)]) + user + bytes([len(password)]) + password)
            if _recv_exact(sock, 2)[1] != 0:
                raise FatalError("SOCKS proxy rejected the user name or password")
        name = host.encode("idna")
        sock.sendall(b"\x05\x01\x00\x03" + bytes([len(name)]) + name + port.to_bytes(2, "big"))
        _, status, _, address_type = _recv_exact(sock, 4)
        if status != 0:
            raise ConnectionError(f"SOCKS proxy: {SOCKS_ERRORS.get(status, f'error {status}')}")
        _recv_exact(sock, {1: 4, 4: 16}.get(address_type) or _recv_exact(sock, 1)[0])
        _recv_exact(sock, 2)  # bound port
        return sock
    except BaseException:
        sock.close()
        raise


class _SocksMixin:
    def __init__(self, *args, proxy: Proxy, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_connection = lambda address, timeout=None, source_address=None: \
            socks5_connect(proxy, address[0], address[1], timeout)


class SocksHTTPConnection(_SocksMixin, http.client.HTTPConnection):
    pass


class SocksHTTPSConnection(_SocksMixin, http.client.HTTPSConnection):
    pass


class _SocksHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, proxy: Proxy):
        super().__init__()
        self.proxy = proxy

    def http_open(self, req):
        return self.do_open(functools.partial(SocksHTTPConnection, proxy=self.proxy), req)


class _SocksHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, proxy: Proxy):
        super().__init__()
        self.proxy = proxy

    def https_open(self, req):
        return self.do_open(functools.partial(SocksHTTPSConnection, proxy=self.proxy), req, context=self._context)


def configure_proxy(url: str | None) -> None:
    """Route every request through --proxy. Without it, http_proxy / https_proxy still apply."""
    global _proxy, _opener
    if not url:
        return
    _proxy = Proxy(url)
    if _proxy.socks:
        _opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                              _SocksHTTPHandler(_proxy), _SocksHTTPSHandler(_proxy))
    else:
        _opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": url, "https": url}))


def request(url, headers, timeout, start=None, end=None):
    req = urllib.request.Request(url, headers=build_headers(headers, start, end))
    try:
        return _opener.open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code in RETRYABLE_HTTP:
            raise
        raise FatalError(f"HTTP {e.code} {e.reason}", e.code) from None


def keep_alive_possible(url: str) -> bool:
    """Our own persistent connections work directly or through SOCKS, but not through an HTTP proxy."""
    if _proxy:
        return _proxy.socks
    parts = urllib.parse.urlsplit(url)
    return not (parts.scheme in urllib.request.getproxies()
                and not urllib.request.proxy_bypass(parts.hostname or ""))


class KeepAlive:
    """One worker's persistent connection, so each new piece skips the TCP and TLS handshakes."""

    def __init__(self, url: str, timeout: float):
        parts = urllib.parse.urlsplit(url)
        self.https = parts.scheme == "https"
        self.host, self.port = parts.hostname, parts.port
        self.target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        self.timeout = timeout
        self.conn: http.client.HTTPConnection | None = None

    def _connect(self) -> http.client.HTTPConnection:
        extra = {"proxy": _proxy} if _proxy else {}
        if self.https:
            cls = SocksHTTPSConnection if _proxy else http.client.HTTPSConnection
            return cls(self.host, self.port, timeout=self.timeout, context=ssl.create_default_context(), **extra)
        cls = SocksHTTPConnection if _proxy else http.client.HTTPConnection
        return cls(self.host, self.port, timeout=self.timeout, **extra)

    def get(self, headers: dict) -> http.client.HTTPResponse:
        reused = self.conn is not None
        while True:
            if self.conn is None:
                self.conn = self._connect()
            try:
                self.conn.request("GET", self.target, headers=headers)
                return self.conn.getresponse()
            except (OSError, http.client.HTTPException):
                self.close()
                if not reused:
                    raise
                reused = False  # the server dropped the idle connection; one fresh try

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def safe_name(name: str) -> str:
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r'[\x00-\x1f<>:"|?*]', "_", name).strip(" .")
    return name[:200]


def filename_from(resp, url: str) -> str:
    disposition = resp.headers.get("Content-Disposition", "")
    name = ""
    encoded = re.search(r"filename\*\s*=\s*([^']*)'[^']*'([^;]+)", disposition, re.I)
    if encoded:
        try:
            name = urllib.parse.unquote(encoded.group(2).strip().strip('"'),
                                        encoding=encoded.group(1) or "utf-8", errors="replace")
        except LookupError:
            name = urllib.parse.unquote(encoded.group(2).strip().strip('"'))
    else:
        plain = re.search(r'filename\s*=\s*(?:"([^"]+)"|([^;]+))', disposition, re.I)
        if plain:
            name = (plain.group(1) or plain.group(2)).strip()
    if not name:
        name = urllib.parse.unquote(urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1])
    return safe_name(name) or "download"


def probe(url, headers, timeout):
    """Return (final_url, total_size_or_None, supports_ranges, filename)."""
    try:
        resp = request(url, headers, timeout, start=0)
    except FatalError as e:
        if e.status != 416:  # servers answer "bytes=0-" on an empty file with 416
            raise
        resp = request(url, headers, timeout)
    with resp:
        final = resp.geturl()
        name = filename_from(resp, final)
        if resp.status == 206:
            match = re.match(r"bytes\s+\d+-\d+/(\d+)", resp.headers.get("Content-Range", ""))
            if match:
                return final, int(match.group(1)), True, name
        length = resp.headers.get("Content-Length", "")
        total = int(length) if length.isdigit() and resp.status == 200 else None
        return final, total, False, name


if hasattr(os, "pwrite"):
    def write_at(fd: int, data: bytes, pos: int) -> None:
        view = memoryview(data)
        while view:
            written = os.pwrite(fd, view, pos)
            view, pos = view[written:], pos + written
else:  # Windows
    _write_lock = threading.Lock()

    def write_at(fd: int, data: bytes, pos: int) -> None:
        with _write_lock:
            os.lseek(fd, pos, os.SEEK_SET)
            os.write(fd, data)


_claimed_paths: set[str] = set()  # paths taken by downloads in this run
_claim_lock = threading.Lock()


class RateLimiter:
    """Token bucket shared by every connection of every download."""

    def __init__(self, rate: int | None):
        self.rate = rate
        self.capacity = max((rate or 0) // 4, CHUNK)  # small bucket: no big burst at start
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, n: int) -> None:
        if not self.rate:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                wait = (n - self.tokens) / self.rate
            time.sleep(wait)


# ---------------------------------------------------------------- download

class Download:
    def __init__(self, url: str, opts, name: str | None = None, sha256: str | None = None):
        self.url = self.final_url = url
        self.opts = opts
        self.custom_name = name
        self.name = name or safe_name(urllib.parse.unquote(urllib.parse.urlsplit(url).path)) or url
        self.sha256 = sha256.lower() if sha256 else None
        self.status = "waiting"
        self.message = ""
        self.error_status: int | None = None
        self.total: int | None = None
        self.done = 0
        self.ranges = False
        self.active = 0
        self.path: str | None = None
        self.segments: list[dict] = []
        self.lock = threading.Lock()
        self.failed = threading.Event()
        self.samples: deque = deque()
        self.retired = 0
        self.work_changed = threading.Condition(self.lock)  # signalled whenever a piece finishes

    # -- public

    def restart(self) -> Download:
        """A fresh copy that picks up from the saved state file, for the next scheduled session."""
        return Download(self.url, self.opts, self.custom_name, self.sha256)

    def run(self, stop: threading.Event, limiter: RateLimiter) -> None:
        if stop.is_set():
            return
        try:
            self._run(stop, limiter)
        except FatalError as e:
            self.status, self.message, self.error_status = "error", str(e), e.status
        except Exception as e:  # keep one broken download from killing the rest
            self.status, self.message = "error", f"{type(e).__name__}: {e}"

    def speed(self) -> float:
        now = time.monotonic()
        self.samples.append((now, self.done))
        while len(self.samples) > 2 and now - self.samples[0][0] > 4:
            self.samples.popleft()
        then, done_then = self.samples[0]
        return (self.done - done_then) / (now - then) if now - then > 0.5 else 0.0

    # -- orchestration

    def _run(self, stop, limiter):
        self.status = "connecting"
        info = self._probe(stop)
        if info is None:
            self.status = "paused"
            return
        self.final_url, self.total, self.ranges, server_name = info
        if not self.custom_name:
            self.name = server_name
        self.path = self._choose_path()
        self.name = os.path.basename(self.path)
        if self.status == "done":
            return

        state_path, part_path = self.path + STATE_SUFFIX, self.path + PART_SUFFIX
        resumed = self._load_state(state_path, part_path)
        self.segments = resumed or self._plan()
        self.done = sum(s["done"] for s in self.segments)
        if resumed:
            self.message = f"resumed at {human_size(self.done)}"

        with open(part_path, "r+b" if resumed else "wb", buffering=0) as f:
            if not resumed and self.total:
                f.truncate(self.total)
            self.status = "downloading"
            connections = self.opts.connections if self.ranges else 1
            workers = [threading.Thread(target=self._worker, args=(f.fileno(), stop, limiter), daemon=True)
                       for _ in range(connections)]
            for w in workers:
                w.start()
            while alive := [w for w in workers if w.is_alive()]:
                alive[0].join(SAVE_INTERVAL)
                self._save_state(state_path)
            if self.total is None and not self.failed.is_set() and not stop.is_set():
                f.truncate(self.done)

        if self.failed.is_set():
            self._save_state(state_path)
            self.status = "error"
            return
        if stop.is_set():
            self._save_state(state_path)
            self.status = "paused"
            return
        if self.total is not None and self.done != self.total:
            raise FatalError(f"incomplete: got {self.done} of {self.total} bytes")

        if self.sha256:
            self.status = "verifying"
            digest = sha256_file(part_path)
            if digest != self.sha256:
                self._remove(state_path)
                raise FatalError(f"SHA-256 mismatch (got {digest[:16]}…), kept {os.path.basename(part_path)}")
            self.message = "SHA-256 verified"
        os.replace(part_path, self.path)
        self._remove(state_path)
        self.status = "done"

    def _probe(self, stop):
        attempts = 0
        while not stop.is_set():
            try:
                return probe(self.url, self.opts.headers, self.opts.timeout)
            except RETRYABLE_ERRORS as e:
                attempts += 1
                if attempts > self.opts.retries:
                    raise FatalError(f"cannot connect: {reason(e)}") from None
                self.message = f"retry {attempts}: {reason(e)}"
                stop.wait(min(2**attempts, 30))
        return None

    def _choose_path(self) -> str:
        os.makedirs(self.opts.dir, exist_ok=True)
        path = os.path.join(self.opts.dir, self.name)
        base, ext = os.path.splitext(path)
        counter = 1
        with _claim_lock:
            while True:
                if os.path.abspath(path) not in _claimed_paths:
                    if os.path.exists(path + STATE_SUFFIX) or not os.path.exists(path):
                        break
                    if self.total is not None and os.path.isfile(path) and os.path.getsize(path) == self.total:
                        self.status, self.message, self.done = "done", "already downloaded", self.total
                        break
                path = f"{base} ({counter}){ext}"
                counter += 1
            _claimed_paths.add(os.path.abspath(path))
        return path

    def _plan(self) -> list[dict]:
        if self.total == 0:
            return []
        if not self.ranges or self.total is None:
            end = None if self.total is None else self.total - 1
            return [{"start": 0, "end": end, "done": 0}]
        count = max(1, min(self.opts.connections, self.total // MIN_SPLIT))
        size = self.total // count
        return [{"start": i * size,
                 "end": self.total - 1 if i == count - 1 else (i + 1) * size - 1,
                 "done": 0} for i in range(count)]

    # -- state file (lets an interrupted download resume)

    def _load_state(self, state_path, part_path) -> list[dict] | None:
        if not (self.ranges and os.path.exists(state_path) and os.path.exists(part_path)):
            return None
        try:
            with open(state_path, encoding="utf-8") as f:
                state = json.load(f)
            if state.get("total") != self.total or os.path.getsize(part_path) != self.total:
                return None
            return [{"start": int(s["start"]), "end": int(s["end"]), "done": int(s["done"])}
                    for s in state["segments"]]
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def _save_state(self, state_path) -> None:
        if not self.ranges:
            return
        with self.lock:
            segments = [{k: s[k] for k in ("start", "end", "done")} for s in self.segments]
        state = {"version": 1, "url": self.url, "total": self.total, "segments": segments}
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, state_path)

    @staticmethod
    def _remove(path) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    # -- connections

    def _halted(self, stop) -> bool:
        return stop.is_set() or self.failed.is_set()

    @staticmethod
    def _complete(seg) -> bool:
        if seg["end"] is None:
            return seg.get("eof", False)
        return seg["start"] + seg["done"] > seg["end"]

    @staticmethod
    def _speed_of(seg, now: float) -> float:
        """Bytes/s of a piece: its smoothed rate, or failing that its rate since its request began."""
        if seg.get("speed"):
            return seg["speed"]
        elapsed = now - seg.get("t0", now)
        if elapsed < MIN_MEASURE:
            return 0.0
        received = seg["done"] - seg["b0"]
        if received:
            return received / elapsed
        # Nothing yet. Only call it stalled after a long while: on a high-latency link a
        # fresh request can easily take a second to deliver its first byte.
        return 1.0 if elapsed >= STALL_TIME else 0.0

    @staticmethod
    def _free_from(seg) -> int:
        """First byte nobody has claimed: past both the written data and any chunk being written."""
        return max(seg["start"] + seg["done"], seg.get("reserved", 0))

    def _next_segment(self, my_speed: float = 0.0) -> dict | None:
        """Claim unfinished work, or take over part of the piece that will finish last.

        `my_speed` is how fast the asking connection ran its previous piece.
        """
        with self.lock:
            for seg in self.segments:
                if not seg.get("busy") and not self._complete(seg):
                    seg["busy"] = True
                    return seg
            if not self.ranges:
                return None
            # Only pieces that have run long enough to have a measured speed can be split:
            # guessing the speed of a fresh piece lets idle connections pass it back and
            # forth forever without anyone downloading it.
            now = time.monotonic()
            speeds = {id(s): self._speed_of(s, now) for s in self.segments if s.get("busy")}
            measured = [s for s in self.segments if speeds.get(id(s))]
            if not measured:
                return None
            thief = my_speed or sum(speeds[id(s)] for s in measured) / len(measured)

            def eta(seg) -> float:
                return (seg["end"] - self._free_from(seg) + 1) / speeds[id(seg)]

            victim = max(measured, key=eta)
            if eta(victim) < MIN_STEAL_ETA:  # finishing soon anyway; a new request would not pay off
                return None
            free = self._free_from(victim)
            left = victim["end"] - free + 1
            # Split so both sides finish together: a connection half as fast keeps a third.
            theirs = speeds[id(victim)]
            keep = int(left * theirs / (theirs + thief))
            mid = free + keep
            if victim["end"] - mid + 1 < MIN_STEAL:
                return None
            stolen = {"start": mid, "end": victim["end"], "done": 0, "busy": True}
            victim["end"] = mid - 1
            self.segments.append(stolen)
            return stolen

    def _worker(self, fd, stop, limiter) -> None:
        with self.lock:
            self.active += 1
        link = KeepAlive(self.final_url, self.opts.timeout) \
            if self.ranges and keep_alive_possible(self.final_url) else None
        my_speed = 0.0
        try:
            while not self._halted(stop):
                seg = self._next_segment(my_speed)
                if seg is None:
                    with self.lock:
                        if not any(not self._complete(s) for s in self.segments):
                            break
                        # A busy piece may become worth splitting soon; wake early if one finishes.
                        self.work_changed.wait(IDLE_POLL)
                    continue
                try:
                    self._fetch(seg, fd, stop, limiter, link)
                finally:
                    with self.lock:
                        seg["busy"] = False
                        my_speed = self._speed_of(seg, time.monotonic()) or my_speed
                        self.work_changed.notify_all()
        except Retire:
            with self.lock:
                self.retired += 1
                self.message = f"server limits connections, using {self.active - 1}"
        except FatalError as e:
            self.message, self.error_status = str(e), e.status
            self.failed.set()
        except Exception as e:
            self.message = f"{type(e).__name__}: {e}"
            self.failed.set()
        finally:
            if link:
                link.close()
            with self.lock:
                self.active -= 1

    def _open_range(self, link: KeepAlive | None, start, end):
        if not self.ranges:
            return request(self.final_url, self.opts.headers, self.opts.timeout)
        if link is None:  # going through a proxy: let urllib handle it
            resp = request(self.final_url, self.opts.headers, self.opts.timeout, start, end)
        else:
            resp = link.get(build_headers(self.opts.headers, start, end))
            if resp.status != 206:
                link.close()
                if resp.status in RETRYABLE_HTTP:
                    raise ServerBusy(resp.status)
                if resp.status != 200:
                    raise FatalError(f"HTTP {resp.status} {resp.reason}", resp.status)
        if resp.status != 206:
            raise FatalError("server stopped honoring range requests")
        return resp

    def _fetch(self, seg, fd, stop, limiter, link) -> None:
        attempts = 0
        slow_reconnects = 0
        while not self._halted(stop):
            with self.lock:
                if self._complete(seg):
                    return
                pos, end = seg["start"] + seg["done"], seg["end"]
                seg["t0"], seg["b0"] = time.monotonic(), seg["done"]
            received = 0
            finished = False
            try:
                with self._open_range(link, pos, end) as resp:
                    window_start, window_bytes = time.monotonic(), 0
                    speed_start, speed_bytes = window_start, 0
                    while not self._halted(stop):
                        with self.lock:
                            end = seg["end"]
                        want = CHUNK if end is None else min(CHUNK, end - pos + 1)
                        if want <= 0:
                            finished = True
                            break
                        data = resp.read1(want)
                        if not data:
                            break
                        limiter.consume(len(data))
                        with self.lock:
                            # Another connection may have taken over the tail meanwhile:
                            # keep only our share, and reserve it before writing.
                            end = seg["end"]
                            if end is not None:
                                data = data[:max(0, end - pos + 1)]
                            seg["reserved"] = pos + len(data)
                        if not data:
                            finished = True
                            break
                        write_at(fd, data, pos)
                        if not received and self.message.startswith("retry"):
                            self.message = ""
                        pos += len(data)
                        received += len(data)
                        window_bytes += len(data)
                        speed_bytes += len(data)
                        now = time.monotonic()
                        with self.lock:
                            seg["done"] += len(data)
                            self.done += len(data)
                            if now - speed_start >= SPEED_WINDOW:
                                instant = speed_bytes / (now - speed_start)
                                seg["speed"] = 0.5 * seg["speed"] + 0.5 * instant if seg.get("speed") else instant
                                speed_start, speed_bytes = now, 0
                        if now - window_start >= SLOW_WINDOW:
                            if slow_reconnects < SLOW_RECONNECTS and self._is_slow(window_bytes / (now - window_start)):
                                raise SlowConnection
                            window_start, window_bytes = now, 0
                    # A keep-alive connection is reusable only once its response is fully read.
                    if link and not resp.isclosed():
                        link.close()
                if finished or self._halted(stop):
                    return
                if end is None:
                    with self.lock:
                        seg["eof"] = True
                    return
                raise ConnectionError("connection closed early")
            except SlowConnection:
                slow_reconnects += 1
                if link:
                    link.close()
            except RETRYABLE_ERRORS as e:
                if link:
                    link.close()
                if self._server_busy(e) and self.active > MIN_ACTIVE:
                    raise Retire from None
                attempts = 1 if received else attempts + 1
                if attempts > self.opts.retries:
                    raise FatalError(f"gave up after {self.opts.retries} retries: {reason(e)}") from None
                if not self.ranges:  # no way to continue mid-file; start over
                    with self.lock:
                        self.done -= seg["done"]
                        seg["done"] = 0
                self.message = f"retry {attempts}: {reason(e)}"
                stop.wait(min(2**attempts, 30))

    def _is_slow(self, my_speed: float) -> bool:
        if self.active < 2 or self.opts.limit:  # with a speed cap, slowness is on purpose
            return False
        with self.lock:
            measured = [s["speed"] for s in self.segments if s.get("busy") and s.get("speed")]
        if len(measured) < 2:
            return False
        fastest_half = sorted(measured)[len(measured) // 2:]
        typical = sum(fastest_half) / len(fastest_half)
        return my_speed < SLOW_RATIO * typical

    @staticmethod
    def _server_busy(e: BaseException) -> bool:
        status = getattr(e, "status", None) if isinstance(e, ServerBusy) else getattr(e, "code", None)
        return status in BUSY_HTTP


def reason(e: BaseException) -> str:
    if isinstance(e, urllib.error.HTTPError):
        return f"HTTP {e.code}"
    if isinstance(e, urllib.error.URLError):
        return str(e.reason)
    return str(e) or type(e).__name__


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------- display

STYLE = {  # status: (icon, ANSI color)
    "waiting": ("·", "2"),
    "connecting": ("…", "33"),
    "downloading": ("↓", "36"),
    "verifying": ("#", "35"),
    "done": ("✔", "32"),
    "error": ("✖", "31"),
    "paused": ("‖", "33"),
}


class Display:
    def __init__(self, downloads: list[Download], stream=sys.stderr):
        self.downloads = downloads
        self.stream = stream
        self.tty = stream.isatty()
        self.drawn = 0
        self.reported: dict[int, str] = {}

    def draw(self) -> None:
        if not self.tty:
            self._log_changes()
            return
        size = shutil.get_terminal_size((100, 24))
        width = size.columns - 1
        rows = [self._row(d, width) for d in self._visible(max(1, size.lines - 2))]
        rows.append(self._summary(width))
        out = ["\x1b[?25l"]
        if self.drawn:
            out.append(f"\x1b[{self.drawn}F")
        out.extend(f"\x1b[2K{row}\n" for row in rows)
        leftover = self.drawn - len(rows)
        if leftover > 0:
            out.append("\x1b[2K\n" * leftover + f"\x1b[{leftover}F")
        self.drawn = len(rows)
        self.stream.write("".join(out))
        self.stream.flush()

    def close(self) -> None:
        if self.tty:
            self.stream.write("\x1b[?25h")
            self.stream.flush()

    def _visible(self, limit: int) -> list[Download]:
        if len(self.downloads) <= limit:
            return self.downloads
        rank = {"downloading": 0, "connecting": 0, "verifying": 0, "error": 1, "paused": 1, "waiting": 2, "done": 3}
        chosen = sorted(range(len(self.downloads)), key=lambda i: (rank[self.downloads[i].status], i))[:limit]
        return [self.downloads[i] for i in sorted(chosen)]

    def _row(self, d: Download, width: int) -> str:
        icon, color = STYLE[d.status]
        bar_width = 20 if width >= 100 else 10
        if d.total:
            fraction = min(1.0, d.done / d.total)
            filled = int(bar_width * fraction)
            bar = "█" * filled + "░" * (bar_width - filled)
            percent = f"{fraction * 100:5.1f}%"
        elif d.status == "downloading":
            head = int(time.monotonic() * 8) % bar_width
            bar = "".join("█" if (i - head) % bar_width < 3 else "░" for i in range(bar_width))
            percent = "      "
        else:
            bar, percent = "░" * bar_width, "      "
        if d.status == "done" and d.total == 0:
            bar, percent = "█" * bar_width, "100.0%"
        size = "" if d.status == "waiting" else f"{human_size(d.done)}/{human_size(d.total)}"

        if d.status == "downloading":
            speed = d.speed()
            left = (d.total - d.done) / speed if d.total and speed > 0 else None
            tail = f"{human_size(speed) + '/s':>11} {human_time(left):>8} {d.active:>2}×"
            if d.message.startswith("retry"):
                tail = d.message
        else:
            tail = d.message or d.status

        right = f" {percent} {size:>19}  "
        tail_width = 24
        name_width = max(8, width - 2 - bar_width - len(right) - tail_width - 1)
        line = [(f"{icon} ", color), (clip(d.name, name_width) + " ", ""), (bar, color),
                (right, ""), (clip(tail, tail_width), "2" if d.status != "downloading" else "")]
        return fit(line, width)

    def _summary(self, width: int) -> str:
        counts: dict[str, int] = {}
        for d in self.downloads:
            counts[d.status] = counts.get(d.status, 0) + 1
        speed = sum(d.speed() for d in self.downloads if d.status == "downloading") \
            if counts.get("downloading") else 0
        parts = [f"{counts.get('done', 0)}/{len(self.downloads)} done"]
        if counts.get("error"):
            parts.append(f"{counts['error']} failed")
        if speed:
            parts.append(f"{human_size(speed)}/s total")
        if any(d.status in ("waiting", "connecting", "downloading") for d in self.downloads):
            parts.append("Ctrl+C to pause")
        return fit([("  " + " · ".join(parts), "2")], width)

    def _log_changes(self) -> None:
        for i, d in enumerate(self.downloads):
            if d.status in ("done", "error", "paused") and self.reported.get(i) != d.status:
                self.reported[i] = d.status
                detail = d.message or f"{human_size(d.done)}/{human_size(d.total)}"
                print(f"[{d.status}] {d.name}: {detail}", file=self.stream, flush=True)


def fit(parts: list[tuple[str, str]], width: int) -> str:
    out, left = [], width
    for text, color in parts:
        if left <= 0:
            break
        text = text[:left]
        left -= sum(char_width(c) for c in text)
        out.append(f"\x1b[{color}m{text}\x1b[0m" if color else text)
    return "".join(out)


# ---------------------------------------------------------------- CLI

def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="mox", description="Fast, resumable, multi-connection downloads in the terminal.",
        epilog="Interrupted downloads resume automatically: run the same command again.")
    parser.add_argument("urls", nargs="*", metavar="URL")
    parser.add_argument("-i", "--input", metavar="FILE", help="read URLs from FILE, one per line ('-' for stdin)")
    parser.add_argument("-d", "--dir", default=".", help="directory to save into (default: current)")
    parser.add_argument("-o", "--output", metavar="NAME", help="file name to save as (single URL only)")
    parser.add_argument("-c", "--connections", type=int, default=DEFAULT_CONNECTIONS,
                        help=f"connections per file, up to {MAX_CONNECTIONS} (default: {DEFAULT_CONNECTIONS})")
    parser.add_argument("-j", "--jobs", type=int, default=3, help="files downloaded at once (default: 3)")
    parser.add_argument("-l", "--limit", type=parse_rate, metavar="RATE", help="total speed limit, e.g. 500K or 2M")
    parser.add_argument("-r", "--retries", type=int, default=10, help="retries per connection (default: 10)")
    parser.add_argument("-t", "--timeout", type=float, default=30, help="network timeout in seconds (default: 30)")
    parser.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'",
                        help="extra request header, e.g. cookie or referer (repeatable)")
    parser.add_argument("--sha256", metavar="HEX", help="verify the file against this checksum (single URL only)")
    parser.add_argument("-x", "--proxy", metavar="URL",
                        help="proxy for every request: socks5://host:port, socks5h://... or http://host:port")
    parser.add_argument("--at", type=parse_clock, metavar="HH:MM",
                        help="wait until this time of day before starting")
    parser.add_argument("--until", type=parse_clock, metavar="HH:MM",
                        help="pause at this time of day; with --at, carry on at the next --at, day after day")
    parser.add_argument("-V", "--version", action="version", version=f"mox {VERSION}")
    args = parser.parse_args(argv)

    urls = list(args.urls)
    if args.input:
        source = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
        with source:
            urls += [line.strip() for line in source if line.strip() and not line.lstrip().startswith("#")]
    urls = [u if "://" in u else "https://" + u for u in dict.fromkeys(urls)]
    if not urls:
        parser.error("give at least one URL (or -i FILE)")
    if len(urls) > 1 and (args.output or args.sha256):
        parser.error("--output and --sha256 work with a single URL only")
    if not 1 <= args.connections <= MAX_CONNECTIONS:
        parser.error(f"--connections must be between 1 and {MAX_CONNECTIONS}")
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")
    if args.at is not None and args.at == args.until:
        parser.error("--at and --until must differ")
    if args.proxy:
        try:
            Proxy(args.proxy)
        except ValueError as e:
            parser.error(str(e))

    headers = {}
    for header in args.header:
        name, sep, value = header.partition(":")
        if not sep or not name.strip():
            parser.error(f"bad header {header!r}, expected 'Name: value'")
        headers[name.strip()] = value.strip()
    args.headers = headers
    args.urls = urls
    return args


# ---------------------------------------------------------------- scheduling

def parse_clock(text: str) -> datetime.time:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*", text)
    if not match or int(match.group(1)) > 23 or int(match.group(2)) > 59 or int(match.group(3) or 0) > 59:
        raise argparse.ArgumentTypeError(f"invalid time {text!r} (examples: 02:00, 23:30)")
    return datetime.time(int(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def show_clock(clock: datetime.time) -> str:
    return f"{clock:%H:%M:%S}" if clock.second else f"{clock:%H:%M}"


def next_time(clock: datetime.time, now: datetime.datetime) -> datetime.datetime:
    """The next moment the wall clock shows `clock`: later today, or else tomorrow."""
    moment = now.replace(hour=clock.hour, minute=clock.minute, second=clock.second, microsecond=0)
    return moment if moment > now else moment + datetime.timedelta(days=1)


def in_window(now: datetime.datetime, start: datetime.time, end: datetime.time) -> bool:
    """Whether `now` falls between start and end, for windows that may cross midnight."""
    t = now.time()
    return start <= t < end if start < end else (t >= start or t < end)


def wait_until(moment: datetime.datetime) -> bool:
    """Sleep until `moment`, showing a countdown. False if the user pressed Ctrl+C."""
    tty = sys.stderr.isatty()
    if not tty:
        print(f"Waiting until {moment:%H:%M:%S}", file=sys.stderr, flush=True)
    try:
        while (left := (moment - datetime.datetime.now()).total_seconds()) > 0:
            if tty:
                sys.stderr.write(f"\r\x1b[2K\x1b[2m… waiting until {moment:%H:%M:%S} "
                                 f"({human_time(left)} left) · Ctrl+C to cancel\x1b[0m")
                sys.stderr.flush()
            time.sleep(min(1.0, left))
    except KeyboardInterrupt:
        return False
    finally:
        if tty:
            sys.stderr.write("\r\x1b[2K")
            sys.stderr.flush()
    return True


# ---------------------------------------------------------------- main

def run_session(downloads: list[Download], args, limiter: RateLimiter, deadline) -> str:
    """Run every waiting download. Returns 'finished', 'interrupted' or 'deadline'."""
    _claimed_paths.clear()
    stop = threading.Event()
    display = Display(downloads)
    pool = ThreadPoolExecutor(max_workers=args.jobs)
    futures = [pool.submit(d.run, stop, limiter) for d in downloads if d.status == "waiting"]
    outcome = "finished"
    try:
        try:
            while not all(f.done() for f in futures):
                if deadline and outcome == "finished" and datetime.datetime.now() >= deadline:
                    outcome = "deadline"
                    stop.set()
                display.draw()
                time.sleep(FRAME_INTERVAL)
        except KeyboardInterrupt:
            outcome = "interrupted"
            stop.set()
            while not all(f.done() for f in futures):
                display.draw()
                time.sleep(0.1)
        pool.shutdown(wait=True)
        display.draw()
    finally:
        display.close()
    return outcome


def expired_link_hint(d: Download) -> str:
    folder = os.path.dirname(d.path) if d.path else d.opts.dir
    return ("  If this link worked before, it has probably expired. Get a fresh link and run\n"
            f"  mox -d {shlex.quote(folder or '.')} -o {shlex.quote(d.name)} 'NEW_LINK'\n"
            "  to continue from where it stopped.")


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_proxy(args.proxy)
    downloads = [Download(url, args, name=args.output, sha256=args.sha256) for url in args.urls]
    limiter = RateLimiter(args.limit)

    while True:
        now = datetime.datetime.now()
        if args.at and not (args.until and in_window(now, args.at, args.until)):
            if not wait_until(next_time(args.at, now)):
                return 130
        deadline = next_time(args.until, datetime.datetime.now()) if args.until else None
        outcome = run_session(downloads, args, limiter, deadline)
        unfinished = [d for d in downloads if d.status in ("paused", "waiting")]
        if outcome != "deadline" or not unfinished or not args.at:
            break
        print(f"‖ Stopped at {show_clock(args.until)} as scheduled. Carrying on at {show_clock(args.at)}.",
              file=sys.stderr, flush=True)
        downloads = [d.restart() if d.status in ("paused", "waiting") else d for d in downloads]

    failed = [d for d in downloads if d.status == "error"]
    for d in failed:
        print(f"✖ {d.name}: {d.message}", file=sys.stderr)
        if d.error_status in EXPIRED_HTTP:
            print(expired_link_hint(d), file=sys.stderr)
    if unfinished and outcome == "deadline":
        print(f"‖ Stopped at {show_clock(args.until)} as scheduled with {len(unfinished)} download(s) unfinished. "
              "Run the same command again to continue.", file=sys.stderr)
    elif unfinished:
        print(f"‖ Paused {len(unfinished)} download(s). Run the same command again to resume.", file=sys.stderr)
    if outcome == "interrupted":
        return 130
    if failed:
        return 1
    return 3 if unfinished else 0


if __name__ == "__main__":
    sys.exit(main())
