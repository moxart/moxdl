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
import io
import json
import math
import mimetypes
import os
import re
import shlex
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import wave
from collections import deque
from concurrent.futures import ThreadPoolExecutor

VERSION = "1.5"
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
OFFLINE_POLL = 5.0      # seconds between checks for the internet coming back
ONLINE_CHECKS = (("1.1.1.1", 443), ("8.8.8.8", 443))  # well-known addresses, so checking needs no DNS
ONLINE_CHECK_TIMEOUT = 3.0
# Connections that fail together share one check's answer for this long. Longer, and an "online"
# from just before an outage made its first errors count as retries.
ONLINE_CHECK_SHARED = 1.0
OFFLINE_MESSAGE = "no internet, waiting for it to come back"
SOUND_PEAK = 0.07       # loudness of --sound, about -23 dBFS: noticeable, never startling
THEN_DELAY = 60         # seconds of warning before --then shuts down or sleeps, time to press Ctrl+C
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


def parse_size(text: str) -> int:
    try:
        return parse_rate(text)
    except argparse.ArgumentTypeError:
        raise argparse.ArgumentTypeError(f"invalid size {text!r} (examples: 500M, 5G)") from None


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
        _proxy, _opener = None, urllib.request.build_opener()
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
        e.close()  # the error carries the open response; we are done with it
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


def online_checks() -> list[tuple[str, int]]:
    """Where to look for a working internet. MOX_ONLINE_CHECK=host:port,... replaces the defaults,
    for networks where those addresses are blocked."""
    custom = os.environ.get("MOX_ONLINE_CHECK")
    if not custom:
        return list(ONLINE_CHECKS)
    checks = []
    for item in custom.split(","):
        host, sep, port = item.strip().rpartition(":")
        if sep and host and port.isdigit():
            checks.append((host.strip("[]"), int(port)))
    return checks


def can_connect(host: str, port: int) -> bool:
    try:
        if _proxy and _proxy.socks:
            sock = socks5_connect(_proxy, host, port, ONLINE_CHECK_TIMEOUT)
        else:
            sock = socket.create_connection((host, port), ONLINE_CHECK_TIMEOUT)
    except FatalError:  # the proxy turned down our login: a problem, but the internet is there
        return True
    except OSError:
        return False
    sock.close()
    return True


_online_lock = threading.Lock()
_online_cache: dict[tuple[str, int], tuple[float, bool]] = {}  # server -> (when checked, online)


def internet_down(url: str) -> bool:
    """True when neither the server nor any of a few well-known addresses accepts a connection.

    The server alone cannot tell a dead server from a dead internet, and the well-known addresses
    alone cannot either: during a shutdown a domestic server may work while the rest is cut off.
    Through an HTTP proxy only the proxy itself can be checked.
    """
    parts = urllib.parse.urlsplit(url)
    server = (parts.hostname or "", parts.port or (443 if parts.scheme == "https" else 80))
    targets = [(_proxy.host, _proxy.port)] if _proxy and not _proxy.socks else [server] + online_checks()
    with _online_lock:
        checked, online = _online_cache.get(server, (None, True))
        if checked is None or time.monotonic() - checked >= ONLINE_CHECK_SHARED:
            online = any(can_connect(host, port) for host, port in targets)
            _online_cache[server] = (time.monotonic(), online)
    return not online


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


def validator_from(resp) -> str | None:
    """What identifies this version of the file: a strong ETag, or failing that Last-Modified.

    Weak ETags (W/"...") are useless here, since If-Range never matches them.
    """
    etag = resp.headers.get("ETag", "").strip()
    if etag.startswith('"'):
        return etag
    return resp.headers.get("Last-Modified", "").strip() or None


def probe(url, headers, timeout):
    """Return (final_url, total_size_or_None, supports_ranges, filename, validator_or_None, content_type)."""
    try:
        resp = request(url, headers, timeout, start=0)
    except FatalError as e:
        if e.status != 416:  # servers answer "bytes=0-" on an empty file with 416
            raise
        resp = request(url, headers, timeout)
    with resp:
        final = resp.geturl()
        name = filename_from(resp, final)
        validator = validator_from(resp)
        content_type = resp.headers.get("Content-Type", "")
        if resp.status == 206:
            match = re.match(r"bytes\s+\d+-\d+/(\d+)", resp.headers.get("Content-Range", ""))
            if match:
                return final, int(match.group(1)), True, name, validator, content_type
        length = resp.headers.get("Content-Length", "")
        total = int(length) if length.isdigit() and resp.status == 200 else None
        return final, total, False, name, validator, content_type


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
        self.used = 0  # bytes received by every download, for --quota and the traffic in the history

    def consume(self, n: int) -> None:
        with self.lock:
            self.used += n
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


# ---------------------------------------------------------------- kinds of files

# Checked before the MIME type: mimetypes calls .zip and .exe "application/...", like everything else.
KIND_EXTENSIONS = {
    "archive": {".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".tbz2", ".xz", ".txz", ".zst", ".lz4"},
    "program": {".exe", ".msi", ".apk", ".xapk", ".dmg", ".pkg", ".deb", ".rpm", ".appimage", ".flatpak", ".snap"},
    "disk image": {".iso", ".img", ".qcow2", ".vmdk", ".vdi"},
    "document": {".pdf", ".epub", ".mobi", ".azw3", ".djvu", ".doc", ".docx", ".odt", ".rtf", ".txt", ".md",
                 ".xls", ".xlsx", ".ods", ".csv", ".ppt", ".pptx", ".odp"},
    "subtitle": {".srt", ".vtt", ".ass", ".ssa", ".sub"},
}
KIND_ORDER = ("video", "audio", "image", "document", "archive", "program", "disk image", "subtitle", "other")
# --sort puts each kind in its own folder; "other" stays in --dir itself.
SORT_FOLDERS = {"video": "Videos", "audio": "Music", "image": "Pictures", "document": "Documents",
                "archive": "Archives", "program": "Programs", "disk image": "Disk images", "subtitle": "Subtitles"}
# Episodes of a series go to Series/<show>/Season NN instead, subtitles included, so players find
# them next to the video. This is the layout Plex, Jellyfin and Kodi expect.
SERIES_FOLDER = "Series"
EPISODE_PATTERNS = [re.compile(pattern, re.I) for pattern in (
    r"(?<![a-z0-9])s(\d{1,2})[\s._-]?e(\d{1,3})(?!\d)",                    # S02E05, s2e5, S02.E05
    r"(?<![\dx])(\d{1,2})x(\d{2,3})(?!\d)",                               # 2x05, but not 1920x1080
    r"season[\s._-]*(\d{1,2})[\s._-]*(?:episode|ep)[\s._-]*(\d{1,3})(?!\d)",
    r"(?:فصل|fasl)[\s._-]*(\d{1,2})[\s._-]*(?:قسمت|اپیزود|ghesmat|qesmat)[\s._-]*(\d{1,3})(?!\d)",
)]
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def file_kind(name: str, content_type: str = "") -> str:
    """Video, audio, image and so on, from the file name, or from the server's Content-Type if that says nothing."""
    ext = os.path.splitext(name)[1].lower()
    for kind, extensions in KIND_EXTENSIONS.items():
        if ext in extensions:
            return kind
    for mime in (mimetypes.guess_type(name)[0] or "", content_type.split(";")[0].strip().lower()):
        major = mime.split("/")[0]
        if major in ("video", "audio", "image"):
            return major
        if major == "text" or mime in ("application/pdf", "application/epub+zip"):
            return "document"
    return "other"


def episode_of(name: str) -> tuple[str, int] | None:
    """('Breaking Bad', 2) for 'Breaking.Bad.S02E05.720p.mkv': the show and season, if the name is an episode's."""
    stem = os.path.splitext(name)[0].translate(PERSIAN_DIGITS)
    for pattern in EPISODE_PATTERNS:
        match = pattern.search(stem)
        if match:
            break
    else:
        return None
    show = re.sub(r"^(\s*\[[^\]]*\])+", "", stem[:match.start()])  # release tags such as [Group]
    show = re.sub(r'[<>:"/\\|?*]', "", re.sub(r"[._\s]+", " ", show)).strip(" -–")
    if not show:
        return None
    if show.islower() or show.isupper():  # so breaking.bad and BREAKING.BAD share Breaking Bad
        show = show.title()
    return show, int(match.group(1))


def folder_named(parent: str, name: str) -> str:
    """`name`, or a folder already in `parent` that differs from it only in case, such as 'The office'."""
    try:
        for entry in os.listdir(parent):
            if entry.casefold() == name.casefold() and os.path.isdir(os.path.join(parent, entry)):
                return entry
    except OSError:
        pass
    return name


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
        self.validator: str | None = None
        self.content_type = ""
        self.kind = "other"
        self.dir = opts.dir  # --dir, or the folder --sort picks for this kind of file
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

    def pieces(self) -> list[tuple[int, int]]:
        """(start, bytes done) for every piece, for drawing the map."""
        with self.lock:
            return [(s["start"], s["done"]) for s in self.segments]

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
        self.final_url, self.total, self.ranges, server_name, self.validator, self.content_type = info
        if not self.custom_name:
            self.name = server_name
        self.kind = file_kind(self.name, self.content_type)
        if getattr(self.opts, "sort", False):
            self.dir = self._sorted_dir()
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
        if getattr(self.opts, "history", False):  # download() leaves it out: a library keeps no history
            record_history(self)

    def _sorted_dir(self) -> str:
        episode = episode_of(self.name) if self.kind in ("video", "subtitle") else None
        if episode:
            show, season = episode
            series = os.path.join(self.opts.dir, SERIES_FOLDER)
            return os.path.join(series, folder_named(series, show), f"Season {season:02}")
        if self.kind in SORT_FOLDERS:
            return os.path.join(self.opts.dir, SORT_FOLDERS[self.kind])
        return self.opts.dir

    def _probe(self, stop):
        attempts = 0
        while not stop.is_set():
            try:
                return probe(self.url, self.opts.headers, self.opts.timeout)
            except RETRYABLE_ERRORS as e:
                if self._wait_while_offline(e, stop):
                    attempts = 0
                    continue
                attempts += 1
                if attempts > self.opts.retries:
                    raise FatalError(f"cannot connect: {reason(e)}") from None
                self.message = f"retry {attempts}: {reason(e)}"
                stop.wait(min(2**attempts, 30))
        return None

    def _choose_path(self) -> str:
        os.makedirs(self.dir, exist_ok=True)
        path = os.path.join(self.dir, self.name)
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
            # Same size is not proof of the same file: a new release can keep the size.
            # Without a validator on either side, the size check is all there is.
            saved = state.get("validator")
            if saved and self.validator and saved != self.validator:
                self.message = "changed on the server, started over"
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
        state = {"version": 1, "url": self.url, "total": self.total, "validator": self.validator,
                 "segments": segments}
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
        # If-Range makes the server send the whole new file instead of a piece of it
        # when the file has changed, so pieces of two versions never get mixed.
        headers = {**self.opts.headers, "If-Range": self.validator} if self.validator else self.opts.headers
        if link is None:  # going through a proxy: let urllib handle it
            resp = request(self.final_url, headers, self.opts.timeout, start, end)
        else:
            resp = link.get(build_headers(headers, start, end))
            if resp.status != 206:
                link.close()
                if resp.status in RETRYABLE_HTTP:
                    raise ServerBusy(resp.status)
                if resp.status != 200:
                    raise FatalError(f"HTTP {resp.status} {resp.reason}", resp.status)
        if resp.status != 206:
            resp.close()
            if self.validator:
                raise FatalError("the file changed on the server; run again to start over")
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
                offline = self._wait_while_offline(e, stop)
                attempts = 0 if offline else 1 if received else attempts + 1
                if attempts > self.opts.retries:
                    raise FatalError(f"gave up after {self.opts.retries} retries: {reason(e)}") from None
                if not self.ranges:  # no way to continue mid-file; start over
                    with self.lock:
                        self.done -= seg["done"]
                        seg["done"] = 0
                if not offline:
                    self.message = f"retry {attempts}: {reason(e)}"
                    stop.wait(min(2**attempts, 30))

    def _wait_while_offline(self, e: BaseException, stop) -> bool:
        """If the internet is down, wait for it to come back without spending retries. True if it was down.

        Retries alone used to give up after about four minutes without internet, so a night
        download failed on the first long outage while nobody was there to restart it.
        """
        if isinstance(e, (ServerBusy, urllib.error.HTTPError)) or not internet_down(self.final_url):
            return False  # the server answered, so the internet works
        self.message = OFFLINE_MESSAGE
        while not self._halted(stop) and internet_down(self.final_url):
            stop.wait(OFFLINE_POLL)
        if self.message == OFFLINE_MESSAGE:
            self.message = ""
        return True

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
# In place of the icon while a download is busy but no data flows: connecting, retrying, waiting
# for the internet, checking the SHA-256. One dot splits into six and back, the way one
# connection becomes many. In the copper of the progress bar, one frame per redraw.
SPINNER = "⠂⠅⠕⠝⠟⠿⠟⠝⠕⠅"
SPINNER_COLOR = "38;5;173"


BAR = "━"           # every cell of the progress bar; its color says how much of that cell is done
# By shade: nothing, some, most, all of the cell's bytes on disk. Marking the cells connections
# were writing to made the bar solid white once there were more connections than cells.
# Copper uses the 256-color palette: 24-bit color is missing from some terminals, macOS Terminal among them.
BAR_COLORS = ("2;37", "2;38;5;166", "38;5;166", "38;5;173")
BAR_MIN, BAR_GOOD, BAR_MAX = 10, 20, 40  # past BAR_MAX a bar only spreads the same information thinner
NAME_MIN = 16       # narrower than this and rows drop columns to keep the name readable
NAME_WANT = 40      # names get this much before the bar grows past BAR_MIN
FIELDS = [(6, ""), (10, ""), (7, ""), (19, "2"), (3, "2")]  # width and style: %, speed, ETA, size, connections


def fields_width(count: int) -> int:
    return sum(w + 2 for w, _ in FIELDS[:count])


def coverage_map(pieces: list[tuple[int, int]], total: int, width: int) -> list[int]:
    """Split the file into `width` cells and shade each by how much of it is on disk: 0 for
    nothing, 1 for some, 2 for most, 3 for all."""
    if not total or width <= 0:
        return []
    cell = total / width
    filled = [0.0] * width
    for start, done in pieces:
        end = start + done  # bytes [start, end) are on disk
        if done > 0:
            for i in range(int(start // cell), min(width - 1, int((end - 1) // cell)) + 1):
                overlap = min(end, (i + 1) * cell) - max(start, i * cell)
                if overlap > 0:
                    filled[i] += overlap / cell
    return [3 if f >= 0.999 else 2 if f >= 0.5 else 1 if f > 0 else 0 for f in filled]


class Display:
    def __init__(self, downloads: list[Download], stream=sys.stderr, watching: bool = False):
        self.downloads = downloads
        self.stream = stream
        self.tty = stream.isatty()
        self.watching = watching
        self.stopping = False  # Ctrl+C was pressed and the workers are winding down
        self.drawn: list[int] = []  # screen width of each row of the last frame
        self.reported: dict[int, str] = {}

    def draw(self) -> None:
        if not self.tty:
            self._log_changes()
            return
        size = shutil.get_terminal_size((100, 24))
        # One column spare: a row that fills the last column leaves some terminals waiting to wrap.
        width = size.columns - 1
        visible = self._visible(max(1, size.lines - 2))
        layout = self._layout(visible, width)
        rows = [self._row(d, layout, width) for d in visible]
        rows.append(self._summary(width))
        # After a resize the terminal rewraps the old frame, so a row drawn at 120 columns now
        # takes two rows at 80. Moving up one row per old row left stale copies of the frame
        # behind every time the window got narrower.
        up = sum(max(1, -(-w // size.columns)) for w in self.drawn)
        out = ["\x1b[?25l"]
        if up:
            out.append(f"\x1b[{up}F")
        out.extend(f"\x1b[2K{row}\n" for row in rows)
        out.append("\x1b[J")
        self.drawn = [screen_width(row) for row in rows]
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

    @staticmethod
    def _layout(visible: list[Download], width: int) -> tuple[int, int, int]:
        """Column widths shared by every row, so bars and numbers line up: (name, bar, fields
        kept). Fields are dropped from the right, least useful first, until the name has room."""
        longest = max([screen_width(d.name) for d in visible] + [NAME_MIN])
        count = len(FIELDS)
        # Size and connection count matter less than a bar long enough to read as a map.
        while count > 3 and width - 2 - (BAR_GOOD + 2) - fields_width(count) < min(longest, NAME_WANT):
            count -= 1
        while count > 2 and width - 2 - (BAR_MIN + 2) - fields_width(count) < NAME_MIN:
            count -= 1
        room = width - 2 - fields_width(count)
        bar = min(BAR_MAX, room - min(longest, NAME_WANT) - 2)
        if bar < BAR_MIN:
            bar = BAR_MIN if room - NAME_MIN - 2 >= BAR_MIN else 0
        name = max(1, min(longest, room - (bar + 2 if bar else 0)))
        return name, bar, count

    def _row(self, d: Download, layout: tuple[int, int, int], width: int) -> str:
        """One line per download: name, a bar that doubles as the piece map, then the numbers."""
        name_width, bar_width, count = layout
        icon, color = STYLE[d.status]
        if d.status in ("connecting", "verifying") or \
                (d.status == "downloading" and d.message.startswith(("retry", OFFLINE_MESSAGE))):
            icon, color = SPINNER[int(time.monotonic() / FRAME_INTERVAL) % len(SPINNER)], SPINNER_COLOR
        parts = [(f"{icon} ", color), (clip(d.name, name_width), "2" if d.status == "done" else "")]
        if bar_width:
            parts.append(("  ", ""))
            parts.extend(self._bar(d, bar_width) if d.status != "done" else [(" " * bar_width, "")])
        if d.status == "downloading" and not d.message.startswith(("retry", OFFLINE_MESSAGE)):
            speed = d.speed()
            left = (d.total - d.done) / speed if d.total and speed > 0 else None
            values = [percent(d), f"{human_size(speed)}/s", human_time(left),
                      f"{human_size(d.done)}/{human_size(d.total)}", f"{d.active}×"]
            parts.extend(("  " + value.rjust(w), style) for value, (w, style) in zip(values, FIELDS[:count]))
        else:
            if d.status == "done":
                note, style = f"{d.message or 'done'} · {human_size(d.total or d.done)}", "2"
            elif d.status == "downloading":  # retrying, or waiting for the internet
                note, style = d.message, "33"
            else:
                note, style = d.message or d.status, "2"
            parts.append(("  " + ("" if d.status == "done" else percent(d)).rjust(FIELDS[0][0]), ""))
            parts.append(("  " + note, style))
        return fit(parts, width)

    @staticmethod
    def _bar(d: Download, width: int) -> list[tuple[str, str]]:
        if d.total:
            cells = coverage_map(d.pieces(), d.total, width)
        elif d.status == "downloading":  # unknown size: a pulse that shows it is alive
            head = int(time.monotonic() * 8) % width
            cells = [3 if (i - head) % width < 3 else 0 for i in range(width)]
        else:
            cells = [0] * width
        parts: list[tuple[str, str]] = []
        for shade in cells:
            style = BAR_COLORS[shade]
            if d.status not in ("downloading", "verifying") and shade:
                style = "2;33" if d.status == "paused" else "2;37"
            if parts and parts[-1][1] == style:
                parts[-1] = (parts[-1][0] + BAR, style)
            else:
                parts.append((BAR, style))
        return parts

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
        if self.stopping:
            parts.append("pausing, saving progress · Ctrl+C again to quit now")
        elif self.watching:
            parts.append("watching the clipboard for links · Ctrl+C to stop")
        elif any(d.status in ("waiting", "connecting", "downloading") for d in self.downloads):
            parts.append("Ctrl+C to pause")
        return fit([("  " + " · ".join(parts), "2")], width)

    def _log_changes(self) -> None:
        for i, d in enumerate(self.downloads):
            if d.status in ("done", "error", "paused") and self.reported.get(i) != d.status:
                self.reported[i] = d.status
                detail = d.message or f"{human_size(d.done)}/{human_size(d.total)}"
                print(f"[{d.status}] {d.name}: {detail}", file=self.stream, flush=True)


def percent(d: Download) -> str:
    if d.total:
        return f"{min(1.0, d.done / d.total) * 100:.1f}%"
    return "100.0%" if d.status == "done" else ""


def screen_width(text: str) -> int:
    return sum(char_width(c) for c in re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", text))


def fit(parts: list[tuple[str, str]], width: int) -> str:
    """Join colored pieces, cut at `width` screen columns. Cutting by characters let wide
    characters in a file name push the row past the edge, where it wrapped and broke the redraw."""
    out, left = [], width
    for text, color in parts:
        kept = []
        for c in text:
            w = char_width(c)
            if w > left:
                left = 0
                break
            kept.append(c)
            left -= w
        if kept:
            text = "".join(kept)
            out.append(f"\x1b[{color}m{text}\x1b[0m" if color else text)
        if left <= 0:
            break
    return "".join(out)


# ---------------------------------------------------------------- CLI

def add_network_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-t", "--timeout", type=float, default=30, help="network timeout in seconds (default: 30)")
    parser.add_argument("-H", "--header", action="append", default=[], metavar="'NAME: VALUE'",
                        help="extra request header, e.g. cookie or referer (repeatable)")
    parser.add_argument("-x", "--proxy", metavar="URL",
                        help="proxy for every request: socks5://host:port, socks5h://... or http://host:port")


def add_download_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-c", "--connections", type=int, default=DEFAULT_CONNECTIONS,
                        help=f"connections per file, up to {MAX_CONNECTIONS} (default: {DEFAULT_CONNECTIONS})")
    parser.add_argument("-j", "--jobs", type=int, default=3, help="files downloaded at once (default: 3)")
    parser.add_argument("-l", "--limit", type=parse_rate, metavar="RATE", help="total speed limit, e.g. 500K or 2M")
    parser.add_argument("-r", "--retries", type=int, default=10, help="retries per connection (default: 10)")
    add_network_options(parser)
    parser.add_argument("--at", type=parse_clock, metavar="HH:MM",
                        help="wait until this time of day before starting")
    parser.add_argument("--until", type=parse_clock, metavar="HH:MM",
                        help="pause at this time of day; with --at, carry on at the next --at, day after day")
    parser.add_argument("-n", "--notify", action="store_true",
                        help="when downloads end, send a desktop notification, and a Telegram message "
                             "if MOX_TELEGRAM_TOKEN and MOX_TELEGRAM_CHAT are set")
    parser.add_argument("-q", "--quota", type=parse_size, metavar="SIZE",
                        help="pause once this run has downloaded this much, e.g. 5G")
    parser.add_argument("--then", choices=("shutdown", "sleep"),
                        help=f"when the downloads end, shut down or sleep the computer, after {THEN_DELAY} "
                             "seconds to cancel with Ctrl+C")
    parser.add_argument("--sound", action="store_const", const=True, default=None,
                        help="play a soft sound when downloads finish; MOX_SOUND=1 turns it on for good")
    parser.add_argument("--no-sound", dest="sound", action="store_const", const=False,
                        help="no sound this time, even with MOX_SOUND=1")
    parser.add_argument("--no-history", dest="history", action="store_false",
                        help="do not add these downloads to the history ('mox history'); "
                             "MOX_NO_HISTORY=1 turns it off for good")


def check_network_options(parser: argparse.ArgumentParser, args) -> None:
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


def check_download_options(parser: argparse.ArgumentParser, args) -> None:
    if not 1 <= args.connections <= MAX_CONNECTIONS:
        parser.error(f"--connections must be between 1 and {MAX_CONNECTIONS}")
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")
    if args.at is not None and args.at == args.until:
        parser.error("--at and --until must differ")
    check_network_options(parser, args)
    if history_disabled():
        args.history = False
    if args.sound is None:
        args.sound = os.environ.get("MOX_SOUND", "") not in ("", "0")


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="mox", description="Fast, resumable, multi-connection downloads in the terminal.",
        epilog="Interrupted downloads resume automatically: run the same command again. "
               "Other commands: 'mox resume [DIR]' continues every unfinished download in a folder, "
               "'mox history' shows what you have downloaded, "
               "'mox bench URL' measures how much faster many connections are.")
    parser.add_argument("urls", nargs="*", metavar="URL")
    parser.add_argument("-i", "--input", metavar="FILE", help="read URLs from FILE, one per line ('-' for stdin)")
    parser.add_argument("-d", "--dir", default=".", help="directory to save into (default: current)")
    parser.add_argument("-o", "--output", metavar="NAME", help="file name to save as (single URL only)")
    parser.add_argument("--sha256", metavar="HEX", help="verify the file against this checksum (single URL only)")
    parser.add_argument("-w", "--watch", action="store_true",
                        help="keep running and download every file link you copy to the clipboard")
    parser.add_argument("-s", "--sort", action="store_true",
                        help="save each kind of file in its own folder in --dir: Videos, Music, Pictures, "
                             "Documents, Archives, Programs and so on, and episodes in Series/<show>/Season NN")
    add_download_options(parser)
    parser.add_argument("-V", "--version", action="version", version=f"mox {VERSION}")
    args = parser.parse_args(argv)

    urls = list(args.urls)
    if args.input:
        source = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
        with source:
            urls += [line.strip() for line in source if line.strip() and not line.lstrip().startswith("#")]
    urls = [u if "://" in u else "https://" + u for u in dict.fromkeys(urls)]
    if not urls and not args.watch:
        parser.error("give at least one URL (or -i FILE, or --watch)")
    if (len(urls) > 1 or args.watch) and (args.output or args.sha256):
        parser.error("--output and --sha256 work with a single URL only")
    if args.watch and (args.at or args.until or args.quota or args.then):
        parser.error("--watch cannot be combined with --at, --until, --quota or --then")
    check_download_options(parser, args)
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


# ---------------------------------------------------------------- notifications

def notify(title: str, body: str) -> None:
    """Desktop notification where a tool for it exists, plus Telegram when configured.

    A notification that fails only prints a warning: it never changes the exit code.
    """
    token, chat = os.environ.get("MOX_TELEGRAM_TOKEN"), os.environ.get("MOX_TELEGRAM_CHAT")
    if token and chat:
        # MOX_TELEGRAM_API points at a self-hosted Bot API server, if you run one.
        api = os.environ.get("MOX_TELEGRAM_API", "https://api.telegram.org").rstrip("/")
        data = urllib.parse.urlencode({"chat_id": chat, "text": f"{title}\n{body}"}).encode()
        try:
            _opener.open(urllib.request.Request(f"{api}/bot{token}/sendMessage", data=data), timeout=15).close()
        except (OSError, http.client.HTTPException) as e:
            print(f"! Telegram notification failed: {reason(e)}", file=sys.stderr)
    for command in (["notify-send", "--app-name=mox", title, body],
                    ["termux-notification", "--title", title, "--content", body],
                    ["osascript", "-e", f"display notification {json.dumps(body)} with title {json.dumps(title)}"]):
        if shutil.which(command[0]):
            try:
                subprocess.run(command, capture_output=True, timeout=10)
            except (OSError, subprocess.SubprocessError):
                pass
            break


def notify_summary(downloads: list[Download], outcome: str) -> None:
    done = [d for d in downloads if d.status == "done"]
    failed = [d for d in downloads if d.status == "error"]
    unfinished = [d for d in downloads if d.status in ("paused", "waiting")]
    if outcome == "deadline" and unfinished:
        title = f"mox stopped as scheduled, {len(unfinished)} unfinished"
    elif outcome == "quota" and unfinished:
        title = f"mox reached the --quota, {len(unfinished)} unfinished"
    elif failed:
        title = f"mox: {len(done)} done, {len(failed)} failed"
    else:
        title = "mox: all downloads finished" if len(done) != 1 else "mox: download finished"
    names = [d.name for d in done + failed][:5]
    more = len(done) + len(failed) - len(names)
    notify(title, "\n".join(names) + (f"\nand {more} more" if more > 0 else ""))


# ---------------------------------------------------------------- --then

def power_command(action: str) -> list[str] | None:
    """The command that shuts down ("shutdown") or sleeps ("sleep") this computer without sudo."""
    if sys.platform == "win32":
        # SetSuspendState hibernates instead when hibernation is turned on.
        return ["shutdown", "/s", "/t", "0"] if action == "shutdown" \
            else ["rundll32.exe", "powrprof.dll,SetSuspendState", "0,1,0"]
    if sys.platform == "darwin":
        return ["osascript", "-e", 'tell application "System Events" to shut down'] if action == "shutdown" \
            else ["pmset", "sleepnow"]
    for tool in ("systemctl", "loginctl"):
        if shutil.which(tool):
            return [tool, "poweroff" if action == "shutdown" else "suspend"]
    return None


def power_off(action: str) -> None:
    """Shut down or sleep after THEN_DELAY seconds, unless Ctrl+C is pressed first."""
    command = power_command(action)
    if not command:
        print(f"! --then {action}: no way to do that on this system was found", file=sys.stderr)
        return
    verb = "Shutting down" if action == "shutdown" else "Going to sleep"
    tty = sys.stderr.isatty()
    if not tty:
        print(f"{verb} in {THEN_DELAY} seconds. Ctrl+C to cancel.", file=sys.stderr, flush=True)
    deadline = time.monotonic() + THEN_DELAY
    try:
        while (left := deadline - time.monotonic()) > 0:
            if tty:
                sys.stderr.write(f"\r\x1b[2K{verb} in {left:.0f} seconds · Ctrl+C to cancel")
                sys.stderr.flush()
            time.sleep(min(1.0, left))
    except KeyboardInterrupt:
        print(f"\r\x1b[2K‖ Cancelled: not {'shutting down' if action == 'shutdown' else 'going to sleep'}."
              if tty else "‖ Cancelled.", file=sys.stderr)
        return
    if tty:
        sys.stderr.write("\r\x1b[2K")
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"! --then {action} failed: {e}", file=sys.stderr)
        return
    if result.returncode != 0:
        print(f"! --then {action} failed: {result.stderr.strip() or f'{command[0]} exited with {result.returncode}'}",
              file=sys.stderr)


# ---------------------------------------------------------------- --sound

def done_sound() -> bytes:
    """The --sound chime as a WAV file: one low, hollow note, like blowing across a bottle.

    Made here rather than shipped, so mox stays one file and sounds the same everywhere. Every
    choice keeps it gentle, after brighter versions were found grating: a low note (220 Hz) with
    only a faint third harmonic, a 25 ms S-shaped rise with no click at the start, a low-pass at
    1.2 kHz, and a quiet peak.
    """
    rate, length, freq, rise, decay = 44100, 0.45, 220.0, 0.025, 0.12
    lowpass = 1 - math.exp(-2 * math.pi * 1200 / rate)
    samples, low = [], 0.0
    for i in range(int(length * rate)):
        t = i / rate
        attack = 0.5 - 0.5 * math.cos(math.pi * min(1.0, t / rise))
        wave_value = math.sin(2 * math.pi * freq * t) + 0.03 * math.sin(6 * math.pi * freq * t) * math.exp(-6 * t)
        low += lowpass * (attack * math.exp(-t / decay) * wave_value - low)
        samples.append(low)
    fade = int(0.02 * rate)  # the last 20 ms go to silence, so the end does not click either
    for i in range(fade):
        samples[-1 - i] *= i / fade
    scale = SOUND_PEAK * 32767 / max(abs(v) for v in samples)
    data = io.BytesIO()
    with wave.open(data, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(b"".join(struct.pack("<h", int(v * scale)) for v in samples))
    return data.getvalue()


def sound_file() -> str:
    """The chime saved in the cache folder, since the players below take a file, not bytes."""
    base = os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache")
    path = os.path.join(base, "mox", "done.wav")
    data = done_sound()
    try:
        with open(path, "rb") as f:
            if f.read() == data:
                return path
    except OSError:
        pass
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".new", "wb") as f:
        f.write(data)
    os.replace(path + ".new", path)
    return path


def play_done_sound() -> None:
    """Play the chime without waiting for it. No player, or a failing one, means no sound, never an error."""
    try:
        if sys.platform == "win32":
            import winsound  # Windows only, part of the standard library there
            winsound.PlaySound(done_sound(), winsound.SND_MEMORY)
            return
        players = (["pw-play"], ["paplay"], ["aplay", "-q"], ["afplay"], ["termux-media-player", "play"])
        player = next((p for p in players if shutil.which(p[0])), None)
        if player:
            # A session of its own, so the sound finishes even when mox exits first.
            subprocess.Popen(player + [sound_file()], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    except (OSError, RuntimeError):
        pass


# ---------------------------------------------------------------- clipboard

URL_PATTERN = re.compile(r"https?://[^\s<>\"'`]+")
PAGE_EXTENSIONS = {"", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi"}
CLIPBOARD_POLL = 1.0  # seconds between clipboard reads in --watch mode


def clipboard_command() -> list[str] | None:
    custom = os.environ.get("MOX_CLIPBOARD_COMMAND")
    if custom:
        return shlex.split(custom)
    candidates = [["wl-paste", "--no-newline"]] if os.environ.get("WAYLAND_DISPLAY") else []
    candidates += [["xclip", "-o", "-selection", "clipboard"], ["xsel", "--clipboard", "--output"],
                   ["pbpaste"], ["termux-clipboard-get"],
                   ["powershell.exe", "-NoProfile", "-Command", "Get-Clipboard"]]
    return next((c for c in candidates if shutil.which(c[0])), None)


def read_clipboard(command: list[str]) -> str:
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def links_in(text: str) -> list[str]:
    """File links in a piece of text. Links to web pages are skipped: copying the address of
    an article should not download its HTML."""
    links = []
    for url in URL_PATTERN.findall(text):
        url = url.rstrip(".,;:)]}>")
        name = urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]
        if os.path.splitext(name)[1].lower() not in PAGE_EXTENSIONS:
            links.append(url)
    return links


# ---------------------------------------------------------------- running downloads

def run_session(downloads: list[Download], args, limiter: RateLimiter, deadline) -> str:
    """Run every waiting download. Returns 'finished', 'interrupted', 'deadline' or 'quota'."""
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
                if args.quota and outcome == "finished" and limiter.used >= args.quota:
                    outcome = "quota"
                    stop.set()
                display.draw()
                time.sleep(FRAME_INTERVAL)
        except KeyboardInterrupt:
            outcome = "interrupted"
            wind_down(futures, stop, display)
        pool.shutdown(wait=True)
        display.draw()
    finally:
        display.close()
    return outcome


def wind_down(futures, stop: threading.Event, display: Display) -> None:
    """After Ctrl+C: let the workers save their progress and stop. A second Ctrl+C quits at once.

    A connection stuck on a slow server notices `stop` only when its read times out (--timeout),
    so people pressed Ctrl+C again, and that printed three tracebacks: one from this loop and one
    from Python joining the worker threads at exit. os._exit skips that join. The state file is
    saved about every second and replaced atomically, so quitting loses at most that much.
    """
    stop.set()
    display.stopping = True
    try:
        while not all(f.done() for f in futures):
            display.draw()
            time.sleep(0.1)
    except KeyboardInterrupt:
        display.close()
        print("\n‖ Quit without waiting. Run the same command again to continue.", file=sys.stderr, flush=True)
        os._exit(130)


def watch_session(downloads: list[Download], args, limiter: RateLimiter, command: list[str]) -> str:
    """Download what is given, then every new file link that shows up on the clipboard."""
    _claimed_paths.clear()
    seen = {d.url for d in downloads} | set(links_in(read_clipboard(command)))  # not what was there before
    stop = threading.Event()
    display = Display(downloads, watching=True)
    pool = ThreadPoolExecutor(max_workers=args.jobs)
    futures = [pool.submit(d.run, stop, limiter) for d in downloads]
    announced: set[int] = set()
    last_poll = 0.0
    try:
        try:
            while True:
                if time.monotonic() - last_poll >= CLIPBOARD_POLL:
                    last_poll = time.monotonic()
                    for url in links_in(read_clipboard(command)):
                        if url not in seen:
                            seen.add(url)
                            d = Download(url, args)
                            downloads.append(d)
                            futures.append(pool.submit(d.run, stop, limiter))
                for d in downloads:
                    if d.status in ("done", "error") and id(d) not in announced:
                        announced.add(id(d))
                        if args.notify:
                            notify("mox: download finished" if d.status == "done" else "mox: download failed",
                                   d.name if d.status == "done" else f"{d.name}: {d.message}")
                        if args.sound and d.status == "done":
                            play_done_sound()
                display.draw()
                time.sleep(FRAME_INTERVAL)
        except KeyboardInterrupt:
            wind_down(futures, stop, display)
        pool.shutdown(wait=True)
        display.draw()
    finally:
        display.close()
    return "interrupted" if any(d.status in ("paused", "waiting") for d in downloads) else "finished"


def expired_link_hint(d: Download) -> str:
    folder = os.path.dirname(d.path) if d.path else d.dir
    started = os.path.exists(os.path.join(folder or ".", d.name + STATE_SUFFIX))
    query = urllib.parse.urlsplit(d.url).query
    # An unquoted link like 'file.mkv?md5=...&expires=...' reaches mox as 'file.mkv?md5=...': the
    # shell takes '&' to mean "run in the background". The server then refuses the link, and
    # telling the user it expired sent them looking for a fresh link they did not need.
    if query and "&" not in query and not started:
        return ("  If the link has an '&' in it, the shell cut it off there. Run the command again\n"
                "  with the whole link inside single quotes:  mox 'https://...the whole link...'")
    return ("  If this link worked before, it has probably expired. Get a fresh link and run\n"
            f"  mox -d {shlex.quote(folder or '.')} -o {shlex.quote(d.name)} 'NEW_LINK'\n"
            "  to continue from where it stopped.")


def report(downloads: list[Download], outcome: str, args) -> int:
    failed = [d for d in downloads if d.status == "error"]
    unfinished = [d for d in downloads if d.status in ("paused", "waiting")]
    for d in failed:
        print(f"✖ {d.name}: {d.message}", file=sys.stderr)
        if d.error_status in EXPIRED_HTTP:
            print(expired_link_hint(d), file=sys.stderr)
    if unfinished and outcome == "quota":
        print(f"‖ Stopped after {human_size(args.quota)} as --quota {human_size(args.quota)} asked, "
              f"with {len(unfinished)} download(s) unfinished. Run the same command again to continue.",
              file=sys.stderr)
    elif unfinished and outcome == "deadline":
        print(f"‖ Stopped at {show_clock(args.until)} as scheduled with {len(unfinished)} download(s) unfinished. "
              "Run the same command again to continue.", file=sys.stderr)
    elif unfinished:
        print(f"‖ Paused {len(unfinished)} download(s). Run the same command again, "
              "or 'mox resume' in that folder, to continue.", file=sys.stderr)
    if outcome == "interrupted":
        return 130
    if failed:
        return 1
    return 3 if unfinished else 0


def run_downloads(downloads: list[Download], args) -> int:
    """Run downloads, on the --at/--until schedule if there is one, and report how it went."""
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
    if args.history:
        record_traffic(limiter.used)
    if args.notify and outcome != "interrupted":
        notify_summary(downloads, outcome)
    code = report(downloads, outcome, args)
    if args.sound and outcome != "interrupted" and any(d.status == "done" for d in downloads):
        play_done_sound()
    if args.then and outcome != "interrupted":
        power_off(args.then)
    return code


# ---------------------------------------------------------------- mox resume

def resume_main(argv) -> int:
    parser = argparse.ArgumentParser(
        prog="mox resume", description="Continue every unfinished download in a folder.",
        epilog="mox remembers each unfinished download in a FILE.mox next to it, link included.")
    parser.add_argument("dir", nargs="?", default=".", help="folder to look in (default: current)")
    add_download_options(parser)
    args = parser.parse_args(argv)
    check_download_options(parser, args)
    configure_proxy(args.proxy)

    downloads = []
    # --sort keeps unfinished downloads in the kind folders, next to where they will end up.
    folders = [args.dir] + [os.path.join(args.dir, sub) for sub in SORT_FOLDERS.values()]
    folders += sorted(root for root, _, _ in os.walk(os.path.join(args.dir, SERIES_FOLDER)))
    for folder in folders:
        for entry in sorted(os.listdir(folder)) if os.path.isdir(folder) else []:
            if not entry.endswith(STATE_SUFFIX):
                continue
            try:
                with open(os.path.join(folder, entry), encoding="utf-8") as f:
                    url = json.load(f)["url"]
            except (OSError, ValueError, KeyError, TypeError):
                print(f"! skipping {entry}: not a state file mox can read", file=sys.stderr)
                continue
            d = Download(url, args, name=entry[:-len(STATE_SUFFIX)])
            d.dir = folder
            downloads.append(d)
    if not downloads:
        print(f"No unfinished downloads in {args.dir}.", file=sys.stderr)
        return 0
    return run_downloads(downloads, args)


# ---------------------------------------------------------------- mox bench

def bench_round(url: str, total: int | None, connections: int, seconds: float, args) -> tuple[float, bool, list[str]]:
    """Download for `seconds` over `connections` connections, keeping nothing.

    Returns (bytes per second, whether the round was too short to trust, why each refused
    connection was refused).
    """
    received = [0]
    first_finish: list[tuple[float, int]] = []  # (time, bytes) when the first connection ran out of work
    refused: list[str] = []
    lock = threading.Lock()
    deadline = time.monotonic() + seconds

    def fetch(start: int | None, end: int | None) -> None:
        link = KeepAlive(url, args.timeout) if start is not None and keep_alive_possible(url) else None
        try:
            if link:
                resp = link.get(build_headers(args.headers, start, end))
                if resp.status not in (200, 206):
                    raise ServerBusy(resp.status)
            else:
                resp = request(url, args.headers, args.timeout, start, end)
            with resp:
                while time.monotonic() < deadline:
                    data = resp.read1(CHUNK)
                    if not data:
                        # Only a connection that got through its whole share ends the window.
                        # A refused one (Hetzner answers 429 past two connections) must not:
                        # it would stop the clock at zero bytes.
                        with lock:
                            if not first_finish:
                                first_finish.append((time.monotonic(), received[0]))
                        break
                    with lock:
                        received[0] += len(data)
        except (RETRYABLE_ERRORS + (FatalError,)) as e:
            with lock:
                refused.append(reason(e))
        finally:
            if link:
                link.close()

    if total:
        bounds = [(total * i // connections, total * (i + 1) // connections - 1) for i in range(connections)]
    else:
        bounds = [(None, None)]
    threads = [threading.Thread(target=fetch, args=b, daemon=True) for b in bounds]
    started = time.monotonic()
    for t in threads:
        t.start()
    tty = sys.stderr.isatty()
    label = f"{connections:>3} connection{'s' if connections > 1 else ' '}"
    while any(t.is_alive() for t in threads):
        time.sleep(FRAME_INTERVAL)
        if tty:
            elapsed = time.monotonic() - started
            sys.stderr.write(f"\r\x1b[2K{label}  {human_size(received[0] / elapsed) + '/s':>11}  "
                             f"\x1b[2m{max(0.0, seconds - elapsed):.0f}s left\x1b[0m")
            sys.stderr.flush()
    if tty:
        sys.stderr.write("\r\x1b[2K")
    # Count only while every connection was busy. Once the fast ones finish their share, the
    # slow stragglers alone would drag the average down and hide what N connections can do.
    ended, got = first_finish[0] if first_finish else (time.monotonic(), received[0])
    window = max(ended - started, 1e-3)
    return got / window, window < min(1.0, seconds / 2), refused


def bench_main(argv) -> int:
    parser = argparse.ArgumentParser(
        prog="mox bench", description="Measure how much faster many connections are than one. "
                                      "Nothing is saved to disk.")
    parser.add_argument("url", metavar="URL")
    parser.add_argument("-s", "--seconds", type=float, default=5, help="length of each round (default: 5)")
    parser.add_argument("-c", "--connections", type=int, default=DEFAULT_CONNECTIONS,
                        help=f"most connections to try (default: {DEFAULT_CONNECTIONS})")
    add_network_options(parser)
    args = parser.parse_args(argv)
    if not 1 <= args.connections <= MAX_CONNECTIONS:
        parser.error(f"--connections must be between 1 and {MAX_CONNECTIONS}")
    check_network_options(parser, args)
    configure_proxy(args.proxy)
    url = args.url if "://" in args.url else "https://" + args.url

    try:
        final, total, ranges, name, _, _ = probe(url, args.headers, args.timeout)
    except (RETRYABLE_ERRORS + (FatalError,)) as e:
        print(f"✖ {reason(e)}", file=sys.stderr)
        return 1
    support = "supports range requests" if ranges else "does not support range requests"
    print(f"{name} · {human_size(total)} · the server {support}")
    levels = sorted({1, min(4, args.connections), args.connections}) if ranges else [1]
    rates: dict[int, float] = {}
    allowed: int | None = None  # how many connections the server accepted, when it refused some
    rough = False
    try:
        for connections in levels:
            rate, short, refused = bench_round(final, total if ranges else None, connections, args.seconds, args)
            rates[connections] = rate
            rough = rough or short
            gain = f"  {rate / rates[1]:4.1f}×" if connections > 1 and rates[1] else ""
            label = f"{connections:>3} connection{'s' if connections > 1 else ' '}"
            note = f"  \x1b[2m{len(refused)} refused: {refused[0]}\x1b[0m" if refused and sys.stdout.isatty() \
                else (f"  {len(refused)} refused: {refused[0]}" if refused else "")
            print(f"{label}  {human_size(rate) + '/s':>11}{gain}{note}", flush=True)
            if refused and allowed is None:
                allowed = connections - len(refused)
    except KeyboardInterrupt:
        return 130

    best = max(rates, key=rates.get)
    gain = rates[best] / rates[1] if rates[1] else 0
    if not ranges:
        print("This server sends files in one piece, so extra connections cannot help here.")
    elif allowed is not None:
        print(f"This server accepts only about {max(allowed, 1)} connections at once and refuses the rest. "
              f"mox backs off by itself, but -c {max(allowed, 1)} avoids the refusals.")
    elif gain >= 1.5:
        print(f"This server limits each connection. With -c {best}, mox is about {gain:.0f}× faster here "
              "than a one-connection download such as wget or curl.")
    else:
        print("Extra connections barely help here: your line or the server's total is the limit. "
              "A few connections, -c 4, are plenty.")
    if rough:
        print("The file is small enough to finish within a round, so these numbers are rough.")
    return 0


# ---------------------------------------------------------------- mox history

HISTORY_LIST = 20  # rows 'mox history -l' shows without a number
_history_lock = threading.Lock()  # jobs finish on different threads; one line must not cut into another


def history_disabled() -> bool:
    return os.environ.get("MOX_NO_HISTORY", "") not in ("", "0")


def history_path() -> str:
    if os.name == "nt":
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
    else:
        base = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    return os.path.join(base, "mox", "history.jsonl")


def link_without_secrets(url: str) -> str:
    """The link minus its query, fragment and password: signed links carry their keys there."""
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    if parts.port:
        host += f":{parts.port}"
    return urllib.parse.urlunsplit((parts.scheme, host, parts.path, "", ""))


def record_history(d: Download) -> None:
    """Add one finished download to the history."""
    append_history({"time": datetime.datetime.now().isoformat(timespec="seconds"), "name": d.name,
                    "path": os.path.abspath(d.path), "size": d.done, "kind": d.kind,
                    "url": link_without_secrets(d.url)})


def record_traffic(received: int) -> None:
    """Add what a run received, finished or not, so 'mox history' can tell how much of a data plan went."""
    if received:
        append_history({"time": datetime.datetime.now().isoformat(timespec="seconds"), "traffic": received})


def append_history(entry: dict) -> None:
    """A failure here must never fail a download."""
    path = history_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with _history_lock, open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass


def read_history(path: str) -> list[dict]:
    """Every entry, files and traffic, oldest first. Lines that do not parse, such as one cut short by a full disk, are skipped."""
    entries = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if isinstance(entry, dict):
                    entries.append(entry)
    except FileNotFoundError:
        pass
    return entries


def traffic_line(entries: list[dict]) -> str:
    """How much was received today and in the last 7 and 30 days, unfinished downloads included.

    Days rather than calendar months: data plans run 30 days from purchase, and the calendar
    month means different things to different users.
    """
    now = datetime.datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    spans = [("today", today), ("7 days", now - datetime.timedelta(days=7)),
             ("30 days", now - datetime.timedelta(days=30))]
    totals = [0, 0, 0]
    seen = False
    for e in entries:
        received = e.get("traffic")
        if not isinstance(received, int) or isinstance(received, bool):
            continue
        try:
            when = datetime.datetime.fromisoformat(str(e.get("time")))
        except ValueError:
            continue
        seen = True
        for i, (_, since) in enumerate(spans):
            if when >= since:
                totals[i] += received
    if not seen:
        return ""
    return "Received " + ", ".join(f"{human_size(total)} {'' if label == 'today' else 'in '}{label}"
                                   for (label, _), total in zip(spans, totals))


def history_main(argv) -> int:
    parser = argparse.ArgumentParser(
        prog="mox history", description="Show what mox has downloaded, counted by kind of file.",
        epilog="Every finished download is added to the history. 'mox --no-history URL' leaves one out, "
               "and MOX_NO_HISTORY=1 stops keeping a history at all. Links are saved without "
               "their ?query part, where signed links keep their keys.")
    parser.add_argument("-l", "--list", nargs="?", type=int, const=HISTORY_LIST, metavar="N",
                        help=f"also list the last N downloads (default: {HISTORY_LIST})")
    parser.add_argument("-k", "--kind", choices=KIND_ORDER, help="only this kind of file")
    parser.add_argument("--clear", action="store_true", help="erase the history")
    args = parser.parse_args(argv)
    path = history_path()

    if args.clear:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        print("History erased.")
        return 0

    everything = read_history(path)
    entries = [e for e in everything if isinstance(e.get("size"), int) and not isinstance(e.get("size"), bool)]
    if args.kind:
        entries = [e for e in entries if e.get("kind") == args.kind]
    traffic = traffic_line(everything) if not args.kind else ""
    if not entries:
        print(f"No {args.kind} files downloaded yet." if args.kind else "Nothing downloaded yet.")
        if traffic:
            print(traffic)
        if history_disabled():
            print("MOX_NO_HISTORY is set, so mox keeps no history.")
        return 0

    counts: dict[str, list[int]] = {}
    for e in entries:
        kind = e.get("kind") if e.get("kind") in KIND_ORDER else "other"
        totals = counts.setdefault(kind, [0, 0])
        totals[0] += 1
        totals[1] += e["size"]
    print(f"{len(entries)} file{'s' if len(entries) != 1 else ''}, "
          f"{human_size(sum(e['size'] for e in entries))}, since {str(entries[0].get('time', ''))[:10]}")
    for kind in sorted(counts, key=lambda k: (-counts[k][0], KIND_ORDER.index(k))):
        number, size = counts[kind]
        print(f"  {kind:<11}{number:>6}  {human_size(size):>9}")
    if traffic:
        print(traffic)

    if args.list:
        print()
        for e in entries[-args.list:]:
            when = str(e.get("time", "")).replace("T", " ")[:16]
            print(f"{when}  {human_size(e['size']):>9}  {e.get('kind', 'other'):<10}  {e.get('path') or e.get('name')}")
    return 0


# ---------------------------------------------------------------- library

class DownloadError(Exception):
    """Raised by download() when a file could not be downloaded."""


def download(url: str, directory: str = ".", *, name: str | None = None,
             connections: int = DEFAULT_CONNECTIONS, limit: int | str | None = None, retries: int = 10,
             timeout: float = 30, headers: dict | None = None, sha256: str | None = None,
             proxy: str | None = None, progress=None) -> str:
    """Download one URL over many connections and return the path of the saved file.

    It resumes a matching unfinished download in `directory`, just like the command does.
    `limit` is bytes per second, or text such as "2M". `progress`, if given, is called about
    four times a second with (bytes done, total bytes or None). `proxy` applies to the whole
    process, since all requests share one opener. Raises DownloadError on failure.
    """
    if not 1 <= connections <= MAX_CONNECTIONS:
        raise ValueError(f"connections must be between 1 and {MAX_CONNECTIONS}")
    if isinstance(limit, str):
        try:
            limit = parse_rate(limit)
        except argparse.ArgumentTypeError as e:
            raise ValueError(str(e)) from None
    configure_proxy(proxy)
    opts = argparse.Namespace(dir=directory, connections=connections, retries=retries,
                              timeout=timeout, headers=dict(headers or {}), limit=limit)
    d = Download(url if "://" in url else "https://" + url, opts, name=name, sha256=sha256)
    stop = threading.Event()
    worker = threading.Thread(target=d.run, args=(stop, RateLimiter(limit)), daemon=True)
    worker.start()
    try:
        while worker.is_alive():
            worker.join(FRAME_INTERVAL)
            if progress:
                progress(d.done, d.total)
    except BaseException:
        stop.set()
        worker.join()
        raise
    finally:
        if d.path:
            with _claim_lock:
                _claimed_paths.discard(os.path.abspath(d.path))
    if d.status != "done":
        raise DownloadError(f"{d.name}: {d.message or d.status}")
    return d.path


# ---------------------------------------------------------------- main

def main(argv=None) -> int:
    try:
        return run_command(argv)
    except KeyboardInterrupt:  # Ctrl+C outside the places that handle it: a short line, not a traceback
        print("\n‖ Cancelled.", file=sys.stderr)
        return 130


def run_command(argv) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv and argv[0] == "resume":
        return resume_main(argv[1:])
    if argv and argv[0] == "bench":
        return bench_main(argv[1:])
    if argv and argv[0] == "history":
        return history_main(argv[1:])
    args = parse_args(argv)
    configure_proxy(args.proxy)
    downloads = [Download(url, args, name=args.output, sha256=args.sha256) for url in args.urls]
    if not args.watch:
        return run_downloads(downloads, args)
    command = clipboard_command()
    if not command:
        print("✖ --watch needs a clipboard tool: wl-clipboard on Wayland, xclip or xsel on X11, "
              "termux-api on Termux. Or set MOX_CLIPBOARD_COMMAND.", file=sys.stderr)
        return 2
    limiter = RateLimiter(args.limit)
    outcome = watch_session(downloads, args, limiter, command)
    if args.history:
        record_traffic(limiter.used)
    return report(downloads, outcome, args)


if __name__ == "__main__":
    sys.exit(main())
