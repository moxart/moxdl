#!/usr/bin/env python3
"""Record a real mox run and turn it into the animated SVG at the top of the README.

    python3 docs/demo/make_demo.py            # writes docs/demo/demo.svg

It starts a local server that caps every connection at CONNECTION_RATE, the way many real
download servers do, and makes about one connection in five much slower, to show the work
stealing. Then it runs mox in a pseudo-terminal, replays what mox printed through a tiny
terminal emulator, and writes one SVG frame per FRAME seconds.

Linux and macOS only (it needs the pty module).
"""

import fcntl
import html
import os
import pty
import random
import re
import select
import shutil
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
MOX = os.path.join(HERE, "..", "..", "mox.py")
OUTPUT = os.path.join(HERE, "demo.svg")

COLUMNS, ROWS = 96, 9
CONNECTION_RATE = 512 * 1024   # bytes/s per connection
SLOW_SHARE, SLOW_FACTOR = 0.2, 0.08
FILES = {"big-file.iso": 40, "dataset.tar.zst": 20, "backup.zip": 12}  # MB
COMMAND = "mox -c 16 -i links.txt"
FRAME = 0.2       # seconds per frame
TYPING = 1.2      # seconds spent typing the command
HOLD = 3.0        # seconds the last frame stays up before the loop restarts

CELL_W, CELL_H, PAD, BAR = 8.4, 19, 16, 28
PALETTE = {"fg": "#d7dae0", "bg": "#1e2127", "30": "#5c6370", "31": "#e06c75", "32": "#98c379",
           "33": "#e5c07b", "34": "#61afef", "35": "#c678dd", "36": "#56b6c2", "37": "#d7dae0",
           "96": "#8be9f5", "97": "#ffffff", "dim": "#7f848e", "prompt": "#98c379"}
BLOCKS = {"█": 1.0, "▓": 0.72, "▒": 0.45, "░": 0.13}
LINE = "━"  # mox's progress bar, drawn as a thin rule so it looks like it does in a terminal


# ---------------------------------------------------------------- the throttled server

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    rng = random.Random(7)  # same slow connections on every run, so the demo is reproducible

    def log_message(self, *args):
        pass

    def do_GET(self):
        path = os.path.join(self.server.root, self.path.rsplit("/", 1)[-1])
        if not os.path.isfile(path):
            self.send_error(404)
            return
        size = os.path.getsize(path)
        start, end = 0, size - 1
        match = re.match(r"bytes=(\d+)-(\d*)", self.headers.get("Range", ""))
        rate = CONNECTION_RATE * (SLOW_FACTOR if Handler.rng.random() < SLOW_SHARE else 1)
        if match:
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else size - 1
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(end - start + 1))
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            left = end - start + 1
            while left > 0:
                chunk = f.read(min(32768, left))
                try:
                    self.wfile.write(chunk)
                except OSError:
                    self.close_connection = True
                    return
                left -= len(chunk)
                time.sleep(len(chunk) / rate)


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass


# ---------------------------------------------------------------- recording

def record(workdir: str, port: int) -> list[tuple[float, bytes]]:
    with open(os.path.join(workdir, "links.txt"), "w") as f:
        f.writelines(f"http://127.0.0.1:{port}/{name}\n" for name in FILES)
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS - 1, COLUMNS, 0, 0))
    env = {**os.environ, "COLUMNS": str(COLUMNS), "LINES": str(ROWS - 1), "TERM": "xterm-256color"}
    cmd = [sys.executable, MOX] + COMMAND.split()[1:]
    proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, cwd=workdir, env=env)
    os.close(slave)
    started, chunks = time.monotonic(), []
    while True:
        ready, _, _ = select.select([master], [], [], 0.1)
        if ready:
            try:
                data = os.read(master, 65536)
            except OSError:  # the child closed the terminal
                break
            if not data:
                break
            chunks.append((time.monotonic() - started, data))
        elif proc.poll() is not None:
            break
    proc.wait()
    os.close(master)
    return chunks


# ---------------------------------------------------------------- a tiny terminal

class Screen:
    """Just enough of a VT100 for what mox prints: text, CR/LF, erase line and below, cursor up, colors."""

    def __init__(self, rows: int, cols: int):
        self.rows, self.cols = rows, cols
        self.grid = [[(" ", "")] * cols for _ in range(rows)]
        self.row = self.col = 0
        self.style = ""

    def feed(self, text: str) -> None:
        for token in re.findall(r"\x1b\[[0-9;?]*[A-Za-z]|[\s\S]", text):
            if token.startswith("\x1b["):
                self._escape(token[2:-1], token[-1])
            elif token == "\r":
                self.col = 0
            elif token == "\n":
                self._line_feed()
            elif token >= " ":
                if self.col >= self.cols:
                    continue
                self.grid[self.row][self.col] = (token, self.style)
                self.col += 1

    def _line_feed(self) -> None:
        if self.row == self.rows - 1:
            self.grid.pop(0)
            self.grid.append([(" ", "")] * self.cols)
        else:
            self.row += 1

    def _escape(self, params: str, command: str) -> None:
        count = int(params) if params.isdigit() else 1
        if command == "m":
            codes = [c for c in params.split(";") if c] or ["0"]
            self.style = "" if codes == ["0"] else ";".join(codes)
        elif command == "K":
            self.grid[self.row] = [(" ", "")] * self.cols
        elif command == "J":
            for r in range(self.row, self.rows):
                self.grid[r] = [(" ", "")] * self.cols
        elif command == "F":
            self.row, self.col = max(0, self.row - count), 0
        elif command == "A":
            self.row = max(0, self.row - count)

    def snapshot(self) -> list[list[tuple[str, str]]]:
        return [list(line) for line in self.grid]


def frames_from(chunks: list[tuple[float, bytes]]) -> list[list[list[tuple[str, str]]]]:
    screen = Screen(ROWS, COLUMNS)
    frames = []
    prompt = "$ "
    for i in range(int(TYPING / FRAME)):
        typed = COMMAND[: round(len(COMMAND) * i / (TYPING / FRAME - 2))] if i else ""
        screen.grid[0] = [(c, "prompt" if j < 2 else "") for j, c in enumerate((prompt + typed).ljust(COLUMNS))]
        frames.append(screen.snapshot())
    screen.row, screen.col = 1, 0
    end = chunks[-1][0] if chunks else 0
    t, index = 0.0, 0
    while t <= end + FRAME:
        while index < len(chunks) and chunks[index][0] <= t:
            screen.feed(chunks[index][1].decode("utf-8", "replace"))
            index += 1
        frames.append(screen.snapshot())
        t += FRAME
    frames.extend([frames[-1]] * int(HOLD / FRAME))
    return frames


# ---------------------------------------------------------------- SVG

def color_of(style: str) -> tuple[str, bool, bool]:
    codes = style.split(";") if style else []
    color = next((PALETTE[c] for c in reversed(codes) if c in PALETTE), PALETTE["fg"])
    if style == "prompt":
        color = PALETTE["prompt"]
    dim = "2" in codes
    return (PALETTE["dim"] if dim and color == PALETTE["fg"] else color), "1" in codes, dim


def render_frame(grid) -> str:
    out = []
    for r, line in enumerate(grid):
        y = PAD + BAR + r * CELL_H
        c = 0
        while c < len(line):
            char, style = line[c]
            if char == " ":
                c += 1
                continue
            run_end = c
            if char == LINE:
                while run_end < len(line) and line[run_end] == (char, style):
                    run_end += 1
                color, _, dim = color_of(style)
                out.append(f'<rect x="{PAD + c * CELL_W:.1f}" y="{y + CELL_H / 2 - 1:.1f}" '
                           f'width="{(run_end - c) * CELL_W:.1f}" height="4" fill="{color}" '
                           f'fill-opacity="{0.35 if dim else 1}"/>')
            elif char in BLOCKS:
                while run_end < len(line) and line[run_end] == (char, style):
                    run_end += 1
                color, _, dim = color_of(style)
                opacity = BLOCKS[char] * (0.6 if dim else 1)
                out.append(f'<rect x="{PAD + c * CELL_W:.1f}" y="{y + 3}" width="{(run_end - c) * CELL_W:.1f}" '
                           f'height="{CELL_H - 6}" fill="{color}" fill-opacity="{opacity:.2f}"/>')
            else:
                while (run_end < len(line) and line[run_end][1] == style and line[run_end][0] not in BLOCKS and line[run_end][0] != LINE
                       and not (line[run_end][0] == " " and run_end + 1 < len(line)
                                and line[run_end + 1][0] == " ")):
                    run_end += 1
                text = "".join(ch for ch, _ in line[c:run_end]).rstrip()
                color, bold, _ = color_of(style)
                weight = ' font-weight="bold"' if bold else ""
                out.append(f'<text x="{PAD + c * CELL_W:.1f}" y="{y + CELL_H - 5}" fill="{color}"{weight}>'
                           f"{html.escape(text)}</text>")
            c = run_end
    return "".join(out)


def svg(frames) -> str:
    width = PAD * 2 + COLUMNS * CELL_W
    height = PAD * 2 + BAR + ROWS * CELL_H
    total = len(frames) * FRAME
    share = 100 / len(frames)
    # Image viewers built on librsvg ignore CSS animations, so every frame stays hidden there. The
    # busiest frame (the most files downloading, then the most of their bars filled) sits underneath
    # as a still, and each animated frame paints over it while shown.
    cover = f'<rect y="{PAD + BAR}" width="100%" height="{ROWS * CELL_H}" fill="{PALETTE["bg"]}"/>'
    busiest = max(frames, key=lambda grid: (sum(line[0][0] == "↓" for line in grid),
                                            sum(char == LINE and "2" not in style.split(";")
                                                for line in grid for char, style in line)))
    still = f"<g>{render_frame(busiest)}</g>"
    groups = still + "\n" + "\n".join(
        f'<g class="f" style="animation-delay:{i * FRAME:.2f}s">{cover}{render_frame(f)}</g>'
        for i, f in enumerate(frames))
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">
<title>mox downloading three files over 16 connections each</title>
<style>
text {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "DejaVu Sans Mono", monospace; font-size: 14px; white-space: pre; }}
.f {{ visibility: hidden; animation: show {total:.2f}s linear infinite; }}
@keyframes show {{ 0% {{ visibility: visible; }} {share:.4f}% {{ visibility: hidden; }} 100% {{ visibility: hidden; }} }}
</style>
<rect width="100%" height="100%" rx="10" fill="{PALETTE["bg"]}"/>
<circle cx="{PAD + 6}" cy="{PAD + 4}" r="6" fill="#e06c75"/>
<circle cx="{PAD + 26}" cy="{PAD + 4}" r="6" fill="#e5c07b"/>
<circle cx="{PAD + 46}" cy="{PAD + 4}" r="6" fill="#98c379"/>
{groups}
</svg>
'''


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="mox-demo-")
    server = Server(("127.0.0.1", 0), Handler)
    server.root = tempfile.mkdtemp(prefix="mox-demo-files-")
    for name, megabytes in FILES.items():
        with open(os.path.join(server.root, name), "wb") as f:
            f.write(os.urandom(megabytes * 1024 * 1024))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        chunks = record(workdir, server.server_address[1])
    finally:
        server.shutdown()
        shutil.rmtree(workdir, ignore_errors=True)
        shutil.rmtree(server.root, ignore_errors=True)
    frames = frames_from(chunks)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(svg(frames))
    print(f"wrote {OUTPUT}: {len(frames)} frames, {len(frames) * FRAME:.1f}s, "
          f"{os.path.getsize(OUTPUT) // 1024} KB")


if __name__ == "__main__":
    main()
