# mox

[![tests](https://github.com/moxart/moxdl/actions/workflows/test.yml/badge.svg)](https://github.com/moxart/moxdl/actions/workflows/test.yml)
![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue)
![no dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)
[![license: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

[فارسی](README.fa.md)

A download manager for the terminal. It splits each file across many connections, resumes after you stop it, and survives flaky networks. It is one Python file and needs nothing outside the standard library.

![mox downloading three files at once, each line followed by a map of which parts of the file are done](https://raw.githubusercontent.com/moxart/moxdl/main/docs/demo/demo.svg)

The line under each file is a map of the file. Bright cells are where a connection is writing right now. The gaps that stay dark longer are slow connections, and you can watch the fast ones take their work away. The recording comes from [`docs/demo/make_demo.py`](docs/demo/make_demo.py), which runs mox against a local server that caps each connection at 512 KB/s.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/moxart/moxdl/main/mox.py -o ~/.local/bin/mox && chmod +x ~/.local/bin/mox
```

That is the whole install: one file, Python 3.8 or newer, nothing else. Or as a package, with [pipx](https://pipx.pypa.io):

```bash
pipx install git+https://github.com/moxart/moxdl.git
```

On Android, in [Termux](https://termux.dev): `pkg install python`, then the `curl` line above with `$PREFIX/bin/mox` in place of `~/.local/bin/mox`.

The package is called `moxdl` because `mox` on PyPI belongs to an old mocking library. The command is `mox` everywhere.

## Why

Some servers cap the speed of each connection. Open 16 connections and you get up to 16 times the speed, as far as your line allows. That is the trick behind IDM and aria2. I wanted it in one file I could drop on any machine with Python, with resume that works without thinking about it.

A 100 MB file from a local server that caps each connection at 4 MB/s:

| Command | Time | Speed |
| --- | ---: | ---: |
| `wget` | 24.5 s | 3.9 MB/s |
| `curl` | 24.4 s | 3.9 MB/s |
| `mox -c 4` | 6.3 s | 15.0 MB/s |
| `mox` (16 connections) | 1.8 s | 51.7 MB/s |
| `mox -c 32` | 1.1 s | 88.2 MB/s |

Real servers vary, and some gain nothing. Hetzner's speed test server takes two connections from one address and answers the rest with 429 Too Many Requests, so there mox is no faster than wget. To see what a given server allows, ask mox:

```
$ mox bench http://127.0.0.1:8782/100MB.bin
100MB.bin · 95.4 MB · the server supports range requests
  1 connection      3.7 MB/s
  4 connections    15.7 MB/s   4.2×
 16 connections    62.4 MB/s  16.9×
This server limits each connection. With -c 16, mox is about 17× faster here than a one-connection download such as wget or curl.
```

That run is against the same local server. Against Hetzner, `mox bench` reports that only about 2 connections are accepted and suggests `-c 2`. It downloads for a few seconds per round and keeps nothing.

## Features

- Up to 64 connections per file, 16 by default.
- A live map of each file shows which parts are done and where each connection is working.
- Idle connections take over work from slow ones in proportion to their speed, so one bad route does not hold up the whole file.
- A connection that runs far slower than its siblings gets dropped and reopened, since a new connection often takes a better route.
- Connections are kept alive between pieces, which skips repeated TCP and TLS handshakes.
- Ctrl+C pauses. Run the same command again, or `mox resume` in that folder, and it picks up where it stopped, even after a reboot.
- Retries with exponential backoff. A dropped connection resumes from the byte where it broke.
- If the server answers 429 or 503 because you opened too many connections, mox backs off to fewer instead of failing.
- `--watch` downloads every file link you copy, like a browser download manager without the browser extension.
- A schedule. Start at 2 a.m., stop at 7, and carry on the next night, for internet plans that are cheap or free at night.
- A desktop notification, or a Telegram message, when the downloads are done.
- SOCKS5 and HTTP proxies, with user name and password. Keep-alive still works through SOCKS5.
- When a signed link expires halfway, give mox a fresh one and it continues from the same byte.
- Notices when the file on the server changes. A new release with the same size is not glued onto half of the old one.
- A total speed limit, SHA-256 verification, several files at once, URLs from a list file.
- Takes the file name from `Content-Disposition`, so non-Latin names come out right. It never overwrites an existing file. A second `file.zip` becomes `file (1).zip`.
- Works as a Python library too: `mox.download(url)`.

## Usage

```bash
mox https://example.com/file.iso
```

Several files, saved to a folder, two at a time:

```bash
mox -d ~/Downloads -j 2 https://example.com/a.zip https://example.com/b.zip
```

A list of URLs, one per line. Blank lines and lines starting with `#` are skipped:

```bash
mox -i links.txt
```

Cap the total speed so the rest of the house can still use the internet:

```bash
mox -l 2M https://example.com/big.mkv
```

A file behind a login. Copy the cookie from your browser:

```bash
mox -H "Cookie: session=abc123" -H "Referer: https://example.com/" https://example.com/private.zip
```

Check the file against a published checksum:

```bash
mox --sha256 9f86d081884c7d65... https://example.com/release.tar.gz
```

### Picking up where you left off

Every unfinished download leaves a `FILE.mox` next to its `FILE.part`, and the link is saved in it. So you do not need to remember the command:

```bash
cd ~/Downloads && mox resume
```

`mox resume DIR` continues everything unfinished in `DIR`. It takes the same options as a normal download, such as `-c`, `-l` or `--until`.

### Copy a link, get the file

```bash
mox --watch -d ~/Downloads
```

mox keeps running and downloads every file link that lands on your clipboard. Links to web pages, ending in `.html`, `.php` or nothing at all, are skipped, so copying an article's address does nothing. Whatever was on the clipboard before you started is ignored too. Ctrl+C stops it.

It reads the clipboard with `wl-paste` on Wayland, `xclip` or `xsel` on X11, `pbpaste` on macOS, `termux-clipboard-get` in Termux, and PowerShell on Windows. For anything else, set `MOX_CLIPBOARD_COMMAND` to a command that prints the clipboard.

### Night downloads

Start at 2:00, stop at 7:00, and keep going night after night until everything is done:

```bash
mox --at 02:00 --until 07:00 -i links.txt
```

mox waits with a countdown, downloads inside the window, pauses at the end of it, and waits for the next night. Leave the terminal open and the computer awake. If you start it inside the window, it begins right away. Windows that cross midnight, such as `--at 23:00 --until 06:00`, work too.

With only `--until`, mox stops at that time and exits with code 3. Run the same command later to continue. With only `--at`, it waits and then runs to the end.

### Notifications

```bash
mox --notify --at 02:00 --until 07:00 -i links.txt
```

When the downloads end, `--notify` sends a desktop notification through `notify-send` on Linux, `osascript` on macOS or `termux-notification` on Android. In `--watch` mode it sends one per file.

To get a Telegram message as well, make a bot with [@BotFather](https://t.me/BotFather), send it any message, and set two variables:

```bash
export MOX_TELEGRAM_TOKEN=123456:ABC...   # from BotFather
export MOX_TELEGRAM_CHAT=987654321        # your chat id, e.g. from @userinfobot
```

The message goes through `--proxy` if you gave one. `MOX_TELEGRAM_API` points mox at a self-hosted Bot API server instead of `api.telegram.org`.

### Proxies

```bash
mox --proxy socks5://127.0.0.1:1080 https://example.com/file.iso
mox --proxy socks5h://user:pass@proxy.example:1080 https://example.com/file.iso
mox --proxy http://127.0.0.1:8080 https://example.com/file.iso
```

Through SOCKS5, mox sends the host name to the proxy and lets the proxy resolve it, for both `socks5://` and `socks5h://`. Local DNS never sees the name. Without `--proxy`, the usual `http_proxy` and `https_proxy` environment variables apply.

### Expired links

Many file hosts hand out links that stop working after a few hours. When that happens mid-download, mox fails with a 403 and prints the command to continue. Get a fresh link from the site and pass the same file name:

```bash
mox -d ~/Downloads -o big-file.iso 'https://host.example/new-signed-link'
```

mox finds `big-file.iso.mox`, checks that the size on the server is the same, and carries on from where the old link stopped.

### From Python

`pip install moxdl`, or drop `mox.py` next to your script:

```python
import mox

path = mox.download("https://example.com/big.iso", "downloads",
                    progress=lambda done, total: print(f"{done}/{total}"))
```

`download()` takes the same settings as the command, as keyword arguments: `name`, `connections`, `limit` (bytes per second, or text such as `"2M"`), `retries`, `timeout`, `headers`, `sha256` and `proxy`. It resumes unfinished downloads, returns the path of the saved file, and raises `mox.DownloadError` when the download fails.

### Options

| Option | Default | Meaning |
| --- | --- | --- |
| `-d, --dir DIR` | current directory | Where to save files |
| `-o, --output NAME` | from the server | File name, single URL only |
| `-i, --input FILE` | | Read URLs from a file, `-` for stdin |
| `-c, --connections N` | 16 | Connections per file, 1 to 64 |
| `-j, --jobs N` | 3 | Files downloaded at the same time |
| `-l, --limit RATE` | none | Total speed limit, such as `500K` or `2M` |
| `-r, --retries N` | 10 | Retries per connection before giving up |
| `-t, --timeout SEC` | 30 | Network timeout |
| `-H, --header 'K: V'` | | Extra request header, repeatable |
| `--sha256 HEX` | | Verify the finished file, single URL only |
| `-x, --proxy URL` | environment | `socks5://`, `socks5h://` or `http://` proxy |
| `--at HH:MM` | | Wait until this time of day before starting |
| `--until HH:MM` | | Pause at this time of day |
| `-w, --watch` | | Download every file link copied to the clipboard |
| `-n, --notify` | | Desktop or Telegram notification when done |

Other commands: `mox resume [DIR]` takes the download options above. `mox bench URL` takes `-s SECONDS` per round, `-c` for the most connections to try, and `-t`, `-H` and `-x`.

Exit codes are 0 when everything finished, 1 when any download failed, 2 for a usage error, 3 when `--until` stopped unfinished downloads, and 130 when you pressed Ctrl+C.

## How it works

First mox asks the server for `bytes=0-`. A `206 Partial Content` reply means the server accepts range requests and tells the total size. mox then creates `file.part` at full size and splits it into one piece per connection. Each connection fetches its piece with a `Range` header and writes straight into its spot in the file with `pwrite`, so no locking is needed.

Every connection measures its own speed. When one finishes, it looks for the piece with the longest time left and takes part of it. The split matches the two speeds so both sides finish together: a connection 27 times slower keeps about 4% of what is left. Pieces only get split after their speed has been measured. Without that rule, two idle connections can hand the same fresh piece back and forth forever, and an early version of mox did exactly that.

Once a second, progress goes into `file.mox`, a small JSON file with the link and each piece's start, end and bytes done. It is written to a temporary file and renamed, so a crash cannot corrupt it.

The state file also keeps the file's `ETag`, or its `Last-Modified` date if the server sends no strong ETag. On resume, mox compares it with what the server sends now and starts over if the file has changed. Every piece is requested with `If-Range`, so if the file changes mid-download the server sends the whole new file instead of a piece. mox then stops with "the file changed on the server", and the next run starts over. A server that sends neither header gets only the size check. When the download completes, `file.part` becomes `file` and the state file is deleted. If you see a `.part` file, the download is not finished.

## Limitations

- HTTP and HTTPS only. No FTP, BitTorrent or Metalink.
- If a server does not support range requests, mox falls back to one connection, and an interrupted download starts over.
- Some servers allow only a few connections per address. mox backs off when they refuse, but the first seconds go to refused connections. `mox bench` tells you the number, and `-c` with it skips the refusals.
- Behind an HTTP proxy, keep-alive is turned off, so starting each piece costs an extra handshake. SOCKS5 does not have this problem. The tests cover SOCKS5 but not HTTP proxies.
- The schedule only runs while mox is running. If the computer sleeps through the window, nothing downloads.
- An existing file with the same name and the same size counts as already downloaded. Delete it if you want a fresh copy.
- I have not tried it in Termux or on Windows yet. Reports are welcome.

## Tests

The tests start a local HTTP server that can send slow responses, drop connections and refuse links, plus a small SOCKS5 proxy, then run mox against them:

```bash
python3 -m unittest discover -s tests -v
```

GitHub Actions runs them on Python 3.8 to 3.13 and on macOS. Releases are described in [RELEASING.md](RELEASING.md).

## License

MIT. See [LICENSE](LICENSE).
