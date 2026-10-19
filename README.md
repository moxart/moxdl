# mox

[![tests](https://github.com/moxart/moxdl/actions/workflows/test.yml/badge.svg)](https://github.com/moxart/moxdl/actions/workflows/test.yml)
![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue)
![no dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)
[![license: MIT](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

[فارسی](README.fa.md)

A download manager for the terminal. It splits each file across many connections, resumes after you stop it, and survives flaky networks. It is one Python file and needs nothing outside the standard library.

![mox downloading three files at once, each with a progress bar that maps which parts of the file are done](https://raw.githubusercontent.com/moxart/moxdl/main/docs/demo/demo.svg)

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/moxart/moxdl/main/mox.py -o ~/.local/bin/mox && chmod +x ~/.local/bin/mox
```

It needs Python 3.8 or newer. Or install with [pipx](https://pipx.pypa.io):

```bash
pipx install moxdl
```

On Android, in [Termux](https://termux.dev): `pkg install python`, then the `curl` line above with `$PREFIX/bin/mox` in place of `~/.local/bin/mox`.

The package is called `moxdl`; the command is `mox`.

## Speed

Many servers cap the speed of each connection, so more connections means more speed. A 100 MB file from a local server that caps each connection at 4 MB/s:

| Command | Time | Speed |
| --- | ---: | ---: |
| `wget` | 24.5 s | 3.9 MB/s |
| `curl` | 24.4 s | 3.9 MB/s |
| `mox -c 4` | 6.3 s | 15.0 MB/s |
| `mox` (16 connections) | 1.8 s | 51.7 MB/s |
| `mox -c 32` | 1.1 s | 88.2 MB/s |

Some servers gain nothing or refuse extra connections. `mox bench URL` measures what a server allows and suggests a `-c` value. It downloads nothing to disk.

## Features

- Up to 64 connections per file, 16 by default.
- The progress bar shows which parts of each file are done.
- Fast connections take over work from slow ones.
- Very slow connections are reopened.
- Ctrl+C pauses. Run the same command again, or `mox resume`, to continue, even after a reboot.
- Retries dropped connections, and uses fewer connections when the server refuses more.
- `--watch` downloads every file link you copy.
- Scheduled downloads, such as 2:00 to 7:00 every night.
- A desktop notification, or a Telegram message, when the downloads are done.
- SOCKS5 and HTTP proxies, with user name and password.
- An expired link can be replaced with a fresh one without starting over.
- Starts over if the file on the server has changed.
- A total speed limit, SHA-256 verification, several files at once, URLs from a list file.
- Never overwrites an existing file: a second `file.zip` becomes `file (1).zip`.
- Works as a Python library: `mox.download(url)`.

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

Limit the total speed:

```bash
mox -l 2M https://example.com/big.mkv
```

A file behind a login, with a cookie from your browser:

```bash
mox -H "Cookie: session=abc123" -H "Referer: https://example.com/" https://example.com/private.zip
```

Check the file against a published checksum:

```bash
mox --sha256 9f86d081884c7d65... https://example.com/release.tar.gz
```

### Picking up where you left off

An unfinished download leaves `FILE.part` and `FILE.mox` behind. To continue everything unfinished in a folder:

```bash
mox resume ~/Downloads
```

It takes the same options as a normal download, such as `-c`, `-l` or `--until`.

### Copy a link, get the file

```bash
mox --watch -d ~/Downloads
```

mox downloads every file link you copy, and skips links to web pages. Ctrl+C stops it.

On Linux it needs `wl-paste`, `xclip` or `xsel`. If your clipboard tool isn't supported, set `MOX_CLIPBOARD_COMMAND` to a command that prints the clipboard.

### Night downloads

Download between 2:00 and 7:00 every night until everything is done:

```bash
mox --at 02:00 --until 07:00 -i links.txt
```

Leave the terminal open and the computer awake. With only `--until`, mox stops at that time; run the same command later to continue. With only `--at`, it waits and then runs to the end.

### Notifications

```bash
mox --notify --at 02:00 --until 07:00 -i links.txt
```

`--notify` sends a desktop notification when the downloads end. For a Telegram message too, make a bot with [@BotFather](https://t.me/BotFather), send it any message, and set:

```bash
export MOX_TELEGRAM_TOKEN=123456:ABC...   # from BotFather
export MOX_TELEGRAM_CHAT=987654321        # your chat id, e.g. from @userinfobot
```

`MOX_TELEGRAM_API` sets a self-hosted Bot API server instead of `api.telegram.org`.

### Proxies

```bash
mox --proxy socks5://127.0.0.1:1080 https://example.com/file.iso
mox --proxy socks5h://user:pass@proxy.example:1080 https://example.com/file.iso
mox --proxy http://127.0.0.1:8080 https://example.com/file.iso
```

With SOCKS5, the proxy resolves host names. Without `--proxy`, the `http_proxy` and `https_proxy` environment variables apply.

### Expired links

If a link expires mid-download, mox prints the command to continue. Get a fresh link and pass the same file name:

```bash
mox -d ~/Downloads -o big-file.iso 'https://host.example/new-signed-link'
```

### From Python

`pip install moxdl`, or drop `mox.py` next to your script:

```python
import mox

path = mox.download("https://example.com/big.iso", "downloads",
                    progress=lambda done, total: print(f"{done}/{total}"))
```

Keyword arguments: `name`, `connections`, `limit` (bytes per second, or text such as `"2M"`), `retries`, `timeout`, `headers`, `sha256` and `proxy`. It returns the saved file's path and raises `mox.DownloadError` on failure.

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

## Limitations

- HTTP and HTTPS only. No FTP, BitTorrent or Metalink.
- If a server does not support range requests, mox falls back to one connection, and an interrupted download starts over.
- Servers that allow only a few connections waste the first seconds on refusals. Use `mox bench` to find the right `-c`.
- HTTP proxies are slower than SOCKS5.
- The schedule only works while mox is running and the computer is awake.
- An existing file with the same name and size counts as already downloaded.
- Not yet tested in Termux or on Windows.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

## License

MIT. See [LICENSE](LICENSE).
