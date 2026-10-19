# mox

A download manager for the terminal. It splits each file across many connections, resumes after you stop it, and survives flaky networks. It is one Python file and needs nothing outside the standard library.

```
↓ ubuntu-24.04-desktop.iso        ████████████░░░░░░░░  64.1%    3.8 GB/5.9 GB    48.2 MB/s    00:44 16×
↓ dataset.tar.zst                 ███████████████░░░░░  79.5%    1.2 GB/1.5 GB    11.2 MB/s    00:27 16×
· backup.zip                      ░░░░░░░░░░░░░░░░░░░░                            waiting
  0/3 done · 59.4 MB/s total · Ctrl+C to pause
```

## Why

Plenty of servers cap the speed of a single connection. Open 16 of them and you get 16 times the speed, up to whatever your line can carry. That is the whole trick behind IDM and aria2. I wanted it in a single file I could drop on any machine with Python, with resume that works without thinking about it.

## Features

- Up to 64 connections per file, 16 by default.
- Idle connections take over work from slow ones in proportion to their speed, so one bad route does not hold up the whole file.
- A connection that runs far slower than its siblings gets dropped and reopened, since a new connection often takes a better route.
- Connections are kept alive between pieces, which skips repeated TCP and TLS handshakes.
- Ctrl+C pauses. Run the same command again and it picks up where it stopped, even after a reboot.
- Retries with exponential backoff. A dropped connection resumes from the byte where it broke.
- If the server answers 429 or 503 because you opened too many connections, mox backs off to fewer instead of failing.
- Downloads several files at once from the command line or from a list file.
- A total speed limit shared across every connection and file.
- SHA-256 verification.
- A schedule. Start at 2 a.m., stop at 7, and carry on the next night, for internet plans that are cheap or free at night.
- SOCKS5 and HTTP proxies, with user name and password. Keep-alive still works through SOCKS5.
- When a signed link expires halfway, give mox a fresh one and it continues from the same byte.
- Takes the file name from `Content-Disposition`, so non-Latin names come out right. It never overwrites an existing file. A second `file.zip` becomes `file (1).zip`.

## Install

You need Python 3.8 or newer. I have tested it on Python 3.14 on Linux.

Clone the repository and link the script into a directory on your `PATH`:

```bash
git clone https://github.com/moxart/moxdl.git ~/moxdl
chmod +x ~/moxdl/mox.py
ln -s ~/moxdl/mox.py ~/.local/bin/mox
```

Or install it as a package with [pipx](https://pipx.pypa.io):

```bash
pipx install git+https://github.com/moxart/moxdl.git
```

The repository and the package are called `moxdl`, because `mox` on PyPI belongs to an old mocking library. The command is still `mox`.

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

### Night downloads

Start at 2:00, stop at 7:00, and keep going night after night until everything is done:

```bash
mox --at 02:00 --until 07:00 -i links.txt
```

mox waits with a countdown, downloads inside the window, pauses at the end of it, and waits for the next night. Leave the terminal open and the computer awake. If you start it inside the window, it begins right away. Windows that cross midnight, such as `--at 23:00 --until 06:00`, work too.

With only `--until`, mox stops at that time and exits with code 3. Run the same command later to continue. With only `--at`, it waits and then runs to the end.

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

Exit codes are 0 when everything finished, 1 when any download failed, 3 when `--until` stopped unfinished downloads, and 130 when you pressed Ctrl+C.

## How it works

First mox asks the server for `bytes=0-`. A `206 Partial Content` reply means the server accepts range requests and tells the total size. mox then creates `file.part` at full size and splits it into one piece per connection. Each connection fetches its piece with a `Range` header and writes straight into its spot in the file with `pwrite`, so no locking is needed.

Every connection measures its own speed. When one finishes, it looks for the piece with the longest time left and takes part of it. The split matches the two speeds so both sides finish together: a connection 27 times slower keeps about 4% of what is left. Pieces only get split after their speed has been measured. Without that rule, two idle connections can hand the same fresh piece back and forth forever, and an early version of mox did exactly that.

Once a second, progress goes into `file.mox`, a small JSON file with each piece's start, end and bytes done. It is written to a temporary file and renamed, so a crash cannot corrupt it. When the download completes, `file.part` becomes `file` and the state file is deleted. If you see a `.part` file, the download is not finished.

## Limitations

- HTTP and HTTPS only. No FTP, BitTorrent or Metalink.
- If a server does not support range requests, mox falls back to one connection, and an interrupted download starts over.
- Behind an HTTP proxy, keep-alive is turned off, so starting each piece costs an extra handshake. SOCKS5 does not have this problem. The tests cover SOCKS5 but not HTTP proxies.
- The schedule only runs while mox is running. If the computer sleeps through the window, nothing downloads.
- An existing file with the same name and the same size counts as already downloaded. Delete it if you want a fresh copy.
- Some servers dislike many connections from one address. If you get blocked, try `-c 4`.

## Tests

The tests start a local HTTP server that can send slow responses, drop connections and refuse links, plus a small SOCKS5 proxy, then run mox against them:

```bash
python3 -m unittest discover -s tests -v
```

## License

MIT. See [LICENSE](LICENSE).
