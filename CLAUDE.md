# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

mox is a multi-connection, resumable download manager. All of it lives in `mox.py`, which uses only the Python standard library. Keep it that way: no third-party dependencies and no splitting into a package. `pyproject.toml` declares `requires-python = ">=3.8"`, so avoid syntax newer than 3.8 outside annotations (annotations are deferred by `from __future__ import annotations`). The PyPI package is `moxdl` and the command is `mox`. Bump the version in both `VERSION` in `mox.py` and `pyproject.toml`.

## Commands

```bash
python3 mox.py URL                                   # run from the checkout
python3 -m unittest discover -s tests -v             # all tests
python3 -m unittest tests.test_mox.MoxTest.test_pause_and_resume -v   # one test
python3 mox.py bench URL                             # how much more connections help on a server
python3 docs/demo/make_demo.py                       # re-record docs/demo/demo.svg
```

There is no build step or linter config. GitHub Actions runs the tests on Python 3.8 to 3.13 (`.github/workflows/test.yml`), and pushing a `v*` tag publishes to PyPI (`release.yml`, see `RELEASING.md`). `packaging/` holds the AUR PKGBUILD and the Homebrew formula. `promo/` is gitignored drafts, not part of the project.

## Tests

`tests/test_mox.py` is end-to-end. It starts a local `ThreadingHTTPServer` and SOCKS5 proxies, then runs `mox.py` as a subprocess (`self.mox(...)`) and checks the output file's SHA-256 and that no `.part`/`.mox` files are left over. The server's misbehavior is chosen by URL prefix (`/norange/`, `/flaky/`, `/slowconn/`, `/busy/`, `/named/`, `/gone/`, `/alt/`, `/short/`, `/stall/`, and `/index/` for a folder listing), as described in the module docstring. The SOCKS5 proxies map the name `blocked.test` to 127.0.0.1, so a server only reachable through a proxy can be tested. `setUp` points the history and cache at a temporary folder, turns the sound off and clears the proxy environment variables. To test new network behavior, add a prefix to `Handler` rather than mocking. `load_mox()` imports the module directly for pure-function tests such as the clock math.

## Architecture

**Flow:** `main` → builds one `Download` per URL → `run_session` starts them through a `Scheduler` while `Display` redraws on stderr every `FRAME_INTERVAL`. The download list is the queue: the `Scheduler` runs the first `--jobs` unfinished ones (or only its `focus`), each on a thread with its own stop event, pauses any that fall out of that set and swaps in `Download.requeue()` copies so they continue later. With more than one download on a terminal, `Keyboard` reads keys from `/dev/tty` (not stdin, which may be the links pipe) and `handle_keys` reorders the list. A shared `threading.Event` (`stop`) handles Ctrl+C, `--until` and `--quota`. In schedule mode, `main` loops over sessions and calls `Download.restart()` to make fresh objects that resume from the state file.

**One download (`Download._run`):** `probe()` sends `Range: bytes=0-`, and a 206 reply means ranges are supported and gives the total size. Then it picks a non-clobbering path (`_choose_path`, using the module-global `_claimed_paths` so concurrent jobs don't collide), loads `file.mox` or plans fresh segments, preallocates `file.part`, and starts N `_worker` threads. Each thread writes with `pwrite` (`write_at`) at its own offsets, so file writes need no locking. On success `.part` is renamed to the final name and `.mox` is deleted.

**Segments** are plain dicts (`start`, `end`, `done`, plus runtime keys `busy`/`reserved` and speed-sampling fields), all guarded by `Download.lock`. `work_changed` is a Condition on that lock. `_save_state` writes only the persistent fields, about once per second, via a temp file + rename. The state file stores `url`, `total` and `validator` (a strong ETag, or Last-Modified; see `validator_from`). On resume, a validator that differs from the server's discards the state and starts over. When either side has none, only the size is checked, which is also how a fresh link from an expired one resumes. `_open_range` sends `If-Range` with the validator, so a 200 reply to a range request means the file changed.

**Work stealing (`_next_segment`):** an idle worker first claims any unbusy, unfinished segment. Otherwise it splits the segment with the longest ETA in proportion to the two connections' speeds. Only segments with a *measured* speed may be split. This prevents an old bug where idle workers passed a fresh segment back and forth forever. `_free_from` accounts for the chunk currently being written.

**Failure handling, by exception type:**
- `RETRYABLE_ERRORS` / `RETRYABLE_HTTP`: retry with exponential backoff, resuming at the byte where the connection broke.
- `ServerBusy` (429/503): the worker raises `Retire` and exits, which lowers the connection count, but never below `MIN_ACTIVE`.
- `SlowConnection`: a worker far below the average speed (`SLOW_RATIO`) reconnects, at most `SLOW_RECONNECTS` times per piece.
- `FatalError`: sets `Download.failed`, which stops all workers. 401/403/410 makes `main` print `expired_link_hint`.

**Networking:** `KeepAlive` reuses one `http.client` connection per worker. When `--proxy` is set, urllib openers are installed globally by `configure_proxy`. `--fallback-proxy` (`configure_fallback`) goes direct, ignoring the proxy environment variables, and moves a host to its SOCKS5 proxy (`_proxied_hosts`) after its first failed connection; `request` and `KeepAlive._connect` look the route up on every connection. SOCKS5 has its own handshake (`socks5_connect`, which always resolves names remotely) and keeps keep-alive. HTTP proxies go through urllib with keep-alive off (`keep_alive_possible`).

**Commands besides plain downloads:** `main` dispatches `mox resume [DIR]` (`resume_main`, rebuilds `Download`s from the `url` in each `.mox` file) and `mox bench URL` (`bench_main`/`bench_round`, keeps nothing; a round's rate counts only until the first connection *finishes its share*, so refused connections cannot stop the clock). `--watch` runs `watch_session`, which polls the clipboard via `clipboard_command` and adds `Download`s to the live list. `mox config` (`config_main`) writes `key = value` lines that `config_defaults` turns into argparse defaults, so the command line still wins. `mox doctor` (`doctor_main`) checks the machine. A plain `mox` on a terminal asks for links (`ask_for_links`). `--notify` calls `notify`, which uses desktop tools plus Telegram when `MOX_TELEGRAM_TOKEN`/`MOX_TELEGRAM_CHAT` are set. `download()` is the library entry point and raises `DownloadError`.

**Display:** one row per download. Column widths are computed once per frame (`Display._layout`) so rows line up, and columns drop out as the terminal narrows. The progress bar is the piece map, drawn from `coverage_map`. Each redraw moves up by the rows the previous frame takes *after* the terminal rewraps it, so resizing leaves no stale copies.

**Tuning constants** sit at the top of `mox.py` with comments explaining each one. Change them there, not inline.

## Style

The code and README use plain, direct prose. Comments explain *why*, often naming the bug a rule prevents. Keep the README's feature list, options table and exit codes (0 ok, 1 failed, 2 usage error, 3 stopped by `--until`, 130 Ctrl+C) in sync with `parse_args` and `main`. `README.fa.md` is a Persian README written for Iranian users, not a translation; update it when user-facing behavior changes. Benchmark numbers in the READMEs must come from real runs.
