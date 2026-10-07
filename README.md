a simple tool to check availability of roblox usernames. supports various generation modes, proxy rotation, output formats, and resuming interrupted sessions.

## Features

- Multithreaded: concurrent checking with configurable thread count.
- Flexible generation: random or systematic (brute‑force) usernames with custom alphabet, prefix, suffix, and starting point.
- Proxy support: list proxies via command line or file, with rotation.
- User‑agent rotation: avoid basic fingerprinting.
- Output formats: plain text, JSON, or CSV (with timestamps).
- Resume capability: skip previously checked usernames.
- Rate‑limit handling: exponential backoff and retries.
- Progress bar: shows found count (need `tqdm`).
- Logging: console and optional file logging.

## Installation
### Note
- Use responsibly: don't do too many requests, it can get you IP blocked.

clone this repository or download the main.py

```bash
pip install requests urllib3
# optional for progress bar
pip install tqdm
