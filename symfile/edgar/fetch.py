"""Async EDGAR fetcher with rate limiting and disk
cache.

Generic fetch infrastructure: rate-limited HTTP,
429 retry, MD5-keyed disk cache, and a concurrent
bulk fetcher with token-bucket throttle.
"""

import asyncio
import hashlib
import time
import urllib.request
from pathlib import Path
from typing import Callable, TypeVar

DATA_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / 'data'
)
CACHE_DIR = DATA_DIR / 'filings'

USER_AGENT = 'symfile dev@symfile.dev'
SEC_RPS = 6
MAX_RETRIES = 3
FILING_BASE = 'https://www.sec.gov/Archives/'

_last_fetch = 0.0


# --- HTTP helpers ---


def fetch_url(url: str) -> bytes | None:
    """Fetch URL with SEC rate limiting."""
    global _last_fetch
    elapsed = time.time() - _last_fetch
    if elapsed < 0.12:
        time.sleep(0.12 - elapsed)
    req = urllib.request.Request(
        url,
        headers={'User-Agent': USER_AGENT},
    )
    try:
        _last_fetch = time.time()
        return urllib.request.urlopen(
            req, timeout=30
        ).read()
    except Exception as e:
        print(f'  fetch error: {url}: {e}')
        return None


def fetch_url_retry(url: str) -> bytes | None:
    """Fetch URL with 429 retry. No rate limiting
    (caller handles via token bucket)."""
    req = urllib.request.Request(
        url,
        headers={'User-Agent': USER_AGENT},
    )
    for attempt in range(MAX_RETRIES):
        try:
            return urllib.request.urlopen(
                req, timeout=30
            ).read()
        except urllib.request.HTTPError as e:
            if (
                e.code == 429
                and attempt < MAX_RETRIES - 1
            ):
                time.sleep(2**attempt)
                continue
            print(f'  fetch error: {url}: {e}')
            return None
        except Exception as e:
            print(f'  fetch error: {url}: {e}')
            return None


# --- Disk cache ---


def cache_key(name: str) -> str:
    """Stable short key from string."""
    return hashlib.md5(
        name.encode()
    ).hexdigest()[:12]


def cache_path(name: str) -> Path:
    return CACHE_DIR / f'{cache_key(name)}.txt'


def get_cached(name: str) -> bytes | None:
    p = cache_path(name)
    if p.exists():
        return p.read_bytes()
    return None


def put_cache(
    name: str, data: bytes
) -> None:
    CACHE_DIR.mkdir(
        parents=True, exist_ok=True
    )
    cache_path(name).write_bytes(data)


# --- Async bulk fetch ---

T = TypeVar('T')


async def fetch_many_async(
    items: list[T],
    key_fn: Callable[[T], str],
    url_fn: Callable[[T], str],
    callback: Callable[[T, bytes], None],
) -> None:
    """Fetch + process items concurrently.

    Checks disk cache first; fetches uncached.
    Token-bucket rate limiter stays under
    SEC_RPS req/s.
    """
    to_fetch: list[T] = []
    for item in items:
        cached = get_cached(key_fn(item))
        if cached:
            callback(item, cached)
        else:
            to_fetch.append(item)

    cached_n = len(items) - len(to_fetch)
    if cached_n:
        print(f'  {cached_n} from cache')
    if not to_fetch:
        return

    print(f'  {len(to_fetch)} to fetch...')
    bucket = asyncio.Queue(maxsize=SEC_RPS)
    done = 0
    total = len(to_fetch)

    async def refill() -> None:
        interval = 1.0 / SEC_RPS
        while True:
            await bucket.put(True)
            await asyncio.sleep(interval)

    async def fetch_one(item: T) -> None:
        nonlocal done
        await bucket.get()
        raw = await asyncio.to_thread(
            fetch_url_retry,
            url_fn(item),
        )
        done += 1
        if done % 500 == 0:
            print(f'  ... {done}/{total}')
        if raw:
            put_cache(key_fn(item), raw)
            callback(item, raw)

    filler = asyncio.create_task(refill())
    try:
        await asyncio.gather(
            *(
                fetch_one(item)
                for item in to_fetch
            )
        )
    finally:
        filler.cancel()
