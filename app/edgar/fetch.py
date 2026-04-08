"""Async EDGAR fetcher with rate limiting and disk
cache.

Generic fetch infrastructure: rate-limited HTTP,
429 retry with global backoff, MD5-keyed disk cache,
and a concurrent bulk fetcher with token-bucket
throttle.
"""

import asyncio
import gzip
import hashlib
import time
import urllib.request
from pathlib import Path
from typing import Callable, TypeVar

from app.util.log import log

DATA_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / 'data'
)
CACHE_DIR = DATA_DIR / 'filings'
INDEX_DIR = DATA_DIR / 'indices'

USER_AGENT = 'symfile dev@symfile.dev'
SEC_RPS = 8
MAX_RETRIES = 3
FILING_BASE = 'https://www.sec.gov/Archives/'

_last_fetch = 0.0

# Global backoff: when any thread gets a 429,
# all threads pause until this time
_backoff_until = 0.0


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
        log.warning('fetch error', url=url, err=str(e))
        return None


BACKOFF_SECS = 600  # 10 minutes on 429


def fetch_url_retry(url: str) -> bytes | None:
    """Fetch URL with 429 handling.

    On 429: assume 10-minute IP ban. Set global
    backoff so all threads pause, sleep, then
    retry once.
    """
    global _backoff_until
    req = urllib.request.Request(
        url,
        headers={'User-Agent': USER_AGENT},
    )
    for attempt in range(2):  # initial + 1 retry
        # Respect global backoff from any thread
        wait = _backoff_until - time.time()
        if wait > 0:
            time.sleep(wait)
        try:
            return urllib.request.urlopen(
                req, timeout=30
            ).read()
        except urllib.request.HTTPError as e:
            if e.code == 429 and attempt == 0:
                _backoff_until = (
                    time.time() + BACKOFF_SECS
                )
                log.warning(
                    '429 backoff',
                    seconds=BACKOFF_SECS,
                )
                time.sleep(BACKOFF_SECS)
                continue
            if e.code == 429:
                return None
            log.warning('fetch error', url=url, err=str(e))
            return None
        except Exception as e:
            log.warning('fetch error', url=url, err=str(e))
            return None


# --- Disk cache ---


def cache_key(name: str) -> str:
    """Stable short key from string."""
    return hashlib.md5(
        name.encode()
    ).hexdigest()[:12]


def cache_path(name: str) -> Path:
    return CACHE_DIR / f'{cache_key(name)}.gz'


def _legacy_path(name: str) -> Path:
    return CACHE_DIR / f'{cache_key(name)}.txt'


def get_cached(name: str) -> bytes | None:
    p = cache_path(name)
    if p.exists():
        return gzip.decompress(p.read_bytes())
    # Fall back to uncompressed legacy file
    lp = _legacy_path(name)
    if lp.exists():
        return lp.read_bytes()
    return None


def put_cache(
    name: str, data: bytes
) -> None:
    CACHE_DIR.mkdir(
        parents=True, exist_ok=True
    )
    cache_path(name).write_bytes(
        gzip.compress(data)
    )


# --- Index cache (separate dir) ---


def index_path(name: str) -> Path:
    return INDEX_DIR / f'{name}.idx'


def get_index(name: str) -> bytes | None:
    p = index_path(name)
    if p.exists():
        return p.read_bytes()
    return None


def put_index(
    name: str, data: bytes
) -> None:
    INDEX_DIR.mkdir(
        parents=True, exist_ok=True
    )
    index_path(name).write_bytes(data)


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
    SEC_RPS req/s. Global backoff on 429.
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
        log.info('cache hit', count=cached_n)
    if not to_fetch:
        return

    log.info('fetching', count=len(to_fetch))
    bucket = asyncio.Queue(maxsize=SEC_RPS)
    done = 0
    failed = 0
    total = len(to_fetch)

    async def refill() -> None:
        interval = 1.0 / SEC_RPS
        while True:
            # Pause refill during global backoff
            wait = _backoff_until - time.time()
            if wait > 0:
                await asyncio.sleep(wait)
            await bucket.put(True)
            await asyncio.sleep(interval)

    async def fetch_one(item: T) -> None:
        nonlocal done, failed
        await bucket.get()
        raw = await asyncio.to_thread(
            fetch_url_retry,
            url_fn(item),
        )
        done += 1
        if done % 500 == 0:
            log.info(
                'progress',
                done=done,
                total=total,
                failed=failed,
            )
        if raw:
            put_cache(key_fn(item), raw)
            callback(item, raw)
        else:
            failed += 1

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
    if failed:
        log.warning(
            'fetch incomplete',
            failed=failed,
            total=total,
        )
