"""Server sync: keep filings and market data current.

init_mds()  — ensure tickers, refs, cusips are fresh
sync()      — catch up on daily indices and fetch
              new filings (144, 13F-HR/A, reg)
"""

import re
from datetime import date, timedelta

from symfile.edgar.fetch import (
    INDEX_DIR,
    get_cached,
)
from symfile.edgar.index import (
    Filing,
    fetch_daily_index,
    fetch_filings_async,
    fetch_full_index,
)
from symfile.holdings.build import build_all
from symfile.mds.syms import (
    load_cusips,
    load_syms,
    load_tickers,
)
from symfile.util.dates import (
    prev_weekday,
    quarter,
    weekdays,
)
from symfile.util.log import log

WATCHED_FORMS = ('144', '13F-HR/A')


def init_mds() -> dict:
    """Ensure all market data caches are fresh."""
    log.info('init mds')
    tickers = load_tickers()
    syms = load_syms()
    cusip_map = load_cusips()

    log.info('mds loaded', tickers=len(tickers), syms=len(syms), cusips=len(cusip_map))

    log.info('init holdings')
    build_all(cusip_map)

    return {
        'tickers': tickers,
        'syms': syms,
        'cusip_map': cusip_map,
    }


def _last_daily() -> date | None:
    """Find the most recent daily index on disk."""
    if not INDEX_DIR.exists():
        return None
    pat = re.compile(r'daily\.(\d{8})\.idx$')
    best: date | None = None
    for f in INDEX_DIR.iterdir():
        m = pat.match(f.name)
        if m:
            s = m.group(1)
            d = date(
                int(s[:4]), int(s[4:6]), int(s[6:])
            )
            if best is None or d > best:
                best = d
    return best


def sync(
    callback=None,
) -> list[Filing]:
    """Catch up on filings since last sync.

    1. Find last daily index date
    2. If none for current quarter, fetch full
    3. Walk dailies forward to today
    4. Re-fetch today's daily (may have grown)
    5. Filter to watched forms
    6. Async fetch unfetched filings
    7. Call callback(filing, raw) for each

    Returns list of new filings found.
    """
    today = prev_weekday(date.today())
    last = _last_daily()
    qtr = quarter(today)
    year = today.year

    log.info('sync', today=str(today))

    all_filings: list[Filing] = []

    if last is None or quarter(last) != qtr:
        log.info('fetching full index', year=year, qtr=qtr)
        full = fetch_full_index(year, qtr)
        watched = [
            f
            for f in full
            if any(
                f.form_type.startswith(p)
                for p in WATCHED_FORMS
            )
        ]
        all_filings.extend(watched)
        if full:
            dates = set(f.date_filed for f in full)
            earliest = min(dates)
            d = date(
                int(earliest[:4]),
                int(earliest[5:7]),
                int(earliest[8:]),
            )
            last = d

    start = (
        last + timedelta(days=1)
        if last
        else today
    )
    days = weekdays(start, today)

    if last == today:
        days = [today]
        log.info('re-fetching daily', date=str(today))

    if days:
        log.info('fetching dailies', count=len(days), start=str(days[0]), end=str(days[-1]))

    for d in days:
        filings = fetch_daily_index(d)
        watched = [
            f
            for f in filings
            if any(
                f.form_type.startswith(p)
                for p in WATCHED_FORMS
            )
        ]
        all_filings.extend(watched)

    new = [
        f
        for f in all_filings
        if get_cached(f.filename) is None
    ]

    log.info('sync complete', watched=len(all_filings), unfetched=len(new))

    if new and callback:
        import asyncio

        asyncio.run(
            fetch_filings_async(new, callback)
        )

    return new
