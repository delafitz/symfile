"""Server sync: keep filings and market data current.

init_mds()  — ensure tickers, refs, cusips are fresh
sync()      — catch up on daily indices and fetch
              new filings (144, 13F-HR/A, reg)
"""

import asyncio
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
from symfile.edgar.parse.form13f import (
    parse_13f_holdings,
)
from symfile.edgar.parse.form4 import (
    parse_form4,
)
from symfile.edgar.parse.schedule13d import (
    parse_13d,
)
from symfile.holdings.build import (
    build_all,
    upsert_amendment,
)
from symfile.holdings.form4 import (
    upsert_form4,
)
from symfile.holdings.schedule13d import (
    upsert_13d,
)
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

WATCHED_PREFIXES = (
    '144',
    '13F-HR/A',
    'SCHEDULE 13D',
    'SC 13D',
)
WATCHED_EXACT = {'4', '4/A'}


def _is_watched(form_type: str) -> bool:
    if form_type in WATCHED_EXACT:
        return True
    return any(
        form_type.startswith(p)
        for p in WATCHED_PREFIXES
    )


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


def _prior_quarter(
    d: date,
) -> tuple[int, int]:
    """Quarter whose holdings this amendment
    covers. Amendments filed in Q are for Q-1."""
    q = quarter(d)
    y = d.year
    q -= 1
    if q == 0:
        q = 4
        y -= 1
    return y, q


def _process_13f_amendment(
    filing: Filing,
    raw: bytes,
    cusip_map: dict[str, str],
) -> None:
    """Parse + upsert a 13F-HR/A filing."""
    holdings = parse_13f_holdings(raw)
    if not holdings:
        return

    mapped = [
        (cusip_map[h.cusip], h.shares)
        for h in holdings
        if h.cusip in cusip_map
    ]
    if not mapped:
        return

    filed = date.fromisoformat(filing.date_filed)
    y, q = _prior_quarter(filed)

    upsert_amendment(
        y,
        q,
        filing.company,
        filing.date_filed,
        mapped,
    )


def sync(
    callback=None,
    cusip_map: dict[str, str] | None = None,
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

    log.info(
        'sync',
        today=str(today),
        last_daily=str(last) if last else None,
    )

    all_filings: list[Filing] = []

    prev_qtr = qtr - 1
    prev_year = year
    if prev_qtr == 0:
        prev_qtr = 4
        prev_year -= 1

    if last is None or quarter(last) != qtr:
        for fy, fq in [
            (prev_year, prev_qtr),
            (year, qtr),
        ]:
            log.info(
                'fetching full index',
                year=fy,
                qtr=fq,
            )
            full = fetch_full_index(fy, fq)
            watched = [
                f
                for f in full
                if _is_watched(f.form_type)
            ]
            all_filings.extend(watched)

        if all_filings:
            dates = set(
                f.date_filed for f in all_filings
            )
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

    if days:
        log.info(
            'fetching dailies',
            days=len(days),
            start=str(days[0]),
            end=str(days[-1]),
        )

    for d in days:
        filings = fetch_daily_index(d)
        watched = [
            f
            for f in filings
            if _is_watched(f.form_type)
        ]
        all_filings.extend(watched)
        if watched:
            from collections import Counter

            by_type = Counter(
                f.form_type for f in watched
            )
            log.info(
                'daily',
                date=str(d),
                filings=dict(by_type),
            )

    seen = set()
    unique: list[Filing] = []
    for f in all_filings:
        if f.filename not in seen:
            seen.add(f.filename)
            unique.append(f)
    all_filings = unique

    new = [
        f
        for f in all_filings
        if get_cached(f.filename) is None
    ]

    from collections import Counter

    by_type = Counter(f.form_type for f in new)
    log.info(
        'sync summary',
        total_watched=len(all_filings),
        to_fetch=len(new),
        by_type=dict(by_type),
    )

    if cusip_map is None:
        cusip_map = load_cusips()

    def is_13d(f):
        return f.form_type.startswith(
            'SCHEDULE 13D'
        ) or f.form_type.startswith('SC 13D')

    def is_form4(f):
        return f.form_type in ('4', '4/A')

    syms = load_syms()
    sym_universe = set(syms.keys())
    universe_ciks = set()
    from symfile.mds.massive.refs import (
        build_cik_map,
    )
    for cik in build_cik_map(syms):
        universe_ciks.add(cik)

    amend_all = [
        f for f in all_filings
        if f.form_type == '13F-HR/A'
    ]
    d13_all = [
        f for f in all_filings if is_13d(f)
    ]
    f4_all = [
        f for f in all_filings
        if is_form4(f)
        and f.cik in universe_ciks
    ]
    other_new = [
        f for f in new
        if f.form_type != '13F-HR/A'
        and not is_13d(f)
        and not is_form4(f)
    ]

    if amend_all:
        def on_amend(f, raw):
            _process_13f_amendment(
                f, raw, cusip_map
            )

        log.info(
            'processing 13F amendments',
            count=len(amend_all),
        )
        asyncio.run(
            fetch_filings_async(
                amend_all, on_amend
            )
        )

    if d13_all:
        def on_13d(f, raw):
            d = parse_13d(raw)
            if d:
                upsert_13d(
                    f.date_filed, d, cusip_map
                )

        log.info(
            'processing 13D filings',
            count=len(d13_all),
        )
        asyncio.run(
            fetch_filings_async(
                d13_all, on_13d
            )
        )

    if f4_all:
        def on_f4(f, raw):
            txns = parse_form4(raw)
            if txns:
                upsert_form4(
                    f.date_filed,
                    txns,
                    sym_universe,
                )

        log.info(
            'processing Form 4',
            count=len(f4_all),
        )
        asyncio.run(
            fetch_filings_async(
                f4_all, on_f4
            )
        )

    if other_new and callback:
        asyncio.run(
            fetch_filings_async(
                other_new, callback
            )
        )

    return new
