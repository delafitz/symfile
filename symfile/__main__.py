"""symfile — SEC filing tools.

Usage:
    uv run python -m symfile init
    uv run python -m symfile sync
    uv run python -m symfile holders SYMBOL
    uv run python -m symfile tickers
    uv run python -m symfile refs
    uv run python -m symfile cusips
    uv run python -m symfile build
    uv run python -m symfile scan [--full] [--date YYYYMMDD]

Commands:
    init      Initialize all market data + holdings
    sync      Catch up on daily filings (144, 13F-HR/A)
    holders   Top holders report for a symbol
    tickers   Load/refresh the symbol-CIK mapping
    refs      Build/refresh reference data cache
    cusips    Build CUSIP->symbol map from 13F filings
    build     Build quarterly holdings parquet files
    scan      Scan EDGAR 144 index for block trades
"""

import asyncio
import sys
import time
from datetime import date, timedelta

from symfile.edgar.index import (
    fetch_daily_index,
    fetch_filings_async,
    fetch_full_index,
    filter_forms,
)
from symfile.edgar.parse.form144 import parse_144
from symfile.mds.syms import (
    RefRow,
    build_cik_map,
    load_cusips,
    load_syms,
    load_tickers,
)

MIN_TRADE_VALUE = 25_000_000


def _prev_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def cmd_tickers() -> None:
    tickers = load_tickers()
    print(f'{len(tickers)} symbols loaded')


def cmd_refs() -> None:
    syms = load_syms(max_age_days=0)
    print(f'{len(syms)} refs loaded')


def cmd_scan(args: list[str]) -> None:
    target_date = _prev_weekday(
        date.today() - timedelta(days=1)
    )
    if '--date' in args:
        idx = args.index('--date')
        ds = args[idx + 1]
        target_date = date(
            int(ds[:4]), int(ds[4:6]), int(ds[6:])
        )
    use_full = '--full' in args

    # Load syms (mkt_cap >= $1B, has price)
    syms = load_syms()
    cik_map = build_cik_map(syms)
    print(f'\nuniverse: {len(cik_map)} CIKs')

    # Fetch EDGAR index
    if use_full:
        qtr = (target_date.month - 1) // 3 + 1
        print(
            f'fetching full-index '
            f'{target_date.year}/QTR{qtr}...'
        )
        filings = fetch_full_index(
            target_date.year, qtr
        )
    else:
        print(
            f'fetching daily-index '
            f'{target_date.isoformat()}...'
        )
        filings = fetch_daily_index(target_date)

    if not filings:
        print('no filings returned')
        return

    # Filter to 144s in our universe
    tx = filter_forms(filings)
    matched = [
        f for f in tx if f.cik in cik_map
    ]
    print(
        f'144 filings: {len(tx)} total, '
        f'{len(matched)} in universe'
    )

    # Async fetch + parse, compute implied value
    blocks: list[
        tuple[
            RefRow, str, int, float, str, str,
        ]
    ] = []

    def on_filing(f, raw):
        d = parse_144(raw)
        if not d or d.shares <= 0:
            return
        ref = cik_map[f.cik]
        implied = d.shares * ref.price
        if implied < MIN_TRADE_VALUE:
            return
        blocks.append((
            ref,
            f.date_filed,
            d.shares,
            implied,
            d.seller,
            d.relationship,
        ))

    t0 = time.time()
    print(f'fetching {len(matched)} filings...')
    asyncio.run(
        fetch_filings_async(matched, on_filing)
    )
    elapsed = time.time() - t0
    print(f'  done in {elapsed:.0f}s')

    # Dedupe: collapse same (sym, seller, shares)
    # on same or consecutive days into one entry
    # using the earliest date
    from collections import defaultdict

    groups: dict[
        tuple[str, str, int],
        list[tuple[str, float, str]],
    ] = defaultdict(list)
    for ref, dt, shares, impl, seller, rel in blocks:
        key = (ref.symbol, seller, shares)
        groups[key].append((dt, impl, rel))

    deduped: list[
        tuple[
            RefRow, str, int, float, str, str,
        ]
    ] = []
    duped = 0
    for (sym, seller, shares), entries in (
        groups.items()
    ):
        # Sort by date, keep earliest
        entries.sort()
        dt0, impl0, rel0 = entries[0]
        deduped.append((
            syms[sym],
            dt0,
            shares,
            impl0,
            seller,
            rel0,
        ))
        duped += len(entries) - 1

    deduped.sort(key=lambda x: -x[3])
    if duped:
        print(f'  deduped {duped} filings')
    print(
        f'\n{len(deduped)} blocks >= '
        f'${MIN_TRADE_VALUE / 1e6:.0f}M'
    )
    print(
        f'\n{"SYM":<6s} {"DATE":<12s} '
        f'{"SHARES":>12s} {"IMPLIED":>14s} '
        f'{"MKT_CAP":>10s}  SELLER [REL]'
    )
    print('-' * 105)
    for (
        ref, dt, shares, impl, seller, rel,
    ) in deduped:
        cap_b = ref.mkt_cap / 1e9
        print(
            f'{ref.symbol:<6s} {dt:<12s} '
            f'{shares:>12,} '
            f'${impl:>13,.0f} '
            f'{cap_b:>9.1f}B  '
            f'{seller[:35]} [{rel}]'
        )


def cmd_cusips() -> None:
    from symfile.edgar.bulk13f import (
        extract_cusips,
        fetch_bulk_zip,
    )

    syms = load_syms()
    universe = set(syms.keys())

    all_cusips: set[str] = set()
    for year, qtr in [(2025, 3), (2025, 4)]:
        zp = fetch_bulk_zip(year, qtr)
        cusips = extract_cusips(zp)
        all_cusips.update(cusips)
        print(
            f'  {year}/Q{qtr}: '
            f'{len(cusips)} cusips'
        )
    print(f'{len(all_cusips)} unique cusips')

    mapping = load_cusips(
        cusips=all_cusips,
        universe=universe,
        max_age_days=0,
    )
    print(
        f'\n{len(mapping)} cusips mapped '
        f'to {len(set(mapping.values()))} '
        f'symbols in universe'
    )


def cmd_build() -> None:
    from symfile.holdings.build import build_all

    cusip_map = load_cusips()
    build_all(cusip_map)


def cmd_holders(args: list[str]) -> None:
    from symfile.holdings.report import (
        top_holders,
    )

    if not args:
        print('usage: holders SYMBOL')
        return
    symbol = args[0].upper()
    top_holders(symbol)


def cmd_init() -> None:
    from symfile.sync import init_mds

    init_mds()


def cmd_sync() -> None:
    from symfile.sync import sync

    new = sync()
    print(f'{len(new)} new filings')


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    cmd = args[0]
    if cmd == 'init':
        cmd_init()
    elif cmd == 'sync':
        cmd_sync()
    elif cmd == 'tickers':
        cmd_tickers()
    elif cmd == 'refs':
        cmd_refs()
    elif cmd == 'cusips':
        cmd_cusips()
    elif cmd == 'build':
        cmd_build()
    elif cmd == 'holders':
        cmd_holders(args[1:])
    elif cmd == 'scan':
        cmd_scan(args[1:])
    else:
        print(f'unknown command: {cmd}')
        print(__doc__)


if __name__ == '__main__':
    main()
