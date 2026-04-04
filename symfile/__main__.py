"""symfile — SEC filing tools.

Usage:
    uv run python -m symfile init
    uv run python -m symfile sync
    uv run python -m symfile holders SYMBOL
    uv run python -m symfile tickers
    uv run python -m symfile refs
    uv run python -m symfile cusips
    uv run python -m symfile build
    uv run python -m symfile scan [--date YYYYMMDD] [--symbol SYM] [--144|--reg]

Commands:
    init      Initialize all market data + holdings
    sync      Catch up on daily filings (144, 13F-HR/A)
    holders   Top holders report for a symbol
    tickers   Load/refresh the symbol-CIK mapping
    refs      Build/refresh reference data cache
    cusips    Build CUSIP->symbol map from 13F filings
    build     Build quarterly holdings parquet files
    scan      Scan for block trades (144 + reg)
"""

import sys
from datetime import date

from symfile.mds.syms import (
    load_cusips,
    load_syms,
    load_tickers,
)


def cmd_tickers() -> None:
    tickers = load_tickers()
    print(f'{len(tickers)} symbols loaded')


def cmd_refs() -> None:
    syms = load_syms(max_age_days=0)
    print(f'{len(syms)} refs loaded')


def cmd_scan(args: list[str]) -> None:
    from symfile.trades.hist import get_trades

    syms = load_syms()

    start = end = None
    symbol = None
    types = 'both'

    if '--date' in args:
        idx = args.index('--date')
        ds = args[idx + 1]
        d = date(
            int(ds[:4]), int(ds[4:6]), int(ds[6:])
        )
        start = end = d
    if '--symbol' in args:
        idx = args.index('--symbol')
        symbol = args[idx + 1].upper()
    if '--144' in args:
        types = '144'
    elif '--reg' in args:
        types = 'reg'

    trades = get_trades(
        syms,
        start=start,
        end=end,
        symbol=symbol,
        types=types,
    )
    trades.sort(key=lambda t: -t.implied_value)

    print(f'\n{len(trades)} trades')
    print(
        f'\n{"SYM":<6s} {"DATE":<12s} '
        f'{"TYPE":<8s} {"SHARES":>12s} '
        f'{"IMPLIED":>14s} '
        f'{"MKT_CAP":>10s}  SELLER'
    )
    print('-' * 90)
    for t in trades:
        cap_b = t.mkt_cap / 1e9
        print(
            f'{t.symbol:<6s} {t.date_filed:<12s} '
            f'{t.filing_type:<8s} '
            f'{t.shares:>12,} '
            f'${t.implied_value:>13,.0f} '
            f'{cap_b:>9.1f}B  '
            f'{t.seller[:30]}'
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
