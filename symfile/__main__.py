"""symfile — SEC filing tools."""

from datetime import date
from typing import Annotated, Optional

import typer

app = typer.Typer(
    help='SEC filing scanner for block trades'
    ' and institutional holdings.',
    add_completion=False,
)


@app.command()
def init() -> None:
    """Initialize all market data + holdings."""
    from symfile.sync import init_mds

    init_mds()


@app.command()
def sync() -> None:
    """Catch up on daily filings (144, 13F-HR/A)."""
    from symfile.sync import sync as do_sync

    new = do_sync()
    print(f'{len(new)} new filings')


@app.command()
def tickers() -> None:
    """Load/refresh the symbol-CIK mapping."""
    from symfile.mds.syms import load_tickers

    t = load_tickers()
    print(f'{len(t)} symbols loaded')


@app.command()
def refs() -> None:
    """Build/refresh reference data cache."""
    from symfile.mds.syms import load_syms

    syms = load_syms(max_age_days=0)
    print(f'{len(syms)} refs loaded')


@app.command()
def cusips() -> None:
    """Build CUSIP->symbol map from 13F bulk zips."""
    from symfile.edgar.bulk13f import (
        extract_cusips,
        fetch_bulk_zip,
    )
    from symfile.mds.syms import (
        load_cusips,
        load_syms,
    )

    syms = load_syms()
    universe = set(syms.keys())

    all_cusips: set[str] = set()
    for year, qtr in [(2025, 3), (2025, 4)]:
        zp = fetch_bulk_zip(year, qtr)
        c = extract_cusips(zp)
        all_cusips.update(c)
        print(f'  {year}/Q{qtr}: {len(c)} cusips')
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


@app.command()
def build() -> None:
    """Build quarterly holdings parquet files."""
    from symfile.holdings.build import build_all
    from symfile.mds.syms import load_cusips

    build_all(load_cusips())


@app.command()
def holders(
    symbol: Annotated[
        str, typer.Argument(help='Ticker symbol')
    ],
    n: Annotated[
        int,
        typer.Option('--top', help='Number of holders'),
    ] = 20,
) -> None:
    """Top holders report for a symbol."""
    from symfile.holdings.report import top_holders

    top_holders(symbol.upper(), n=n)


@app.command()
def scan(
    date_str: Annotated[
        Optional[str],
        typer.Option(
            '--date', help='YYYYMMDD'
        ),
    ] = None,
    symbol: Annotated[
        Optional[str],
        typer.Option(
            '--symbol', help='Filter to symbol'
        ),
    ] = None,
    only_144: Annotated[
        bool,
        typer.Option(
            '--144', help='Only Form 144'
        ),
    ] = False,
    only_reg: Annotated[
        bool,
        typer.Option(
            '--reg', help='Only registered'
        ),
    ] = False,
) -> None:
    """Scan for block trades (144 + reg)."""
    from symfile.mds.syms import load_syms
    from symfile.trades.hist import get_trades

    syms = load_syms()

    start = end = None
    if date_str:
        d = date(
            int(date_str[:4]),
            int(date_str[4:6]),
            int(date_str[6:]),
        )
        start = end = d

    types = 'both'
    if only_144:
        types = '144'
    elif only_reg:
        types = 'reg'

    trades = get_trades(
        syms,
        start=start,
        end=end,
        symbol=symbol.upper() if symbol else None,
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
            f'{t.symbol:<6s} '
            f'{t.date_filed:<12s} '
            f'{t.filing_type:<8s} '
            f'{t.shares:>12,} '
            f'${t.implied_value:>13,.0f} '
            f'{cap_b:>9.1f}B  '
            f'{t.seller[:30]}'
        )


@app.command()
def backfill_13d() -> None:
    """Backfill 13D table from full indices."""
    import asyncio

    import polars as pl

    from symfile.edgar.index import (
        fetch_filings_async,
        fetch_full_index,
    )
    from symfile.edgar.parse.schedule13d import (
        parse_13d,
    )
    from symfile.holdings.schedule13d import (
        HOLDINGS_DIR,
        SCHEMA,
        TABLE_PATH,
    )
    from symfile.mds.syms import load_cusips
    from symfile.util.log import log

    cusip_map = load_cusips()
    rows: list[dict] = []

    quarters = [
        (2024, 1), (2024, 2),
        (2024, 3), (2024, 4),
        (2025, 1), (2025, 2),
        (2025, 3), (2025, 4),
        (2026, 1), (2026, 2),
    ]

    for year, qtr in quarters:
        idx = fetch_full_index(year, qtr)
        d13 = [
            f for f in idx
            if f.form_type.startswith(
                'SCHEDULE 13D'
            )
            or f.form_type.startswith('SC 13D')
        ]
        log.info(
            'backfill quarter',
            year=year,
            qtr=qtr,
            filings=len(d13),
        )
        before = len(rows)

        def on_filing(f, raw):
            d = parse_13d(raw)
            if not d:
                return
            sym = cusip_map.get(d.issuer_cusip)
            if not sym:
                return
            rows.append({
                'symbol': sym,
                'holder': d.holder,
                'holder_cik': d.holder_cik,
                'event_date': d.event_date,
                'filing_date': f.date_filed,
                'shares': d.shares,
                'pct_class': d.pct_class,
            })

        asyncio.run(
            fetch_filings_async(d13, on_filing)
        )
        log.info(
            'quarter done',
            new_rows=len(rows) - before,
            total=len(rows),
        )

    df = pl.DataFrame(rows, schema=SCHEMA)

    deduped = (
        df.sort('filing_date', descending=True)
        .group_by(['holder_cik', 'symbol'])
        .first()
    )

    HOLDINGS_DIR.mkdir(parents=True, exist_ok=True)
    deduped.write_parquet(TABLE_PATH)
    log.info(
        'backfill complete',
        rows=deduped.height,
        symbols=deduped.n_unique('symbol'),
        holders=deduped.n_unique('holder'),
    )


@app.command()
def serve(
    port: Annotated[
        int, typer.Option(help='Port')
    ] = 8000,
) -> None:
    """Run the API server."""
    print(f'server on :{port} (not yet wired)')


if __name__ == '__main__':
    app()
