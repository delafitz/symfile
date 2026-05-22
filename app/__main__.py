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
    from app.sync import init_mds

    init_mds()


@app.command()
def sync() -> None:
    """Catch up on daily filings (144, 13F-HR/A)."""
    from app.sync import sync as do_sync

    new = do_sync()
    print(f'{len(new)} new filings')


@app.command()
def tickers() -> None:
    """Load/refresh the symbol-CIK mapping."""
    from app.mds.syms import load_tickers

    t = load_tickers()
    print(f'{len(t)} symbols loaded')


@app.command()
def refs() -> None:
    """Build/refresh reference data cache."""
    from app.mds.syms import load_syms

    syms = load_syms(max_age_days=0)
    print(f'{len(syms)} refs loaded')


@app.command()
def adv() -> None:
    """Build/refresh 30-day ADV cache."""
    from app.mds.massive.adv import load_adv

    result = load_adv(build=True)
    print(f'{len(result)} symbols')


@app.command()
def cusips() -> None:
    """Build CUSIP->symbol map from 13F bulk zips."""
    from app.edgar.bulk13f import (
        extract_cusips,
        fetch_bulk_zip,
    )
    from app.mds.syms import (
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
    from app.holdings.build import build_all
    from app.mds.syms import load_cusips

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
    from app.holdings.report import top_holders

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
    """Scan for block trades over a date range.

    Runs the detect pass directly without going through
    full sync intake. Useful for backfilling candidates
    after a parser improvement.

    Writes to trades.parquet with protect_curated=True
    so golden-seeded rows aren't disturbed.
    """
    from app.detect.reg import detect_reg_blocks
    from app.detect.unreg import detect_unreg_blocks
    from app.mds.massive.refs import build_cik_map
    from app.mds.syms import load_syms
    from app.trades.table import upsert_trades

    syms = load_syms()
    cik_map = build_cik_map(syms)
    all_ciks = set(cik_map.keys())
    if symbol:
        sym = symbol.upper()
        ref = syms.get(sym)
        if not ref:
            print(f'{sym} not in universe')
            return
        cik = ref.cik.lstrip('0') or '0'
        target_ciks = {cik}
    else:
        target_ciks = all_ciks

    start = end = None
    if date_str:
        d = date(
            int(date_str[:4]),
            int(date_str[4:6]),
            int(date_str[6:]),
        )
        start = end = d
    if start is None or end is None:
        print('--date required (YYYYMMDD)')
        return

    candidates: list[dict] = []
    if not only_144:
        candidates.extend(detect_reg_blocks(
            touched_ciks=target_ciks,
            cik_map=cik_map,
            lo=start, hi=end,
        ))
    if not only_reg:
        candidates.extend(detect_unreg_blocks(
            touched_ciks=target_ciks,
            cik_map=cik_map,
            lo=start, hi=end,
        ))

    added = upsert_trades(candidates, protect_curated=True)
    print(f'\n{len(candidates)} candidates, {added} new')
    candidates.sort(key=lambda r: -r['adj_shares'] * r['adj_price'])
    print(
        f'\n{"SYM":<6s} {"PxDt":<12s} {"TYPE":<6s} '
        f'{"SHARES":>12s} {"PRICE":>9s} '
        f'{"NOTIONAL":>14s}  SELLER'
    )
    print('-' * 100)
    for r in candidates[:50]:
        n = r['adj_shares'] * r['adj_price']
        print(
            f'{r["symbol"]:<6s} '
            f'{str(r["price_date"]):<12s} '
            f'{r["type"]:<6s} '
            f'{r["adj_shares"]:>12,} '
            f'${r["adj_price"]:>8.2f} '
            f'${n:>13,.0f}  '
            f'{(r["seller"] or "")[:30]}'
        )


@app.command()
def backfill_13d() -> None:
    """Backfill 13D table from full indices."""
    import asyncio

    import polars as pl

    from app.edgar.index import (
        fetch_filings_async,
        fetch_full_index,
    )
    from app.edgar.parse.schedule13d import (
        parse_13d,
    )
    from app.holdings.schedule13d import (
        HOLDINGS_DIR,
        SCHEMA,
        TABLE_PATH,
    )
    from app.mds.syms import load_cusips
    from app.util.log import log

    cusip_map = load_cusips()
    rows: list[dict] = []

    quarters = [
        (2025, 4),
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
def reprocess_form4(
    year: Annotated[
        int,
        typer.Option(
            '--year',
            help='Restrict to one year (default: 2026 YTD)',
        ),
    ] = 2026,
) -> None:
    """Reparse cached Form 4s and rebuild form4.parquet."""
    from app.holdings.form4 import reprocess_cached
    from app.mds.massive.refs import build_cik_map
    from app.mds.syms import load_syms

    syms = load_syms()
    cik_map = build_cik_map(syms)
    n = reprocess_cached(cik_map, year=year)
    print(f'{n} rows')


@app.command()
def flag_form4() -> None:
    """Deprecated: legacy Form 4 -> trades emitter.

    Schema cutover in progress — use seed-goldens for
    now; sync-time flagging will be rewritten next.
    """
    print(
        'flag_form4 disabled during schema cutover. '
        'See tools/seed_goldens.py'
    )


@app.command()
def review() -> None:
    """Deprecated: interactive review CLI.

    Used the old per-filing key. Will be rewritten
    against the new (price_date, symbol, offer_price)
    schema after the seed lands.
    """
    print(
        'review disabled during schema cutover. '
        'Blocks are seeded from confirmed goldens.'
    )


@app.command()
def backup() -> None:
    """Create a dated tar.gz backup of downloaded data."""
    from app.backup import create_backup

    path = create_backup()
    print(path)


@app.command()
def serve(
    port: Annotated[
        int, typer.Option(help='Port')
    ] = 8000,
) -> None:
    """Run the API server."""
    import uvicorn

    uvicorn.run(
        'app.main:app',
        host='0.0.0.0',
        port=port,
        reload=True,
    )


if __name__ == '__main__':
    app()
