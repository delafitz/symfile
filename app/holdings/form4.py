"""Form 4 insider transaction table.

Rolling window like 13D — keeps current + prior
quarter. Stores post-transaction positions from
insider filings (purchases, sales, grants).

Schema: symbol, reporter, reporter_cik, txn_date,
        txn_code, shares_txn, post_shares, txn_price,
        filing_date
"""

from pathlib import Path

import polars as pl

from app.mds import DATA_DIR as MDS_DIR
from app.mds.massive.refs import RefRow
from app.util.log import log

HOLDINGS_DIR = Path(MDS_DIR).parent / 'holdings'
TABLE_PATH = HOLDINGS_DIR / 'form4.parquet'

SCHEMA = {
    'symbol': pl.Utf8,
    'reporter': pl.Utf8,
    'reporter_cik': pl.Utf8,
    'txn_date': pl.Utf8,
    'txn_code': pl.Utf8,
    'shares_txn': pl.Int64,
    'post_shares': pl.Int64,
    'txn_price': pl.Float64,
    'filing_date': pl.Utf8,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_form4() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    df = pl.read_parquet(TABLE_PATH)
    # Back-fill any missing columns with nulls for
    # older parquets written before a schema change.
    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(dtype).alias(col)
            )
    return df.select(list(SCHEMA.keys()))


def _save(df: pl.DataFrame) -> None:
    HOLDINGS_DIR.mkdir(
        parents=True, exist_ok=True
    )
    df.write_parquet(TABLE_PATH)


def upsert_form4(
    filing_date: str,
    txns: list,
    cik_map: dict[str, RefRow],
    sym_universe: set[str] | None = None,
) -> None:
    """Upsert parsed Form 4 transactions.

    Resolves symbol from the filing's issuer_cik via
    cik_map (authoritative); falls back to the
    self-reported issuer_ticker when CIK lookup
    misses.

    Keyed on (reporter_cik, symbol, txn_date) —
    latest filing per reporter+symbol+date wins.
    """
    new_rows = []
    for t in txns:
        sym = ''
        cik_key = t.issuer_cik.lstrip('0') or '0'
        ref = cik_map.get(cik_key)
        if ref:
            sym = ref.symbol
        elif (
            sym_universe is not None
            and t.issuer_ticker in sym_universe
        ):
            sym = t.issuer_ticker
        if not sym:
            continue
        new_rows.append({
            'symbol': sym,
            'reporter': t.reporter,
            'reporter_cik': t.reporter_cik,
            'txn_date': t.txn_date,
            'txn_code': t.txn_code,
            'shares_txn': t.shares_txn,
            'post_shares': t.post_shares,
            'txn_price': t.txn_price,
            'filing_date': filing_date,
        })

    if not new_rows:
        return

    existing = load_form4()
    new_df = pl.DataFrame(
        new_rows, schema=SCHEMA
    )

    keys = new_df.select(
        'reporter_cik', 'symbol', 'txn_date'
    ).unique()

    kept = existing.join(
        keys,
        on=['reporter_cik', 'symbol', 'txn_date'],
        how='anti',
    )

    merged = pl.concat([kept, new_df])
    _save(merged)
    log.info(
        'upserted form4',
        reporter=new_rows[0]['reporter'],
        symbol=new_rows[0]['symbol'],
        txns=len(new_rows),
    )


def reprocess_cached(
    cik_map: dict[str, RefRow],
    year: int | None = None,
) -> int:
    """Reparse all Form 4s in the daily indices
    from disk cache and rebuild form4.parquet.

    No network — skips any filing not already
    cached. Useful after a parser/schema change.
    Returns number of rows written.
    """
    import gzip

    from app.edgar.fetch import (
        INDEX_DIR,
        cache_path,
    )
    from app.edgar.parse.form4 import parse_form4

    glob = (
        f'daily.{year}*.idx' if year else 'daily.*.idx'
    )
    idx_files = sorted(INDEX_DIR.glob(glob))
    log.info(
        'reprocess form4',
        indices=len(idx_files),
        year=year,
    )

    seen_files: set[str] = set()
    rows: list[dict] = []
    scanned = cached = 0

    for idx in idx_files:
        for line in idx.read_text().splitlines():
            parts = line.split('|')
            if len(parts) < 5 or parts[2] != '4':
                continue
            date_filed = parts[3]
            filename = parts[4]
            if filename in seen_files:
                continue
            seen_files.add(filename)
            scanned += 1

            p = cache_path(filename)
            if not p.exists():
                continue
            cached += 1

            try:
                raw = gzip.decompress(p.read_bytes())
            except Exception:
                continue
            txns = parse_form4(raw)
            if not txns:
                continue

            iso = (
                f'{date_filed[:4]}-'
                f'{date_filed[4:6]}-'
                f'{date_filed[6:]}'
            )
            for t in txns:
                cik_key = (
                    t.issuer_cik.lstrip('0') or '0'
                )
                ref = cik_map.get(cik_key)
                sym = ref.symbol if ref else ''
                if not sym:
                    continue
                rows.append({
                    'symbol': sym,
                    'reporter': t.reporter,
                    'reporter_cik': t.reporter_cik,
                    'txn_date': t.txn_date,
                    'txn_code': t.txn_code,
                    'shares_txn': t.shares_txn,
                    'post_shares': t.post_shares,
                    'txn_price': t.txn_price,
                    'filing_date': iso,
                })

        if len(seen_files) % 10000 < 10:
            log.info(
                'reprocess progress',
                scanned=scanned,
                cached=cached,
                rows=len(rows),
            )

    log.info(
        'reprocess complete',
        scanned=scanned,
        cached=cached,
        rows=len(rows),
    )

    if not rows:
        return 0

    df = pl.DataFrame(rows, schema=SCHEMA)
    _save(df)
    log.info('form4 saved', rows=df.height)
    return df.height


def truncate(
    keep_after_year: int,
    keep_after_qtr: int,
) -> None:
    """Drop Form 4 filings before a quarter."""
    df = load_form4()
    if df.height == 0:
        return

    from app.holdings.build import (
        _quarter_end,
        _to_iso,
    )

    cutoff = _quarter_end(
        keep_after_year, keep_after_qtr
    )

    before = df.height
    df = df.filter(
        pl.col('filing_date').map_elements(
            _to_iso, return_dtype=pl.Utf8
        )
        > cutoff
    )
    dropped = before - df.height

    if dropped > 0:
        _save(df)
        log.info(
            'truncated form4',
            dropped=dropped,
            remaining=df.height,
            cutoff=cutoff,
        )
