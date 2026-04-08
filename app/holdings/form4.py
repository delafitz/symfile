"""Form 4 insider transaction table.

Rolling window like 13D — keeps current + prior
quarter. Stores post-transaction positions from
insider filings (purchases, sales, grants).

Schema: symbol, reporter, reporter_cik, txn_date,
        txn_code, shares_txn, post_shares,
        filing_date
"""

from pathlib import Path

import polars as pl

from app.mds import DATA_DIR as MDS_DIR
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
    'filing_date': pl.Utf8,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_form4() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    return pl.read_parquet(TABLE_PATH).select(
        list(SCHEMA.keys())
    )


def _save(df: pl.DataFrame) -> None:
    HOLDINGS_DIR.mkdir(
        parents=True, exist_ok=True
    )
    df.write_parquet(TABLE_PATH)


def upsert_form4(
    filing_date: str,
    txns: list,
    sym_universe: set[str],
) -> None:
    """Upsert parsed Form 4 transactions.

    Keyed on (reporter_cik, symbol, txn_date) —
    latest filing per reporter+symbol+date wins.
    """
    new_rows = []
    for t in txns:
        if t.issuer_ticker not in sym_universe:
            continue
        new_rows.append({
            'symbol': t.issuer_ticker,
            'reporter': t.reporter,
            'reporter_cik': t.reporter_cik,
            'txn_date': t.txn_date,
            'txn_code': t.txn_code,
            'shares_txn': t.shares_txn,
            'post_shares': t.post_shares,
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
