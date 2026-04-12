"""Persisted trades table.

Stores block-trade records (144 + registered offerings)
as a parquet file. Keyed on (symbol, date_filed,
filing_type, seller, shares) — latest filing wins.

Usage:
    upsert_trades(trades)  — merge new Trade records
    load_trades()          — read table as DataFrame
"""

from dataclasses import asdict
from pathlib import Path

import polars as pl

from app.mds import DATA_DIR as MDS_DIR
from app.trades.hist import Trade
from app.util.log import log

TABLE_DIR = Path(MDS_DIR).parent / 'trades'
TABLE_PATH = TABLE_DIR / 'trades.parquet'

SCHEMA = {
    'symbol': pl.Utf8,
    'date_filed': pl.Utf8,
    'shares': pl.Int64,
    'implied_value': pl.Float64,
    'price': pl.Float64,
    'price_source': pl.Utf8,
    'filing_type': pl.Utf8,
    'seller': pl.Utf8,
    'relationship': pl.Utf8,
    'underwriter': pl.Utf8,
    'mkt_cap': pl.Float64,
    'flagged_block': pl.Boolean,
    'is_ipo': pl.Boolean,
    'nature': pl.Utf8,
    'pct_outstanding': pl.Float64,
    'lockup': pl.Boolean,
    'lockup_days': pl.Int64,
}

KEY_COLS = [
    'symbol', 'date_filed', 'filing_type',
    'seller', 'shares',
]


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_trades() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    df = pl.read_parquet(TABLE_PATH)
    # Add columns missing from older files
    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(dtype).alias(col)
            )
    return df.select(list(SCHEMA.keys()))


def _save(df: pl.DataFrame) -> None:
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(TABLE_PATH)


def upsert_trades(trades: list[Trade]) -> int:
    """Merge new Trade records into the table.

    Returns count of net-new rows added.
    """
    if not trades:
        return 0

    new_df = pl.DataFrame(
        [asdict(t) for t in trades],
        schema=SCHEMA,
    )

    existing = load_trades()

    keys = new_df.select(KEY_COLS).unique()
    kept = existing.join(
        keys, on=KEY_COLS, how='anti',
    )

    merged = pl.concat([kept, new_df])
    _save(merged)

    added = merged.height - existing.height
    log.info(
        'upsert trades',
        new=len(trades),
        added=added,
        total=merged.height,
    )
    return added
