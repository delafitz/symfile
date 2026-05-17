"""Confirmed blocks table.

Same schema as the trades table plus a status/audit
pair. A row in `blocks` represents a confirmed block
(seeded from golden or human-reviewed). The shared
primary key lets `blocks` join `trades` trivially.

Primary key (unique): (price_date, symbol, offer_price)
"""

from pathlib import Path

import polars as pl

from app.trades.table import KEY_COLS, SCHEMA as TRADES_SCHEMA
from app.trades.table import TABLE_DIR
from app.util.log import log

TABLE_PATH = TABLE_DIR / 'blocks.parquet'

SCHEMA = {
    **TRADES_SCHEMA,
    # Review metadata
    'status':       pl.Utf8,    # 'confirmed' | 'rejected'
    'reviewed_at':  pl.Utf8,
}


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_blocks() -> pl.DataFrame:
    if not TABLE_PATH.exists():
        return _empty()
    df = pl.read_parquet(TABLE_PATH)
    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(dtype).alias(col)
            )
    return df.select(list(SCHEMA.keys()))


def _save(df: pl.DataFrame) -> None:
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(TABLE_PATH)


def upsert_blocks(rows: list[dict]) -> int:
    """Merge confirmed/rejected block rows. Returns
    count of net-new rows added."""
    if not rows:
        return 0

    new_df = pl.DataFrame(rows, schema=SCHEMA)
    new_df = new_df.unique(subset=KEY_COLS, keep='last')

    existing = load_blocks()
    keys = new_df.select(KEY_COLS)
    kept = existing.join(keys, on=KEY_COLS, how='anti')
    merged = pl.concat([kept, new_df])
    _save(merged)

    added = merged.height - existing.height
    log.info(
        'upsert blocks',
        new=len(rows),
        added=added,
        total=merged.height,
    )
    return added


def load_confirmed() -> pl.DataFrame:
    """Confirmed blocks only (status='confirmed')."""
    return load_blocks().filter(
        pl.col('status') == 'confirmed'
    )
