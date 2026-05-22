"""Persisted trades table — deal-level candidates.

One row per detected/seeded block. Sync loosely flags
new candidates into here; the review/confirmed
workflow promotes confirmed rows into the blocks
table (same schema + status).

Primary key (unique): (price_date, symbol, offer_price)

  upsert_trades(rows: list[dict]) — merge
  load_trades()                   — read as DataFrame
"""

from pathlib import Path

import polars as pl

from app.mds import DATA_DIR as MDS_DIR
from app.util.log import log

TABLE_DIR = Path(MDS_DIR).parent / 'trades'
TABLE_PATH = TABLE_DIR / 'trades.parquet'

SCHEMA = {
    # ---- Composite key (unique) ----
    'price_date':   pl.Date,
    'symbol':       pl.Utf8,
    'offer_price':  pl.Float64,     # as-filed (historical)
    # ---- Classification ----
    'type':         pl.Utf8,        # 'Reg' | 'Unreg'
    # ---- Trade details ----
    'trade_date':   pl.Date,
    'intraday':     pl.Boolean,
    'shares':       pl.Int64,       # as-filed
    'notional':     pl.Float64,     # shares * offer_price
    # ---- Split-adjusted (today's basis) ----
    # Backtests/analytics should use these fields —
    # they reflect current share prices and counts so
    # cross-deal comparisons stay apples-to-apples.
    # split_factor: cumulative factor from price_date
    # forward. >1 = forward split since; <1 = reverse;
    # =1 = no split.
    'split_factor': pl.Float64,
    'adj_shares':   pl.Int64,    # shares * split_factor
    'adj_price':    pl.Float64,  # offer_price / split_factor
    # ---- Seller ----
    'seller':       pl.Utf8,
    'relationship': pl.Utf8,
    # ---- Banking ----
    'banks':        pl.List(pl.Utf8),
    # ---- Provenance ----
    'cik':          pl.Utf8,
    'evidence':     pl.Utf8,        # 'golden' | 'form4' | 'reg' | ...
    'source':       pl.Utf8,        # source ref (golden filename etc.)
}

KEY_COLS = ['price_date', 'symbol', 'offer_price']


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=SCHEMA)


def load_trades() -> pl.DataFrame:
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


def upsert_trades(
    rows: list[dict],
    protect_curated: bool = False,
) -> int:
    """Merge new rows. Key collisions overwrite the
    existing row. Returns count of net-new rows.

    When protect_curated=True, rows whose key already
    exists with evidence != 'detected' are skipped —
    used by sync's detect pass so it doesn't clobber
    golden-seeded data on a re-run.
    """
    if not rows:
        return 0

    new_df = pl.DataFrame(rows, schema=SCHEMA)
    new_df = new_df.unique(subset=KEY_COLS, keep='last')

    existing = load_trades()

    if protect_curated and existing.height > 0:
        curated = existing.filter(
            pl.col('evidence') != 'detected'
        ).select(KEY_COLS)
        new_df = new_df.join(
            curated, on=KEY_COLS, how='anti'
        )
        if new_df.height == 0:
            log.info(
                'upsert trades',
                new=len(rows),
                added=0,
                skipped_curated=len(rows),
                total=existing.height,
            )
            return 0

    keys = new_df.select(KEY_COLS)
    kept = existing.join(keys, on=KEY_COLS, how='anti')
    merged = pl.concat([kept, new_df])
    _save(merged)

    added = merged.height - existing.height
    log.info(
        'upsert trades',
        new=len(rows),
        added=added,
        total=merged.height,
    )
    return added
