"""Block-trade detection layer.

Sync's job is to fetch filings; detect's job is to spot
block trades inside that filing stream and emit
deal-level candidate rows into trades.parquet.

  thresholds  — size + size-relative gates a deal must
                clear to qualify as a candidate
  reg         — 424B cluster -> RegDeal -> candidate row
  unreg       — 144 + Form 4 cluster -> UnregDeal ->
                candidate row (includes Form4-only path)
"""
