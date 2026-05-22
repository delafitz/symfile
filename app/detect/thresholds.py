"""Block-trade qualification thresholds.

A resolved deal becomes a candidate trade if it meets:
  1. Minimum notional ($50M floor across reg + unreg)
  2. Either of the size-relative gates:
     - shares >= BLOCK_PCT_OUTSTANDING * float
     - shares >= BLOCK_PCT_ADV * 30-day ADV
  3. Notional <= MAX_MCAP_PCT * market cap (filters
     impossible parser results)

Size-relative gates are evaluated using the ADJUSTED
share count (today's basis) against today's float and
ADV, so the comparison stays apples-to-apples for
deals that experienced subsequent splits.
"""

from __future__ import annotations

# Minimum block notional in USD (both reg + unreg).
MIN_NOTIONAL = 50_000_000

# Either-or gates against today's float / ADV.
BLOCK_PCT_OUTSTANDING = 0.01   # >= 1% of shares out
BLOCK_PCT_ADV = 0.50           # >= 50% of 30-day ADV

# Sanity ceiling: notional > 20% mkt cap is almost
# certainly a parser error.
MAX_MCAP_PCT = 0.20


def shares_outstanding(mkt_cap: float, price: float) -> float:
    """Float estimate. mkt_cap and price come from refs."""
    if price <= 0:
        return 0.0
    return mkt_cap / price


def qualifies(
    *,
    notional: float,
    adj_shares: int,
    mkt_cap: float,
    ref_price: float,
    adv: float,
) -> bool:
    """True if a resolved deal meets all block gates.

    `notional` should be on the current basis
    (adj_shares * adj_price) so it lines up with today's
    mkt_cap.
    """
    if notional < MIN_NOTIONAL:
        return False
    if mkt_cap > 0 and notional > MAX_MCAP_PCT * mkt_cap:
        return False
    so = shares_outstanding(mkt_cap, ref_price)
    pct_so = adj_shares / so if so > 0 else 0.0
    pct_adv = adj_shares / adv if adv > 0 else 0.0
    if pct_so >= BLOCK_PCT_OUTSTANDING:
        return True
    if pct_adv >= BLOCK_PCT_ADV:
        return True
    return False
