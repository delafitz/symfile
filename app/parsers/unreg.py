"""UnregDeal: cluster 144 + Form 4 filings for one
unregistered block trade.

  144s give intent + shares + a market-value reference
  (sale_price ≈ mkt_value/shares, but this is just the
  market snapshot at filing time, not the block clear).

  Form 4s give the actual transaction (txn_code='S' for
  outright sale) with txn_price + shares_txn.

Resolution rule of thumb:
  - sum shares across Form 4 sales whose txn_date matches
    the golden TradeDt (or falls in the cluster window)
  - share-weighted average of Form 4 txn_price is the
    block clear estimate
  - 144 mkt_value gives a sanity-check reference
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class UnregDeal:
    cik: str
    symbol: str
    price_date: date | None = None
    trade_date: date | None = None
    intraday: bool = False

    # Counts
    n_144: int = 0
    n_form4: int = 0

    # Form 4 aggregates (sale transactions only)
    shares_sold: int = 0
    txn_price_wavg: float = 0.0
    txn_price_min: float = 0.0
    txn_price_max: float = 0.0
    txn_codes: set[str] = field(default_factory=set)

    # 144 aggregates
    f144_shares: int = 0
    f144_mkt_value: float = 0.0

    @property
    def f144_price(self) -> float:
        """Implicit market-reference price from the 144."""
        if self.f144_shares <= 0:
            return 0.0
        return self.f144_mkt_value / self.f144_shares

    @property
    def evidence(self) -> str:
        """Which filings contributed real evidence."""
        f4 = self.shares_sold > 0
        f144 = self.f144_shares > 0
        if f4 and f144:
            return 'both'
        if f4:
            return 'form4'
        if f144:
            return '144'
        return 'none'

    @property
    def block_shares(self) -> int:
        """Best size estimate. Prefers Form 4 sum
        (actual executed); falls back to 144 sum
        (declared intent)."""
        return self.shares_sold or self.f144_shares

    @property
    def block_price(self) -> float:
        """Best price estimate. Form 4 weighted avg
        is closer to the block clear; 144 implicit
        price is a filing-day market reference."""
        return self.txn_price_wavg or self.f144_price


def resolve_unreg_deal(
    *,
    cik: str,
    symbol: str,
    price_date: date | None,
    trade_date: date | None,
    intraday: bool,
    form4_txns: list,    # list[Filing4]
    f144_filings: list,  # list[Filing144]
) -> UnregDeal:
    """Aggregate parsed 144 + Form 4 records into one
    UnregDeal.

    Form 4: only count sale-type transactions
    (txn_code 'S') whose txn_date matches the trade
    window. Tax-withholding ('F') and option-exercise
    ('M') are excluded — they're not block sales.
    """
    d = UnregDeal(
        cik=cik,
        symbol=symbol,
        price_date=price_date,
        trade_date=trade_date,
        intraday=intraday,
    )

    # --- Form 4 aggregation ---
    # Drop issuer-as-reporter transactions: when the
    # company files a Form 4 on its own CIK, the txns
    # are typically restricted-share reorganizations
    # (forfeitures, ESPP returns) at par/nominal price,
    # not market sales. They poison a weighted average.
    relevant = []
    issuer_unpadded = (cik or '').lstrip('0') or '0'
    for t in form4_txns:
        d.txn_codes.add(t.txn_code)
        if t.txn_code != 'S':
            continue
        if t.shares_txn <= 0 or t.txn_price <= 0:
            continue
        rpt = (t.reporter_cik or '').lstrip('0') or '0'
        if rpt == issuer_unpadded:
            continue
        # txn_date must fall within ±5 of trade_date.
        # ±5 catches amended Form 4s that report sales
        # a few days earlier than the block we labelled.
        if trade_date is not None and t.txn_date:
            try:
                from datetime import datetime
                td = datetime.strptime(
                    t.txn_date, '%Y-%m-%d'
                ).date()
            except ValueError:
                continue
            if abs((td - trade_date).days) > 5:
                continue
        relevant.append(t)

    if relevant:
        total_sh = sum(t.shares_txn for t in relevant)
        d.shares_sold = total_sh
        d.txn_price_wavg = (
            sum(t.shares_txn * t.txn_price for t in relevant)
            / total_sh
        )
        prices = [t.txn_price for t in relevant]
        d.txn_price_min = min(prices)
        d.txn_price_max = max(prices)
    d.n_form4 = len(form4_txns)

    # --- 144 aggregation ---
    for f in f144_filings:
        if f.shares > 0:
            d.f144_shares += f.shares
        if f.mkt_value > 0:
            d.f144_mkt_value += f.mkt_value
    d.n_144 = len(f144_filings)

    return d
