"""Unified symbol reference data.

Provides load_syms() -> dict[symbol, RefRow] combining
Polygon tickers and market data.
"""

from app.mds.massive.adv import load_adv
from app.mds.massive.cusips import load_cusips
from app.mds.massive.refs import (
    RefRow,
    build_cik_map,
    load_refs,
)
from app.mds.massive.tickers import load_tickers


def load_syms(
    max_age_days: int | None = None,
) -> dict[str, RefRow]:
    """Load symbol reference table.

    Returns dict[symbol] -> RefRow(symbol, cik, name,
    mkt_cap, price, adv) for stocks with mkt_cap
    >= $1B. ADV is 30-day mean volume when the
    adv.*.csv cache exists; 0.0 otherwise.
    """
    kw: dict = {}
    if max_age_days is not None:
        kw['max_age_days'] = max_age_days
    refs = load_refs(**kw)

    # Merge in ADV from its own cache. If missing,
    # leave adv=0 on each RefRow — call `adv` CLI to
    # populate.
    try:
        adv = load_adv()
    except Exception:
        adv = {}
    for sym, v in adv.items():
        if sym in refs:
            refs[sym].adv = v
    return refs


__all__ = [
    'RefRow',
    'build_cik_map',
    'load_cusips',
    'load_syms',
    'load_tickers',
]
