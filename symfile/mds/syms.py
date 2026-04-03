"""Unified symbol reference data.

Provides load_syms() -> dict[symbol, RefRow] combining
Polygon tickers and market data.
"""

from symfile.mds.massive.refs import (
    RefRow,
    build_cik_map,
    load_refs,
)
from symfile.mds.massive.tickers import load_tickers


def load_syms(
    max_age_days: int | None = None,
) -> dict[str, RefRow]:
    """Load symbol reference table.

    Returns dict[symbol] -> RefRow(symbol, cik, name,
    mkt_cap, price) for stocks with mkt_cap >= $1B.
    """
    kw: dict = {}
    if max_age_days is not None:
        kw['max_age_days'] = max_age_days
    return load_refs(**kw)


__all__ = [
    'RefRow',
    'build_cik_map',
    'load_syms',
    'load_tickers',
]
