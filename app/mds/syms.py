"""Unified symbol reference data.

  load_syms()       — current trading universe (refs +
                       ADV merged in).
  resolve_cik(sym)  — canonical symbol→CIK lookup.
                       Checks active refs first; falls
                       back to the inactive ticker cache
                       so historical (delisted) symbols
                       still resolve.

The split keeps trading/sync decisions on the filtered
universe while letting analysis tools resolve any
historical symbol that ever existed.
"""

from app.mds.massive.adv import load_adv
from app.mds.massive.cusips import load_cusips
from app.mds.massive.refs import (
    RefRow,
    build_cik_map,
    load_refs,
)
from app.mds.massive.tickers import load_tickers


_resolver: dict[str, str] | None = None


def _build_resolver() -> dict[str, str]:
    """Combine inactive + active Polygon tickers into a
    single symbol→CIK map. Active wins on collision so
    recycled tickers point at the current issuer.

    Uses raw Polygon tickers — NOT the filtered refs
    cache — because resolution should succeed even for
    symbols that don't pass the trading universe filter
    (small caps, recently uplisted, etc.).
    """
    out: dict[str, str] = {}
    try:
        inactive = load_tickers(active=False)
        for sym, info in inactive.items():
            cik = info.get('cik')
            if cik:
                out[sym] = cik.lstrip('0') or '0'
    except Exception:
        pass

    active = load_tickers(active=True)
    for sym, info in active.items():
        cik = info.get('cik')
        if cik:
            out[sym] = cik.lstrip('0') or '0'
    return out


def resolve_cik(symbol: str) -> str | None:
    """Resolve a (possibly historical) symbol to its
    issuer CIK. CIK is returned unpadded (matches
    EDGAR index format)."""
    global _resolver
    if _resolver is None:
        _resolver = _build_resolver()
    return _resolver.get(symbol)


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
    'resolve_cik',
]
