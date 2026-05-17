"""424B5 parser — primary offering supplement.

A 424B5 is a primary-shelf prospectus supplement: the
issuer is selling new shares off an existing S-3 shelf.
Selling stockholders may piggy-back. Both preliminary
(red herring) and final (priced) variants land on the
same form code.
"""

from __future__ import annotations

from app.parsers.reg import (
    RegFiling,
    decode_raw,
    find_exchange,
    find_issuer_shares,
    find_last_price,
    find_offer_price,
    find_ssh,
    find_ticker,
    find_title_shares,
    find_total,
    is_bought_deal,
    is_ipo,
    is_preliminary,
    strip_html,
)


def parse_424b5(raw: bytes) -> RegFiling | None:
    """Parse a 424B5 filing.

    Returns None for filings that are clearly not
    equity (debt notes, ATM programs, etc.). Otherwise
    returns a RegFiling — fields may be 0/''/False
    when extraction fails; missing list records gaps.
    """
    text = decode_raw(raw)
    clean = strip_html(text)
    low = clean[:10000].lower()

    if _is_debt_or_atm(low):
        return None

    f = RegFiling(form_type='424B5')
    f.shares_offered = find_title_shares(clean)
    if f.shares_offered == 0:
        f.missing.append('shares_offered')

    f.offer_price = find_offer_price(clean)
    if f.offer_price == 0.0:
        f.missing.append('offer_price')

    f.total = find_total(clean)
    if f.total == 0.0:
        f.missing.append('total')

    f.last_price, f.last_price_date = find_last_price(clean)
    if f.last_price == 0.0:
        f.missing.append('last_price')

    f.issuer_shares = find_issuer_shares(clean)
    has_ssh, ssh_n = find_ssh(clean)
    f.has_selling_stockholder = has_ssh
    f.ssh_shares = ssh_n

    f.ticker = find_ticker(clean)
    if not f.ticker:
        f.missing.append('ticker')
    f.exchange = find_exchange(clean)
    if not f.exchange:
        f.missing.append('exchange')

    f.is_preliminary = is_preliminary(clean)
    f.is_ipo = is_ipo(clean)
    f.is_bought = is_bought_deal(clean)

    return f


def _is_debt_or_atm(low: str) -> bool:
    """Heuristics from the prior parser: reject debt
    notes, ATM programs, shelf base prospectuses, and
    preferred-stock offerings (none of which are
    equity blocks for our purposes).

    'Depositary shares' is preferred-stock-receipt
    language and rejects, but 'American Depositary
    Shares' (ADSs) are common-equivalent and keep.
    """
    if 'notes' in low[:2000] and 'shares' not in low[:2000]:
        return True
    if 'at the market' in low or 'at-the-market' in low:
        return True
    if 'may from time to time' in low[:3000]:
        return True
    title = low[:3000]
    if 'trust preferred' in title or 'series of preferred' in title:
        return True
    # Preferred depositary receipts vs ADSs
    if 'depositary shares' in title and 'american depositary' not in title:
        return True
    return False
