"""424B7 parser — selling-stockholder resale supplement.

A 424B7 is a secondary-only prospectus supplement:
shares are sold by existing holders off a resale S-3.
Issuer receives no proceeds. The cover almost always
opens with: "The selling stockholder identified in
this prospectus supplement is offering N shares..."
"""

from __future__ import annotations

from app.parsers.reg import (
    RegFiling,
    decode_raw,
    find_exchange,
    find_last_price,
    find_offer_price,
    find_ssh,
    find_ticker,
    find_title_shares,
    find_total,
    is_bought_deal,
    is_preliminary,
    strip_html,
)


def parse_424b7(raw: bytes) -> RegFiling | None:
    """Parse a 424B7 filing.

    424B7 is by definition a selling-stockholder
    resale, so has_selling_stockholder is always True
    on successful parse.
    """
    text = decode_raw(raw)
    clean = strip_html(text)
    low = clean[:10000].lower()

    if _is_not_equity_resale(low):
        return None

    f = RegFiling(form_type='424B7')
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

    f.has_selling_stockholder = True
    _, f.ssh_shares = find_ssh(clean)
    # By construction the issuer is selling nothing
    f.issuer_shares = 0

    f.ticker = find_ticker(clean)
    if not f.ticker:
        f.missing.append('ticker')
    f.exchange = find_exchange(clean)
    if not f.exchange:
        f.missing.append('exchange')

    f.is_preliminary = is_preliminary(clean)
    f.is_bought = is_bought_deal(clean)
    # IPO doesn't apply to 424B7

    return f


def _is_not_equity_resale(low: str) -> bool:
    """Reject non-common-stock resales. ADSs are
    fine; preferred depositary receipts are not."""
    title = low[:3000]
    if 'trust preferred' in title or 'series of preferred' in title:
        return True
    if 'depositary shares' in title and 'american depositary' not in title:
        return True
    if 'notes' in low[:2000] and 'shares' not in low[:2000]:
        return True
    return False
