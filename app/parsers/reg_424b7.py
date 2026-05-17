"""424B7 parser — selling-stockholder resale supplement.

Secondary-only shelf supplement. Same cover structure
as 424B3/B4/B5; the form code is procedural.
"""

from __future__ import annotations

from app.parsers.reg import RegFiling, parse_supplement


def parse_424b7(raw: bytes) -> RegFiling | None:
    f = parse_supplement(raw, '424B7')
    if f is not None:
        # 424B7 is by definition a SSH resale
        f.has_selling_stockholder = True
        f.issuer_shares = 0
    return f
