"""424B3 parser — post-effective shelf resale.

Used by some issuers (CCC, SOFI, GEHC, INTA, ...)
in place of 424B7 for selling-stockholder resales.
Cover structure is identical to B5/B7.
"""

from __future__ import annotations

from app.parsers.reg import RegFiling, parse_supplement


def parse_424b3(raw: bytes) -> RegFiling | None:
    return parse_supplement(raw, '424B3')
