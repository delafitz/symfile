"""424B5 parser — primary offering supplement.

Issuer-led primary shelf supplement. Cover structure
is the same as B3/B4/B7, so this is a thin wrapper
over the shared parse_supplement.
"""

from __future__ import annotations

from app.parsers.reg import RegFiling, parse_supplement


def parse_424b5(raw: bytes) -> RegFiling | None:
    return parse_supplement(raw, '424B5')
