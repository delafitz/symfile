"""424B2 parser — primary supplement (Rule 424(b)(2)).

This form covers a wide range of offerings: medium-term
notes, structured products, forward-sale common stock
(ED), preferred. The reject heuristics in parse_supplement
already discard notes / ATM / preferred, so what's left
is the common-stock subset (utility forward sales and
some primary blocks).
"""

from __future__ import annotations

from app.parsers.reg import RegFiling, parse_supplement


def parse_424b2(raw: bytes) -> RegFiling | None:
    return parse_supplement(raw, '424B2')
