"""424B4 parser — supplement under Rule 424(b)(4).

Used for IPO finals and some shelf supplements. The
cover structure matches B5/B7. IPO context will set
is_ipo on the returned RegFiling.
"""

from __future__ import annotations

from app.parsers.reg import RegFiling, parse_supplement


def parse_424b4(raw: bytes) -> RegFiling | None:
    return parse_supplement(raw, '424B4')
