"""Parse 13F-HR/A infotable from individual filings.

Extracts holdings from the second XML block in
the SGML .txt filing. Handles both bare and
namespace-prefixed tags.
"""

import re
from dataclasses import dataclass


@dataclass
class Holding13F:
    cusip: str
    shares: int
    sh_type: str
    put_call: str


_CUSIP_RE = re.compile(
    r'<(?:\w+:)?cusip>([^<]+)'
    r'</(?:\w+:)?cusip>'
)
_SHARES_RE = re.compile(
    r'<(?:\w+:)?sshPrnamt>([^<]+)'
    r'</(?:\w+:)?sshPrnamt>'
)
_TYPE_RE = re.compile(
    r'<(?:\w+:)?sshPrnamtType>([^<]+)'
    r'</(?:\w+:)?sshPrnamtType>'
)
_PUTCALL_RE = re.compile(
    r'<(?:\w+:)?putCall>([^<]+)'
    r'</(?:\w+:)?putCall>'
)
_ENTRY_RE = re.compile(
    r'<(?:\w+:)?infoTable>(.*?)'
    r'</(?:\w+:)?infoTable>',
    re.S,
)


def parse_13f_holdings(
    raw: bytes,
) -> list[Holding13F] | None:
    """Parse infotable from a 13F .txt filing.

    Returns list of holdings or None if no
    infotable found.
    """
    text = raw.decode('latin-1')

    xml_starts = [
        m.start()
        for m in re.finditer('<XML>', text)
    ]
    xml_ends = [
        m.start()
        for m in re.finditer('</XML>', text)
    ]

    if len(xml_starts) < 2:
        return None

    infotable = text[
        xml_starts[1] + 5 : xml_ends[1]
    ].strip()

    entries = _ENTRY_RE.findall(infotable)
    if not entries:
        return None

    holdings = []
    for entry in entries:
        cusip_m = _CUSIP_RE.search(entry)
        shares_m = _SHARES_RE.search(entry)
        type_m = _TYPE_RE.search(entry)
        pc_m = _PUTCALL_RE.search(entry)

        if not cusip_m or not shares_m:
            continue

        sh_type = (
            type_m.group(1).strip()
            if type_m
            else ''
        )
        if sh_type != 'SH':
            continue

        put_call = (
            pc_m.group(1).strip() if pc_m else ''
        )
        if put_call:
            continue

        try:
            shares = int(
                shares_m.group(1)
                .strip()
                .replace(',', '')
            )
        except ValueError:
            continue

        holdings.append(
            Holding13F(
                cusip=cusip_m.group(1).strip(),
                shares=shares,
                sh_type=sh_type,
                put_call=put_call,
            )
        )

    return holdings
