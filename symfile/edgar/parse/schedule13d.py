"""Parse Schedule 13D/A XML filings from EDGAR.

Extracts issuer CUSIP, event date, and reporting
person ownership (shares + percent of class).
"""

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

_NS_RE = re.compile(r'\{[^}]+\}')


@dataclass
class Filing13D:
    issuer_cusip: str
    issuer_name: str
    event_date: str
    holder: str
    shares: int
    pct_class: float


def _find(node, path: str) -> str:
    for tag in path.split('/'):
        found = None
        for child in node:
            local = _NS_RE.sub('', child.tag)
            if local == tag:
                found = child
                break
        if found is None:
            return ''
        node = found
    return (node.text or '').strip()


def _find_all(node, tag: str) -> list:
    results = []
    for child in node:
        local = _NS_RE.sub('', child.tag)
        if local == tag:
            results.append(child)
    return results


def _parse_float(s: str) -> float:
    try:
        return float(s.replace(',', ''))
    except (ValueError, TypeError):
        return 0.0


def parse_13d(
    raw: bytes,
) -> Filing13D | None:
    """Parse a Schedule 13D .txt filing.

    Returns the reporting person with the largest
    aggregate position, or None if unparseable.
    """
    text = raw.decode('latin-1')

    xs = text.find('<XML>')
    xe = text.find('</XML>')
    if xs < 0 or xe < 0:
        return None
    xml_str = text[xs + 5 : xe].strip()

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None

    form_data = None
    for child in root:
        if _NS_RE.sub('', child.tag) == 'formData':
            form_data = child
            break
    if form_data is None:
        return None

    cusip = _find(
        form_data,
        'coverPageHeader/issuerInfo/issuerCUSIP',
    )
    issuer = _find(
        form_data,
        'coverPageHeader/issuerInfo/issuerName',
    )
    event = _find(
        form_data,
        'coverPageHeader/dateOfEvent',
    )

    rp_node = None
    for child in form_data:
        if (
            _NS_RE.sub('', child.tag)
            == 'reportingPersons'
        ):
            rp_node = child
            break
    if rp_node is None:
        return None

    persons = _find_all(
        rp_node, 'reportingPersonInfo'
    )
    if not persons:
        return None

    best_name = ''
    best_shares = 0
    best_pct = 0.0

    for p in persons:
        name = _find(p, 'reportingPersonName')
        shares = _parse_float(
            _find(p, 'aggregateAmountOwned')
        )
        pct = _parse_float(
            _find(p, 'percentOfClass')
        )
        if shares > best_shares:
            best_shares = int(shares)
            best_pct = pct
            best_name = name

    if not best_name:
        return None

    return Filing13D(
        issuer_cusip=cusip,
        issuer_name=issuer,
        event_date=event,
        holder=best_name,
        shares=best_shares,
        pct_class=best_pct,
    )
