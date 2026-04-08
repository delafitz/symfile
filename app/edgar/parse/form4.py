"""Parse Form 4 XML filings from EDGAR.

Extracts issuer ticker, reporter name, transaction
details, and post-transaction share position.
"""

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

_NS_RE = re.compile(r'\{[^}]+\}')


@dataclass
class Filing4:
    issuer_cik: str
    issuer_ticker: str
    issuer_name: str
    reporter: str
    reporter_cik: str
    txn_date: str
    txn_code: str
    shares_txn: int
    acquired: bool
    post_shares: int


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


def parse_form4(
    raw: bytes,
) -> list[Filing4]:
    """Parse a Form 4 .txt filing.

    Returns one Filing4 per transaction. Multiple
    transactions may exist in a single filing.
    Returns empty list if unparseable.
    """
    text = raw.decode('latin-1')

    xs = text.find('<XML>')
    xe = text.find('</XML>')
    if xs < 0 or xe < 0:
        return []
    xml_str = text[xs + 5 : xe].strip()

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return []

    issuer_cik = _find(root, 'issuer/issuerCik')
    issuer_ticker = _find(
        root, 'issuer/issuerTradingSymbol'
    )
    issuer_name = _find(root, 'issuer/issuerName')

    rpt_owner = None
    for child in root:
        if _NS_RE.sub('', child.tag) == 'reportingOwner':
            rpt_owner = child
            break
    if rpt_owner is None:
        return []

    reporter = _find(
        rpt_owner, 'reportingOwnerId/rptOwnerName'
    )
    reporter_cik = _find(
        rpt_owner, 'reportingOwnerId/rptOwnerCik'
    )

    txn_table = None
    non_deriv = None
    for child in root:
        local = _NS_RE.sub('', child.tag)
        if local == 'nonDerivativeTable':
            non_deriv = child
            break

    if non_deriv is None:
        return []

    results = []
    txns = _find_all(non_deriv, 'nonDerivativeTransaction')
    for txn in txns:
        txn_date = _find(
            txn, 'transactionDate/value'
        )
        code = _find(
            txn, 'transactionCoding/transactionCode'
        )
        shares_val = _parse_float(
            _find(
                txn,
                'transactionAmounts'
                '/transactionShares/value',
            )
        )
        ad = _find(
            txn,
            'transactionAmounts'
            '/transactionAcquiredDisposedCode'
            '/value',
        )
        post_val = _parse_float(
            _find(
                txn,
                'postTransactionAmounts'
                '/sharesOwnedFollowingTransaction'
                '/value',
            )
        )

        results.append(
            Filing4(
                issuer_cik=issuer_cik,
                issuer_ticker=issuer_ticker.upper(),
                issuer_name=issuer_name,
                reporter=reporter,
                reporter_cik=reporter_cik,
                txn_date=txn_date,
                txn_code=code,
                shares_txn=int(shares_val),
                acquired=ad == 'A',
                post_shares=int(post_val),
            )
        )

    return results
