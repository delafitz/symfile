"""Parse Form 144 XML filings from EDGAR.

The .txt filing wraps an SGML envelope around
primary_doc.xml. We extract the XML and parse it.
"""

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

# Strip namespace prefixes for easier xpath
_NS_RE = re.compile(r'\{[^}]+\}')


@dataclass
class Filing144:
    seller: str
    relationship: str
    title: str
    shares: int
    outstanding: int  # shares outstanding
    mkt_value: float
    sale_date: str
    broker: str
    nature: str  # how acquired (RSU, founders, etc.)
    remarks: str


def parse_144(raw: bytes) -> Filing144 | None:
    """Parse a Form 144 .txt filing into structured
    data. Returns None if XML cannot be parsed."""
    text = raw.decode('latin-1')

    # Extract the XML block from SGML wrapper
    start = text.find('<XML>')
    end = text.find('</XML>')
    if start < 0 or end < 0:
        return None
    xml_str = text[start + 5 : end].strip()

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None

    def find(path: str) -> str:
        """Find element by local name path
        (namespace-agnostic)."""
        node = root
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

    def find_all(
        parent_path: str, child_tag: str
    ) -> list[str]:
        """Find all matching children under a
        parent path."""
        node = root
        for tag in parent_path.split('/'):
            found = None
            for child in node:
                local = _NS_RE.sub('', child.tag)
                if local == tag:
                    found = child
                    break
            if found is None:
                return []
            node = found
        results = []
        for child in node:
            local = _NS_RE.sub('', child.tag)
            if local == child_tag:
                results.append(
                    (child.text or '').strip()
                )
        return results

    seller = find(
        'formData/issuerInfo'
        '/nameOfPersonForWhoseAccount'
        'TheSecuritiesAreToBeSold'
    )
    rels = find_all(
        'formData/issuerInfo/relationshipsToIssuer',
        'relationshipToIssuer',
    )
    title = find(
        'formData/securitiesInformation'
        '/securitiesClassTitle'
    )
    shares_str = find(
        'formData/securitiesInformation'
        '/noOfUnitsSold'
    )
    value_str = find(
        'formData/securitiesInformation'
        '/aggregateMarketValue'
    )
    outstanding_str = find(
        'formData/securitiesInformation'
        '/noOfUnitsOutstanding'
    )
    sale_date = find(
        'formData/securitiesInformation'
        '/approxSaleDate'
    )
    broker = find(
        'formData/securitiesInformation'
        '/brokerOrMarketmakerDetails/name'
    )
    nature = find(
        'formData/securitiesToBeSold'
        '/natureOfAcquisitionTransaction'
    )
    remarks = find('formData/remarks')

    try:
        shares = int(
            shares_str.replace(',', '')
        )
    except ValueError:
        shares = 0
    try:
        outstanding = int(
            outstanding_str.replace(',', '')
        )
    except ValueError:
        outstanding = 0
    try:
        mkt_value = float(
            value_str.replace(',', '')
        )
    except ValueError:
        mkt_value = 0.0

    return Filing144(
        seller=seller,
        relationship=', '.join(rels),
        title=title,
        shares=shares,
        outstanding=outstanding,
        mkt_value=mkt_value,
        sale_date=sale_date,
        broker=broker,
        nature=nature,
        remarks=remarks,
    )
