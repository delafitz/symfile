"""Resolve a cluster of 424B* prospectus supplements
into a single RegDeal.

A registered block trade typically files in stages:

  Day T   424B5 (preliminary) — "Subject to completion"
                                 has last_price, shares
  Day T+0 424B7 (preliminary) — parallel SSH resale
  Day T+1 424B5 (final)       — has offer_price + total
  Day T+1 424B7 (final)       — has offer_price + total

The resolver takes the cluster (already grouped by
issuer + time window upstream), parses each filing,
and merges fields with priority rules:

  shares, offer_price, total -> prefer final
  last_price, last_price_date -> prefer preliminary
  is_bought, has_ssh, underwriter, lockup -> any
  announce_date -> earliest filing date
  price_date    -> final-filing date
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from app.parsers.reg import RegFiling
from app.parsers.reg_424b5 import parse_424b5
from app.parsers.reg_424b7 import parse_424b7


@dataclass
class ClusterMember:
    """One filing within a deal cluster."""
    filename: str
    filing_date: date
    form_type: str
    cik: str
    parsed: RegFiling | None


@dataclass
class RegDeal:
    """Composite of one announce-to-pricing cluster."""
    cik: str
    symbol: str

    # Timeline
    announce_date: date | None = None
    price_date: date | None = None

    # Provenance
    filenames: list[str] = field(default_factory=list)
    forms: list[str] = field(default_factory=list)

    # Best-resolved equity fields
    issuer_name: str = ''
    shares_offered: int = 0
    offer_price: float = 0.0
    total: float = 0.0
    last_price: float = 0.0
    last_price_date: str = ''
    ticker: str = ''
    exchange: str = ''

    # Classification
    is_bought: bool = False
    is_ipo: bool = False
    has_selling_stockholder: bool = False
    ssh_shares: int = 0
    issuer_shares: int = 0
    underwriter: str = ''
    lockup: bool = False
    lockup_days: int = 0


def parse_member(
    filename: str,
    filing_date: date,
    form_type: str,
    cik: str,
    raw: bytes,
) -> ClusterMember:
    """Run the appropriate per-form parser."""
    if form_type == '424B5':
        parsed = parse_424b5(raw)
    elif form_type == '424B7':
        parsed = parse_424b7(raw)
    else:
        parsed = None
    return ClusterMember(
        filename=filename,
        filing_date=filing_date,
        form_type=form_type,
        cik=cik,
        parsed=parsed,
    )


def resolve_deal(
    members: list[ClusterMember],
    symbol: str,
) -> RegDeal | None:
    """Merge a cluster of parsed filings into one deal.

    Returns None if no member parsed successfully.
    """
    parsed_members = [m for m in members if m.parsed is not None]
    if not parsed_members:
        return None

    cik = parsed_members[0].cik
    deal = RegDeal(cik=cik, symbol=symbol)
    deal.filenames = [m.filename for m in members]
    deal.forms = [m.form_type for m in members]

    # Timeline
    deal.announce_date = min(
        m.filing_date for m in parsed_members
    )
    finals = [
        m for m in parsed_members
        if not m.parsed.is_preliminary
    ]
    if finals:
        deal.price_date = min(
            m.filing_date for m in finals
        )
    else:
        deal.price_date = deal.announce_date

    # Prefer final for pricing fields; preliminary
    # for reference fields. Within each preference
    # group, take the first non-empty.
    finals_first = sorted(
        parsed_members,
        key=lambda m: (m.parsed.is_preliminary, m.filing_date),
    )
    prelims_first = sorted(
        parsed_members,
        key=lambda m: (not m.parsed.is_preliminary, m.filing_date),
    )

    deal.offer_price = _first_nonzero(
        finals_first, 'offer_price'
    )
    deal.total = _first_nonzero(
        finals_first, 'total'
    )
    deal.shares_offered = _first_nonzero(
        finals_first, 'shares_offered'
    )
    deal.issuer_shares = _first_nonzero(
        finals_first, 'issuer_shares'
    )
    deal.ssh_shares = _first_nonzero(
        finals_first, 'ssh_shares'
    )
    deal.last_price = _first_nonzero(
        prelims_first, 'last_price'
    )
    deal.last_price_date = _first_nonempty(
        prelims_first, 'last_price_date'
    )

    deal.ticker = _first_nonempty(
        finals_first, 'ticker'
    ) or symbol
    deal.exchange = _first_nonempty(
        finals_first, 'exchange'
    )
    deal.issuer_name = _first_nonempty(
        finals_first, 'issuer_name'
    )
    deal.underwriter = _first_nonempty(
        finals_first, 'underwriter'
    )

    deal.is_bought = any(
        m.parsed.is_bought for m in parsed_members
    )
    deal.is_ipo = any(
        m.parsed.is_ipo for m in parsed_members
    )
    deal.has_selling_stockholder = any(
        m.parsed.has_selling_stockholder
        for m in parsed_members
    )
    deal.lockup = any(
        m.parsed.lockup for m in parsed_members
    )
    deal.lockup_days = max(
        (m.parsed.lockup_days for m in parsed_members),
        default=0,
    )

    return deal


def _first_nonzero(members: list[ClusterMember], attr: str):
    for m in members:
        v = getattr(m.parsed, attr, 0)
        if v:
            return v
    return type(getattr(members[0].parsed, attr))()


def _first_nonempty(members: list[ClusterMember], attr: str) -> str:
    for m in members:
        v = getattr(m.parsed, attr, '')
        if v:
            return v
    return ''
