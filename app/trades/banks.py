"""Bank-name normalization for underwriter strings.

Maps raw underwriter strings to a canonical key using
keyword-substring matching. Each bank has a small list
of distinguishing keywords (lowercased); a raw string
matches a bank if any keyword appears as a substring.

Public banks use their ticker symbol as the canonical
key (GS, JPM, BAC, ...). Private firms use a short
code: first main word for firm-named houses (Cantor,
Leerink), initials for personal names (WB, SB).

Anything that doesn't match any known keyword is
bucketed as OTHER — boutique/foreign firms below our
block-trade thresholds aren't tracked individually.

    parse_banks(raw)  — extract canonical keys
"""

OTHER = 'Other'

# Canonical key → distinguishing keywords (lowercase).
# Keywords must not collide with another bank's. When a
# bank has multiple obvious spellings (J.P./JP/JPMorgan),
# list them all rather than relying on a single keyword.
BANKS: dict[str, list[str]] = {
    # Bulge / public — ticker as primary key
    'GS':   ['goldman'],
    'JPM':  ['j.p. morgan', 'jp morgan', 'j.p morgan', 'jpmorgan'],
    'MS':   ['morgan stanley'],
    'BAC':  ['bofa', 'bank of america', 'merrill'],
    'C':    ['citigroup', 'citibank'],
    'BCS':  ['barclays'],
    'UBS':  ['ubs'],
    'JEF':  ['jefferies'],
    'RBC':  ['rbc capital', 'rbc '],
    'WFC':  ['wells fargo'],
    'SF':   ['stifel'],
    'SCHW': ['charles schwab', 'schwab'],
    'PIPR': ['piper sandler'],
    'RJF':  ['raymond james'],
    'CF':   ['canaccord'],
    'TD':   ['td securities', 'td cowen', 'cowen'],
    'TFC':  ['truist'],
    'DB':   ['deutsche bank'],
    'BMO':  ['bmo capital', 'bmo nesbitt'],
    'OPY':  ['oppenheimer'],
    'EVR':  ['evercore'],
    'KEY':  ['keybanc'],
    'NMR':  ['nomura'],
    'MFG':  ['mizuho'],
    'MC':   ['moelis'],
    'NTRS': ['northern trust'],
    'IBKR': ['interactive brokers'],
    'RILY': ['b. riley', 'b riley'],
    'BNP':  ['bnp paribas'],
    # Private — first main word or initials
    'Fidelity':   ['fidelity'],
    'BTIG':       ['btig'],
    'Cantor':     ['cantor'],
    'Leerink':    ['leerink'],
    'WB':         ['william blair'],
    'Needham':    ['needham'],
    'Guggenheim': ['guggenheim'],
    'Baird':      ['baird'],
    'Stephens':   ['stephens'],
    'SB':         ['bernstein'],
}


def parse_banks(raw: str) -> list[str]:
    """Extract canonical bank keys from a raw underwriter
    string, ordered by where each bank appears in raw.
    Returns [OTHER] if no known keyword matches."""
    if not raw or not raw.strip():
        return []
    raw_lower = raw.lower()
    matches: list[tuple[int, str]] = []
    for canon, keywords in BANKS.items():
        earliest = -1
        for kw in keywords:
            i = raw_lower.find(kw)
            if i >= 0 and (earliest < 0 or i < earliest):
                earliest = i
        if earliest >= 0:
            matches.append((earliest, canon))
    if not matches:
        return [OTHER]
    matches.sort()
    return [c for _, c in matches]
