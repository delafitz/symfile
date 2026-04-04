"""Date utilities."""

from datetime import date, timedelta


def quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1


def prev_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def weekdays(
    start: date, end: date
) -> list[date]:
    """Weekdays from start to end inclusive."""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def quarters(
    start: date, end: date
) -> list[tuple[int, int]]:
    """(year, quarter) tuples covering range."""
    qtrs = []
    y, q = start.year, quarter(start)
    ey, eq = end.year, quarter(end)
    while (y, q) <= (ey, eq):
        qtrs.append((y, q))
        q += 1
        if q > 4:
            q = 1
            y += 1
    return qtrs
