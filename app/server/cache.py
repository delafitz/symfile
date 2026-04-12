"""Shared application state — loaded once at startup."""

import polars as pl

from app.holdings.build import load_effective
from app.mds.syms import load_cusips, load_syms
from app.trades.table import load_trades
from app.util.log import log

QUARTERS = [(2025, 3), (2025, 4)]


class Cache:
    def __init__(self) -> None:
        self.syms: dict | None = None
        self.cusips: dict | None = None
        self.prev: pl.DataFrame | None = None
        self.curr: pl.DataFrame | None = None
        self.trades: pl.DataFrame | None = None

    async def startup(self) -> None:
        log.info('cache startup')
        self.syms = load_syms()
        self.cusips = load_cusips()
        py, pq = QUARTERS[0]
        cy, cq = QUARTERS[1]
        self.prev = load_effective(py, pq)
        self.curr = load_effective(cy, cq)
        self.trades = load_trades()
        log.info(
            'cache ready',
            symbols=len(self.syms),
            cusips=len(self.cusips),
            curr_holdings=self.curr.height,
            trades=self.trades.height,
        )
