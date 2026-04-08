"""Pydantic response models for the API."""

from pydantic import BaseModel


class HolderRow(BaseModel):
    name: str
    form_type: str
    date: str
    shares_mm: float
    pct_out: float
    chg_mm: float
    tag: str


class SymbolMeta(BaseModel):
    symbol: str
    name: str
    mkt_cap_b: float
    price: float
    quarter: str


class HoldersSummary(BaseModel):
    total_holders: int
    total_mm: float
    total_pct: float
    total_chg_mm: float


class HoldersResponse(BaseModel):
    meta: SymbolMeta
    holders: list[HolderRow]
    adds: list[HolderRow]
    subs: list[HolderRow]
    summary: HoldersSummary
