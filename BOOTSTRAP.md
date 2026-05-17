# Block-Trade Bootstrap

A curated dataset of historical equity block trades, assembled from SEC
filings and reconciled against an external bootstrap (the legacy
`block_trades.20260321.json` file). The output is
`data/bootstrap/block_golden_bootstrap.YYYYMMDD.json` — used to seed the
backtest framework in `symtools`.

## Output schema

One row per priced deal. Unique key: `(price_date, ticker, offer_price)`.

| Field | Description |
| --- | --- |
| `ticker` | Symbol at the time of the deal (may now be delisted/renamed) |
| `cik` | Issuer CIK, unpadded |
| `type` | `Reg` (registered, prospectus supplement filed) or `Unreg` (block by an affiliate, no prospectus) |
| `price_date` | ISO date the deal was priced / publicly announced |
| `trade_date` | ISO date the trade executed (= `price_date` when `intraday=true`; next weekday otherwise) |
| `intraday` | `true` if announced during market hours (same-day execution) |
| `offer_price` | **As-filed** public offering price per share (gross / reoffer price) |
| `shares` | **As-filed** share count |
| `notional` | `shares * offer_price` |
| `split_factor` | Cumulative split factor from `price_date` to today (>1 forward, <1 reverse, =1 none) |
| `adj_price` | `offer_price / split_factor` — **what backtests should use** |
| `adj_shares` | `shares * split_factor` — **what backtests should use** |
| `seller` | Selling entity name |
| `relationship` | `selling stockholder` \| `company` \| `affiliate` \| `insider` |
| `banks` | Canonical bank codes, **lead-left first** (`GS`, `MS`, `JPM`, `BAC`, `RBC`, `BMO`, `BCS`, …) |
| `evidence` | How the row was assembled — see provenance below |
| `source` | Bootstrap file the row came from |

### Backtest convention

Use `adj_price` and `adj_shares`. They're already split-adjusted to today's
basis, so cross-deal comparisons work without an adjustment step.
`offer_price` and `shares` stay on the row as the as-filed audit trail.

## Provenance (the `evidence` field)

| Value | Meaning |
| --- | --- |
| `golden+parser` | Reg deal whose cover page had a traditional pricing table; gross price extracted directly. |
| `golden+parser+override` | Reg block deal — the cover only states the net to seller. Gross sourced from `block_deals_for_offerpx.csv` (manual entry, cross-checked against legacy). |
| `both` | Unreg deal with both a Form 144 (intent to sell) and a Form 4 (executed sale) in window. |
| `144` | Unreg deal with only a Form 144 (seller below the Section 16 reporting threshold). |
| `form4` | Unreg deal with a Form 4 but no Form 144. |
| `none` | Unreg golden anchor — we know the deal happened (from the bootstrap CSV) but no SEC filing was located. Kept so the key is present; size/seller may be empty. |
| `legacy_bootstrap` | Foreign issuer with no SEC filing path (GFL, TMUS, CIGI, BNTX, …). Seeded straight from `legacy_seed.csv`. |

## Input bootstraps (you edit these)

All under `data/bootstrap/`:

| File | Used for | Format |
| --- | --- | --- |
| `regs_golden.YYYYMMDD.json` | Reg-deal anchor list | `[{"Ticker", "Type": "Reg", "PriceDt": "D-Mmm-YYYY"}, …]` |
| `unreg.csv` | Unreg-deal anchor list | Year-grouped columns: `date, symbol, intraday, offerpx` × 3 |
| `block_deals_for_offerpx.csv` | Manual gross OfferPx for bought-deal blocks | `Ticker, PriceDt, net_parsed, OfferPx` |
| `legacy_seed.csv` | Foreign-issuer deals with no SEC filings | `Ticker, PxDt, TradeDt, Type, OfferPx, Shares, LeftBank, …` |

The `block_trades.20260321.json` legacy file is the external reference used
for reconciliation and as a source for many of the values in the seed CSVs.

## Pipeline

```
unreg.csv ──build_unreg_golden──▶ unreg_golden.YYYYMMDD.json ──┐
                                                                │
regs_golden.YYYYMMDD.json ─────────────────────────────────────▶│
                                                                │
                          ┌──build_reg_corpus──▶ reg_corpus +   │
                          │                       reg_labels    │
                          ▼                       (parquet)     │
       SEC indices, 424B/144/Form 4 cached filings              │
                          │                                     │
                          └──app/parsers + app/mds──────────────┤
                                                                │
block_deals_for_offerpx.csv ──────────────────(override)───────▶│
                                                                │
legacy_seed.csv ─────────────────(direct seed)──────────────────┤
                                                                ▼
                                              tools/seed_goldens.py
                                                        │
                                                        ▼
                                         data/trades/{trades,blocks}.parquet
                                                        │
                                                        ▼
                                          tools/export_block_golden.py
                                                        │
                                                        ▼
                            data/bootstrap/block_golden_bootstrap.YYYYMMDD.json
```

## Regenerating from scratch

```bash
# 1. Build/refresh universe + adjustments
uv run python -m app refs           # mkt cap + price snapshot
uv run python -c "from app.mds.massive.splits import load_splits; load_splits('AAPL')"

# 2. Build the labeled reg corpus (after editing regs_golden)
uv run python tools/build_reg_corpus.py

# 3. Build the unreg golden JSON (after editing unreg.csv)
uv run python tools/build_unreg_golden.py

# 4. Seed trades + blocks
uv run python tools/seed_goldens.py

# 5. Export
uv run python tools/export_block_golden.py
```

`seed_goldens.py` is idempotent — it wipes any existing row that collides
on the primary key. Delete `data/trades/{trades,blocks}.parquet` first if
you want a clean rebuild.

## Schema / module map

| Module | Role |
| --- | --- |
| `app/trades/table.py` | `trades.parquet` schema + upsert |
| `app/trades/blocks.py` | `blocks.parquet` schema (= trades + status) |
| `app/trades/banks.py` | Underwriter-name → canonical bank-code mapping |
| `app/parsers/reg.py` | Shared 424B cover-page extractors (shares, price, ticker, exchange, underwriter, …) |
| `app/parsers/reg_424b{2,3,4,5,7}.py` | Thin per-form dispatchers over `parse_supplement` |
| `app/parsers/reg_deal.py` | Cluster resolver — merges prelim + final 424Bs into one `RegDeal` |
| `app/parsers/unreg.py` | `UnregDeal` resolver — aggregates 144 + Form 4 cluster |
| `app/mds/massive/splits.py` | Polygon `list_splits` cache + `cumulative_factor(symbol, since_date)` |
| `app/mds/syms.py` | `resolve_cik(symbol)` — active + inactive Polygon tickers, used for historical ticker resolution |

## Reconciliation report

`tools/compare_old_blocks.py` joins the current `blocks.parquet` against
`block_trades.20260321.json` on `(price_date, symbol)` and reports field
agreement. Current state:

```
matched on (price_date, symbol): 382 / 491

offer_price agreement (adj_price vs legacy OfferPx)
  within 0.1%:  372 (97.4%)
  within 1%:      3 ( 0.8%)
  within 5%:      4 ( 1.0%)
  mismatch >5%:   3 ( 0.8%)  — known: CWAN (legacy error), WDC (SanDisk spinoff,
                                       not modeled), ENVX (7-for-8 split residual)

banks (reg-only on 148 legacy rows with LeftBank)
  match:        144 (97.3%)
  empty:          3  (covers without a bookrunner section)
  disagreement:   1  (CWAN — legacy wrong)
```

`legacy only` rows (108) are dominated by deals below your $100M cutoff,
plus a few parse-failures (BECN, ASPI, AKR, RVNC) and a handful of
unresolvable typos (CRVV, CRWW, OWAN, ALHG, FVRO).
