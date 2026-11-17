---
title: Dealer Axes in BQL
description: Dealer axes (IMGR AXE, QMGR AXE) on AT1 bonds with BQL in polars-bloomberg - axes per bond and per dealer, large axes, net axes per issuer, today versus the last 1 and 5 weeks, axes in a relative-value screen and the top dealers.
tags:
    - BQL
    - Credit
    - AT1
    - Axes
---

# Dealer Axes

An axe is a dealer's signal that it wants to buy (bid) or sell (ask) a bond. BQL
returns the axes you see in `IMGR AXE <GO>` and `QMGR AXE <GO>`, so you can rank,
join and chart them in Python.

| You want | Use | One row per |
|---|---|---|
| Is the bond axed, by how many dealers, total size per side | `axes()` | bond |
| Each dealer's axe: dealer, size, price, spread, yield | `axes_by_dealer()` | dealer and bond |
| Axe size over the last week or five weeks | `axes(calc_interval=1w)` or `5w`, columns `BID_HIST_AGG_SIZE`, `ASK_HIST_AGG_SIZE` | bond |
| Only bonds that have live axes | universe `axedUniv('all')` (or `'bid'`, `'ask'`), filter `is_axed()` | bond |

Every recipe uses the same universe: the USD AT1 bonds of Swedish banks, a handful of
SEB, Handelsbanken and Swedbank perpetuals. For other bonds, change the `for()`
clause. Sizes are face amounts in the bond's currency.

The outputs below are live data as of 2026-11-17 and will differ when you run the
queries.

## AT1 axes by bond

Which bonds are axed, by how many dealers, and for how much in total.

```python
from polars_bloomberg import BQuery

query = """
get(name, axes() as #ax)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df)
```
Output:
```python
shape: (8, 7)
┌───────────────┬─────────────────┬─────┬───────────┬───────────┬────────────────┬────────────────┐
│ ID            ┆ name            ┆ #ax ┆ ASK_DEPTH ┆ BID_DEPTH ┆ ASK_TOTAL_SIZE ┆ BID_TOTAL_SIZE │
│ ---           ┆ ---             ┆ --- ┆ ---       ┆ ---       ┆ ---            ┆ ---            │
│ str           ┆ str             ┆ str ┆ i64       ┆ i64       ┆ f64            ┆ f64            │
╞═══════════════╪═════════════════╪═════╪═══════════╪═══════════╪════════════════╪════════════════╡
│ BR069680 Corp ┆ SWEDA 4 PERP    ┆ Y   ┆ null      ┆ 9         ┆ null           ┆ 2e7            │
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ Y   ┆ 1         ┆ 6         ┆ 2e6            ┆ 3.3e7          │
│ DL161992 Corp ┆ SEB 6 ¾ PERP    ┆ Y   ┆ 5         ┆ null      ┆ 1.22e7         ┆ null           │
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ Y   ┆ 1         ┆ 1         ┆ 5e6            ┆ 1e6            │
│ ZF859199 Corp ┆ SWEDA 7 ¾ PERP  ┆ Y   ┆ 1         ┆ 7         ┆ 1e6            ┆ 3.1e7          │
│ ZL122341 Corp ┆ SWEDA 7 ⅝ PERP  ┆ Y   ┆ 2         ┆ 1         ┆ 8e6            ┆ 3e6            │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ Y   ┆ 2         ┆ 8         ┆ 7e6            ┆ 4.9e7          │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ Y   ┆ 5         ┆ 1         ┆ 7.6e6          ┆ 200000.0       │
└───────────────┴─────────────────┴─────┴───────────┴───────────┴────────────────┴────────────────┘
```

Notes:

- `#ax` is `Y` when the bond has at least one live axe.
- `BID_DEPTH` is the number of dealers bidding and `BID_TOTAL_SIZE` the sum of their
  sizes; `ASK_*` is the same for the offer side. A null side has no axes.
- `preferences(addcols=all)` adds more columns, among them one dealer's price,
  spread and yield, and the 1-week `*_HIST_AGG_SIZE`.
- To start from axed bonds only, use `axedUniv('all')` instead of
  `bondsUniv(ACTIVE)`.

## Large axes only

Dealer axes of 10 million or more, filtered by Bloomberg before the data is sent.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#axe = axes_by_dealer();
    #large = dropna(matches(#axe, #axe().bid_size >= 10M or #axe().ask_size >= 10M),
                    remove_id=True);)
get(name, #large)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID", how="inner")
print(df.select(
    "name",
    dealer=pl.coalesce("BID_DEALER", "ASK_DEALER"),
    bid_mm=pl.col("BID_SIZE") / 1e6,
    ask_mm=pl.col("ASK_SIZE") / 1e6,
    bid_px="BID_PRICE",
    ask_px="ASK_PRICE",
).sort("name", "dealer"))
```
Output:
```python
shape: (4, 6)
┌─────────────────┬────────┬────────┬────────┬─────────┬────────┐
│ name            ┆ dealer ┆ bid_mm ┆ ask_mm ┆ bid_px  ┆ ask_px │
│ ---             ┆ ---    ┆ ---    ┆ ---    ┆ ---     ┆ ---    │
│ str             ┆ str    ┆ f64    ┆ f64    ┆ f64     ┆ f64    │
╞═════════════════╪════════╪════════╪════════╪═════════╪════════╡
│ SEB 6 ⅞ PERP    ┆ HSBC   ┆ 15.0   ┆ null   ┆ 100.25  ┆ null   │
│ SHBASS 4 ⅜ PERP ┆ HSBC   ┆ 15.0   ┆ null   ┆ 99.5    ┆ null   │
│ SHBASS 4 ⅜ PERP ┆ ING    ┆ 10.0   ┆ null   ┆ 99.35   ┆ null   │
│ SWEDA 7 ¾ PERP  ┆ MS     ┆ 10.0   ┆ null   ┆ 101.875 ┆ null   │
└─────────────────┴────────┴────────┴────────┴─────────┴────────┘
```

Notes:

- `axes_by_dealer()` has one row per dealer and bond. Most dealers are axed on one
  side only, so the other side is null.
- `matches()` keeps the rows that pass the test and `dropna(..., remove_id=True)`
  removes the others. `how="inner"` then keeps only the bonds with a large axe.
- `10M` is BQL shorthand for 10,000,000, and `K` stands for thousands.
- Compare dealers on price or yield. A dealer's spread can be `0` or quoted on
  another basis.

## Net axes per issuer

Are dealers buyers or sellers of each issuer's AT1s? A bid axe is a dealer that
wants to buy, an ask axe a dealer that wants to sell.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(sum(group(axes().bid_total_size, ticker)) / 1M as #bid,
    sum(group(axes().ask_total_size, ticker)) / 1M as #ask)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
net = pl.col("#bid").fill_null(0) - pl.col("#ask").fill_null(0)
print(df.select(
    issuer="ID",
    bid_mm="#bid",
    ask_mm="#ask",
    net_mm=net,
    dealers=pl.when(net > 0).then(pl.lit("buyers")).otherwise(pl.lit("sellers")),
))
```
Output:
```python
shape: (3, 5)
┌────────┬────────┬────────┬────────┬─────────┐
│ issuer ┆ bid_mm ┆ ask_mm ┆ net_mm ┆ dealers │
│ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---     │
│ str    ┆ f64    ┆ f64    ┆ f64    ┆ str     │
╞════════╪════════╪════════╪════════╪═════════╡
│ SEB    ┆ 34.0   ┆ 19.2   ┆ 14.8   ┆ buyers  │
│ SHBASS ┆ 49.2   ┆ 14.6   ┆ 34.6   ┆ buyers  │
│ SWEDA  ┆ 54.0   ┆ 9.0    ┆ 45.0   ┆ buyers  │
└────────┴────────┴────────┴────────┴─────────┘
```

Notes:

- With `group(..., ticker)` the `ID` column holds the ticker, not a bond.
- `/ 1M` turns the sizes into millions. Sizes add up in each bond's own currency; for
  bonds in several currencies, add `with(currency=USD)`.

## Total axes: today, 1 week, 5 weeks

All bid and ask axes on the universe, now and over the last one and five weeks.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(sum(group(axes().bid_total_size)) / 1M as #bid_today,
    sum(group(axes().ask_total_size)) / 1M as #ask_today,
    sum(group(axes(calc_interval=1w).bid_hist_agg_size)) / 1M as #bid_1w,
    sum(group(axes(calc_interval=1w).ask_hist_agg_size)) / 1M as #ask_1w,
    sum(group(axes(calc_interval=5w).bid_hist_agg_size)) / 1M as #bid_5w,
    sum(group(axes(calc_interval=5w).ask_hist_agg_size)) / 1M as #ask_5w)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    row = bq.bql(query).combine(on="ID").row(0, named=True)
windows = ["today", "1w", "5w"]
df = pl.DataFrame({
    "window": windows,
    "bid_mm": [row[f"#bid_{w}"] for w in windows],
    "ask_mm": [row[f"#ask_{w}"] for w in windows],
}).with_columns(net_mm=pl.col("bid_mm") - pl.col("ask_mm"))
print(df)
```
Output:
```python
shape: (3, 4)
┌────────┬─────────┬────────┬────────┐
│ window ┆ bid_mm  ┆ ask_mm ┆ net_mm │
│ ---    ┆ ---     ┆ ---    ┆ ---    │
│ str    ┆ f64     ┆ f64    ┆ f64    │
╞════════╪═════════╪════════╪════════╡
│ today  ┆ 137.2   ┆ 42.8   ┆ 94.4   │
│ 1w     ┆ 177.44  ┆ 58.76  ┆ 118.68 │
│ 5w     ┆ 143.756 ┆ 73.648 ┆ 70.108 │
└────────┴─────────┴────────┴────────┘
```

Notes:

- `group()` without a key puts every bond in one group, whose `ID` is
  `IdentityGroup`.
- `*_HIST_AGG_SIZE` is on the same scale as today's `*_TOTAL_SIZE` (the 5-week value
  is not five times the 1-week value), so the rows compare directly.
- BQL has no day-by-day axe history: `calc_interval` takes only `1w` and `5w`,
  `axes(dates=...)` is rejected and `with(dates=...)` has no effect. For a daily
  series, store the output of the first recipe every day.

## Per bond: today vs 1 and 5 weeks

Is today's axe size normal for each bond, or has one side dried up?

```python
from polars_bloomberg import BQuery

query = """
get(name,
    axes().bid_total_size / 1M as #bid,
    axes(calc_interval=1w).bid_hist_agg_size / 1M as #bid_1w,
    axes(calc_interval=5w).bid_hist_agg_size / 1M as #bid_5w,
    axes().ask_total_size / 1M as #ask,
    axes(calc_interval=1w).ask_hist_agg_size / 1M as #ask_1w,
    axes(calc_interval=5w).ask_hist_agg_size / 1M as #ask_5w)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df.drop("ID"))
```
Output:
```python
shape: (8, 7)
┌─────────────────┬──────┬─────────┬─────────┬──────┬─────────┬─────────┐
│ name            ┆ #bid ┆ #bid_1w ┆ #bid_5w ┆ #ask ┆ #ask_1w ┆ #ask_5w │
│ ---             ┆ ---  ┆ ---     ┆ ---     ┆ ---  ┆ ---     ┆ ---     │
│ str             ┆ f64  ┆ f64     ┆ f64     ┆ f64  ┆ f64     ┆ f64     │
╞═════════════════╪══════╪═════════╪═════════╪══════╪═════════╪═════════╡
│ SWEDA 4 PERP    ┆ 20.0 ┆ 23.76   ┆ 16.608  ┆ null ┆ null    ┆ null    │
│ SEB 6 ⅞ PERP    ┆ 33.0 ┆ 38.52   ┆ 26.768  ┆ 2.0  ┆ 1.0     ┆ 8.248   │
│ SEB 6 ¾ PERP    ┆ null ┆ null    ┆ null    ┆ 12.2 ┆ 14.68   ┆ 20.008  │
│ SEB 6 ¾ PERP    ┆ 1.0  ┆ 16.6    ┆ 7.408   ┆ 5.0  ┆ 6.12    ┆ 9.296   │
│ SWEDA 7 ¾ PERP  ┆ 31.0 ┆ 35.0    ┆ 27.14   ┆ 1.0  ┆ 0.6     ┆ 0.312   │
│ SWEDA 7 ⅝ PERP  ┆ 3.0  ┆ 8.32    ┆ 3.64    ┆ 8.0  ┆ 10.32   ┆ 7.232   │
│ SHBASS 4 ⅜ PERP ┆ 49.0 ┆ 52.44   ┆ 59.792  ┆ 7.0  ┆ 10.8    ┆ 4.04    │
│ SHBASS 4 ¾ PERP ┆ 0.2  ┆ 2.8     ┆ 2.4     ┆ 7.6  ┆ 15.24   ┆ 24.512  │
└─────────────────┴──────┴─────────┴─────────┴──────┴─────────┴─────────┘
```

Notes:

- A bid well below its 5-week level means fewer dealers want the bond than usual.
- A null means no axes on that side today, or over the whole window.

## Axes in a relative-value screen

Spreads next to axes: can you buy the cheap bonds, and sell the rich ones?

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(name, nxt_call_dt,
    flt_spread().value as #back_end,
    spread(spread_type=OAS).value as #oas,
    axes() as #ax)
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df.select(
    "name",
    call="nxt_call_dt",
    back_end="#back_end",
    oas=pl.col("#oas").round(0),
    bid_mm=pl.col("BID_TOTAL_SIZE") / 1e6,
    ask_mm=pl.col("ASK_TOTAL_SIZE") / 1e6,
).sort("oas", descending=True))
```
Output:
```python
shape: (8, 6)
┌─────────────────┬────────────┬──────────┬───────┬────────┬────────┐
│ name            ┆ call       ┆ back_end ┆ oas   ┆ bid_mm ┆ ask_mm │
│ ---             ┆ ---        ┆ ---      ┆ ---   ┆ ---    ┆ ---    │
│ str             ┆ date       ┆ f64      ┆ f64   ┆ f64    ┆ f64    │
╞═════════════════╪════════════╪══════════╪═══════╪════════╪════════╡
│ SEB 6 ¾ PERP    ┆ 2034-06-02 ┆ 285.0    ┆ 283.0 ┆ null   ┆ 12.2   │
│ SEB 6 ¾ PERP    ┆ 2031-11-04 ┆ 312.7    ┆ 233.0 ┆ 1.0    ┆ 5.0    │
│ SWEDA 7 ¾ PERP  ┆ 2030-03-17 ┆ 365.7    ┆ 229.0 ┆ 31.0   ┆ 1.0    │
│ SHBASS 4 ¾ PERP ┆ 2031-03-01 ┆ 404.8    ┆ 228.0 ┆ 0.2    ┆ 7.6    │
│ SWEDA 4 PERP    ┆ 2029-03-17 ┆ 286.4    ┆ 198.0 ┆ 20.0   ┆ null   │
│ SEB 6 ⅞ PERP    ┆ 2027-06-30 ┆ 407.3    ┆ 196.0 ┆ 33.0   ┆ 2.0    │
│ SWEDA 7 ⅝ PERP  ┆ 2028-03-17 ┆ 358.9    ┆ 186.0 ┆ 3.0    ┆ 8.0    │
│ SHBASS 4 ⅜ PERP ┆ 2027-03-01 ┆ 394.2    ┆ 155.0 ┆ 49.0   ┆ 7.0    │
└─────────────────┴────────────┴──────────┴───────┴────────┴────────┘
```

Notes:

- `flt_spread()` is the back end: the margin the coupon resets to if the bond is not
  called. `#oas` is the option-adjusted spread.
- A wide OAS with ask axes is a cheap bond that dealers offer; a tight OAS with bid
  axes is a rich bond that dealers want to buy.
- Add more fields (ratings, CET1, returns) to `get()` in the same way.
  `combine(on="ID")` joins every item on the bond.

## Top 3 dealers

The dealers with the most axes on these bonds, bid and ask together.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(axes_by_dealer())
for(filter(bondsUniv(ACTIVE),
        crncy() == 'USD' and country_iso() == 'SE' and
        basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query)[0]
top = (
    df.group_by(dealer=pl.coalesce("BID_DEALER", "ASK_DEALER"))
    .agg(
        bonds=pl.col("ID").n_unique(),
        bid_mm=pl.col("BID_SIZE").sum() / 1e6,
        ask_mm=pl.col("ASK_SIZE").sum() / 1e6,
    )
    .with_columns(total_mm=pl.col("bid_mm") + pl.col("ask_mm"))
    .sort(["total_mm", "dealer"], descending=[True, False])
    .head(3)
)
print(top)
```
Output:
```python
shape: (3, 5)
┌────────┬───────┬────────┬────────┬──────────┐
│ dealer ┆ bonds ┆ bid_mm ┆ ask_mm ┆ total_mm │
│ ---    ┆ ---   ┆ ---    ┆ ---    ┆ ---      │
│ str    ┆ u32   ┆ f64    ┆ f64    ┆ f64      │
╞════════╪═══════╪════════╪════════╪══════════╡
│ HSBC   ┆ 4     ┆ 39.0   ┆ 0.0    ┆ 39.0     │
│ GS     ┆ 6     ┆ 10.4   ┆ 12.2   ┆ 22.6     │
│ MS     ┆ 6     ┆ 16.0   ┆ 5.0    ┆ 21.0     │
└────────┴───────┴────────┴────────┴──────────┘
```

Notes:

- A dealer axed on both sides of a bond has its bid and ask on one row, so
  `coalesce()` gives one dealer per row.
- `preferences(addcols=all)` adds `BID_TIMESTAMP` and `ASK_TIMESTAMP` as UTC
  `Datetime` columns, useful for dropping axes that were not updated today.

See also `IMGR AXE <GO>`, `QMGR AXE <GO>` and `BQLX <GO>`. For how `bql()` and
`combine()` work, see [BQL](../usage/bql.md).
