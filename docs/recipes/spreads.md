---
title: Spread Statistics in BQL
description: Spread statistics with BQL in polars-bloomberg - count, quartiles and median OAS per credit rating for AT1 (CoCo) and other bonds, monthly median OAS history, the daily average Z-spread of index members, index yield to worst vs OAS, and YAS custom analytics - Z-spread to call, to maturity or to a custom workout date at any price, and the discount margin of floating rate notes.
tags:
    - BQL
    - Credit
    - AT1
    - Spreads
    - YAS
---

# Spread Statistics

How wide are spreads for each rating? How much more does an AT1 pay than a bond
with the same rating? Is the market wider than last month? And what spread does a
bond have at the price you are about to pay?
{ .recipe-lead }

BQL answers the first three for a whole index in one request: it groups the bonds'
spreads and sends back only the statistics. The last one is the YAS calculator
(`YAS <GO>`), which BQL runs at any price and workout you give it.

!!! info "Data on this page"
    Spreads are in basis points, yields in percent. The outputs below are live data
    as of 2026-11-17 and will differ when you run the queries. Before you adapt a
    query, read the [Pitfalls](#pitfalls).

## Quick reference

=== "By task"

    | You want | Use | One row per |
    |---|---|---|
    | Count, median and quartiles of spreads per rating | `count()`, `median()`, `quantile(..., 0.75)` of `group(spread(...), by=[bb_composite(), ...])` | group |
    | The same statistics month by month | `spread(dates=range(-6M, 0D), per=M)`, grouped by `year()` and `month()` of its `DATE` | group and month |
    | One number per day for all bonds of an index | `avg(group(#s, by=#s().DATE))` | day |
    | An index's own yield and spread | `yield(yield_type=YTW, universe_type=PROJECTED)`, `spread(spread_type=OAS_SOVEREIGN, universe_type=RETURNS)` | date |
    | Spread or yield at your price, to call, maturity or a date | `spread(price=..., workout=TO_NEXT_CALL)`, `TO_MATURITY`, `TO_WORST`, `TO_CUSTOM` | bond |
    | Discount margin of a floating rate note | `discount_margin(price=...)` | bond |

=== "Spread types"

    | `spread_type=` | Spread | Works for |
    |---|---|---|
    | `Z` (the default) | Z-spread: constant spread over the swap zero curve | bonds, indices |
    | `OAS` | option-adjusted spread | bonds |
    | `G`, `I`, `ASW` | over government yields, over swap rates, asset swap | bonds |
    | `OAS_SOVEREIGN`, `OAS_SWAP` | index OAS over government or swap curve | indices |

!!! warning "Name the spread type"
    `spread()` without `spread_type` is the Z-spread, not the OAS, so every recipe
    names the type.

## OAS by rating: AT1 vs other bonds

`median()`, `quantile()`, `group(..., by=[bb_composite(), ...])`
{ .recipe-uses }

Count, median and upper quartile OAS per rating from BBB+ to BB-, for AT1s (CoCos) and
for all other bonds of three European indices: Bloomberg Contingent Capital Western
Europe (`I31095`), Pan-European High Yield (`LP01STAT`) and Pan-European Aggregate
Corporate (`LP05STAT`).

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#is_coco = if(capital_contingent_security(), true, false);
    #oas = group(spread(spread_type=OAS), by=[bb_composite(), #is_coco]);)
get(count(#oas) as #n, median(#oas) as #median, quantile(#oas, 0.75) as #q3)
for(filter(members(['I31095 Index', 'LP01STAT Index', 'LP05STAT Index']),
           bb_composite() in ['BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-']))
preferences(dropCols=['ORIG_IDS'])
"""
ratings = pl.Enum(["BBB+", "BBB", "BBB-", "BB+", "BB", "BB-"])
with BQuery() as bq:
    df = bq.bql(query).combine()
stats = df.select(
    rating=pl.col("BB_COMPOSITE()").cast(ratings),
    at1="#IS_COCO",
    n="#n",
    median=pl.col("#median").round().cast(pl.Int64),
    q3=pl.col("#q3").round().cast(pl.Int64),
)
print(
    stats.filter(~pl.col("at1")).drop("at1")
    .join(stats.filter(pl.col("at1")).drop("at1"), on="rating", suffix="_at1")
    .with_columns(premium=pl.col("median_at1") - pl.col("median"))
    .sort("rating")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (6, 8)
┌────────┬──────┬────────┬─────┬───────┬────────────┬────────┬─────────┐
│ rating ┆ n    ┆ median ┆ q3  ┆ n_at1 ┆ median_at1 ┆ q3_at1 ┆ premium │
│ ---    ┆ ---  ┆ ---    ┆ --- ┆ ---   ┆ ---        ┆ ---    ┆ ---     │
│ enum   ┆ i64  ┆ i64    ┆ i64 ┆ i64   ┆ i64        ┆ i64    ┆ i64     │
╞════════╪══════╪════════╪═════╪═══════╪════════════╪════════╪═════════╡
│ BBB+   ┆ 1043 ┆ 96     ┆ 131 ┆ 3     ┆ 228        ┆ 256    ┆ 132     │
│ BBB    ┆ 695  ┆ 92     ┆ 122 ┆ 21    ┆ 229        ┆ 269    ┆ 137     │
│ BBB-   ┆ 385  ┆ 107    ┆ 147 ┆ 92    ┆ 259        ┆ 287    ┆ 152     │
│ BB+    ┆ 172  ┆ 129    ┆ 170 ┆ 44    ┆ 257        ┆ 288    ┆ 128     │
│ BB     ┆ 106  ┆ 149    ┆ 202 ┆ 37    ┆ 285        ┆ 325    ┆ 136     │
│ BB-    ┆ 102  ┆ 161    ┆ 247 ┆ 5     ┆ 252        ┆ 270    ┆ 91      │
└────────┴──────┴────────┴─────┴───────┴────────────┴────────┴─────────┘
```

!!! warning "Watch out"

    `if(capital_contingent_security(), true, false)` matters. The flag itself is
    null for most bonds that are not CoCos, their groups get a null key, and
    `combine()` cannot join them (see [Pitfalls](#pitfalls)).

Notes
{ .recipe-label }

- `n` is the number of bonds, `median` and `q3` the 50th and 75th percentile OAS
  of the group. Columns ending in `_at1` are the AT1s. `premium` is how much wider
  the median AT1 is than the median bond of the same rating.
- A group column is named after its expression in upper case: `BB_COMPOSITE()`,
  `#IS_COCO`. The `ID` column holds the whole group key, such as `BB+:true`.
- `bb_composite()` is today's rating. For a box plot, add `quantile(#oas, 0.25)`
  for the box and `quantile(#oas, 0.05)`, `quantile(#oas, 0.95)` for the whiskers.
- For the AT1 premium per rating over ten years, with charts, see the
  [AT1 vs Credits OAS per Rating](../examples/credit/at1-vs-credits-oas.ipynb)
  example.

## Median OAS by rating, month by month

`per=M`, `month()`, `year()`
{ .recipe-uses }

Median OAS at the last six month-ends and today, for the ratings around the line
between investment grade and high yield, in the Pan-European High Yield and
Aggregate Corporate indices. `crossover` is BB+ minus BBB-: the extra spread for
one notch below investment grade.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#oas = spread(spread_type=OAS, dates=range(-6M, 0D), per=M, fill=PREV);
    #year = year(#oas().DATE);
    #month = month(#oas().DATE);
    #median = median(group(#oas, by=[bb_composite(), #year, #month]));)
get(#median)
for(filter(members(['LP01STAT Index', 'LP05STAT Index']),
           bb_composite() in ['BBB', 'BBB-', 'BB+', 'BB']))
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.drop_nulls("DATE")
    .pivot(on="BB_COMPOSITE()", index="DATE", values="#median")
    .sort("DATE")
    .select(
        "DATE",
        pl.col("BBB", "BBB-", "BB+", "BB").round(),
        crossover=(pl.col("BB+") - pl.col("BBB-")).round(),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (7, 6)
┌────────────┬──────┬───────┬───────┬───────┬───────────┐
│ DATE       ┆ BBB  ┆ BBB-  ┆ BB+   ┆ BB    ┆ crossover │
│ ---        ┆ ---  ┆ ---   ┆ ---   ┆ ---   ┆ ---       │
│ date       ┆ f64  ┆ f64   ┆ f64   ┆ f64   ┆ f64       │
╞════════════╪══════╪═══════╪═══════╪═══════╪═══════════╡
│ 2026-05-29 ┆ 81.0 ┆ 98.0  ┆ 120.0 ┆ 151.0 ┆ 22.0      │
│ 2026-06-30 ┆ 81.0 ┆ 95.0  ┆ 121.0 ┆ 146.0 ┆ 25.0      │
│ 2026-07-31 ┆ 78.0 ┆ 91.0  ┆ 114.0 ┆ 129.0 ┆ 23.0      │
│ 2026-08-31 ┆ 77.0 ┆ 90.0  ┆ 105.0 ┆ 118.0 ┆ 15.0      │
│ 2026-09-30 ┆ 82.0 ┆ 94.0  ┆ 128.0 ┆ 149.0 ┆ 33.0      │
│ 2026-10-30 ┆ 93.0 ┆ 106.0 ┆ 145.0 ┆ 172.0 ┆ 39.0      │
│ 2026-11-17 ┆ 92.0 ┆ 107.0 ┆ 129.0 ┆ 149.0 ┆ 21.0      │
└────────────┴──────┴───────┴───────┴───────┴───────────┘
```

!!! warning "Watch out"

    - `per=M` gives the last business day of each month and the latest close.
      `frq=M` instead steps back from today in whole months: run it on the 17th and
      every row is the 17th.
    - `members()` is today's index. Bonds that have left the index are missing from the
      history, and new issues count only from their issue date. Add
      `count(group(...))` to see how many bonds each median uses.

Notes
{ .recipe-label }

- `year()` and `month()` of `#oas().DATE` put the period into the group key, so
  each rating gets one median per month.
- A bond without any spread history falls into a group whose date is null.
  `drop_nulls("DATE")` removes that row.
- `fill=PREV` carries the last spread forward over days without a price.
- For AT1 vs other bonds per rating since 2014, see the
  [AT1 vs Credits OAS per Rating](../examples/credit/at1-vs-credits-oas.ipynb)
  example.

## Daily average Z-spread of index members

`avg(group(#s, by=#s().DATE))`
{ .recipe-uses }

The average and median Z-spread of the bonds in the Bloomberg European Banks CoCo
Tier 1 index (`I31415`), for each business day of the last two weeks, with the
daily change of the average.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#z = spread(spread_type=Z, dates=range(-14D, 0D));
    #day = #z().DATE;
    #avg = avg(group(#z, by=#day));
    #median = median(group(#z, by=#day));
    #n = count(group(#z, by=#day));)
get(#avg, #median, #n)
for(members('I31415 Index'))
preferences(dropCols=['ORIG_IDS'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.filter(pl.col("#n") > 0)
    .sort("DATE")
    .select(
        "DATE",
        bonds="#n",
        avg=pl.col("#avg").round(1),
        median=pl.col("#median").round(1),
        change=pl.col("#avg").diff().round(1),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (10, 5)
┌────────────┬───────┬───────┬────────┬────────┐
│ DATE       ┆ bonds ┆ avg   ┆ median ┆ change │
│ ---        ┆ ---   ┆ ---   ┆ ---    ┆ ---    │
│ date       ┆ i64   ┆ f64   ┆ f64    ┆ f64    │
╞════════════╪═══════╪═══════╪════════╪════════╡
│ 2026-11-03 ┆ 232   ┆ 291.0 ┆ 284.6  ┆ null   │
│ 2026-11-04 ┆ 232   ┆ 287.0 ┆ 282.4  ┆ -4.0   │
│ 2026-11-05 ┆ 234   ┆ 288.1 ┆ 285.5  ┆ 1.1    │
│ 2026-11-06 ┆ 234   ┆ 288.8 ┆ 284.8  ┆ 0.7    │
│ 2026-11-09 ┆ 225   ┆ 274.2 ┆ 270.7  ┆ -14.6  │
│ 2026-11-10 ┆ 234   ┆ 263.1 ┆ 259.0  ┆ -11.1  │
│ 2026-11-11 ┆ 139   ┆ 261.1 ┆ 258.9  ┆ -2.0   │
│ 2026-11-12 ┆ 235   ┆ 259.4 ┆ 255.1  ┆ -1.7   │
│ 2026-11-13 ┆ 235   ┆ 265.0 ┆ 260.0  ┆ 5.6    │
│ 2026-11-16 ┆ 235   ┆ 270.5 ┆ 264.8  ┆ 5.4    │
└────────────┴───────┴───────┴────────┴────────┘
```

!!! warning "Watch out"

    - `bonds` is the number of bonds with a spread that day. Weekends, and today until
      the close is stored, have none and are dropped. On a holiday in one market fewer
      bonds are priced, and the average moves because the set of bonds changed; check
      `bonds` before you read `change` as a market move.
    - `preferences(dropCols=['ORIG_IDS'])` is needed here: `ORIG_IDS` is null in every
      row, and the three items would not join.

Notes
{ .recipe-label }

- `#z().DATE` is the date of each spread. Grouping by it gives one value per day
  for all bonds together.
- This is an equal-weighted average. The index's own spread is weighted by market
  value; the next recipe gets it.

## Index yield and spread, week by week

`universe_type`, `per=W`
{ .recipe-uses }

Yield to worst and OAS of the Bloomberg Contingent Capital EUR index (`I30904EU`)
at each Friday close for the last eight weeks. `rates` is the yield minus the
spread: what is left once the credit spread is taken out, mostly the government
bond yield. `spread_pct` is the spread's share of the yield, in percent.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#ytw = yield(yield_type=YTW, universe_type=PROJECTED);
    #oas = spread(spread_type=OAS_SOVEREIGN, universe_type=RETURNS);)
get(#ytw, #oas)
for(['I30904EU Index'])
with(dates=range(-8W, 0D), per=W, fill=PREV)
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
oas_pct = pl.col("#oas") / 100
print(
    df.select(
        "DATE",
        ytw=pl.col("#ytw").round(2),
        oas_bp=pl.col("#oas").round(),
        rates=(pl.col("#ytw") - oas_pct).round(2),
        spread_pct=(100 * oas_pct / pl.col("#ytw")).round(),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (9, 5)
┌────────────┬──────┬────────┬───────┬────────────┐
│ DATE       ┆ ytw  ┆ oas_bp ┆ rates ┆ spread_pct │
│ ---        ┆ ---  ┆ ---    ┆ ---   ┆ ---        │
│ date       ┆ f64  ┆ f64    ┆ f64   ┆ f64        │
╞════════════╪══════╪════════╪═══════╪════════════╡
│ 2026-09-25 ┆ 5.97 ┆ 248.0  ┆ 3.5   ┆ 41.0       │
│ 2026-10-02 ┆ 6.3  ┆ 297.0  ┆ 3.33  ┆ 47.0       │
│ 2026-10-09 ┆ 6.12 ┆ 279.0  ┆ 3.33  ┆ 46.0       │
│ 2026-10-16 ┆ 6.01 ┆ 274.0  ┆ 3.27  ┆ 46.0       │
│ 2026-10-23 ┆ 6.33 ┆ 303.0  ┆ 3.3   ┆ 48.0       │
│ 2026-10-30 ┆ 6.21 ┆ 296.0  ┆ 3.26  ┆ 48.0       │
│ 2026-11-06 ┆ 6.05 ┆ 287.0  ┆ 3.18  ┆ 47.0       │
│ 2026-11-13 ┆ 6.01 ┆ 270.0  ┆ 3.31  ┆ 45.0       │
│ 2026-11-17 ┆ 6.04 ┆ 273.0  ┆ 3.32  ┆ 45.0       │
└────────────┴──────┴────────┴───────┴────────────┘
```

!!! warning "Watch out"

    - Index analytics need `universe_type`; without it, `yield()` and `spread()`
      return null. `PROJECTED` is the index as it will be after the next rebalancing,
      `RETURNS` the index fixed for this month's returns. Yield to worst accepts only
      `PROJECTED`; spreads accept both.
    - `per=W` gives Friday closes, like weekly BDH data, and the latest close as the
      last row. `frq=W` would step back seven days at a time from today.
    - `spread(spread_type=OAS)` is null for an index. Use `OAS_SOVEREIGN`, `OAS_SWAP`,
      or `Z` with `universe_type`.

Notes
{ .recipe-label }

- The same numbers come from BDH:
  `bq.bdh(["I30904EU Index"], ["INDEX_YIELD_TO_WORST", "INDEX_OAS_TSY"], start, end)`.
  `INDEX_OAS_TSY` is the OAS with `RETURNS`, but in percent instead of basis
  points.
- The hedged index `H30904EU` has the same yield and spread; only its returns
  differ.

## Tier 2: spread to call or to maturity

YAS: `spread(price=..., workout=...)`
{ .recipe-uses }

The EUR Tier 2 bonds of SEB, Handelsbanken and Swedbank at today's price: Z-spread
and yield if the bank calls the bond at the first call date, and if it does not.
This is the YAS calculator run from BQL.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#px = px_last();
    #z_call = spread(spread_type=Z, price=#px, workout=TO_NEXT_CALL);
    #z_mat = spread(spread_type=Z, price=#px, workout=TO_MATURITY);
    #y_call = yield(price=#px, workout=TO_NEXT_CALL);
    #y_mat = yield(price=#px, workout=TO_MATURITY);)
get(name, nxt_call_dt, #px, #z_call, #z_mat, #y_call, #y_mat)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['SEB', 'SHBASS', 'SWEDA'] and crncy() == 'EUR' and
           basel_iii_designation() == 'Tier 2'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(
    df.select(
        "name",
        call="nxt_call_dt",
        price=pl.col("#px").round(2),
        z_call=pl.col("#z_call").round(),
        z_mat=pl.col("#z_mat").round(),
        y_call=pl.col("#y_call").round(2),
        y_mat=pl.col("#y_mat").round(2),
        extension=(pl.col("#z_mat") - pl.col("#z_call")).round(),
    ).sort("call")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (9, 8)
┌─────────────────────┬────────────┬────────┬────────┬───────┬────────┬───────┬───────────┐
│ name                ┆ call       ┆ price  ┆ z_call ┆ z_mat ┆ y_call ┆ y_mat ┆ extension │
│ ---                 ┆ ---        ┆ ---    ┆ ---    ┆ ---   ┆ ---    ┆ ---   ┆ ---       │
│ str                 ┆ date       ┆ f64    ┆ f64    ┆ f64   ┆ f64    ┆ f64   ┆ f64       │
╞═════════════════════╪════════════╪════════╪════════╪═══════╪════════╪═══════╪═══════════╡
│ SWEDA 3 ⅝ 08/23/32  ┆ 2027-08-23 ┆ 99.69  ┆ 85.0   ┆ 196.0 ┆ 4.01   ┆ 5.32  ┆ 110.0     │
│ SHBASS 3 ¼ 06/01/33 ┆ 2028-06-01 ┆ 98.69  ┆ 79.0   ┆ 153.0 ┆ 4.14   ┆ 4.91  ┆ 75.0      │
│ SEB 5 08/17/33      ┆ 2028-08-17 ┆ 101.31 ┆ 83.0   ┆ 159.0 ┆ 4.19   ┆ 4.96  ┆ 75.0      │
│ SHBASS 5 08/16/34   ┆ 2029-08-16 ┆ 101.78 ┆ 91.0   ┆ 151.0 ┆ 4.29   ┆ 4.89  ┆ 59.0      │
│ SEB 4 ½ 11/27/34    ┆ 2029-11-27 ┆ 100.92 ┆ 79.0   ┆ 137.0 ┆ 4.17   ┆ 4.76  ┆ 58.0      │
│ SWEDA 3 ½ 08/19/35  ┆ 2030-08-19 ┆ 96.92  ┆ 103.0  ┆ 111.0 ┆ 4.4    ┆ 4.51  ┆ 8.0       │
│ SWEDA 4 ½ 09/29/36  ┆ 2031-09-29 ┆ 99.83  ┆ 117.0  ┆ 113.0 ┆ 4.54   ┆ 4.54  ┆ -4.0      │
│ SHBASS 3 ⅝ 11/04/36 ┆ 2031-11-04 ┆ 96.19  ┆ 113.0  ┆ 125.0 ┆ 4.5    ┆ 4.66  ┆ 12.0      │
│ SEB 4 08/18/37      ┆ 2032-08-18 ┆ 97.48  ┆ 114.0  ┆ 108.0 ┆ 4.51   ┆ 4.5   ┆ -6.0      │
└─────────────────────┴────────────┴────────┴────────┴───────┴────────┴───────┴───────────┘
```

!!! warning "Watch out"

    - `price=` takes a number or an expression; `price=#px` runs YAS at `px_last()`.
      `workout=` without `price=` is rejected with "Incorrect parameters provided".
    - `TO_WORST` picks the workout with the lower yield, not the lower spread. A bond
      can be worst to call although its spread to maturity is lower.
    - Without `price=`, `spread()` uses Bloomberg's own pricing source, not
      `px_last()`. Pass the same `price=` to every value you compare.

Notes
{ .recipe-label }

- `workout=` is `TO_WORST` (the default), `TO_NEXT_CALL`, `TO_NEXT_PUT`,
  `TO_MATURITY` or `TO_CUSTOM`. `yield()` and the other spread types take the same
  parameters.
- These bonds reset their coupon to the swap rate plus the original margin if they
  are not called. `extension` is the spread to maturity minus the spread to call.
  When it is positive, the reset margin is above today's market spread and the
  bank saves money by calling. When it is negative, an investor earns less spread
  if the bond is not called.

## One bond at four prices

YAS: `workout=TO_CUSTOM`, `workout_date`
{ .recipe-uses }

The SEB 4 08/18/37 Tier 2 bond, first call 18 August 2032, at four prices:
Z-spread to the call, to a custom date two years later, and to maturity, and the
yield to worst.

```python
import polars as pl
from polars_bloomberg import BQuery

bond = "DO405623 Corp"  # SEB 4 08/18/37, first call 2032-08-18
rows = []
with BQuery() as bq:
    for price in [96, 98, 100, 102]:
        query = f"""
        get(spread(spread_type=Z, price={price}, workout=TO_NEXT_CALL) as #z_call,
            spread(spread_type=Z, price={price}, workout=TO_CUSTOM,
                   workout_date=2034-08-18) as #z_2034,
            spread(spread_type=Z, price={price}, workout=TO_MATURITY) as #z_mat,
            yield(price={price}, workout=TO_WORST) as #ytw)
        for(['{bond}'])
        """
        rows.append(bq.bql(query).combine().with_columns(price=pl.lit(price)))
print(
    pl.concat(rows).select(
        "price",
        z_call=pl.col("#z_call").round(),
        z_2034=pl.col("#z_2034").round(),
        z_mat=pl.col("#z_mat").round(),
        ytw=pl.col("#ytw").round(2),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (4, 5)
┌───────┬────────┬────────┬───────┬──────┐
│ price ┆ z_call ┆ z_2034 ┆ z_mat ┆ ytw  │
│ ---   ┆ ---    ┆ ---    ┆ ---   ┆ ---  │
│ i32   ┆ f64    ┆ f64    ┆ f64   ┆ f64  │
╞═══════╪════════╪════════╪═══════╪══════╡
│ 96    ┆ 144.0  ┆ 136.0  ┆ 126.0 ┆ 4.68 │
│ 98    ┆ 103.0  ┆ 104.0  ┆ 102.0 ┆ 4.4  │
│ 100   ┆ 63.0   ┆ 73.0   ┆ 78.0  ┆ 4.0  │
│ 102   ┆ 24.0   ┆ 43.0   ┆ 55.0  ┆ 3.61 │
└───────┴────────┴────────┴───────┴──────┘
```

Notes
{ .recipe-label }

- `workout=TO_CUSTOM, workout_date=...` assumes the bond is repaid at 100 on that
  date; `workout_price=` sets another redemption price. Use it for a call that
  comes late, or for the date you plan to sell.
- The spread to the nearest date moves most with the price: the gain or loss
  against 100 is spread over fewer years.
- The other direction works too: `price(spread=100, spread_type=Z,
  workout=TO_NEXT_CALL)` is the price for a Z-spread of 100 bp to the call, and
  `price(yield=4.5, workout=TO_NEXT_CALL)` the price for a yield.

## SEK AT1 floaters: discount margin

YAS: `discount_margin(price=...)`
{ .recipe-uses }

The floating rate AT1s in SEK of six Swedish banks: the quoted margin over STIBOR,
the discount margin at today's price, and the discount margin if you pay one point
more.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#px = px_last();)
get(name, nxt_call_dt, #px, flt_spread() as #margin,
    discount_margin(price=#px) as #dm,
    discount_margin(price=#px + 1) as #dm_plus_1)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['SEB', 'SHBASS', 'SBAB', 'LFBANK', 'KLARNA', 'SVEABK'] and
           crncy() == 'SEK' and cpn_typ() == 'FLOATING' and
           basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(
    df.select(
        "name",
        call="nxt_call_dt",
        price=pl.col("#px").round(2),
        margin="#margin",
        dm=pl.col("#dm").round(),
        dm_plus_1=pl.col("#dm_plus_1").round(),
        bp_per_point=(pl.col("#dm") - pl.col("#dm_plus_1")).round(),
    ).sort("call")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (10, 7)
┌───────────────────┬────────────┬────────┬────────┬───────┬───────────┬──────────────┐
│ name              ┆ call       ┆ price  ┆ margin ┆ dm    ┆ dm_plus_1 ┆ bp_per_point │
│ ---               ┆ ---        ┆ ---    ┆ ---    ┆ ---   ┆ ---       ┆ ---          │
│ str               ┆ date       ┆ f64    ┆ f64    ┆ f64   ┆ f64       ┆ f64          │
╞═══════════════════╪════════════╪════════╪════════╪═══════╪═══════════╪══════════════╡
│ KLARNA Float PERP ┆ 2027-03-25 ┆ 101.5  ┆ 700.0  ┆ 277.0 ┆ 3.0       ┆ 274.0        │
│ SBAB Float PERP   ┆ 2027-04-13 ┆ 100.61 ┆ 320.0  ┆ 166.0 ┆ -79.0     ┆ 245.0        │
│ SVEABK Float PERP ┆ 2028-04-03 ┆ 103.79 ┆ 750.0  ┆ 462.0 ┆ 389.0     ┆ 73.0         │
│ SVEABK Float PERP ┆ 2028-12-20 ┆ 101.5  ┆ 750.0  ┆ 735.0 ┆ 724.0     ┆ 12.0         │
│ LFBANK Float PERP ┆ 2029-01-23 ┆ 102.0  ┆ 290.0  ┆ 195.0 ┆ 148.0     ┆ 47.0         │
│ SBAB Float PERP   ┆ 2029-02-21 ┆ 102.28 ┆ 300.0  ┆ 195.0 ┆ 150.0     ┆ 45.0         │
│ SEB Float PERP    ┆ 2029-09-03 ┆ 101.97 ┆ 280.0  ┆ 206.0 ┆ 169.0     ┆ 37.0         │
│ LFBANK Float PERP ┆ 2030-09-09 ┆ 100.81 ┆ 235.0  ┆ 212.0 ┆ 184.0     ┆ 28.0         │
│ SBAB Float PERP   ┆ 2030-09-23 ┆ 100.59 ┆ 235.0  ┆ 218.0 ┆ 190.0     ┆ 28.0         │
│ SHBASS Float PERP ┆ 2031-04-01 ┆ 100.62 ┆ 235.0  ┆ 219.0 ┆ 195.0     ┆ 25.0         │
└───────────────────┴────────────┴────────┴────────┴───────┴───────────┴──────────────┘
```

Notes
{ .recipe-label }

- `flt_spread()` is the quoted margin, the spread the coupon pays over the
  reference rate. `discount_margin(price=100)` is close to it: at par a floater
  earns its margin.
- For these perpetual notes the discount margin is to the next call (`TO_WORST`
  and `TO_NEXT_CALL` give the same value). `workout=TO_MATURITY` changes it.
- `bp_per_point` is the discount margin you give up for each point of price. Close
  to the call date one point costs a lot of margin, so check the call date before
  you pay up for a short floater.
- `price=#px + 1` shows that `price=` takes arithmetic too.

## Pitfalls

What surprised us while writing these recipes.

<div class="recipe-pitfalls" markdown>

| Symptom | Cause | Fix |
|---|---|---|
| "OAS" numbers match the Z-spread | `spread()` without a type is the Z-spread | `spread(spread_type=OAS)` |
| Each group appears twice after `combine()`, half of the values null | A null group key: `capital_contingent_security()` is null for most non-CoCos, and `combine()` does not join null keys | `if(capital_contingent_security(), true, false)` |
| Grouped items over dates do not join, three times as many rows | `ORIG_IDS` is null in every row | `preferences(dropCols=['ORIG_IDS'])` |
| A monthly history on the 17th of each month | `frq=M` steps back from the end date | `per=M` for month-end closes |
| A weekly history on Tuesdays | `frq=W` steps back seven days from today | `per=W` for Friday closes |
| An index's yield or spread is null | No `universe_type` | `universe_type=PROJECTED` (the only one for yield to worst) or `RETURNS` |
| "Incorrect parameters provided" for `spread()` or `yield()` | `workout=` without `price=` | Add `price=px_last()` or a number |
| The index OAS is 100 times smaller in BDH | `INDEX_OAS_TSY` is in percent, BQL in basis points | Multiply the BDH value by 100 |
| The daily average jumps on a holiday | Fewer bonds are priced that day | Compare the bond count, or use the median |
| Two aliases, one column | BQL merges identical expressions, such as `spread()` and `spread(spread_type=Z)`, into one item | Ask for each expression once |

</div>

!!! abstract "Related"
    See also `YAS <GO>` and `BQLX <GO>`. For how `bql()` and `combine()` work, see
    [BQL](../usage/bql.md).
