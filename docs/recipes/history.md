---
title: Market History with BDH
description: Historical time series with BDH in polars-bloomberg - weekly OAS of high-yield and CoCo bond indices with periodicitySelection, markets with different holidays on one calendar with nonTradingDayFillOption and nonTradingDayFillMethod, fund NAV, assets under management and net flows, CDS index (iTraxx Main and Crossover) spread vs intrinsic value and basis, US Treasuries hedged into EUR vs Bunds with an FX hedging cost index, fiscal-year EBITDA, net debt and leverage with periodicityAdjustment=FISCAL, and weekly OHLC candles.
tags:
    - BDH
    - Credit
    - CDS
    - Funds
    - FX
    - Rates
    - Fundamentals
---

# Market History with BDH

BQL screens a universe and computes. BDH, `bq.bdh()` here and `=BDH()` in Excel,
does something simpler: it returns the history of a few fields for a list of
securities. The query is short, so the work is in the options.
{ .recipe-lead }

The options decide which dates you get (days, Fridays, month-ends, fiscal years)
and what happens on a day when one market is closed and another is open. Get them
wrong and you get a frame that looks right but has gaps, half-empty rows or
misaligned dates.

!!! info "Data on this page"
    This page uses the series a credit desk pulls every week: bond index spreads, CDS
    indices, fund NAVs and flows, the cost of hedging dollars into euros, government
    yields, fiscal-year fundamentals and price candles.

    The outputs below are live data as of 2026-11-17 and will differ when you run the
    queries. The recipes with fixed dates change only when Bloomberg revises its
    data. Before you adapt a query, read the [Pitfalls](#pitfalls).

## Quick reference

`bdh()` returns a long frame with a `security` column, a `date` column and one
column per field. `pivot(on="security", ...)` makes one column per security.

=== "By task"

    | You want | Use | One row per |
    |---|---|---|
    | Weekly, monthly or yearly closes | `options={"periodicitySelection": "WEEKLY"}`, `MONTHLY`, `YEARLY` | security and period |
    | Markets with different holidays on the same dates | `"nonTradingDayFillOption": "NON_TRADING_WEEKDAYS"`, `"nonTradingDayFillMethod": "PREVIOUS_VALUE"` | security and weekday |
    | A fund's NAV, assets and flows | `FUND_NET_ASSET_VAL`, `FUND_TOTAL_ASSETS`, `FUND_CLASS_ASSETS`, `FUND_FLOW` | fund and date |
    | A CDS index against its constituents | `CDS_INDEX_INTRINSIC_VALUE`, `CDS_INDEX_INTRINSIC_BASIS_VALUE` | index and date |
    | FX, hedging cost and yields side by side | several securities, `PX_LAST`, then `pivot(on="security")` | date |
    | Fundamentals per fiscal year | `"periodicitySelection": "YEARLY"`, `"periodicityAdjustment": "FISCAL"` | company and fiscal year |
    | Values in another currency | `"currency": "USD"` | |
    | Open, high, low and close | `PX_OPEN`, `PX_HIGH`, `PX_LOW`, `PX_LAST` | security and day, week or month |

=== "Options"

    The options go in one dict, `options={...}`. The values are strings.

    | Option | Values | Default | Effect |
    |---|---|---|---|
    | `periodicitySelection` | `DAILY`, `WEEKLY`, `MONTHLY`, `QUARTERLY`, `SEMI_ANNUALLY`, `YEARLY` | `DAILY` | One row per period |
    | `periodicityAdjustment` | `CALENDAR`, `ACTUAL`, `FISCAL` | calendar periods | `CALENDAR`: weeks end on Friday, months on the last business day. `ACTUAL`: periods end on `end_date`, so weeks end on its weekday. `FISCAL`: on each company's fiscal period end |
    | `nonTradingDayFillOption` | `ACTIVE_DAYS_ONLY`, `NON_TRADING_WEEKDAYS`, `ALL_CALENDAR_DAYS` | `ACTIVE_DAYS_ONLY` | Which dates get a row |
    | `nonTradingDayFillMethod` | `PREVIOUS_VALUE`, `NIL_VALUE` | `PREVIOUS_VALUE` | What an added date holds |
    | `currency` | `EUR`, `USD`, `SEK`, ... | the security's own | Converts prices, fund assets and fundamentals |

    `start_date` and `end_date` take a `date` or a string, `"2026-04-01"` or
    `"20260401"`.

## High-yield spreads, week by week

`periodicitySelection=WEEKLY`, `INDEX_OAS_TSY_BP`
{ .recipe-uses }

Friday OAS of four Bloomberg bond indices for the last twelve weeks: US High Yield
(`LF98TRUU`), its Industrials sector (`I00423`), Pan-European High Yield
(`LP01TREU`) and Contingent Capital EUR (`I30904EU`). In basis points.

```python
from datetime import date, timedelta

import polars as pl
from polars_bloomberg import BQuery

indices = {
    "LF98TRUU Index": "us_hy",
    "I00423 Index": "us_hy_industrials",
    "LP01TREU Index": "euro_hy",
    "I30904EU Index": "eur_coco",
}
with BQuery() as bq:
    df = bq.bdh(
        list(indices),
        ["INDEX_OAS_TSY_BP"],
        start_date=date.today() - timedelta(weeks=12),
        end_date=date.today(),
        options={"periodicitySelection": "WEEKLY"},
    )
oas = df.with_columns(pl.col("security").replace_strict(indices)).pivot(
    on="security", index="date", values="INDEX_OAS_TSY_BP"
)
with pl.Config(tbl_rows=20):
    print(oas.select("date", pl.col(list(indices.values())).round().cast(pl.Int64)))
```

Output
``` { .text .no-copy .recipe-output }
shape: (12, 5)
┌────────────┬───────┬───────────────────┬─────────┬──────────┐
│ date       ┆ us_hy ┆ us_hy_industrials ┆ euro_hy ┆ eur_coco │
│ ---        ┆ ---   ┆ ---               ┆ ---     ┆ ---      │
│ date       ┆ i64   ┆ i64               ┆ i64     ┆ i64      │
╞════════════╪═══════╪═══════════════════╪═════════╪══════════╡
│ 2026-08-28 ┆ 260   ┆ 268               ┆ 277     ┆ 224      │
│ 2026-09-04 ┆ 266   ┆ 276               ┆ 285     ┆ 227      │
│ 2026-09-11 ┆ 266   ┆ 274               ┆ 286     ┆ 230      │
│ 2026-09-18 ┆ 268   ┆ 275               ┆ 286     ┆ 230      │
│ 2026-09-25 ┆ 295   ┆ 300               ┆ 300     ┆ 248      │
│ 2026-10-02 ┆ 310   ┆ 312               ┆ 347     ┆ 297      │
│ 2026-10-09 ┆ 310   ┆ 313               ┆ 342     ┆ 279      │
│ 2026-10-16 ┆ 334   ┆ 340               ┆ 356     ┆ 274      │
│ 2026-10-23 ┆ 343   ┆ 347               ┆ 395     ┆ 303      │
│ 2026-10-30 ┆ 345   ┆ 349               ┆ 384     ┆ 296      │
│ 2026-11-06 ┆ 337   ┆ 345               ┆ 378     ┆ 287      │
│ 2026-11-13 ┆ 318   ┆ 323               ┆ 354     ┆ 270      │
└────────────┴───────┴───────────────────┴─────────┴──────────┘
```

!!! warning "Watch out"

    - `WEEKLY` gives each week's Friday close. The current week is not there until it
      is complete. For the latest close, add a daily request or use BQL's `per=W`, as in
      [Index yield and spread, week by week](spreads.md#index-yield-and-spread-week-by-week).
    - `"periodicityAdjustment": "ACTUAL"` ends each week on the weekday of `end_date`.
      Run it on a Wednesday and you get Wednesday closes.

Notes
{ .recipe-label }

- `INDEX_OAS_TSY_BP` is the OAS in basis points, and `INDEX_OAS_TSY` is the same
  number in percent. `INDEX_Z_SPREAD_BP` is the Z-spread.
- Field ids work as well as mnemonics: `"BX218"` is `INDEX_OAS_TSY_BP` and
  `"BX326"` is `INDEX_Z_SPREAD_BP`. The column is named as you asked, so `BX218`.
  Look fields up with `FLDS <GO>`.
- `replace_strict` turns tickers into short names before the pivot. It raises an
  error for a ticker that is missing from the dict, so a typo does not go unnoticed.

## Three markets, one calendar

`nonTradingDayFillOption`, `nonTradingDayFillMethod`
{ .recipe-uses }

The S&P 500, Euro Stoxx 50 and OMX Stockholm 30 around Easter 2026. On Good Friday,
3 April, all three were closed. On Easter Monday, 6 April, only New York traded.
The recipe asks for every weekday, carries the last close into holidays and lists
the markets that were closed.

```python
import polars as pl
from polars_bloomberg import BQuery

markets = ["SPX Index", "SX5E Index", "OMX Index"]
every_weekday = {
    "nonTradingDayFillOption": "NON_TRADING_WEEKDAYS",
    "nonTradingDayFillMethod": "PREVIOUS_VALUE",
}
with BQuery() as bq:
    traded = bq.bdh(markets, "PX_LAST", "2026-04-01", "2026-04-08")
    filled = bq.bdh(
        markets, "PX_LAST", "2026-04-01", "2026-04-08", options=every_weekday
    )
closed = (
    filled.join(traded, on=["security", "date"], how="anti")
    .group_by("date")
    .agg(closed=pl.col("security").str.replace(" Index", "").sort().str.join(", "))
)
print(
    filled.pivot(on="security", index="date", values="PX_LAST")
    .join(closed, on="date", how="left")
    .sort("date")
    .select("date", pl.col("date").dt.strftime("%a").alias("day"), *markets, "closed")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (6, 6)
┌────────────┬─────┬───────────┬────────────┬───────────┬────────────────┐
│ date       ┆ day ┆ SPX Index ┆ SX5E Index ┆ OMX Index ┆ closed         │
│ ---        ┆ --- ┆ ---       ┆ ---        ┆ ---       ┆ ---            │
│ date       ┆ str ┆ f64       ┆ f64        ┆ f64       ┆ str            │
╞════════════╪═════╪═══════════╪════════════╪═══════════╪════════════════╡
│ 2026-04-01 ┆ Wed ┆ 6575.32   ┆ 5732.71    ┆ 3006.37   ┆ null           │
│ 2026-04-02 ┆ Thu ┆ 6582.69   ┆ 5692.86    ┆ 2965.688  ┆ null           │
│ 2026-04-03 ┆ Fri ┆ 6582.69   ┆ 5692.86    ┆ 2965.688  ┆ OMX, SPX, SX5E │
│ 2026-04-06 ┆ Mon ┆ 6611.83   ┆ 5692.86    ┆ 2965.688  ┆ OMX, SX5E      │
│ 2026-04-07 ┆ Tue ┆ 6616.85   ┆ 5633.22    ┆ 2967.917  ┆ null           │
│ 2026-04-08 ┆ Wed ┆ 6782.81   ┆ 5913.37    ┆ 3094.161  ┆ null           │
└────────────┴─────┴───────────┴────────────┴───────────┴────────────────┘
```

!!! warning "Watch out"

    - Without options (`traded`) each security keeps its own trading days. Good Friday
      has no row at all, and Easter Monday has one only for the S&P 500. After a pivot
      Europe is null on Easter Monday. Computed per security, Europe's return on
      Tuesday covers Thursday to Tuesday, and New York's only Monday to Tuesday.
    - `nonTradingDayFillMethod` alone changes nothing. The default
      `nonTradingDayFillOption`, `ACTIVE_DAYS_ONLY`, adds no days to fill.

Notes
{ .recipe-label }

- `NON_TRADING_WEEKDAYS` gives a row for every Monday to Friday. `PREVIOUS_VALUE`
  repeats the last close on the added days, so the return on a holiday is 0.
- `ALL_CALENDAR_DAYS` adds Saturdays and Sundays too, which helps when a month ends
  on a weekend. `NIL_VALUE` adds the days but leaves them null.
- The anti join finds the rows that only the fill created. Those are the holidays.

## Fund NAV, assets and flows

`FUND_TOTAL_ASSETS`, `FUND_FLOW`, `MONTHLY`
{ .recipe-uses }

Month-end NAV, return and assets of two AT1 funds: the SEB Hybrid Capital Bond
Fund (`SEBVVGD`, a UCITS fund, share class R EUR) and the Invesco USD AT1 CoCo
Bond UCITS ETF (`AT1 LN`). `class_flow` is Bloomberg's flow into the share class.
`fund_flow` estimates the flow into the whole fund from its assets and the NAV
return. Assets and flows are in millions of the fund's currency, EUR and USD.

```python
from datetime import date, timedelta

import polars as pl
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdh(
        ["SEBVVGD SS Equity", "AT1 LN Equity"],
        ["FUND_NET_ASSET_VAL", "FUND_TOTAL_ASSETS", "FUND_FLOW"],
        start_date=date.today() - timedelta(days=200),
        end_date=date.today(),
        options={"periodicitySelection": "MONTHLY"},
    )
growth = pl.col("FUND_NET_ASSET_VAL") / pl.col("FUND_NET_ASSET_VAL").shift()
aum = pl.col("FUND_TOTAL_ASSETS")
table = (
    df.with_columns(
        ret_pct=(100 * (growth - 1)).over("security"),
        fund_flow=(aum - aum.shift() * growth).over("security"),
    )
    .drop_nulls("fund_flow")
    .select(
        fund=pl.col("security").str.split(" ").list.first(),
        month="date",
        nav=pl.col("FUND_NET_ASSET_VAL").round(2),
        ret_pct=pl.col("ret_pct").round(2),
        aum=aum.round(1),
        class_flow=pl.col("FUND_FLOW").round(1),
        fund_flow=pl.col("fund_flow").round(1),
    )
)
with pl.Config(tbl_rows=20):
    print(table)
```

Output
``` { .text .no-copy .recipe-output }
shape: (10, 7)
┌─────────┬────────────┬────────┬─────────┬────────┬────────────┬───────────┐
│ fund    ┆ month      ┆ nav    ┆ ret_pct ┆ aum    ┆ class_flow ┆ fund_flow │
│ ---     ┆ ---        ┆ ---    ┆ ---     ┆ ---    ┆ ---        ┆ ---       │
│ str     ┆ date       ┆ f64    ┆ f64     ┆ f64    ┆ f64        ┆ f64       │
╞═════════╪════════════╪════════╪═════════╪════════╪════════════╪═══════════╡
│ SEBVVGD ┆ 2026-06-30 ┆ 133.68 ┆ 0.78    ┆ 209.4  ┆ 0.0        ┆ -4.4      │
│ SEBVVGD ┆ 2026-07-31 ┆ 133.28 ┆ -0.3    ┆ 221.7  ┆ -0.1       ┆ 12.9      │
│ SEBVVGD ┆ 2026-08-31 ┆ 133.74 ┆ 0.34    ┆ 220.0  ┆ -1.6       ┆ -2.4      │
│ SEBVVGD ┆ 2026-09-30 ┆ 130.96 ┆ -2.08   ┆ 188.0  ┆ 0.0        ┆ -27.4     │
│ SEBVVGD ┆ 2026-10-30 ┆ 130.78 ┆ -0.14   ┆ 192.1  ┆ -0.4       ┆ 4.4       │
│ AT1     ┆ 2026-06-30 ┆ 30.32  ┆ 0.56    ┆ 1657.1 ┆ 4.5        ┆ -16.7     │
│ AT1     ┆ 2026-07-31 ┆ 30.23  ┆ -0.3    ┆ 1633.8 ┆ -19.2      ┆ -18.2     │
│ AT1     ┆ 2026-08-31 ┆ 30.41  ┆ 0.58    ┆ 1645.8 ┆ -0.1       ┆ 2.5       │
│ AT1     ┆ 2026-09-30 ┆ 29.76  ┆ -2.13   ┆ 1557.1 ┆ -40.3      ┆ -53.7     │
│ AT1     ┆ 2026-10-30 ┆ 29.78  ┆ 0.06    ┆ 1500.0 ┆ -28.6      ┆ -58.1     │
└─────────┴────────────┴────────┴─────────┴────────┴────────────┴───────────┘
```

!!! warning "Watch out"

    - With `MONTHLY`, `FUND_FLOW` is the sum of the month's daily flows. NAV and assets
      are month-end values. The current month is missing until it ends.
    - `PX_LAST` of a mutual fund is its NAV. For an ETF it is the exchange price, and
      `PX_LAST / FUND_NET_ASSET_VAL - 1` is the premium to NAV.

Notes
{ .recipe-label }

- `FUND_TOTAL_ASSETS` is the whole fund, all share classes together.
  `FUND_CLASS_ASSETS` is the share class of the ticker you ask for, and
  `FUND_TOTAL_ASSETS_CRNCY` gives the currency.
- Assets are shares times NAV. So the change in assets less the NAV return is the
  money that came in or went out. Computed with `FUND_CLASS_ASSETS`, it comes close
  to Bloomberg's `FUND_FLOW`. With `FUND_TOTAL_ASSETS` it is an estimate for the
  whole fund, because the other share classes earn slightly different returns
  (other currencies, other fees).
- A month where `class_flow` is close to 0 and `fund_flow` is large means money
  moved in the fund's other share classes.
- `"currency": "SEK"` returns the assets in SEK. Bloomberg converts them.

## CDS index vs intrinsic: the basis

`CDS_INDEX_INTRINSIC_BASIS_VALUE`
{ .recipe-uses }

iTraxx Europe Main and Crossover, 5-year, for the last three weeks. For each index
the quoted spread, the intrinsic spread (the index spread implied by the CDS of its
constituents) and the basis between them, in basis points.

```python
from datetime import date, timedelta

import polars as pl
from polars_bloomberg import BQuery

indices = {"ITRXEBE Index": "main", "ITRXEXE Index": "xover"}
with BQuery() as bq:
    df = bq.bdh(
        list(indices),
        ["PX_LAST", "CDS_INDEX_INTRINSIC_VALUE", "CDS_INDEX_INTRINSIC_BASIS_VALUE"],
        start_date=date.today() - timedelta(weeks=3),
        end_date=date.today(),
    )
wide = df.select(
    "date",
    index=pl.col("security").replace_strict(indices),
    spread=pl.col("PX_LAST").round(1),
    intrinsic=pl.col("CDS_INDEX_INTRINSIC_VALUE").round(1),
    basis=pl.col("CDS_INDEX_INTRINSIC_BASIS_VALUE"),
).pivot(on="index", index="date", values=["spread", "intrinsic", "basis"])
with pl.Config(tbl_rows=30):
    print(
        wide.select(
            "date",
            "spread_main",
            "intrinsic_main",
            "basis_main",
            "spread_xover",
            "intrinsic_xover",
            "basis_xover",
        )
    )
```

Output
``` { .text .no-copy .recipe-output }
shape: (16, 7)
┌────────────┬─────────────┬────────────────┬────────────┬──────────────┬─────────────────┬─────────────┐
│ date       ┆ spread_main ┆ intrinsic_main ┆ basis_main ┆ spread_xover ┆ intrinsic_xover ┆ basis_xover │
│ ---        ┆ ---         ┆ ---            ┆ ---        ┆ ---          ┆ ---             ┆ ---         │
│ date       ┆ f64         ┆ f64            ┆ f64        ┆ f64          ┆ f64             ┆ f64         │
╞════════════╪═════════════╪════════════════╪════════════╪══════════════╪═════════════════╪═════════════╡
│ 2026-10-27 ┆ 64.8        ┆ 67.3           ┆ -2.44      ┆ 322.1        ┆ 331.5           ┆ -9.37       │
│ 2026-10-28 ┆ 64.2        ┆ 65.7           ┆ -1.53      ┆ 317.0        ┆ 323.0           ┆ -5.99       │
│ 2026-10-29 ┆ 63.2        ┆ 64.2           ┆ -1.0       ┆ 313.4        ┆ 318.0           ┆ -4.59       │
│ 2026-10-30 ┆ 65.9        ┆ 65.3           ┆ 0.53       ┆ 326.2        ┆ 320.1           ┆ 6.14        │
│ 2026-11-02 ┆ 64.3        ┆ 65.0           ┆ -0.65      ┆ 318.6        ┆ 318.0           ┆ 0.62        │
│ 2026-11-03 ┆ 65.1        ┆ 65.3           ┆ -0.2       ┆ 323.1        ┆ 319.0           ┆ 4.06        │
│ 2026-11-04 ┆ 61.3        ┆ 63.0           ┆ -1.73      ┆ 306.0        ┆ 310.3           ┆ -4.36       │
│ 2026-11-05 ┆ 62.3        ┆ 62.4           ┆ -0.14      ┆ 310.3        ┆ 306.2           ┆ 4.1         │
│ 2026-11-06 ┆ 62.6        ┆ 63.3           ┆ -0.69      ┆ 311.0        ┆ 307.8           ┆ 3.18        │
│ 2026-11-09 ┆ 59.8        ┆ 61.8           ┆ -1.95      ┆ 298.3        ┆ 306.5           ┆ -8.2        │
│ 2026-11-10 ┆ 60.4        ┆ 60.7           ┆ -0.25      ┆ 298.9        ┆ 298.8           ┆ 0.02        │
│ 2026-11-11 ┆ 58.9        ┆ 59.8           ┆ -0.85      ┆ 290.5        ┆ 297.2           ┆ -6.62       │
│ 2026-11-12 ┆ 58.8        ┆ 60.2           ┆ -1.38      ┆ 290.4        ┆ 297.8           ┆ -7.43       │
│ 2026-11-13 ┆ 59.2        ┆ 60.7           ┆ -1.48      ┆ 292.5        ┆ 299.6           ┆ -7.06       │
│ 2026-11-16 ┆ 60.3        ┆ 61.2           ┆ -0.93      ┆ 296.4        ┆ 300.6           ┆ -4.24       │
│ 2026-11-17 ┆ 61.0        ┆ null           ┆ null       ┆ 299.8        ┆ null            ┆ null        │
└────────────┴─────────────┴────────────────┴────────────┴──────────────┴─────────────────┴─────────────┘
```

!!! warning "Watch out"

    - `basis` is the quoted spread minus the intrinsic spread, although the field's
      description says it the other way round. A positive basis means the index trades
      wider than its constituents.
    - `ITRXEBE` and `ITRXEXE` follow the current series. They roll to a new series
      around 20 March and 20 September. The spread jumps on the roll, because the new
      series runs six months longer and some of its companies change. Main closed at
      54.6 on Friday 18 September 2026 and at 58.2 on the roll day, Monday 21
      September; Crossover went from 260 to 284.

Notes
{ .recipe-label }

- Today's quoted spread is there during the day, but the intrinsic spread and the
  basis are still null. `drop_nulls("basis_main")` removes that row.
- `values=[...]` in `pivot` gives one column per field and index, named
  `<field>_<index>`.
- For the return of selling protection on Main, use the excess return index
  `ERIXITEU Index`.

## US Treasuries hedged into euros vs Bunds

`FXHCEUUS Index`, `pivot(on="security")`
{ .recipe-uses }

Should a euro investor buy 5-year US Treasuries hedged back into euros, or 5-year
Bunds? Month-end EURUSD, the cost of hedging dollars into euros with 3-month
forwards, both yields, and the hedged pickup over Bunds. Four securities, one
field, one wide frame.

```python
from datetime import date, timedelta

import polars as pl
from polars_bloomberg import BQuery

names = {
    "EURUSD Curncy": "eurusd",
    "FXHCEUUS Index": "hedge_cost",
    "USGG5YR Index": "ust_5y",
    "GDBR5 Index": "bund_5y",
}
with BQuery() as bq:
    df = bq.bdh(
        list(names),
        "PX_LAST",
        start_date=date.today() - timedelta(days=365),
        end_date=date.today(),
        options={"periodicitySelection": "MONTHLY"},
    )
wide = df.with_columns(pl.col("security").replace_strict(names)).pivot(
    on="security", index="date", values="PX_LAST"
)
hedged = pl.col("ust_5y") + pl.col("hedge_cost")
with pl.Config(tbl_rows=20):
    print(
        wide.select(
            "date",
            *names.values(),
            ust_hedged=hedged.round(2),
            pickup_bp=(100 * (hedged - pl.col("bund_5y"))).round(),
        )
    )
```

Output
``` { .text .no-copy .recipe-output }
shape: (12, 7)
┌────────────┬────────┬────────────┬────────┬─────────┬────────────┬───────────┐
│ date       ┆ eurusd ┆ hedge_cost ┆ ust_5y ┆ bund_5y ┆ ust_hedged ┆ pickup_bp │
│ ---        ┆ ---    ┆ ---        ┆ ---    ┆ ---     ┆ ---        ┆ ---       │
│ date       ┆ f64    ┆ f64        ┆ f64    ┆ f64     ┆ f64        ┆ f64       │
╞════════════╪════════╪════════════╪════════╪═════════╪════════════╪═══════════╡
│ 2025-11-28 ┆ 1.1598 ┆ -1.8702    ┆ 3.5964 ┆ 2.288   ┆ 1.73       ┆ -56.0     │
│ 2025-12-31 ┆ 1.1746 ┆ -1.6823    ┆ 3.7252 ┆ 2.448   ┆ 2.04       ┆ -41.0     │
│ 2026-01-30 ┆ 1.1851 ┆ -1.6871    ┆ 3.7881 ┆ 2.417   ┆ 2.1        ┆ -32.0     │
│ 2026-02-27 ┆ 1.1812 ┆ -1.706     ┆ 3.5017 ┆ 2.239   ┆ 1.8        ┆ -44.0     │
│ 2026-03-31 ┆ 1.1553 ┆ -1.6378    ┆ 3.9427 ┆ 2.727   ┆ 2.3        ┆ -42.0     │
│ 2026-04-30 ┆ 1.1731 ┆ -1.5603    ┆ 4.0055 ┆ 2.748   ┆ 2.45       ┆ -30.0     │
│ 2026-05-29 ┆ 1.1659 ┆ -1.4989    ┆ 4.1407 ┆ 2.637   ┆ 2.64       ┆ 0.0       │
│ 2026-06-30 ┆ 1.1422 ┆ -1.5013    ┆ 4.2265 ┆ 2.595   ┆ 2.73       ┆ 13.0      │
│ 2026-07-31 ┆ 1.1527 ┆ -1.4253    ┆ 4.4489 ┆ 2.943   ┆ 3.02       ┆ 8.0       │
│ 2026-08-31 ┆ 1.1618 ┆ -1.3948    ┆ 4.5002 ┆ 3.064   ┆ 3.11       ┆ 4.0       │
│ 2026-09-30 ┆ 1.133  ┆ -1.5442    ┆ 5.0859 ┆ 3.355   ┆ 3.54       ┆ 19.0      │
│ 2026-10-30 ┆ 1.116  ┆ -1.5169    ┆ 4.7198 ┆ 3.128   ┆ 3.2        ┆ 7.0       │
└────────────┴────────┴────────────┴────────┴─────────┴────────────┴───────────┘
```

!!! warning "Watch out"

    `FXHCEUUS` (Bloomberg EURUSD 3 Month Hedging Cost) is the annualised return, in
    percent, of selling dollars three months forward for euros. It is negative when
    hedging costs a euro investor money, so add it to the dollar yield. It follows
    the forward points. On 13 November 2026, EURUSD at 1.1023 and 3-month points of
    44.77 (`EUR3M Curncy`) came to about -1.6% a year, and the index stood at -1.59.

Notes
{ .recipe-label }

- `FXHCUSEU` is the other side: a dollar investor hedging euros earns about the
  same amount.
- The hedge is rolled every three months. `ust_hedged` holds only while the gap
  between dollar and euro short rates stays where it is.
- Holidays differ between New York, Frankfurt and the FX market. Month-ends hide
  that. For daily data, add the options from
  [Three markets, one calendar](#three-markets-one-calendar).
- To get the hedge cost known on each trade date, sort both frames by date and use
  `trades.join_asof(hedge, on="date")`. Each trade gets the last value on or before
  its date.

## Fiscal-year leverage

`YEARLY`, `periodicityAdjustment=FISCAL`, `currency`
{ .recipe-uses }

Reported and adjusted EBITDA, net debt and net leverage for each fiscal year, for
three companies whose years end in different months: IBM (December), Vodafone
(March) and Microsoft (June). In USD billions.

```python
from datetime import date

import polars as pl
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdh(
        ["IBM US Equity", "VOD LN Equity", "MSFT US Equity"],
        ["TRAIL_12M_EBITDA", "EBITDA_ADJUSTED", "NET_DEBT"],
        start_date=date(date.today().year - 4, 1, 1),
        end_date=date.today(),
        options={
            "periodicitySelection": "YEARLY",
            "periodicityAdjustment": "FISCAL",
            "currency": "USD",
        },
    )
with pl.Config(tbl_rows=20):
    print(
        df.select(
            company=pl.col("security").str.split(" ").list.first(),
            fiscal_year_end="date",
            ebitda=(pl.col("TRAIL_12M_EBITDA") / 1000).round(1),
            ebitda_adj=(pl.col("EBITDA_ADJUSTED") / 1000).round(1),
            net_debt=(pl.col("NET_DEBT") / 1000).round(1),
            leverage=(pl.col("NET_DEBT") / pl.col("EBITDA_ADJUSTED")).round(1),
        )
    )
```

Output
``` { .text .no-copy .recipe-output }
shape: (14, 6)
┌─────────┬─────────────────┬────────┬────────────┬──────────┬──────────┐
│ company ┆ fiscal_year_end ┆ ebitda ┆ ebitda_adj ┆ net_debt ┆ leverage │
│ ---     ┆ ---             ┆ ---    ┆ ---        ┆ ---      ┆ ---      │
│ str     ┆ date            ┆ f64    ┆ f64        ┆ f64      ┆ f64      │
╞═════════╪═════════════════╪════════╪════════════╪══════════╪══════════╡
│ IBM     ┆ 2022-12-31      ┆ 13.3   ┆ 13.0       ┆ 45.3     ┆ 3.5      │
│ IBM     ┆ 2023-12-31      ┆ 13.9   ┆ 14.3       ┆ 46.5     ┆ 3.2      │
│ IBM     ┆ 2024-12-31      ┆ 14.0   ┆ 18.2       ┆ 43.8     ┆ 2.4      │
│ IBM     ┆ 2025-12-31      ┆ 16.9   ┆ 17.6       ┆ 50.2     ┆ 2.9      │
│ VOD     ┆ 2022-03-31      ┆ 18.3   ┆ 18.7       ┆ 60.6     ┆ 3.2      │
│ VOD     ┆ 2023-03-31      ┆ 25.3   ┆ 16.2       ┆ 51.8     ┆ 3.2      │
│ VOD     ┆ 2024-03-31      ┆ 15.4   ┆ 15.7       ┆ 49.3     ┆ 3.1      │
│ VOD     ┆ 2025-03-31      ┆ 11.3   ┆ 15.7       ┆ 37.9     ┆ 2.4      │
│ VOD     ┆ 2026-03-31      ┆ 18.2   ┆ 18.4       ┆ 42.6     ┆ 2.3      │
│ MSFT    ┆ 2022-06-30      ┆ 100.3  ┆ 100.3      ┆ -26.4    ┆ -0.3     │
│ MSFT    ┆ 2023-06-30      ┆ 105.3  ┆ 106.4      ┆ -31.8    ┆ -0.3     │
│ MSFT    ┆ 2024-06-30      ┆ 135.3  ┆ 135.3      ┆ 22.3     ┆ 0.2      │
│ MSFT    ┆ 2025-06-30      ┆ 168.2  ┆ 168.2      ┆ 17.6     ┆ 0.1      │
│ MSFT    ┆ 2026-06-30      ┆ 200.7  ┆ 200.7      ┆ 52.0     ┆ 0.3      │
└─────────┴─────────────────┴────────┴────────────┴──────────┴──────────┘
```

!!! warning "Watch out"

    - `"periodicityAdjustment": "FISCAL"` puts every field on the company's fiscal year
      end. Without it, `TRAIL_12M_EBITDA` lands on the last trading day of each
      calendar year and `NET_DEBT` on the fiscal year end. When the two dates differ,
      as they do every year for Vodafone and Microsoft, the year has two rows, each
      half null.
    - `CALENDAR` does not calendarise the figures; it only relabels them. Microsoft's
      fiscal year to June 2023 and Vodafone's to March 2023 both come back dated
      2022-12-30. Keep `FISCAL` and the real period end.
    - Vodafone reports in EUR and `"currency": "USD"` converts. EBITDA is converted at
      the average rate of the year and net debt at the rate at year end: about 1.16
      and 1.11 USD per EUR for fiscal 2022. So leverage in USD can differ from
      leverage in the reporting currency.

Notes
{ .recipe-label }

- `TRAIL_12M_EBITDA` is EBITDA as reported. `EBITDA_ADJUSTED` takes out one-off
  items. Vodafone's fiscal 2023 shows the difference: 25.3 reported, 16.2
  adjusted.
- Amounts are in millions; the recipe divides by 1,000. Negative net debt is net
  cash.
- For period offsets, segments, adjusted figures and estimates in BQL, see
  [Company Fundamentals](fundamentals.md).

## Weekly candles

`PX_OPEN`, `PX_HIGH`, `PX_LOW`, `WEEKLY`
{ .recipe-uses }

Weekly open, high, low and close of the S&P 500 for the last eight weeks, with the
week's range and change in percent.

```python
from datetime import date, timedelta

import polars as pl
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdh(
        "SPX Index",
        ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"],
        start_date=date.today() - timedelta(weeks=8),
        end_date=date.today(),
        options={"periodicitySelection": "WEEKLY"},
    )
print(
    df.select(
        week_ending="date",
        open="PX_OPEN",
        high="PX_HIGH",
        low="PX_LOW",
        close="PX_LAST",
        range_pct=(
            100 * (pl.col("PX_HIGH") - pl.col("PX_LOW")) / pl.col("PX_LAST")
        ).round(1),
        chg_pct=(100 * pl.col("PX_LAST").pct_change()).round(1),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (8, 7)
┌─────────────┬─────────┬─────────┬─────────┬─────────┬───────────┬─────────┐
│ week_ending ┆ open    ┆ high    ┆ low     ┆ close   ┆ range_pct ┆ chg_pct │
│ ---         ┆ ---     ┆ ---     ┆ ---     ┆ ---     ┆ ---       ┆ ---     │
│ date        ┆ f64     ┆ f64     ┆ f64     ┆ f64     ┆ f64       ┆ f64     │
╞═════════════╪═════════╪═════════╪═════════╪═════════╪═══════════╪═════════╡
│ 2026-09-25  ┆ 7692.83 ┆ 7782.19 ┆ 7662.57 ┆ 7743.41 ┆ 1.5       ┆ null    │
│ 2026-10-02  ┆ 7721.7  ┆ 7754.67 ┆ 7616.78 ┆ 7722.72 ┆ 1.8       ┆ -0.3    │
│ 2026-10-09  ┆ 7730.86 ┆ 7844.52 ┆ 7727.59 ┆ 7811.54 ┆ 1.5       ┆ 1.2     │
│ 2026-10-16  ┆ 7813.04 ┆ 7821.67 ┆ 7631.77 ┆ 7659.44 ┆ 2.5       ┆ -1.9    │
│ 2026-10-23  ┆ 7702.77 ┆ 7770.4  ┆ 7612.07 ┆ 7623.68 ┆ 2.1       ┆ -0.5    │
│ 2026-10-30  ┆ 7639.7  ┆ 7758.43 ┆ 7527.56 ┆ 7580.07 ┆ 3.0       ┆ -0.6    │
│ 2026-11-06  ┆ 7610.83 ┆ 7715.57 ┆ 7350.35 ┆ 7376.86 ┆ 5.0       ┆ -2.7    │
│ 2026-11-13  ┆ 7422.24 ┆ 7652.76 ┆ 7422.24 ┆ 7561.67 ┆ 3.0       ┆ 2.5     │
└─────────────┴─────────┴─────────┴─────────┴─────────┴───────────┴─────────┘
```

Notes
{ .recipe-label }

- With `WEEKLY`, BDH builds real weekly bars: the open of the first day, the
  highest high, the lowest low and the last close. `group_by_dynamic("date",
  every="1w")` over daily data gives the same bars, dated by their Monday instead
  of their Friday. `MONTHLY` builds monthly bars the same way.
- A start date in the middle of a week still gives the whole first week. The
  current week appears once it is complete.
- To check the data, test `low <= min(open, close)` and `max(open, close) <= high`
  on each row.
- For a chart, plotly's `go.Candlestick(x=..., open=..., high=..., low=...,
  close=...)` takes these columns as they are.

## Pitfalls

What surprised us while writing these recipes.

<div class="recipe-pitfalls" markdown>

| Symptom | Cause | Fix |
|---|---|---|
| Weekly rows on Wednesdays | `"periodicityAdjustment": "ACTUAL"` ends weeks on the weekday of `end_date` | Leave it out, or `CALENDAR`, for Friday closes |
| The current week or month is missing | Weekly and monthly BDH return complete periods only | Add a daily request, or use BQL with `per=W` |
| Nulls after a pivot, holidays missing | Each security keeps its own trading days | `NON_TRADING_WEEKDAYS` with `PREVIOUS_VALUE` |
| `"nonTradingDayFillMethod": "PREVIOUS_VALUE"` changes nothing | The default fill option adds no days | Add `"nonTradingDayFillOption": "NON_TRADING_WEEKDAYS"` or `ALL_CALENDAR_DAYS` |
| Two rows per year, each half null | `YEARLY` without `FISCAL`: trailing fields on calendar year ends, reported fields on fiscal year ends | `"periodicityAdjustment": "FISCAL"` |
| Microsoft's fiscal 2023 dated 2022-12-30 | `CALENDAR` relabels fiscal years | `FISCAL` |
| A column named `BX218` | Field ids are accepted, and the column keeps the name you asked for | Use the mnemonic, `INDEX_OAS_TSY_BP`, or rename |
| Index OAS 100 times smaller than in BQL | `INDEX_OAS_TSY` is in percent | `INDEX_OAS_TSY_BP` |
| The CDS basis has the opposite sign to its description | `CDS_INDEX_INTRINSIC_BASIS_VALUE` is quoted minus intrinsic | Read positive as "index wider than its constituents" |
| Today's intrinsic spread is null | During the day only the quoted spread is there | `drop_nulls()`, or end yesterday |
| A CDS index jumps around 20 March or 20 September | The generic ticker rolled to a new series | Mark the roll dates, or use the series' own ticker |
| `FUND_FLOW` is 0 while the fund's assets fell | `FUND_FLOW` is for one share class, `FUND_TOTAL_ASSETS` for all of them | Compare with `FUND_CLASS_ASSETS`, or estimate the fund's flows |
| Leverage in USD differs from leverage in EUR | EBITDA converts at the year's average rate, net debt at the year-end rate | Compute leverage in the reporting currency |
| The hedged yield is too high | `FXHCEUUS` is negative when hedging costs money | Add it to the yield; do not subtract it |

</div>

!!! abstract "Related"
    See also `FLDS <GO>` for field mnemonics and ids, and `HP <GO>` for one security's
    history on the Terminal. For how `bdh()` works, see [BDH](../usage/bdh.md).
