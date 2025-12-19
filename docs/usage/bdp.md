---
title: BDP - Bloomberg Data Point
description: Bloomberg BDP single-value data point requests in Polars-Bloomberg, including overrides and complementary Bloomberg BQL screening workflows for Polars DataFrames.
---

# BDP

Use Case: Fetch single-value data points (like last price, currency, or descriptive fields).

```python
with BQuery() as bq:
    df = bq.bdp(["AAPL US Equity", "SEBA SS Equity"], ["PX_LAST", "CRNCY"])
    print(df)

┌────────────────┬─────────┬───────┐
│ security       ┆ PX_LAST ┆ CRNCY │
│ ---            ┆ ---     ┆ ---   │
│ str            ┆ f64     ┆ str   │
╞════════════════╪═════════╪═══════╡
│ AAPL US Equity ┆ 248.13  ┆ USD   │
│ SEBA SS Equity ┆ 155.2   ┆ SEK   │
└────────────────┴─────────┴───────┘
```

## BDP with different column types

`polars-bloomberg` correctly infers column type as shown in this example:

```python
with BQuery() as bq:
    df = bq.bdp(["XS2930103580 Corp", "USX60003AC87 Corp"],
                ["SECURITY_DES", "YAS_ZSPREAD", "CRNCY", "NXT_CALL_DT"])
    print(df)

┌───────────────────┬────────────────┬─────────────┬───────┬─────────────┐
│ security          ┆ SECURITY_DES   ┆ YAS_ZSPREAD ┆ CRNCY ┆ NXT_CALL_DT │
│ ---               ┆ ---            ┆ ---         ┆ ---   ┆ ---         │
│ str               ┆ str            ┆ f64         ┆ str   ┆ date        │
╞═══════════════════╪════════════════╪═════════════╪═══════╪═════════════╡
│ XS2930103580 Corp ┆ SEB 6 3/4 PERP ┆ 304.676112  ┆ USD   ┆ 2031-11-04  │
│ USX60003AC87 Corp ┆ NDAFH 6.3 PERP ┆ 292.477506  ┆ USD   ┆ 2031-09-25  │
└───────────────────┴────────────────┴─────────────┴───────┴─────────────┘
```

## BDP with overrides
User can submit list of tuples with overrides
```python
with BQuery() as bq:
    df = bq.bdp(
        ["IBM US Equity"],
        ["PX_LAST", "CRNCY_ADJ_PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")],
    )
    print(df)

┌───────────────┬─────────┬───────────────────┐
│ security      ┆ PX_LAST ┆ CRNCY_ADJ_PX_LAST │
│ ---           ┆ ---     ┆ ---               │
│ str           ┆ f64     ┆ f64               │
╞═══════════════╪═════════╪═══════════════════╡
│ IBM US Equity ┆ 230.82  ┆ 2535.174          │
└───────────────┴─────────┴───────────────────┘
```

## BDP with date overrides
Overrides for dates has to be in format YYYYMMDD
```python
with BQuery() as bq:
    df = bq.bdp(["USX60003AC87 Corp"], ["SETTLE_DT"],
                overrides=[("USER_LOCAL_TRADE_DATE", "20241014")])
    print(df)

┌───────────────────┬────────────┐
│ security          ┆ SETTLE_DT  │
│ ---               ┆ ---        │
│ str               ┆ date       │
╞═══════════════════╪════════════╡
│ USX60003AC87 Corp ┆ 2024-10-15 │
└───────────────────┴────────────┘
```

```python
with BQuery() as bq:
    df = bq.bdp(['USDSEK Curncy', 'SEKCZK Curncy'], 
                ['SETTLE_DT', 'PX_LAST'], 
                overrides=[('REFERENCE_DATE', '20200715')]
               )

┌───────────────┬────────────┬─────────┐
│ security      ┆ SETTLE_DT  ┆ PX_LAST │
│ ---           ┆ ---        ┆ ---     │
│ str           ┆ date       ┆ f64     │
╞═══════════════╪════════════╪═════════╡
│ USDSEK Curncy ┆ 2020-07-17 ┆ 10.9778 │
│ SEKCZK Curncy ┆ 2020-07-17 ┆ 2.1698  │
└───────────────┴────────────┴─────────┘
```
