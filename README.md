![Polars Bloomberg Logo](https://raw.githubusercontent.com/MarekOzana/polars-bloomberg/main/assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API
[![Tests](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml/badge.svg)](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**polars-bloomberg** is a Python library that extracts Bloomberg’s financial data directly into [Polars](https://www.pola.rs/) DataFrames.   
If you’re a quant financial analyst, data scientist, or quant developer working in capital markets, this library makes it easy to fetch, transform, and analyze Bloomberg data right in Polars—offering speed, efficient memory usage, and a lot of fun to use!

**Why use polars-bloomberg?**

- **User-Friendly Functions:** Shortcuts like `bdp()`, `bdh()`, and `bql()` (inspired by Excel-like Bloomberg calls) let you pull data with minimal boilerplate.
- **High-Performance Analytics:** Polars is a lightning-fast DataFrame library. Combined with Bloomberg’s rich dataset, you get efficient data retrieval and minimal memory footprint
- **No Pandas Dependency:** Enjoy a clean integration that relies solely on Polars for speed and simplicity.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Core Methods](#core-methods)
    - [BDP (Bloomberg Data Point)](#bdp)
    - [BDH (Bloomberg Data History)](#bdh)
    - [BQL (Bloomberg Query Language)](#bql) <details><summary>BQL Examples</summary>
        - [Single Data Item and Single Security](#simple-bql-example)
        - [Single Item and Multiple Securities](#single-item-with-multiple-securities)
        - [Multiple Items](#multiple-data-items-in-get)
        - [SRCH](#zspread-vs-duration-on-seb-and-shbass-coco-bonds-from-srch)
        - [Aggregation (AVG)](#average-pe-per-sector)
        - [Axes](#axes)
        - [Segments](#segments)
        - [Average Spread per Bucket](#average-issuer-oas-spread-per-maturity-bucket)
        - [Technical Analysis Screening](#technical-analysis-stocks-with-20d-ema--200d-ema-and-rsi--55)
        - [Bonds Universe from Equity](#bond-universe-from-equity-ticker)
        - [Bonds Total Return](#bonds-total-returns)
        </details>
6. [Additional Documentation and Resources](#additional-documentation--resources)

## Introduction
Working with Bloomberg data in Python often feels more complicated than using their well-known Excel interface.
Great projects like [blp](https://github.com/matthewgilbert/blp), [xbbg](https://github.com/alpha-xone/xbbg), and [pdblp](https://github.com/matthewgilbert/pdblp) have made this easier by pulling data directly into pandas. 

With polars-bloomberg, you can enjoy the speed and simplicity of [Polars](https://www.pola.rs/) DataFrames—accessing both familiar Excel-style calls (`bdp`, `bdh`) and advanced `bql` queries—without extra pandas conversions. 

I hope you enjoy using it as much as I had fun building it!


## Prerequisites

- **Bloomberg Access:** A valid Bloomberg terminal license.
- **Bloomberg Python API:** The `blpapi` library must be installed. See the [Bloomberg API Library](https://www.bloomberg.com/professional/support/api-library/) for guidance.
- **Python Version:** Python 3.8+ recommended.

## Installation

```bash
pip install polars-bloomberg
```

# Quick Start
"Hello World" Example (under 1 minute):
```python
from polars_bloomberg import BQuery

# Fetch the latest price for Apple (AAPL US Equity)
with BQuery() as bq:
    df = bq.bdp(["AAPL US Equity"], ["PX_LAST"])
    print(df)

┌────────────────┬─────────┐
│ security       ┆ PX_LAST │
│ ---            ┆ ---     │
│ str            ┆ f64     │
╞════════════════╪═════════╡
│ AAPL US Equity ┆ 248.13  │
└────────────────┴─────────┘
```
What this does:
- Establishes a Bloomberg connection using the context manager.
- Retrieves the last price of Apple shares.
- Returns the result as a Polars DataFrame.

If you see a price in `df`, your setup is working 🤩!!!

## Core Methods
`BQuery` is your main interface. Using a context manager ensures the connection opens and closes cleanly. Within this session, you can use:
- `bq.bdp()` for Bloomberg Data Points (single-value fields).
- `bq.bdh()` for Historical Data (time series).
- `bq.bql()` for complex Bloomberg Query Language requests.

## BDP
Use Case: Fetch the latest single-value data points (like last price, currency, or descriptive fields).

### Example: Fetching the Last Price & Currency of Apple and SEB
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

<details><summary>Expand for more BDP Examples</summary>

### BDP with different column types

`polars-bloomberg` correctly infers column type as shown in this example:

```python
with BQuery() as bq:
    df = bq.bdp(["XS2930103580 Corp", "USX60003AC87 Corp"],
                ["SECURITY_DES", "YAS_ZSPREAD", "CRNCY", "NXT_CALL_DT"])

┌───────────────────┬────────────────┬─────────────┬───────┬─────────────┐
│ security          ┆ SECURITY_DES   ┆ YAS_ZSPREAD ┆ CRNCY ┆ NXT_CALL_DT │
│ ---               ┆ ---            ┆ ---         ┆ ---   ┆ ---         │
│ str               ┆ str            ┆ f64         ┆ str   ┆ date        │
╞═══════════════════╪════════════════╪═════════════╪═══════╪═════════════╡
│ XS2930103580 Corp ┆ SEB 6 3/4 PERP ┆ 304.676112  ┆ USD   ┆ 2031-11-04  │
│ USX60003AC87 Corp ┆ NDAFH 6.3 PERP ┆ 292.477506  ┆ USD   ┆ 2031-09-25  │
└───────────────────┴────────────────┴─────────────┴───────┴─────────────┘
```

### BDP with overrides
User can submit list of tuples with overrides
```python
with BQuery() as bq:
    df = bq.bdp(
        ["IBM US Equity"],
        ["PX_LAST", "CRNCY_ADJ_PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")],
    )

┌───────────────┬─────────┬───────────────────┐
│ security      ┆ PX_LAST ┆ CRNCY_ADJ_PX_LAST │
│ ---           ┆ ---     ┆ ---               │
│ str           ┆ f64     ┆ f64               │
╞═══════════════╪═════════╪═══════════════════╡
│ IBM US Equity ┆ 230.82  ┆ 2535.174          │
└───────────────┴─────────┴───────────────────┘
```

### BDP with date overrides
Overrides for dates has to be in format YYYYMMDD
```python
with BQuery() as bq:
    df = bq.bdp(["USX60003AC87 Corp"], ["SETTLE_DT"],
                overrides=[("USER_LOCAL_TRADE_DATE", "20241014")])

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

</details>

## BDH
Use Case: Retrieve historical data over a date range, such as daily closing prices or volumes.
```python
with BQuery() as bq:
    df = bq.bdh(
        ["TLT US Equity"],
        ["PX_LAST"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 7),
    )
    print(df)

┌───────────────┬────────────┬─────────┐
│ security      ┆ date       ┆ PX_LAST │
│ ---           ┆ ---        ┆ ---     │
│ str           ┆ date       ┆ f64     │
╞═══════════════╪════════════╪═════════╡
│ TLT US Equity ┆ 2019-01-02 ┆ 122.15  │
│ TLT US Equity ┆ 2019-01-03 ┆ 123.54  │
│ TLT US Equity ┆ 2019-01-04 ┆ 122.11  │
│ TLT US Equity ┆ 2019-01-07 ┆ 121.75  │
└───────────────┴────────────┴─────────┘
```

<details><summary>Expand for more BDH examples</summary>

### BDH with multiple securities / fields
```python
with BQuery() as bq:
    df = bq.bdh(
        securities=["SPY US Equity", "TLT US Equity"],
        fields=["PX_LAST", "VOLUME"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 10),
        options={"adjustmentSplit": True},
    )
    print(df)

shape: (14, 4)
┌───────────────┬────────────┬─────────┬──────────────┐
│ security      ┆ date       ┆ PX_LAST ┆ VOLUME       │
│ ---           ┆ ---        ┆ ---     ┆ ---          │
│ str           ┆ date       ┆ f64     ┆ f64          │
╞═══════════════╪════════════╪═════════╪══════════════╡
│ SPY US Equity ┆ 2019-01-02 ┆ 250.18  ┆ 1.26925199e8 │
│ SPY US Equity ┆ 2019-01-03 ┆ 244.21  ┆ 1.44140692e8 │
│ SPY US Equity ┆ 2019-01-04 ┆ 252.39  ┆ 1.42628834e8 │
│ SPY US Equity ┆ 2019-01-07 ┆ 254.38  ┆ 1.031391e8   │
│ SPY US Equity ┆ 2019-01-08 ┆ 256.77  ┆ 1.02512587e8 │
│ …             ┆ …          ┆ …       ┆ …            │
│ TLT US Equity ┆ 2019-01-04 ┆ 122.11  ┆ 1.2970226e7  │
│ TLT US Equity ┆ 2019-01-07 ┆ 121.75  ┆ 8.498104e6   │
│ TLT US Equity ┆ 2019-01-08 ┆ 121.43  ┆ 7.737103e6   │
│ TLT US Equity ┆ 2019-01-09 ┆ 121.24  ┆ 9.349245e6   │
│ TLT US Equity ┆ 2019-01-10 ┆ 120.46  ┆ 8.22286e6    │
└───────────────┴────────────┴─────────┴──────────────┘
```

### BDH with options - periodicitySelection: Monthly
```python
with BQuery() as bq:
    df = bq.bdh(['AAPL US Equity'], 
                ['PX_LAST'], 
                start_date=date(2019, 1, 1), 
                end_date=date(2019, 3, 29),
                options={"periodicitySelection": "MONTHLY"})

┌────────────────┬────────────┬─────────┐
│ security       ┆ date       ┆ PX_LAST │
│ ---            ┆ ---        ┆ ---     │
│ str            ┆ date       ┆ f64     │
╞════════════════╪════════════╪═════════╡
│ AAPL US Equity ┆ 2019-01-31 ┆ 41.61   │
│ AAPL US Equity ┆ 2019-02-28 ┆ 43.288  │
│ AAPL US Equity ┆ 2019-03-29 ┆ 47.488  │
└────────────────┴────────────┴─────────┘
```
</details>


## BQL
*Use Case*: Run more advanced queries to screen securities, calculate analytics (like moving averages), or pull fundamental data with complex conditions.

*Returns*: list of polars dataframes, one per each data-item in `get()`statement.

### Simple BQL Example
```python
# resulting object is list of pl.DataFrames, extract and print the first one
with BQuery() as bq:
    df_lst = bq.bql("get(px_last) for(['IBM US Equity'])")
    print(df_lst[0])
┌───────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴─────────┴────────────┴──────────┘
```
    
### Single Item with Multiple Securities
Another example with single data item but two securities. Still only one pl.DataFrame in 
resulting list (only one data item in `get()`)
```python
with BQuery() as bq:
    df_lst = bq.bql("get(px_last) for(['IBM US Equity', 'SEBA SS Equity'])")

> print(f"n={len(df_lst)}")
n=1

> print(df_lst[0])
┌────────────────┬─────────┬────────────┬──────────┐
│ ID             ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---            ┆ ---     ┆ ---        ┆ ---      │
│ str            ┆ f64     ┆ date       ┆ str      │
╞════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity  ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
│ SEBA SS Equity ┆ 155.2   ┆ 2024-12-14 ┆ SEK      │
└────────────────┴─────────┴────────────┴──────────┘
```

### Multiple data-items in `get`
Lets consider example with two data-items in get statement. Note that the resulting list has two pl.DataFrames.
```python

with BQuery() as bq:
    df_lst = bq.bql("get(name, px_last) for(['IBM US Equity'])")
    
> print(f"n={len(df_lst)}")
n=2

> print(df_lst[0])
┌───────────────┬────────────────────────────────┐
│ ID            ┆ name                           │
│ ---           ┆ ---                            │
│ str           ┆ str                            │
╞═══════════════╪════════════════════════════════╡
│ IBM US Equity ┆ International Business Machine │
└───────────────┴────────────────────────────────┘

> print(df_lst[1])
shape: (1, 4)
┌───────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴─────────┴────────────┴──────────┘
```

Since both DataFrames have teh same index `ID` one can join the results into single table.
```python
>>> print(df_lst[0].join(df_lst[1], on='ID'))

┌───────────────┬────────────────────────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ name                           ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---                            ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ str                            ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪════════════════════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ International Business Machine ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴────────────────────────────────┴─────────┴────────────┴──────────┘
```

### ZSpread vs Duration on SEB and SHBASS CoCo bonds from SRCH
In this example we have three data-items in `get`statement. The universe is from Bloomberg SRCH function
filtered only on tickers 'SEB' and 'SHBASS'.
```python
query="""
    let(#dur=duration(duration_type=MODIFIED); 
        #zsprd=spread(spread_type=Z);) 
    get(name(), #dur, #zsprd) 
    for(filter(screenresults(type=SRCH, screen_name='@COCO'), 
            ticker in ['SEB', 'SHBASS']))
"""

with BQuery() as bq:
    df_lst = bq.bql(query)

    df = df_lst[0].join(df_lst[1], on='ID').join(df_lst[2], on=['ID', 'DATE'])
    print(df)

┌───────────────┬─────────────────┬──────────┬────────────┬────────────┐
│ ID            ┆ name()          ┆ #dur     ┆ DATE       ┆ #zsprd     │
│ ---           ┆ ---             ┆ ---      ┆ ---        ┆ ---        │
│ str           ┆ str             ┆ f64      ┆ date       ┆ f64        │
╞═══════════════╪═════════════════╪══════════╪════════════╪════════════╡
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ 0.395636 ┆ 2024-12-14 ┆ 185.980438 │
│ YV402592 Corp ┆ SEB Float PERP  ┆ 0.212973 ┆ 2024-12-14 ┆ 232.71     │
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ 5.37363  ┆ 2024-12-14 ┆ 308.810572 │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ 4.946231 ┆ 2024-12-14 ┆ 255.85428  │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ 1.956536 ┆ 2024-12-14 ┆ 213.358921 │
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ 2.231859 ┆ 2024-12-14 ┆ 211.55125  │
└───────────────┴─────────────────┴──────────┴────────────┴────────────┘
```

### Average PE per Sector
This example shows aggregation (average) per group (sector) for members of an index.
The reulting list has only one element since there is only one data-item in `get`
```python
query = """
    let(#avg_pe=avg(group(pe_ratio(), gics_sector_name()));)
    get(#avg_pe)
    for(members('OMX Index'))
"""
with BQuery() as bq:
    df_lst = bq.bql(query)
    print(df_lst[0].head(5))

┌──────────────┬───────────┬──────────────┬────────────┬──────────────┬──────────────┬─────────────┐
│ ID           ┆ #avg_pe   ┆ REVISION_DAT ┆ AS_OF_DATE ┆ PERIOD_END_D ┆ ORIG_IDS     ┆ GICS_SECTOR │
│ ---          ┆ ---       ┆ E            ┆ ---        ┆ ATE          ┆ ---          ┆ _NAME()     │
│ str          ┆ f64       ┆ ---          ┆ date       ┆ ---          ┆ str          ┆ ---         │
│              ┆           ┆ date         ┆            ┆ date         ┆              ┆ str         │
╞══════════════╪═══════════╪══════════════╪════════════╪══════════════╪══════════════╪═════════════╡
│ Communicatio ┆ 19.561754 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Communicati │
│ n Services   ┆           ┆              ┆            ┆              ┆              ┆ on Services │
│ Consumer Dis ┆ 19.117295 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Consumer    │
│ cretionary   ┆           ┆              ┆            ┆              ┆              ┆ Discretiona │
│              ┆           ┆              ┆            ┆              ┆              ┆ ry          │
│ Consumer     ┆ 15.984743 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ ESSITYB SS   ┆ Consumer    │
│ Staples      ┆           ┆              ┆            ┆              ┆ Equity       ┆ Staples     │
│ Financials   ┆ 6.815895  ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Financials  │
│ Health Care  ┆ 22.00628  ┆ 2024-11-12   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Health Care │
└──────────────┴───────────┴──────────────┴────────────┴──────────────┴──────────────┴─────────────┘
```

### Axes
Get current axes of all Swedish USD AT1 bonds
```python
# Get current axes for Swedish AT1 bonds in USD
query="""
    let(#ax=axes();)
    get(security_des, #ax)
    for(filter(bondsuniv(ACTIVE),
        crncy()=='USD' and
        basel_iii_designation() == 'Additional Tier 1' and
        country_iso() == 'SE'))
"""

with BQuery() as bq:
    df_lst = bq.bql(query)
    print(df_lst[0].join(df_lst[1], on='ID'))

┌───────────────┬─────────────────┬─────┬───────────┬───────────┬────────────────┬────────────────┐
│ ID            ┆ security_des    ┆ #ax ┆ ASK_DEPTH ┆ BID_DEPTH ┆ ASK_TOTAL_SIZE ┆ BID_TOTAL_SIZE │
│ ---           ┆ ---             ┆ --- ┆ ---       ┆ ---       ┆ ---            ┆ ---            │
│ str           ┆ str             ┆ str ┆ i64       ┆ i64       ┆ f64            ┆ f64            │
╞═══════════════╪═════════════════╪═════╪═══════════╪═══════════╪════════════════╪════════════════╡
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ BR069680 Corp ┆ SWEDA 4 PERP    ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ ZL122341 Corp ┆ SWEDA 7 ⅝ PERP  ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ ZF859199 Corp ┆ SWEDA 7 ¾ PERP  ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ N   ┆ null      ┆ null      ┆ null           ┆ null           │
└───────────────┴─────────────────┴─────┴───────────┴───────────┴────────────────┴────────────────┘
```

### Segments
The following example shows handling of two data-items with different length. Teh first dataframe 
describes the segments (and has length 5 in this case), while the second dataframe contains time series.
One can join teh dataframes on common columns and pivot the segments into columns as shown below:
```python
# revenue per segment
query = """
    let(#segment=segment_name();
        #revenue=sales_Rev_turn(fpt=q, fpr=range(2023Q3, 2024Q3));
        )
    get(#segment, #revenue)
    for(segments('GTN US Equity',type=reported,hierarchy=PRODUCT, level=1))
"""
with BQuery() as bq:
    df_lst = bq.bql(query)
    df = (
        df_lst[0]
        .join(df_lst[1], on=["ID", "ID_DATE", "AS_OF_DATE"])
        .pivot(index="PERIOD_END_DATE", on="#segment", values="#revenue")
    )
    print(df)

┌─────────────────┬──────────────┬──────────────────────┬────────┬────────────┐
│ PERIOD_END_DATE ┆ Broadcasting ┆ Production Companies ┆ Other  ┆ Adjustment │
│ ---             ┆ ---          ┆ ---                  ┆ ---    ┆ ---        │
│ date            ┆ f64          ┆ f64                  ┆ f64    ┆ f64        │
╞═════════════════╪══════════════╪══════════════════════╪════════╪════════════╡
│ 2023-09-30      ┆ 7.83e8       ┆ 2e7                  ┆ 1.6e7  ┆ null       │
│ 2023-12-31      ┆ 8.13e8       ┆ 3.2e7                ┆ 1.9e7  ┆ null       │
│ 2024-03-31      ┆ 7.8e8        ┆ 2.4e7                ┆ 1.9e7  ┆ null       │
│ 2024-06-30      ┆ 8.08e8       ┆ 1.8e7                ┆ 0.0    ┆ null       │
│ 2024-09-30      ┆ 9.24e8       ┆ 2.6e7                ┆ 1.7e7  ┆ null       │
└─────────────────┴──────────────┴──────────────────────┴────────┴────────────┘
```

### Actual and Forward EPS Estimates
```python
with BQuery() as bq:
    df_lst = bq.bql("""
        let(#eps=is_eps(fa_period_type='A',
                        fa_period_offset=range(-4,2));)
        get(#eps)
        for(['IBM US Equity'])
    """)
    print(df_lst[0])

┌───────────────┬───────┬───────────────┬────────────┬─────────────────┬──────────┐
│ ID            ┆ #eps  ┆ REVISION_DATE ┆ AS_OF_DATE ┆ PERIOD_END_DATE ┆ CURRENCY │
│ ---           ┆ ---   ┆ ---           ┆ ---        ┆ ---             ┆ ---      │
│ str           ┆ f64   ┆ date          ┆ date       ┆ date            ┆ str      │
╞═══════════════╪═══════╪═══════════════╪════════════╪═════════════════╪══════════╡
│ IBM US Equity ┆ 10.63 ┆ 2022-02-22    ┆ 2024-12-14 ┆ 2019-12-31      ┆ USD      │
│ IBM US Equity ┆ 6.28  ┆ 2023-02-28    ┆ 2024-12-14 ┆ 2020-12-31      ┆ USD      │
│ IBM US Equity ┆ 6.41  ┆ 2023-02-28    ┆ 2024-12-14 ┆ 2021-12-31      ┆ USD      │
│ IBM US Equity ┆ 1.82  ┆ 2024-03-18    ┆ 2024-12-14 ┆ 2022-12-31      ┆ USD      │
│ IBM US Equity ┆ 8.23  ┆ 2024-03-18    ┆ 2024-12-14 ┆ 2023-12-31      ┆ USD      │
│ IBM US Equity ┆ 7.891 ┆ 2024-12-13    ┆ 2024-12-14 ┆ 2024-12-31      ┆ USD      │
│ IBM US Equity ┆ 9.236 ┆ 2024-12-13    ┆ 2024-12-14 ┆ 2025-12-31      ┆ USD      │
└───────────────┴───────┴───────────────┴────────────┴─────────────────┴──────────┘
```

### Average issuer OAS spread per maturity bucket
```python
# Example: Average OAS-spread per maturity bucket
query = """
let(
    #bins = bins(maturity_years,
                 [3,9,18,30],
                 ['(1) 0-3','(2) 3-9','(3) 9-18','(4) 18-30','(5) 30+']);
    #average_spread = avg(group(spread(st=oas),#bins));
)
get(#average_spread)
for(filter(bonds('NVDA US Equity', issuedby = 'ENTITY'),
           maturity_years != NA))
"""

with BQuery() as bq:
    df_lst = bq.bql(query)
    print(df_lst[0])

┌───────────┬─────────────────┬────────────┬───────────────┬───────────┐
│ ID        ┆ #average_spread ┆ DATE       ┆ ORIG_IDS      ┆ #BINS     │
│ ---       ┆ ---             ┆ ---        ┆ ---           ┆ ---       │
│ str       ┆ f64             ┆ date       ┆ str           ┆ str       │
╞═══════════╪═════════════════╪════════════╪═══════════════╪═══════════╡
│ (1) 0-3   ┆ 31.195689       ┆ 2024-12-14 ┆ QZ552396 Corp ┆ (1) 0-3   │
│ (2) 3-9   ┆ 59.580383       ┆ 2024-12-14 ┆ null          ┆ (2) 3-9   │
│ (3) 9-18  ┆ 110.614416      ┆ 2024-12-14 ┆ BH393780 Corp ┆ (3) 9-18  │
│ (4) 18-30 ┆ 135.160279      ┆ 2024-12-14 ┆ BH393781 Corp ┆ (4) 18-30 │
│ (5) 30+   ┆ 150.713405      ┆ 2024-12-14 ┆ BH393782 Corp ┆ (5) 30+   │
└───────────┴─────────────────┴────────────┴───────────────┴───────────┘
```

### Technical Analysis: stocks with 20d EMA > 200d EMA and RSI > 55
```python
with BQuery() as bq:
    df_lst = bq.bql(
        """
        let(#ema20=emavg(period=20);
            #ema200=emavg(period=200);
            #rsi=rsi(close=px_last());)
        get(name(), #ema20, #ema200, #rsi)
        for(filter(members('OMX Index'),
                    and(#ema20 > #ema200, #rsi > 55)))
        with(fill=PREV)
        """
    )
    df = (
        df_lst[0]
        .join(df_lst[1], on="ID")
        .join(df_lst[2], on=["ID", "DATE", "CURRENCY"])
        .join(df_lst[3], on=["ID", "DATE"])
    )
    print(df)

┌─────────────────┬──────────────────┬────────────┬────────────┬──────────┬────────────┬───────────┐
│ ID              ┆ name()           ┆ #ema20     ┆ DATE       ┆ CURRENCY ┆ #ema200    ┆ #rsi      │
│ ---             ┆ ---              ┆ ---        ┆ ---        ┆ ---      ┆ ---        ┆ ---       │
│ str             ┆ str              ┆ f64        ┆ date       ┆ str      ┆ f64        ┆ f64       │
╞═════════════════╪══════════════════╪════════════╪════════════╪══════════╪════════════╪═══════════╡
│ ERICB SS Equity ┆ Telefonaktiebola ┆ 90.094984  ┆ 2024-12-14 ┆ SEK      ┆ 74.917219  ┆ 57.454412 │
│                 ┆ get LM Ericsso   ┆            ┆            ┆          ┆            ┆           │
│ SKFB SS Equity  ┆ SKF AB           ┆ 214.383743 ┆ 2024-12-14 ┆ SEK      ┆ 205.174139 ┆ 58.403269 │
│ SEBA SS Equity  ┆ Skandinaviska    ┆ 153.680261 ┆ 2024-12-14 ┆ SEK      ┆ 150.720922 ┆ 57.692703 │
│                 ┆ Enskilda Banken  ┆            ┆            ┆          ┆            ┆           │
│ ASSAB SS Equity ┆ Assa Abloy AB    ┆ 338.829971 ┆ 2024-12-14 ┆ SEK      ┆ 316.8212   ┆ 55.467329 │
│ SWEDA SS Equity ┆ Swedbank AB      ┆ 217.380431 ┆ 2024-12-14 ┆ SEK      ┆ 213.776784 ┆ 56.303481 │
└─────────────────┴──────────────────┴────────────┴────────────┴──────────┴────────────┴───────────┘
```

### Bond Universe from Equity Ticker
```python
query = """
let(#rank=normalized_payment_rank();
    #oas=spread(st=oas);
    #nxt_call=nxt_call_dt();
    )
get(name(), #rank, #nxt_call, #oas)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""

with BQuery() as bq:
    df_lst = bq.bql(query)

    df = (
        df_lst[0]
        .join(df_lst[1], on="ID")
        .join(df_lst[2], on="ID")
        .join(df_lst[3], on="ID")
    )
    print(df)

┌───────────────┬───────────────────┬──────────────────┬────────────┬─────────────┬────────────┐
│ ID            ┆ name()            ┆ #rank            ┆ #nxt_call  ┆ #oas        ┆ DATE       │
│ ---           ┆ ---               ┆ ---              ┆ ---        ┆ ---         ┆ ---        │
│ str           ┆ str               ┆ str              ┆ date       ┆ f64         ┆ date       │
╞═══════════════╪═══════════════════╪══════════════════╪════════════╪═════════════╪════════════╡
│ YX231113 Corp ┆ GTN 10 ½ 07/15/29 ┆ 1st Lien Secured ┆ 2026-07-15 ┆ 597.329513  ┆ 2024-12-14 │
│ BS116983 Corp ┆ GTN 5 ⅜ 11/15/31  ┆ Sr Unsecured     ┆ 2026-11-15 ┆ 1192.83614  ┆ 2024-12-14 │
│ AV438089 Corp ┆ GTN 7 05/15/27    ┆ Sr Unsecured     ┆ 2024-12-23 ┆ 391.133436  ┆ 2024-12-14 │
│ ZO860846 Corp ┆ GTN 4 ¾ 10/15/30  ┆ Sr Unsecured     ┆ 2025-10-15 ┆ 1232.554695 ┆ 2024-12-14 │
│ LW375188 Corp ┆ GTN 5 ⅞ 07/15/26  ┆ Sr Unsecured     ┆ 2025-01-12 ┆ 171.708702  ┆ 2024-12-14 │
└───────────────┴───────────────────┴──────────────────┴────────────┴─────────────┴────────────┘
```

### Bonds Total Returns
This is example of a single-item query returning total return for all GTN bonds in a long dataframe.
We can easily pivot it into wide format, as in the example below
```python
# Total Return of GTN Bonds
query="""
let(#rng = range(-1M, 0D);
    #rets = return_series(calc_interval=#rng,per=W);)
get(#rets)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""

with BQuery() as bq:
    df_lst = bq.bql(query)
    df = df_lst[0].pivot(on='ID', index='DATE', values='#rets')
    print(df)

┌────────────┬───────────────┬───────────────┬───────────────┬───────────────┬───────────────┐
│ DATE       ┆ YX231113 Corp ┆ BS116983 Corp ┆ AV438089 Corp ┆ ZO860846 Corp ┆ LW375188 Corp │
│ ---        ┆ ---           ┆ ---           ┆ ---           ┆ ---           ┆ ---           │
│ date       ┆ f64           ┆ f64           ┆ f64           ┆ f64           ┆ f64           │
╞════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╡
│ 2024-11-14 ┆ null          ┆ null          ┆ null          ┆ null          ┆ null          │
│ 2024-11-21 ┆ -0.002378     ┆ 0.016565      ┆ 0.022831      ┆ 0.000987      ┆ -0.002815     │
│ 2024-11-28 ┆ 0.002345      ┆ -0.005489     ┆ -0.004105     ┆ 0.011748      ┆ 0.00037       │
│ 2024-12-05 ┆ 0.001403      ┆ 0.016999      ┆ 0.002058      ┆ 0.013095      ┆ 0.001003      │
│ 2024-12-12 ┆ -0.000485     ┆ -0.040228     ┆ -0.000872     ┆ -0.038048     ┆ 0.001122      │
│ 2024-12-14 ┆ 0.000988      ┆ -0.003833     ┆ 0.000247      ┆ -0.004818     ┆ 0.00136       │
└────────────┴───────────────┴───────────────┴───────────────┴───────────────┴───────────────┘
```


## Additional Documentation & Resources

- *API Documentation*: Detailed documentation and function references are available in the [API documentation](examples/API-docs.md) file within the `examples/` directory.

- *Additional Examples*: Check out (examples/Examples.ipynb) for hands-on notebooks demonstrating a variety of use cases.

- *Bloomberg Developer Resources*: For more details on the Bloomberg API itself, visit the [Bloomberg Developer's page](https://developer.bloomberg.com/).
