<!-- markdownlint-disable MD033 MD041 -->
<div align="center">

<img src="https://raw.githubusercontent.com/MarekOzana/polars-bloomberg/main/assets/polars-bloomberg-logo.jpg" alt="Polars Bloomberg Logo">

# Polars + Bloomberg Open API

[![PyPI version](https://img.shields.io/pypi/v/polars-bloomberg.svg?color=5FA8FF)](https://pypi.org/project/polars-bloomberg/)
[![Python versions](https://img.shields.io/pypi/pyversions/polars-bloomberg.svg)](https://pypi.org/project/polars-bloomberg/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/polars-bloomberg?color=4c1)](https://pypistats.org/packages/polars-bloomberg)
[![Docs](https://img.shields.io/badge/docs-site-1E3A8A?logo=readthedocs&logoColor=white)](https://marekozana.github.io/polars-bloomberg/)
<br>
[![Tests](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml/badge.svg)](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

</div>
<!-- markdownlint-enable MD033 MD041 -->

**polars-bloomberg** is a Python library that extracts Bloomberg's financial data directly into [Polars](https://www.pola.rs/) DataFrames.
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
    - [BDIB (Bloomberg Data Intraday Bar)](#bdib)
    - [BSRCH (Bloomberg Search)](#bsrch)
    - [BQL (Bloomberg Query Language)](#bql) <details><summary>BQL Examples</summary>
        - [Single Item and Single Security](#1-basic-example-single-item-and-single-security)
        - [Multiple Securities with Single Item](#2-multiple-securities-with-a-single-item)
        - [Multiple Items](#3-multiple-items)
        - [SRCH](#4-advanced-example-screening-securities)
        - [Aggregation (AVG)](#average-pe-per-sector)
        - [Axes](#axes)
        - [Axes with All Columns](#axes-with-all-columns)
        - [Segments](#segments)
        - [Average Spread per Bucket](#average-issuer-oas-spread-per-maturity-bucket)
        - [Technical Analysis Screening](#technical-analysis-stocks-with-20d-ema--200d-ema-and-rsi--53)
        - [Bonds Universe from Equity](#bond-universe-from-equity-ticker)
        - [Bonds Total Return](#bonds-total-returns)
        - [Maturity Wall for US HY](#maturity-wall-for-us-hy-bonds)
        </details>
6. [Additional Documentation and Resources](#additional-documentation--resources)

## Introduction
Working with Bloomberg data in Python often feels more complicated than using their well-known Excel interface.
Great projects like [blp](https://github.com/matthewgilbert/blp), [xbbg](https://github.com/alpha-xone/xbbg), and [pdblp](https://github.com/matthewgilbert/pdblp) have made this easier by pulling data directly into pandas.

With polars-bloomberg, you can enjoy the speed and simplicity of [Polars](https://www.pola.rs/) DataFrames—accessing both familiar Excel-style calls (`bdp`, `bdh` ,`bdip`, `bsrch`) and advanced `bql` queries—without extra pandas conversions.

For detailed documentation and function references, visit the documentation site [https://marekozana.github.io/polars-bloomberg](https://marekozana.github.io/polars-bloomberg/).

I hope you enjoy using it as much as I had fun building it!


## Prerequisites

- **Bloomberg Access:** A valid Bloomberg terminal license.
- **Bloomberg Python API:** The `blpapi` library must be installed. See the [Bloomberg API Library](https://www.bloomberg.com/professional/support/api-library/) for guidance.
- **Python Version:** Python 3.12+ recommended.

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
- `bq.bsrch()` for saved custom SRCH searches and BI templates.
- `bq.bdip()` for intraday bars

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


## BDIB
Use Case: Retrieve intraday bars (1- to 1440-minute intervals) over a precise intraday
window without managing tick aggregation yourself.

```python
with BQuery() as bq:  # set debug=False for normal usage
    df = bq.bdib(
        "OMX Index",
        event_type="TRADE",
        interval=60,
        start_datetime=datetime(2025, 11, 5),
        end_datetime=datetime(2025, 11, 5, 12),
    )
    print(df)
```

Output:
```
shape: (4, 9)
┌───────────┬──────────────┬──────────┬──────────┬───┬──────────┬────────┬───────────┬───────┐
│ security  ┆ time         ┆ open     ┆ high     ┆ … ┆ close    ┆ volume ┆ numEvents ┆ value │
│ ---       ┆ ---          ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---    ┆ ---       ┆ ---   │
│ str       ┆ datetime[μs] ┆ f64      ┆ f64      ┆   ┆ f64      ┆ i64    ┆ i64       ┆ f64   │
╞═══════════╪══════════════╪══════════╪══════════╪═══╪══════════╪════════╪═══════════╪═══════╡
│ OMX Index ┆ 2025-11-05   ┆ 2726.603 ┆ 2742.014 ┆ … ┆ 2739.321 ┆ 0      ┆ 3591      ┆ 0.0   │
│           ┆ 08:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2739.466 ┆ 2739.706 ┆ … ┆ 2733.836 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 09:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2733.747 ┆ 2734.827 ┆ … ┆ 2731.724 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 10:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2731.721 ┆ 2742.015 ┆ … ┆ 2741.185 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 11:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
└───────────┴──────────────┴──────────┴──────────┴───┴──────────┴────────┴───────────┴───────┘
```

Each row is a 60-minute bar built from TRADE events, and the `time` column is returned
in UTC (matching Bloomberg's wide format).

## BSRCH
Use Case: Excel-style searches (SRCH/BI domains). Supports overrides such as `LIMIT` and custom keys (e.g., `BIKEY`).

### Small example: two COCO bonds (limit = 2)
```python
with BQuery() as bq:
    df = bq.bsrch("FI:SRCHEX.@COCO", overrides={"LIMIT": 2})
    print(df)
```
Example output:
```
shape: (2, 1)
┌──────────────┐
│ id           │
│ ---          │
│ str          │
╞══════════════╡
│ DA785784 Corp│
│ DA773901 Corp│
└──────────────┘
```

### Larger example: BI template (BI:TPD) with BIKEY and LIMIT
```python
with BQuery() as bq:
    df = bq.bsrch(
        "BI:TPD",
        overrides={
            "BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR",
            "LIMIT": 20000,  # avoid ReachMax warning
        },
    )
    print(df.head())
```
Example output (truncated):
```
shape: (16, 6)
┌──────────┬──────────┬─────────┬──────────┬──────────┬─────────┐
│ Main_Cat ┆ Bclass3_ ┆ Category┆ 06/30/20 ┆ 03/31/20 ┆ 12/31/20│
│ ...      ┆ ...      ┆ ...     ┆ 25       ┆ 25       ┆ 24      │
│ str      ┆ str      ┆ str     ┆ f64      ┆ f64      ┆ f64     │
╞══════════╪══════════╪═════════╪══════════╪══════════╪═════════╡
│ Leverage ┆ Non-Fin… ┆ B       ┆ 3.956051 ┆ 4.118212 ┆ 4.269732│
│ …        ┆ …        ┆ …       ┆ …        ┆ …        ┆ …       │
└──────────┴──────────┴─────────┴──────────┴──────────┴─────────┘
```

## BQL
*Use Case*: Run more advanced queries to screen securities, calculate analytics (like moving averages), or pull fundamental data with complex conditions.

*Returns*: The `bql()` method returns a `BqlResult` object, which:
- Acts like a list of Polars DataFrames (one for each item in BQL `get` statement).
- Provides a `.combine()` method to merge DataFrames. With no arguments it preserves
  the legacy behavior of joining on common columns. Use `.combine(on=...)` to join
  only on explicit keys such as `"ID"` or `["ID", "DATE"]`.

### 1. Basic Example: Single Item and Single Security
```python
# Fetch the last price of IBM stock
with BQuery() as bq:
    results = bq.bql("get(px_last) for(['IBM US Equity'])")
    print(results[0])  # Access the first DataFrame
```
Output:
```python
┌───────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴─────────┴────────────┴──────────┘
```

### 2. Multiple Securities with a Single Item
```python
# Fetch the last price for IBM and SEB
with BQuery() as bq:
    results = bq.bql("get(px_last) for(['IBM US Equity', 'SEBA SS Equity'])")
    print(results[0])
```
Output:
```python
┌────────────────┬─────────┬────────────┬──────────┐
│ ID             ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---            ┆ ---     ┆ ---        ┆ ---      │
│ str            ┆ f64     ┆ date       ┆ str      │
╞════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity  ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
│ SEBA SS Equity ┆ 155.2   ┆ 2024-12-14 ┆ SEK      │
└────────────────┴─────────┴────────────┴──────────┘
```

### 3. Multiple Items
When querying for multiple items, `bql()` returns a list of DataFrames
```python
# Fetch name and last price of IBM (two items)
with BQuery() as bq:
    results = bq.bql("get(name, px_last) for(['IBM US Equity'])")
```
Output:
```python
>>> print(len(results))  # 2 DataFrames
n=2

>>> print(results[0])    # First DataFrame: 'name'
┌───────────────┬────────────────────────────────┐
│ ID            ┆ name                           │
│ ---           ┆ ---                            │
│ str           ┆ str                            │
╞═══════════════╪════════════════════════════════╡
│ IBM US Equity ┆ International Business Machine │
└───────────────┴────────────────────────────────┘

>>> print(results[1])    # Second DataFrame: 'px_last'
┌───────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴─────────┴────────────┴──────────┘
```

#### Combining Results
```python
>>> combined_df = results.combine()
>>> print(combined_df)
```
Output:
```python
┌───────────────┬────────────────────────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ name                           ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---                            ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ str                            ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪════════════════════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ International Business Machine ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴────────────────────────────────┴─────────┴────────────┴──────────┘
```

By default, `combine()` keeps the historical behavior and joins each result
DataFrame on all common column names. This is convenient for simple BQL results, but
it can be too broad when several item tables share metadata columns such as `DATE`,
`CURRENCY`, `PERIOD`, `VALUE`, or `MULTIPLIER`.

For safer joins, pass the intended key columns explicitly:

```python
# Join only by security ID. Other common columns are kept as data columns.
combined_df = results.combine(on="ID")

# Join by a compound key when both columns are true row identifiers.
combined_df = results.combine(on=["ID", "DATE"])

# Use Polars join names. "full" is the default for explicit joins.
combined_df = results.combine(on="ID", how="inner")
```

If non-key columns overlap when using `on=...`, they are preserved with suffixes
based on the BQL item name, for example `DATE_#ret_1d`. To fail instead of
suffixing overlapping non-key columns, use:

```python
combined_df = results.combine(on="ID", allow_common_columns=False)
```

### 4. Advanced Example: Screening Securities
Find list of SEB and Handelsbanken's AT1 bonds and print their names, duration and Z-Spread.
```python
query="""
    let(#dur=duration(duration_type=MODIFIED);
        #zsprd=spread(spread_type=Z);)
    get(name(), #dur, #zsprd)
    for(filter(screenresults(type=SRCH, screen_name='@COCO'),
            ticker in ['SEB', 'SHBASS']))
"""

with BQuery() as bq:
    results = bq.bql(query)
    combined_df = results.combine()
    print(combined_df)
```
Output:
```python
┌───────────────┬─────────────────┬──────┬────────────┬────────┐
│ ID            ┆ name()          ┆ #dur ┆ DATE       ┆ #zsprd │
│ ---           ┆ ---             ┆ ---  ┆ ---        ┆ ---    │
│ str           ┆ str             ┆ f64  ┆ date       ┆ f64    │
╞═══════════════╪═════════════════╪══════╪════════════╪════════╡
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ 2.23 ┆ 2024-12-16 ┆ 212.0  │
│ YV402592 Corp ┆ SEB Float PERP  ┆ 0.21 ┆ 2024-12-16 ┆ 233.0  │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ 0.39 ┆ 2024-12-16 ┆ 186.0  │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ 1.95 ┆ 2024-12-16 ┆ 213.0  │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ 4.94 ┆ 2024-12-16 ┆ 256.0  │
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ 5.37 ┆ 2024-12-16 ┆ 309.0  │
└───────────────┴─────────────────┴──────┴────────────┴────────┘
```

### Average PE per Sector
This example shows aggregation (average) per group (sector) for members of an index.
The resulting list has only one element since there is only one data-item in `get`
```python
query = """
    let(#avg_pe=avg(group(pe_ratio(), gics_sector_name()));)
    get(#avg_pe)
    for(members('OMX Index'))
"""
with BQuery() as bq:
    results = bq.bql(query)
    print(results[0].head(5))
```
Output:
```python
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
    results = bq.bql(query)
    print(results.combine())

┌───────────────┬─────────────────┬─────┬───────────┬───────────┬────────────────┬────────────────┐
│ ID            ┆ security_des    ┆ #ax ┆ ASK_DEPTH ┆ BID_DEPTH ┆ ASK_TOTAL_SIZE ┆ BID_TOTAL_SIZE │
│ ---           ┆ ---             ┆ --- ┆ ---       ┆ ---       ┆ ---            ┆ ---            │
│ str           ┆ str             ┆ str ┆ i64       ┆ i64       ┆ f64            ┆ f64            │
╞═══════════════╪═════════════════╪═════╪═══════════╪═══════════╪════════════════╪════════════════╡
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ Y   ┆ 2         ┆ null      ┆ 5.6e6          ┆ null           │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ Y   ┆ 1         ┆ 2         ┆ 5e6            ┆ 6e6            │
│ BR069680 Corp ┆ SWEDA 4 PERP    ┆ Y   ┆ null      ┆ 1         ┆ null           ┆ 3e6            │
│ ZL122341 Corp ┆ SWEDA 7 ⅝ PERP  ┆ Y   ┆ null      ┆ 6         ┆ null           ┆ 2.04e7         │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ Y   ┆ 2         ┆ 4         ┆ 5.5e6          ┆ 3e7            │
│ ZF859199 Corp ┆ SWEDA 7 ¾ PERP  ┆ Y   ┆ 1         ┆ 1         ┆ 2e6            ┆ 2e6            │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ Y   ┆ 1         ┆ 3         ┆ 1.2e6          ┆ 1.1e7          │
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ Y   ┆ 1         ┆ 3         ┆ 5e6            ┆ 1.1e7          │
└───────────────┴─────────────────┴─────┴───────────┴───────────┴────────────────┴────────────────┘
```

### Axes with all columns
```python
# RT1 Axes with all columns
query = """
let(#ax=axes();)
get(name, #ax, amt_outstanding)
for(filter(bondsuniv(ACTIVE),
    crncy() in ['USD', 'EUR'] and
    solvency_ii_designation() == 'Restricted Tier 1' and
    amt_outstanding() > 7.5e8 and
    is_axed('Bid') == True))
preferences(addcols=all)
"""

with BQuery() as bq:
    results = bq.bql(query)
    print(results.combine())
```
Output:
<div>
<small>shape: (3, 33)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>name</th><th>#ax</th><th>ASK_PRICE</th><th>BID_PRICE</th><th>ASK_DEPTH</th><th>BID_DEPTH</th><th>ASK_DEALER</th><th>BID_DEALER</th><th>ASK_SIZE</th><th>BID_SIZE</th><th>ASK_TOTAL_SIZE</th><th>BID_TOTAL_SIZE</th><th>ASK_PRICE_IS_DERIVED</th><th>BID_PRICE_IS_DERIVED</th><th>ASK_SPREAD</th><th>BID_SPREAD</th><th>ASK_SPREAD_IS_DERIVED</th><th>BID_SPREAD_IS_DERIVED</th><th>ASK_YIELD</th><th>BID_YIELD</th><th>ASK_YIELD_IS_DERIVED</th><th>BID_YIELD_IS_DERIVED</th><th>ASK_AXE_SOURCE</th><th>BID_AXE_SOURCE</th><th>ASK_BROKER</th><th>BID_BROKER</th><th>ASK_HIST_AGG_SIZE</th><th>BID_HIST_AGG_SIZE</th><th>amt_outstanding</th><th>CURRENCY_OF_ISSUE</th><th>MULTIPLIER</th><th>CURRENCY</th></tr><tr><td>str</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>i64</td><td>i64</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>bool</td><td>bool</td><td>f64</td><td>f64</td><td>bool</td><td>bool</td><td>f64</td><td>f64</td><td>bool</td><td>bool</td><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>f64</td><td>str</td></tr></thead><tbody><tr><td>&quot;BM368057 Corp&quot;</td><td>&quot;ALVGR 2 ⅝ PERP&quot;</td><td>&quot;Y&quot;</td><td>88.034</td><td>87.427</td><td>5</td><td>1</td><td>&quot;BARC&quot;</td><td>&quot;IMI&quot;</td><td>1.2e6</td><td>1e6</td><td>7.2e6</td><td>1e6</td><td>null</td><td>null</td><td>287.031</td><td>300.046</td><td>true</td><td>true</td><td>4.854</td><td>4.976</td><td>true</td><td>true</td><td>&quot;ERUN&quot;</td><td>&quot;ERUN&quot;</td><td>&quot;BXOL&quot;</td><td>&quot;IMIC&quot;</td><td>6.68e6</td><td>8.92e6</td><td>1.2500e9</td><td>&quot;EUR&quot;</td><td>1.0</td><td>&quot;EUR&quot;</td></tr><tr><td>&quot;EK588238 Corp&quot;</td><td>&quot;ASSGEN 4.596 PERP&quot;</td><td>&quot;Y&quot;</td><td>101.0</td><td>100.13</td><td>4</td><td>6</td><td>&quot;MSAX&quot;</td><td>&quot;A2A&quot;</td><td>500000.0</td><td>100000.0</td><td>1.556e7</td><td>3.83e7</td><td>null</td><td>null</td><td>108.9</td><td>207.889</td><td>true</td><td>true</td><td>3.466</td><td>4.434</td><td>null</td><td>true</td><td>&quot;ERUN&quot;</td><td>&quot;BBX&quot;</td><td>&quot;MSAX&quot;</td><td>&quot;A2A&quot;</td><td>1.70424e7</td><td>3.17e7</td><td>1.0004e9</td><td>&quot;EUR&quot;</td><td>1.0</td><td>&quot;EUR&quot;</td></tr><tr><td>&quot;BR244025 Corp&quot;</td><td>&quot;ALVGR 3.2 PERP&quot;</td><td>&quot;Y&quot;</td><td>88.0</td><td>86.875</td><td>3</td><td>4</td><td>&quot;UBS&quot;</td><td>&quot;DB&quot;</td><td>5e6</td><td>1e6</td><td>1.1e7</td><td>1.4e7</td><td>null</td><td>null</td><td>49.33</td><td>414.602</td><td>true</td><td>true</td><td>7.34258</td><td>8.553</td><td>null</td><td>true</td><td>&quot;ERUN&quot;</td><td>&quot;ERUN&quot;</td><td>&quot;UBSW&quot;</td><td>&quot;DABC&quot;</td><td>1.6876e6</td><td>3.6e7</td><td>1.2500e9</td><td>&quot;USD&quot;</td><td>1.0</td><td>&quot;USD&quot;</td></tr></tbody></table></div>


### Segments
The following example shows handling of two data-items with different length. The first dataframe
describes the segments (and has length 5 in this case), while the second dataframe contains time series.
One can join the dataframes on common columns and pivot the segments into columns as shown below:
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
    results = bq.bql(query)
    df = results.combine().pivot(
        index="PERIOD_END_DATE", on="#segment", values="#revenue"
    )
    print(df)
```
Output:
```python
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
    results = bq.bql("""
        let(#eps=is_eps(fa_period_type='A',
                        fa_period_offset=range(-4,2));)
        get(#eps)
        for(['IBM US Equity'])
    """)
    print(results[0])

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
    results = bq.bql(query)
    print(results[0])
```
Output:
```python
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

### Technical Analysis: stocks with 20d EMA > 200d EMA and RSI > 53
```python
with BQuery() as bq:
    results = bq.bql(
        """
        let(#ema20=emavg(period=20);
            #ema200=emavg(period=200);
            #rsi=rsi(close=px_last());)
        get(name(), #ema20, #ema200, #rsi)
        for(filter(members('OMX Index'),
                    and(#ema20 > #ema200, #rsi > 53)))
        with(fill=PREV)
        """
    )
    print(results.combine())
```
Output:
```python
┌─────────────────┬──────────────────┬────────────┬────────────┬──────────┬────────────┬───────────┐
│ ID              ┆ name()           ┆ #ema20     ┆ DATE       ┆ CURRENCY ┆ #ema200    ┆ #rsi      │
│ ---             ┆ ---              ┆ ---        ┆ ---        ┆ ---      ┆ ---        ┆ ---       │
│ str             ┆ str              ┆ f64        ┆ date       ┆ str      ┆ f64        ┆ f64       │
╞═════════════════╪══════════════════╪════════════╪════════════╪══════════╪════════════╪═══════════╡
│ ERICB SS Equity ┆ Telefonaktiebola ┆ 90.152604  ┆ 2024-12-16 ┆ SEK      ┆ 75.072151  ┆ 56.010028 │
│                 ┆ get LM Ericsso   ┆            ┆            ┆          ┆            ┆           │
│ ABB SS Equity   ┆ ABB Ltd          ┆ 630.622469 ┆ 2024-12-16 ┆ SEK      ┆ 566.571183 ┆ 53.763102 │
│ SEBA SS Equity  ┆ Skandinaviska    ┆ 153.80595  ┆ 2024-12-16 ┆ SEK      ┆ 150.742394 ┆ 56.460733 │
│                 ┆ Enskilda Banken  ┆            ┆            ┆          ┆            ┆           │
│ ASSAB SS Equity ┆ Assa Abloy AB    ┆ 339.017591 ┆ 2024-12-16 ┆ SEK      ┆ 317.057573 ┆ 53.351619 │
└─────────────────┴──────────────────┴────────────┴────────────┴──────────┴────────────┴───────────┘
```

### Bond Universe from Equity Ticker
```python
# Get Bond Universe from Equity Ticker
query = """
let(#rank=normalized_payment_rank();
    #oas=spread(st=oas);
    #nxt_call=nxt_call_dt();
    )
get(name(), #rank, #nxt_call, #oas)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""

with BQuery() as bq:
    results = bq.bql(query)
    df = results.combine()
    print(df)
```
Output:
```
┌───────────────┬───────────────────┬──────────────────┬────────────┬────────────┬────────────┐
│ ID            ┆ name()            ┆ #rank            ┆ #nxt_call  ┆ #oas       ┆ DATE       │
│ ---           ┆ ---               ┆ ---              ┆ ---        ┆ ---        ┆ ---        │
│ str           ┆ str               ┆ str              ┆ date       ┆ f64        ┆ date       │
╞═══════════════╪═══════════════════╪══════════════════╪════════════╪════════════╪════════════╡
│ YX231113 Corp ┆ GTN 10 ½ 07/15/29 ┆ 1st Lien Secured ┆ 2026-07-15 ┆ 598.66491  ┆ 2024-12-17 │
│ BS116983 Corp ┆ GTN 5 ⅜ 11/15/31  ┆ Sr Unsecured     ┆ 2026-11-15 ┆ 1193.17529 ┆ 2024-12-17 │
│ AV438089 Corp ┆ GTN 7 05/15/27    ┆ Sr Unsecured     ┆ 2024-12-24 ┆ 400.340456 ┆ 2024-12-17 │
│ ZO860846 Corp ┆ GTN 4 ¾ 10/15/30  ┆ Sr Unsecured     ┆ 2025-10-15 ┆ 1249.34346 ┆ 2024-12-17 │
│ LW375188 Corp ┆ GTN 5 ⅞ 07/15/26  ┆ Sr Unsecured     ┆ 2025-01-13 ┆ 173.761744 ┆ 2024-12-17 │
└───────────────┴───────────────────┴──────────────────┴────────────┴────────────┴────────────┘
```

### Bonds Total Returns
This is example of a single-item query returning total return for all GTN bonds in a long dataframe.
We can easily pivot it into wide format, as in the example below
```python
# Total Return of GTN Bonds
query = """
let(#rng = range(-1M, 0D);
    #rets = return_series(calc_interval=#rng,per=W);)
get(#rets)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""

with BQuery() as bq:
    results = bq.bql(query)
    df = results[0].pivot(on="ID", index="DATE", values="#rets")
    print(df)
```
Output:
```python
shape: (6, 6)
┌────────────┬───────────────┬───────────────┬───────────────┬───────────────┬───────────────┐
│ DATE       ┆ YX231113 Corp ┆ BS116983 Corp ┆ AV438089 Corp ┆ ZO860846 Corp ┆ LW375188 Corp │
│ ---        ┆ ---           ┆ ---           ┆ ---           ┆ ---           ┆ ---           │
│ date       ┆ f64           ┆ f64           ┆ f64           ┆ f64           ┆ f64           │
╞════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╡
│ 2024-11-17 ┆ null          ┆ null          ┆ null          ┆ null          ┆ null          │
│ 2024-11-24 ┆ 0.001653      ┆ 0.051179      ┆ 0.020363      ┆ 0.001371      ┆ -0.002939     │
│ 2024-12-01 ┆ 0.002837      ┆ 0.010405      ┆ -0.001466     ┆ 0.007275      ┆ 0.000581      │
│ 2024-12-08 ┆ -0.000041     ┆ 0.016145      ┆ 0.000766      ┆ 0.024984      ┆ 0.000936      │
│ 2024-12-15 ┆ 0.001495      ┆ -0.047        ┆ -0.000233     ┆ -0.043509     ┆ 0.002241      │
│ 2024-12-17 ┆ 0.00008       ┆ -0.000004     ┆ -0.0035       ┆ -0.007937     ┆ 0.000064      │
└────────────┴───────────────┴───────────────┴───────────────┴───────────────┴───────────────┘
```

### Maturity Wall for US HY Bonds
```python
query = """
let(#mv=sum(group(amt_outstanding(currency=USD),
                  by=[year(maturity()), industry_sector()]));)
get(#mv)
for(members('LF98TRUU Index'))
"""
with BQuery() as bq:
    results = bq.bql(query)
df = results.combine().rename(
    {"YEAR(MATURITY())": "maturity", "INDUSTRY_SECTOR()": "sector", "#mv": "mv"}
)

print(df.pivot(index="maturity", on="sector", values="mv").head())
```
Output:
```python
shape: (5, 11)
┌──────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
│ maturity ┆ Basic     ┆ Consumer, ┆ Energy    ┆ … ┆ Financial ┆ Technolog ┆ Utilities ┆ Diversifi │
│ ---      ┆ Materials ┆ Non-cycli ┆ ---       ┆   ┆ ---       ┆ y         ┆ ---       ┆ ed        │
│ i64      ┆ ---       ┆ cal       ┆ f64       ┆   ┆ f64       ┆ ---       ┆ f64       ┆ ---       │
│          ┆ f64       ┆ ---       ┆           ┆   ┆           ┆ f64       ┆           ┆ f64       │
│          ┆           ┆ f64       ┆           ┆   ┆           ┆           ┆           ┆           │
╞══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
│ 2025     ┆ 1.5e8     ┆ 5.34916e8 ┆ 5e8       ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
│ 2026     ┆ 4.4013e9  ┆ 9.3293e9  ┆ 8.2931e9  ┆ … ┆ 1.3524e10 ┆ 4.0608e9  ┆ 2.5202e9  ┆ null      │
│ 2027     ┆ 8.3921e9  ┆ 2.3409e10 ┆ 1.2427e10 ┆ … ┆ 1.9430e10 ┆ 4.3367e9  ┆ 3.6620e9  ┆ null      │
│ 2028     ┆ 1.4701e10 ┆ 3.7457e10 ┆ 2.2442e10 ┆ … ┆ 2.3341e10 ┆ 9.9143e9  ┆ 7.6388e9  ┆ 5e8       │
│ 2029     ┆ 1.6512e10 ┆ 5.7381e10 ┆ 3.9286e10 ┆ … ┆ 4.2337e10 ┆ 2.2660e10 ┆ 5.8558e9  ┆ null      │
└──────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘
```


## Additional Documentation & Resources

- *API Documentation*: Detailed documentation and function references are available in https://marekozana.github.io/polars-bloomberg

- *Additional Examples*: Check out [examples/](examples/) for hands-on notebooks demonstrating a variety of use cases.
    - BQL examples and use cases: [examples/Examples-BQL.ipynb](https://github.com/MarekOzana/polars-bloomberg/blob/main/examples/Examples-BQL.ipynb)

- *Bloomberg Developer Resources*: For more details on the Bloomberg API itself, visit the [Bloomberg Developer's page](https://developer.bloomberg.com/).
