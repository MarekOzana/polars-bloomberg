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

<details><summary>More BDP Examples</summary>

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

## BDH - Bloomberg Data History
```python
with BQuery() as bq:
    df = bq.bdh(['AAPL US Equity', 'TSLA US Equity'], 
                ['PX_LAST', 'VOLUME'], 
                start_date=date(2019, 1, 1), 
                end_date=date(2019, 1, 10))
```
<div>
<small>shape: (14, 4)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>date</th><th>PX_LAST</th><th>VOLUME</th></tr><tr><td>str</td><td>date</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-01-02</td><td>39.48</td><td>1.48158948e8</td></tr><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-01-03</td><td>35.548</td><td>3.6524878e8</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>&quot;TSLA US Equity&quot;</td><td>2019-01-09</td><td>22.5687</td><td>8.1494175e7</td></tr><tr><td>&quot;TSLA US Equity&quot;</td><td>2019-01-10</td><td>22.998</td><td>9.084531e7</td></tr></tbody></table></div>

<details><summary>More BDH Examples</summary>

### BDH with options - periodicitySelection: Monthly
```python
with BQuery() as bq:
    df = bq.bdh(['AAPL US Equity'], 
                ['PX_LAST'], 
                start_date=date(2019, 1, 1), 
                end_date=date(2019, 3, 29),
                options={"periodicitySelection": "MONTHLY"})
```
<div>
<small>shape: (3, 3)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>date</th><th>PX_LAST</th></tr><tr><td>str</td><td>date</td><td>f64</td></tr></thead><tbody><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-01-31</td><td>41.61</td></tr><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-02-28</td><td>43.288</td></tr><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-03-29</td><td>47.488</td></tr></tbody></table>
</div>

</details>


## BQL - Bloomberg Query Language
Allows to run complex `bql` queries and get result in wide `polars.DataFrame`with correct polars types

```python
with BQuery() as bq:
    df = bq.bql("get(px_last) for(['IBM US Equity', 'OMX Index'])")
```
```plaintext
┌───────────────┬────────────┬──────────────┬──────────────────┐
│ ID            ┆ px_last    ┆ px_last.DATE ┆ px_last.CURRENCY │
│ ---           ┆ ---        ┆ ---          ┆ ---              │
│ str           ┆ f64        ┆ date         ┆ str              │
╞═══════════════╪════════════╪══════════════╪══════════════════╡
│ IBM US Equity ┆ 231.024994 ┆ 2024-12-10   ┆ USD              │
│ OMX Index     ┆ 2602.806   ┆ 2024-12-10   ┆ SEK              │
└───────────────┴────────────┴──────────────┴──────────────────┘
```

<details><summary>More BQL Examples</summary>
    
### Actual and Forward EPS Estimates
```python
df = bq.bql("""
    let(#eps=is_eps(fa_period_type='A',
                    fa_period_offset=range(-4,2));)
    get(#eps)
    for(['IBM US Equity'])
""")
```
<div>
<small>shape: (7, 6)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>#eps</th><th>#eps.REVISION_DATE</th><th>#eps.AS_OF_DATE</th><th>#eps.PERIOD_END_DATE</th><th>#eps.CURRENCY</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>date</td><td>date</td><td>str</td></tr></thead><tbody>
<tr><td>&quot;IBM US Equity&quot;</td><td>10.63</td><td>2022-02-22</td><td>2024-12-07</td><td>2019-12-31</td><td>&quot;USD&quot;</td></tr>
<tr><td>&quot;IBM US Equity&quot;</td><td>6.28</td><td>2023-02-28</td><td>2024-12-07</td><td>2020-12-31</td><td>&quot;USD&quot;</td></tr>
<tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr>
<tr><td>&quot;IBM US Equity&quot;</td><td>9.236</td><td>2024-12-07</td><td>2024-12-07</td><td>2025-12-31</td><td>&quot;USD&quot;</td></tr>
</tbody></table>
</div>

### ZSpread vs Duration on bonds from SRCH
```python
query="""
let(#dur=duration(duration_type=MODIFIED); 
    #zsprd=spread(spread_type=Z);) 
get(name(), #dur, #zsprd) 
for(filter(screenresults(type=SRCH, screen_name='@COCO'), 
           ticker in ['SEB', 'SHBASS']))
"""

with BQuery() as bq:
    df = bq.bql(query)
```
```plaintext
shape: (6, 6)
┌───────────────┬─────────────────┬──────────┬────────────┬────────────┬─────────────┐
│ ID            ┆ name()          ┆ #dur     ┆ #dur.DATE  ┆ #zsprd     ┆ #zsprd.DATE │
│ ---           ┆ ---             ┆ ---      ┆ ---        ┆ ---        ┆ ---         │
│ str           ┆ str             ┆ f64      ┆ date       ┆ f64        ┆ date        │
╞═══════════════╪═════════════════╪══════════╪════════════╪════════════╪═════════════╡
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ 2.244382 ┆ 2024-12-10 ┆ 229.930933 ┆ 2024-12-10  │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ 4.958582 ┆ 2024-12-10 ┆ 269.963777 ┆ 2024-12-10  │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ 1.968658 ┆ 2024-12-10 ┆ 232.839648 ┆ 2024-12-10  │
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ 5.388785 ┆ 2024-12-10 ┆ 324.70196  ┆ 2024-12-10  │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ 0.409083 ┆ 2024-12-10 ┆ 165.405465 ┆ 2024-12-10  │
│ YV402592 Corp ┆ SEB Float PERP  ┆ 0.22527  ┆ 2024-12-10 ┆ 248.756    ┆ 2024-12-10  │
└───────────────┴─────────────────┴──────────┴────────────┴────────────┴─────────────┘
```

### Average issuer OAS spread per maturity bucket
```python
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
    df = bq.bql(query)
```
```plaintext
┌───────────┬─────────────────┬──────────────────────┬──────────────────────┬──────────────────────┐
│ ID        ┆ #average_spread ┆ #average_spread.DATE ┆ #average_spread.ORIG ┆ #average_spread.#BIN │
│ ---       ┆ ---             ┆ ---                  ┆ _IDS                 ┆ S                    │
│ str       ┆ f64             ┆ date                 ┆ ---                  ┆ ---                  │
│           ┆                 ┆                      ┆ str                  ┆ str                  │
╞═══════════╪═════════════════╪══════════════════════╪══════════════════════╪══════════════════════╡
│ (1) 0-3   ┆ 30.638311       ┆ 2024-12-10           ┆ QZ552396 Corp        ┆ (1) 0-3              │
│ (2) 3-9   ┆ 59.772151       ┆ 2024-12-10           ┆ null                 ┆ (2) 3-9              │
│ (3) 9-18  ┆ 106.722341      ┆ 2024-12-10           ┆ BH393780 Corp        ┆ (3) 9-18             │
│ (4) 18-30 ┆ 129.945414      ┆ 2024-12-10           ┆ BH393781 Corp        ┆ (4) 18-30            │
│ (5) 30+   ┆ 151.318634      ┆ 2024-12-10           ┆ BH393782 Corp        ┆ (5) 30+              │
└───────────┴─────────────────┴──────────────────────┴──────────────────────┴──────────────────────┘

```

### Technical Analysis: stocks with 20d EMA > 200d EMA and RSI > 70
```python
with BQuery() as bq:
    df = bq.bql(
        """
        let(#ema20=emavg(period=20); 
            #ema200=emavg(period=200); 
            #rsi=rsi(close=px_last());)
        get(name(), #ema20, #ema200, #rsi)
        for(filter(members('OMX Index'), 
                    and(#ema20 > #ema200, #rsi > 70)))
        with(fill=PREV)
        """
    )
```
<div>
<small>shape: (2, 10)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>name()</th><th>#ema20</th><th>#ema20.DATE</th><th>#ema20.CURRENCY</th><th>#ema200</th><th>#ema200.DATE</th><th>#ema200.CURRENCY</th><th>#rsi</th><th>#rsi.DATE</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>date</td><td>str</td><td>f64</td><td>date</td><td>str</td><td>f64</td><td>date</td></tr></thead><tbody><tr><td>&quot;SKFB SS Equity&quot;</td><td>&quot;SKF AB&quot;</td><td>210.185019</td><td>2024-12-08</td><td>&quot;SEK&quot;</td><td>204.16756</td><td>2024-12-08</td><td>&quot;SEK&quot;</td><td>72.255568</td><td>2024-12-08</td></tr><tr><td>&quot;ABB SS Equity&quot;</td><td>&quot;ABB Ltd&quot;</td><td>623.496942</td><td>2024-12-08</td><td>&quot;SEK&quot;</td><td>561.902577</td><td>2024-12-08</td><td>&quot;SEK&quot;</td><td>72.144556</td><td>2024-12-08</td></tr></tbody></table></div>

### Swedish USD AT1 Bonds with Bid Axis
```python
query="""
let(#ax=axes();)
get(ticker, cpn(), nxt_call_dt(), #ax)
for(filter(bondsuniv(ACTIVE), 
    crncy()=='USD' and 
    basel_iii_designation() == 'Additional Tier 1' and 
    country_iso() == 'SE' and 
    is_axed('Bid') == True))
"""
with BQuery() as bq:
    df = bq.bql(query)
```
<div>
<small>shape: (8, 11)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>ticker</th><th>cpn()</th><th>cpn().MULTIPLIER</th><th>cpn().CPN_TYP</th><th>nxt_call_dt()</th><th>#ax</th><th>#ax.ASK_DEPTH</th><th>#ax.BID_DEPTH</th><th>#ax.ASK_TOTAL_SIZE</th><th>#ax.BID_TOTAL_SIZE</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>f64</td><td>str</td><td>date</td><td>str</td><td>i64</td><td>i64</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;YU819930 Corp&quot;</td><td>&quot;SEB&quot;</td><td>6.75</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2031-11-04</td><td>&quot;Y&quot;</td><td>1</td><td>1</td><td>5e6</td><td>1.8e6</td></tr><tr><td>&quot;ZQ349286 Corp&quot;</td><td>&quot;SEB&quot;</td><td>5.125</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2025-05-13</td><td>&quot;Y&quot;</td><td>3</td><td>9</td><td>6.7e6</td><td>5e7</td></tr><tr><td>&quot;ZF859199 Corp&quot;</td><td>&quot;SWEDA&quot;</td><td>7.75</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2030-03-17</td><td>&quot;Y&quot;</td><td>1</td><td>2</td><td>5e6</td><td>7e6</td></tr><tr><td>&quot;BW924993 Corp&quot;</td><td>&quot;SEB&quot;</td><td>6.875</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2027-06-30</td><td>&quot;Y&quot;</td><td>2</td><td>3</td><td>8.2e6</td><td>1.1e7</td></tr><tr><td>&quot;ZL122341 Corp&quot;</td><td>&quot;SWEDA&quot;</td><td>7.625</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2028-03-17</td><td>&quot;Y&quot;</td><td>1</td><td>6</td><td>2.6e6</td><td>2.34e7</td></tr><tr><td>&quot;ZO703956 Corp&quot;</td><td>&quot;SHBASS&quot;</td><td>4.75</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2031-03-01</td><td>&quot;Y&quot;</td><td>1</td><td>2</td><td>3.2e6</td><td>6e6</td></tr><tr><td>&quot;BR069680 Corp&quot;</td><td>&quot;SWEDA&quot;</td><td>4.0</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2029-03-17</td><td>&quot;Y&quot;</td><td>null</td><td>1</td><td>null</td><td>3e6</td></tr><tr><td>&quot;ZO703315 Corp&quot;</td><td>&quot;SHBASS&quot;</td><td>4.375</td><td>1.0</td><td>&quot;VARIABLE&quot;</td><td>2027-03-01</td><td>&quot;Y&quot;</td><td>1</td><td>3</td><td>3e6</td><td>7.4e6</td></tr></tbody></table>
</div>

### Bond universe from Equity Ticker
```python
query="""
let(#rank=normalized_payment_rank();
    #oas=spread(st=oas);
    #nxt_call=nxt_call_dt();
    )
get(name(), #rank, #nxt_call, #oas)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""
with BQuery() as bq:
    df = bq.bql(query)
```

```plaintext
┌───────────────┬───────────────────┬──────────────────┬────────────┬─────────────┬────────────┐
│ ID            ┆ name()            ┆ #rank            ┆ #nxt_call  ┆ #oas        ┆ #oas.DATE  │
│ ---           ┆ ---               ┆ ---              ┆ ---        ┆ ---         ┆ ---        │
│ str           ┆ str               ┆ str              ┆ date       ┆ f64         ┆ date       │
╞═══════════════╪═══════════════════╪══════════════════╪════════════╪═════════════╪════════════╡
│ YX231113 Corp ┆ GTN 10 ½ 07/15/29 ┆ 1st Lien Secured ┆ 2026-07-15 ┆ 615.798149  ┆ 2024-12-10 │
│ BS116983 Corp ┆ GTN 5 ⅜ 11/15/31  ┆ Sr Unsecured     ┆ 2026-11-15 ┆ 1144.393892 ┆ 2024-12-10 │
│ AV438089 Corp ┆ GTN 7 05/15/27    ┆ Sr Unsecured     ┆ 2024-12-17 ┆ 389.022271  ┆ 2024-12-10 │
│ ZO860846 Corp ┆ GTN 4 ¾ 10/15/30  ┆ Sr Unsecured     ┆ 2025-10-15 ┆ 1184.969597 ┆ 2024-12-10 │
│ LW375188 Corp ┆ GTN 5 ⅞ 07/15/26  ┆ Sr Unsecured     ┆ 2025-01-06 ┆ 185.544312  ┆ 2024-12-10 │
└───────────────┴───────────────────┴──────────────────┴────────────┴─────────────┴────────────┘
```

### Weekly (total) Returns
```python
query="""
let(#rng = range(-3M, 0D);
    #rets = return_series(calc_interval=#rng,per=W);
    )
get(#rets)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""
with BQuery() as bq:
    df = bq.bql(query)
```
```plaintext
shape: (20, 3)
┌───────────────┬───────────┬────────────┐
│ ID            ┆ #rets     ┆ #rets.DATE │
│ ---           ┆ ---       ┆ ---        │
│ str           ┆ f64       ┆ date       │
╞═══════════════╪═══════════╪════════════╡
│ YX231113 Corp ┆ null      ┆ 2024-11-19 │
│ YX231113 Corp ┆ 0.005028  ┆ 2024-11-26 │
│ YX231113 Corp ┆ 0.000326  ┆ 2024-12-03 │
│ YX231113 Corp ┆ 0.000414  ┆ 2024-12-10 │
│ BS116983 Corp ┆ null      ┆ 2024-11-19 │
│ …             ┆ …         ┆ …          │
│ ZO860846 Corp ┆ -0.010198 ┆ 2024-12-10 │
│ LW375188 Corp ┆ null      ┆ 2024-11-19 │
│ LW375188 Corp ┆ -0.000997 ┆ 2024-11-26 │
│ LW375188 Corp ┆ 0.001294  ┆ 2024-12-03 │
│ LW375188 Corp ┆ 0.0011    ┆ 2024-12-10 │
└───────────────┴───────────┴────────────┘
```

</details>

## API Documentation
Read the [API documentation](examples/API-docs.md) in `examples/` directory

## More Examples
Explore additional [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.

## Bloomberg Documentation

For documentation on the Bloomberg API, check out the [Bloomberg Developer's page](https://developer.bloomberg.com/).




