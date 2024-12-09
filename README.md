# ![Polars Bloomberg Logo](assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API
[![Tests](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml/badge.svg)](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Polars + Bloomberg Open API** is a Python library that facilitates integration of Bloomberg data into Polars DataFrames. Designed for users familiar with Pandas or Excel, it offers minimal-boilerplate functions such as `bdp()`, `bdh()`, and `bql()`. Leverage Polars' high-performance capabilities alongside the Bloomberg API for lightning-fast DataFrame operations and a minimal memory footprint.


*Key Benefits:*
- Intuitive "Excel-like" methods: `bdp()`, `bdh()`, `bql()`
- Outputs data as Polars DataFrames
- Lightweight design with no dependency on `pandas`
- Quickly prototype, test, and scale complex financial analyses.

## Prerequisites

- **Bloomberg Access:** A valid Bloomberg terminal license.
- **Bloomberg Python API:** The `blpapi` library must be installed. See the [Bloomberg API Library](https://www.bloomberg.com/professional/support/api-library/) for guidance.
- **Python Version:** Python 3.8+ recommended.
- **Installation:** `pip install polars-bloomberg`


## Quick Start Guide (5 Minutes)

Below is a simple example to get you started. For more comprehensive examples, please see the [examples/](examples/) directory.

**Concept:**  
`BQuery` is your main interface. Using a context manager ensures the connection opens and closes cleanly. Within this session, you can use:
- `bq.bdp()` for Bloomberg Data Points (single-value fields).
- `bq.bdh()` for Historical Data (time series).
- `bq.bql()` for complex Bloomberg Query Language requests.

## BDP - Bloomberg Data Point

### Example: Fetching the Last Price of Apple and Microsoft
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
```

<div>
<small>shape: (2, 2)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>PX_LAST</th></tr><tr><td>str</td><td>f64</td></tr></thead><tbody><tr><td>&quot;AAPL US Equity&quot;</td><td>242.84</td></tr><tr><td>&quot;MSFT US Equity&quot;</td><td>443.57</td></tr></tbody></table>
</div>

<details><summary>More BDP Examples</summary>

### BDP with different column types

`polars-bloomberg` correctly infers column type as shown in this example:

```python
with BQuery() as bq:
    df = bq.bdp(["XS2930103580 Corp", "USX60003AC87 Corp"],
                ["SECURITY_DES", "YAS_ZSPREAD", "CRNCY", "NXT_CALL_DT"])
```
<div>
<small>shape: (2, 5)</small>
<table border="1" class="dataframe"><thead><tr><th>security</th><th>SECURITY_DES</th><th>YAS_ZSPREAD</th><th>CRNCY</th><th>NXT_CALL_DT</th></tr><tr><td>str</td><td>str</td><td>f64</td><td>str</td><td>date</td></tr></thead><tbody><tr><td>&quot;XS2930103580 Corp&quot;</td><td>&quot;SEB 6 3/4 PERP&quot;</td><td>327.309349</td><td>&quot;USD&quot;</td><td>2031-11-04</td></tr><tr><td>&quot;USX60003AC87 Corp&quot;</td><td>&quot;NDAFH 6.3 PERP&quot;</td><td>315.539222</td><td>&quot;USD&quot;</td><td>2031-09-25</td></tr></tbody></table>
</div>

### BDP with overrides
User can submit list of tuples with overrides
```python
with BQuery() as bq:
    df = bq.bdp(["IBM US Equity"], ["PX_LAST", "CRNCY_ADJ_PX_LAST"], 
                overrides=[("EQY_FUND_CRNCY", "SEK")])
```
<div>
</style>
<small>shape: (1, 3)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>PX_LAST</th><th>CRNCY_ADJ_PX_LAST</th></tr><tr><td>str</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;IBM US Equity&quot;</td><td>238.04</td><td>2607.401</td></tr></tbody></table>
</div>

### BDP with date overrides
Overrides for dates has to be in format YYYYMMDD
```python
with BQuery() as bq:
    df = bq.bdp(["USX60003AC87 Corp"], ["SETTLE_DT"], overrides=[("USER_LOCAL_TRADE_DATE", "20241014")])
```
<div>
<small>shape: (1, 2)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>SETTLE_DT</th></tr><tr><td>str</td><td>date</td></tr></thead><tbody><tr><td>&quot;USX60003AC87 Corp&quot;</td><td>2024-10-15</td></tr></tbody></table>
</div>

```python
with BQuery() as bq:
    df = bq.bdp(['USDSEK Curncy', 'SEKCZK Curncy'], 
                ['SETTLE_DT', 'PX_LAST'], 
                overrides=[('REFERENCE_DATE', '20200715')]
               )
```
<div>
<small>shape: (2, 3)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>SETTLE_DT</th><th>PX_LAST</th></tr><tr><td>str</td><td>date</td><td>f64</td></tr></thead><tbody><tr><td>&quot;USDSEK Curncy&quot;</td><td>2020-07-17</td><td>10.9343</td></tr><tr><td>&quot;SEKCZK Curncy&quot;</td><td>2020-07-17</td><td>2.1718</td></tr></tbody></table></div>

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
<div>
<small>shape: (2, 4)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>px_last</th><th>px_last.DATE</th><th>px_last.CURRENCY</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>str</td></tr></thead><tbody><tr><td>&quot;IBM US Equity&quot;</td><td>238.04</td><td>2024-12-07</td><td>&quot;USD&quot;</td></tr><tr><td>&quot;OMX Index&quot;</td><td>2614.268</td><td>2024-12-07</td><td>&quot;SEK&quot;</td></tr></tbody></table></div>

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
<div>
<small>shape: (5, 5)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>#average_spread</th><th>#average_spread.DATE</th><th>#average_spread.ORIG_IDS</th><th>#average_spread.#BINS</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>str</td><td>str</td></tr></thead><tbody><tr><td>&quot;(1) 0-3&quot;</td><td>30.74</td><td>2024-12-08</td><td>&quot;QZ552396 Corp&quot;</td><td>&quot;(1) 0-3&quot;</td></tr><tr><td>&quot;(2) 3-9&quot;</td><td>59.79</td><td>2024-12-08</td><td>null</td><td>&quot;(2) 3-9&quot;</td></tr><tr><td>&quot;(3) 9-18&quot;</td><td>105.39</td><td>2024-12-08</td><td>&quot;BH393780 Corp&quot;</td><td>&quot;(3) 9-18&quot;</td></tr><tr><td>&quot;(4) 18-30&quot;</td><td>131.72</td><td>2024-12-08</td><td>&quot;BH393781 Corp&quot;</td><td>&quot;(4) 18-30&quot;</td></tr><tr><td>&quot;(5) 30+&quot;</td><td>150.33</td><td>2024-12-08</td><td>&quot;BH393782 Corp&quot;</td><td>&quot;(5) 30+&quot;</td></tr></tbody></table>
</div>

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

</details>

## API Documentation
Read the [API documentation](examples/API-docs.md) in `examples/` directory

## More Examples
Explore additional [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.

## Bloomberg Documentation

For documentation on the Bloomberg API, check out the [Bloomberg Developer's page](https://developer.bloomberg.com/).




