# ![Polars Bloomberg Logo](assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API
![python-package](https://github.com/MarekOzana/polars-bloomberg/actions/workflows/python-package.yml/badge.svg)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Polars + Bloomberg Open API** is a Python library that facilitates seamless integration of Bloomberg data into Polars DataFrames. Designed for users familiar with Pandas or Excel, it offers minimal-boilerplate functions such as `bdp()`, `bdh()`, and `bql()`. Leverage Polars' high-performance capabilities alongside the Bloomberg API for lightning-fast DataFrame operations and a minimal memory footprint.


## Features
- Intuitive "Excel-like" methods: `bdp()`, `bdh()`, `bql()`
- Outputs data as Polars DataFrames
- Lightweight design with no dependency on `pandas`


# Installation
```bash
pip install polars-bloomberg
```

# 5 Minutes Start Guide
Here are some basic examples demonstrating how to use the `Polars + Bloomberg Open API` to fetch Bloomberg data and return them in a Polars DataFrame.

User starts by creating a BQuery() object using context manager syntax. The object can then be used for data-point bdp() queries, historical bdh() queries or even BLoomberg Query Language bql() queries.

## BDP - Bloomberg Data Point
Full-code example, getting last price for Apple and Microsoft shares:
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

### BDH with currency overrides


</details>


## BQL - Bloomberg Query Language
Allows to run complex `bql` queries and get result in wide `polars.DataFrame`with correct types
```python
df = bq.bql("get(px_last) for(['IBM US Equity', 'OMX Index'])")
```
<div>
<small>shape: (2, 4)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>px_last</th><th>px_last.DATE</th><th>px_last.CURRENCY</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>str</td></tr></thead><tbody><tr><td>&quot;IBM US Equity&quot;</td><td>238.04</td><td>2024-12-07</td><td>&quot;USD&quot;</td></tr><tr><td>&quot;OMX Index&quot;</td><td>2614.268</td><td>2024-12-07</td><td>&quot;SEK&quot;</td></tr></tbody></table></div>

<details><summary>More BQL Examples</summary>
    
Example of more complex query:
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
</tbody></table></div>

</details>

## API Documentation
Read the [API documentation](examples/API-docs.md) in `examples/` directory

## More Examples
Explore additional [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.

## Bloomberg Documentation

For documentation on the Bloomberg API, check out the [Bloomberg Developer's page](https://developer.bloomberg.com/).




