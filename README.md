# ![Polars Bloomberg Logo](assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API
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

# Quick Start Guide
Here are some basic examples demonstrating how to use the `Polars + Bloomberg Open API` to fetch Bloomberg data and return them in a Polars DataFrame.

## BDP - Bloomberg Data Point
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
```

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (2, 2)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>PX_LAST</th></tr><tr><td>str</td><td>f64</td></tr></thead><tbody><tr><td>&quot;AAPL US Equity&quot;</td><td>242.84</td></tr><tr><td>&quot;MSFT US Equity&quot;</td><td>443.57</td></tr></tbody></table>
</div>

## BDH - Bloomberg Data History
```python
df = bq.bdh(['AAPL US Equity', 'TSLA US Equity'], ['PX_LAST', 'VOLUME'], start_date=date(2019, 1, 1), end_date=date(2019, 1, 10))
```
<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (14, 4)</small><table border="1" class="dataframe"><thead><tr><th>security</th><th>date</th><th>PX_LAST</th><th>VOLUME</th></tr><tr><td>str</td><td>date</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-01-02</td><td>39.48</td><td>1.48158948e8</td></tr><tr><td>&quot;AAPL US Equity&quot;</td><td>2019-01-03</td><td>35.548</td><td>3.6524878e8</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>&quot;TSLA US Equity&quot;</td><td>2019-01-09</td><td>22.5687</td><td>8.1494175e7</td></tr><tr><td>&quot;TSLA US Equity&quot;</td><td>2019-01-10</td><td>22.998</td><td>9.084531e7</td></tr></tbody></table></div>

## BQL - Bloomberg Query Language
Allows to run complex `bql` queries and get result in wide `polars.DataFrame`with correct types
```python
df = bq.bql("get(px_last) for(['IBM US Equity', 'OMX Index'])")
```
<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (2, 4)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>px_last</th><th>px_last.DATE</th><th>px_last.CURRENCY</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>str</td></tr></thead><tbody><tr><td>&quot;IBM US Equity&quot;</td><td>238.04</td><td>2024-12-07</td><td>&quot;USD&quot;</td></tr><tr><td>&quot;OMX Index&quot;</td><td>2614.268</td><td>2024-12-07</td><td>&quot;SEK&quot;</td></tr></tbody></table></div>

Example of more complex query:
```python
df = bq.bql("""
    let(#eps=is_eps(fa_period_type='A',
                    fa_period_offset=range(-4,2));)
    get(#eps)
    for(['IBM US Equity'])
""")
```
<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (7, 6)</small><table border="1" class="dataframe"><thead><tr><th>ID</th><th>#eps</th><th>#eps.REVISION_DATE</th><th>#eps.AS_OF_DATE</th><th>#eps.PERIOD_END_DATE</th><th>#eps.CURRENCY</th></tr><tr><td>str</td><td>f64</td><td>date</td><td>date</td><td>date</td><td>str</td></tr></thead><tbody>
<tr><td>&quot;IBM US Equity&quot;</td><td>10.63</td><td>2022-02-22</td><td>2024-12-07</td><td>2019-12-31</td><td>&quot;USD&quot;</td></tr>
<tr><td>&quot;IBM US Equity&quot;</td><td>6.28</td><td>2023-02-28</td><td>2024-12-07</td><td>2020-12-31</td><td>&quot;USD&quot;</td></tr>
<tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr>
<tr><td>&quot;IBM US Equity&quot;</td><td>9.236</td><td>2024-12-07</td><td>2024-12-07</td><td>2025-12-31</td><td>&quot;USD&quot;</td></tr>
</tbody></table></div>



## More Examples
Explore additional [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.




