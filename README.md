# ![Polars Bloomberg Logo](assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Polars + Bloomberg Open API** is a Python library that facilitates seamless integration of Bloomberg data into Polars DataFrames. Designed for users familiar with Pandas or Excel, it offers minimal-boilerplate functions such as `bdp()`, `bdh()`, and `bql()`. Leverage Polars' high-performance capabilities alongside the Bloomberg API for lightning-fast DataFrame operations and a minimal memory footprint.


## Features
- Intuitive "Excel-like" methods: `bdp()`, `bdh()`, `bql()`
- Outputs data as Polars DataFrames
- Lightweight design with no dependency on `pandas`
- Simplified access to Bloomberg data for streamlined workflows


# Installation
```bash
pip install polars-bloomberg
```

# Quick Start Guide
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
    df_hist = bq.bdh(['AAPL US Equity'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))
    df_bql = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")
```
Explore additional [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.

