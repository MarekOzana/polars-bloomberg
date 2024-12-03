# ![Polars Bloomberg Logo](assets/polars_bloomberg_logo.png)

# Polars + Bloomberg Open API
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Python library providing a Polars DataFrame interface for easy and intuitive access to the Bloomberg Open API

# Features
- Excel like functions `bdp()`, `bdh()`, `bql()`
- Polars DataFrame outputs
- No dependency on Pandas

# Usage
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
    df_hist = bq.bdh(['AAPL US Equity'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))
    df_bql = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")
```
Explore the [usage examples](examples/Examples-1.ipynb) in the `examples/` directory.