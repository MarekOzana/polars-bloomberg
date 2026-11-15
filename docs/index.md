---
title: Polars-Bloomberg
description: Polars-Bloomberg brings Bloomberg blpapi data into fast Polars DataFrames, covering BDP, BDH, BDIB, BSRCH, and Bloomberg BQL workflows with examples.
---

![Polars Bloomberg Logo](https://raw.githubusercontent.com/MarekOzana/polars-bloomberg/main/assets/polars-bloomberg-logo.jpg)

# Polars + Bloomberg Open API

**polars-bloomberg** is a Python library that extracts Bloomberg’s financial data directly into [Polars](https://www.pola.rs/) DataFrames.   
If you’re a quant financial analyst, data scientist, or quant developer working in capital markets, this library makes it easy to fetch, transform, and analyze Bloomberg data right in Polars—offering speed, efficient memory usage, and a lot of fun to use!

## Installation & Requirements

- **Bloomberg Access:** A valid Bloomberg terminal license is required!
- **Bloomberg Python API:** The `blpapi` library must be installed. See the [Bloomberg API Library](https://www.bloomberg.com/professional/support/api-library/).
- **Python Version:** Python 3.12+ required. Supported versions: 3.12, 3.13, 3.14, and 3.15.

```bash
pip install polars-bloomberg
```

## Quick Start
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

## Cheat Sheet

| Method | Description |
|--------|-------------|
| `bdp()` | Fetch single-value fields (e.g., last price). [more.. ](usage/bdp.md) |
| `bdh()` | Retrieve historical data (e.g., time series). [more.. ](usage/bdh.md)  |
| `bdib()` | Download intraday bar data at minute resolution. [more.. ](usage/bdib.md)  |
| `bsrch()` | Excel-style search grids (SRCH/BI/COMDTY domains). [more.. ](usage/bsrch.md)  |
| `bql()` | Execute complex Bloomberg Query Language requests. [more.. ](usage/bql.md)  |
