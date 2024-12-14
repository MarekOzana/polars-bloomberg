
# BQuery API Documentation

`BQuery` is a Polars-based Python interface to the Bloomberg Open API, enabling efficient data retrieval and manipulation for financial analysis.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Public Methods](#public-methods)
   - [bdp (Bloomberg Data Point)](#bdp)
   - [bdh (Bloomberg Data History)](#bdh)
   - [bql (Bloomberg Query Language)](#bql)
5. [Examples](#examples)
6. [Error Handling](#error-handling)

---

## Overview

`BQuery` simplifies interaction with the Bloomberg API using:
- `bdp()` for single-value reference data.
- `bdh()` for time-series historical data.
- `bql()` for complex analytical queries.

All results are returned as Polars DataFrames for high performance and flexibility.

---

## Installation

Install via pip:

```bash
pip install polars-bloomberg
```

Ensure that `blpapi` is installed and configured. See the [Bloomberg API Library](https://www.bloomberg.com/professional/support/api-library/) for setup instructions.

---

## Usage

```python
from datetime import date
from polars_bloomberg import BQuery

with BQuery() as bq:
    # Fetch reference data
    df_ref = bq.bdp(["AAPL US Equity"], ["PX_LAST"])

    # Fetch historical data
    df_hist = bq.bdh(["AAPL US Equity"], ["PX_LAST"], date(2020, 1, 1), date(2020, 1, 31))

    # Run a Bloomberg Query Language (BQL) expression
    df_bql = bq.bql("get(px_last) for(['AAPL US Equity'])")
```

---

## Public Methods

### `bdp(securities, fields, overrides=None, options=None)`

Fetches single-value reference data, equivalent to Bloomberg Excel's `BDP()`.

| Parameter   | Type                  | Description                                                            |
|-------------|-----------------------|------------------------------------------------------------------------|
| `securities`| `list[str]`           | List of securities (e.g., `["AAPL US Equity"]`).                      |
| `fields`    | `list[str]`           | List of fields to retrieve (e.g., `["PX_LAST", "SECURITY_DES"]`).     |
| `overrides` | `list[tuple]`, optional | Key-value pairs for overriding defaults (e.g., `[("EQY_FUND_CRNCY", "SEK")]`). |
| `options`   | `dict`, optional      | Additional options for fine-tuning the request.                       |

**Returns**:
- `pl.DataFrame`: A DataFrame with one row per security and one column per field.

**Example**:

```python
df = bq.bdp(["IBM US Equity"], ["PX_LAST", "CRNCY"])
print(df)
```

Output:

```
┌───────────────┬─────────┬───────┐
│ security      ┆ PX_LAST ┆ CRNCY │
│ ---           ┆ ---     ┆ ---   │
│ str           ┆ f64     ┆ str   │
╞═══════════════╪═════════╪═══════╡
│ IBM US Equity ┆ 123.45  ┆ USD   │
└───────────────┴─────────┴───────┘
```

---

### `bdh(securities, fields, start_date, end_date, overrides=None, options=None)`

Retrieves historical data over a date range, equivalent to Bloomberg Excel's `BDH()`.

| Parameter   | Type                  | Description                                                            |
|-------------|-----------------------|------------------------------------------------------------------------|
| `securities`| `list[str]`           | List of securities.                                                   |
| `fields`    | `list[str]`           | Fields to retrieve (e.g., `["PX_LAST"]`).                             |
| `start_date`| `date`                | Start date for the query.                                             |
| `end_date`  | `date`                | End date for the query.                                               |
| `overrides` | `list[tuple]`, optional | Key-value pairs for overrides.                                        |
| `options`   | `dict`, optional      | Additional options for the query.                                     |

**Returns**:
- `pl.DataFrame`: A DataFrame with columns for `security`, `date`, and requested fields.

**Example**:

```python
df = bq.bdh(
    ["AAPL US Equity"], 
    ["PX_LAST"], 
    start_date=date(2023, 1, 1), 
    end_date=date(2023, 1, 7)
)
print(df)
```

Output:

```
┌───────────────┬────────────┬─────────┐
│ security      ┆ date       ┆ PX_LAST │
│ ---           ┆ ---        ┆ ---     │
│ str           ┆ date       ┆ f64     │
╞═══════════════╪════════════╪═════════╡
│ AAPL US Equity┆ 2023-01-01 ┆ 150.23  │
│ AAPL US Equity┆ 2023-01-02 ┆ 152.00  │
└───────────────┴────────────┴─────────┘
```

---

### `bql(expression)`

Executes a Bloomberg Query Language (BQL) expression for advanced analytics.

| Parameter   | Type        | Description                                 |
|-------------|-------------|---------------------------------------------|
| `expression`| `str`       | A valid BQL query.                         |

**Returns**:
- `list[pl.DataFrame]`: A list of DataFrames, one for each requested data item.

**Example**:

```python
df_list = bq.bql("get(px_last) for(['AAPL US Equity'])")
print(df_list[0])
```

---

## Error Handling

1. **Timeouts**: Ensure proper API connection and increase `timeout` if necessary.
2. **Empty Responses**: Verify input securities and fields for typos.
3. **Connection Errors**: Check Bloomberg API access and session configuration.


