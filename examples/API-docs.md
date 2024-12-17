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
    results = bq.bql("get(px_last) for(['AAPL US Equity'])")
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
- `BqlResult`: An object containing the query results as a list of Polars DataFrames.

**Example**:

```python
results = bq.bql("get(px_last) for(['AAPL US Equity'])")
print(results[0])
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

---

## BqlResult

### Description

`BqlResult` is a class that holds the result of a BQL query. It contains a list of Polars DataFrames and provides methods to work with the results.

**Attributes:**

- `dataframes` (list[pl.DataFrame]): A list of DataFrames, each containing the data for one item in the BQL `get` statement.
- `names` (list[str]): The names of the data items corresponding to each DataFrame.

### Methods

- `combine()`:

  **Description**: Combines all DataFrames in the `BqlResult` into a single DataFrame by joining on common columns.

  **Syntax**:

  ```python
  BqlResult.combine() -> pl.DataFrame
  ```

  **Example Usage**:

  ```python
  df_combined = result.combine()
  ```

- `__getitem__(index)`:

  **Description**: Allows access to individual DataFrames by index.

  **Syntax**:

  ```python
  BqlResult[index] -> pl.DataFrame
  ```

  **Example Usage**:

  ```python
  df_px_last = result[0]
  ```

- `__len__()`:

  **Description**: Returns the number of DataFrames in the `BqlResult`.

  **Syntax**:

  ```python
  len(BqlResult) -> int
  ```

  **Example Usage**:

  ```python
  n = len(result)
  ```

- `__iter__()`:

  **Description**: Returns an iterator over the DataFrames.

  **Syntax**:

  ```python
  iter(BqlResult) -> Iterator[pl.DataFrame]
  ```

  **Example Usage**:

  ```python
  for df in result:
      print(df)
  ```

### Example Usage

#### Fetching Multiple Fields

```python
with BQuery() as bq:
    result = bq.bql("get(name, px_last) for('AAPL US Equity')")

# Access individual DataFrames
df_name = result[0]
df_px_last = result[1]

# Combine DataFrames
df_combined = result.combine()
print(df_combined)
```

Output:

```
shape: (1, 3)
┌───────────────┬─────────────────┬─────────┐
│ ID            ┆ name            ┆ PX_LAST │
│ ---           ┆ ---             ┆ ---     │
│ str           ┆ str             ┆ f64     │
╞═══════════════╪═════════════════╪═════════╡
│ AAPL US Equity┆ Apple Inc.      ┆ 150.25  │
└───────────────┴─────────────────┴─────────┘
```

#### Iterating Over Results

```python
for df in result:
    print(df)
```

#### Checking the Number of DataFrames

```python
n = len(result)
print(f"Number of DataFrames: {n}")
```

---

## Error Handling

1. **Timeouts**: Ensure proper API connection and increase `timeout` if necessary.
2. **Empty Responses**: Verify input securities and fields for typos.
3. **Connection Errors**: Check Bloomberg API access and session configuration.


