
# BQuery API Documentation

## Overview

`BQuery` is a Python module providing a Polars-based interface to interact with the Bloomberg Open API. It enables users to fetch reference, historical, and query-based data in a convenient manner.

---

## Usage

Below is an example of how to use the `BQuery` class to fetch data:

```python
from datetime import date
from polars_bloomberg import BQuery

# Create a query instance
with BQuery() as bq:
    # Fetch reference data
    df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])

    # Fetch reference data with overrides
    df_rf2 = bq.bdp(
        ["OMX Index", "SPX Index", "SEBA SS Equity"],
        ["PX_LAST", "SECURITY_DES", "DVD_EX_DT", "CRNCY_ADJ_PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")]
    )

    # Fetch historical data
    df_hist = bq.bdh(
        ['AAPL US Equity'],
        ['PX_LAST'],
        date(2020, 1, 1),
        date(2020, 1, 30)
    )

    # Fetch data using a Bloomberg Query Language (BQL) expression
    df_px = bq.bql("get(px_last) for(['IBM US Equity', 'AAPL US Equity'])")
```

---

## Public Methods

### `bdp(securities, fields, overrides=None, options=None)`

Fetch reference data for given securities and fields. Equivalent to the Excel `BDP()` function.

#### Parameters:
- `securities` (List[str]): List of securities to query.
- `fields` (List[str]): List of fields to fetch.
- `overrides` (Optional[Sequence]): Optional overrides as key-value pairs.
- `options` (Optional[Dict]): Additional options for the request.

#### Returns:
- `pl.DataFrame`: A Polars DataFrame containing the requested reference data.

---

### `bdh(securities, fields, start_date, end_date, overrides=None, options=None)`

Fetch historical data for given securities and fields between specified dates. Equivalent to the Excel `BDH()` function.

#### Parameters:
- `securities` (List[str]): List of securities to query.
- `fields` (List[str]): List of fields to fetch.
- `start_date` (date): Start date for the historical query.
- `end_date` (date): End date for the historical query.
- `overrides` (Optional[Sequence]): Optional overrides as key-value pairs.
- `options` (Optional[Dict]): Additional options for the request.

#### Returns:
- `pl.DataFrame`: A Polars DataFrame containing the requested historical data.

---

### `bql(expression)`

Fetch data using a Bloomberg Query Language (BQL) expression.

#### Parameters:
- `expression` (str): A BQL expression to execute.

#### Returns:
- `pl.DataFrame`: A Polars DataFrame containing the results of the BQL query.

---

## Author
**Marek Ozana**  
Date: December 2024
