---
title: BSRCH - Bloomberg Search
description: Run Bloomberg BSRCH (SRCH/BI) searches in Polars-Bloomberg with overrides and limits, alongside Bloomberg BQL screeners for discovery.
---

# BSRCH

Use case: Excel-style searches (SRCH/BI domains). Good for running saved SRCH screens or Bloomberg BI templates.

Key points:
- Pass `overrides` as a dict; values are stringified. Common keys: `LIMIT` to raise the row cap (default ~5k), custom keys like `BIKEY` for BI templates, or domain-specific parameters.

## Example: two COCO bonds (LIMIT=2)
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bsrch("FI:SRCHEX.@COCO", overrides={"LIMIT": 2})
    print(df)

# shape: (2, 1)
# ┌──────────────┐
# │ id           │
# │ ---          │
# │ str          │
# ╞══════════════╡
# │ DA785784 Corp│
# │ DA773901 Corp│
# └──────────────┘
```

## Example: BI template (BI:TPD) with BIKEY + LIMIT
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bsrch(
        "BI:TPD",
        overrides={
            "BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR",
            "LIMIT": 20000,  # avoid ReachMax
        },
    )
    print(df.head())

# Example output (truncated):
# shape: (16, 6)
# ┌──────────┬──────────┬─────────┬──────────┬──────────┬─────────┐
# │ Main_Cat ┆ Bclass3_ ┆ Category┆ 06/30/20 ┆ 03/31/20 ┆ 12/31/20│
# │ ...      ┆ ...      ┆ ...     ┆ 25       ┆ 25       ┆ 24      │
# │ str      ┆ str      ┆ str     ┆ f64      ┆ f64      ┆ f64     │
# ╞══════════╪══════════╪═════════╪══════════╪══════════╪═════════╡
# │ Leverage ┆ Non-Fin… ┆ B       ┆ 3.956051 ┆ 4.118212 ┆ 4.269732│
# └──────────┴──────────┴─────────┴──────────┴──────────┴─────────┘
```
