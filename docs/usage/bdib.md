---
title: BDIB - Bloomberg Intraday Bar
description: Fetch Bloomberg BDIB intraday OHLCV bars into Polars for 1-1440 minute analytics, a fast companion to Bloomberg BQL and BDP workflows.
---

# BDIB – Bloomberg Data Intraday Bar

`bdib()` fetches pre-aggregated intraday bars directly from Bloomberg, so you can
work with 1- to 1440-minute candles without managing tick-level aggregation
yourself. Each bar is timestamped in UTC and returned as a Polars DataFrame.

## When to use BDIB

- You need intraday OHLCV bars for a single security over a specific time window.
- You want Bloomberg to enforce consistent interval lengths (e.g., 5-, 15-, 60-minute bars).

## Example

```python
from datetime import datetime
from polars_bloomberg import BQuery

with BQuery() as bq:
    df = bq.bdib(
        "OMX Index",
        event_type="TRADE",   # TRADE, BID, ASK, BEST_BID, BEST_ASK
        interval=60,          # minutes (1-1440)
        start_datetime=datetime(2025, 11, 5),
        end_datetime=datetime(2025, 11, 5, 12),
    )
    print(df)
```

Sample output:

```
shape: (4, 9)
┌───────────┬──────────────┬──────────┬──────────┬───┬──────────┬────────┬───────────┬───────┐
│ security  ┆ time         ┆ open     ┆ high     ┆ … ┆ close    ┆ volume ┆ numEvents ┆ value │
│ ---       ┆ ---          ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---    ┆ ---       ┆ ---   │
│ str       ┆ datetime[μs] ┆ f64      ┆ f64      ┆   ┆ f64      ┆ i64    ┆ i64       ┆ f64   │
╞═══════════╪══════════════╪══════════╪══════════╪═══╪══════════╪════════╪═══════════╪═══════╡
│ OMX Index ┆ 2025-11-05   ┆ 2726.603 ┆ 2742.014 ┆ … ┆ 2739.321 ┆ 0      ┆ 3591      ┆ 0.0   │
│           ┆ 08:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2739.466 ┆ 2739.706 ┆ … ┆ 2733.836 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 09:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2733.747 ┆ 2734.827 ┆ … ┆ 2731.724 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 10:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2025-11-05   ┆ 2731.721 ┆ 2742.015 ┆ … ┆ 2741.185 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 11:00:00     ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
└───────────┴──────────────┴──────────┴──────────┴───┴──────────┴────────┴───────────┴───────┘
```

## Notes

- `start_datetime` and `end_datetime` can be naive or timezone-aware. Naive values
  are treated as UTC; aware values are converted to UTC before the request is sent.
- Supported `event_type` values: `TRADE`, `BID`, `ASK`, `BEST_BID`, `BEST_ASK`.
- Bloomberg enforces the interval range (1-1440 minutes). Requests outside that
  range raise a schema error.
- Optional `overrides` and `options` parameters work the same way as in `bdp()` and
  `bdh()`
