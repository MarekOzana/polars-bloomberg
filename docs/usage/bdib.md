---
title: BDIB - Bloomberg Intraday Bar
description: Fetch Bloomberg BDIB intraday OHLCV bars into Polars for 1-1440 minute analytics, a fast companion to Bloomberg BQL and BDP workflows.
---

# BDIB – Bloomberg Data Intraday Bar

`bdib()` fetches pre-aggregated intraday bars directly from Bloomberg, so you can
work with 1- to 1440-minute candles without managing tick-level aggregation
yourself. Each bar is returned as a row of a Polars DataFrame; its `time` is the start
of the bar, as a naive datetime (no time zone) holding UTC.

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
        event_type="TRADE",  # TRADE, BID, ASK, BEST_BID, BEST_ASK
        interval=60,  # minutes (1-1440)
        start_datetime=datetime(2026, 11, 13),
        end_datetime=datetime(2026, 11, 13, 12),
    )
    print(df)
```

Sample output:

```
shape: (4, 9)
┌───────────┬───────────────┬──────────┬──────────┬───┬──────────┬────────┬───────────┬───────┐
│ security  ┆ time          ┆ open     ┆ high     ┆ … ┆ close    ┆ volume ┆ numEvents ┆ value │
│ ---       ┆ ---           ┆ ---      ┆ ---      ┆   ┆ ---      ┆ ---    ┆ ---       ┆ ---   │
│ str       ┆ datetime[μs]  ┆ f64      ┆ f64      ┆   ┆ f64      ┆ i64    ┆ i64       ┆ f64   │
╞═══════════╪═══════════════╪══════════╪══════════╪═══╪══════════╪════════╪═══════════╪═══════╡
│ OMX Index ┆ 2026-11-13    ┆ 3351.464 ┆ 3353.647 ┆ … ┆ 3350.421 ┆ 0      ┆ 3591      ┆ 0.0   │
│           ┆ 08:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2026-11-13    ┆ 3350.322 ┆ 3359.014 ┆ … ┆ 3358.95  ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 09:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2026-11-13    ┆ 3358.95  ┆ 3366.616 ┆ … ┆ 3365.006 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 10:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
│ OMX Index ┆ 2026-11-13    ┆ 3365.006 ┆ 3365.888 ┆ … ┆ 3362.596 ┆ 0      ┆ 3600      ┆ 0.0   │
│           ┆ 11:00:00      ┆          ┆          ┆   ┆          ┆        ┆           ┆       │
└───────────┴───────────────┴──────────┴──────────┴───┴──────────┴────────┴───────────┴───────┘
```

## Notes

- `start_datetime` and `end_datetime` can be naive or timezone-aware. Naive values
  are treated as UTC; aware values are converted to UTC before the request is sent.
  A `date` means midnight, and an ISO 8601 string (`"2024-01-02T08:00:00"`) is
  sent as it is.
- Supported `event_type` values: `TRADE`, `BID`, `ASK`, `BEST_BID`, `BEST_ASK`.
- Bloomberg enforces the interval range (1-1440 minutes). A request outside that
  range raises `BloombergError` with Bloomberg's unspecific message:
  `Response error: Data not available from service. [nid:…] (NOT_AVAILABLE)`.
- An unknown security raises `BloombergError` too (unlike `bdp()` and `bdh()`, which
  give nulls and a warning). A window without bars, for example one older than the
  few months of intraday history Bloomberg keeps, gives an empty frame that keeps
  its columns.
- Optional `overrides` and `options` parameters work the same way as in `bdp()` and
  `bdh()`
