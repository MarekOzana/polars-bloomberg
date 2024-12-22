# BDH

Use Case: Retrieve historical data over a date range, such as daily closing prices or volumes.
```python
with BQuery() as bq:
    df = bq.bdh(
        ["TLT US Equity"],
        ["PX_LAST"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 7),
    )
    print(df)

┌───────────────┬────────────┬─────────┐
│ security      ┆ date       ┆ PX_LAST │
│ ---           ┆ ---        ┆ ---     │
│ str           ┆ date       ┆ f64     │
╞═══════════════╪════════════╪═════════╡
│ TLT US Equity ┆ 2019-01-02 ┆ 122.15  │
│ TLT US Equity ┆ 2019-01-03 ┆ 123.54  │
│ TLT US Equity ┆ 2019-01-04 ┆ 122.11  │
│ TLT US Equity ┆ 2019-01-07 ┆ 121.75  │
└───────────────┴────────────┴─────────┘
```

## BDH with multiple securities / fields
```python
with BQuery() as bq:
    df = bq.bdh(
        securities=["SPY US Equity", "TLT US Equity"],
        fields=["PX_LAST", "VOLUME"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 10),
        options={"adjustmentSplit": True},
    )
    print(df)

┌───────────────┬────────────┬─────────┬──────────────┐
│ security      ┆ date       ┆ PX_LAST ┆ VOLUME       │
│ ---           ┆ ---        ┆ ---     ┆ ---          │
│ str           ┆ date       ┆ f64     ┆ f64          │
╞═══════════════╪════════════╪═════════╪══════════════╡
│ SPY US Equity ┆ 2019-01-02 ┆ 250.18  ┆ 1.26925199e8 │
│ SPY US Equity ┆ 2019-01-03 ┆ 244.21  ┆ 1.44140692e8 │
│ SPY US Equity ┆ 2019-01-04 ┆ 252.39  ┆ 1.42628834e8 │
│ SPY US Equity ┆ 2019-01-07 ┆ 254.38  ┆ 1.031391e8   │
│ SPY US Equity ┆ 2019-01-08 ┆ 256.77  ┆ 1.02512587e8 │
│ …             ┆ …          ┆ …       ┆ …            │
│ TLT US Equity ┆ 2019-01-04 ┆ 122.11  ┆ 1.2970226e7  │
│ TLT US Equity ┆ 2019-01-07 ┆ 121.75  ┆ 8.498104e6   │
│ TLT US Equity ┆ 2019-01-08 ┆ 121.43  ┆ 7.737103e6   │
│ TLT US Equity ┆ 2019-01-09 ┆ 121.24  ┆ 9.349245e6   │
│ TLT US Equity ┆ 2019-01-10 ┆ 120.46  ┆ 8.22286e6    │
└───────────────┴────────────┴─────────┴──────────────┘
```

## BDH with options - periodicitySelection: Monthly
```python
with BQuery() as bq:
    df = bq.bdh(['AAPL US Equity'], 
                ['PX_LAST'], 
                start_date=date(2019, 1, 1), 
                end_date=date(2019, 3, 29),
                options={"periodicitySelection": "MONTHLY"})
    print(df)

┌────────────────┬────────────┬─────────┐
│ security       ┆ date       ┆ PX_LAST │
│ ---            ┆ ---        ┆ ---     │
│ str            ┆ date       ┆ f64     │
╞════════════════╪════════════╪═════════╡
│ AAPL US Equity ┆ 2019-01-31 ┆ 41.61   │
│ AAPL US Equity ┆ 2019-02-28 ┆ 43.288  │
│ AAPL US Equity ┆ 2019-03-29 ┆ 47.488  │
└────────────────┴────────────┴─────────┘
```