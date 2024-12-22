# BQL

*Use Case*: Run more advanced queries to screen securities, calculate analytics (like moving averages), or pull fundamental data with complex conditions.

*Returns*: The `bql()` method returns a `BqlResult` object, which:

- Acts like a list of Polars DataFrames (one for each item in BQL `get` statement).
- Provides a `.combine()` method to merge DataFrames on common columns.

## Basic Example
```python
# Fetch the last price of IBM stock
with BQuery() as bq:
    results = bq.bql("get(px_last) for(['IBM US Equity'])")
    print(results[0])  # Access the first DataFrame
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
    
## Multiple Securities
```python
# Fetch the last price for IBM and SEB
with BQuery() as bq:
    results = bq.bql("get(px_last) for(['IBM US Equity', 'SEBA SS Equity'])")
    print(results[0])
```
Output:
```python
┌────────────────┬─────────┬────────────┬──────────┐
│ ID             ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---            ┆ ---     ┆ ---        ┆ ---      │
│ str            ┆ f64     ┆ date       ┆ str      │
╞════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity  ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
│ SEBA SS Equity ┆ 155.2   ┆ 2024-12-14 ┆ SEK      │
└────────────────┴─────────┴────────────┴──────────┘
```

## Multiple Items - combine()
When querying for multiple items, `bql()` returns `BqlResult` object which is actually just a list of polars dataframes with extra method `combine()`
```python
# Fetch name and last price of IBM (two items)
with BQuery() as bq:
    results = bq.bql("get(name, px_last) for(['IBM US Equity'])")
```
Output:
```python
>>> print(len(results))  # 2 DataFrames
n=2

>>> print(results[0])    # First DataFrame: 'name'
┌───────────────┬────────────────────────────────┐
│ ID            ┆ name                           │
│ ---           ┆ ---                            │
│ str           ┆ str                            │
╞═══════════════╪════════════════════════════════╡
│ IBM US Equity ┆ International Business Machine │
└───────────────┴────────────────────────────────┘

>>> print(results[1])    # Second DataFrame: 'px_last'
┌───────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴─────────┴────────────┴──────────┘
```

We can use `combine()` method to join the results into single dataframe:
```python
>>> combined_df = results.combine()
>>> print(combined_df)
```
Output:
```python
┌───────────────┬────────────────────────────────┬─────────┬────────────┬──────────┐
│ ID            ┆ name                           ┆ px_last ┆ DATE       ┆ CURRENCY │
│ ---           ┆ ---                            ┆ ---     ┆ ---        ┆ ---      │
│ str           ┆ str                            ┆ f64     ┆ date       ┆ str      │
╞═══════════════╪════════════════════════════════╪═════════╪════════════╪══════════╡
│ IBM US Equity ┆ International Business Machine ┆ 230.82  ┆ 2024-12-14 ┆ USD      │
└───────────────┴────────────────────────────────┴─────────┴────────────┴──────────┘
```

## Screening
Example of using saved SRCH search and filtering on ticker:
Find list of SEB and Handelsbanken's AT1 bonds and print their names, duration and Z-Spread. The result of the query is list of 3 polars dataframes and can be conveniently combined using `combine()` method.
```python
query="""
    let(#dur=duration(duration_type=MODIFIED); 
        #zsprd=spread(spread_type=Z);) 
    get(name(), #dur, #zsprd) 
    for(filter(screenresults(type=SRCH, screen_name='@COCO'), 
            ticker in ['SEB', 'SHBASS']))
"""

with BQuery() as bq:
    results = bq.bql(query)
    combined_df = results.combine()
    print(combined_df)
```
Output:
```python
┌───────────────┬─────────────────┬──────┬────────────┬────────┐
│ ID            ┆ name()          ┆ #dur ┆ DATE       ┆ #zsprd │
│ ---           ┆ ---             ┆ ---  ┆ ---        ┆ ---    │
│ str           ┆ str             ┆ f64  ┆ date       ┆ f64    │
╞═══════════════╪═════════════════╪══════╪════════════╪════════╡
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ 2.23 ┆ 2024-12-16 ┆ 212.0  │
│ YV402592 Corp ┆ SEB Float PERP  ┆ 0.21 ┆ 2024-12-16 ┆ 233.0  │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ 0.39 ┆ 2024-12-16 ┆ 186.0  │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ 1.95 ┆ 2024-12-16 ┆ 213.0  │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ 4.94 ┆ 2024-12-16 ┆ 256.0  │
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ 5.37 ┆ 2024-12-16 ┆ 309.0  │
└───────────────┴─────────────────┴──────┴────────────┴────────┘
```

## Aggregation
**Average PE per Index Sector**

This example shows aggregation (average) per group (sector) for members of an index.
The resulting list has only one element since there is only one data-item in `get`
```python
query = """
    let(#avg_pe=avg(group(pe_ratio(), gics_sector_name()));)
    get(#avg_pe)
    for(members('OMX Index'))
"""
with BQuery() as bq:
    results = bq.bql(query)
    print(results[0].head(5))
```
Output:
```python
┌──────────────┬───────────┬──────────────┬────────────┬──────────────┬──────────────┬─────────────┐
│ ID           ┆ #avg_pe   ┆ REVISION_DAT ┆ AS_OF_DATE ┆ PERIOD_END_D ┆ ORIG_IDS     ┆ GICS_SECTOR │
│ ---          ┆ ---       ┆ E            ┆ ---        ┆ ATE          ┆ ---          ┆ _NAME()     │
│ str          ┆ f64       ┆ ---          ┆ date       ┆ ---          ┆ str          ┆ ---         │
│              ┆           ┆ date         ┆            ┆ date         ┆              ┆ str         │
╞══════════════╪═══════════╪══════════════╪════════════╪══════════════╪══════════════╪═════════════╡
│ Communicatio ┆ 19.561754 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Communicati │
│ n Services   ┆           ┆              ┆            ┆              ┆              ┆ on Services │
│ Consumer Dis ┆ 19.117295 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Consumer    │
│ cretionary   ┆           ┆              ┆            ┆              ┆              ┆ Discretiona │
│              ┆           ┆              ┆            ┆              ┆              ┆ ry          │
│ Consumer     ┆ 15.984743 ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ ESSITYB SS   ┆ Consumer    │
│ Staples      ┆           ┆              ┆            ┆              ┆ Equity       ┆ Staples     │
│ Financials   ┆ 6.815895  ┆ 2024-10-24   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Financials  │
│ Health Care  ┆ 22.00628  ┆ 2024-11-12   ┆ 2024-12-14 ┆ 2024-09-30   ┆ null         ┆ Health Care │
└──────────────┴───────────┴──────────────┴────────────┴──────────────┴──────────────┴─────────────┘
```

**HY Maturity Wall**

And another example on the same topic. Calculate amount outstanding bonds in HY index per maturity year
```python
query = """
let(#mv=sum(group(amt_outstanding(currency=USD),
                  by=[year(maturity()), industry_sector()]));)
get(#mv)
for(members('LF98TRUU Index'))
"""
with BQuery() as bq:
    results = bq.bql(query)
df = results.combine().rename(
    {"YEAR(MATURITY())": "maturity", "INDUSTRY_SECTOR()": "sector", "#mv": "mv"}
)

print(df.pivot(index="maturity", on="sector", values="mv").head())
```
Output:
```python
shape: (5, 11)
┌──────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
│ maturity ┆ Basic     ┆ Consumer, ┆ Energy    ┆ … ┆ Financial ┆ Technolog ┆ Utilities ┆ Diversifi │
│ ---      ┆ Materials ┆ Non-cycli ┆ ---       ┆   ┆ ---       ┆ y         ┆ ---       ┆ ed        │
│ i64      ┆ ---       ┆ cal       ┆ f64       ┆   ┆ f64       ┆ ---       ┆ f64       ┆ ---       │
│          ┆ f64       ┆ ---       ┆           ┆   ┆           ┆ f64       ┆           ┆ f64       │
│          ┆           ┆ f64       ┆           ┆   ┆           ┆           ┆           ┆           │
╞══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
│ 2025     ┆ 1.5e8     ┆ 5.34916e8 ┆ 5e8       ┆ … ┆ null      ┆ null      ┆ null      ┆ null      │
│ 2026     ┆ 4.4013e9  ┆ 9.3293e9  ┆ 8.2931e9  ┆ … ┆ 1.3524e10 ┆ 4.0608e9  ┆ 2.5202e9  ┆ null      │
│ 2027     ┆ 8.3921e9  ┆ 2.3409e10 ┆ 1.2427e10 ┆ … ┆ 1.9430e10 ┆ 4.3367e9  ┆ 3.6620e9  ┆ null      │
│ 2028     ┆ 1.4701e10 ┆ 3.7457e10 ┆ 2.2442e10 ┆ … ┆ 2.3341e10 ┆ 9.9143e9  ┆ 7.6388e9  ┆ 5e8       │
│ 2029     ┆ 1.6512e10 ┆ 5.7381e10 ┆ 3.9286e10 ┆ … ┆ 4.2337e10 ┆ 2.2660e10 ┆ 5.8558e9  ┆ null      │
└──────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘
```


## Axes
Get current axes of all Swedish USD AT1 bonds
```python
# Get current axes for Swedish AT1 bonds in USD
query="""
    let(#ax=axes();)
    get(security_des, #ax)
    for(filter(bondsuniv(ACTIVE),
        crncy()=='USD' and
        basel_iii_designation() == 'Additional Tier 1' and
        country_iso() == 'SE'))
"""

with BQuery() as bq:
    results = bq.bql(query)
    print(results.combine())

┌───────────────┬─────────────────┬─────┬───────────┬───────────┬────────────────┬────────────────┐
│ ID            ┆ security_des    ┆ #ax ┆ ASK_DEPTH ┆ BID_DEPTH ┆ ASK_TOTAL_SIZE ┆ BID_TOTAL_SIZE │
│ ---           ┆ ---             ┆ --- ┆ ---       ┆ ---       ┆ ---            ┆ ---            │
│ str           ┆ str             ┆ str ┆ i64       ┆ i64       ┆ f64            ┆ f64            │
╞═══════════════╪═════════════════╪═════╪═══════════╪═══════════╪════════════════╪════════════════╡
│ YU819930 Corp ┆ SEB 6 ¾ PERP    ┆ Y   ┆ 2         ┆ null      ┆ 5.6e6          ┆ null           │
│ ZO703315 Corp ┆ SHBASS 4 ⅜ PERP ┆ Y   ┆ 1         ┆ 2         ┆ 5e6            ┆ 6e6            │
│ BR069680 Corp ┆ SWEDA 4 PERP    ┆ Y   ┆ null      ┆ 1         ┆ null           ┆ 3e6            │
│ ZL122341 Corp ┆ SWEDA 7 ⅝ PERP  ┆ Y   ┆ null      ┆ 6         ┆ null           ┆ 2.04e7         │
│ ZQ349286 Corp ┆ SEB 5 ⅛ PERP    ┆ Y   ┆ 2         ┆ 4         ┆ 5.5e6          ┆ 3e7            │
│ ZF859199 Corp ┆ SWEDA 7 ¾ PERP  ┆ Y   ┆ 1         ┆ 1         ┆ 2e6            ┆ 2e6            │
│ ZO703956 Corp ┆ SHBASS 4 ¾ PERP ┆ Y   ┆ 1         ┆ 3         ┆ 1.2e6          ┆ 1.1e7          │
│ BW924993 Corp ┆ SEB 6 ⅞ PERP    ┆ Y   ┆ 1         ┆ 3         ┆ 5e6            ┆ 1.1e7          │
└───────────────┴─────────────────┴─────┴───────────┴───────────┴────────────────┴────────────────┘
```

## Segments
The following example shows handling of two data-items with different length. The first dataframe 
describes the segments (and has length 5 in this case), while the second dataframe contains time series.
One can join the dataframes on common columns and pivot the segments into columns as shown below:
```python
# revenue per segment
query = """
    let(#segment=segment_name();
        #revenue=sales_Rev_turn(fpt=q, fpr=range(2023Q3, 2024Q3));
        )
    get(#segment, #revenue)
    for(segments('GTN US Equity',type=reported,hierarchy=PRODUCT, level=1))
"""
with BQuery() as bq:
    results = bq.bql(query)
    df = results.combine().pivot(
        index="PERIOD_END_DATE", on="#segment", values="#revenue"
    )
    print(df)
```
Output:
```python
┌─────────────────┬──────────────┬──────────────────────┬────────┬────────────┐
│ PERIOD_END_DATE ┆ Broadcasting ┆ Production Companies ┆ Other  ┆ Adjustment │
│ ---             ┆ ---          ┆ ---                  ┆ ---    ┆ ---        │
│ date            ┆ f64          ┆ f64                  ┆ f64    ┆ f64        │
╞═════════════════╪══════════════╪══════════════════════╪════════╪════════════╡
│ 2023-09-30      ┆ 7.83e8       ┆ 2e7                  ┆ 1.6e7  ┆ null       │
│ 2023-12-31      ┆ 8.13e8       ┆ 3.2e7                ┆ 1.9e7  ┆ null       │
│ 2024-03-31      ┆ 7.8e8        ┆ 2.4e7                ┆ 1.9e7  ┆ null       │
│ 2024-06-30      ┆ 8.08e8       ┆ 1.8e7                ┆ 0.0    ┆ null       │
│ 2024-09-30      ┆ 9.24e8       ┆ 2.6e7                ┆ 1.7e7  ┆ null       │
└─────────────────┴──────────────┴──────────────────────┴────────┴────────────┘
```

## Time Series
This is example of a single-item query returning total return for all GTN bonds in a long dataframe.
We can easily pivot it into wide format, as in the example below
```python
# Total Return of GTN Bonds
query = """
let(#rng = range(-1M, 0D);
    #rets = return_series(calc_interval=#rng,per=W);)
get(#rets)
for(filter(bonds('GTN US Equity'), series() == '144A'))
"""

with BQuery() as bq:
    results = bq.bql(query)
    df = results[0].pivot(on="ID", index="DATE", values="#rets")
    print(df)
```
Output:
```python
shape: (6, 6)
┌────────────┬───────────────┬───────────────┬───────────────┬───────────────┬───────────────┐
│ DATE       ┆ YX231113 Corp ┆ BS116983 Corp ┆ AV438089 Corp ┆ ZO860846 Corp ┆ LW375188 Corp │
│ ---        ┆ ---           ┆ ---           ┆ ---           ┆ ---           ┆ ---           │
│ date       ┆ f64           ┆ f64           ┆ f64           ┆ f64           ┆ f64           │
╞════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╪═══════════════╡
│ 2024-11-17 ┆ null          ┆ null          ┆ null          ┆ null          ┆ null          │
│ 2024-11-24 ┆ 0.001653      ┆ 0.051179      ┆ 0.020363      ┆ 0.001371      ┆ -0.002939     │
│ 2024-12-01 ┆ 0.002837      ┆ 0.010405      ┆ -0.001466     ┆ 0.007275      ┆ 0.000581      │
│ 2024-12-08 ┆ -0.000041     ┆ 0.016145      ┆ 0.000766      ┆ 0.024984      ┆ 0.000936      │
│ 2024-12-15 ┆ 0.001495      ┆ -0.047        ┆ -0.000233     ┆ -0.043509     ┆ 0.002241      │
│ 2024-12-17 ┆ 0.00008       ┆ -0.000004     ┆ -0.0035       ┆ -0.007937     ┆ 0.000064      │
└────────────┴───────────────┴───────────────┴───────────────┴───────────────┴───────────────┘
```

## Technical Analysis
```python
with BQuery() as bq:
    results = bq.bql(
        """
        let(#ema20=emavg(period=20);
            #ema200=emavg(period=200);
            #rsi=rsi(close=px_last());)
        get(name(), #ema20, #ema200, #rsi)
        for(filter(members('OMX Index'),
                    and(#ema20 > #ema200, #rsi > 53)))
        with(fill=PREV)
        """
    )
    print(results.combine())
```
Output:
```python
┌─────────────────┬──────────────────┬────────────┬────────────┬──────────┬────────────┬───────────┐
│ ID              ┆ name()           ┆ #ema20     ┆ DATE       ┆ CURRENCY ┆ #ema200    ┆ #rsi      │
│ ---             ┆ ---              ┆ ---        ┆ ---        ┆ ---      ┆ ---        ┆ ---       │
│ str             ┆ str              ┆ f64        ┆ date       ┆ str      ┆ f64        ┆ f64       │
╞═════════════════╪══════════════════╪════════════╪════════════╪══════════╪════════════╪═══════════╡
│ ERICB SS Equity ┆ Telefonaktiebola ┆ 90.152604  ┆ 2024-12-16 ┆ SEK      ┆ 75.072151  ┆ 56.010028 │
│                 ┆ get LM Ericsso   ┆            ┆            ┆          ┆            ┆           │
│ ABB SS Equity   ┆ ABB Ltd          ┆ 630.622469 ┆ 2024-12-16 ┆ SEK      ┆ 566.571183 ┆ 53.763102 │
│ SEBA SS Equity  ┆ Skandinaviska    ┆ 153.80595  ┆ 2024-12-16 ┆ SEK      ┆ 150.742394 ┆ 56.460733 │
│                 ┆ Enskilda Banken  ┆            ┆            ┆          ┆            ┆           │
│ ASSAB SS Equity ┆ Assa Abloy AB    ┆ 339.017591 ┆ 2024-12-16 ┆ SEK      ┆ 317.057573 ┆ 53.351619 │
└─────────────────┴──────────────────┴────────────┴────────────┴──────────┴────────────┴───────────┘
```