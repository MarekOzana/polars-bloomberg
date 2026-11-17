---
title: New Issues in BQL
description: Screen the primary bond market in real time with BQL in polars-bloomberg - new AT1 and RT1 deals, new Technology deals, and the pipeline by currency.
tags:
    - BQL
    - Credit
    - Primary market
---

# New Issues

BQL can see bonds that are announced but not yet priced, in line with
`SRCH <GO>` and `PREL <GO>`. The universe covers deals that are announced and
unpriced, or that priced within the last calendar day.
{ .recipe-lead }

Each deal's progress is in `LATST_STATS_FOR_MOST_RECENT_DEAL`:

<div class="recipe-flow" markdown>

1. Mandate
2. Guidance
3. IPT
4. New Deal

</div>

!!! note "Two switches are needed"
    `bondsUniv(INCLUDEPRELIMINARYSECURITIES='Y')` adds preliminary securities
    to the universe, and `PRELIMINARY_SECURITY=='Y'` keeps only those.
    Without the filter you get the whole active bond universe.

!!! info "Data on this page"
    The outputs below are live data as of 2026-11-17 and will differ when you run the
    queries.

## New AT1 / RT1 deals in USD, EUR, GBP

`INCLUDEPRELIMINARYSECURITIES`, `issuance_pricing`, `as #alias`
{ .recipe-uses }

Bank AT1s and insurance RT1s currently in the pipeline, with deal status,
marketed tenor and initial price talk.

```python
from polars_bloomberg import BQuery

query = """
for(filter(bondsUniv(INCLUDEPRELIMINARYSECURITIES='Y'),
        PRELIMINARY_SECURITY=='Y' and
        crncy() in ['USD', 'EUR', 'GBP'] and
        (basel_iii_designation() == 'Additional Tier 1' or
         solvency_ii_designation() == 'Restricted Tier 1')))
get(name, LATST_STATS_FOR_MOST_RECENT_DEAL, marketing_tenor,
    issuance_pricing as #IPT)
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df)
```

Output
``` { .text .no-copy .recipe-output }
shape: (2, 6)
┌───────────────┬───────────────┬──────────────────────────────────┬─────────────────┬──────┬──────────────┐
│ ID            ┆ name          ┆ LATST_STATS_FOR_MOST_RECENT_DEAL ┆ marketing_tenor ┆ #IPT ┆ PRICING_TYPE │
│ ---           ┆ ---           ┆ ---                              ┆ ---             ┆ ---  ┆ ---          │
│ str           ┆ str           ┆ str                              ┆ str             ┆ f64  ┆ str          │
╞═══════════════╪═══════════════╪══════════════════════════════════╪═════════════════╪══════╪══════════════╡
│ DT628319 Corp ┆ ADCBUH 0 PERP ┆ Mandate                          ┆ PERP-NC5.5      ┆ null ┆ null         │
│ DT661776 Corp ┆ BUNQNA 0 PERP ┆ Mandate                          ┆ PERP-NC5        ┆ null ┆ null         │
└───────────────┴───────────────┴──────────────────────────────────┴─────────────────┴──────┴──────────────┘
```

Notes
{ .recipe-label }

- `as #IPT` renames a column without a `let()` block.
- `#IPT` and `PRICING_TYPE` stay `null` until the deal reaches the IPT stage.
  Until the deal prices, the coupon in `name` is often a placeholder `0`.
- `issuance_pricing` returns two columns (the value and `PRICING_TYPE`), so
  the result holds four DataFrames, and `combine(on="ID")` joins them into one
  row per bond.

## New Technology deals in USD, EUR, GBP

`classification_name(BCLASS)`
{ .recipe-uses }

The same screen by sector instead of capital tier, using Bloomberg's BCLASS
classification at level 3.

```python
from polars_bloomberg import BQuery

query = """
for(filter(bondsUniv(INCLUDEPRELIMINARYSECURITIES='Y'),
        PRELIMINARY_SECURITY=='Y' and
        crncy() in ['USD', 'EUR', 'GBP'] and
        classification_name(classification_scheme=BCLASS,
                            classification_level=3) == 'Technology'))
get(name, LATST_STATS_FOR_MOST_RECENT_DEAL, marketing_tenor,
    issuance_pricing as #IPT,
    classification_name(classification_scheme=BCLASS,
                        classification_level=3) as #sector)
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df)
```

Output
``` { .text .no-copy .recipe-output }
shape: (8, 7)
┌───────────────┬───────────────────┬──────────────────────────────────┬─────────────────┬───────┬────────────────┬────────────┐
│ ID            ┆ name              ┆ LATST_STATS_FOR_MOST_RECENT_DEAL ┆ marketing_tenor ┆ #IPT  ┆ PRICING_TYPE   ┆ #sector    │
│ ---           ┆ ---               ┆ ---                              ┆ ---             ┆ ---   ┆ ---            ┆ ---        │
│ str           ┆ str               ┆ str                              ┆ str             ┆ f64   ┆ str            ┆ str        │
╞═══════════════╪═══════════════════╪══════════════════════════════════╪═════════════════╪═══════╪════════════════╪════════════╡
│ DO780075 Corp ┆ GLBUCP 0 5.00Y    ┆ Mandate                          ┆ 5.00            ┆ null  ┆ null           ┆ Technology │
│ DS862826 Corp ┆ GLOWAF 0 ½ 10.00Y ┆ New Deal                         ┆ 10.00           ┆ null  ┆ null           ┆ Technology │
│ DT469787 Corp ┆ NXPI 0 11.00Y     ┆ Mandate                          ┆ 11.00           ┆ null  ┆ null           ┆ Technology │
│ DT469789 Corp ┆ NXPI 0 7.00Y      ┆ Mandate                          ┆ Long 7          ┆ null  ┆ null           ┆ Technology │
│ DT632232 Corp ┆ SOMACI 0 7.00Y    ┆ IPT                              ┆ 7-NC1           ┆ 400.0 ┆ Floater Spread ┆ Technology │
│ DT657219 Corp ┆ PFPT 0 5.00Y      ┆ IPT                              ┆ 5-NC2           ┆ 9.25  ┆ Yield          ┆ Technology │
│ DT661863 Corp ┆ EFOR 0 5.00Y      ┆ IPT                              ┆ 5-NC2           ┆ 10.5  ┆ Yield          ┆ Technology │
│ DT678962 Corp ┆ CCC 0 7.00Y       ┆ New Deal                         ┆ 7-NC3           ┆ 9.0   ┆ Yield          ┆ Technology │
└───────────────┴───────────────────┴──────────────────────────────────┴─────────────────┴───────┴────────────────┴────────────┘
```

!!! warning "Watch out"

    `#IPT` is in the unit that `PRICING_TYPE` names: `Yield` in percent,
    `Floater Spread` in basis points. Don't compare the numbers across rows
    without it.

Notes
{ .recipe-label }

- Without `as #sector`, the column is named after the whole
  `classification_name(...)` expression.
- To screen another sector, change `'Technology'` to another BCLASS level-3
  name.

## Pipeline by currency

`count(group(...))`
{ .recipe-uses }

How many deals are in the pipeline in each currency.

```python
from polars_bloomberg import BQuery

query = """
get(count(group(id, by=crncy())) as #n)
for(filter(bondsUniv(INCLUDEPRELIMINARYSECURITIES='Y'),
           PRELIMINARY_SECURITY=='Y'))
"""
with BQuery() as bq:
    df = bq.bql(query)[0]
print(df.select(currency="ID", deals="#n").sort("deals", descending=True).head(6))
```

Output (top 6 currencies)
``` { .text .no-copy .recipe-output }
shape: (6, 2)
┌──────────┬───────┐
│ currency ┆ deals │
│ ---      ┆ ---   │
│ str      ┆ i64   │
╞══════════╪═══════╡
│ JPY      ┆ 103   │
│ CNY      ┆ 98    │
│ USD      ┆ 49    │
│ EUR      ┆ 30    │
│ IDR      ┆ 28    │
│ MYR      ┆ 28    │
└──────────┴───────┘
```

Notes
{ .recipe-label }

- With `group(...)`, the `ID` column holds the group key, not a security.
- The result also has `ORIG_IDS:*` columns, which are filled only when a group
  has one member. The `select()` drops them.

!!! abstract "Related"
    See also `BQLX <GO>`. The Bloomberg Excel templates "BQL for Primary Markets"
    and "BQL for Issuance Pricing Analysis" cover the same workflows.
