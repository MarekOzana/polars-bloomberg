---
title: Company Fundamentals in BQL
description: Company fundamentals with BQL in polars-bloomberg - find the company that reports the financials behind a bond with fundamentalTicker(), annual revenue and EBITDA, reported vs adjusted EBITDA, revenue by product and by country with segments(), quarterly CET1 and ROE of Swedish AT1 issuers, how much of a high-yield index has fundamentals (issuerOf vs parent(type=CAST) vs fundamentalTicker), median leverage per sector, and Bloomberg Intelligence BI:TPD templates with BSRCH.
tags:
    - BQL
    - BSRCH
    - Credit
    - AT1
    - High yield
    - Fundamentals
---

# Company Fundamentals

A bond has no EBITDA. Often its issuer has none either. Virgin Media O2 issues bonds
through eight vehicles, and the accounts a credit analyst reads belong to none of
them. So before you ask for leverage, you have to find the company that reports
it. BQL's `fundamentalTicker()` does that for bonds and loans.
{ .recipe-lead }

!!! note "Changed in May 2026"
    In May 2026 Bloomberg changed how it chooses that company. It now skips
    financing vehicles and SPVs, and it prefers operating or holding companies with
    current, frequently reported accounts.

!!! info "Data on this page"
    This page starts from bonds, as a credit investor does. It covers European
    high-yield issuers (Virgin Media O2, INEOS, Altice, Telecom Italia, iliad), the
    AT1 issuers SEB, Handelsbanken and Swedbank, and the Bloomberg Pan-European High
    Yield index.

    Amounts are in millions. The outputs below are live data as of 2026-11-17 and will
    differ when you run the queries. Before you adapt a query, read the
    [Pitfalls](#pitfalls).

## Quick reference

=== "By task"

    | You want | Use | One row per |
    |---|---|---|
    | The company whose accounts cover a bond or loan | `fundamentalTicker(<bonds>)` as the universe | company |
    | A company's figure next to each bond | `value(<field>, fundamentalticker, mapby=LINEAGE)` | bond |
    | Annual, half-year, quarterly or LTM figures | `with(fa_period_type=A, fa_period_offset=range(-1Y, 0Y))`, `fpt=S`, `Q`, `LTM` | company and period |
    | Reported or adjusted figures | `fa_adjusted=N` or `Y`; `fa_act_est_data=A` for actuals | company and period |
    | Revenue per business line or country | `segments('<equity>', type=reported, hierarchy=PRODUCT)` or `hierarchy=LOCATION` | segment and period |
    | Bank capital and profitability | `bs_tier1_com_equity_ratio()`, `normalized_roe()` with `fpt=Q` | bank and quarter |
    | A statistic per sector | `median(group(<field>, classification_name(BCLASS, 3)))` over the index's companies | sector |
    | Bloomberg Intelligence's sector figures | `bq.bsrch("BI:TPD", overrides={"BIKEY": ...})` | row of the template |

=== "Period parameters"

    The period parameters work on any fundamental field, either in `with(...)` for the
    whole query or on one field, as in `ebitda(fpt=LTM)`.

    | Parameter | Short | Example values | Meaning |
    |---|---|---|---|
    | `fa_period_type` | `fpt` | `A`, `S`, `Q`, `LTM` | annual, half-year, quarter, last twelve months |
    | `fa_period_offset` | `fpo` | `0`, `-1`, `range(-3Q, 0Q)` | periods counted back from the company's latest report |
    | `fa_period_reference` | `fpr` | `2026Q3`, `range(2026Q1, 2026Q3)` | calendar periods, the same for every company |
    | `fa_adjusted` | | `N`, `Y` | as reported, or Bloomberg-adjusted |
    | `fa_act_est_data` | | `A`, `E` | actuals or estimates |
    | `currency` | | `EUR` | the currency of the result; check the `CURRENCY` column |

## From a bond to the company that reports

`value(..., fundamentalticker, mapby=LINEAGE)`
{ .recipe-uses }

For each ticker, the issuing entities of its live bonds, the company whose accounts
`fundamentalTicker` uses, and the capital structure (CAST) parent.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#accounts = value(name, fundamentalticker, mapby=LINEAGE);
    #cast = value(name, parent(type=CAST), mapby=LINEAGE);)
get(ticker, issuer(), #accounts, #cast)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['VMED', 'INEGRP', 'ALTICE', 'TITIM', 'ILDFP', 'HEIBOS', 'SEB']))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(
    df.group_by("ticker", "#accounts", "#cast")
    .agg(issuers=pl.col("issuer()").n_unique(), bonds=pl.len())
    .sort("ticker", "bonds", descending=[False, True])
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (8, 5)
┌────────┬───────────────────────────────┬───────────────────────────────┬─────────┬───────┐
│ ticker ┆ #accounts                     ┆ #cast                         ┆ issuers ┆ bonds │
│ ---    ┆ ---                           ┆ ---                           ┆ ---     ┆ ---   │
│ str    ┆ str                           ┆ str                           ┆ u32     ┆ u32   │
╞════════╪═══════════════════════════════╪═══════════════════════════════╪═════════╪═══════╡
│ ALTICE ┆ Altice International Sarl     ┆ Next Alt Sarl                 ┆ 2       ┆ 6     │
│ HEIBOS ┆ Heimstaden AB                 ┆ Heimstaden AB                 ┆ 2       ┆ 25    │
│ ILDFP  ┆ iliad SA                      ┆ Maya SAS/Paris France         ┆ 1       ┆ 11    │
│ INEGRP ┆ INEOS Group Holdings SA       ┆ INEOS Ltd                     ┆ 1       ┆ 7     │
│ SEB    ┆ Skandinaviska Enskilda Banken ┆ Skandinaviska Enskilda Banken ┆ 2       ┆ 79    │
│ TITIM  ┆ Telecom Italia SpA/Milano     ┆ Poste Italiane SpA            ┆ 3       ┆ 11    │
│ TITIM  ┆ TIM SA/Brazil                 ┆ Poste Italiane SpA            ┆ 1       ┆ 1     │
│ VMED   ┆ VMED O2 UK Holdings Ltd       ┆ Vmed O2 UK Ltd                ┆ 8       ┆ 19    │
└────────┴───────────────────────────────┴───────────────────────────────┴─────────┴───────┘
```

!!! warning "Watch out"

    - Heimstaden Bostad bonds map to Heimstaden AB, although Bostad publishes its own
      accounts. For a subsidiary's own figures, query its equity directly:
      `issuerOf('<bond>')` gives `1546973D SS Equity` for Heimstaden Bostad.
    - The projected items add `SOURCE_ID`, so join on `ID`.

Notes
{ .recipe-label }

- Eight Virgin Media O2 vehicles, from Virgin Media Secured Finance to the vendor
  financing DACs, lead to one set of accounts. Bloomberg's own example is INEOS:
  the old logic mapped its bonds to the issuer INEOS Finance plc, the new one maps
  them to INEOS Group Holdings SA.
- `fundamentalTicker` and the CAST parent answer different questions. The CAST
  parent of Telecom Italia is Poste Italiane, its largest shareholder. The accounts
  stay with Telecom Italia, which reports them. The CAST parent is the top of the
  capital structure; `fundamentalTicker` is the company whose figures matter for
  the bond.
- One `TITIM` bond is issued by TIM SA, the Brazilian subsidiary, and maps to TIM's
  own accounts.

## Annual revenue and EBITDA, last two years

`fundamentalticker(...)`, `fa_period_type=A`, `fa_period_offset=range(-1Y, 0Y)`
{ .recipe-uses }

Revenue, EBITDA and EBITDA margin of the high-yield companies above, for the last
two fiscal years, in EUR millions.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(name, sales_rev_turn / 1M as #revenue, ebitda / 1M as #ebitda)
for(fundamentalticker(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
                             ticker in ['VMED', 'INEGRP', 'ALTICE', 'TITIM', 'ILDFP'])))
with(fa_period_type=A, fa_period_offset=range(-1Y, 0Y), currency=EUR)
preferences(dropCols=['AS_OF_DATE', 'REVISION_DATE'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
table = df.sort("name", "PERIOD_END_DATE").select(
    "name",
    "PERIOD_END_DATE",
    revenue=pl.col("#revenue").round().cast(pl.Int64),
    ebitda=pl.col("#ebitda").round().cast(pl.Int64),
    margin_pct=(100 * pl.col("#ebitda") / pl.col("#revenue")).round(1),
    ebitda_chg_pct=(100 * pl.col("#ebitda").pct_change().over("name")).round(1),
)
with pl.Config(tbl_rows=20):
    print(table)
```

Output
``` { .text .no-copy .recipe-output }
shape: (12, 6)
┌───────────────────────────┬─────────────────┬─────────┬────────┬────────────┬────────────────┐
│ name                      ┆ PERIOD_END_DATE ┆ revenue ┆ ebitda ┆ margin_pct ┆ ebitda_chg_pct │
│ ---                       ┆ ---             ┆ ---     ┆ ---    ┆ ---        ┆ ---            │
│ str                       ┆ date            ┆ i64     ┆ i64    ┆ f64        ┆ f64            │
╞═══════════════════════════╪═════════════════╪═════════╪════════╪════════════╪════════════════╡
│ Altice International Sarl ┆ 2024-12-31      ┆ 3906    ┆ 1396   ┆ 35.8       ┆ null           │
│ Altice International Sarl ┆ 2025-12-31      ┆ 3845    ┆ 1409   ┆ 36.7       ┆ 0.9            │
│ INEOS Group Holdings SA   ┆ 2024-12-31      ┆ 16180   ┆ 2139   ┆ 13.2       ┆ null           │
│ INEOS Group Holdings SA   ┆ 2025-12-31      ┆ 14298   ┆ 1194   ┆ 8.4        ┆ -44.2          │
│ TIM SA/Brazil             ┆ 2024-12-31      ┆ 4381    ┆ 2172   ┆ 49.6       ┆ null           │
│ TIM SA/Brazil             ┆ 2025-12-31      ┆ 4223    ┆ 2146   ┆ 50.8       ┆ -1.2           │
│ Telecom Italia SpA/Milano ┆ 2024-12-31      ┆ 13653   ┆ 4732   ┆ 34.7       ┆ null           │
│ Telecom Italia SpA/Milano ┆ 2025-12-31      ┆ 13734   ┆ 4502   ┆ 32.8       ┆ -4.9           │
│ VMED O2 UK Holdings Ltd   ┆ 2024-12-31      ┆ 12619   ┆ 4470   ┆ 35.4       ┆ null           │
│ VMED O2 UK Holdings Ltd   ┆ 2025-12-31      ┆ 11808   ┆ 3195   ┆ 27.1       ┆ -28.5          │
│ iliad SA                  ┆ 2024-12-31      ┆ 10024   ┆ 4650   ┆ 46.4       ┆ null           │
│ iliad SA                  ┆ 2025-12-31      ┆ 10349   ┆ 5396   ┆ 52.1       ┆ 16.0           │
└───────────────────────────┴─────────────────┴─────────┴────────┴────────────┴────────────────┘
```

!!! warning "Watch out"

    - `fa_period_offset=range(-1Y, 0Y)` is the last two years each company has
      reported. A company with a March year-end gets other dates than one with a
      December year-end, so keep `PERIOD_END_DATE`.
    - `dropCols=['AS_OF_DATE', 'REVISION_DATE']`: two fields of the same year can carry
      different revision dates, and `combine()` would then put them on two rows.

Notes
{ .recipe-label }

- `fundamentalticker(...)` around a bond screen returns each company once, however
  many bonds and issuers it has.

## Reported vs adjusted EBITDA

`fa_adjusted=N` / `Y`, `fa_act_est_data=A`
{ .recipe-uses }

Bloomberg's adjusted EBITDA next to the reported one, for the last fiscal year.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(name,
    ebitda(fa_adjusted=N) / 1M as #reported,
    ebitda(fa_adjusted=Y) / 1M as #adjusted)
for(fundamentalticker(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
                             ticker in ['VMED', 'INEGRP', 'ALTICE', 'TITIM', 'ILDFP'])))
with(fa_period_type=A, fa_act_est_data=A, currency=EUR)
preferences(dropCols=['AS_OF_DATE', 'REVISION_DATE'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.sort("name").select(
        "name",
        "PERIOD_END_DATE",
        reported=pl.col("#reported").round().cast(pl.Int64),
        adjusted=pl.col("#adjusted").round().cast(pl.Int64),
        adj_pct=(100 * (pl.col("#adjusted") / pl.col("#reported") - 1)).round(1),
    )
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (6, 5)
┌───────────────────────────┬─────────────────┬──────────┬──────────┬─────────┐
│ name                      ┆ PERIOD_END_DATE ┆ reported ┆ adjusted ┆ adj_pct │
│ ---                       ┆ ---             ┆ ---      ┆ ---      ┆ ---     │
│ str                       ┆ date            ┆ i64      ┆ i64      ┆ f64     │
╞═══════════════════════════╪═════════════════╪══════════╪══════════╪═════════╡
│ Altice International Sarl ┆ 2025-12-31      ┆ 1409     ┆ 1385     ┆ -1.7    │
│ INEOS Group Holdings SA   ┆ 2025-12-31      ┆ 1194     ┆ 1315     ┆ 10.2    │
│ TIM SA/Brazil             ┆ 2025-12-31      ┆ 2146     ┆ 2130     ┆ -0.7    │
│ Telecom Italia SpA/Milano ┆ 2025-12-31      ┆ 4502     ┆ 4252     ┆ -5.6    │
│ VMED O2 UK Holdings Ltd   ┆ 2025-12-31      ┆ 3195     ┆ 3195     ┆ 0.0     │
│ iliad SA                  ┆ 2025-12-31      ┆ 5396     ┆ 5223     ┆ -3.2    │
└───────────────────────────┴─────────────────┴──────────┴──────────┴─────────┘
```

!!! warning "Watch out"

    With `fa_period_type=LTM` the same query returns null for VMED O2 UK Holdings,
    although its quarters are there. See the pitfalls below.

Notes
{ .recipe-label }

- Put the parameter on each field, `ebitda(fa_adjusted=Y)`, to get both versions
  in one query. `with(...)` sets it for every field.
- `fa_act_est_data=A` asks for reported actuals, not analysts' estimates.
- The INEOS figures match the example in Bloomberg's announcement of the new
  mapping: EUR 1,194m reported, 1,315m adjusted.

## Revenue by product and by country

`segments(..., hierarchy=PRODUCT)`, `hierarchy=LOCATION`
{ .recipe-uses }

iliad, the French telecom group, reports revenue per business line and per country.
One request per hierarchy, then one table.

```python
import polars as pl
from polars_bloomberg import BQuery

template = """
let(#segment = segment_name();
    #revenue = sales_rev_turn(fpt=S, fpo=range(-1S, 0S)) / 1M;)
get(#segment, #revenue)
for(segments('ILD FP Equity', type=reported, hierarchy={hierarchy}, level=1))
"""
frames = []
with BQuery() as bq:
    for hierarchy in ["PRODUCT", "LOCATION"]:
        df = bq.bql(template.format(hierarchy=hierarchy)).combine()
        frames.append(df.with_columns(hierarchy=pl.lit(hierarchy)))
print(
    pl.concat(frames, how="diagonal_relaxed")
    .with_columns(half=pl.col("PERIOD_END_DATE").cast(pl.String))
    .pivot(on="half", index=["hierarchy", "#segment"], values="#revenue")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (7, 4)
┌───────────┬───────────────────┬────────────┬────────────┐
│ hierarchy ┆ #segment          ┆ 2025-12-31 ┆ 2026-06-30 │
│ ---       ┆ ---               ┆ ---        ┆ ---        │
│ str       ┆ str               ┆ f64        ┆ f64        │
╞═══════════╪═══════════════════╪════════════╪════════════╡
│ PRODUCT   ┆ Devices           ┆ null       ┆ 402.0      │
│ PRODUCT   ┆ Services          ┆ null       ┆ 4838.0     │
│ PRODUCT   ┆ Intra-group sales ┆ null       ┆ null       │
│ LOCATION  ┆ France            ┆ 3350.0     ┆ 3324.0     │
│ LOCATION  ┆ Italy             ┆ 645.0      ┆ 665.0      │
│ LOCATION  ┆ Poland            ┆ 1273.0     ┆ 1258.0     │
│ LOCATION  ┆ Adjustment        ┆ -5.0       ┆ -6.0       │
└───────────┴───────────────────┴────────────┴────────────┘
```

!!! warning "Watch out"

    - `hierarchy=PRODUCT` gives business lines and `hierarchy=LOCATION` gives countries.
      `hierarchy=GEOGRAPHY` fails with "Incorrect parameters provided".
    - Banks report segments too, but `sales_rev_turn` per segment is null for SEB.

Notes
{ .recipe-label }

- iliad reports half-years, hence `fpt=S`. For a quarterly reporter use `fpt=Q` and
  `fpo=range(-1Q, 0Q)`.
- `segments()` needs the equity ticker. Get it from a bond with
  `fundamentalticker` (first recipe): the `ILDFP` bonds map to `ILD FP Equity`.
- Segment rows that do not add up, such as `Adjustment` or `Intra-group sales`, are
  part of the reported split. A segment without a figure for a period is null.

## Swedish AT1 issuers: CET1 and ROE by quarter

`bs_tier1_com_equity_ratio()`, `normalized_roe()`, `fpt=Q`
{ .recipe-uses }

Start from the AT1 bonds of SEB, Handelsbanken and Swedbank. `fundamentalTicker`
turns them into the three banks; then fetch four quarters of CET1 ratio and
return on equity, in percent.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(bs_tier1_com_equity_ratio() as #cet1, normalized_roe() as #roe)
for(fundamentalticker(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
    ticker in ['SEB', 'SHBASS', 'SWEDA'] and
    basel_iii_designation() == 'Additional Tier 1')))
with(fpt=Q, fpo=range(-3Q, 0Q))
preferences(dropCols=['AS_OF_DATE', 'REVISION_DATE'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.select(
        "PERIOD_END_DATE",
        bank=pl.col("ID").str.split(" ").list.first(),
        cet1=pl.col("#cet1"),
        roe=pl.col("#roe").round(1),
    )
    .sort("bank")
    .pivot(on="bank", index="PERIOD_END_DATE", values=["cet1", "roe"])
    .sort("PERIOD_END_DATE")
)
```

Output
``` { .text .no-copy .recipe-output }
shape: (4, 7)
┌─────────────────┬───────────┬───────────┬────────────┬──────────┬──────────┬───────────┐
│ PERIOD_END_DATE ┆ cet1_SEBA ┆ cet1_SHBA ┆ cet1_SWEDA ┆ roe_SEBA ┆ roe_SHBA ┆ roe_SWEDA │
│ ---             ┆ ---       ┆ ---       ┆ ---        ┆ ---      ┆ ---      ┆ ---       │
│ date            ┆ f64       ┆ f64       ┆ f64        ┆ f64      ┆ f64      ┆ f64       │
╞═════════════════╪═══════════╪═══════════╪════════════╪══════════╪══════════╪═══════════╡
│ 2025-12-31      ┆ 17.7      ┆ 17.6      ┆ 17.8       ┆ 13.7     ┆ 11.8     ┆ 16.0      │
│ 2026-03-31      ┆ 17.5      ┆ 17.2      ┆ 17.5       ┆ 13.9     ┆ 13.1     ┆ 20.5      │
│ 2026-06-30      ┆ 17.2      ┆ 17.2      ┆ 17.4       ┆ 14.3     ┆ 12.3     ┆ 21.1      │
│ 2026-09-30      ┆ 17.1      ┆ 17.0      ┆ 17.2       ┆ 13.8     ┆ 11.6     ┆ 19.9      │
└─────────────────┴───────────┴───────────┴────────────┴──────────┴──────────┴───────────┘
```

!!! warning "Watch out"

    `fpt=Q, fpo=range(-3Q, 0Q)` is the last four reported quarters. Before you
    compare banks, check that `PERIOD_END_DATE` is the same for all of them.

Notes
{ .recipe-label }

- The bank tickers (`SEBA`, `SHBA`, `SWEDA`) come from the mapping, not from the
  query. The same code works for any list of bank bonds.
- For CET1 against the AT1 trigger of each bond, see
  [AT1 distance to trigger](capital-structure.md#at1-distance-to-trigger).

## How much of a high-yield index has fundamentals?

`issuerOf` vs `parent(type=CAST)` vs `fundamentalticker`
{ .recipe-uses }

Three ways to reach a company from each bond of the Pan-European High Yield index
(`LP01TREU`): the issuer, the CAST parent and `fundamentalTicker`. For each, the
bonds and index weight with a last-twelve-months net debt / EBITDA, and the number
of companies they lead to.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(id().weights as #weight,
    value(net_debt_to_ebitda(fpt=LTM), issuerOf, mapby=LINEAGE) as #issuerOf,
    value(net_debt_to_ebitda(fpt=LTM), parent(type=CAST), mapby=LINEAGE) as #cast,
    value(net_debt_to_ebitda(fpt=LTM), fundamentalticker, mapby=LINEAGE) as #fundamentalTicker)
for(members('LP01TREU Index'))
"""
with BQuery() as bq:
    result = bq.bql(query)
labels = {
    "#issuerOf": "issuerOf",
    "#cast": "parent(type=CAST)",
    "#fundamentalTicker": "fundamentalTicker",
}
weights = result[0].select("ID", "#weight")
rows = []
for name, item in zip(result.names[1:], result.dataframes[1:]):
    covered = item.filter(pl.col(name).is_not_null()).join(weights, on="ID")
    rows.append(
        {
            "mapping": labels[name],
            "bonds": covered.height,
            "pct_bonds": round(100 * covered.height / weights.height, 1),
            "pct_weight": round(covered["#weight"].sum(), 1),
            "companies": covered["SOURCE_ID"].n_unique(),
        }
    )
print(pl.DataFrame(rows))
```

Output
``` { .text .no-copy .recipe-output }
shape: (3, 5)
┌───────────────────┬───────┬───────────┬────────────┬───────────┐
│ mapping           ┆ bonds ┆ pct_bonds ┆ pct_weight ┆ companies │
│ ---               ┆ ---   ┆ ---       ┆ ---        ┆ ---       │
│ str               ┆ i64   ┆ f64       ┆ f64        ┆ i64       │
╞═══════════════════╪═══════╪═══════════╪════════════╪═══════════╡
│ issuerOf          ┆ 417   ┆ 55.6      ┆ 56.7       ┆ 194       │
│ parent(type=CAST) ┆ 430   ┆ 57.3      ┆ 57.6       ┆ 188       │
│ fundamentalTicker ┆ 553   ┆ 73.7      ┆ 74.5       ┆ 253       │
└───────────────────┴───────┴───────────┴────────────┴───────────┘
```

!!! warning "Watch out"

    The CAST parent can be a shareholder, such as Poste Italiane for Telecom Italia,
    and a shareholder's leverage is not the bond's.

Notes
{ .recipe-label }

- `fundamentalTicker` finds a leverage figure for about three bonds in four, the
  other two mappings for a little over half. It skips financing vehicles without
  accounts, and it reaches companies that issue through a vehicle.
- `companies` counts the companies with a figure. Over the whole index,
  `fundamentalTicker` leads to fewer companies than `issuerOf`, because it puts
  the vehicles of one group under one company. More of those companies have
  figures, though.
- `id().weights` is the index weight in percent; the weights add up to 100.
- The three items are used one by one here, because each has its own `SOURCE_ID`
  and `PERIOD_END_DATE`.

## Median leverage per sector

`median(group(..., classification_name(BCLASS, 3)))`
{ .recipe-uses }

Median net debt / EBITDA per Bloomberg (BCLASS) sector, over the companies behind
the Pan-European High Yield index. The query runs twice, once over the index's
issuers (`issuerOf`) and once over `fundamentalTicker`.

```python
import polars as pl
from polars_bloomberg import BQuery

template = """
let(#sector = classification_name(BCLASS, 3);
    #lev = group(net_debt_to_ebitda(fpt=LTM), #sector);)
get(count(#lev) as #n, median(#lev) as #median)
for({universe}(members('LP01TREU Index')))
preferences(dropCols=['ORIG_IDS', 'AS_OF_DATE', 'REVISION_DATE', 'PERIOD_END_DATE'])
"""
tables = []
with BQuery() as bq:
    for universe in ["issuerOf", "fundamentalTicker"]:
        df = bq.bql(template.format(universe=universe)).combine(on="ID")
        tables.append(
            df.select(
                sector=pl.col("ID").replace({"NullGroup": "(no BCLASS sector)"}),
                n=pl.col("#n"),
                median=pl.col("#median").round(1),
            )
        )
table = (
    tables[0]
    .join(tables[1], on="sector", how="full", coalesce=True, suffix="_ft")
    .sort("n_ft", descending=True, nulls_last=True)
)
with pl.Config(tbl_rows=30):
    print(table)
```

Output
``` { .text .no-copy .recipe-output }
shape: (22, 5)
┌───────────────────────────────────┬──────┬────────┬──────┬───────────┐
│ sector                            ┆ n    ┆ median ┆ n_ft ┆ median_ft │
│ ---                               ┆ ---  ┆ ---    ┆ ---  ┆ ---       │
│ str                               ┆ i64  ┆ f64    ┆ i64  ┆ f64       │
╞═══════════════════════════════════╪══════╪════════╪══════╪═══════════╡
│ Consumer Cyclical                 ┆ 49   ┆ 2.8    ┆ 58   ┆ 2.9       │
│ Communications                    ┆ 22   ┆ 4.0    ┆ 26   ┆ 4.4       │
│ Capital Goods                     ┆ 20   ┆ 3.0    ┆ 25   ┆ 3.1       │
│ Basic Industry                    ┆ 13   ┆ 3.9    ┆ 18   ┆ 4.9       │
│ Consumer Non-Cyclical             ┆ 10   ┆ 4.2    ┆ 18   ┆ 4.7       │
│ (no BCLASS sector)                ┆ null ┆ null   ┆ 15   ┆ 7.8       │
│ Healthcare                        ┆ 12   ┆ 5.5    ┆ 14   ┆ 5.5       │
│ Technology                        ┆ 12   ┆ 3.2    ┆ 14   ┆ 3.4       │
│ Electric                          ┆ 7    ┆ 3.7    ┆ 11   ┆ 3.7       │
│ Real Estate                       ┆ 12   ┆ 10.2   ┆ 11   ┆ 10.3      │
│ Transportation                    ┆ 11   ┆ 4.3    ┆ 9    ┆ 2.0       │
│ Energy                            ┆ 3    ┆ 1.5    ┆ 6    ┆ 0.7       │
│ Other Financial                   ┆ 3    ┆ 5.0    ┆ 6    ┆ 5.1       │
│ Other Industrial                  ┆ 6    ┆ 0.9    ┆ 6    ┆ 0.9       │
│ Banking                           ┆ 4    ┆ 10.4   ┆ 4    ┆ 10.4      │
│ Other Utility                     ┆ 2    ┆ 4.7    ┆ 4    ┆ 7.1       │
│ Brokerage Assetmanagers Exchanges ┆ 3    ┆ -2.3   ┆ 2    ┆ 4.9       │
│ Finance Companies                 ┆ 1    ┆ 3.9    ┆ 2    ┆ 7.7       │
│ Natural Gas                       ┆ 2    ┆ 2.7    ┆ 2    ┆ 2.5       │
│ Government Owned, No Guarantee    ┆ 1    ┆ 2.5    ┆ 1    ┆ 2.5       │
│ REITs                             ┆ 1    ┆ 10.0   ┆ 1    ┆ 9.8       │
│ Insurance                         ┆ 0    ┆ null   ┆ 0    ┆ null      │
└───────────────────────────────────┴──────┴────────┴──────┴───────────┘
```

!!! warning "Watch out"

    - Drop `PERIOD_END_DATE` and `REVISION_DATE` from grouped items. A group's dates
      come from one of its companies and differ between `count` and `median`, so the
      two items would not join.
    - Join on `ID`, the group's name. The group of companies without a sector has
      `ID` `NullGroup` and a null `#SECTOR`, and `combine()` does not match null keys.
    - Leverage of banks and other financials means something else; read those rows
      with care.

Notes
{ .recipe-label }

- `n` is the number of companies with a figure, `median` the median net debt /
  EBITDA. Columns ending in `_ft` are for `fundamentalTicker`.
- `fundamentalTicker` gives more companies with a figure in most sectors. Some of
  the companies it chooses have no BCLASS sector of their own; they end up in
  `(no BCLASS sector)`.
- For an index-weighted figure, Bloomberg's announcement averages over the bonds:
  `wavg(group(#x, #sector), group(weights, #sector))` with
  `#x = value(<field>, fundamentalticker, mapby=LINEAGE)` over `members(...)`.
  An average of ratios moves a lot with a few companies whose EBITDA is near zero
  or negative, so check the inputs.

## Bloomberg Intelligence sector template

BSRCH: `bsrch("BI:TPD", {"BIKEY": ...})`
{ .recipe-uses }

Bloomberg Intelligence (BI) publishes sector figures as dashboards. Each one has a
`BIKEY`, the key in the `=@BSRCH("BI:TPD","BIKEY=...")` formula it exports to Excel.
`bsrch()` gets the same grid. This one has net leverage and spread per turn of
leverage by rating.

```python
import polars as pl
from polars_bloomberg import BQuery

with BQuery() as bq:
    grid = bq.bsrch(
        "BI:TPD", overrides={"BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR", "LIMIT": 20000}
    )
quarters = [c for c in grid.columns if c[:2].isdigit()]
tidy = grid.unpivot(
    index=["Main_Category_Custom_Name", "Category"], on=quarters, variable_name="quarter"
).with_columns(pl.col("quarter").str.to_date("%m/%d/%Y"))
latest = tidy["quarter"].max()
print(f"{len(quarters)} quarters, the latest {latest}")
print(
    tidy.filter(
        pl.col("quarter") == latest,
        pl.col("Main_Category_Custom_Name").str.contains("by Rating|Spread Per Turn"),
        pl.col("Category").is_in(["BB", "B", "CCC"]),
    )
    .with_columns(
        rating=pl.col("Category").cast(pl.Enum(["BB", "B", "CCC"])),
        metric=pl.when(pl.col("Main_Category_Custom_Name").str.starts_with("Spread"))
        .then(pl.lit("spread_per_turn_bp"))
        .otherwise(pl.lit("net_leverage")),
        value=pl.col("value").round(1),
    )
    .pivot(on="metric", index="rating", values="value")
    .sort("rating")
)
```

Output
``` { .text .no-copy .recipe-output }
3 quarters, the latest 2025-06-30
shape: (3, 3)
┌────────┬──────────────┬────────────────────┐
│ rating ┆ net_leverage ┆ spread_per_turn_bp │
│ ---    ┆ ---          ┆ ---                │
│ enum   ┆ f64          ┆ f64                │
╞════════╪══════════════╪════════════════════╡
│ BB     ┆ 2.8          ┆ 73.7               │
│ B      ┆ 4.3          ┆ 77.9               │
│ CCC    ┆ 7.1          ┆ 165.0              │
└────────┴──────────────┴────────────────────┘
```

!!! warning "Watch out"

    The grid has the columns of the saved dashboard. Check the latest quarter before
    you use the numbers, and use the `BIKEY` of your own dashboard for other sectors
    or fields.

Notes
{ .recipe-label }

- The grid has a column per quarter with the date as `MM/DD/YYYY` text; `unpivot`
  and `str.to_date` make it a tidy table.
- `LIMIT` set high avoids a "reached internal limit" warning; the grid is the same.
- See [BSRCH](../usage/bsrch.md) for overrides and column types.

## Pitfalls

What surprised us while writing these recipes.

<div class="recipe-pitfalls" markdown>

| Symptom | Cause | Fix |
|---|---|---|
| Leverage of a bond's issuer is missing | The issuer is a financing vehicle without accounts | `fundamentalticker(...)` as universe, or `value(<field>, fundamentalticker, mapby=LINEAGE)` |
| Companies show different `PERIOD_END_DATE`s for the "same" quarter | `fpo=0` is each company's latest report, not a calendar quarter | Compare `PERIOD_END_DATE`; or use `fpr=2026Q3` for one calendar quarter (null where not yet reported) |
| `fill=PREV` does not fill the missing quarter | It does not carry a value into a period the company has not reported | Use the latest reported period (`fpo=0`) and show its date |
| `fpt=LTM` is null, but the company reports quarters (VMED O2 UK Holdings) | Bloomberg has no LTM figure for that company | Add up the last four quarters: `sum(ebitda(fpt=Q, fpo=range(-3Q, 0Q)))` |
| A group with a null key is split over two rows | `combine()` does not match null keys such as the null `#SECTOR` of `NullGroup` | `combine(on="ID")`; the group's `ID` is never null |
| A company appears on two rows after `combine()` | Fields carry different `REVISION_DATE`s, or grouped items different `PERIOD_END_DATE`s | `preferences(dropCols=['AS_OF_DATE', 'REVISION_DATE'])`, plus `'PERIOD_END_DATE'` for groups |
| `count(group(id))` over `fundamentalticker(members(...))` returns the number of bonds | It counts the bonds that went in (750 for `LP01TREU`), not the companies | Count the rows of `get(name) for(fundamentalticker(...))` |
| `segments(..., hierarchy=GEOGRAPHY)` fails | The hierarchy is called `LOCATION` | `hierarchy=LOCATION` |
| Segment revenue of a bank is null | `sales_rev_turn` is not filled for bank segments | Use group figures, or a field the bank reports per segment |
| Fundamentals in the reporting currency although you asked for EUR | `with(ccy=EUR)` is ignored without an error | `with(currency=EUR)` or `field(currency=EUR)`; check `CURRENCY` |

</div>
