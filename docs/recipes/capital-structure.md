---
title: Bonds and Capital Structure in BQL
description: Capital structure analysis with BQL in polars-bloomberg - Swedish bank capital stacks, AT1 and Tier 2 call schedules, CET1 distance to the AT1 trigger, bonds and loans with leverage for a European high-yield issuer, the capital structure (CAST) parent with parent(type=CAST), and debt by parent across a high-yield index.
tags:
    - BQL
    - Credit
    - AT1
    - High yield
    - Capital structure
---

# Bonds and Capital Structure

Which bonds and loans does a company have, how do they rank, who owns the issuer,
and how much debt sits at each level? This page answers these questions for the
three large Swedish banks, for a European high-yield issuer (Virgin Media O2) and
for a group with several issuers (Heimstaden).

| You want | Use | One row per |
|---|---|---|
| All live bonds of an issuer | `filter(bondsUniv(ACTIVE, consolidateduplicates=True), ticker in [...])` | bond |
| Its loans | `filter(loansUniv(ACTIVE), ticker == '...')` | loan |
| Where a bond ranks | `normalized_payment_rank()` (comparable across issuers), `payment_rank()` (as issued), `basel_iii_designation()` (bank capital) | bond |
| Debt per rank, per issuer or per parent | `sum(group(amt_outstanding(currency=EUR), ...))` | group |
| The company that owns the issuer | `parent(type=DIRECT)`, `type=ULTIMATE` or `type=CAST` | company |
| The parent's fundamentals next to each bond | `value(<field>, parent(type=CAST), mapby=LINEAGE)` | bond |

The capital structure (CAST) parent is the top company of the capital structure
you see in `CAST <GO>`. Unlike the ultimate parent, it stops below private-equity
sponsors, owners that hold the company as an investment, and governments that
guarantee less than half of the group's debt. Since May 2026 BQL's `parent()`
returns it with `type=CAST`.

| `parent(type=...)` | Returns |
|---|---|
| `DIRECT` (default) | the immediate parent company |
| `ULTIMATE` | the company that ultimately controls the issuer |
| `CAST` | the top of the capital structure in `CAST <GO>` |

Amounts are converted to EUR so that they add up across currencies. The outputs
below are live data as of 2026-11-17 and will differ when you run the queries.

## Swedish bank capital stacks

Covered bonds, senior, senior non-preferred, Tier 2 and AT1 outstanding for SEB,
Handelsbanken and Swedbank, in EUR billions.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#rank = normalized_payment_rank().value;
    #amt = group(amt_outstanding(currency=EUR).value / 1B, [ticker, #rank]);)
get(sum(#amt) as #eur_bn)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['SEB', 'SHBASS', 'SWEDA']))
"""
layers = {
    "Secured": "1 Secured (covered)",
    "Sr Unsecured": "2 Senior preferred",
    "Senior Bail-In": "3 Senior non-preferred",
    "Subordinated Unsecured": "4 Tier 2",
    "Jr Subordinated Unsecured": "5 AT1",
}
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.with_columns(layer=pl.col("#RANK").replace(layers), eur_bn=pl.col("#eur_bn").round(1))
    .pivot(on="TICKER", index="layer", values="eur_bn")
    .sort("layer")
)
```
Output:
```python
shape: (5, 4)
┌────────────────────────┬──────┬────────┬───────┐
│ layer                  ┆ SEB  ┆ SHBASS ┆ SWEDA │
│ ---                    ┆ ---  ┆ ---    ┆ ---   │
│ str                    ┆ f64  ┆ f64    ┆ f64   │
╞════════════════════════╪══════╪════════╪═══════╡
│ 1 Secured (covered)    ┆ 32.0 ┆ 56.8   ┆ 36.8  │
│ 2 Senior preferred     ┆ 11.7 ┆ 27.0   ┆ 14.3  │
│ 3 Senior non-preferred ┆ 12.0 ┆ 7.6    ┆ 12.5  │
│ 4 Tier 2               ┆ 2.8  ┆ 2.3    ┆ 2.8   │
│ 5 AT1                  ┆ 1.9  ┆ 1.4    ┆ 1.5   │
└────────────────────────┴──────┴────────┴───────┘
```

Notes:

- `normalized_payment_rank()` maps each bond to one ladder, the same for all issuers.
  `Senior Bail-In` is senior non-preferred (MREL), `Subordinated` is Tier 2 and
  `Jr Subordinated` is AT1.
- The group column comes back as `#RANK` (upper case) and the ticker as `TICKER`.
- `consolidateduplicates=True` keeps one line per bond instead of one per listing.
- Swedish banks also issue structured notes and MTNs under the same ticker, so the
  senior preferred counts are large.

## AT1 and Tier 2 call schedule

How much AT1 and Tier 2 each bank can call per year, in EUR millions.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
get(ticker, basel_iii_designation, nxt_call_dt,
    amt_outstanding(currency=EUR).value / 1M as #eur_m)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['SEB', 'SHBASS', 'SWEDA'] and
           basel_iii_designation() in ['Additional Tier 1', 'Tier 2']))
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
tier = pl.col("basel_iii_designation").replace({"Additional Tier 1": "AT1", "Tier 2": "T2"})
print(
    df.with_columns(
        bank_tier=pl.col("ticker") + " " + tier,
        call_year=pl.col("nxt_call_dt").dt.year(),
        eur_m=pl.col("#eur_m").round().cast(pl.Int64),
    )
    .sort("bank_tier")
    .pivot(on="bank_tier", index="call_year", values="eur_m", aggregate_function="sum")
    .sort("call_year")
)
```
Output:
```python
shape: (7, 7)
┌───────────┬─────────┬────────┬────────────┬───────────┬───────────┬──────────┐
│ call_year ┆ SEB AT1 ┆ SEB T2 ┆ SHBASS AT1 ┆ SHBASS T2 ┆ SWEDA AT1 ┆ SWEDA T2 │
│ ---       ┆ ---     ┆ ---    ┆ ---        ┆ ---       ┆ ---       ┆ ---      │
│ i32       ┆ i64     ┆ i64    ┆ i64        ┆ i64       ┆ i64       ┆ i64      │
╞═══════════╪═════════╪════════╪════════════╪═══════════╪═══════════╪══════════╡
│ 2027      ┆ 466     ┆ 0      ┆ 424        ┆ 594       ┆ 0         ┆ 1258     │
│ 2028      ┆ 0       ┆ 842    ┆ 0          ┆ 500       ┆ 472       ┆ 393      │
│ 2029      ┆ 439     ┆ 500    ┆ 0          ┆ 750       ┆ 425       ┆ 0        │
│ 2030      ┆ 0       ┆ 411    ┆ 0          ┆ 0         ┆ 607       ┆ 500      │
│ 2031      ┆ 459     ┆ 0      ┆ 975        ┆ 500       ┆ 0         ┆ 675      │
│ 2032      ┆ 0       ┆ 1007   ┆ 0          ┆ 0         ┆ 0         ┆ 0        │
│ 2034      ┆ 516     ┆ 0      ┆ 0          ┆ 0         ┆ 0         ┆ 0        │
└───────────┴─────────┴────────┴────────────┴───────────┴───────────┴──────────┘
```

Notes:

- `nxt_call_dt` is the next call date. AT1s are perpetual, so the call date is the
  date that matters.
- `basel_iii_designation()` is the regulatory tier, a better filter for bank capital
  than the payment rank.

## AT1 distance to trigger

The USD AT1s of the Swedish banks (the bonds on the [Dealer Axes](axes.md) page),
each with its bank's CET1 ratio and the bond's trigger, in percent of risk-weighted
assets.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#cet1 = value(bs_tier1_com_equity_ratio(), parent(type=CAST), mapby=LINEAGE);
    #trigger = capital_type_coco_trigger_level();)
get(name, nxt_call_dt, #cet1, #trigger)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['SEB', 'SHBASS', 'SWEDA'] and crncy() == 'USD' and
           basel_iii_designation() == 'Additional Tier 1'))
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(
    df.select(
        "name", "nxt_call_dt", "PERIOD_END_DATE", "#cet1", "#trigger",
        buffer_pp=pl.col("#cet1") - pl.col("#trigger"),
    ).sort("name", "nxt_call_dt")
)
```
Output:
```python
shape: (8, 6)
┌─────────────────┬─────────────┬─────────────────┬───────┬──────────┬───────────┐
│ name            ┆ nxt_call_dt ┆ PERIOD_END_DATE ┆ #cet1 ┆ #trigger ┆ buffer_pp │
│ ---             ┆ ---         ┆ ---             ┆ ---   ┆ ---      ┆ ---       │
│ str             ┆ date        ┆ date            ┆ f64   ┆ f64      ┆ f64       │
╞═════════════════╪═════════════╪═════════════════╪═══════╪══════════╪═══════════╡
│ SEB 6 ¾ PERP    ┆ 2031-11-04  ┆ 2026-09-30      ┆ 17.1  ┆ 5.125    ┆ 11.975    │
│ SEB 6 ¾ PERP    ┆ 2034-06-02  ┆ 2026-09-30      ┆ 17.1  ┆ 5.125    ┆ 11.975    │
│ SEB 6 ⅞ PERP    ┆ 2027-06-30  ┆ 2026-09-30      ┆ 17.1  ┆ 5.125    ┆ 11.975    │
│ SHBASS 4 ¾ PERP ┆ 2031-03-01  ┆ 2026-09-30      ┆ 17.0  ┆ 5.125    ┆ 11.875    │
│ SHBASS 4 ⅜ PERP ┆ 2027-03-01  ┆ 2026-09-30      ┆ 17.0  ┆ 5.125    ┆ 11.875    │
│ SWEDA 4 PERP    ┆ 2029-03-17  ┆ 2026-09-30      ┆ 17.2  ┆ 5.125    ┆ 12.075    │
│ SWEDA 7 ¾ PERP  ┆ 2030-03-17  ┆ 2026-09-30      ┆ 17.2  ┆ 5.125    ┆ 12.075    │
│ SWEDA 7 ⅝ PERP  ┆ 2028-03-17  ┆ 2026-09-30      ┆ 17.2  ┆ 5.125    ┆ 12.075    │
└─────────────────┴─────────────┴─────────────────┴───────┴──────────┴───────────┘
```

Notes:

- `value(..., parent(type=CAST), mapby=LINEAGE)` reads the field from each bond's
  capital structure parent, here the listed bank. `PERIOD_END_DATE` is the reporting
  quarter of the CET1 ratio.
- The projected item adds `SOURCE_ID`, the parent's ID. Join on `ID`, or the rows of
  the two items do not match.
- For CET1 against the full capital requirement, see the
  [Bank CET1 Ratio vs Requirements](../examples/credit/bank-CET1-vs-requirements.ipynb)
  example.

## Bonds and loans with leverage

The whole debt stack of Virgin Media O2, bonds and term loans, by rank. Leverage is
the debt up to and including each rank divided by last-twelve-months EBITDA.

```python
import polars as pl
from polars_bloomberg import BQuery

fields = """
get(name, maturity, normalized_payment_rank().value as #rank,
    amt_outstanding(currency=EUR).value / 1M as #eur_m)
"""
bonds = "for(filter(bondsUniv(ACTIVE, consolidateduplicates=True), ticker == 'VMED'))"
loans = "for(filter(loansUniv(ACTIVE), ticker == 'VMED' and amt_outstanding() > 0))"
parent = """
get(name, ebitda(fpt=LTM, currency=EUR) / 1M as #ebitda)
for(parent('ZO388622 Corp', type=CAST))
"""
seniority = {"1st Lien Secured": 1, "Secured": 2, "Sr Unsecured": 3}
with BQuery() as bq:
    debt = pl.concat([
        bq.bql(fields + bonds).combine().with_columns(kind=pl.lit("bond")),
        bq.bql(fields + loans).combine().with_columns(kind=pl.lit("loan")),
    ])
    group = bq.bql(parent).combine()
ebitda = group["#ebitda"].item()
print(f"{group['name'].item()}: LTM EBITDA EUR {ebitda:,.0f}m ({group['PERIOD_END_DATE'].item()})")
print(
    debt.group_by("#rank")
    .agg(
        bonds=(pl.col("kind") == "bond").sum(),
        loans=(pl.col("kind") == "loan").sum(),
        eur_m=pl.col("#eur_m").sum().round().cast(pl.Int64),
    )
    .sort(pl.col("#rank").replace_strict(seniority, default=9))
    .with_columns(cum_leverage=(pl.col("eur_m").cum_sum() / ebitda).round(1))
)
```
Output:
```python
Vmed O2 UK Ltd: LTM EBITDA EUR 3,186m (2025-12-31)
shape: (3, 5)
┌──────────────────┬───────┬───────┬───────┬──────────────┐
│ #rank            ┆ bonds ┆ loans ┆ eur_m ┆ cum_leverage │
│ ---              ┆ ---   ┆ ---   ┆ ---   ┆ ---          │
│ str              ┆ u32   ┆ u32   ┆ i64   ┆ f64          │
╞══════════════════╪═══════╪═══════╪═══════╪══════════════╡
│ 1st Lien Secured ┆ 5     ┆ 15    ┆ 18972 ┆ 6.0          │
│ Secured          ┆ 12    ┆ 0     ┆ 10120 ┆ 9.1          │
│ Sr Unsecured     ┆ 2     ┆ 0     ┆ 1314  ┆ 9.5          │
└──────────────────┴───────┴───────┴───────┴──────────────┘
```

Notes:

- `parent('ZO388622 Corp', type=CAST)` turns one bond into its group, Vmed O2 UK Ltd,
  whose accounts cover all the issuing entities.
- Put the currency on the item, `ebitda(..., currency=EUR)`, or use
  `with(currency=EUR)`. `with(ccy=EUR)` is ignored without an error, and the numbers
  stay in the reporting currency. Check the `CURRENCY` column.
- The order comes from Bloomberg's normalized rank. Read it as a debt stack, not a
  legal waterfall: debt of neighbouring ranks can share the same collateral.
- Private groups report less often; `PERIOD_END_DATE` shows which accounts you got.
- `loans()` on the parent returns nothing for this group, so the loans come from
  `loansUniv(ACTIVE)` by ticker. `amt_outstanding() > 0` drops repaid and undrawn
  facilities.
- Some `VMED` bonds are vendor-financing notes issued by separate vehicles. Use
  `issuer()` to see the issuing entity of each bond.

## Direct, ultimate and CAST parent

Heimstaden has two bond issuers: Heimstaden AB and its residential subsidiary
Heimstaden Bostad. The three parent types answer "who owns this?" differently.

```python
from polars_bloomberg import BQuery

query = """
get(name, issuer(),
    value(name, parent(type=DIRECT), mapby=LINEAGE) as #direct,
    value(name, parent(type=ULTIMATE), mapby=LINEAGE) as #ultimate,
    value(name, parent(type=CAST), mapby=LINEAGE) as #cast)
for(['BJ158441 Corp', 'BR871064 Corp'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(df.select("ID", "name", "issuer()", "#direct", "#ultimate", "#cast"))
```
Output:
```python
shape: (2, 6)
┌───────────────┬─────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬───────────────┐
│ ID            ┆ name                ┆ issuer()             ┆ #direct              ┆ #ultimate            ┆ #cast         │
│ ---           ┆ ---                 ┆ ---                  ┆ ---                  ┆ ---                  ┆ ---           │
│ str           ┆ str                 ┆ str                  ┆ str                  ┆ str                  ┆ str           │
╞═══════════════╪═════════════════════╪══════════════════════╪══════════════════════╪══════════════════════╪═══════════════╡
│ BJ158441 Corp ┆ HEIBOS 2.8 05/04/35 ┆ HEIMSTADEN BOSTAD AB ┆ Heimstaden Bostad AB ┆ Heimstaden Bostad AB ┆ Heimstaden AB │
│ BR871064 Corp ┆ HEIMST 6 ¾ PERP     ┆ HEIMSTADEN AB        ┆ Fredensborg AS       ┆ Fredensborg 1994 AS  ┆ Heimstaden AB │
└───────────────┴─────────────────────┴──────────────────────┴──────────────────────┴──────────────────────┴───────────────┘
```

Notes:

- For the Heimstaden AB bond the ultimate parent is its owner, Fredensborg. The CAST
  parent stays at Heimstaden AB, the top of the capital structure.
- For the Heimstaden Bostad bond the CAST parent is Heimstaden AB, which
  consolidates Bostad. Both issuers belong to one capital structure.
- `for(parent('BJ158441 Corp', type=CAST))` makes the parent the universe, so you
  can ask for its fundamentals directly.

## One capital structure, several issuers

All Heimstaden bonds grouped by CAST parent, issuer ticker and rank, in EUR
millions.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#cast = value(name, parent(type=CAST), mapby=LINEAGE);
    #rank = normalized_payment_rank().value;
    #amt = group(amt_outstanding(currency=EUR).value / 1M, [#cast, ticker, #rank]);)
get(sum(#amt) as #eur_m, count(#amt) as #bonds)
for(filter(bondsUniv(ACTIVE, consolidateduplicates=True),
           ticker in ['HEIBOS', 'HEIMST', 'HEMVLR']))
preferences(dropCols=['ORIG_IDS'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine()
print(
    df.select("#CAST", "TICKER", "#RANK", "#bonds", eur_m=pl.col("#eur_m").round().cast(pl.Int64))
    .sort("TICKER", "#RANK")
)
```
Output:
```python
shape: (5, 5)
┌───────────────┬────────┬───────────────────────────┬────────┬───────┐
│ #CAST         ┆ TICKER ┆ #RANK                     ┆ #bonds ┆ eur_m │
│ ---           ┆ ---    ┆ ---                       ┆ ---    ┆ ---   │
│ str           ┆ str    ┆ str                       ┆ i64    ┆ i64   │
╞═══════════════╪════════╪═══════════════════════════╪════════╪═══════╡
│ Heimstaden AB ┆ HEIBOS ┆ Jr Subordinated Unsecured ┆ 5      ┆ 3000  │
│ Heimstaden AB ┆ HEIBOS ┆ Sr Unsecured              ┆ 20     ┆ 5036  │
│ Heimstaden AB ┆ HEIMST ┆ Jr Subordinated Unsecured ┆ 2      ┆ 730   │
│ Heimstaden AB ┆ HEIMST ┆ Sr Unsecured              ┆ 3      ┆ 895   │
│ Heimstaden AB ┆ HEMVLR ┆ Sr Unsecured              ┆ 3      ┆ 74    │
└───────────────┴────────┴───────────────────────────┴────────┴───────┘
```

Notes:

- `Jr Subordinated` here are hybrid bonds, which rank below all senior debt of the
  same issuer. Heimstaden AB's debt is also structurally below Bostad's, because
  Heimstaden AB is a holding company.
- `bonds('<company>')` returns the bonds of that company only, not of its
  subsidiaries, so start from the tickers and let `#cast` show what belongs
  together.
- `dropCols=['ORIG_IDS']` matters: a group with one bond carries `ORIG_IDS` in
  `count()` but not in `sum()`, and the two items would not join.

## Largest capital structures in a high-yield index

The ten CAST parents with the most debt in the Bloomberg Pan-European High Yield
index, with their share of the index.

```python
import polars as pl
from polars_bloomberg import BQuery

query = """
let(#cast = value(name, parent(type=CAST), mapby=LINEAGE);
    #amt = group(amt_outstanding(currency=EUR).value / 1B, #cast);)
get(sum(#amt) as #eur_bn, count(#amt) as #bonds)
for(members('LP01TREU Index'))
preferences(dropCols=['ORIG_IDS'])
"""
with BQuery() as bq:
    df = bq.bql(query).combine(on="ID")
print(
    df.select(
        "#CAST", "#bonds",
        eur_bn=pl.col("#eur_bn").round(1),
        share_pct=(100 * pl.col("#eur_bn") / pl.col("#eur_bn").sum()).round(1),
    )
    .filter(pl.col("#CAST").is_not_null())
    .sort("eur_bn", descending=True)
    .head(10)
)
```
Output:
```python
shape: (10, 4)
┌────────────────────────────────┬────────┬────────┬───────────┐
│ #CAST                          ┆ #bonds ┆ eur_bn ┆ share_pct │
│ ---                            ┆ ---    ┆ ---    ┆ ---       │
│ str                            ┆ i64    ┆ f64    ┆ f64       │
╞════════════════════════════════╪════════╪════════╪═══════════╡
│ SoftBank Group Corp            ┆ 15     ┆ 8.7    ┆ 2.1       │
│ Kemble Water Holdings Ltd      ┆ 17     ┆ 8.5    ┆ 2.0       │
│ Vmed O2 UK Ltd                 ┆ 11     ┆ 7.9    ┆ 1.9       │
│ INA-Holding Schaeffler GmbH &  ┆ 11     ┆ 6.8    ┆ 1.6       │
│ Telefonica SA                  ┆ 7      ┆ 6.6    ┆ 1.6       │
│ Maya SAS/Paris France          ┆ 10     ┆ 6.2    ┆ 1.5       │
│ Bayer AG                       ┆ 7      ┆ 6.0    ┆ 1.5       │
│ Optics Holdco Srl              ┆ 8      ┆ 5.8    ┆ 1.4       │
│ Zeppelin-Stiftung Ferdinand Gm ┆ 8      ┆ 5.6    ┆ 1.4       │
│ Next Alt Sarl                  ┆ 9      ┆ 5.5    ┆ 1.3       │
└────────────────────────────────┴────────┴────────┴───────────┘
```

Notes:

- Grouping by CAST parent adds up holding companies, operating companies and
  finance vehicles of one group, which a ticker would split.
- Bonds without a CAST parent fall into one `NullGroup` row with a null `#CAST`.
  The filter removes it after the share is computed.
- `combine(on="ID")` joins the two items once; the second `#CAST` column is kept as
  `#CAST_#bonds`.

## Pitfalls

What surprised us while writing these recipes.

| Symptom | Cause | Fix |
|---|---|---|
| A bond appears on two rows after `combine()` | `mapby=LINEAGE` adds `SOURCE_ID`, which differs between items | `combine(on="ID")`, or `preferences(dropCols=['SOURCE_ID'])` |
| Thousands of rows from a parent universe | A security without a parent returns the ID `NULL_ID`, a string that matches itself in every join | Drop `NULL_ID` rows before combining, or keep one item |
| Fundamentals in GBP or SEK although you asked for EUR | `with(ccy=EUR)` leaves them in the reporting currency, without an error | `with(currency=EUR)` or `field(currency=EUR)`; check `CURRENCY` |
| `bonds('<parent>')` misses the subsidiaries' bonds | `bonds()` does not follow the CAST group: Heimstaden AB's list has no Heimstaden Bostad bonds | Start from tickers or an index and group by `parent(type=CAST)` |
| Screening all bonds of a country by CAST parent times out | The parent is looked up for every bond in the universe | Narrow the universe first (tickers, an index), then group |
| Grouped items do not line up | One-bond groups carry `ORIG_IDS` in some items only | `preferences(dropCols=['ORIG_IDS'])` |
