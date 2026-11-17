---
title: Recipes
description: Short, copy-paste polars-bloomberg queries grouped by workflow, each with the query, the Python code and the output it gives.
tags:
    - BQL
    - BDH
---

# Recipes

Short reference queries grouped by workflow. Each recipe has the query, the Python
code that runs it and the output it printed.
{ .recipe-lead }

For how `bql()` and `combine()` work, see [BQL](../usage/bql.md); for `bdh()`, see
[BDH](../usage/bdh.md). For worked analyses with charts, see the Examples.

## Workflows

<div class="grid cards recipe-cards" markdown>

-   :material-new-box:{ .lg .middle } **[New Issues](new-issues.md)**

    ---

    New AT1, RT1 and Technology deals in the primary pipeline, and the pipeline by
    currency.

    `BQL` 3 recipes
    { .recipe-card-meta }

-   :material-hand-coin-outline:{ .lg .middle } **[Dealer Axes](axes.md)**

    ---

    What dealers want to buy or sell: per bond, per dealer and per issuer, today and
    over 1 and 5 weeks, next to spreads.

    `BQL` 7 recipes
    { .recipe-card-meta }

-   :material-layers-triple:{ .lg .middle } **[Bonds and Capital Structure](capital-structure.md)**

    ---

    Bank capital stacks, call schedules, distance to the AT1 trigger, debt by rank
    with leverage, and the CAST parent.

    `BQL` 7 recipes
    { .recipe-card-meta }

-   :material-chart-bell-curve-cumulative:{ .lg .middle } **[Spread Statistics](spreads.md)**

    ---

    OAS per rating and the AT1 premium, monthly and daily spread history, index
    yield vs OAS, and YAS at any price.

    `BQL` `YAS` 7 recipes
    { .recipe-card-meta }

-   :material-file-chart-outline:{ .lg .middle } **[Company Fundamentals](fundamentals.md)**

    ---

    From a bond to the company that reports: revenue, EBITDA, segments, bank CET1
    and ROE, leverage per sector.

    `BQL` `BSRCH` 8 recipes
    { .recipe-card-meta }

-   :material-chart-timeline-variant:{ .lg .middle } **[Market History with BDH](history.md)**

    ---

    Weekly spreads, holiday calendars, fund flows, CDS basis, hedged yields,
    fiscal-year leverage and candles.

    `BDH` 7 recipes
    { .recipe-card-meta }

</div>

## How to read a recipe

<div class="recipe-anatomy" markdown>

1. **Question**: the heading and the line under it say what the recipe answers.
2. **Uses**: the functions and options it shows.
3. **Code**: copy it with the button and run it at a Bloomberg Terminal.
4. **Output**: what it printed on the date at the top of the page.
5. **Watch out**: traps that give wrong numbers, often without an error.
6. **Notes**: how to read the output and adapt the query.

</div>

## All recipes

<div class="recipe-index" markdown>

### New Issues

| Recipe | Uses |
|---|---|
| [New AT1 / RT1 deals](new-issues.md#new-at1-rt1-deals-in-usd-eur-gbp) | `INCLUDEPRELIMINARYSECURITIES`, `issuance_pricing`, `as #alias` |
| [New Technology deals](new-issues.md#new-technology-deals-in-usd-eur-gbp) | `classification_name(BCLASS)` |
| [Pipeline by currency](new-issues.md#pipeline-by-currency) | `count(group(...))` |

### Dealer Axes

| Recipe | Uses |
|---|---|
| [AT1 axes by bond](axes.md#at1-axes-by-bond) | `axes()` |
| [Large axes only](axes.md#large-axes-only) | `axes_by_dealer()`, `matches()`, `dropna()` |
| [Net axes per issuer](axes.md#net-axes-per-issuer) | `sum(group(..., ticker))` |
| [Total axes: today, 1 week, 5 weeks](axes.md#total-axes-today-1-week-5-weeks) | `calc_interval`, `*_HIST_AGG_SIZE` |
| [Per bond: today vs 1 and 5 weeks](axes.md#per-bond-today-vs-1-and-5-weeks) | `calc_interval`, `*_HIST_AGG_SIZE` |
| [Axes in a relative-value screen](axes.md#axes-in-a-relative-value-screen) | `flt_spread()`, `spread(spread_type=OAS)` |
| [Top 3 dealers](axes.md#top-3-dealers) | `axes_by_dealer()` |

### Bonds and Capital Structure

| Recipe | Uses |
|---|---|
| [Swedish bank capital stacks](capital-structure.md#swedish-bank-capital-stacks) | `normalized_payment_rank()`, `sum(group(..., [ticker, #rank]))` |
| [AT1 and Tier 2 call schedule](capital-structure.md#at1-and-tier-2-call-schedule) | `basel_iii_designation()`, `nxt_call_dt` |
| [AT1 distance to trigger](capital-structure.md#at1-distance-to-trigger) | `value(..., parent(type=CAST), mapby=LINEAGE)`, `capital_type_coco_trigger_level()` |
| [Bonds and loans with leverage](capital-structure.md#bonds-and-loans-with-leverage) | `loansUniv(ACTIVE)`, `ebitda(fpt=LTM)` |
| [Direct, ultimate and CAST parent](capital-structure.md#direct-ultimate-and-cast-parent) | `parent(type=DIRECT)`, `ULTIMATE`, `CAST` |
| [One capital structure, several issuers](capital-structure.md#one-capital-structure-several-issuers) | `group(..., [#cast, ticker, #rank])` |
| [Largest capital structures in a high-yield index](capital-structure.md#largest-capital-structures-in-a-high-yield-index) | `sum(group(..., #cast))` over `members()` |

### Spread Statistics

| Recipe | Uses |
|---|---|
| [OAS by rating: AT1 vs other bonds](spreads.md#oas-by-rating-at1-vs-other-bonds) | `median()`, `quantile()`, `group(..., by=[bb_composite(), ...])` |
| [Median OAS by rating, month by month](spreads.md#median-oas-by-rating-month-by-month) | `per=M`, `month()`, `year()` |
| [Daily average Z-spread of index members](spreads.md#daily-average-z-spread-of-index-members) | `avg(group(#s, by=#s().DATE))` |
| [Index yield and spread, week by week](spreads.md#index-yield-and-spread-week-by-week) | `universe_type`, `per=W` |
| [Tier 2: spread to call or to maturity](spreads.md#tier-2-spread-to-call-or-to-maturity) | YAS: `spread(price=..., workout=...)` |
| [One bond at four prices](spreads.md#one-bond-at-four-prices) | YAS: `workout=TO_CUSTOM`, `workout_date` |
| [SEK AT1 floaters: discount margin](spreads.md#sek-at1-floaters-discount-margin) | YAS: `discount_margin(price=...)` |

### Company Fundamentals

| Recipe | Uses |
|---|---|
| [From a bond to the company that reports](fundamentals.md#from-a-bond-to-the-company-that-reports) | `value(..., fundamentalticker, mapby=LINEAGE)` |
| [Annual revenue and EBITDA, last two years](fundamentals.md#annual-revenue-and-ebitda-last-two-years) | `fundamentalticker(...)`, `fa_period_type=A`, `fa_period_offset=range(-1Y, 0Y)` |
| [Reported vs adjusted EBITDA](fundamentals.md#reported-vs-adjusted-ebitda) | `fa_adjusted=N` / `Y`, `fa_act_est_data=A` |
| [Revenue by product and by country](fundamentals.md#revenue-by-product-and-by-country) | `segments(..., hierarchy=PRODUCT)`, `hierarchy=LOCATION` |
| [Swedish AT1 issuers: CET1 and ROE by quarter](fundamentals.md#swedish-at1-issuers-cet1-and-roe-by-quarter) | `bs_tier1_com_equity_ratio()`, `normalized_roe()`, `fpt=Q` |
| [How much of a high-yield index has fundamentals?](fundamentals.md#how-much-of-a-high-yield-index-has-fundamentals) | `issuerOf` vs `parent(type=CAST)` vs `fundamentalticker` |
| [Median leverage per sector](fundamentals.md#median-leverage-per-sector) | `median(group(..., classification_name(BCLASS, 3)))` |
| [Bloomberg Intelligence sector template](fundamentals.md#bloomberg-intelligence-sector-template) | BSRCH: `bsrch("BI:TPD", {"BIKEY": ...})` |

### Market History with BDH

| Recipe | Uses |
|---|---|
| [High-yield spreads, week by week](history.md#high-yield-spreads-week-by-week) | `periodicitySelection=WEEKLY`, `INDEX_OAS_TSY_BP` |
| [Three markets, one calendar](history.md#three-markets-one-calendar) | `nonTradingDayFillOption`, `nonTradingDayFillMethod` |
| [Fund NAV, assets and flows](history.md#fund-nav-assets-and-flows) | `FUND_TOTAL_ASSETS`, `FUND_FLOW`, `MONTHLY` |
| [CDS index vs intrinsic: the basis](history.md#cds-index-vs-intrinsic-the-basis) | `CDS_INDEX_INTRINSIC_BASIS_VALUE` |
| [US Treasuries hedged into euros vs Bunds](history.md#us-treasuries-hedged-into-euros-vs-bunds) | `FXHCEUUS Index`, `pivot(on="security")` |
| [Fiscal-year leverage](history.md#fiscal-year-leverage) | `YEARLY`, `periodicityAdjustment=FISCAL`, `currency` |
| [Weekly candles](history.md#weekly-candles) | `PX_OPEN`, `PX_HIGH`, `PX_LOW`, `WEEKLY` |

</div>

!!! tip "Keeping outputs fresh"
    The outputs are pasted in by `python dev/refresh_recipes.py`, run at a
    Bloomberg Terminal. See the developer guide.
