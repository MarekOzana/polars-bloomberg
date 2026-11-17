---
title: Recipes
description: Short, copy-paste polars-bloomberg queries grouped by workflow, each with the query, the Python code and the output it gives.
tags:
    - BQL
---

# Recipes

Short reference queries grouped by workflow. Each recipe has the query, the Python
code that runs it and the output it printed. For how `bql()` and `combine()` work,
see [BQL](../usage/bql.md). For worked analyses with charts, see the Examples.

| Page | Recipe | BQL used |
|---|---|---|
| [New Issues](new-issues.md) | [New AT1 / RT1 deals](new-issues.md#new-at1-rt1-deals-in-usd-eur-gbp) | `INCLUDEPRELIMINARYSECURITIES`, `issuance_pricing`, `as #alias` |
| | [New Technology deals](new-issues.md#new-technology-deals-in-usd-eur-gbp) | `classification_name(BCLASS)` |
| | [Pipeline by currency](new-issues.md#pipeline-by-currency) | `count(group(...))` |
| [Dealer Axes](axes.md) | [AT1 axes by bond](axes.md#at1-axes-by-bond) | `axes()` |
| | [Large axes only](axes.md#large-axes-only) | `axes_by_dealer()`, `matches()`, `dropna()` |
| | [Net axes per issuer](axes.md#net-axes-per-issuer) | `sum(group(..., ticker))` |
| | [Total axes: today, 1 week, 5 weeks](axes.md#total-axes-today-1-week-5-weeks) | `calc_interval`, `*_HIST_AGG_SIZE` |
| | [Per bond: today vs 1 and 5 weeks](axes.md#per-bond-today-vs-1-and-5-weeks) | `calc_interval`, `*_HIST_AGG_SIZE` |
| | [Axes in a relative-value screen](axes.md#axes-in-a-relative-value-screen) | `flt_spread()`, `spread(spread_type=OAS)` |
| | [Top 3 dealers](axes.md#top-3-dealers) | `axes_by_dealer()` |

!!! tip "Keeping outputs fresh"
    The outputs are pasted in by `python dev/refresh_recipes.py`, run at a
    Bloomberg Terminal. See the developer guide.
