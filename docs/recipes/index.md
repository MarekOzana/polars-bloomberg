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

!!! tip "Keeping outputs fresh"
    The outputs are pasted in by `python dev/refresh_recipes.py`, run at a
    Bloomberg Terminal. See the developer guide.
