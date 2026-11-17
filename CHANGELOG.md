# Changelog

All notable changes to polars-bloomberg are listed here.

## Unreleased

### Added

- A Recipes section in the docs: short reference queries with their output,
  starting with New Issues (new AT1/RT1 deals, new Technology deals, the
  pipeline by currency). `dev/refresh_recipes.py` re-runs the recipes at a
  Terminal and pastes their fresh output into the page.
- Recipes: Dealer Axes, on the USD AT1s of Swedish banks - axes per bond, large
  dealer axes, net axes per issuer, totals and per-bond axes against the last 1
  and 5 weeks, axes in a relative-value screen, and the top 3 dealers.
- Recipes: Company Fundamentals - from a bond to the company that reports with
  `fundamentalTicker()`, annual and adjusted EBITDA of European high-yield
  issuers, revenue by product and country, quarterly CET1 and ROE of Swedish AT1
  issuers, fundamentals coverage and median leverage per sector of a high-yield
  index, and a Bloomberg Intelligence template via BSRCH.
- Recipes: Market History with BDH - weekly OAS of high-yield and CoCo indices,
  markets with different holidays on one calendar (`nonTradingDayFillOption`),
  fund NAV, assets and net flows, the iTraxx basis to intrinsic, US Treasuries
  hedged into EUR vs Bunds, fiscal-year leverage (`periodicityAdjustment=FISCAL`)
  and weekly OHLC candles.

## 0.7.2 - 2026-11-16

### Changed

- `plbbg.py` is reorganized for readability, with no change in behavior: all
  five methods follow the same five visible steps, the parsers are plain
  functions grouped in one section per method, and the comments on Bloomberg's
  quirks name the recording that shows each one.

### Added

- A developer guide, `docs/developer-guide.md`: the pipeline every call follows,
  a map of `plbbg.py`, the Bloomberg quirks the code handles, and recipes for
  fixing a bug, adding a feature and turning a `debug=True` dump into an offline
  test.

## 0.7.1 - 2026-11-16

### Added

- `bdib` accepts its start and end also as ISO 8601 strings
  (`"2024-01-02T08:00:00"`), which it already did without saying so.

### Changed

- A BQL date or datetime value that is not ISO text raises `BloombergError`
  naming the item and column, instead of a bare `ValueError`.

### Fixed

- `start()` on a `BQuery` that is already started now does nothing. It used to
  open a second session and leave the first one running, after which every call
  failed with `//blp/refdata not open`.
- A BQL screen that matches nothing (Bloomberg's "No results match ... Try
  expanding criteria.") is logged as a warning, not an error. The message is
  still kept in `result.errors`.

## 0.7.0 - 2026-11-16

### Added

- `BloombergError`, raised when Bloomberg rejects or fails a request. It derives
  from `ValueError`, so existing `except ValueError` and `except Exception` blocks
  still catch it.
- `BQuery.start()` and `BQuery.stop()`, for use without a `with` block:
  `bq = BQuery().start()` ... `bq.stop()`. `stop()` is safe to call twice.

- `debug=` also accepts a directory: `BQuery(debug="dumps")`. `debug=True` still
  writes to `debug_cases/` in the working directory.

- `bdp` and `bdh` accept a single security or field as a string:
  `bq.bdp("IBM US Equity", "NAME")`.
- Overrides can be a dict or a sequence of (name, value) pairs in `bdp`, `bdh`,
  `bdib` and `bsrch` alike.
- `bdh` accepts its dates also as `YYYYMMDD` or `YYYY-MM-DD` strings, and `bdib` its
  start and end also as a `date` (midnight).
- `BqlResult.errors`: the messages BQL returned with the answer, for example a
  syntax error. `combine()` on a failed query now raises
  `No DataFrames to combine. BQL error: <message>`.

### Changed

- New debug dump format: one JSON file per request, named after the request type,
  with the request, every message Bloomberg sent back, and the library and Polars
  versions. Dates keep their type, so a dump can be replayed. The dump is also
  written when the request fails, and a dump that cannot be written no longer fails
  the request. The separate BQL-only dump (`bql_parse_results_*.json`) is gone.

- Services (reference data, BQL, search) open when a method first needs them, not
  all at once when the session starts. A user without access to one service can now
  use the others.
- Calling a method on a `BQuery` that was never started raises `ConnectionError`
  with a hint, instead of `AttributeError`.

- Error messages for a rejected request now include Bloomberg's error category, for
  example `Response error: No securities specified (BAD_ARGS)`.
- A failed request is reported at once as `BloombergError`, and a lost session as
  `ConnectionError`. Both used to surface as a `TimeoutError` after the full wait.
- The `timeout` argument is documented as what it is: the longest wait for the next
  event from Bloomberg, not a limit on the whole request.

- Empty `bdp` and `bdh` results have their columns (`security`, for `bdh` also
  `date`, then one per field) instead of no columns at all. Field columns of an
  empty result have dtype `Null`.
- `bdp` and `bdh` log one warning per call that lists the invalid securities and
  fields Bloomberg reported, with its reason, for example
  `NOT A SECURITY Equity: Unknown/Invalid Security (BAD_SEC)`. The DataFrame is
  unchanged: an invalid security is still a null row in `bdp` and has no rows in
  `bdh`; an invalid field is still a column of nulls.

- A `date` (or `datetime`) override value is sent as `YYYYMMDD`. Bloomberg silently
  ignores an override date in another format.
- An override value of `None` raises `TypeError`; it used to be sent as the text
  `"None"`. Overrides that are neither a dict nor (name, value) pairs raise
  `TypeError` too.
- `bdp` raises `ValueError` when `security` is requested as a field, and `bdh` when
  `security` or `date` is; such a field used to overwrite the key column.

- BQL: a value type the library does not know is no longer read as text; Polars
  infers its dtype and a warning names the type.
- BQL: the value types `ENUM` (for example `CPN_TYP` of `cpn()` or the rating of
  `bb_composite()`) and `DATETIME` (the quote times of `axes()`) are known: `ENUM`
  becomes `String`, also when a column has no values, and `DATETIME` a naive
  `Datetime` holding UTC, like the `time` of `bdib`. Neither logs a warning.
- BQL: when two columns of one item have the same name (an item named `ID`, or named
  like one of its secondary columns), the second gets a suffix (`ID_1`) and a
  warning is logged. It used to replace the first column silently.
- BQL: an item whose DataFrame cannot be built raises `BloombergError` naming the
  item.

- `bsrch`: a search without results keeps its columns instead of having none.
- `bsrch`: a failed search raises `BloombergError` (still a `ValueError`). An error
  that Bloomberg sends together with rows is logged as a warning; it used to be
  dropped.
- `bsrch`: a repeated column title gets a suffix (`Name`, `Name_1`) and a warning is
  logged; the second column used to overwrite the first.
- `bsrch`: the parser reads the answer as Bloomberg sends it (the grid at the top
  level, values as `StringValue` or `DoubleValue`) and no longer guesses other
  spellings. A value under any other key is kept as it is, and a warning names the
  key.

### Fixed

- After a `TimeoutError` or an interrupted call (for example "interrupt kernel" in a
  notebook), the next call could return the previous request's data, and every later
  call stayed one answer behind. Each call now accepts only the answer to its own
  request, and an abandoned request is cancelled.
- Session and service status messages are no longer collected as part of an answer.
- `bdp` no longer fails with a Polars `ComputeError` when a field's first value comes
  after the first 100 securities.
- A bare string for `securities` or `fields` is no longer sent one character at a
  time.
- Overrides given as one pair without the enclosing list, such as `("AB", "CD")`, are
  no longer sent as the overrides `A=B` and `C=D`.
- `bsrch` with a `LIMIT` given as a (name, value) pair no longer crashes.
- `bdib` with a `date` instead of a `datetime` no longer crashes.
- BQL: results that arrive together with an error message are kept; they used to
  be discarded.
- BQL: a large answer, which Bloomberg sends as several fragments of one JSON text,
  is joined before parsing. It used to give an empty result and log every
  multi-megabyte fragment as an error.
- BQL: `DATE` values with fractional seconds or without a time are parsed.
- BQL: an item with a column that has no type, or with null parts, no longer
  crashes the query.
- `bsrch`: blank cells become null in a numeric column also when the column is
  missing from the first row.
- `polars_bloomberg.__version__` said 0.5.4 while the package was 0.6.0. It is now
  read from the installed package's metadata, so the two cannot disagree again.

### Documentation

- README: a new section "Errors, Warnings and Empty Results" lists which exception
  each failure raises (`BloombergError`, `ConnectionError`, `TimeoutError`), the
  warning for invalid securities and fields, what an empty result looks like,
  `BqlResult.errors`, the `timeout` and `debug` options, `start()`/`stop()`, and
  that one `BQuery` must not be used from several threads at once. Each page under
  `docs/usage/` says how its method reports errors and empty results.
- `combine()`: without `on` it joins on every shared column, so a row whose shared
  columns hold nulls (for example a security without a `DATE`) appears twice.
  `combine(on="ID")` gives one row per security; the README examples that mean one
  row per security use it.
- `bdib`: `time` is a naive datetime holding UTC; the docstring said the time zone
  was kept. An interval outside 1-1440 raises `BloombergError`, not a schema error.
- The `Raises` sections name `BloombergError`, `ConnectionError` and `TimeoutError`.
- Corrected docstring examples: wrong `shape:` lines, an example that asked for IBM
  and showed AAPL, a "no common columns" example that cannot raise, and a `bsrch`
  example that showed the limit warning the `LIMIT` override suppresses. The
  README's `bdip` is `bdib`. Changed example outputs were re-run on a Bloomberg
  Terminal.
