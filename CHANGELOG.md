# Changelog

All notable changes to polars-bloomberg are listed here.

## Unreleased

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
- BQL: when two columns of one item have the same name (an item named `ID`, or named
  like one of its secondary columns), the second gets a suffix (`ID_1`) and a
  warning is logged. It used to replace the first column silently.
- BQL: an item whose DataFrame cannot be built raises `BloombergError` naming the
  item.

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
