# Review findings: IMPROVEMENT_PLAN implementation (7a07c8a..HEAD)

## Summary

**Verdict: navigable.** `plbbg.py` reads in the order a call runs; every public method is
one contiguous block (method, request builder, parser); the five navigation exercises
each ended at the right lines within two searches. The event loop's correlation-ID
filter, the BQL error path and the bsrch type handling are where a search lands, and the
comments say why. The implementation does what the plan asked: all 23 plan-named tests
exist, every "Do" item I checked is in the code, `CHANGELOG.md` covers each user-visible
change, and the step 9 reorder moved definitions without altering them. Decisions
D1-D11 are not hurting; none should be reversed.

**Recommended changes, in order:** R02 `start()` idempotent (one-line + test);
R03 log `BENIGN` BQL exceptions below ERROR (small); R01 rename
`_coerce_bsrch_numeric_columns` and say in `bsrch` where types come from (one-line);
R04 fold or label the five pre-plan test files (medium).

**Not verified:** anything that needs a Terminal (live tests, contract tests, the step 11
re-run of example outputs); the minimum-dependency run (polars 1.7.1, blpapi 3.24.2);
`mkdocs build` (mkdocs is not installed in this environment).

## Status (2026-11-16)

R01-R04 implemented, uncommitted. `start()` returns early when started;
`_extract_results` logs an all-`BENIGN` answer as a warning;
`_coerce_bsrch_numeric_columns` is `_blank_strings_to_none`; `test_bdp_bdh.py`,
`test_schema.py`, `test_bql_parsing.py`, `test_utils.py` and `TestBsrch` are gone,
their few uncovered cases rewritten through the public methods. CHANGELOG has an
"Unreleased" section for the two fixes.

Second pass, same day, on the minor items: `_apply_schema` raises `BloombergError`
naming item and column for a date that is not ISO text; `bdib` says it accepts ISO
strings; `bsrch` reads its overrides once; `combine()` joins on the left frame's
column order; the three refdata parsers and the shared `_create_request` have
docstrings; `_extract_results` collects the exceptions first and judges them once;
`[tool.mypy]` and the stale rule-group comment are gone from `pyproject.toml`;
every `tests/unit` file has a docstring and `tests/README.md` maps them. Not
changed, as this list says: infinite `INT`, per-message `_unique_titles` warning,
case-sensitive key-column check, `_format_date` placement.

## Progress
- [x] Phase 1: cold read and navigation
- [x] Phase 2: bug hunt
- [x] Phase 3: plan conformance
- [x] Phase 4: tests and tooling
- [x] Phase 5: summary

## Top findings so far
(ranked; rewrite this list at the end of every phase)
1. R02 `start()` twice leaks a session and breaks the next call (Likely bug, one-line + test)
2. R03 `BENIGN` "No results match" logged as ERROR (Likely bug, small)
3. R04 old private-method test files overlap the end-to-end ones (Clarity, medium)
4. R01 `_coerce_bsrch_numeric_columns` name misleads (Clarity, one-line)

## Findings

### Phase 1 notes: cold read and navigation

Module order (top to bottom): `BloombergError`, `SITable`, `BqlResult`; module helpers in
five labelled groups (debug dump, refdata item errors, inputs, bsrch titles, BQL values);
then `BQuery`: `__init__`/`start`/`stop`/`_get_service`/`_send_request`, and one contiguous
block per public method (`bdp`, `bdh`, `bdib`, `bql`, `bsrch`), each as
method -> `_create_*` -> `_parse_*`. This is the order a call runs (step 9.3). No method's
block is interleaved with another's. `pytest` 284 passed / 76 skipped, 100 % coverage of
`plbbg.py`; `ruff check` and `ruff format --check` clean.

- **bdh all-null column.** `bdh()` -> `_parse_bdh_responses` (directly below it):
  `entry.get(field)` for each row of `fieldData`. Two searches, under a minute. Recording
  `bdh/lowercase_field` shows Bloomberg echoes the requested casing, so a null column means
  Bloomberg sent nothing, and the reason is in the warning `_item_errors` builds. Nothing
  misled me.
- **"Hangs then returns someone else's data".** grep `correlation` -> one hit in
  `_send_request`: `if cid not in msg.correlationIds(): continue  # status message, or late
  answer to an old request`. The comment says why. `_cancel_quietly` on the except path
  explains why late answers are rare. Obvious.
- **Empty `BqlResult`, no error.** `bql()` -> `_parse_bql_responses` -> `_extract_results`
  (JSON decode failure and `responseExceptions` go to `errors` and one `logger.error`) and
  `_parse_result` (an item whose content is `None` is skipped silently; a `results` that is
  empty or missing is skipped silently). All within 120 lines. Good.
- **bsrch string column holding numbers.** `bsrch()` -> `_parse_bsrch_responses` ->
  `_extract_bsrch_field_value` (StringValue vs DoubleValue is the only type signal) ->
  `pl.DataFrame(..., strict=False)` infers. Slowed down by `_coerce_bsrch_numeric_columns`:
  its name promises coercion but it only replaces blank strings with None (see R01).
- **Adding `bds`.** The pattern is visible from `bdp`/`bdh`: `_as_list`,
  `_check_key_columns`, `_create_request("...Request", ...)`, `_send_request`, a
  `_parse_*_responses`, then `pl.DataFrame(data, schema=..., infer_schema_length=None)`.
  Obvious. `_create_request` lives under the `# --- bdp ---` header but serves `bdh` too;
  a one-screen scroll, not a problem.
- **Helper placement.** `_convert_number`/`_convert_datetime` are ~800 lines above
  `_apply_schema`, `_unique_titles` ~900 lines above `_parse_bsrch_responses`. The group
  headers (`# --- BQL values ---`, `# --- bsrch column titles ---`) name their users'
  method, so a search finds them. Acceptable.

### R01 · Clarity · `plbbg.py:1509` · `_coerce_bsrch_numeric_columns` does not coerce anything
**What:** The name says the function makes columns numeric. It only replaces empty or
whitespace strings with `None`, and only in columns whose other values are numbers or
None; a column of `StringValue` digits is left as strings.
**Why it matters:** A maintainer with "bsrch returns a str column for a numeric field"
opens this function first (it is the only hit for "numeric" in the bsrch block) and may
conclude the library tries to parse numeric strings and fails, when in fact the only
type signal is `StringValue` vs `DoubleValue` in `_extract_bsrch_field_value`.
**Evidence:** read in code; the docstring and body agree with each other, not with the name.
**Suggested change:** rename to `_blank_strings_to_none` (or similar) and add one line to
the `bsrch` docstring: column types follow Bloomberg's `StringValue`/`DoubleValue`; a
number sent as text stays a string.

### R02 · Likely bug · `plbbg.py:571` · `start()` on a started `BQuery` leaks the session and breaks the next call
**What:** `start()` always creates a new `blpapi.Session` and overwrites `self.session`
without stopping the old one, and it does not clear `_open_services`. After a second
`start()` the cache says `//blp/refdata` is open, but it is open on the discarded
session; `_get_service` skips `openService` and `getService` raises.
**Why it matters:** `bq = BQuery().start()` in a notebook cell that is run twice, or
`with bq:` on an instance already started, leaves one leaked session and every later call
fails with `blpapi.InvalidStateException: //blp/refdata not open`, an error that names
neither `start` nor the cause. The `stop()` docstring says "safe to call more than once";
nothing says `start()` is not.
**Evidence:** reproduced with a `FakeSession` whose `getService` raises like blpapi when
the service was not opened on that session (`blpapi.Session.getService` docs: raises
`InvalidStateException` if not open): first `bdp` OK, `start()` again, second `bdp` raises.
**Suggested change:** first line of `start()`: `self.stop()` (which also clears
`_open_services`), or `if self.session is not None: return self`. One line plus a test
`test_start_twice_keeps_one_session`.

### R03 · Likely bug · `plbbg.py:1279` · a `BENIGN` BQL exception ("No results match ...") is logged as an error
**What:** `_extract_results` logs every `responseExceptions` entry with `logger.error`
and puts it in `BqlResult.errors`, without looking at the entry's `type`. Recording
`bql/empty_universe` shows that an empty screen comes back as an exception of type
`BENIGN` with the message "No results match filter(...). Try expanding criteria."
**Why it matters:** A screen that legitimately matches nothing prints
`ERROR ... BQL error: No results match ...` to stderr, so a user (or a log monitor)
reads a normal outcome as a failure. The plan's live check L4 recorded this ("today
logged as an error") but no step acted on it. A returning maintainer chasing "why does my
empty screen log an error" ends up in `_extract_results` with no hint that the type is
available.
**Evidence:** read in code and in the recording; the entry carries
`"type": "BENIGN"`, syntax errors carry `"type": "PARTIAL"`.
**Suggested change:** in `_extract_results`, log at `warning` (or `info`) when every
exception has `type == "BENIGN"`, keep the message in `errors` so `combine()` still
explains the empty result. About five lines; a test on the `empty_universe` recording
with `caplog`.

### R04 · Clarity · `tests/unit/` · five pre-plan test files overlap the end-to-end files and do not say what they cover
**What:** Next to the new end-to-end files (`test_bdp.py`, `test_bdh.py`, `test_bql.py`,
`test_send_request.py`, `test_requests.py`, each with a docstring) the old files remain,
without a module docstring, and test the same behaviour through private methods:
- `test_bdp_bdh.py`: two tests that call `_parse_bdp_responses`/`_parse_bdh_responses`
  directly; everything they check is covered in `test_bdp.py`/`test_bdh.py`.
- `test_schema.py`: `_map_types`/`_apply_schema` directly; `test_bql.py` covers the same
  types end to end (NaN, infinity, unknown type, ENUM, DATETIME, fractional seconds).
- `test_bql_parsing.py` (581 lines): 13 `_extract_results` tests duplicating
  `test_bql.py`; `test_parse_bql_responses` and `test__extract_results` feed session and
  service status payloads that `_send_request` now filters out (and carry the machine
  name `bbcomm-5CG3290LGQ-3511155`, lines 173 and 310); `test_real_life_cases` is a
  `combine()` test on the five `tests/data/bql/*.yaml` fixtures and belongs with
  `test_bql_result.py`.
- `TestBsrch` in `test_bsrch.py`: ten tests on `_create_bsrch_request`,
  `_parse_bsrch_responses`, `_coerce_bsrch_numeric_columns`, above the public ones.
- `test_utils.py`: three `_format_datetime` tests plus the version test; `test_requests.py`
  already checks the naive, aware and `date` cases on real requests.
**Why it matters:** The maintainer with "bql drops a column" must choose between
`test_bql.py`, `test_bql_parsing.py`, `test_bql_result.py` and `test_schema.py`, and a
fix that changes a private signature breaks tests that pin the old shape rather than a
behaviour. The plan's rule ("go through the public methods") is met by the new files
only; step 9.1 deleted just the six tests it named.
**Evidence:** read in code; `grep "bq\._" tests/unit` lists the private calls.
**Suggested change:** Move `test_real_life_cases` to `test_bql_result.py`, delete
`test_bdp_bdh.py`, `test_schema.py`, the `_extract_results` tests and the two status-
payload tests of `test_bql_parsing.py`, `TestBsrch`, and the `_format_datetime` tests
(move the version test to `test_bquery_internals.py`); what remains of
`test_bql_parsing.py` goes into `test_bql.py`. Medium. The small alternative: a one-line
docstring on each old file saying it predates the recordings and which new file covers
the same behaviour.

## Considered and not worth changing
- `_apply_schema`: a `DATE`/`DATETIME` value that `fromisoformat` rejects raises a bare `ValueError`, not the `BloombergError` the `bql` docstring promises; L5 and all 31 recordings show ISO text only.
- `_convert_number` puts `float('inf')` into an `INT` column, which then fails as `BloombergError`; no recording shows an infinite INT.
- `_unique_titles` logs its rename once per grid message; L8 shows one message per search.
- `bsrch` runs `_override_items` twice (request, then `limit_applied`); harmless.
- `_format_datetime` accepts a `str` that the `bdib` signature does not mention; passthrough, harmless.
- Step 9 reorder (c1dde92): compared the AST of every function and class body before and after; nothing changed. Debug dump sits in `finally`, so it is written on the failure path too (checked by reading and by `test_debug_dump_is_written_when_the_request_fails`).
- F21 (per-item BQL errors): `partialErrorMap` is `{"errorIterator": null}` in every recording; ruled out, as L4 says.
- Step 7 "Done when" names `test_incorrect_bql_syntax_logs_error`; it became `test_bql_error_gives_an_empty_result_that_carries_the_message` in the live-tour commit. Same check.
- `[tool.mypy]` remains in `pyproject.toml` after step 10 removed mypy from nox; one dead table, no reader cost.
- `pyproject.toml` comment on rule group `R` says "flake8-return (RET)"; ruff's `R` prefix also covers RSE and RUF. Not worth a line.
- `tests/data/bql/*.yaml` (five files) are kept for `test_real_life_cases`; fine once that test sits in `test_bql_result.py` (R04).
- `bdp`/`bdh` key-column check is case-sensitive (`DATE` passes); Bloomberg echoes the requested spelling, so no column is overwritten.
- `combine()` without `on` joins on a `set` of column names; Polars keeps the left frame's column order regardless, so output is stable.
- `_format_datetime` is a `BQuery` staticmethod in the bdib block while `_format_date` is a module helper under inputs; the bdib reader finds it right below `_create_intraday_bar_request`, so no cost.
- `_parse_bdp/bdh/bdib_responses` have no docstrings; each is 15 lines and obvious.
- `_create_request` sits under the `bdp` header but is shared with `bdh`; one screen away.
