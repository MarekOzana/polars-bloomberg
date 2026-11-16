---
title: Developer Guide
description: How polars-bloomberg is built - the one pipeline every call follows, a map of plbbg.py, the Bloomberg quirks it handles, and recipes for fixing bugs and adding features with offline tests.
---

# Developer guide

This page is for someone who opens the repository with a bug report or a feature
idea. It takes about ten minutes to read. After it you should know how a call flows,
where a bug must be, and how to turn a strange Bloomberg answer into an offline test.

## The library in one minute

polars-bloomberg is five methods on `BQuery`: `bdp`, `bdh`, `bdib`, `bql` and
`bsrch`. Each one sends one Bloomberg request and turns the answer into one Polars
DataFrame (`bql` returns a `BqlResult`, which holds one DataFrame per item of the
query). Almost all of the code is in one file, `polars_bloomberg/plbbg.py`.

Every method runs the same five steps. This is `bdp` without its docstring:

```python
def bdp(self, securities, fields, overrides=None, options=None):
    # Step 1: check the inputs.
    securities = _as_list(securities)
    fields = _as_list(fields)
    _check_no_field_is_named(fields, "security")

    # Step 2: build the request.
    request = self._create_request("//blp/refdata", "ReferenceDataRequest")
    _append_values(request, "securities", securities)
    _append_values(request, "fields", fields)
    _set_overrides(request, overrides)
    _set_options(request, options)

    # Steps 3 to 5: send it, parse the answer, build the DataFrame.
    payloads = self._send_request(request)
    rows = _parse_bdp(payloads, fields)
    schema = {"security": pl.String, **dict.fromkeys(fields)}
    return pl.DataFrame(rows, schema=schema, infer_schema_length=None)
```

A *payload* is one message of Bloomberg's answer as plain Python (`msg.toPy()`):
dicts, lists, numbers, strings and dates. Steps 4 and 5 work on such plain data only.
That is why the offline tests need no Bloomberg: they hand recorded payloads to the
same code.

The other four methods read the same way. They differ in these places:

| Method | Service | Request | Parser | What is different |
|---|---|---|---|---|
| `bdp` | `//blp/refdata` | `ReferenceDataRequest` | `_parse_bdp` | the model for the others |
| `bdh` | `//blp/refdata` | `HistoricalDataRequest` | `_parse_bdh` | two dates in the request; one security per payload |
| `bdib` | `//blp/refdata` | `IntradayBarRequest` | `_parse_bdib` | one security; times instead of dates, in UTC; fixed columns |
| `bql` | `//blp/bqlsvc` | `sendQuery` | `_parse_bql` | the answer is JSON text; one DataFrame per item; errors are part of the answer |
| `bsrch` | `//blp/exrsvc` | `ExcelGetGridRequest` | `_parse_bsrch` | overrides are sent as text; the answer is a grid without column types |

## A map of `plbbg.py`

The module docstring is the table of contents. The file has eight numbered sections.
The class comes first and shows the five steps of each method; the sections after it
hold what the steps call.

| Section | What is in it | Go here when |
|---|---|---|
| 1. `BloombergError`, `BQuery` | `start`/`stop`; `_create_request` (opens a service on first use); `_send_request` (the event loop); the five public methods | a call hangs, times out, raises, or returns another call's data; a request is built wrongly |
| 2. Inputs and requests | `_as_list`, `_check_no_field_is_named`, `_yyyymmdd`, `_append_values`, `_set_overrides`, `_override_pairs`, `_set_options` | an input is refused or sent in the wrong form |
| 3. bdp and bdh | `_parse_bdp`, `_parse_bdh`, `_security_errors`, `_warn_security_errors` | a value or row is missing; a warning about a security or field is wrong |
| 4. bdib | `_iso_datetime`, `_BAR_SCHEMA`, `_parse_bdib` | times are off; a bar column is missing |
| 5. bql | `_set_bql_client_context`, `_parse_bql`, `_load_bql_answers`, `_bql_item`, `_BQL_DTYPES`, the readers of text values, `_bql_dataframe` | a BQL column has the wrong type or name; an error message is lost |
| 6. bsrch | `_set_bsrch_overrides`, `_parse_bsrch`, `_cell_value`, `_blank_strings_to_none` | a search column has the wrong type or title |
| 7. `BqlResult` | the frames of a BQL answer and `combine()` | joining BQL items gives unexpected rows or columns |
| 8. Debug dump | `_write_debug_dump`, `_json_default` | you change what `BQuery(debug=True)` writes |

## One call, end to end

Take `bq.bdp(["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"])`. Bloomberg's
answer to it is stored in `tests/data/recordings/bdp/invalid_security_mixed.json`.

1. **Check the inputs.** `_as_list` lets a caller pass one name as a string.
   `_check_no_field_is_named` refuses a field called `security`, because it would
   overwrite the key column of the result.
2. **Build the request.** `_create_request` opens `//blp/refdata` if this `BQuery`
   has not used it yet and returns an empty `ReferenceDataRequest`. `_append_values`
   fills `securities` and `fields`. `_set_overrides` checks the overrides (through
   `_override_pairs`) and writes them; `_set_options` sets each option under
   Bloomberg's own element name.
3. **Send it.** `_send_request` sends the request and reads events until the
   `RESPONSE` event that ends the answer. It keeps only messages that carry the
   correlation ID of this request, raises `BloombergError` if Bloomberg refuses the
   request, and returns the payloads. Here that is one payload:
   `{"securityData": [{"security": "IBM US Equity", "fieldData": {"NAME": ...}},
   {"security": "NOT A SECURITY Equity", "securityError": {...}, "fieldData": {}}]}`.
   If the `BQuery` was created with `debug=`, `_write_debug_dump` writes the request
   and the messages to a file at this point, also when the request failed.
4. **Parse the answer.** `_parse_bdp` makes one row per security. `_security_errors`
   turns the `securityError` into the line
   `NOT A SECURITY Equity: Unknown/Invalid Security (BAD_SEC)`, and
   `_warn_security_errors` logs all such lines of the call as one warning.
5. **Build the DataFrame.** Two rows; `NAME` is null for the unknown security.

## Bloomberg quirks the code handles

Each quirk has a comment at the place that handles it. A name like
`bdp/partial_responses` is the recording `tests/data/recordings/bdp/partial_responses.json`;
open it to see the raw answer.

| Quirk | Handled in | Shown by |
|---|---|---|
| An answer arrives as several events, and `bdp` rows not in the order of the request | `_send_request`, `_parse_bdp` | `bdp/partial_responses` |
| The reply to an old request that timed out arrives during the next request | `_send_request` (correlation ID check, and the cancel on any error) | `test_late_answer_to_timed_out_request_is_not_returned_to_next_request` in `tests/unit/test_send_request.py` |
| Bloomberg refuses a request in two ways: a `RequestFailure` message, or a `responseError` inside a normal response | `_send_request` | `bdp/no_securities`, `bdib/invalid_security` |
| An unknown security or field does not fail the request; it is reported inside the answer | `_security_errors` | `bdp/invalid_security_mixed`, `bdp/invalid_field_mixed`, `bdh/invalid_field_mixed` |
| An unknown security is a row of nulls in `bdp` but no rows in `bdh` | `_parse_bdp`, `_parse_bdh` | `bdp/invalid_security_mixed`, `bdh/invalid_security_mixed` |
| Field names come back spelled as the request spelled them | `_parse_bdp` | `bdp/lowercase_field` |
| An override date must be sent as `YYYYMMDD`; Bloomberg ignores other forms without an error | `_override_pairs` | `bdp/override_reference_date` (the accepted form) |
| Intraday times are UTC, in the request and in the answer | `_iso_datetime`, `_BAR_SCHEMA` | `bdib/trade_bars_60min` |
| The intraday answer does not name the security | `_parse_bdib` | `bdib/trade_bars_60min` |
| A BQL answer is JSON text, and a long one is cut into fragments over several messages | `_load_bql_answers` | `test_bql_answer_split_over_several_messages_is_joined` in `tests/unit/test_bql.py` |
| A failed BQL query is an answer too, with its reason inside | `_parse_bql` | `bql/syntax_error`, `bql/invalid_item` |
| "No results" is sent as an exception of type `BENIGN` | `_parse_bql` | `bql/empty_universe` |
| BQL sends dates, NaN and infinity as text | `_bql_date`, `_bql_datetime`, `_bql_number` | `bql/date_value`, `bql/nan_and_infinity` |
| A BQL item can be named like one of its own columns | `_bql_item` | `bql/item_named_id` |
| A search grid has no column types; an empty numeric cell is a blank string | `_cell_value`, `_blank_strings_to_none` | `test_bsrch_blank_cells_in_numeric_column_become_null` in `tests/unit/test_bsrch.py` |
| `ReachMax` is true also when a `LIMIT` override cut the search short | `_parse_bsrch` | `bsrch/coco_limit_2` |
| A failed search is a grid with an `Error` text | `_parse_bsrch` | `bsrch/invalid_domain` |

## Recipes

### Set up and run the checks

```bash
uv sync --extra dev --extra docs
```

`blpapi` is not on PyPI; `pyproject.toml` tells uv where Bloomberg publishes it.

| Check | Command | Needs |
|---|---|---|
| Offline tests | `pytest` | nothing |
| Lint and format | `ruff check .` and `ruff format --check .` | nothing |
| Recordings replay to the same results | `python tests/record.py --refresh-expected`, then `git diff --stat tests/data/recordings` must print nothing | nothing |
| Docs build | `mkdocs build --strict` | the `docs` extra |
| Live tests | `pytest --bbg` | a Bloomberg Terminal on this machine |

The offline tests must also pass on the oldest supported versions (Python 3.12,
polars 1.7.1, blpapi 3.24.2). Build that environment outside the repository and
leave the project environment alone:

```bash
MIN=~/venvs/plbbg-min    # any directory outside the repository
uv venv $MIN --python 3.12
uv pip install --python $MIN --no-config \
    --index https://blpapi.bloomberg.com/repository/releases/python/simple/ \
    --index https://pypi.org/simple \
    -e . pytest pytest-cov pyyaml "polars==1.7.1" "blpapi==3.24.2"
$MIN/bin/python -m pytest tests/unit -m no_bbg
```

How the tests are organised, and in which order to read the live tests as a tour of
the library, is in
[tests/README.md](https://github.com/MarekOzana/polars-bloomberg/blob/main/tests/README.md).

### Capture a failing case and turn it into an offline test

1. **Get the raw answer.** Ask the user to run the failing call with `debug=True`:

    ```python
    with BQuery(debug=True) as bq:
        bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])
    ```

    Every request writes one JSON file to `debug_cases/` in the working directory,
    for example `ReferenceDataRequest_20261116_201008_297370.json`, also when the
    call raises. The file holds the request, every message Bloomberg sent back, and
    the library versions. `BQuery(debug="some/dir")` writes to that directory.

2. **Make it small and public.** The repository is public. Cut the answer down by
   hand to what shows the problem: at most three securities and five rows where the
   case allows it. Never commit `debug_cases/` itself.

3. **Store it as a recording.** Copy the file to
   `tests/data/recordings/<method>/<short_name>.json`, and add the call under the
   same name to `SCENARIOS` in `tests/recordings.py`:

    ```python
    "bdp/unknown_field_next_to_known": lambda bq: bq.bdp(
        ["IBM US Equity"], ["NAME", "NOT_A_FIELD"]
    ),
    ```

    If nobody can record this answer again (the user's own instruments, a market
    state that is gone), also add the name to `CONVERTED` in the same file. Then
    `python tests/record.py` leaves the file alone.

4. **Write the test that shows the bug**, in `tests/unit/test_<method>.py`:

    ```python
    def test_bdp_unknown_field_next_to_known_gives_a_null_column():
        bq, _ = replay("bdp/unknown_field_next_to_known")

        df = bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])

        assert df["NOT_A_FIELD"].to_list() == [None]
    ```

    `replay` (in `tests/support.py`) returns a `BQuery` whose fake session answers
    the next request with the recorded messages.

5. **Fix the code**, then store the result the call now gives:

    ```bash
    python tests/record.py --refresh-expected bdp/unknown_field_next_to_known
    ```

    This replays the answer, writes the result into the file as `"expected"`, and
    replaces server names in Bloomberg's error details. Read the file once more, then
    run `pytest`: `tests/unit/test_recordings.py` now replays the new scenario
    together with all the others.

A recording made this way works like one recorded live. It differs in two harmless
ways: it has no `"scenario"` and `"call"` keys, and a BQL answer stays one long JSON
string instead of readable JSON.

### Fix a parsing bug

1. Find the parser in the method table above and open a recording of that method
   to see the shape of the payloads.
2. Reproduce the bug in a test. Replay a recording as in the recipe above, or script
   a payload by hand: `fake_bq([answer(payload)])` from `tests/support.py`. The
   files `tests/unit/test_bdp.py` to `test_bsrch.py` show both.
3. Fix the parser and run `pytest`.
4. Run `python tests/record.py --refresh-expected` and read
   `git diff tests/data/recordings`. Only the scenarios whose result you meant to
   change may differ.
5. If users can notice the fix, add a line to `CHANGELOG.md`.

### Add an option

A Bloomberg request option needs no new code. `options={"periodicitySelection":
"MONTHLY"}` reaches the request through `_set_options`, under Bloomberg's own
element name.

The offline tests build real `blpapi` requests on a reduced, hand-written schema,
`tests/data/schemas/refdata_test.xml`. An option that the schema does not list
raises `NotFoundException` there. To test a request with a new option, add the
`<element>` to the request type in that file, then assert on the request in
`tests/unit/test_requests.py` with `request_sent_by`.

A new keyword argument of a public method goes the same way as the existing ones:
check it in step 1, set it in step 2, test the request in
`tests/unit/test_requests.py`, describe it in the docstring and in
`docs/usage/<method>.md`, and add it to `CHANGELOG.md`.

### Add a new request type

1. Write the public method on `BQuery` in the five steps, after the method it
   resembles most.
2. Add a numbered section for it below the class with its `_parse_<name>` function,
   and add the method to the table and the section list in the module docstring.
3. Make the request testable offline. For `//blp/refdata`, add the operation and its
   request and response types to `tests/data/schemas/refdata_test.xml`. For another
   service there is no offline schema; the tests use a `MagicMock` service, as
   `tests/unit/test_bsrch.py` does.
4. Add scenarios to `SCENARIOS` in `tests/recordings.py` and record them at a
   Terminal: `python tests/record.py <name>/`. List a scenario in `VOLATILE` if its
   data changes from run to run, and in `CONTRACT` if a live test should watch the
   format of its answer.
5. Add `tests/unit/test_<name>.py` (offline, end to end) and
   `tests/integration/test_integration_<name>.py` (live).
6. Document it: a Google-style docstring with an example (`docs/api.md` renders
   it), a page `docs/usage/<name>.md` listed in `mkdocs.yml`, a section in
   `README.md`, a line in `CHANGELOG.md`.

### Re-record or refresh recordings

A recording is one file: the request, the raw messages, and `"expected"`, the result
the call gave.

- **You changed the output on purpose** (no Terminal needed). Run
  `python tests/record.py --refresh-expected`; it replays every recording and
  rewrites only `"expected"`. Add name prefixes to limit it, for example
  `python tests/record.py --refresh-expected bql/`. Review
  `git diff tests/data/recordings` before you commit.
- **You need a fresh answer from Bloomberg** (Terminal needed). Run
  `python tests/record.py bdp/ bql/syntax_error` for the scenarios whose names start
  with these prefixes, or `python tests/record.py` for all. Scenarios in `VOLATILE`
  change on every run; scenarios in `CONVERTED` are skipped.
- **You suspect Bloomberg changed a format.** `pytest --bbg` includes
  `tests/integration/test_contract.py`, which asks Bloomberg again for the scenarios
  in `CONTRACT` and compares the shape of the fresh answer with the recording.
- **You changed the dump format** in `_write_debug_dump`. Every recording is a dump,
  so all of them have to be recorded again. Avoid it.

## Conventions

- **Users must not be surprised.** Method names, parameters, column names, dtypes,
  exception types and the wording of log messages are part of the interface; tests
  assert on them. A change that users can notice goes into `CHANGELOG.md`.
- **One file, plain functions.** Being able to open `plbbg.py` and follow a call
  from request to DataFrame is the main strength of the library. Prefer a function
  to a new class, and do not split the file.
- **Comments say why.** When a comment explains a Bloomberg quirk, name the
  recording that shows it.
- **Ruff** checks and formats the code (`pyproject.toml`): line length 89,
  docstrings required in the package but not in tests, at most six arguments per
  function.
- **Public methods** keep Google-style docstrings with an example and its output.
- **Diagnostics** go through `logger`, never `print`.
- **No new dependencies**, at runtime or for development. Python 3.12+, polars
  1.7.1+, blpapi 3.24.2+.
- **Tests** check one behavior each, and the name states the scenario and the
  expected result, for example
  `test_bdh_weekend_range_returns_empty_frame_with_columns`. They go through the
  public methods with the fake session; they do not patch private functions.
  Offline test modules set `pytestmark = pytest.mark.no_bbg`, live ones
  `pytest.mark.requires_bbg`.
- **Recordings stay small and public:** well-known instruments, at most three
  securities and five rows where the scenario allows it, no machine names.
- **Releases** are the maintainer's: do not bump the version, tag or publish.
