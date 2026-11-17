---
title: Developer Guide
description: How polars-bloomberg is built - the one pipeline every call follows, a map of plbbg.py, how to find a bug with a debug dump, the Bloomberg quirks the code handles, and recipes for fixing bugs and adding features with offline tests.
---

# Developer guide

You have a bug report or a feature idea for polars-bloomberg. Three facts keep the
work small:

- **The code is one file.** Almost everything is in `polars_bloomberg/plbbg.py`,
  and it reads from top to bottom.
- **Every call runs the same five steps.** When you can read `bdp`, you can read
  the other four methods.
- **You need no Bloomberg Terminal.** The tests replay recorded Bloomberg answers,
  and a user's failing call can be saved to a file and replayed on your machine.

The first six sections, up to [Find where a bug is](#find-where-a-bug-is), are a
tour: read them in order, in about eight minutes. The sections after them are for
looking things up.

| You want to | Go to |
|---|---|
| run the tests for the first time | [Set up](#set-up) |
| find the code behind a wrong result | [Find where a bug is](#find-where-a-bug-is) |
| reproduce a user's bug without a Terminal | [Capture a failing case](#capture-a-failing-case-and-turn-it-into-an-offline-test) |
| know whether a strange behavior is Bloomberg's | [Bloomberg quirks the code handles](#bloomberg-quirks-the-code-handles) |
| add an option or a new method | [Add an option](#add-an-option), [Add a new request type](#add-a-new-request-type) |
| know what a pull request must satisfy | [Before you open a pull request](#before-you-open-a-pull-request) |

## Set up

In your clone of the repository:

```bash
uv sync --extra dev --extra docs
uv run pytest
```

All tests pass in about a second. The live tests are reported as skipped: they run
only with `pytest --bbg`, at a Bloomberg Terminal.

The other commands on this page are written without `uv run`. Put it in front of
each, or activate the environment once: `source .venv/bin/activate`, on Windows
`.venv\Scripts\activate`.

`blpapi` is not on PyPI; `pyproject.toml` tells uv where Bloomberg publishes it.
Without uv, run `pip install -r requirements.txt` and then
`pip install -e ".[dev]"` in an environment of your own.

## The library in one minute

polars-bloomberg is five methods on `BQuery`: `bdp`, `bdh`, `bdib`, `bql` and
`bsrch`. Each one sends one Bloomberg request and turns the answer into one Polars
DataFrame (`bql` returns a `BqlResult`, which holds one DataFrame per item of the
query).

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
dicts, lists, numbers, strings and dates. The schema names the columns and gives a
dtype to `security` only; Polars infers the dtype of each field from all rows.

Only step 3 talks to Bloomberg, and it does so through one object, the session:

```text
bq.bdp(...)                                                         DataFrame
    │                                                                   ▲
    ▼                                                                   │
1 check inputs ─► 2 build request ─► 3 send ─► 4 parse answer ─► 5 build DataFrame
                                      │  ▲
                              request ▼  │ payloads
                                   the session
                      live:   Bloomberg answers
                      tests:  a fake session replays a recorded answer
```

This is why the offline tests need no Bloomberg. They call the public methods on a
`BQuery` whose session is a `FakeSession` (in `tests/support.py`). All five steps
run as they do for a user; only the answer comes from a file.

The other four methods read the same way. They differ in these places:

| Method | Service | Request | Parser | What is different |
|---|---|---|---|---|
| `bdp` | `//blp/refdata` | `ReferenceDataRequest` | `_parse_bdp` | the model for the others |
| `bdh` | `//blp/refdata` | `HistoricalDataRequest` | `_parse_bdh` | two dates in the request; one security per payload |
| `bdib` | `//blp/refdata` | `IntradayBarRequest` | `_parse_bdib` | one security; times instead of dates, in UTC; fixed columns |
| `bql` | `//blp/bqlsvc` | `sendQuery` | `_parse_bql` | the answer is JSON text; one DataFrame per item; errors are part of the answer |
| `bsrch` | `//blp/exrsvc` | `ExcelGetGridRequest` | `_parse_bsrch` | overrides are sent as text; the answer is a grid without column types |

## Words used on this page

The first seven rows are Bloomberg's words, which you will also meet in Bloomberg's
documentation. The last four belong to this repository.

| Word | Meaning |
|---|---|
| security, field | What a request asks about (`IBM US Equity`) and what it asks for (`PX_LAST`). |
| override | A value that replaces one that Bloomberg would otherwise use in a calculation, for example `REFERENCE_DATE`. |
| option | A setting of the request itself, for example `periodicitySelection`. |
| session | The connection to Bloomberg, a `blpapi.Session`. `BQuery.start()` opens it. |
| service | A group of request types, for example `//blp/refdata`. A session opens a service before it uses it. |
| event, message | Bloomberg answers in events, and an event holds messages. The last event of an answer has the type `RESPONSE`; the events before it have the type `PARTIAL_RESPONSE`. |
| correlation ID | The ticket that the session returns when a request is sent. Every message of the answer carries it. |
| payload | One message as plain Python. |
| debug dump | The JSON file that `BQuery(debug=True)` writes for each request: the request and the messages that came back. |
| recording | A debug dump stored in `tests/data/recordings/`, with the result of the call added as `"expected"`. |
| scenario | A named call in `tests/recordings.py`. Its answer is the recording of the same name. |

## A map of `plbbg.py`

The module docstring is the table of contents. The file has eight numbered sections.
The class comes first and shows the five steps of each method; the sections after it
hold what the steps call. Each section starts with a banner comment, so a search for
`# 3.` takes you to section 3. The table names the main functions of each section.

| Section | What is in it | Go here when |
|---|---|---|
| 1. `BloombergError`, `BQuery` | `start`/`stop`; `_create_request` (opens a service on first use); `_send_request` (the event loop); the five public methods | a call hangs, times out, raises, or returns another call's data; a request is built wrongly |
| 2. Inputs and requests | `_as_list`, `_check_no_field_is_named`, `_yyyymmdd`, `_append_values`, `_set_overrides`, `_override_pairs`, `_set_options` | an input is refused or sent in the wrong form |
| 3. bdp and bdh | `_parse_bdp`, `_parse_bdh`, `_security_errors`, `_warn_security_errors` | a value or row is missing; a warning about a security or field is wrong |
| 4. bdib | `_iso_datetime`, `_BAR_SCHEMA`, `_parse_bdib` | times are off; a bar column is missing |
| 5. bql | `_set_bql_client_context`, `_parse_bql`, `_load_bql_answers`, `_bql_item`, `_BQL_DTYPES`, `_bql_number`, `_bql_date`, `_bql_datetime`, `_bql_dataframe` | a BQL column has the wrong type or name; an error message is lost |
| 6. bsrch | `_set_bsrch_overrides`, `_parse_bsrch`, `_cell_value`, `_blank_strings_to_none` | a search column has the wrong type or title |
| 7. `BqlResult` | the frames of a BQL answer and `combine()` | joining BQL items gives unexpected rows or columns |
| 8. Debug dump | `_write_debug_dump`, `_json_default` | you change what `BQuery(debug=True)` writes |

## One call, end to end

Take `bq.bdp(["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"])`: one security
that exists and one that does not. Bloomberg's answer to it is stored in
`tests/data/recordings/bdp/invalid_security_mixed.json`.

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
   request, and returns the payloads. Here that is one payload (shortened):

    ```python
    {
        "securityData": [
            {
                "security": "IBM US Equity",
                "fieldData": {"NAME": "INTL BUSINESS MACHINES CORP"},
            },
            {
                "security": "NOT A SECURITY Equity",
                "securityError": {
                    "category": "BAD_SEC",
                    "message": "Unknown/Invalid Security  [nid:<server>]",
                },
                "fieldData": {},
            },
        ]
    }
    ```

    If the `BQuery` was created with `debug=`, `_write_debug_dump` writes the
    request and the messages to a file at this point, also when the request failed.

4. **Parse the answer.** `_parse_bdp` makes one row per security. The unknown
   security has no `NAME` in its `fieldData`, so its row holds `None`.
   `_security_errors` turns the `securityError` into one line of text, and
   `_warn_security_errors` logs all such lines of the call as one warning.
5. **Build the DataFrame.** Two rows; `NAME` is null for the unknown security.

The caller sees a warning in the log and gets the DataFrame:

```text
Bloomberg reported 1 error(s):
  NOT A SECURITY Equity: Unknown/Invalid Security  [nid:<server>] (BAD_SEC)

shape: (2, 2)
┌───────────────────────┬─────────────────────────────┐
│ security              ┆ NAME                        │
│ ---                   ┆ ---                         │
│ str                   ┆ str                         │
╞═══════════════════════╪═════════════════════════════╡
│ IBM US Equity         ┆ INTL BUSINESS MACHINES CORP │
│ NOT A SECURITY Equity ┆ null                        │
└───────────────────────┴─────────────────────────────┘
```

`[nid:<server>]` stands for the name of the Bloomberg server that answered. A
recording never stores that name.

## Find where a bug is

A debug dump tells you which of the five steps went wrong. Run the failing call with
`BQuery(debug=True)`. Every request then writes one JSON file to `debug_cases/`: the
request that was sent, and every message that came back. This is the dump of
`bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])`, shortened:

```json
{
 "format": 1,
 "recorded": {"date": "2026-11-16", "blpapi": "3.26.10.1", "polars": "2.1.0"},
 "request": {
  "operation": "ReferenceDataRequest",
  "body": {"securities": ["IBM US Equity"], "fields": ["NAME", "NOT_A_FIELD"]}
 },
 "events": [{
   "type": "RESPONSE",
   "messages": [{
     "type": "ReferenceDataResponse",
     "payload": {"securityData": [{
       "security": "IBM US Equity",
       "fieldExceptions": [{
         "fieldId": "NOT_A_FIELD",
         "errorInfo": {"category": "BAD_FLD", "message": "Field not valid"}
       }],
       "fieldData": {"NAME": "INTL BUSINESS MACHINES CORP"}
     }]}
   }]
 }]
}
```

Compare the dump with what the user expected, from the top:

| What you find | Where to look |
|---|---|
| No file was written | Nothing was sent. The call failed in step 1 or in `_create_request`: the session is not started, or the service did not open. |
| `request` is not what the user asked for | Step 2: the body of the method, and section 2 of `plbbg.py`. |
| `request` is right, but `events` does not hold the data, or holds an error | Bloomberg did not send the data. Look for the behavior in the [quirks table](#bloomberg-quirks-the-code-handles). If it is not there, the cause is usually the content of the request (a field, an override, an entitlement) and not the library. |
| `events` holds the data, but the DataFrame lacks it, or has it under the wrong name or dtype | Steps 4 and 5: the parser of the method, in sections 3 to 6. |
| The call hangs, times out, or returns the data of another call | Step 3: `_send_request`. |

The same file is the input of an offline test. See
[Capture a failing case](#capture-a-failing-case-and-turn-it-into-an-offline-test).

## Bloomberg quirks the code handles

Read this table before you "fix" something: what looks like a bug of the library
is often a behavior of Bloomberg that the code handles on purpose. Each quirk has a
comment at the place that handles it. A name like `bdp/partial_responses` is the
recording `tests/data/recordings/bdp/partial_responses.json`; open it to see the
raw answer. Where no recording shows the quirk, the table names a test.

| Quirk | Handled in | Shown by |
|---|---|---|
| **Any call** | | |
| An answer arrives as several events | `_send_request` | `bdp/partial_responses` |
| The reply to an old request that timed out arrives during the next request | `_send_request` (correlation ID check, and the cancel on any error) | `test_late_answer_to_timed_out_request_is_not_returned_to_next_request` in `tests/unit/test_send_request.py` |
| Bloomberg refuses a request in two ways: a `RequestFailure` message, or a `responseError` inside a normal response | `_send_request` | `test_request_failure_raises_bloomberg_error_with_reason` in `tests/unit/test_send_request.py` (the first way); `bdp/no_securities` (the second) |
| An override date must be sent as `YYYYMMDD`; Bloomberg ignores other forms without an error | `_override_pairs` | `bdp/override_reference_date` (the accepted form) |
| **`bdp` and `bdh`** | | |
| An unknown security or field does not fail the request; it is reported inside the answer | `_security_errors` | `bdp/invalid_security_mixed`, `bdp/invalid_field_mixed`, `bdh/invalid_field_mixed` |
| An unknown security is a row of nulls in `bdp` but no rows in `bdh` | `_parse_bdp`, `_parse_bdh` | `bdp/invalid_security_mixed`, `bdh/invalid_security_mixed` |
| `bdp` rows come back in Bloomberg's order, not in the order of the request | `_parse_bdp` | `bdp/partial_responses` |
| Field names come back spelled as the request spelled them | `_parse_bdp` | `bdp/lowercase_field` |
| **`bdib`** | | |
| An unknown security fails the whole request here, with a `responseError` | `_send_request` | `bdib/invalid_security` |
| Intraday times are UTC, in the request and in the answer | `_iso_datetime`, `_BAR_SCHEMA` | `bdib/trade_bars_60min` |
| The intraday answer does not name the security | `_parse_bdib` | `bdib/trade_bars_60min` |
| **`bql`** | | |
| The request says that it comes from Excel, as Bloomberg's own Excel add-in does | `_set_bql_client_context` | `test_bql_request_sets_expression_and_client_context` in `tests/unit/test_requests.py` |
| A BQL answer is JSON text, and a long one is cut into fragments over several messages | `_load_bql_answers` | `test_bql_answer_split_over_several_messages_is_joined` in `tests/unit/test_bql.py` |
| A failed BQL query is an answer too, with its reason inside | `_parse_bql` | `bql/syntax_error`, `bql/invalid_item` |
| "No results" is sent as an exception of type `BENIGN` | `_parse_bql` | `bql/empty_universe` |
| BQL sends dates, NaN and infinity as text | `_bql_date`, `_bql_datetime`, `_bql_number` | `bql/date_value`, `bql/nan_and_infinity` |
| A BQL item can be named like one of its own columns | `_bql_item` | `bql/item_named_id` |
| **`bsrch`** | | |
| A search grid has no column types; an empty numeric cell is a blank string | `_cell_value`, `_blank_strings_to_none` | `test_bsrch_blank_cells_in_numeric_column_become_null` in `tests/unit/test_bsrch.py` |
| `ReachMax` is true also when a `LIMIT` override cut the search short | `_parse_bsrch` | `bsrch/coco_limit_2` |
| A failed search is a grid with an `Error` text | `_parse_bsrch` | `bsrch/invalid_domain` |

## Recipes

### Run the checks

| Check | Command | Run it |
|---|---|---|
| Offline tests | `pytest` | before every commit |
| Lint and format | `ruff check .` and `ruff format --check .` | before every commit |
| Recordings replay to the same results | `python tests/record.py --refresh-expected`, then `git diff --stat tests/data/recordings` must print nothing | after a change to `plbbg.py` |
| Minimum dependencies | see below | before a pull request that changes `plbbg.py` |
| Docs build | `mkdocs build --strict` | after a change to `docs/` or to a public docstring |
| Live tests | `pytest --bbg` | when you have a Bloomberg Terminal on this machine |
| Recipe outputs | `python dev/refresh_recipes.py`, then review `git diff docs/recipes` | at a Terminal, after adding or changing a recipe in `docs/recipes/`, or when its output is stale |

A recipe in `docs/recipes/` is a self-contained ```` ```python ```` block that prints
its result, followed by a line starting with `Output` and a fenced block. The
script runs each recipe, pastes what it printed into that block and sets the page's
"as of" date. To add a recipe, write the code with an empty output block and run
the script. A page with a failing recipe is not written, and that includes a screen
that matches nothing on the day.

A recipe on the page looks like this; `docs/stylesheets/recipes.css` styles it.

````markdown
## Large axes only

`axes_by_dealer()`, `matches()`, `dropna()`
{ .recipe-uses }

One sentence: the question the recipe answers.

```python
...
```

Output
``` { .text .no-copy .recipe-output }
```

!!! warning "Watch out"
    Only traps that give a wrong number or an error.

Notes
{ .recipe-label }

- How to read the output and adapt the query.
````

- The `{ .recipe-uses }` line is the "Uses" cell of the recipe's row in
  `recipes/index.md`; keep the two the same. It also numbers the heading.
- Keep the blank line before `Output`. Without it the label is not a paragraph and
  loses its style.
- The output block is `text`, not `python`, and has no copy button.

The recordings check repeats what `pytest` does, but more strictly: `pytest`
compares numbers with a tolerance, and `git diff` shows every difference.

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

On Windows the interpreter of that environment is `Scripts\python.exe` instead of
`bin/python`.

How the tests are organised, and in which order to read the live tests as a tour of
the library, is in
[tests/README.md](https://github.com/MarekOzana/polars-bloomberg/blob/main/tests/README.md).

### Capture a failing case and turn it into an offline test

The call and the names in this recipe are examples; use those of your case.

1. **Get the raw answer.** Run the failing call with `debug=True`, or ask the user
   who reported it to do so:

    ```python
    with BQuery(debug=True) as bq:
        bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])
    ```

    Every request writes one JSON file to `debug_cases/` in the working directory,
    for example `ReferenceDataRequest_20261116_201008_297370.json`, also when the
    call raises. The file holds the request, every message Bloomberg sent back, and
    the library versions; [Find where a bug is](#find-where-a-bug-is) shows one.
    `BQuery(debug="some/dir")` writes to that directory.

2. **Make it small and public.** The repository is public. Cut the answer down by
   hand to what shows the problem: at most three securities and five rows where the
   case allows it. Replace instruments and names that are the user's own by
   well-known ones, in the request and in the answer alike. Keep the markers that
   hold dates, such as `{"$date": "2024-01-02"}`: the replay turns them back into
   dates. Never commit `debug_cases/` itself.

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

    From now until step 5, `tests/unit/test_recordings.py` fails for the new
    scenario with `KeyError: 'expected'`. That is correct: the file does not say
    yet what the call should give.

4. **Write the test that shows the bug**, in `tests/unit/test_<method>.py`:

    ```python
    def test_bdp_unknown_field_next_to_known_gives_a_null_column():
        bq, _ = replay("bdp/unknown_field_next_to_known")

        df = bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])

        assert df["NOT_A_FIELD"].to_list() == [None]
    ```

    `replay` (in `tests/support.py`) returns a `BQuery` whose fake session answers
    the next request with the recorded messages. Run the test and see it fail.

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

1. Find the parser in the [method table](#the-library-in-one-minute) and open a
   recording of that method to see the shape of the payloads.
2. Reproduce the bug in a test. Replay a recording as in the recipe above, or
   write the payload by hand when a few lines are enough:

    ```python
    def test_bdp_security_without_field_data_gives_a_null():
        payload = {"securityData": [{"security": "IBM US Equity", "fieldData": {}}]}
        bq = fake_bq([answer(payload)])

        df = bq.bdp(["IBM US Equity"], ["NAME"])

        assert df["NAME"].to_list() == [None]
    ```

    `fake_bq` and `answer` are in `tests/support.py`. The files
    `tests/unit/test_bdp.py` to `test_bsrch.py` show both kinds of test.

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
   `README.md`, a line in `CHANGELOG.md`, and a row in the tables of this page.

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

## Before you open a pull request

1. `pytest`, `ruff check .` and `ruff format --check .` pass.
2. `python tests/record.py --refresh-expected` changes no recording, or only the
   recordings whose result you meant to change.
3. If you changed `plbbg.py`, the offline tests also pass on the
   [minimum dependencies](#run-the-checks).
4. A bug fix comes with a test that failed before the fix.
5. A change that users can notice has a line under "Unreleased" in `CHANGELOG.md`,
   and the docstring and the page in `docs/usage/` say what the code now does.
6. The version number is untouched.
