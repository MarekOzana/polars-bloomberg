# polars-bloomberg improvement plan

Reviewed commit: `7a07c8ad0bfa987348071fc0cac4f8c695244044` (branch `main`). All `file:line`
references are against that commit. `plbbg.py` means `polars_bloomberg/plbbg.py`.

This file is the complete task description for the agent that implements it. A reviewer
wrote it after reading and running the code without a Bloomberg connection. You, the
implementer, have a logged-in Bloomberg Terminal. Everything you need is in this file and
in the repository.

## 1. Summary

polars-bloomberg is small and easy to follow, and valid requests work. The problems are
at the edges, where Bloomberg answers with something other than plain data. There the
library mostly returns nulls or an empty frame and says nothing, and in one case it
returns the answer to a different request. The offline tests have 100% line coverage but
never send a request through a public method, so none of these edges is tested.

The four problems that matter most:

1. **Wrong data after a timeout or interrupt (F01).** `_send_request` ignores
   correlation IDs. After a `TimeoutError` or a notebook "interrupt kernel", the next
   call returns the previous request's answer, and every later call stays one behind.
2. **Silent nulls (F09, F10).** An invalid security becomes a null row in `bdp` and
   vanishes from `bdh`; a misspelled field becomes an all-null column. Bloomberg reports
   both in the response; the parsers never read the report.
3. **Fragile BQL parsing (F19, F22, F23).** A response with both data and an exception
   is discarded; one line uses the Python type `str` where the text `"STRING"` was
   meant; a value type outside the five known ones fails the whole query.
4. **Fragile output shape (F12, F13).** `bdp` crashes when a field's first value comes
   after row 100; empty results are DataFrames with no columns.

The plan has eleven steps, each one pull request. Steps 1-4 build the safety net: tests
through the public methods, the event-loop fix, and recordings of real Bloomberg answers
that are replayed offline. Steps 5-8 fix the bugs against those recordings. Steps 9-11
clean up, align the tooling, and update the documentation.

## 2. How to work through this plan

Do the steps of section 7 in order. Later steps use the test tools of earlier ones.

- [x] Step 1: test plumbing and first end-to-end tests (offline)
- [x] Step 2: event loop (live gate before coding, live check before merging)
- [x] Step 3: session lifecycle
- [x] Step 4: debug dump, recordings, contract tests, live checks (needs Bloomberg)
- [x] Step 5: `bdp` and `bdh` results
- [x] Step 6: inputs
- [x] Step 7: BQL
- [x] Step 8: `bsrch`
- [x] Step 9: clean-up
- [x] Step 10: tooling and CI
- [x] Step 11: documentation

Tick a box when the step's "Done when" list is met, and commit this file with the step.

For every step:

1. Create a branch from the previous step's branch. One step is one pull request.
2. For each bug fix, write the test first, run it, and confirm it fails for the reason
   the plan describes. Then fix the code. If such a test passes before the fix, stop
   and find out why before changing library code.
3. Change only what the step lists. Clean-up has its own step (step 9).
4. Run the checks of section 3 and the step's "Done when" list.
5. List every user-visible change in the pull request description and in
   `CHANGELOG.md` (create it in the first step that has a visible change).

Stop and report to the maintainer, instead of improvising, when:

- a live gate fails (step 2);
- a recording contradicts what this plan assumes, in a way the live-check table of
  step 4 does not cover;
- a fix would change a public signature or a default output beyond what the step says.

## 3. Rules

**Environment.** Python 3.12 or newer. Install with `uv sync --extra dev`, or
`pip install -r requirements.txt` followed by `pip install -e ".[dev]"`
(`requirements.txt` holds the index URL for `blpapi`, which is not on PyPI).

**Checks to run before every commit.**

| Check | Command |
|---|---|
| Offline tests | `python -m pytest tests/unit -m no_bbg` (from step 1 on: `pytest`) |
| Lint | `ruff check .` |
| Live tests | `python -m pytest tests/integration -m requires_bbg` (from step 1 on: `pytest --bbg`) |
| Minimum dependencies | see below; needed in steps 1, 2, 4, 5, 7, 8 |

At the reviewed commit: 115 offline tests pass, `ruff check .` passes.

Minimum dependencies (polars 1.7.1, blpapi 3.24.2): create an environment in a
directory outside the repository (called `MIN` here) and run the offline tests with it.
Do not change the project environment.

```
uv venv MIN --python 3.12
uv pip install --python MIN --no-config --index https://blpapi.bloomberg.com/repository/releases/python/simple/ --index https://pypi.org/simple -e . pytest pytest-cov pyyaml "polars==1.7.1" "blpapi==3.24.2"
MIN/bin/python -m pytest tests/unit -m no_bbg        # on Windows: MIN\Scripts\python
```

**Conventions.**

- Ruff settings are in `pyproject.toml`: line length 89, docstrings required in the
  package and not in tests, at most 6 arguments per function.
- Public methods keep their Google-style docstrings with an example.
- Diagnostics go through `logger` (`logging.getLogger(__name__)`), never `print`.
- No new runtime dependencies. No new development dependencies.
- Keep the code simple. The library's main strength is that a reader can open one file
  and follow request, send, parse, DataFrame. Prefer a plain function to a new class. Do
  not split `plbbg.py`, and do not add abstractions this plan does not ask for.
- Tests: one behavior per test. The name states the scenario and the expected result,
  for example `test_bdh_weekend_range_returns_empty_frame_with_columns`. Go through the
  public methods with the fake session of Appendix A; do not patch private methods.
  Offline test modules set `pytestmark = pytest.mark.no_bbg`.
- Recordings of Bloomberg data in this public repository stay small: at most 3
  securities and 5 rows where the scenario allows it, well-known instruments, no machine
  names. Never commit `debug_cases/`.

## 4. Decisions already made

The reviewer chose these defaults so that the plan can be executed without waiting. The
maintainer can overrule any of them; until then, implement the default.

| # | Question | Default to implement |
|---|---|---|
| D1 | Invalid securities and fields (F09, F10, F21) | Log one warning per call that lists them. The DataFrame stays as it is today. Do not raise. |
| D2 | BQL errors (F20) | Keep returning an empty `BqlResult` and logging. Add `BqlResult.errors` and put the BQL message into the error that `combine()` raises. Do not raise from `bql()`. |
| D3 | Error class (F06) | One class, `BloombergError(ValueError)`. Deriving from `ValueError` keeps `except ValueError` (used with `bsrch`) and `except Exception` working. |
| D4 | Lifecycle (F03-F05) | Add `start()` and `stop()`. Open each service on first use. Users who can use the library today have all three services, so nothing changes for them. |
| D5 | `bdib` time zone (F17) | Keep the naive `time` column; document that it holds UTC. |
| D6 | Bloomberg data in recordings | Commit small real recordings; do not alter values. |
| D7 | `combine()` and null keys (F26) | Document the trap; do not change the join. |
| D8 | Type checking (F36) | Do not add mypy. It reports 26 errors today and 21 of them come from `blpapi`'s own type hints (`str` where `Name` is declared). Keep `py.typed` and complete the missing annotations by hand. |
| D9 | Empty results (F13) | Empty `bdp`, `bdh` and `bsrch` results get their columns. |
| D10 | Debug mode (F30) | `debug=` also accepts a directory. The dump format changes. The BQL-only dump is removed. |
| D11 | Releases | Add `CHANGELOG.md`. Steps 1-4 fit a 0.6.x release, steps 5-11 a 0.7.0 release. Do not bump versions or publish; the maintainer does that. |

## 5. Background

### The pipeline

Every public method of `BQuery` runs the same four stages:

```
bdp / bdh / bdib / bql / bsrch          public method                  plbbg.py:405-771
  -> _create_*request                   builds a blpapi.Request        plbbg.py:773-880
  -> _send_request                      sendRequest, then nextEvent in a loop; collects
                                        msg.toPy() of every message    plbbg.py:882-912
  -> _parse_*_responses                 list of payloads -> rows       plbbg.py:914-1202
  -> pl.DataFrame(...)
```

### What the payloads look like

`msg.toPy()` returns plain Python: dicts, lists, numbers, strings, `datetime.date`,
`datetime.datetime`, `datetime.time`.

| Method | Payload |
|---|---|
| `bdp` | `{"securityData": [{"security", "fieldData": {FIELD: value}, "fieldExceptions": [...], "sequenceNumber", "eidData", optional "securityError"}, ...]}`; one or more messages |
| `bdh` | `{"securityData": {"security", "fieldData": [{"date": date, FIELD: value}, ...], "fieldExceptions", optional "securityError"}}`; one message per security |
| `bdib` | `{"barData": {"eidData": [], "barTickData": [{"time", "open", "high", "low", "close", "volume", "numEvents", "value"}, ...]}}`; no `security` key |
| `bql` | one JSON string; parsed: `{"results": {ITEM: {"idColumn", "valuesColumn", "secondaryColumns": [...], "partialErrorMap", "responseExceptions"}}, "responseExceptions": [...] or null}` |
| `bsrch` | assumed `{"GridResponse": {"ColumnTitles", "DataRecords": [{"DataFields": [...]}], "ReachMax", "Error"}}`; the repository has no recording |
| any | `{"responseError": {"source", "code", "category", "message", "subcategory"}}` instead of data |

Error details inside a `bdp`/`bdh` answer:
`"securityError": {"source", "code", "category", "message", "subcategory"}` and
`"fieldExceptions": [{"fieldId", "errorInfo": {same five keys}}]`.

### Facts the reviewer verified offline

Checked on polars 2.1.0 with blpapi 3.26.10, and on polars 1.7.1 with blpapi 3.24.2.
You can rely on them.

- `blpapi.test` builds real events, messages and requests offline from a hand-written
  schema. Appendix A and B contain working code and a working schema.
- `request.asElement().toPy()` returns a request as a dict and
  `request.asElement().name()` its operation name. `Request.toPy()` does not exist.
- `cid in msg.correlationIds()` works; `blpapi.Names.REQUEST_FAILURE` and
  `blpapi.Names.SESSION_TERMINATED` exist; `Session.cancel(correlationId)` exists.
- A real `RequestFailure` message has the payload
  `{"reason": {"source", "errorCode", "category", "description"}}`.
- `pl.DataFrame(rows, schema={"security": pl.String, "x": None}, infer_schema_length=None)`
  works for empty and non-empty `rows`; a `None` dtype means "infer".
- Polars infers the schema of a list of dicts from the first 100 rows unless
  `infer_schema_length=None` is passed.
- `datetime.fromisoformat` accepts `2024-01-02T00:00:00Z`, fractional seconds and
  date-only strings.
- The Polars option to match nulls in joins is `join_nulls` up to 1.23 and
  `nulls_equal` from 1.24; `join_nulls` no longer exists in 2.0. Neither spelling works
  across the supported range.
- `blpapi` converts override values itself when the element is a string: `1.5` becomes
  `"1.500000"`, `True` becomes `"true"`, `None` becomes `"None"`, `date(2024, 1, 2)`
  becomes `"2024-01-02"`.

### Assumptions about Bloomberg that are not verified

They come from the reviewer's general knowledge. Do not rely on them before the live
check that covers them (the gate in step 2, L1-L10 in step 4). The second one cannot be
provoked on purpose; it is covered by offline tests only.

- Every response message carries the correlation ID that `sendRequest` returned.
- A failed request arrives as a `REQUEST_STATUS` event with a `RequestFailure` message
  and no `RESPONSE` event follows; a dead session arrives as `SessionTerminated`.
- The element names of `securityError` and `fieldExceptions` are as listed above.
- Bloomberg may name fields in its own spelling, not the user's (F11).
- BQL may attach errors to single items, and may split large answers (F21, F25).
- Intraday bars are available for about 140 days back.

## 6. Findings

"Offline" says whether the reviewer reproduced the problem without Bloomberg. "Live"
names the check in step 4 that settles what could not be reproduced. Severity: High
means wrong or lost data without an error; Medium means a crash, a misleading error or a
leak in normal use; Low means the rest.

| ID | Problem | Location in `plbbg.py` | Sev. | Offline | Live | Step |
|---|---|---|---|---|---|---|
| F01 | After a timeout or interrupt the next request returns the previous request's data | 882-903 | High | yes | gate | 2 |
| F02 | A failed request or dead session is reported as `TimeoutError` after the full wait | 886-903 | Med | loop only | – | 2 |
| F03 | Session is not stopped when `__enter__` fails after `start()` | 380-398 | Med | yes | – | 3 |
| F04 | All three services must open or nothing works | 390-396 | Med | no | – | 3 |
| F05 | Use without `with` gives `AttributeError`; no `start()`/`stop()` | 346-403 | Low | yes | – | 3 |
| F06 | Request errors are a bare `Exception` without the error category | 896-899 | Low | yes | – | 2 |
| F07 | Session and service status messages are collected as responses | 894-900 | Low | yes | – | 2 |
| F08 | `timeout` limits the wait per event, not per request; undocumented | 888 | Low | yes | – | 2 |
| F09 | Invalid security: null row in `bdp`, no rows in `bdh`, no message | 914-942 | High | yes | L1 | 5 |
| F10 | Invalid or misspelled field: all-null column, no message | 924-925, 939-940 | High | yes | L1 | 5 |
| F11 | Field names in another case than Bloomberg's may give all-null columns | 924-925, 939-940 | High | no | L2 | 5 |
| F12 | `bdp` crashes when a column's first value comes after row 100 | 459 | Med | yes | – | 5 |
| F13 | Empty `bdp`/`bdh`/`bsrch` results have no columns | 459, 529, 771 | Med | yes | – | 5, 8 |
| F14 | A bare string for `securities`/`fields` is sent one character at a time | 785-793 | Med | yes | – | 6 |
| F15 | Overrides: different shapes per method, confusing errors, `None` sent as `"None"` | 796-801, 821-826, 853-858 | Low | yes | L3 | 6 |
| F16 | `bdh` dates must be `date` objects; a field named `security` or `date` overwrites the key column | 525-526, 923-925 | Low | yes | – | 6 |
| F17 | `bdib` `time` column is naive; the docstring says the UTC zone is preserved | 554-557, 618 | Low | yes | L9 | 11 |
| F18 | `bdib`: a `date` argument crashes; dead fallback in the parser | 953, 1205-1211 | Low | yes | – | 6, 9 |
| F19 | A BQL response with both results and exceptions is discarded | 1149-1159 | High | code path | L4 | 7 |
| F20 | BQL errors are only logged; `combine()` then fails with an unrelated message | 1141-1143, 1157-1159 | Med | yes | – | 7 |
| F21 | Per-item BQL errors (`partialErrorMap`, item `responseExceptions`) are ignored | 1166-1185 | High | no | L4 | 7 |
| F22 | `_parse_result` crashes on a missing `type` or on `null` parts of an item | 1173-1184, 1202 | Med | yes | – | 7 |
| F23 | Unknown BQL value types and other date formats fail the whole query | 713-716, 1100-1127, 1193-1202 | Med | yes | L5 | 7 |
| F24 | Column-name clashes inside one BQL item overwrite data | 1173-1183 | Med | yes | L6 | 7 |
| F25 | A BQL answer split over several messages is lost or duplicated | 1129-1164 | Med | no | L7 | 7 |
| F26 | `combine()` without `on` duplicates rows whose shared columns hold nulls | 214-220 | Med | yes | – | 11 |
| F27 | The `bsrch` parser guesses key names; no real `GridResponse` is recorded | 977-984, 1020-1061 | Med | no | L8 | 8 |
| F28 | `bsrch` drops duplicate column titles and error texts without a message | 986-1018, 1063-1089 | Med | yes | L8 | 8 |
| F29 | `bsrch` docstring shows a warning that the code suppresses | 752-769 | Low | yes | – | 11 |
| F30 | Debug dumps: current directory, can fail the request, lose date types, overwrite each other, are not written on failure | 905-911, 1187-1189, 1213-1238 | Low | yes | – | 4 |
| F31 | Offline tests never run a request through a public method; several patch out the code they are named after | `tests/unit/` | Med | yes | – | 1, 4 |
| F32 | 15 offline tests fail unless pytest starts in the repository root | `tests/unit/` | Low | yes | – | 1 |
| F33 | `pytest tests` and `nox -s tests` run the live tests | `noxfile.py:27` | Low | yes | – | 1 |
| F34 | README, docs and docstrings disagree with the code in several places | various | Low | yes | – | 11 |
| F35 | `__version__` says 0.5.4; the package is 0.6.0 | `__init__.py:7` | Low | yes | – | 10 |
| F36 | nox, CI and `pyproject.toml` disagree on lint and format | `noxfile.py`, CI | Low | yes | – | 10 |

## 7. Steps

### Step 1: test plumbing and first end-to-end tests

Fixes F32 and F33 and starts on F31. No library code changes. No Bloomberg needed.

**Do.**

1. Create `tests/support.py` from Appendix A and
   `tests/data/schemas/refdata_test.xml` from Appendix B, verbatim. Tests import it with
   `from support import ...`; this works because `tests/conftest.py` puts `tests/` on
   `sys.path`. Ruff sorts `support` into the same import block as `pytest`
   (`ruff check --fix` does it).
2. In `pyproject.toml`: add `"tests/support.py" = ["N802"]` to
   `[tool.ruff.lint.per-file-ignores]` (the fake classes copy `blpapi`'s method names),
   and register both markers:

   ```toml
   markers = [
       "no_bbg: tests that run without Bloomberg",
       "requires_bbg: tests that need a live Bloomberg Terminal (run with --bbg)",
   ]
   ```

3. In `tests/conftest.py`: delete `pytest_configure`, make `live_bq` session-scoped
   (`@pytest.fixture(scope="session")`), and add:

   ```python
   def pytest_addoption(parser: pytest.Parser) -> None:
       parser.addoption(
           "--bbg",
           action="store_true",
           help="also run the tests that need a live Bloomberg Terminal",
       )


   def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
       if config.getoption("--bbg"):
           return
       skip = pytest.mark.skip(reason="needs a Bloomberg Terminal; run with --bbg")
       for item in items:
           if "requires_bbg" in item.keywords:
               item.add_marker(skip)
   ```

4. Replace every path that is relative to the working directory with
   `DATA_DIR / ...` (`from support import DATA_DIR`): `tests/unit/test_bdp_bdh.py:21`,
   `tests/unit/test_bdib.py:137`, `tests/unit/test_bql_parsing.py:19`, `372-378`,
   `400-530`.
5. Add tests that go through the public methods and pin what works today. Put them in
   new files `tests/unit/test_bdp.py`, `test_bdh.py`, `test_bql.py` and in the existing
   `test_bdib.py`, `test_bsrch.py`, `test_bql_result.py`. Pattern:

   ```python
   def test_bdp_returns_one_row_per_security_with_requested_columns():
       payload = {
           "securityData": [
               {"security": "IBM US Equity", "fieldData": {"NAME": "IBM"}},
               {"security": "SEBA SS Equity", "fieldData": {"NAME": "SEB"}},
           ]
       }
       bq = fake_bq([answer(payload)])

       df = bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME"])

       assert df.columns == ["security", "NAME"]
       assert df["NAME"].to_list() == ["IBM", "SEB"]
   ```

   Write at least these:

   | Test | Scripted answer |
   |---|---|
   | `test_bdp_returns_one_row_per_security_with_requested_columns` | as above |
   | `test_bdp_collects_rows_from_partial_and_final_events` | one `PARTIAL_RESPONSE` and one `RESPONSE` `FakeEvent` |
   | `test_bdp_bulk_field_becomes_list_of_structs` | `response_event("ReferenceDataRequest", ...)` with `INDX_MEMBERS` |
   | `test_bdh_date_column_has_date_dtype` | `response_event("HistoricalDataRequest", ...)` with `"date": "2024-01-02"` |
   | `test_bdh_missing_field_value_is_null` | two `fieldData` entries, one without the field |
   | `test_bdib_no_bars_returns_empty_frame_with_nine_columns` | `{"barData": {"barTickData": []}}` |
   | `test_bdib_rows_are_sorted_by_time` | two bars in reverse order |
   | `test_bql_two_items_give_two_named_frames` | JSON string with two items |
   | `test_bql_nan_string_becomes_null` | `"values": ["NaN", 1.5]`, type `DOUBLE` |
   | `test_bql_infinity_strings_become_inf` | `"values": ["Infinity", "-Infinity"]` |
   | `test_bql_syntax_error_returns_empty_result_and_logs` | the last element of `tests/data/bql/response_bql_raised_exception.json` |
   | `test_bsrch_blank_cells_in_numeric_column_become_null` | grid with `{"DoubleValue": 1.5}` and `{"StringValue": " "}` in one column |
   | `test_bsrch_grid_error_raises_value_error` | `{"GridResponse": {"Error": "Invalid domain", "DataRecords": []}}` |
   | `test_combine_without_on_duplicates_rows_with_null_keys` | two frames sharing `ID` and a `DATE` column that is null for one ID; documents F26 |

   Do not write tests that pin a behavior section 6 calls a bug.
6. Replace the content of `tests/README.md` with:

   ```
   Offline tests (no Bloomberg):   pytest
   Live tests (Terminal needed):   pytest --bbg

   tests/support.py holds the fake Bloomberg session used by the offline tests.
   ```

**Done when.** `pytest` passes from the repository root and from `tests/` with the 9
live tests skipped; the new tests pass against the unchanged library, also on the
minimum dependencies; `pytest --bbg` runs the live tests; `ruff check .` passes.

### Step 2: the event loop

Fixes F01, F02, F06, F07, F08.

**Live gate, before you write code.** The fix accepts only messages that carry the
request's correlation ID. If Bloomberg did not put that ID on some response, the method
concerned would time out forever. Run this against the unchanged library:

```python
import blpapi
from polars_bloomberg import BQuery

with BQuery() as bq:
    requests = {
        "bdp": bq._create_request("ReferenceDataRequest", ["IBM US Equity"], ["NAME"]),
        "bql": bq._create_bql_request("get(name) for(['IBM US Equity'])"),
        "bsrch": bq._create_bsrch_request("FI:SRCHEX.@COCO", {"LIMIT": 2}),
    }
    for name, request in requests.items():
        cid = bq.session.sendRequest(request)
        while True:
            event = bq.session.nextEvent(30_000)
            for msg in event:
                print(
                    name,
                    event.eventType(),
                    msg.messageType(),
                    cid in msg.correlationIds(),
                )
            if event.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.TIMEOUT):
                break
```

Expected: every line of a `PARTIAL_RESPONSE` or `RESPONSE` event ends in `True`; lines
of status events end in `False`. If a response line prints `False`, stop and report.

**Do.**

1. Add the error class near the top of `plbbg.py` and export it from
   `polars_bloomberg/__init__.py` (`__all__ = ["BQuery", "BloombergError", "BqlResult"]`):

   ```python
   class BloombergError(ValueError):
       """Bloomberg rejected or failed a request."""
   ```

2. Replace the loop in `_send_request` (`plbbg.py:882-903`) with the code below. It ran
   offline against real `blpapi.test` events on both dependency sets. Leave the debug
   block (`plbbg.py:905-911`) as it is; step 4 replaces it.

   ```python
   def _send_request(self, request: blpapi.Request) -> list[Any]:
       """Send a request and return the payloads (``msg.toPy()``) of its responses."""
       cid = self.session.sendRequest(request)
       responses: list[Any] = []
       try:
           finished = False
           while not finished:
               event = self.session.nextEvent(self.timeout)
               if event.eventType() == blpapi.Event.TIMEOUT:
                   raise TimeoutError(
                       f"Request timed out after {self.timeout} milliseconds"
                   )
               for msg in event:
                   if msg.messageType() == blpapi.Names.SESSION_TERMINATED:
                       raise ConnectionError("Bloomberg session terminated.")
                   if cid not in msg.correlationIds():
                       continue  # status message, or late answer to an old request
                   payload = msg.toPy()
                   if msg.messageType() == blpapi.Names.REQUEST_FAILURE:
                       reason = payload.get("reason", {})
                       raise BloombergError(
                           f"Request failed: {reason.get('description')} "
                           f"({reason.get('category')})"
                       )
                   if isinstance(payload, dict) and "responseError" in payload:
                       error = payload["responseError"]
                       raise BloombergError(
                           f"Response error: {error.get('message')} "
                           f"({error.get('category')})"
                       )
                   responses.append(payload)
                   finished = event.eventType() == blpapi.Event.RESPONSE
       except BaseException:
           self._cancel_quietly(cid)
           raise
       # ... existing debug block, unchanged ...
       return responses


   def _cancel_quietly(self, cid: blpapi.CorrelationId) -> None:
       """Tell Bloomberg to drop a request; never hide the error being raised."""
       try:
           self.session.cancel(cid)
       except Exception:  # the session may already be gone
           logger.debug("Could not cancel request %s", cid)
   ```

   Why each part is there: the correlation-ID test drops status messages (F07) and late
   answers to abandoned requests (F01). `cancel` stops Bloomberg from sending an answer
   nobody will read. `BaseException` covers `KeyboardInterrupt`. The message keeps the
   text `Response error: <message>` so existing checks on it still match.
3. Change the `timeout` docstring (`plbbg.py:364-366`) to say what it is: the longest
   wait, in milliseconds, for the next event from Bloomberg; `0` waits forever; slow
   BQL queries may need more than the default (F08).
4. In `tests/support.py`, delete `FakeMessage.hasElement` and `getElement`.
5. Rewrite `tests/unit/test_send_request.py` with `fake_bq` and the real events of
   `response_event` / `status_event`. Tests 1, 2, 3 and 6 fail on the old loop; that is
   the proof they test the fix.

   | # | Test | Script |
   |---|---|---|
   | 1 | `test_late_answer_to_timed_out_request_is_not_returned_to_next_request` | call 1 times out on an empty script; then add `response_event(..., cid=1)` and `response_event(..., cid=2)`; call 2 must return the second |
   | 2 | `test_request_failure_raises_bloomberg_error_with_reason` | `status_event(blpapi.Event.REQUEST_STATUS, "RequestFailure", {"reason": {"source": "s", "errorCode": 1, "category": "TIMEOUT", "description": "backend timed out"}}, cid=1)` |
   | 3 | `test_session_terminated_raises_connection_error` | `status_event(blpapi.Event.SESSION_STATUS, "SessionTerminated")` |
   | 4 | `test_status_messages_before_the_answer_are_skipped` | `status_event(blpapi.Event.SERVICE_STATUS, "ServiceOpened", {"serviceName": "//blp/refdata"})`, then the answer |
   | 5 | `test_partial_and_final_messages_are_collected_in_order` | `PARTIAL_RESPONSE` then `RESPONSE` |
   | 6 | `test_timeout_cancels_the_request` | empty script; assert `len(bq.session.cancelled) == 1` |
   | 7 | `test_keyboard_interrupt_cancels_the_request_and_next_call_is_correct` | script `[KeyboardInterrupt(), response_event(..., cid=2)]` |
   | 8 | `test_response_error_raises_bloomberg_error_with_category` | `{"responseError": {"source": "s", "code": 1, "category": "BAD_ARGS", "message": "No securities specified"}}` |

6. In `tests/unit/test_requests.py`, replace the `MagicMock` tests of the `bdp`, `bdh`
   and `bdib` request builders by tests on real requests:
   `assert bq.session.sent[0].asElement().toPy() == {...}`. Cover securities and fields,
   overrides, options, the `bdh` dates (`"startDate": "20240102"`), and `bdib` with a
   naive and a timezone-aware datetime. Keep the BQL and `bsrch` request tests.
7. Fix the tests the new loop breaks because they use `MagicMock` messages, for example
   `tests/unit/test_bquery_internals.py::test_send_request_writes_debug_file`: script
   them with `fake_bq([answer({...})], debug=True)`.

**Done when.** Offline tests pass on both dependency sets. Live: `pytest --bbg` passes.
Live: this prints `True`:

```python
with BQuery(timeout=1) as bq:  # 1 ms: the first call must time out
    try:
        bq.bql("get(px_last(dates=range(-1Y, 0D))) for(members('SPX Index'))")
    except TimeoutError:
        pass
    bq.timeout = 32_000
    print(bq.bdp(["IBM US Equity"], ["NAME"])["security"].to_list() == ["IBM US Equity"])
```

Changelog: new `BloombergError`; error messages include the category; a failed request
or a lost session is reported at once instead of as a timeout.

### Step 3: session lifecycle

Fixes F03, F04, F05 (decision D4).

**Do.** In `plbbg.py`:

```python
# in __init__ (and delete the class-level annotation at line 346)
self.session: blpapi.Session | None = None
self._open_services: set[str] = set()


def start(self) -> "BQuery":
    """Connect to Bloomberg; ``with BQuery() as bq`` calls this for you."""
    options = blpapi.SessionOptions()
    options.setServerHost(self.host)
    options.setServerPort(self.port)
    session = blpapi.Session(options)
    if not session.start():
        raise ConnectionError("Failed to start Bloomberg session.")
    self.session = session
    return self


def stop(self) -> None:
    """Disconnect; safe to call more than once."""
    if self.session is not None:
        self.session.stop()
        self.session = None
        self._open_services.clear()


def __enter__(self) -> "BQuery":
    return self.start()


def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.stop()


def _get_service(self, name: str) -> blpapi.Service:
    """Return a service, opening it the first time it is needed."""
    if self.session is None:
        raise ConnectionError(
            "Bloomberg session is not started. "
            "Use 'with BQuery() as bq:' or call bq.start()."
        )
    if name not in self._open_services:
        if not self.session.openService(name):
            raise ConnectionError(f"Failed to open service {name}.")
        self._open_services.add(name)
    return self.session.getService(name)
```

Use `self._get_service(...)` in the four request builders instead of
`self.session.getService(...)` (`plbbg.py:782`, `817`, `845`, `868`). Give `start` and
`stop` full docstrings with an example. Move the "Raises ConnectionError" note from the
`__init__` docstring to `start`. Remove the done item from `ROADMAP.md`.

**Tests.** In `tests/unit/test_bquery_internals.py`, replace the three parametrized
`test_enter_open_service_failure` cases and add:

- `test_bdp_without_started_session_raises_connection_error`
- `test_start_and_stop_work_without_a_with_block` (patch `blpapi.Session` as the
  existing `test_enter_start_failure` does)
- `test_stop_twice_is_safe`
- `test_bdp_opens_only_the_refdata_service` (`bq.session.opened == ["//blp/refdata"]`)
- `test_service_is_opened_once_for_two_calls`
- `test_bql_raises_connection_error_when_its_service_cannot_be_opened`
  (`FakeSession(unavailable={"//blp/bqlsvc"})`)
- `test_session_is_stopped_when_a_call_fails_inside_a_with_block`

**Done when.** Offline tests pass; `pytest --bbg` passes; on the Terminal,
`bq = BQuery().start(); bq.bdp(["IBM US Equity"], ["NAME"]); bq.stop()` works.

Changelog: `start()` and `stop()`; services open on first use.

### Step 4: debug dump, recordings, contract tests, live checks

Fixes F30, completes F31, and settles every finding that needs Bloomberg.

**4.1 One debug dump that can be replayed.** In `plbbg.py`:

- `__init__`: accept `debug: bool | str | os.PathLike = False`. Keep `self.debug`. Add
  `self._debug_dir`: `"debug_cases"` for `True`, the given path for a path, else `None`.
- Add these module-level functions (run offline on both dependency sets; they need
  `time` in the `from datetime import ...` line):

  ```python
  _EVENT_NAMES = {
      getattr(blpapi.Event, name): name
      for name in ("PARTIAL_RESPONSE", "RESPONSE", "REQUEST_STATUS")
  }


  def _json_default(value: Any) -> Any:
      """json.dump hook: write dates so that a dump can be replayed with its types."""
      if isinstance(value, datetime):
          return {"$datetime": value.isoformat()}
      if isinstance(value, date):
          return {"$date": value.isoformat()}
      if isinstance(value, time):
          return {"$time": value.isoformat()}
      return str(value)


  def _write_debug_dump(directory: str, request: blpapi.Request, events: list) -> None:
      """Write one request and the messages it received to a replayable JSON file."""
      element = request.asElement()
      dump = {
          "format": 1,
          "recorded": {
              "date": date.today().isoformat(),
              "blpapi": blpapi.__version__,
              "polars": pl.__version__,
          },
          "request": {"operation": str(element.name()), "body": element.toPy()},
          "events": [event for event in events if event["messages"]],
      }
      stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
      path = os.path.join(directory, f"{element.name()}_{stamp}.json")
      try:
          os.makedirs(directory, exist_ok=True)
          with open(path, "w", encoding="utf-8") as f:
              json.dump(dump, f, default=_json_default, indent=1, ensure_ascii=False)
              f.write("\n")
      except OSError as error:
          logger.warning("Could not write debug dump %s: %s", path, error)
  ```

- In `_send_request`: start with `events: list[dict] = []`. After the `TIMEOUT` check
  add `received = {"type": _EVENT_NAMES.get(event.eventType(), "OTHER"), "messages": []}`
  and `events.append(received)`. Right after `payload = msg.toPy()` add
  `received["messages"].append({"type": str(msg.messageType()), "payload": payload})`.
  Replace the old debug block with a `finally:` clause on the existing `try`:
  `if self._debug_dir: _write_debug_dump(self._debug_dir, request, events)`. The dump is
  then also written when the request fails, which is when it is needed most.
- Delete `_save_debug_case` (`plbbg.py:1213-1238`), the debug branch in `_parse_result`
  (`plbbg.py:1187-1189`) and their two tests (`test_parse_result_saves_debug_case`,
  `test_save_debug_case_writes_file`).
- Tests: `test_debug_dump_contains_request_and_events`,
  `test_debug_dump_is_written_when_the_request_fails`,
  `test_debug_dump_replays_to_the_same_frame` (dump a `bdh` call, load the file with
  `support._decode`, replay, compare frames),
  `test_unwritable_debug_directory_does_not_fail_the_request`.

**4.2 Scenario list.** Create `tests/recordings.py` with the scenarios of section 8 as
plain data:

```python
SCENARIOS = {
    "bdp/invalid_security_mixed": lambda bq: bq.bdp(["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"]),
    "bdh/weekend_range_no_rows": lambda bq: bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 6), date(2024, 1, 7)),
    ...
}
VOLATILE = {"bdib/trade_bars_60min", ...}   # data differs on every recording
```

**4.3 Record script.** Create `tests/record.py`. `python tests/record.py [prefix ...]`
records the scenarios whose names start with a prefix (all without arguments):

1. Open one `BQuery(debug=<temporary directory>)`.
2. For each scenario: empty the directory, call the lambda, keep the result or the
   exception it raised, and read the single dump file with a plain `json.load` (the
   `$date` markers stay as they are). If the call raised before anything was sent,
   there is no dump file; write the recording with an empty `events` list.
3. Add `"scenario"`, `"call"` (the source line of the lambda) and `"expected"`:
   - DataFrame: `{"schema": {column: str(dtype)}, "rows": [list(row) for row in df.rows()]}`;
     leave out `rows` when the scenario is in `VOLATILE`;
   - `BqlResult`: `{"names": [...], "frames": [one such object per frame]}`;
   - exception: `{"raises": type(error).__name__, "message": str(error)}`.
4. BQL only: replace each payload string by `{"$json": <parsed JSON>}` so diffs are
   readable, and delete the keys that differ on every run (`clientContext`,
   `responseTiming`, `payloadId`, `versionInfo`). The decoder in `tests/support.py`
   turns `$json` back into a string.
5. Write `tests/data/recordings/<scenario>.json` with
   `json.dump(..., default=_json_default, indent=1, ensure_ascii=False)` and a final
   newline.

Add `--refresh-expected`: for each recording, replay its events offline, run the lambda
and rewrite only `expected`. Later steps use it when a fix changes an output on purpose.

**4.4 Record.** Run `python tests/record.py` and commit the files. Check that no file
contains `serverId` and that none is much larger than 20 kB. Run the "live only" checks
of section 8 by hand; do not commit their output.

**4.5 Replay test.** `tests/unit/test_recordings.py`, one parametrized test over
`SCENARIOS` named `test_recording_replays_to_expected_result`: `bq, rec = replay(name)`,
run the lambda, then compare. For a frame: the schema as `{c: str(t)}`, and the rows
with `polars.testing.assert_frame_equal(df, pl.DataFrame(rows, schema=df.schema, orient="row"))`
(this treats NaN as equal; checked on both Polars versions). For a `BqlResult`: the
names, then each frame in the same way. For `raises`: the exception type name and
message.

**4.6 Contract tests.** `tests/integration/test_contract.py`, marked `requires_bbg`.
For each scenario marked C in section 8: run it live with a debug directory, and compare the *shape* of the fresh events with the committed recording.
Shape means the payload with every value replaced by its type name and every list
replaced by the sorted set of its distinct element shapes; put that helper (about 15
lines) in `tests/support.py`. For scenarios that are not volatile, also compare the
result with `expected`. These tests show when Bloomberg changes a response format.

**4.7 Live checks.** Look at the recordings and fill in the Result column. Then follow
the instruction in the last column in the step named.

| # | Finding | Look at | Result | What to do |
|---|---|---|---|---|
| L1 | F09, F10 | `bdp/invalid_security_mixed`, `bdp/invalid_field_mixed`, `bdh/invalid_security_mixed`, `bdh/invalid_field_mixed`: the names and nesting of `securityError` and `fieldExceptions` | Names and nesting as in section 5: `securityError` and `fieldExceptions[].errorInfo`, each with source, code, category, message, subcategory. `bdh` with an invalid field gives a `fieldExceptions` entry (subcategory `NOT_APPLICABLE_TO_HIST_DATA`), not a `responseError`. The invalid security is a null row in `bdp` and has no rows in `bdh` (F09 confirmed). | If they differ from section 5, use the real names in step 5. If `bdh` with an invalid field returns a `responseError` instead, that scenario's `expected` is `raises`; nothing more to do. |
| L2 | F11 | `bdp/lowercase_field`, `bdh/lowercase_field`: the keys of `fieldData` | Keys spelled as the user typed (`name`, `Crncy`, `px_last`), with values. F11 ruled out. | Keys spelled as the user typed (`name`): F11 is ruled out. Keys in Bloomberg's spelling (`NAME`): implement the case-insensitive lookup in step 5. |
| L3 | F15 | live only: `bdp/override_reference_date` with `"2020-07-15"` instead of `"20200715"` | Ignored silently: `"2020-07-15"` returns today's settle date (2026-11-18) with no error or field exception; `"20200715"` returns 2020-07-17. | Note whether Bloomberg rejects, ignores or accepts it. Step 6 formats `date` objects as `YYYYMMDD` in every case. |
| L4 | F19, F21 | `bql/invalid_security_mixed`, `bql/valid_and_invalid_item`, `bql/invalid_item`: the top-level `responseExceptions` and `results`; each item's `partialErrorMap` and `responseExceptions` | `invalid_security_mixed`: no exception at any level; the security gets a null value. `invalid_item` and `valid_and_invalid_item`: `results` is null, one top-level exception (`PARTIAL`, `BQL_SYNTAX_ERROR`), so a valid item is lost with an invalid one. Item-level `partialErrorMap` is `{"errorIterator": null}` and `responseExceptions` is `[]`: F21 ruled out. `empty_universe` gives a top-level exception of type `BENIGN` ("No results match ..."), today logged as an error. | Step 7 keeps results that arrive with exceptions in every case. If the item-level keys hold messages, implement the F21 warning in step 7; if they stay empty, F21 is ruled out. |
| L5 | F23 | all `bql/*` recordings: the set of `type` strings; any `DATE` value whose time is not `T00:00:00Z`; `INT` values written as `5.0` | Types seen: STRING, DOUBLE, DATE, BOOLEAN, INT; no new names. Every DATE value ends in `T00:00:00Z`. INT arrives as `2`, not `2.0`. | New type names: add them to the mapping in step 7 if the Polars type is obvious, else leave them inferred. `DATE` values with a time of day: do not change the mapping; report to the maintainer. `INT` as `5.0`: convert whole floats to `int` in step 7. |
| L6 | F24 | `bql/item_named_id`, `bql/item_named_date`: the item names in `results` | `get(ID)`: item `ID`; the frame has one column `ID`, so the item replaced the ID column (F24 confirmed). `#DATE`: item `#DATE` with secondary columns `DATE`, `CURRENCY`; no clash. | Note them. Step 7 renames clashing columns in every case. |
| L7 | F25 | live only: `bq.bql("get(px_last(dates=range(-5Y, 0D))) for(members('SPX Index'))")` with `debug=`; count the messages and try `json.loads` on each payload | 12 messages (11 `PARTIAL_RESPONSE`, 1 `RESPONSE`), each a 4 MiB fragment of one JSON text; none parses alone. Joined, they parse (918,981 rows). Today: 12 "Failed to decode JSON" errors that each log the multi-MB fragment, and an empty `BqlResult`. | One message: F25 is ruled out; add a comment in `_extract_results`. Several messages, each valid JSON: concatenate tables of the same item (step 7). Fragments of one JSON text: join the strings before parsing (step 7). |
| L8 | F27, F28 | `bsrch/coco_limit_2`, `bsrch/invalid_domain`, `bsrch/bi_template_limit_3`: the key that holds the grid; the keys of each `DataFields` entry; where the error text is; the value of `ReachMax` | The payload is the grid itself (message type `GridResponse`): keys `NumOfFields`, `NumOfRecords`, `ColumnTitles`, `DataRecords`, `ReachMax`, `SequenceNumber`, and `Error` on failure. No `GridResponse` key, so only the root-level fallback is used. Each `DataFields` entry has one key, `StringValue` or `DoubleValue`. `ReachMax` is true also with `LIMIT`; without `LIMIT`: one event, 1022 rows, `ReachMax` true. The step 1 `bsrch` tests use the unreal `GridResponse` wrapper; step 8 must update them. | Step 8 reduces the parser to the keys seen here. |
| L9 | F17 | `bdib/trade_bars_60min`: is `time` a naive or a timezone-aware `datetime`? | Naive `datetime` holding UTC (08:00 = OMX open). A naive, a UTC-aware and a CET-aware request for the same hour give equal frames. | Note it; step 11 documents it. |
| L10 | – | `bdp/partial_responses`: number of events; row order against request order | 3 events (2 `PARTIAL_RESPONSE`, 1 `RESPONSE`). Rows come back out of request order (WMT ... DIS, then AAPL ... JNJ) and the order changes between runs; `sequenceNumber` is the position in the request. Scenario marked volatile. | Note it. If the order differs from the request, mention it in the `bdp` docstring. |

**Done when.** `pytest` passes offline with the recordings on both dependency sets;
`pytest --bbg` passes including the contract tests; the table above is filled in; a
second `python tests/record.py` on the same day changes only volatile scenarios.
Replace `tests/README.md` with:

```
Offline tests (no Bloomberg):   pytest
Live tests (Terminal needed):   pytest --bbg

Offline tests replay recorded Bloomberg answers from tests/data/recordings/.
One file is one scenario: the request, the raw messages, the expected result.
tests/recordings.py lists the scenarios; tests/support.py holds the fake session.

Re-record (Terminal needed):    python tests/record.py            # all scenarios
                                python tests/record.py bdp/ bql/syntax_error
After an intended output change (no Terminal needed):
                                python tests/record.py --refresh-expected
Review `git diff tests/data/recordings` before committing.
```

Changelog: `debug=` accepts a directory; new dump format; the BQL-only dump is gone.

### Step 5: `bdp` and `bdh` results

Fixes F09, F10, F12, F13, and F11 if L2 confirmed it (decisions D1, D9).

**Tests first** (all but `test_bdp_valid_request_logs_nothing` fail before the fix):

- `test_bdh_weekend_range_returns_empty_frame_with_columns`: replay
  `bdh/weekend_range_no_rows`; expect shape `(0, 3)` and columns
  `security, date, PX_LAST`.
- `test_bdp_field_first_seen_after_100_rows_is_not_lost`: scripted answer with 100
  securities whose `fieldData` is `{}` and a 101st with `{"DVD_SH_LAST": 2.5}`. Today
  this raises a Polars `ComputeError` that mentions `infer_schema_length`.
- `test_bdp_invalid_security_gives_null_row_and_warning` (replay
  `bdp/invalid_security_mixed`, use `caplog`): the row is still there with nulls and
  the log names the security.
- `test_bdp_invalid_field_gives_null_column_and_warning`,
  `test_bdh_invalid_security_gives_no_rows_and_warning`.
- `test_bdp_valid_request_logs_nothing`.
- If L2 confirmed F11: `test_bdp_lowercase_field_returns_values`.

**Do.**

1. Build both frames with named columns (F12, F13):

   ```python
   # bdp, plbbg.py:459
   schema = {"security": pl.String, **{field: None for field in fields}}
   return pl.DataFrame(data, schema=schema, infer_schema_length=None)
   # bdh, plbbg.py:529: the same with "date": pl.Date after "security"
   ```

   Say in both docstrings that field columns of an empty result have dtype `Null`.
2. Report item errors (F09, F10). Add one module-level function and call it from both
   parsers (`plbbg.py:914-942`); each parser collects the lines and logs once:

   ```python
   def _item_errors(security_data: dict) -> list[str]:
       """Describe the errors Bloomberg reported for one securityData element."""
       name = security_data.get("security")
       lines = []
       if error := security_data.get("securityError"):
           lines.append(f"{name}: {error.get('message')} ({error.get('category')})")
       for exception in security_data.get("fieldExceptions") or []:
           info = exception.get("errorInfo") or {}
           lines.append(
               f"{name} / {exception.get('fieldId')}: "
               f"{info.get('message')} ({info.get('category')})"
           )
       return lines
   ```

   Log with `logger.warning`: one record per public call, at most 10 lines followed by
   "... and N more". Python prints warnings to stderr when the user has not configured
   logging, so notebook users see it. The DataFrame does not change.
3. Only if L2 confirmed F11: look fields up without regard to case and keep the user's
   spelling as the column name:
   `by_upper = {key.upper(): value for key, value in field_data.items()}`, then
   `record[field] = by_upper.get(field.upper())`, in both parsers.
4. Run `python tests/record.py --refresh-expected`. The diff must show only empty
   results gaining columns (and F11 values, if it applied).

**Done when.** Offline tests pass on both dependency sets; `pytest --bbg` passes.

Changelog: empty results have columns; warnings for invalid securities and fields.

### Step 6: inputs

Fixes F14, F15, F16 and the `date` crash of F18.

**Do.** Add three module-level helpers and use them in the public methods and request
builders:

```python
def _as_list(value: str | Sequence[str]) -> list[str]:
    """Accept one name or several."""
    return [value] if isinstance(value, str) else list(value)


def _override_items(overrides: Mapping[str, Any] | Sequence | None) -> list[tuple]:
    """Return overrides as (name, value) pairs; accept a dict or a sequence of pairs."""
    if not overrides:
        return []
    pairs = overrides.items() if isinstance(overrides, Mapping) else overrides
    items = []
    for pair in pairs:
        if isinstance(pair, str) or len(pair) != 2:  # noqa: PLR2004
            raise TypeError(
                "overrides must be a dict or a sequence of (name, value) pairs"
            )
        name, value = pair
        if value is None:
            raise TypeError(f"override {name!r} has no value")
        if isinstance(value, date):  # also covers datetime
            value = value.strftime("%Y%m%d")
        items.append((name, value))
    return items


def _format_date(value: date | str) -> str:
    """Return a date as Bloomberg's YYYYMMDD; accept YYYYMMDD and ISO strings."""
    if isinstance(value, str):
        return value.replace("-", "")
    return value.strftime("%Y%m%d")
```

- `bdp`, `bdh`: `securities = _as_list(securities)`, `fields = _as_list(fields)` as the
  first lines; type hints `str | Sequence[str]` (F14).
- The three override loops (`plbbg.py:796-801`, `821-826`, `853-858`) iterate over
  `_override_items(overrides)`. `bsrch` keeps its `str(name)`, `str(value)`. Type hint
  on all four methods: `Mapping[str, Any] | Sequence[tuple[str, Any]] | None` (F15).
- `bdh` uses `_format_date` for both dates (F16). `bdp`/`bdh` raise
  `ValueError("'security' and 'date' cannot be requested as fields")` when `fields`
  contains one of the two key column names that the method uses (F16).
- `_format_datetime` (`plbbg.py:1205-1211`): treat a plain `date` as midnight (F18).

**Tests** (on real requests, `bq.session.sent[0].asElement().toPy()`):
`test_bdp_accepts_one_security_and_one_field_as_strings`,
`test_overrides_accept_a_dict`, `test_overrides_accept_pairs`,
`test_override_date_is_sent_as_yyyymmdd`, `test_override_without_value_raises_type_error`,
`test_two_character_override_string_raises_type_error`,
`test_bdh_accepts_iso_date_strings`, `test_bdib_accepts_a_date`,
`test_bdp_field_named_security_raises_value_error`.

**Done when.** Offline tests pass; `pytest --bbg` passes.

Changelog: strings accepted for single securities and fields; overrides as dict or
pairs everywhere; `date` values in overrides sent as `YYYYMMDD`; `None` as an override
value raises `TypeError` (it used to be sent as the text `None`).

### Step 7: BQL

Fixes F19, F20, F22, F23, F24, and F21, F25 if L4, L7 confirmed them (decision D2).
Make one commit per finding, test first. Use the recordings where one fits and a
scripted JSON string otherwise.

1. **F22.** In `_parse_result` (`plbbg.py:1173-1183`) read the three parts with
   `content.get("idColumn") or {}`, `content.get("valuesColumn") or {}`,
   `content.get("secondaryColumns") or []`, and every type with `.get("type") or "STRING"`.
   Line 1183 today passes the Python type `str` as the default, which crashes with
   `TypeError: unbound method str.upper() needs an argument`. Skip an item that is
   `None`. Tests: `test_bql_secondary_column_without_type_is_string`,
   `test_bql_item_with_null_columns_does_not_crash`.
2. **F19, F20.** Make `_extract_results` return `(results, errors)`: collect the
   messages into `errors`, keep logging them with `logger.error`, and drop the
   `continue` at line 1159 so that `results` is kept when it is not empty. Treat a
   payload that is not valid JSON as an error message too. Add
   `errors: list[str] = field(default_factory=list)` as the last field of `BqlResult`,
   fill it in `bql()`, and make `combine()` on an empty result raise
   `ValueError("No DataFrames to combine. BQL error: ...")` when there are errors.
   Update the tests that call `_extract_results` directly. Tests:
   `test_bql_response_with_results_and_exceptions_keeps_results`,
   `test_bql_syntax_error_is_available_in_result_errors`,
   `test_combine_after_failed_query_reports_the_bql_error`.
3. **F23.**
   - `_map_types`: return `None` (let Polars infer) for a type name that is not mapped,
     and log a warning that names it; map with `(t or "STRING").upper()`.
   - `_apply_schema`: parse dates with `datetime.fromisoformat(v).date()`. Move the
     nested `_convert_number` to module level. Apply what L5 found.
   - `bql()`: wrap the DataFrame construction and re-raise as
     `BloombergError(f"Cannot build a DataFrame for BQL item {table.name!r}: {error}")`.
   - Tests: `test_bql_unknown_value_type_is_inferred`,
     `test_bql_date_with_fractional_seconds_is_parsed`,
     `test_bql_item_that_cannot_be_built_names_the_item`.
4. **F24.** In `_parse_result`, when a column name is already taken, use `name_1`,
   `name_2`, ... and log a warning. Today an item named `ID`, or named like one of its
   secondary columns, silently replaces the other column. Test:
   `test_bql_item_named_like_secondary_column_keeps_both_columns`.
5. **F21, F25.** Only as far as L4 and L7 confirmed them.
6. Run `python tests/record.py --refresh-expected` and review the diff.

**Done when.** Offline tests pass on both dependency sets; `pytest --bbg` passes
(`tests/integration/test_integration_bql.py::test_incorrect_bql_syntax_logs_error` still holds).

Changelog: `BqlResult.errors`; results kept when BQL also reports exceptions; unknown
value types no longer fail the query.

### Step 8: `bsrch`

Fixes F27, F28 and the `bsrch` part of F13. Base every change on L8.

1. Make `_parse_bsrch_responses` return `(rows, columns)`, where `columns` is the list
   of column names it used (the titles, plus any `col_<n>` it had to invent), and build
   the frame with
   `pl.DataFrame(rows, schema=columns or None, infer_schema_length=None, strict=False)`,
   so an empty search keeps its columns (checked: Polars accepts a list of names with
   dict rows and with no rows, and still infers the dtypes).
2. Make column titles unique before use (`Name`, `Name_1`); today the second of two
   equal titles overwrites the first.
3. When an `Error` text arrives and rows exist, log it as a warning. Today it is
   dropped (`plbbg.py:990-992`, `1007-1008`). Raise `BloombergError` where `ValueError`
   is raised now.
4. `_coerce_bsrch_numeric_columns` (`plbbg.py:1069`): collect the column names from all
   rows, not only the first.
5. Reduce `key_order` in `_extract_bsrch_field_value` (`plbbg.py:1027-1043`) and the
   root-level fallback (`plbbg.py:979-982`) to what the recordings show; log a warning
   for an unknown key. Remove unit tests that pin spellings Bloomberg does not use.

Tests: `test_bsrch_empty_result_keeps_column_titles`,
`test_bsrch_duplicate_titles_get_a_suffix`,
`test_bsrch_error_next_to_rows_is_logged`,
`test_bsrch_column_missing_in_first_row_is_still_coerced`, and the replay of the three
`bsrch` recordings.

**Done when.** Offline tests pass on both dependency sets; `pytest --bbg` passes.

### Step 9: clean-up

No behavior change. Do not edit any assertion in this step.

1. Delete tests that the replay tests replace: the four `*_calls_components` tests,
   `test_bdh_leading_nulls`, `test_bdib_returns_dataframe`. Delete the old fixtures
   under `tests/data/bdh/`, `tests/data/bdib/` and `tests/data/bql/` once a recording
   covers the same case; convert to the new format any that cannot be re-recorded.
2. Remove parser code for messages that can no longer arrive, each only if the
   recordings confirm it is dead: the "skip non-dict responses" branch
   (`plbbg.py:1145-1147`) and the nested `barTickData` fallback (`plbbg.py:953`).
3. Order `plbbg.py` the way a call runs: `BloombergError`, `SITable`, `BqlResult`;
   module-level helpers; `BQuery` lifecycle; `_send_request`; then one block per method
   in the order `bdp`, `bdh`, `bdib`, `bql`, `bsrch`, each with its public method, its
   request builder and its parser under a comment line such as `# --- bdh ---`.
4. Add the missing type hints (`__iter__`, `_parse_bql_responses`, the parsers).

**Done when.** All tests pass unchanged; searching `plbbg.py` for `bdh` leads to one
contiguous block.

### Step 10: tooling and CI

Fixes F35, F36.

1. `polars_bloomberg/__init__.py`: one source for the version.

   ```python
   from importlib.metadata import PackageNotFoundError, version

   try:
       __version__ = version("polars-bloomberg")
   except PackageNotFoundError:  # running from a source tree that is not installed
       __version__ = "0+unknown"
   ```

   Test: `test_version_matches_package_metadata`.
2. Use ruff only. Remove `[tool.black]` from `pyproject.toml` and the `black` and
   `mypy` installs and calls from `noxfile.py`. Run `ruff format .` once, in a commit
   of its own. Add `ruff format --check .` after `ruff check .` in
   `.github/workflows/python-package.yml` and in the nox `ruff` session; make that
   session run `ruff check .` like CI.
3. In `noxfile.py`, the `tests` session runs plain `pytest` (safe since step 1).
4. Correct the comments on the rule groups `A`, `R`, `G` in `pyproject.toml`
   (`A` is flake8-builtins).

**Done when.** `ruff check .`, `ruff format --check .` and `pytest` pass locally; CI is
green on the pull request.

### Step 11: documentation

Fixes F08, F17, F26, F29, F34 and documents the changes of steps 2-8.

1. README: add a short section "Errors, warnings and empty results": which exception
   each failure raises (`BloombergError`, `TimeoutError`, `ConnectionError`); invalid
   securities and fields give nulls and a warning; what an empty result looks like;
   what `timeout` means; `debug=`; `start()`/`stop()`; one `BQuery` must not be used
   from several threads at once.
2. One sentence on each `docs/usage/` page about errors and empty results.
3. Fix these (F34):
   - `README.md:62`, `112`: the method is `bdib`, not `bdip`.
   - `README.md:293`: remove the stray comment about `debug`.
   - `README.md:324`, `docs/usage/bdib.md:10`, `plbbg.py:554-557`: `time` is a naive
     datetime holding UTC (F17, L9).
   - `docs/usage/bdib.md:59-60`: describe the real error for a bad interval (see
     `bdib/interval_out_of_range`).
   - `plbbg.py:83`, `577`, `664`: wrong `shape:` lines in docstring examples;
     `plbbg.py:657-671`: the example asks for IBM and shows AAPL; `plbbg.py:181-195`:
     the "no common columns" example cannot raise.
   - `plbbg.py:425-427`, `485-487`, `737-738`: name `BloombergError` in the Raises
     sections.
   - `plbbg.py:752-764` (F29): the example passes `LIMIT` and shows the limit warning,
     which the code suppresses when `LIMIT` is given.
4. `combine()` (F26): explain in its docstring and in `docs/usage/bql.md` that without
   `on` it joins on every shared column, that rows whose shared columns hold nulls do
   not match and appear twice, and that `combine(on="ID")` gives one row per security.
   Use `combine(on="ID")` in README examples that mean one row per security.
5. Complete `CHANGELOG.md`.

**Done when.** `mkdocs build` works; every example output that changed was re-run on
the Terminal.

## 8. Scenario catalogue

Use these names and calls in `tests/recordings.py`. "V" marks volatile scenarios (store
the schema only). "C" marks scenarios used by the contract tests. Where a call is marked
*(find)*, spend at most ten minutes finding an instrument or item that produces the
described situation; if none is found, leave the scenario out and say so in the pull
request.

Dates for `bdib` must be relative to the recording day (assumed: intraday history
reaches back about 140 days): `d` is the last weekday before today, `sat` the last
Saturday.

| Scenario | Call | Flags |
|---|---|---|
| `bdp/static_fields_two_securities` | `bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME", "CRNCY", "EQY_INIT_PO_DT"])` | C |
| `bdp/invalid_security_mixed` | `bq.bdp(["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"])` | C |
| `bdp/invalid_security_only` | `bq.bdp(["NOT A SECURITY Equity"], ["NAME"])` | |
| `bdp/invalid_field_mixed` | `bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])` | C |
| `bdp/field_not_applicable` | `bq.bdp(["IBM US Equity"], ["NAME", "YAS_ZSPREAD"])` | |
| `bdp/lowercase_field` | `bq.bdp(["IBM US Equity"], ["name", "Crncy"])` | |
| `bdp/bulk_field_with_overrides` | `bq.bdp(["IBM US Equity"], ["DVD_HIST"], overrides=[("DVD_START_DT", "20240101"), ("DVD_END_DT", "20241231")])` | |
| `bdp/bulk_field_empty_for_one_security` | the same with `["IBM US Equity", "BRK/A US Equity"]` | |
| `bdp/override_reference_date` | `bq.bdp(["USDSEK Curncy"], ["SETTLE_DT"], overrides=[("REFERENCE_DATE", "20200715")])` | |
| `bdp/invalid_override` | `bq.bdp(["IBM US Equity"], ["NAME"], overrides=[("NOT_AN_OVERRIDE", "1")])` | |
| `bdp/no_securities` | `bq.bdp([], ["NAME"])` | |
| `bdp/partial_responses` | `bq.bdp([t + " US Equity" for t in "AAPL MSFT AMZN GOOGL META NVDA TSLA JPM V JNJ WMT PG MA HD XOM CVX KO PEP BAC PFE CSCO ORCL INTC IBM DIS".split()], ["CRNCY"])` | |
| `bdh/two_securities_two_fields` | `bq.bdh(["OMX Index", "SPX Index"], ["PX_LAST", "PX_VOLUME"], date(2024, 1, 2), date(2024, 1, 5))` | C |
| `bdh/weekend_range_no_rows` | `bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 6), date(2024, 1, 7))` | |
| `bdh/invalid_security_mixed` | `bq.bdh(["OMX Index", "NOT A SECURITY Equity"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 5))` | |
| `bdh/invalid_field_mixed` | `bq.bdh(["OMX Index"], ["PX_LAST", "NOT_A_FIELD"], date(2024, 1, 2), date(2024, 1, 5))` | |
| `bdh/reference_only_field` | `bq.bdh(["OMX Index"], ["PX_LAST", "NAME"], date(2024, 1, 2), date(2024, 1, 5))` | |
| `bdh/different_holiday_calendars` | `bq.bdh(["OMX Index", "SPX Index"], ["PX_LAST"], date(2024, 1, 12), date(2024, 1, 16))` (15 January 2024 was a US holiday) | |
| `bdh/periodicity_monthly` | `bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 1), date(2024, 3, 31), options={"periodicitySelection": "MONTHLY"})` | |
| `bdh/start_after_end` | `bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 5), date(2024, 1, 2))` | |
| `bdh/lowercase_field` | `bq.bdh(["OMX Index"], ["px_last"], date(2024, 1, 2), date(2024, 1, 5))` | |
| `bdib/trade_bars_60min` | `bq.bdib("OMX Index", "TRADE", 60, d 08:00, d 12:00)` | V, C |
| `bdib/saturday_no_bars` | `bq.bdib("OMX Index", "TRADE", 60, sat 08:00, sat 12:00)` | V |
| `bdib/invalid_security` | `bq.bdib("NOT A SECURITY Equity", "TRADE", 60, d 08:00, d 12:00)` | |
| `bdib/interval_out_of_range` | `bq.bdib("OMX Index", "TRADE", 2000, d 08:00, d 12:00)` | |
| `bql/px_last_fixed_date` | `bq.bql("get(px_last(dates=2024-01-02)) for(['IBM US Equity', 'SEBA SS Equity'])")` | C |
| `bql/two_items` | `bq.bql("get(name, px_last(dates=2024-01-02)) for(['IBM US Equity'])")` | |
| `bql/time_series` | `bq.bql("get(px_last(dates=range(2024-01-02, 2024-01-05))) for(['IBM US Equity'])")` | |
| `bql/syntax_error` | `bq.bql("get(px_last for(['IBM US Equity'])")` | C |
| `bql/invalid_security_mixed` | `bq.bql("get(px_last(dates=2024-01-02)) for(['IBM US Equity', 'NOT A SECURITY'])")` | |
| `bql/invalid_item` | `bq.bql("get(not_a_real_field) for(['IBM US Equity'])")` | |
| `bql/valid_and_invalid_item` | `bq.bql("get(name, not_a_real_field) for(['IBM US Equity'])")` | |
| `bql/empty_universe` | `bq.bql("get(name) for(filter(members('OMX Index'), px_last < 0))")` | |
| `bql/boolean_value` | `bq.bql("get(px_last(dates=2024-01-02) > 100) for(['IBM US Equity'])")` *(find)* | |
| `bql/date_value` | `bq.bql("get(nxt_call_dt()) for(['USX60003AC87 Corp'])")` *(find)* | |
| `bql/int_value` | an item whose `type` is `INT`, for example `count(group(...))` *(find)* | |
| `bql/nan_and_infinity` | `bq.bql("let(#p = 1 / 0; #m = -1 / 0; #n = 0 / 0;) get(#p, #m, #n) for('OMX Index')")` | |
| `bql/item_named_id` | `bq.bql("get(ID) for(['IBM US Equity'])")` | |
| `bql/item_named_date` | `bq.bql("let(#DATE = px_last(dates=2024-01-02);) get(#DATE) for(['IBM US Equity'])")` | |
| `bql/apostrophe_in_value` | `bq.bql("get(name) for(['BFOR US Equity'])")` | |
| `bsrch/coco_limit_2` | `bq.bsrch("FI:SRCHEX.@COCO", {"LIMIT": 2})` | V, C |
| `bsrch/invalid_domain` | `bq.bsrch("FI:NOT_A_SEARCH")` | |
| `bsrch/bi_template_limit_3` | `bq.bsrch("BI:TPD", {"BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR", "LIMIT": 3})` | V |

Live only, not committed:

- L3 and L7 of step 4.
- `bq.bsrch("FI:SRCHEX.@COCO")` without a limit: does `ReachMax` become true, and how
  many events arrive?
- `bdib` for the same hour with a naive `datetime` and with the equal UTC-aware
  `datetime`: the two frames must be equal.

## 9. Out of scope

Do not do these. Each was considered and rejected, mostly to keep the code simple.

- Splitting `plbbg.py` into modules. The file has about 520 lines of code and 500 of
  docstrings; one file keeps "where is bdh parsed?" a single search.
- Using `blpapi.test` to replay recordings. It needs a schema entry for every field of
  every recording and schemas for two more services. It is used for the event loop and
  for request building only.
- Committing Bloomberg's real service schemas.
- A per-request `blpapi.EventQueue`. The correlation-ID filter does the same and also
  sees a terminated session.
- More than one exception class, response model classes, or a registry of parsers.
- A total timeout, retries, automatic splitting of large requests, caching, async or
  threaded requests.
- Validating securities or fields before sending. Bloomberg already reports them.
- Changing default outputs: wide `bdh` frames, timezone-aware `bdib` times, sorted
  `bdp` rows, a null-matching `combine()`.
- Raising for invalid securities or fields, or from `bql()` (decisions D1, D2).
- Adding mypy, a snapshot-testing plugin, or `hypothesis`.
- Altering recorded values.
- Authentication options for B-PIPE or SAPI.

## Appendix A: `tests/support.py`

Create this file in step 1 with exactly this content. It was run against the unchanged
library and against the new event loop of step 2, on both dependency sets. The
recording functions at the end are used from step 4 on.

```python
"""Test support: a scripted fake session, real blpapi test events, and recordings."""

import functools
import json
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import MagicMock

import blpapi
import blpapi.test

from polars_bloomberg import BQuery

DATA_DIR = Path(__file__).parent / "data"
OWN = object()  # marks a scripted message as the answer to the request being sent


class FakeMessage:
    """Stand-in for blpapi.Message that returns a given payload from toPy()."""

    def __init__(self, payload, message_type="Response", correlation_id=OWN):
        self.payload = payload
        self.message_type = blpapi.Name(message_type)
        self.correlation_id = correlation_id  # OWN, None (status message) or an id

    def toPy(self):
        return self.payload

    def messageType(self):
        return self.message_type

    def correlationIds(self):
        return [] if self.correlation_id is None else [self.correlation_id]

    # The next two methods are needed only by the old _send_request; delete in step 2.
    def hasElement(self, name):
        return isinstance(self.payload, dict) and name in self.payload

    def getElement(self, name):
        element = MagicMock()
        element.getElementAsString.side_effect = lambda key: self.payload[name][key]
        return element


class FakeEvent:
    """Stand-in for blpapi.Event."""

    def __init__(self, event_type, messages=()):
        self.event_type = event_type
        self.messages = list(messages)

    def eventType(self):
        return self.event_type

    def __iter__(self):
        return iter(self.messages)


class FakeSession:
    """Scripted stand-in for blpapi.Session.

    `events` are returned by nextEvent() in order; an exception instance in the list
    is raised instead. When the list is empty, nextEvent() returns a TIMEOUT event.
    """

    def __init__(self, events=(), unavailable=()):
        self.events = list(events)
        self.unavailable = set(unavailable)  # service names that fail to open
        self.sent, self.cancelled, self.opened = [], [], []
        self.stopped = False
        self.last_cid = None

    def start(self):
        return True

    def stop(self):
        self.stopped = True

    def openService(self, name):
        self.opened.append(name)
        return name not in self.unavailable

    def getService(self, name):
        return refdata_test_service() if name == "//blp/refdata" else MagicMock()

    def sendRequest(self, request, **kwargs):
        self.sent.append(request)
        self.last_cid = blpapi.CorrelationId(len(self.sent))  # 1, 2, 3, ...
        return self.last_cid

    def nextEvent(self, timeout=0):
        if not self.events:
            return FakeEvent(blpapi.Event.TIMEOUT)
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        if isinstance(event, FakeEvent):
            for message in event:
                if message.correlation_id is OWN:
                    message.correlation_id = self.last_cid
        return event

    def cancel(self, correlation_id):
        self.cancelled.append(correlation_id)


def fake_bq(events=(), **bquery_kwargs):
    """Return a BQuery whose session is a FakeSession scripted with `events`."""
    bq = BQuery(**bquery_kwargs)
    bq.session = FakeSession(events)
    return bq


def answer(*payloads):
    """One RESPONSE event of stand-in messages: the usual scripted answer."""
    return FakeEvent(blpapi.Event.RESPONSE, [FakeMessage(p) for p in payloads])


# --- real blpapi objects, built offline with blpapi.test ---------------------------


@functools.cache
def refdata_test_service():
    xml = (DATA_DIR / "schemas" / "refdata_test.xml").read_text(encoding="utf-8")
    return blpapi.test.deserializeService(xml)


def response_event(operation, payload, event_type=blpapi.Event.RESPONSE, cid=1):
    """A real blpapi.Event with one response message of `operation`.

    `cid` must equal the number of the request it answers (FakeSession numbers the
    requests 1, 2, 3, ...). Dates in `payload` are ISO strings; toPy() returns dates.
    """
    definition = (
        refdata_test_service()
        .getOperation(blpapi.Name(operation))
        .getResponseDefinitionAt(0)
    )
    properties = blpapi.test.MessageProperties()
    properties.setCorrelationIds([blpapi.CorrelationId(cid)])
    event = blpapi.test.createEvent(event_type)
    formatter = blpapi.test.appendMessage(event, definition, properties)
    formatter.formatMessageJson(json.dumps(payload))
    return event


def status_event(event_type, message_name, payload=None, cid=None):
    """A real status event, e.g. (REQUEST_STATUS, "RequestFailure", {...}, cid=1)."""
    definition = blpapi.test.getAdminMessageDefinition(blpapi.Name(message_name))
    properties = blpapi.test.MessageProperties()
    if cid is not None:
        properties.setCorrelationIds([blpapi.CorrelationId(cid)])
    event = blpapi.test.createEvent(event_type)
    formatter = blpapi.test.appendMessage(event, definition, properties)
    if payload is not None:
        formatter.formatMessageJson(json.dumps(payload))
    return event


# --- recordings (added in step 4) ---------------------------------------------------


def _decode(obj):
    """json object_hook: restore the typed values written by the debug dump."""
    if len(obj) == 1:
        ((key, value),) = obj.items()
        if key == "$date":
            return date.fromisoformat(value)
        if key == "$datetime":
            return datetime.fromisoformat(value)
        if key == "$time":
            return time.fromisoformat(value)
        if key == "$json":  # a BQL answer: Bloomberg sends it as one JSON string
            return json.dumps(value)
    return obj


def load_recording(name):
    path = DATA_DIR / "recordings" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"), object_hook=_decode)


def replay(name, **bquery_kwargs):
    """Return (bq, recording); bq answers its next request with the recording."""
    recording = load_recording(name)
    events = [
        FakeEvent(
            getattr(blpapi.Event, event["type"]),
            [FakeMessage(m["payload"], m["type"]) for m in event["messages"]],
        )
        for event in recording["events"]
    ]
    return fake_bq(events, **bquery_kwargs), recording
```

## Appendix B: `tests/data/schemas/refdata_test.xml`

A reduced, hand-written schema of `//blp/refdata` for tests. The schema format is not
documented; this file is accepted by `blpapi.test.deserializeService` in blpapi 3.24.2
and 3.26.10. A real-event test can only use fields that are declared here: to use
another field, add a line to `ReferenceFieldData` or `HistoricalFieldData`.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!-- Hand-written, reduced TEST schema. It is not Bloomberg's schema. -->
<ServiceDefinition name="blp.refdata" version="1.0.1.0">
  <service name="//blp/refdata" version="1.0.0.0">
    <operation name="ReferenceDataRequest" serviceId="84">
      <request>ReferenceDataRequest</request>
      <response>Response</response>
      <responseSelection>ReferenceDataResponse</responseSelection>
    </operation>
    <operation name="HistoricalDataRequest" serviceId="84">
      <request>HistoricalDataRequest</request>
      <response>Response</response>
      <responseSelection>HistoricalDataResponse</responseSelection>
    </operation>
    <operation name="IntradayBarRequest" serviceId="84">
      <request>IntradayBarRequest</request>
      <response>Response</response>
      <responseSelection>IntradayBarResponse</responseSelection>
    </operation>
  </service>
  <schema>
    <sequenceType name="ReferenceDataRequest">
      <element name="securities" type="String" maxOccurs="unbounded"/>
      <element name="fields" type="String" maxOccurs="unbounded"/>
      <element name="overrides" type="FieldOverride" minOccurs="0" maxOccurs="unbounded"/>
      <element name="returnEids" type="Boolean" minOccurs="0" maxOccurs="1"/>
    </sequenceType>
    <sequenceType name="HistoricalDataRequest">
      <element name="securities" type="String" maxOccurs="unbounded"/>
      <element name="fields" type="String" maxOccurs="unbounded"/>
      <element name="startDate" type="String" minOccurs="0" maxOccurs="1"/>
      <element name="endDate" type="String" minOccurs="0" maxOccurs="1"/>
      <element name="periodicitySelection" type="String" minOccurs="0" maxOccurs="1"/>
      <element name="overrides" type="FieldOverride" minOccurs="0" maxOccurs="unbounded"/>
    </sequenceType>
    <sequenceType name="IntradayBarRequest">
      <element name="security" type="String"/>
      <element name="eventType" type="String"/>
      <element name="interval" type="Int32"/>
      <element name="startDateTime" type="Datetime"/>
      <element name="endDateTime" type="Datetime"/>
      <element name="gapFillInitialBar" type="Boolean" minOccurs="0" maxOccurs="1"/>
      <element name="overrides" type="FieldOverride" minOccurs="0" maxOccurs="unbounded"/>
    </sequenceType>
    <sequenceType name="FieldOverride">
      <element name="fieldId" type="String"/>
      <element name="value" type="String"/>
    </sequenceType>
    <choiceType name="Response">
      <element name="ReferenceDataResponse" type="ReferenceDataResponseType"/>
      <element name="HistoricalDataResponse" type="HistoricalDataResponseType"/>
      <element name="IntradayBarResponse" type="IntradayBarResponseType"/>
    </choiceType>
    <choiceType name="ReferenceDataResponseType">
      <element name="responseError" type="ErrorInfo"/>
      <element name="securityData" type="ReferenceSecurityData" maxOccurs="unbounded"/>
    </choiceType>
    <choiceType name="HistoricalDataResponseType">
      <element name="responseError" type="ErrorInfo"/>
      <element name="securityData" type="HistoricalSecurityData"/>
    </choiceType>
    <choiceType name="IntradayBarResponseType">
      <element name="responseError" type="ErrorInfo"/>
      <element name="barData" type="BarData"/>
    </choiceType>
    <sequenceType name="ErrorInfo">
      <element name="source" type="String"/>
      <element name="code" type="Int32"/>
      <element name="category" type="String"/>
      <element name="message" type="String"/>
      <element name="subcategory" type="String" minOccurs="0" maxOccurs="1"/>
    </sequenceType>
    <sequenceType name="FieldException">
      <element name="fieldId" type="String"/>
      <element name="errorInfo" type="ErrorInfo"/>
    </sequenceType>
    <sequenceType name="ReferenceSecurityData">
      <element name="security" type="String"/>
      <element name="eidData" type="Int32" minOccurs="0" maxOccurs="unbounded"/>
      <element name="securityError" type="ErrorInfo" minOccurs="0" maxOccurs="1"/>
      <element name="fieldExceptions" type="FieldException" minOccurs="0" maxOccurs="unbounded"/>
      <element name="sequenceNumber" type="Int32" minOccurs="0" maxOccurs="1"/>
      <element name="fieldData" type="ReferenceFieldData"/>
    </sequenceType>
    <sequenceType name="HistoricalSecurityData">
      <element name="security" type="String"/>
      <element name="eidData" type="Int32" minOccurs="0" maxOccurs="unbounded"/>
      <element name="sequenceNumber" type="Int32" minOccurs="0" maxOccurs="1"/>
      <element name="securityError" type="ErrorInfo" minOccurs="0" maxOccurs="1"/>
      <element name="fieldExceptions" type="FieldException" minOccurs="0" maxOccurs="unbounded"/>
      <element name="fieldData" type="HistoricalFieldData" minOccurs="0" maxOccurs="unbounded"/>
    </sequenceType>
    <!-- Add a line here for every field a test needs. -->
    <sequenceType name="ReferenceFieldData">
      <element name="PX_LAST" type="Float64" minOccurs="0" maxOccurs="1"/>
      <element name="NAME" type="String" minOccurs="0" maxOccurs="1"/>
      <element name="EQY_INIT_PO_DT" type="Date" minOccurs="0" maxOccurs="1"/>
      <element name="INDX_MEMBERS" type="IndexMember" minOccurs="0" maxOccurs="unbounded"/>
    </sequenceType>
    <sequenceType name="IndexMember">
      <element name="Member Ticker and Exchange Code" type="String"/>
    </sequenceType>
    <sequenceType name="HistoricalFieldData">
      <element name="date" type="Date"/>
      <element name="PX_LAST" type="Float64" minOccurs="0" maxOccurs="1"/>
      <element name="PX_VOLUME" type="Float64" minOccurs="0" maxOccurs="1"/>
    </sequenceType>
    <sequenceType name="BarData">
      <element name="eidData" type="Int32" minOccurs="0" maxOccurs="unbounded"/>
      <element name="barTickData" type="BarTickData" minOccurs="0" maxOccurs="unbounded"/>
    </sequenceType>
    <sequenceType name="BarTickData">
      <element name="time" type="Datetime"/>
      <element name="open" type="Float64"/>
      <element name="high" type="Float64"/>
      <element name="low" type="Float64"/>
      <element name="close" type="Float64"/>
      <element name="volume" type="Int64"/>
      <element name="numEvents" type="Int32"/>
      <element name="value" type="Float64"/>
    </sequenceType>
  </schema>
</ServiceDefinition>
```
