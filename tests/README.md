Offline tests (no Bloomberg):   pytest
Live tests (Terminal needed):   pytest --bbg

Learning the library? Read the live tests in tests/integration/ in this order; each
test is one call and shows what comes back:

  test_integration_session.py   connect, errors, debug dumps
  test_integration_bdp.py       reference data, overrides, bulk fields
  test_integration_bdh.py       history, options, empty ranges
  test_integration_bdib.py      intraday bars, time zones
  test_integration_bql.py       BQL items, combine(), let(), BQL errors
  test_integration_bsrch.py     saved searches
  test_integration_usage.py     requests from real reports and bond screens

Offline tests in tests/unit/, one file per concern:

  test_bdp.py, test_bdh.py, test_bdib.py, test_bql.py, test_bsrch.py
                                one public method each, end to end: a scripted
                                Bloomberg answer goes in, a DataFrame comes out
  test_requests.py              the requests the methods build before sending
  test_send_request.py          the event loop: which messages are the answer
  test_bquery_internals.py      start()/stop(), services, the debug dump
  test_bql_result.py            BqlResult: indexing, iteration, combine()
  test_recordings.py            every recording replays to its expected result
  test_usage.py                 what consumer code relies on, on the recordings

Offline tests replay recorded Bloomberg answers from tests/data/recordings/.
One file is one scenario: the request, the raw messages, the expected result.
tests/recordings.py lists the scenarios; tests/support.py holds the fake session.

The `usage_` scenarios are requests that reports, charts and bond screens send in
practice, reduced to public instruments. tests/usage_checks.py says what such code
relies on; tests/unit/test_usage.py checks it on the recordings, and
tests/integration/test_integration_usage.py on fresh answers.

Re-record (Terminal needed):    python tests/record.py            # all scenarios
                                python tests/record.py bdp/ bql/syntax_error
The scenarios in CONVERTED (tests/recordings.py) come from older fixtures whose
answers cannot be recorded again; record.py leaves them alone.
After an intended output change (no Terminal needed):
                                python tests/record.py --refresh-expected
Review `git diff tests/data/recordings` before committing.
