Offline tests (no Bloomberg):   pytest
Live tests (Terminal needed):   pytest --bbg

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
