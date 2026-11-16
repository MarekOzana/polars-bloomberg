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
