"""Replay every recorded Bloomberg answer and check the call still gives its result.

The recordings live in tests/data/recordings/; tests/README.md says how to make them.
"""

import pytest
from recordings import SCENARIOS
from support import assert_result_matches, replay, run

pytestmark = pytest.mark.no_bbg


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_recording_replays_to_expected_result(name):
    bq, recording = replay(name)

    result = run(SCENARIOS[name], bq)

    assert_result_matches(result, recording["expected"])
