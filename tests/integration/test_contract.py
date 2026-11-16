"""Has Bloomberg changed the format of its answers since they were recorded?

Each test asks Bloomberg again and compares the fresh answer with the committed
recording: always its shape (keys and value types), and for scenarios whose data
does not change, also the result.
"""

import json

import pytest
from record import RECORDINGS_DIR, record
from recordings import CONTRACT, VOLATILE
from support import assert_result_matches, load_recording, shape

from polars_bloomberg import BQuery

pytestmark = pytest.mark.requires_bbg


@pytest.fixture(scope="module")
def recorder(tmp_path_factory):
    """A live BQuery that writes a debug dump of every request, and the dump folder."""
    dump_dir = tmp_path_factory.mktemp("dumps")
    with BQuery(debug=dump_dir) as bq:
        yield bq, dump_dir


@pytest.mark.parametrize("name", CONTRACT)
def test_live_answer_has_the_recorded_shape(name, recorder):
    fresh, _ = record(*recorder, name)
    committed_file = RECORDINGS_DIR / f"{name}.json"
    committed = json.loads(committed_file.read_text(encoding="utf-8"))

    assert shape(fresh["events"]) == shape(committed["events"])


@pytest.mark.parametrize("name", [name for name in CONTRACT if name not in VOLATILE])
def test_live_result_matches_the_recorded_result(name, recorder):
    _, result = record(*recorder, name)

    assert_result_matches(result, load_recording(name)["expected"])
