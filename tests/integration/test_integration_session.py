"""Connect to Bloomberg, see what a rejected request raises, and debug a request.

A BQuery holds one connection to the Terminal. Open it with a `with` block, or
with start() and stop() where a block does not fit, such as a notebook.

Run:  pytest --bbg tests/integration/test_integration_session.py
"""

import json

import pytest

from polars_bloomberg import BloombergError, BQuery

pytestmark = pytest.mark.requires_bbg


def test_a_with_block_connects_and_disconnects():
    with BQuery() as bq:
        df = bq.bdp("IBM US Equity", "CRNCY")

    assert df.rows() == [("IBM US Equity", "USD")]
    assert bq.session is None  # the block has closed the connection


def test_start_and_stop_keep_a_connection_open_between_notebook_cells():
    bq = BQuery().start()  # first cell
    try:
        df = bq.bdp("IBM US Equity", "CRNCY")  # any later cell
    finally:
        bq.stop()  # last cell; calling it twice does no harm

    assert df.rows() == [("IBM US Equity", "USD")]


def test_a_request_that_bloomberg_rejects_raises_bloomberg_error(bq):
    with pytest.raises(BloombergError, match="No securities specified") as error:
        bq.bdp([], "NAME")

    # BloombergError is a ValueError, so `except ValueError` catches it too.
    assert isinstance(error.value, ValueError)


def test_debug_writes_every_request_and_its_answer_to_a_json_file(tmp_path):
    with BQuery(debug=tmp_path) as bq:
        bq.bdp("IBM US Equity", "CRNCY")

    (dump_file,) = tmp_path.glob("ReferenceDataRequest_*.json")  # one per request
    dump = json.loads(dump_file.read_text(encoding="utf-8"))
    assert dump["request"]["body"] == {
        "securities": ["IBM US Equity"],
        "fields": ["CRNCY"],
    }
    answer = dump["events"][-1]["messages"][0]["payload"]
    assert answer["securityData"][0]["fieldData"] == {"CRNCY": "USD"}
