"""bql() end to end: a scripted Bloomberg answer goes in, a BqlResult comes out.

Bloomberg sends a BQL answer as one JSON string; `bql_answer` builds that string.
"""

import json
import logging

import pytest
from support import DATA_DIR, answer, fake_bq

pytestmark = pytest.mark.no_bbg


def bql_item(ids, values, value_type):
    """One BQL item as Bloomberg sends it: an ID column and a values column."""
    return {
        "idColumn": {"type": "STRING", "values": ids},
        "valuesColumn": {"type": value_type, "values": values},
        "secondaryColumns": [],
    }


def bql_answer(items):
    """A RESPONSE event that carries the BQL items as one JSON string."""
    return answer(json.dumps({"results": items, "responseExceptions": None}))


def test_bql_two_items_give_two_named_frames():
    items = {
        "name": bql_item(["IBM US Equity"], ["IBM"], "STRING"),
        "px_last": bql_item(["IBM US Equity"], [150.0], "DOUBLE"),
    }
    bq = fake_bq([bql_answer(items)])

    result = bq.bql("get(name, px_last) for(['IBM US Equity'])")

    assert result.names == ["name", "px_last"]
    assert result[0].columns == ["ID", "name"]
    assert result[1].columns == ["ID", "px_last"]
    assert result[1]["px_last"].to_list() == [150.0]


def test_bql_nan_string_becomes_null():
    items = {"px_last": bql_item(["A", "B"], ["NaN", 1.5], "DOUBLE")}
    bq = fake_bq([bql_answer(items)])

    result = bq.bql("get(px_last) for(['A', 'B'])")

    assert result[0]["px_last"].to_list() == [None, 1.5]


def test_bql_infinity_strings_become_inf():
    items = {"ratio": bql_item(["A", "B"], ["Infinity", "-Infinity"], "DOUBLE")}
    bq = fake_bq([bql_answer(items)])

    result = bq.bql("get(ratio) for(['A', 'B'])")

    assert result[0]["ratio"].to_list() == [float("inf"), float("-inf")]


def test_bql_syntax_error_returns_empty_result_and_logs(caplog):
    recorded = json.loads(
        (DATA_DIR / "bql" / "response_bql_raised_exception.json").read_text()
    )
    syntax_error_payload = recorded[-1]  # the JSON string with responseExceptions
    bq = fake_bq([answer(syntax_error_payload)])

    with caplog.at_level(logging.ERROR):
        result = bq.bql("get(px_lastfor(['IBM US Equity'])")

    assert len(result) == 0
    assert "Unable to parse request" in caplog.text
