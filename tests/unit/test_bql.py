"""bql() end to end: a scripted Bloomberg answer goes in, a BqlResult comes out.

Bloomberg sends a BQL answer as one JSON string; `bql_answer` builds that string.
"""

import json
import logging
from datetime import date

import blpapi
import polars as pl
import pytest
from support import DATA_DIR, FakeEvent, FakeMessage, answer, fake_bq, replay

from polars_bloomberg import BloombergError

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


def test_bql_secondary_column_without_type_is_string():
    item = bql_item(["IBM US Equity"], [150.0], "DOUBLE")
    item["secondaryColumns"] = [{"name": "CURRENCY", "values": ["USD"]}]
    bq = fake_bq([bql_answer({"px_last": item})])

    result = bq.bql("get(px_last) for(['IBM US Equity'])")

    assert result[0].schema["CURRENCY"] == pl.String
    assert result[0]["CURRENCY"].to_list() == ["USD"]


def test_bql_item_with_null_columns_does_not_crash():
    items = {
        "skipped": None,
        "px_last": {"idColumn": None, "valuesColumn": None, "secondaryColumns": None},
    }
    bq = fake_bq([bql_answer(items)])

    result = bq.bql("get(px_last) for(['IBM US Equity'])")

    assert result.names == ["px_last"]
    assert result[0].columns == ["ID", "px_last"]
    assert result[0].height == 0


def test_bql_response_with_results_and_exceptions_keeps_results(caplog):
    items = {"px_last": bql_item(["IBM US Equity"], [150.0], "DOUBLE")}
    payload = {"results": items, "responseExceptions": [{"message": "Partial failure"}]}
    bq = fake_bq([answer(json.dumps(payload))])

    with caplog.at_level(logging.ERROR):
        result = bq.bql("get(px_last) for(['IBM US Equity'])")

    assert result.names == ["px_last"]
    assert result[0]["px_last"].to_list() == [150.0]
    assert result.errors == ["Partial failure"]
    assert "Partial failure" in caplog.text


def test_bql_syntax_error_is_available_in_result_errors():
    bq, _ = replay("bql/syntax_error")

    result = bq.bql("get(px_last for(['IBM US Equity'])")

    assert len(result) == 0
    assert result.errors == ["Error: Unable to parse request at 'get(px_lastfor'."]


def test_bql_valid_query_has_no_errors():
    bq, _ = replay("bql/two_items")

    result = bq.bql("get(name, px_last) for(['IBM US Equity'])")

    assert result.errors == []


def test_bql_unknown_value_type_is_inferred(caplog):
    items = {"px_last": bql_item(["A", "B"], [1.5, 2.5], "DECIMAL")}
    bq = fake_bq([bql_answer(items)])

    with caplog.at_level(logging.WARNING):
        result = bq.bql("get(px_last) for(['A', 'B'])")

    assert result[0].schema["px_last"] == pl.Float64
    assert result[0]["px_last"].to_list() == [1.5, 2.5]
    assert "DECIMAL" in caplog.text


def test_bql_date_with_fractional_seconds_is_parsed():
    items = {
        "day": bql_item(["A", "B"], ["2024-01-02T00:00:00.000Z", "2024-01-03"], "DATE")
    }
    bq = fake_bq([bql_answer(items)])

    result = bq.bql("get(day) for(['A', 'B'])")

    assert result[0]["day"].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]


def test_bql_item_that_cannot_be_built_names_the_item():
    items = {"px_last": bql_item(["A", "B"], [1.5], "DOUBLE")}  # one value, two IDs
    bq = fake_bq([bql_answer(items)])

    with pytest.raises(BloombergError, match="BQL item 'px_last'"):
        bq.bql("get(px_last) for(['A', 'B'])")


def test_bql_item_named_like_secondary_column_keeps_both_columns(caplog):
    item = bql_item(["IBM US Equity"], [161.5], "DOUBLE")
    item["secondaryColumns"] = [
        {"name": "DATE", "type": "DATE", "values": ["2024-01-02T00:00:00Z"]}
    ]
    bq = fake_bq([bql_answer({"DATE": item})])

    with caplog.at_level(logging.WARNING):
        result = bq.bql("let(DATE = px_last(dates=2024-01-02);) get(DATE) for(...)")

    assert result[0].columns == ["ID", "DATE", "DATE_1"]
    assert result[0]["DATE"].to_list() == [161.5]
    assert result[0]["DATE_1"].to_list() == [date(2024, 1, 2)]
    assert "DATE_1" in caplog.text


def test_bql_item_named_id_keeps_the_id_column():
    bq, _ = replay("bql/item_named_id")

    result = bq.bql("get(ID) for(['IBM US Equity'])")

    assert result[0].columns == ["ID", "ID_1"]
    assert result[0].row(0) == ("IBM US Equity", "IBM US Equity")


def test_bql_answer_split_over_several_messages_is_joined():
    items = {"px_last": bql_item(["A", "B", "C"], [1.5, 2.5, 3.5], "DOUBLE")}
    text = json.dumps({"results": items, "responseExceptions": None})
    third = len(text) // 3
    pieces = [text[:third], text[third : 2 * third], text[2 * third :]]
    bq = fake_bq(
        [
            FakeEvent(blpapi.Event.PARTIAL_RESPONSE, [FakeMessage(pieces[0])]),
            FakeEvent(blpapi.Event.PARTIAL_RESPONSE, [FakeMessage(pieces[1])]),
            FakeEvent(blpapi.Event.RESPONSE, [FakeMessage(pieces[2])]),
        ]
    )

    result = bq.bql("get(px_last) for(['A', 'B', 'C'])")

    assert result.errors == []
    assert result[0]["px_last"].to_list() == [1.5, 2.5, 3.5]
