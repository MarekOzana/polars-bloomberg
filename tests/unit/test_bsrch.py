"""bsrch() end to end: a scripted grid goes in, a DataFrame comes out."""

import logging
from unittest.mock import MagicMock

import polars as pl
import pytest
from support import answer, fake_bq, replay

from polars_bloomberg import BloombergError

pytestmark = pytest.mark.no_bbg


def grid(titles, *records, **extra):
    """A GridResponse payload as Bloomberg sends it (recording bsrch/coco_limit_2)."""
    return {
        "NumOfFields": len(titles),
        "NumOfRecords": len(records),
        "ColumnTitles": titles,
        "DataRecords": [{"DataFields": list(fields)} for fields in records],
        "ReachMax": False,
        "SequenceNumber": 0,
        **extra,
    }


# --- bsrch() end to end, through the fake session ---


def test_bsrch_request_carries_domain_overrides_and_options():
    bq = fake_bq([answer(grid(["id"]))])
    service = MagicMock()  # no offline schema for //blp/exrsvc, so a stand-in
    bq.session.getService = lambda name: service

    bq.bsrch("FI:SRCHEX.@CLOSUB", overrides={"LIMIT": 20000}, options={"Foo": "Bar"})

    service.createRequest.assert_called_once_with("ExcelGetGridRequest")
    request = service.createRequest.return_value
    request.set.assert_any_call("Domain", "FI:SRCHEX.@CLOSUB")
    request.set.assert_any_call("Foo", "Bar")
    override = request.getElement.return_value.appendElement.return_value
    override.setElement.assert_any_call("name", "LIMIT")
    override.setElement.assert_any_call("value", "20000")


def test_bsrch_payload_that_is_not_a_grid_is_skipped():
    bq = fake_bq([answer(grid(["Ticker"], [{"StringValue": "AAPL US Equity"}]), {})])

    df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df["Ticker"].to_list() == ["AAPL US Equity"]


def test_bsrch_blank_cells_in_numeric_column_become_null():
    payload = grid(
        ["Ticker", "Yield"],
        [{"StringValue": "A Corp"}, {"DoubleValue": 1.5}],
        [{"StringValue": "B Corp"}, {"StringValue": " "}],
    )
    bq = fake_bq([answer(payload)])

    df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df.schema["Yield"] == pl.Float64
    assert df["Yield"].to_list() == [1.5, None]


def test_bsrch_grid_error_raises_value_error():
    bq = fake_bq([answer(grid([], Error="Invalid domain"))])

    with pytest.raises(ValueError, match="Invalid domain"):
        bq.bsrch("FI:NOT_A_SEARCH")


def test_bsrch_reached_limit_without_limit_override_warns(caplog):
    payload = grid(["id"], [{"StringValue": "DS990344 Corp"}], ReachMax=True)
    bq = fake_bq([answer(payload)])

    with caplog.at_level(logging.WARNING):
        bq.bsrch("FI:SRCHEX.@COCO")

    assert "consider using LIMIT override" in caplog.text


def test_bsrch_limit_given_as_pair_does_not_warn_about_the_limit(caplog):
    bq, _ = replay("bsrch/coco_limit_2")

    with caplog.at_level(logging.WARNING, logger="polars_bloomberg.plbbg"):
        df = bq.bsrch("FI:SRCHEX.@COCO", [("LIMIT", 2)])

    assert df.height == 2
    assert caplog.records == []


def test_bsrch_empty_result_keeps_column_titles():
    bq = fake_bq([answer(grid(["id", "Yield"]))])

    df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df.columns == ["id", "Yield"]
    assert df.height == 0


def test_bsrch_duplicate_titles_get_a_suffix(caplog):
    bq = fake_bq(
        [answer(grid(["Name", "Name"], [{"StringValue": "A"}, {"StringValue": "B"}]))]
    )

    with caplog.at_level(logging.WARNING, logger="polars_bloomberg.plbbg"):
        df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df.columns == ["Name", "Name_1"]
    assert df.row(0) == ("A", "B")
    assert "Name_1" in caplog.text


def test_bsrch_error_next_to_rows_is_logged(caplog):
    payload = grid(["id"], [{"StringValue": "A Corp"}], Error="Some rows missing")
    bq = fake_bq([answer(payload)])

    with caplog.at_level(logging.WARNING, logger="polars_bloomberg.plbbg"):
        df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df["id"].to_list() == ["A Corp"]
    assert "Some rows missing" in caplog.text


def test_bsrch_column_missing_in_first_row_is_still_coerced():
    payload = grid(
        ["Name", "Score"],
        [{"StringValue": "A"}],
        [{"StringValue": "B"}, {"StringValue": " "}],
        [{"StringValue": "C"}, {"DoubleValue": 1.5}],
    )
    bq = fake_bq([answer(payload)])

    df = bq.bsrch("FI:SRCHEX.@COCO")

    assert df.schema["Score"] == pl.Float64
    assert df["Score"].to_list() == [None, None, 1.5]


def test_bsrch_error_without_rows_raises_bloomberg_error():
    payload = grid([], Error="Failed to build query")
    bq = fake_bq([answer(payload)])

    with pytest.raises(BloombergError, match="BSRCH error: Failed to build query"):
        bq.bsrch("FI:NOT_A_SEARCH")


def test_bsrch_unknown_value_key_is_logged_once(caplog):
    payload = grid(["When"], [{"DateValue": "2024-01-02"}], [{"DateValue": "x"}])
    bq = fake_bq([answer(payload)])

    with caplog.at_level(logging.WARNING, logger="polars_bloomberg.plbbg"):
        bq.bsrch("FI:SRCHEX.@COCO")

    assert len(caplog.records) == 1
    assert "DateValue" in caplog.text
