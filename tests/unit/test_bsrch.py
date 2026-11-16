import logging
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from support import answer, fake_bq, replay

from polars_bloomberg import BloombergError, BQuery

pytestmark = pytest.mark.no_bbg


def grid(titles, *records, **extra):
    """A GridResponse payload as Bloomberg sends it (live check L8)."""
    return {
        "NumOfFields": len(titles),
        "NumOfRecords": len(records),
        "ColumnTitles": titles,
        "DataRecords": [{"DataFields": list(fields)} for fields in records],
        "ReachMax": False,
        "SequenceNumber": 0,
        **extra,
    }


class TestBsrch:
    def test_create_bsrch_request(self):
        bq = BQuery()
        session_mock = MagicMock()
        service_mock = MagicMock()
        request_mock = MagicMock()
        overrides_element = MagicMock()
        override_entry = MagicMock()

        overrides_element.appendElement.return_value = override_entry

        def request_get_element(name):
            if name == "Overrides":
                return overrides_element
            return MagicMock()

        request_mock.getElement.side_effect = request_get_element

        service_mock.createRequest.return_value = request_mock
        session_mock.getService.return_value = service_mock
        bq.session = session_mock

        bq._create_bsrch_request("FI:SRCHEX.@CLOSUB", overrides={"LIMIT": 20000})

        session_mock.getService.assert_called_once_with("//blp/exrsvc")
        service_mock.createRequest.assert_called_once_with("ExcelGetGridRequest")
        request_mock.set.assert_any_call("Domain", "FI:SRCHEX.@CLOSUB")
        overrides_element.appendElement.assert_called_once()
        override_entry.setElement.assert_any_call("name", "LIMIT")
        override_entry.setElement.assert_any_call("value", "20000")

    def test_parse_bsrch_responses_success(self):
        bq = BQuery()
        responses = [
            grid(
                ["Ticker", "PX_LAST"],
                [{"StringValue": "IBM US Equity"}, {"DoubleValue": 125.3}],
                [{"StringValue": "MSFT US Equity"}, {"DoubleValue": 402.1}],
            )
        ]

        rows, columns = bq._parse_bsrch_responses(responses)
        assert rows == [
            {"Ticker": "IBM US Equity", "PX_LAST": 125.3},
            {"Ticker": "MSFT US Equity", "PX_LAST": 402.1},
        ]
        assert columns == ["Ticker", "PX_LAST"]

    def test_parse_bsrch_responses_error(self):
        bq = BQuery()
        responses = [grid([], Error="Invalid domain")]

        with pytest.raises(ValueError, match="BSRCH error: Invalid domain"):
            bq._parse_bsrch_responses(responses)

    def test_parse_bsrch_responses_reachmax(self, caplog):
        bq = BQuery()
        responses = [
            grid(["Ticker"], [{"StringValue": "AAPL US Equity"}], ReachMax=True)
        ]

        with caplog.at_level("WARNING"):
            rows, _ = bq._parse_bsrch_responses(responses)

        assert rows == [{"Ticker": "AAPL US Equity"}]
        assert any("reached internal limit" in rec.message for rec in caplog.records)

    def test_parse_bsrch_responses_coerces_numeric_whitespace(self):
        bq = BQuery()
        responses = [
            grid(
                ["Ticker", "PX_LAST", "COUNT"],
                [
                    {"StringValue": "IBM US Equity"},
                    {"DoubleValue": 125.3},
                    {"StringValue": " "},
                ],
                [
                    {"StringValue": "MSFT US Equity"},
                    {"StringValue": ""},
                    {"DoubleValue": 4.0},
                ],
            )
        ]

        rows, _ = bq._parse_bsrch_responses(responses)
        df = pl.DataFrame(rows, infer_schema_length=None, strict=False)

        assert df.schema["PX_LAST"] == pl.Float64
        assert df.schema["COUNT"] == pl.Float64
        assert df["PX_LAST"].to_list() == [125.3, None]
        assert df["COUNT"].to_list() == [None, 4.0]

    def test_create_bsrch_request_with_options(self):
        bq = BQuery()
        session_mock = MagicMock()
        service_mock = MagicMock()
        request_mock = MagicMock()

        service_mock.createRequest.return_value = request_mock
        session_mock.getService.return_value = service_mock
        bq.session = session_mock

        bq._create_bsrch_request("FI:SRCHEX.@CLOSUB", options={"Foo": "Bar"})

        request_mock.set.assert_any_call("Domain", "FI:SRCHEX.@CLOSUB")
        request_mock.set.assert_any_call("Foo", "Bar")

    def test_parse_bsrch_skips_a_payload_that_is_not_a_grid(self):
        bq = BQuery()
        responses = [grid(["Ticker"], [{"StringValue": "AAPL US Equity"}]), {}]

        rows, _ = bq._parse_bsrch_responses(responses)
        assert rows == [{"Ticker": "AAPL US Equity"}]

    def test_bsrch_calls_components(self):
        bq = BQuery()
        mock_request = MagicMock()
        parsed_rows = [{"Ticker": "IBM US Equity"}, {"Ticker": "MSFT US Equity"}]

        with (
            patch.object(bq, "_create_bsrch_request", return_value=mock_request),
            patch.object(bq, "_send_request", return_value=["raw_response"]),
            patch.object(
                bq, "_parse_bsrch_responses", return_value=(parsed_rows, ["Ticker"])
            ),
        ):
            df = bq.bsrch("FI:SRCHEX.@CLOSUB", overrides={"LIMIT": 10000})

        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 1)
        assert df["Ticker"].to_list() == ["IBM US Equity", "MSFT US Equity"]

    def test_coerce_bsrch_numeric_columns_direct(self):
        bq = BQuery()
        rows = [
            {"A": 1, "B": " "},
            {"A": 2, "B": 2},
            {"A": "", "B": 3},
        ]
        bq._coerce_bsrch_numeric_columns(rows)
        assert rows == [
            {"A": 1, "B": None},
            {"A": 2, "B": 2},
            {"A": None, "B": 3},
        ]

    def test_coerce_bsrch_numeric_columns_empty(self):
        bq = BQuery()
        rows: list[dict] = []
        bq._coerce_bsrch_numeric_columns(rows)
        assert rows == []

    def test_extract_bsrch_field_value_reads_string_and_double_values(self):
        bq = BQuery()

        assert bq._extract_bsrch_field_value({"StringValue": "A Corp"}) == "A Corp"
        assert bq._extract_bsrch_field_value({"DoubleValue": 1.5}) == 1.5
        assert bq._extract_bsrch_field_value("raw") == "raw"
        unknown = {"Unknown": "x"}
        assert bq._extract_bsrch_field_value(unknown) == unknown


# --- bsrch() end to end, through the fake session ---


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
