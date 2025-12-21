from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


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
            {
                "GridResponse": {
                    "ColumnTitles": ["Ticker", "PX_LAST", "COUNT"],
                    "DataRecords": [
                        {
                            "DataFields": [
                                {"Ticker": "IBM US Equity"},
                                {"DoubleData": 125.3},
                                {"Int32Data": 5},
                            ]
                        },
                        {
                            "DataFields": [
                                {"Ticker": "MSFT US Equity"},
                                {"DoubleValue": 402.1},
                                {"Int32Value": 4},
                            ]
                        },
                    ],
                    "ReachMax": False,
                }
            }
        ]

        rows = bq._parse_bsrch_responses(responses)
        assert rows == [
            {"Ticker": "IBM US Equity", "PX_LAST": 125.3, "COUNT": 5},
            {"Ticker": "MSFT US Equity", "PX_LAST": 402.1, "COUNT": 4},
        ]

    def test_parse_bsrch_responses_error(self):
        bq = BQuery()
        responses = [
            {
                "GridResponse": {
                    "Error": "Invalid domain",
                    "NumOfRecords": 0,
                    "DataRecords": [],
                }
            }
        ]

        with pytest.raises(ValueError, match="BSRCH error: Invalid domain"):
            bq._parse_bsrch_responses(responses)

    def test_parse_bsrch_responses_reachmax(self, caplog):
        bq = BQuery()
        responses = [
            {
                "GridResponse": {
                    "ColumnTitles": ["Ticker"],
                    "DataRecords": [{"DataFields": [{"StringData": "AAPL US Equity"}]}],
                    "ReachMax": True,
                }
            }
        ]

        with caplog.at_level("WARNING"):
            rows = bq._parse_bsrch_responses(responses)

        assert rows == [{"Ticker": "AAPL US Equity"}]
        assert any("reached internal limit" in rec.message for rec in caplog.records)

    def test_parse_bsrch_responses_coerces_numeric_whitespace(self):
        bq = BQuery()
        responses = [
            {
                "GridResponse": {
                    "ColumnTitles": ["Ticker", "PX_LAST", "COUNT"],
                    "DataRecords": [
                        {
                            "DataFields": [
                                {"StringValue": "IBM US Equity"},
                                {"DoubleValue": 125.3},
                                {"StringValue": " "},
                            ]
                        },
                        {
                            "DataFields": [
                                {"StringValue": "MSFT US Equity"},
                                {"StringValue": ""},
                                {"IntValue": 4},
                            ]
                        },
                    ],
                    "ReachMax": False,
                }
            }
        ]

        rows = bq._parse_bsrch_responses(responses)
        df = pl.DataFrame(rows, infer_schema_length=None, strict=False)

        assert df.schema["PX_LAST"] == pl.Float64
        assert df.schema["COUNT"] == pl.Int64
        assert df["PX_LAST"].to_list() == [125.3, None]
        assert df["COUNT"].to_list() == [None, 4]

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

    def test_parse_bsrch_root_payload_and_empty(self):
        bq = BQuery()
        responses = [
            {
                "NumOfFields": 1,
                "NumOfRecords": 1,
                "ColumnTitles": ["Ticker"],
                "DataRecords": [
                    {"DataFields": [{"StringValue": "AAPL US Equity"}]}
                ],
                "ReachMax": False,
            },
            {},
        ]

        rows = bq._parse_bsrch_responses(responses)
        assert rows == [{"Ticker": "AAPL US Equity"}]

    def test_bsrch_calls_components(self):
        bq = BQuery()
        mock_request = MagicMock()
        parsed_rows = [{"Ticker": "IBM US Equity"}, {"Ticker": "MSFT US Equity"}]

        with (
            patch.object(bq, "_create_bsrch_request", return_value=mock_request),
            patch.object(bq, "_send_request", return_value=["raw_response"]),
            patch.object(bq, "_parse_bsrch_responses", return_value=parsed_rows),
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

    def test_extract_bsrch_field_value_variants(self):
        bq = BQuery()

        assert bq._extract_bsrch_field_value("raw") == "raw"
        assert bq._extract_bsrch_field_value({"DoubleValue": "bad"}) == "bad"
        assert bq._extract_bsrch_field_value({"IntValue": "bad"}) == "bad"
        assert (
            bq._extract_bsrch_field_value({"DateValue": "2024-01-01"})
            == "2024-01-01"
        )
        unknown = {"Unknown": "x"}
        assert bq._extract_bsrch_field_value(unknown) == unknown
