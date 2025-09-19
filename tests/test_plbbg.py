"""Unit tests for the plbbg module.

The tests REQUIRE an active Bloomberg Terminal connection.

:author: Marek Ozana
:date: 2024-12-06
"""

import json
import re
from collections.abc import Generator
from datetime import date
from typing import Final
from unittest.mock import MagicMock, patch

import blpapi
import polars as pl
import pytest
import yaml
from polars.testing import assert_frame_equal

from polars_bloomberg import BQuery
from polars_bloomberg.plbbg import BqlResult, SITable


@pytest.fixture(scope="module")
def bq() -> Generator[BQuery, None, None]:
    """Fixture to create a BQuery instance for testing."""
    with BQuery() as bq_instance:
        yield bq_instance


def test_bdp(bq: BQuery):
    """Test the BDP function."""
    # Plain vanilla
    df = bq.bdp(
        ["OMX Index"],
        ["COUNT_INDEX_MEMBERS", "NAME", "INDEX_MEMBERSHIP_MAINT_DATE"],
    )
    df_exp = pl.DataFrame(
        {
            "security": ["OMX Index"],
            "COUNT_INDEX_MEMBERS": [30],
            "NAME": ["OMX STOCKHOLM 30 INDEX"],
            "INDEX_MEMBERSHIP_MAINT_DATE": [date(2001, 1, 2)],
        }
    )
    assert_frame_equal(df, df_exp)

    # With overrides
    df_1 = bq.bdp(
        ["OMX Index", "SPX Index"],
        ["PX_LAST", "CRNCY_ADJ_PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")],
    )
    assert df_1.filter(pl.col("security") == "OMX Index").select(
        (pl.col("PX_LAST") - pl.col("CRNCY_ADJ_PX_LAST")).abs().alias("diff")
    ).item() == pytest.approx(0), "OMX Index should have PX_LAST same as in SEK"

    much_bigger: Final[int] = 8
    assert (
        df_1.filter(pl.col("security") == "SPX Index")
        .select((pl.col("CRNCY_ADJ_PX_LAST") / pl.col("PX_LAST")).alias("ratio"))
        .item()
        > much_bigger
    ), "SPX Index should have PX_LAST 8x larger in SEK than in USD"


def test_bdh(bq: BQuery):
    """Test the BDH function."""
    # Plain vanilla
    df = bq.bdh(
        ["OMX Index", "SEBA SS Equity"],
        ["PX_LAST", "DIVIDEND_INDICATED_YIELD"],
        date(2024, 1, 1),
        date(2024, 1, 30),
    )
    assert df.shape == (42, 4)
    assert df.columns == ["security", "date", "PX_LAST", "DIVIDEND_INDICATED_YIELD"]
    last_row = df.rows()[-1]
    assert last_row[0] == "SEBA SS Equity"
    assert last_row[1] == date(2024, 1, 30)
    assert last_row[2] == pytest.approx(149.6)
    assert last_row[3] == pytest.approx(5.6818)

    # With options
    df = bq.bdh(
        ["SPY US Equity", "TLT US Equity"],
        ["PX_LAST", "VOLUME"],
        start_date=date(2019, 1, 1),
        end_date=date(2019, 1, 10),
        options={"adjustmentSplit": True},
    )
    assert df.shape == (14, 4)
    df_exp = pl.DataFrame(
        {
            "security": [
                "SPY US Equity",
                "SPY US Equity",
                "SPY US Equity",
                "SPY US Equity",
                "SPY US Equity",
                "SPY US Equity",
                "SPY US Equity",
                "TLT US Equity",
                "TLT US Equity",
                "TLT US Equity",
                "TLT US Equity",
                "TLT US Equity",
                "TLT US Equity",
                "TLT US Equity",
            ],
            "date": [
                date(2019, 1, 2),
                date(2019, 1, 3),
                date(2019, 1, 4),
                date(2019, 1, 7),
                date(2019, 1, 8),
                date(2019, 1, 9),
                date(2019, 1, 10),
                date(2019, 1, 2),
                date(2019, 1, 3),
                date(2019, 1, 4),
                date(2019, 1, 7),
                date(2019, 1, 8),
                date(2019, 1, 9),
                date(2019, 1, 10),
            ],
            "PX_LAST": [
                250.18,
                244.21,
                252.39,
                254.38,
                256.77,
                257.97,
                258.88,
                122.15,
                123.54,
                122.11,
                121.75,
                121.43,
                121.24,
                120.46,
            ],
            "VOLUME": [
                126925199.0,
                144140692.0,
                142628834.0,
                103139100.0,
                102512587.0,
                95006554.0,
                96823923.0,
                19841527.0,
                21187045.0,
                12970226.0,
                8498104.0,
                7737103.0,
                9349245.0,
                8222860.0,
            ],
        }
    )
    assert_frame_equal(df, df_exp)


def test_bql(bq: BQuery):
    """Test the BQL function."""
    query = """
            get(name(), cpn())
            for(['XS2479344561 Corp', 'USX60003AC87 Corp'])
            """
    bql_result = bq.bql(query)
    two: Final[int] = 2
    assert len(bql_result) == two
    assert isinstance(bql_result, BqlResult)
    assert bql_result.names == ["name()", "cpn()"]

    df = bql_result[0].join(bql_result[1], on="ID")

    assert df.shape == (2, 5)
    assert df.columns == ["ID", "name()", "cpn()", "MULTIPLIER", "CPN_TYP"]
    df_exp = pl.DataFrame(
        {
            "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
            "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
            "cpn()": [6.875, 6.3],
            "MULTIPLIER": [1.0, 1.0],
            "CPN_TYP": ["VARIABLE", "VARIABLE"],
        }
    )
    assert_frame_equal(df, df_exp)


def test_bql_with_single_quote(bq: BQuery):
    """Test BQL query with single quotes in securities."""
    query = """
            get(px_last)
            for(['IBM US Equity', 'AAPL US Equity'])
            """
    bql_result = bq.bql(query)

    assert len(bql_result) == 1
    assert isinstance(bql_result, BqlResult)
    assert bql_result.names == ["px_last"]

    df = bql_result[0]
    assert df.shape == (2, 4)
    assert df.columns == ["ID", "px_last", "DATE", "CURRENCY"]
    df_exp = pl.DataFrame(
        {
            "ID": ["IBM US Equity", "AAPL US Equity"],
            "px_last": [241.28, 230.56],
            "DATE": [date(2025, 8, 20), date(2025, 8, 20)],
            "CURRENCY": ["USD", "USD"],
        }
    )
    assert_frame_equal(df, df_exp)


def test_issue_7_bql_with_single_quote(bq):
    """Test BQL query with single quotes in securities.

    https://github.com/MarekOzana/polars-bloomberg/issues/7.
    """
    result = bq.bql("for(['BFOR US Equity']) get(name)")  #   BARRON'S 400 ETF

    assert len(result) == 1
    df = result[0]
    assert isinstance(df, pl.DataFrame)
    df_exp = pl.DataFrame({"ID": "BFOR US Equity", "name": "Barron's 400 ETF"})
    assert_frame_equal(df, df_exp)


@pytest.mark.no_bbg
def test_bdh_leading_nulls():
    """Test on dataset with leading nulls in a field."""
    bq = BQuery()

    # Mock the network call
    with (
        patch.object(bq, "_create_request", return_value=MagicMock()),
        patch.object(bq, "_send_request", return_value="mocked_responses"),
    ):
        # Load data from file
        with open("tests/data/bdh_data_leading_nulls.yaml") as f:
            mock_data = yaml.safe_load(f)

        # Mock parse method to return loaded data
        with patch.object(bq, "_parse_bdh_responses", return_value=mock_data):
            # Call bdh() method
            df = bq.bdh(
                ["BFGHICE LX Equity", "I00185US Index"],
                ["BX115", "BX213", "PX_LAST"],
                start_date=date(2024, 8, 1),
                end_date=date(2025, 1, 10),
            )

    # Validate result
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (222, 5)


def test_create_request(bq: BQuery):
    """Test the _create_request method."""
    request = bq._create_request(
        request_type="ReferenceDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
    )
    assert request.getElement("securities").toPy() == ["OMX Index", "SPX Index"]
    assert request.getElement("fields").toPy() == ["PX_LAST"]


def test_create_request_with_overrides(bq: BQuery):
    """Test the _create_request method with overrides."""
    request = bq._create_request(
        request_type="ReferenceDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
        overrides=[("EQY_FUND_CRNCY", "SEK")],
    )
    overrides_element = request.getElement("overrides")
    overrides_set = {
        (
            override.getElementAsString("fieldId"),
            override.getElementAsString("value"),
        )
        for override in overrides_element.values()
    }
    assert overrides_set == {("EQY_FUND_CRNCY", "SEK")}


def test_create_request_with_options(bq: BQuery):
    """Test the _create_request method with options."""
    request = bq._create_request(
        request_type="HistoricalDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
        options={"adjustmentSplit": True},
    )
    assert request.getElement("adjustmentSplit").toPy() is True


@pytest.mark.no_bbg
def test_parse_bdp_responses():
    """Test the _parse_bdp_responses method."""
    bq = BQuery()  # unitialized object (no BBG connection yet)
    # Mock responses as they might be received from the Bloomberg API
    mock_responses = [
        {
            "securityData": [
                {
                    "security": "IBM US Equity",
                    "fieldData": {"PX_LAST": 125.32, "DS002": 0.85},
                },
                {
                    "security": "AAPL US Equity",
                    "fieldData": {"PX_LAST": 150.75, "DS002": 1.10},
                },
            ]
        }
    ]

    # Expected output after parsing
    expected_output = [
        {"security": "IBM US Equity", "PX_LAST": 125.32, "DS002": 0.85},
        {"security": "AAPL US Equity", "PX_LAST": 150.75, "DS002": 1.10},
    ]

    # Call the _parse_bdp_responses function with mock data
    result = bq._parse_bdp_responses(mock_responses, fields=["PX_LAST", "DS002"])

    # Assert that the parsed result matches the expected output
    assert result == expected_output


@pytest.mark.no_bbg
def test_parse_bdh_responses():
    """Test the _parse_bdh_responses method."""
    bq = BQuery()  # unitialized object (no BBG connection yet)
    # Mock responses as they might be received from the Bloomberg API
    mock_responses = [
        {
            "securityData": {
                "security": "IBM US Equity",
                "fieldData": [
                    {"date": "2023-01-01", "PX_LAST": 125.32, "VOLUME": 1000000},
                    {"date": "2023-01-02", "PX_LAST": 126.50, "VOLUME": 1100000},
                ],
            }
        },
        {
            "securityData": {
                "security": "AAPL US Equity",
                "fieldData": [
                    {"date": "2023-01-01", "PX_LAST": 150.75, "VOLUME": 2000000},
                    {"date": "2023-01-02", "PX_LAST": 151.20, "VOLUME": 2100000},
                ],
            }
        },
    ]

    # Expected output after parsing
    expected_output = [
        {
            "security": "IBM US Equity",
            "date": "2023-01-01",
            "PX_LAST": 125.32,
            "VOLUME": 1000000,
        },
        {
            "security": "IBM US Equity",
            "date": "2023-01-02",
            "PX_LAST": 126.50,
            "VOLUME": 1100000,
        },
        {
            "security": "AAPL US Equity",
            "date": "2023-01-01",
            "PX_LAST": 150.75,
            "VOLUME": 2000000,
        },
        {
            "security": "AAPL US Equity",
            "date": "2023-01-02",
            "PX_LAST": 151.20,
            "VOLUME": 2100000,
        },
    ]

    # Call the _parse_bdh_responses function with mock data
    result = bq._parse_bdh_responses(mock_responses, fields=["PX_LAST", "VOLUME"])

    # Assert that the parsed result matches the expected output
    assert result == expected_output


@pytest.mark.no_bbg
def test_parse_bql_responses():
    """Test the _parse_bql_responses method."""
    bq = BQuery()  # uninitialized object (no BBG connection yet)

    # Mock responses as they might be received from the Bloomberg API
    mock_responses = [
        {
            "server": "localhost:8194",
            "serverId": "bbcomm-5CG3290LGQ-3511155",
            "encryptionStatus": "Clear",
            "compressionStatus": "Uncompressed",
        },
        {"initialEndpoints": [{"address": "localhost:8194"}]},
        {"serviceName": "//blp/refdata"},
        {"serviceName": "//blp/bqlsvc"},
        """
        {
            "results": {
                "px_last": {
                    "name": "px_last",
                    "offsets": [
                        0,
                        1
                    ],
                    "namespace": "DATAITEM_DEFAULT",
                    "source": "CR",
                    "idColumn": {
                        "name": "ID",
                        "type": "STRING",
                        "rank": 0,
                        "values": [
                            "IBM US Equity",
                            "AAPL US Equity"
                        ]
                    },
                    "valuesColumn": {
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "rank": 0,
                        "values": [
                            241.28,
                            230.56
                        ]
                    },
                    "secondaryColumns": [
                        {
                            "name": "DATE",
                            "type": "DATE",
                            "rank": 0,
                            "values": [
                                "2025-08-20T00:00:00Z",
                                "2025-08-20T00:00:00Z"
                            ],
                            "defaultDate": true
                        },
                        {
                            "name": "CURRENCY",
                            "type": "STRING",
                            "rank": 0,
                            "values": [
                                "USD",
                                "USD"
                            ]
                        }
                    ],
                    "partialErrorMap": {
                        "errorIterator": null
                    },
                    "responseExceptions": [],
                    "forUniverse": true,
                    "bqlResponseInfo": null,
                    "defaultDateColumnName": null,
                    "itemPreviewStatistics": null,
                    "indexView": null
                }
            },
            "ordering": [
                {
                    "requestIndex": 0,
                    "responseName": "px_last"
                }
            ]
        }
        """,
    ]

    # Expected output after parsing
    exp_data = {
        "ID": ["IBM US Equity", "AAPL US Equity"],
        "px_last": [241.28, 230.56],
        "DATE": [date(2025, 8, 20), date(2025, 8, 20)],
        "CURRENCY": ["USD", "USD"],
    }
    exp_schema = {
        "ID": pl.String,
        "px_last": pl.Float64,
        "DATE": pl.Date,
        "CURRENCY": pl.String,
    }

    # Call the _parse_bql_responses function with mock data
    tables: list[SITable] = bq._parse_bql_responses(mock_responses)
    assert len(tables) == 1
    tbl = tables[0]
    # Assert that the parsed result matches the expected output
    assert tbl.data == exp_data
    assert tbl.schema == exp_schema


@pytest.mark.no_bbg
def test__extract_results():
    """no_bbg test on apostrof in values."""
    bq = BQuery()

    responses = [
        {
            "server": "localhost:8194",
            "serverId": "bbcomm-5CG3290LGQ-3511155",
            "encryptionStatus": "Clear",
            "compressionStatus": "Uncompressed",
        },
        {"initialEndpoints": [{"address": "localhost:8194"}]},
        {"serviceName": "//blp/refdata"},
        {"serviceName": "//blp/bqlsvc"},
        """
        {
            "results": {
                "name": {
                    "name": "name",
                    "offsets": [
                        0
                    ],
                    "namespace": "FUNCTION_DEFAULT",
                    "source": "BQLAnalyticsEngine",
                    "idColumn": {
                        "name": "ID",
                        "type": "STRING",
                        "rank": 0,
                        "values": [
                            "BFOR US Equity"
                        ]
                    },
                    "valuesColumn": {
                        "name": "VALUE",
                        "type": "STRING",
                        "rank": 0,
                        "values": ["Barron\'s 400 ETF"
                        ]
                    },
                    "secondaryColumns": [],
                    "partialErrorMap": {
                        "errorIterator": null
                    },
                    "responseExceptions": [],
                    "forUniverse": true,
                    "bqlResponseInfo": null,
                    "defaultDateColumnName": null,
                    "itemPreviewStatistics": null,
                    "indexView": null
                }
            },
            "ordering": [
                {
                    "requestIndex": 0,
                    "responseName": "name"
                }
            ],
            "responseExceptions": null,

            "screenCounts": null,
            "payloadId": null
        }
        """,
    ]

    results = bq._extract_results(responses=responses)
    assert len(results) == 1
    assert results[0]["name"] == {
        "name": "name",
        "offsets": [0],
        "namespace": "FUNCTION_DEFAULT",
        "source": "BQLAnalyticsEngine",
        "idColumn": {
            "name": "ID",
            "type": "STRING",
            "rank": 0,
            "values": ["BFOR US Equity"],
        },
        "valuesColumn": {
            "name": "VALUE",
            "type": "STRING",
            "rank": 0,
            "values": ["Barron's 400 ETF"],
        },
        "secondaryColumns": [],
        "partialErrorMap": {"errorIterator": None},
        "responseExceptions": [],
        "forUniverse": True,
        "bqlResponseInfo": None,
        "defaultDateColumnName": None,
        "itemPreviewStatistics": None,
        "indexView": None,
    }


@pytest.mark.no_bbg
@pytest.mark.parametrize(
    "case_json_file",
    [
        "tests/data/bql_parse_results_last_px.json",
        "tests/data/bql_parse_results_dur_zspread.json",
        "tests/data/bql_parse_results_axes.json",
        "tests/data/bql_parse_results_segment.json",
        "tests/data/bql_parse_results_eps.json",
        "tests/data/bql_parse_results_rets.json",
        "tests/data/bql_parse_results_axes_addcolsALL.json",
    ],
)
def test__parse_result_replay(case_json_file):
    """Test the _parse_result on replay life cases."""
    with open(case_json_file) as f:
        data = json.load(f)

    in_result: dict = data["in_results"]
    out_tables: list[dict] = data["out_tables"]

    bq = BQuery()
    tables = bq._parse_result(in_result)
    for i, table in enumerate(tables):
        assert table.name == out_tables[i]["name"]
        assert table.data == out_tables[i]["data"]
        # saved json has schema as string
        assert {k: str(v) for k, v in table.schema.items()} == out_tables[i]["schema"]


@pytest.mark.no_bbg
class TestBQuerySendRequest:
    """Test suite for the BQuery._send_request method."""

    @pytest.fixture
    def bquery(self):
        """Fixture to create a BQuery instance with a mocked session.

        Initializes the BQuery object with a specified timeout and mocks
        the Bloomberg session to control its behavior during tests.
        """
        with patch("polars_bloomberg.plbbg.blpapi.Session") as mock_session_class:
            """This mock session replaces the actual Bloomberg session to avoid
                making real API calls during testing.
            """
            mock_session_instance = MagicMock()
            mock_session_class.return_value = mock_session_instance
            with BQuery(timeout=5000) as bquery:
                yield bquery

    def test_send_request_success(self, bquery: BQuery):
        """Test that _send_request successfully processes partial and final responses.

        This test simulates a scenario where the Bloomberg API returns a partial
        response followed by a final response. It verifies that _send_request
        correctly collects and returns the responses.
        """
        # Create mock events
        partial_event = MagicMock()
        partial_event.eventType.return_value = blpapi.Event.PARTIAL_RESPONSE

        final_event = MagicMock()
        final_event.eventType.return_value = blpapi.Event.RESPONSE

        # Mock messages for each event
        partial_message = MagicMock()
        partial_message.hasElement.return_value = False  # No errors
        partial_message.toPy.return_value = {"partial": "data"}

        final_message = MagicMock()
        final_message.hasElement.return_value = False  # No errors
        final_message.toPy.return_value = {"final": "data"}

        # Set up event messages
        partial_event.__iter__.return_value = iter([partial_message])
        final_event.__iter__.return_value = iter([final_message])

        # Configure nextEvent to return partial and then final event
        bquery.session.nextEvent.side_effect = [partial_event, final_event]

        # Mock request
        mock_request = MagicMock()

        # Call the method under test
        responses = bquery._send_request(mock_request)

        # Assertions
        bquery.session.sendRequest.assert_called_with(mock_request)
        assert responses == [{"partial": "data"}, {"final": "data"}]
        assert bquery.session.nextEvent.call_count == 2  # noqa: PLR2004
        bquery.session.nextEvent.assert_any_call(5000)

    def test_send_request_timeout(self, bquery: BQuery):
        """Test that _send_request raises a TimeoutError when a timeout occurs.

        This test simulates a scenario where the Bloomberg API does not respond
        within the specified timeout period, triggering a timeout event.
        """
        # Create a timeout event
        timeout_event = MagicMock()
        timeout_event.eventType.return_value = blpapi.Event.TIMEOUT
        timeout_event.__iter__.return_value = iter([])  # No messages

        # Configure nextEvent to return a timeout event
        bquery.session.nextEvent.return_value = timeout_event

        # Mock request
        mock_request = MagicMock()

        # Call the method under test and expect a TimeoutError
        with pytest.raises(
            TimeoutError, match="Request timed out after 5000 milliseconds"
        ):
            bquery._send_request(mock_request)

        # Assertions
        bquery.session.sendRequest.assert_called_with(mock_request)
        bquery.session.nextEvent.assert_called_once_with(5000)

    def test_send_request_with_response_error(self, bquery: BQuery):
        """Test _send_request when the response contains an error.

        This test simulates a scenario where the Bloomberg API returns a response
        containing an error message. It verifies that _send_request properly
        detects and raises an exception for the error.
        """
        # Create a response event with an error
        response_event = MagicMock()
        response_event.eventType.return_value = blpapi.Event.RESPONSE

        # Mock message with a response error
        error_message = MagicMock()
        error_message.hasElement.return_value = True

        # Mock the error element returned by getElement("responseError")
        error_element = MagicMock()
        error_element.getElementAsString.return_value = "Invalid field"
        error_message.getElement.return_value = error_element

        response_event.__iter__.return_value = iter([error_message])

        # Configure nextEvent to return the response event
        bquery.session.nextEvent.return_value = response_event

        # Mock request
        mock_request = MagicMock()

        # Call the method under test and expect an Exception
        with pytest.raises(Exception, match="Response error: Invalid field"):
            bquery._send_request(mock_request)

        # Assertions
        bquery.session.sendRequest.assert_called_with(mock_request)
        bquery.session.nextEvent.assert_called_once_with(5000)


@pytest.mark.no_bbg
class TestSchemaMappingAndDataConversion:
    """Test suite for the BQuery._map_column_types_to_schema method."""

    @pytest.fixture
    def bq(self):
        """Fixture to create a BQuery instance for testing."""
        return BQuery()

    @pytest.mark.parametrize(
        "schema_str, schema_exp",
        [
            (
                {"col1": "STRING", "col2": "DOUBLE"},
                {"col1": pl.String, "col2": pl.Float64},
            ),
            (
                {"col1": "INT", "col2": "DATE", "col3": "DOUBLE"},
                {"col1": pl.Int64, "col2": pl.Date, "col3": pl.Float64},
            ),
            (
                {"col1": "UNKNOWN_TYPE"},
                {"col1": pl.String},
            ),
            (
                {"name": "STRING", "age": "INT"},
                {"name": pl.Utf8, "age": pl.Int64},
            ),
            (
                {"price": "DOUBLE", "date": "DATE"},
                {"price": pl.Float64, "date": pl.Date},
            ),
            (
                {"is_active": "BOOLEAN"},
                {"is_active": pl.Boolean},
            ),
            (
                {"is_active": "boolean"},
                {"is_active": pl.Boolean},
            ),
            (
                {"is_active": "BoOlEaN"},
                {"is_active": pl.Boolean},
            ),
            (
                {"name": "STRING", "is_active": "BOOLEAN", "valid": "BOOLEAN"},
                {"name": pl.Utf8, "is_active": pl.Boolean, "valid": pl.Boolean},
            ),
            (
                {"unknown_field": "UNKNOWN"},
                {"unknown_field": pl.Utf8},
            ),
            (
                {},
                {},
            ),
        ],
    )
    def test__map_types(self, schema_str, schema_exp, bq: BQuery):
        """Test mapping column types to schema."""
        schema = bq._map_types(schema_str)
        assert schema_exp == schema

    @pytest.mark.parametrize(
        "data, schema, exp_data",
        [
            # Test with empty data list and schema list
            ({}, {}, {}),
            # Test with date strings in various formats
            (
                {
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                    "number_col": [1, 2.5],
                },
                {"date_col": pl.Date, "number_col": pl.Float64},
                {
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                    "number_col": [1.0, 2.5],
                },
            ),
            # Test with invalid date strings
            (
                {"date_col": [None], "number_col": ["NaN"]},
                {"date_col": pl.Date, "number_col": pl.Float64},
                {"date_col": [None], "number_col": [None]},
            ),
            # Test with data having 5 columns each of different type
            (
                {
                    "string_col": ["a", "b"],
                    "int_col": [1, 2],
                    "float_col": [1.1, "NaN"],
                    "bool_col": [True, False],
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                },
                {
                    "string_col": pl.Utf8,
                    "int_col": pl.Int64,
                    "float_col": pl.Float64,
                    "bool_col": pl.Boolean,
                    "date_col": pl.Date,
                },
                {
                    "string_col": ["a", "b"],
                    "int_col": [1, 2],
                    "float_col": [1.1, None],
                    "bool_col": [True, False],
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                },
            ),
            # Test with NaN values and date conversion
            (
                {
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                    "number_col": ["NaN", 3.14],
                },
                {"date_col": pl.Date, "number_col": pl.Float64},
                {
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                    "number_col": [None, 3.14],
                },
            ),
            # BOOLEAN test cases
            (
                {
                    "is_verified": [None, True, False, False],
                    "user_role": ["admin", "user", "guest", "user"],
                },
                {"is_verified": pl.Boolean, "user_role": pl.String},
                {
                    "is_verified": [None, True, False, False],
                    "user_role": ["admin", "user", "guest", "user"],
                },
            ),
        ],
    )
    def test__apply_schema(self, data, schema, exp_data, bq: BQuery):
        """Test the _apply_schema method with various data and schema inputs."""
        in_table = SITable(name="test", data=data, schema=schema)
        out_table = bq._apply_schema(in_table)
        assert out_table.data == exp_data
        assert out_table.schema == schema


@pytest.mark.no_bbg
class TestBqlResult:
    """Unit tests for the BqlResult class."""

    def test_initialization(self):
        """Test initializing BqlResult with dataframes and names."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        names = ["Data1", "Data2"]
        bql_result = BqlResult(dataframes=[df1, df2], names=names)

        assert bql_result.dataframes == [df1, df2]
        assert bql_result.names == names

    def test_combine_success(self):
        """Test combining dataframes with common columns."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()

        expected_df = pl.DataFrame(
            {"ID": ["A", "B"], "Value1": [1, 2], "Value2": [3, 4]}
        )

        pl.testing.assert_frame_equal(combined_df, expected_df)

    def test_combine_no_common_columns(self):
        """Test combining dataframes with no common columns raises ValueError."""
        df1 = pl.DataFrame({"ID1": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID2": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError, match=re.escape("No common columns found to join on.")
        ):
            bql_result.combine()

    def test_combine_empty_dataframes(self):
        """Test combining with no dataframes raises ValueError."""
        bql_result = BqlResult(dataframes=[], names=[])

        with pytest.raises(ValueError, match=re.escape("No DataFrames to combine.")):
            bql_result.combine()

    def test_getitem(self):
        """Test accessing dataframes by index."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        assert_frame_equal(df1, bql_result[0])
        assert_frame_equal(df2, bql_result[1])

    def test_len(self):
        """Test the length of BqlResult."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value1": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])
        exp_length = 2
        assert len(bql_result) == exp_length

    def test_iter(self):
        """Test iterating over BqlResult dataframes."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value1": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        dataframes: list[pl.DataFrame] = list(bql_result)
        assert dataframes == [df1, df2]

    def test_combine_multiple_dataframes(self):
        """Test combining multiple dataframes with common columns."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        df3 = pl.DataFrame({"ID": ["A", "B"], "Value3": [5, 6]})
        bql_result = BqlResult(
            dataframes=[df1, df2, df3], names=["Data1", "Data2", "Data3"]
        )

        combined_df = bql_result.combine()

        expected_df = pl.DataFrame(
            {"ID": ["A", "B"], "Value1": [1, 2], "Value2": [3, 4], "Value3": [5, 6]}
        )

        pl.testing.assert_frame_equal(combined_df, expected_df)

    def test_combine_with_duplicate_ids(self):
        """Test combining dataframes with duplicate IDs."""
        df1 = pl.DataFrame({"ID": ["A", "A"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "A"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()

        expected_df = pl.DataFrame(
            {"ID": ["A", "A", "A", "A"], "Value1": [1, 2, 1, 2], "Value2": [3, 3, 4, 4]}
        )

        pl.testing.assert_frame_equal(combined_df, expected_df)

    def test_combine_with_different_row_counts(self):
        """Test combining dataframes with different numbers of rows."""
        df1 = pl.DataFrame({"ID": ["A", "B", "C"], "Value1": [1, 2, 3]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [4, 5]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()

        expected_df = pl.DataFrame(
            {"ID": ["A", "B", "C"], "Value1": [1, 2, 3], "Value2": [4, 5, None]}
        )

        pl.testing.assert_frame_equal(combined_df, expected_df)

    def test_combine_single_dataframe(self):
        """Test that combining a single DataFrame returns the DataFrame itself."""
        df = pl.DataFrame({"ID": ["A", "B", "C"], "Value": [1, 2, 3]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        combined_df = bql_result.combine()

        assert_frame_equal(combined_df, df)

    def test_combine_different_schemas(self):
        """Test combining DataFrames with different columns, some overlapping."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Name": ["Alice", "Bob"]})
        df2 = pl.DataFrame({"ID": ["B", "C"], "Age": [30, 25]})
        df3 = pl.DataFrame({"ID": ["A", "C"], "City": ["New York", "Los Angeles"]})
        bql_result = BqlResult(dataframes=[df1, df2, df3], names=["DF1", "DF2", "DF3"])

        combined_df = bql_result.combine().sort("ID")

        expected_df = pl.DataFrame(
            {
                "ID": ["A", "B", "C"],
                "Name": ["Alice", "Bob", None],
                "Age": [None, 30, 25],
                "City": ["New York", None, "Los Angeles"],
            }
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_with_missing_values(self):
        """Test combining DataFrames that contain missing (null) values."""
        df1 = pl.DataFrame({"ID": ["A", "B", "C"], "Value1": [1, None, 3]})
        df2 = pl.DataFrame({"ID": ["B", "C", "D"], "Value2": [None, 4, 5]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["DF1", "DF2"])

        combined_df = bql_result.combine().sort("ID")

        expected_df = pl.DataFrame(
            {
                "ID": ["A", "B", "C", "D"],
                "Value1": [1, None, 3, None],
                "Value2": [None, None, 4, 5],
            }
        )

        assert_frame_equal(combined_df, expected_df)

    @pytest.mark.parametrize(
        "yaml_file, exp_df",
        [
            (
                "tests/data/df_lst_name_dur_zspread.yaml",
                {
                    "ID": [
                        "YV402592 Corp",
                        "BW924993 Corp",
                        "ZO703956 Corp",
                        "ZO703315 Corp",
                        "ZQ349286 Corp",
                        "YU819930 Corp",
                    ],
                    "name()": [
                        "SEB Float PERP",
                        "SEB 6 ⅞ PERP",
                        "SHBASS 4 ¾ PERP",
                        "SHBASS 4 ⅜ PERP",
                        "SEB 5 ⅛ PERP",
                        "SEB 6 ¾ PERP",
                    ],
                    "#dur": [0.21, 2.23, 4.94, 1.95, 0.39, 5.37],
                    "DATE": [
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                    ],
                    "#zsprd": [232.71, 211.55, 255.85, 213.35, 185.98, 308.81],
                },
            ),
            (
                "tests/data/df_lst_name_ema20_ema200_rsi.yaml",
                {
                    "ID": [
                        "ERICB SS Equity",
                        "SKFB SS Equity",
                        "SEBA SS Equity",
                        "ASSAB SS Equity",
                        "SWEDA SS Equity",
                    ],
                    "name()": [
                        "Telefonaktiebolaget LM Ericsso",
                        "SKF AB",
                        "Skandinaviska Enskilda Banken",
                        "Assa Abloy AB",
                        "Swedbank AB",
                    ],
                    "#ema20": [
                        90.09,
                        214.38,
                        153.68,
                        338.82,
                        217.38,
                    ],
                    "CURRENCY": ["SEK", "SEK", "SEK", "SEK", "SEK"],
                    "DATE": [
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                    ],
                    "#ema200": [
                        74.91,
                        205.17,
                        150.72,
                        316.82,
                        213.77,
                    ],
                    "#rsi": [
                        57.45,
                        58.40,
                        57.69,
                        55.46,
                        56.30,
                    ],
                },
            ),
            (
                "tests/data/df_lst_name_px_last.yaml",
                {
                    "ID": ["IBM US Equity"],
                    "name": ["International Business Machine"],
                    "CURRENCY": ["USD"],
                    "DATE": [date(2024, 12, 14)],
                    "px_last": [230.82],
                },
            ),
            (
                "tests/data/df_lst_name_rank_oas_nxtcall.yaml",
                {
                    "ID": [
                        "YX231113 Corp",
                        "BS116983 Corp",
                        "AV438089 Corp",
                        "ZO860846 Corp",
                        "LW375188 Corp",
                    ],
                    "name()": [
                        "GTN 10 ½ 07/15/29",
                        "GTN 5 ⅜ 11/15/31",
                        "GTN 7 05/15/27",
                        "GTN 4 ¾ 10/15/30",
                        "GTN 5 ⅞ 07/15/26",
                    ],
                    "#rank": [
                        "1st Lien Secured",
                        "Sr Unsecured",
                        "Sr Unsecured",
                        "Sr Unsecured",
                        "Sr Unsecured",
                    ],
                    "#nxt_call": [
                        date(2026, 7, 15),
                        date(2026, 11, 15),
                        date(2024, 12, 23),
                        date(2025, 10, 15),
                        date(2025, 1, 12),
                    ],
                    "#oas": [597.32, 1192.83, 391.13, 1232.55, 171.7],
                    "DATE": [
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                        date(2024, 12, 14),
                    ],
                },
            ),
            (
                "tests/data/df_lst_segment_revenue.yaml",
                {
                    "#segment": [
                        "Broadcasting",
                        "Broadcasting",
                        "Production Companies",
                        "Production Companies",
                        "Other ",
                        "Other ",
                        "Adjustment",
                        "Adjustment",
                    ],
                    "AS_OF_DATE": [
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                    ],
                    "FUNDAMENTAL_TICKER": [
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                        "GTN US Equity",
                    ],
                    "ID": [
                        "SEG0000524428 Segment",
                        "SEG0000524428 Segment",
                        "SEG0000524437 Segment",
                        "SEG0000524437 Segment",
                        "SEG0000795330 Segment",
                        "SEG0000795330 Segment",
                        "SEG8339225113 Segment",
                        "SEG8339225113 Segment",
                    ],
                    "ID_DATE": [
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                        date(2024, 12, 15),
                    ],
                    "ORDER": ["1", "1", "2", "2", "3", "3", "4", "4"],
                    "#revenue": [
                        808000000.0,
                        924000000.0,
                        18000000.0,
                        26000000.0,
                        0.0,
                        17000000.0,
                        None,
                        None,
                    ],
                    "CURRENCY": ["USD", "USD", "USD", "USD", "USD", "USD", "USD", "USD"],
                    "PERIOD_END_DATE": [
                        date(2024, 6, 30),
                        date(2024, 9, 30),
                        date(2024, 6, 30),
                        date(2024, 9, 30),
                        date(2024, 6, 30),
                        date(2024, 9, 30),
                        date(2024, 6, 30),
                        date(2024, 9, 30),
                    ],
                    "REVISION_DATE": [
                        date(2024, 8, 8),
                        date(2024, 11, 8),
                        date(2024, 8, 8),
                        date(2024, 11, 8),
                        date(2024, 8, 8),
                        date(2024, 11, 8),
                        None,
                        None,
                    ],
                },
            ),
        ],
    )
    def test_real_life_cases(self, yaml_file, exp_df):
        """Test real-life cases based on yaml files.

        Creation of test case
        >>> with BQuery() as bq:
        >>>     df_lst = bq.bql(query)
        >>> with open("data/df_lst_<case name>.yaml", "w") as f:
        >>>    yaml.dump([d.to_dict(as_series=False) for d in df_lst], f)
        """
        with open(yaml_file) as f:
            d_lst = yaml.safe_load(f)
        df_lst = [pl.DataFrame(dct) for dct in d_lst]
        names = [str(x) for x in list(range(len(df_lst)))]
        bql_result = BqlResult(df_lst, names=names)

        df = bql_result.combine().to_dict(as_series=False)
        assert df == exp_df
