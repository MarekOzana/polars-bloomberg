"""Unit tests for the plbbg module.

The tests REQUIRE an active Bloomberg Terminal connection.

:author: Marek Ozana
:date: 2024-12-06
"""

import json
from collections.abc import Generator
from datetime import date
from typing import Final
from unittest.mock import MagicMock, patch

import blpapi
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg import BQuery


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

    much_bigger: Final[int] = 10
    assert (
        df_1.filter(pl.col("security") == "SPX Index")
        .select((pl.col("CRNCY_ADJ_PX_LAST") / pl.col("PX_LAST")).alias("ratio"))
        .item()
        > much_bigger
    ), "SPX Index should have PX_LAST 10x larger in USD than in SEK"


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
    df = bq.bql(query)
    assert df.shape == (2, 5)
    assert df.columns == ["ID", "name()", "cpn()", "cpn().MULTIPLIER", "cpn().CPN_TYP"]
    df_exp = pl.DataFrame(
        {
            "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
            "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
            "cpn()": [6.875, 6.3],
            "cpn().MULTIPLIER": [1.0, 1.0],
            "cpn().CPN_TYP": ["VARIABLE", "VARIABLE"],
        }
    )
    assert_frame_equal(df, df_exp)


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
        {"other_data": "value1"},
        {"other_data": "value2"},
        "{'results': {'px_last': {'idColumn': "
        "{'values': ['IBM US Equity', 'AAPL US Equity']}, "
        "'valuesColumn': {'type':'DOUBLE', 'values': [125.32, 150.75]}, "
        "'secondaryColumns': [{'name': 'DATE', 'type':'DATE',"
        "'values': ['2024-12-03T00:00:00Z', '2024-12-03T00:00:00Z']}, "
        "{'name': 'CURRENCY', 'values': ['USD', 'USD']}]}}}",
    ]

    # Expected output after parsing
    expected_data = {
        "ID": ["IBM US Equity", "AAPL US Equity"],
        "px_last": [125.32, 150.75],
        "px_last.DATE": [date(2024, 12, 3), date(2024, 12, 3)],
        "px_last.CURRENCY": ["USD", "USD"],
    }
    expected_schema = {
        "ID": pl.String,
        "px_last": pl.Float64,
        "px_last.DATE": pl.Date,
        "px_last.CURRENCY": pl.String,
    }

    # Call the _parse_bql_responses function with mock data
    data, schema = bq._parse_bql_responses(mock_responses)

    # Assert that the parsed result matches the expected output
    assert data == expected_data
    assert schema == expected_schema


@pytest.mark.no_bbg
@pytest.mark.parametrize(
    "json_file, expected_data, expected_schema",
    [
        (
            "tests/data/results_last_px.json",
            {
                "ID": ["IBM US Equity", "AAPL US Equity"],
                "px_last": [227.02, 241.31],
                "px_last.DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
                "px_last.CURRENCY": ["USD", "USD"],
            },
            {
                "ID": "STRING",
                "px_last": "DOUBLE",
                "px_last.DATE": "DATE",
                "px_last.CURRENCY": "STRING",
            },
        ),
        (
            "tests/data/results_dur_zspread.json",
            {
                "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                "#dur": [2.26, 5.36],
                "#dur.DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
                "#zsprd": [244.5, 331.1],
                "#zsprd.DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
            },
            {
                "ID": "STRING",
                "name()": "STRING",
                "#dur": "DOUBLE",
                "#dur.DATE": "DATE",
                "#zsprd": "DOUBLE",
                "#zsprd.DATE": "DATE",
            },
        ),
        (
            "tests/data/results_cpn.json",
            {
                "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                "cpn()": [6.875, 6.3],
                "cpn().MULTIPLIER": [1.0, 1.0],
                "cpn().CPN_TYP": ["VARIABLE", "VARIABLE"],
            },
            {
                "ID": "STRING",
                "name()": "STRING",
                "cpn()": "DOUBLE",
                "cpn().MULTIPLIER": "DOUBLE",
                "cpn().CPN_TYP": "ENUM",
            },
        ),
        (
            "tests/data/results_axes.json",
            {
                "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                "axes()": ["Y", "Y"],
                "axes().ASK_DEPTH": [3, 1],
                "axes().BID_DEPTH": [4, 3],
                "axes().ASK_TOTAL_SIZE": [11200000.0, 2000000.0],
                "axes().BID_TOTAL_SIZE": [15000000.0, 13000000.0],
            },
            {
                "ID": "STRING",
                "name()": "STRING",
                "axes()": "STRING",
                "axes().ASK_DEPTH": "INT",
                "axes().BID_DEPTH": "INT",
                "axes().ASK_TOTAL_SIZE": "DOUBLE",
                "axes().BID_TOTAL_SIZE": "DOUBLE",
            },
        ),
        (
            "tests/data/results_eps_range.json",
            {
                "ID": [
                    "IBM US Equity",
                    "IBM US Equity",
                    "IBM US Equity",
                    "IBM US Equity",
                    "IBM US Equity",
                    "IBM US Equity",
                    "IBM US Equity",
                ],
                "#eps": [10.63, 6.28, 6.41, 1.82, 8.23, 7.89, 9.236],
                "#eps.REVISION_DATE": [
                    "2022-02-22T00:00:00Z",
                    "2023-02-28T00:00:00Z",
                    "2023-02-28T00:00:00Z",
                    "2024-03-18T00:00:00Z",
                    "2024-03-18T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                ],
                "#eps.AS_OF_DATE": [
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                    "2024-12-07T00:00:00Z",
                ],
                "#eps.PERIOD_END_DATE": [
                    "2019-12-31T00:00:00Z",
                    "2020-12-31T00:00:00Z",
                    "2021-12-31T00:00:00Z",
                    "2022-12-31T00:00:00Z",
                    "2023-12-31T00:00:00Z",
                    "2024-12-31T00:00:00Z",
                    "2025-12-31T00:00:00Z",
                ],
                "#eps.CURRENCY": ["USD", "USD", "USD", "USD", "USD", "USD", "USD"],
            },
            {
                "ID": "STRING",
                "#eps": "DOUBLE",
                "#eps.REVISION_DATE": "DATE",
                "#eps.AS_OF_DATE": "DATE",
                "#eps.PERIOD_END_DATE": "DATE",
                "#eps.CURRENCY": "STRING",
            },
        ),
    ],
)
def test_parse_bql_response_dict(json_file, expected_data, expected_schema):
    """Test the _parse_bql_response_dict method with various input files."""
    bq = BQuery()
    with open(json_file) as f:
        results = json.load(f)

    # Call the method to test
    cols, schema = bq._parse_bql_response_dict(results)

    # Assert that the data matches the expected output
    assert cols == expected_data
    # Assert that the column types match the expected schema
    assert schema == expected_schema


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

    def test_send_request_success(self, bquery):
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

    def test_send_request_timeout(self, bquery):
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

    def test_send_request_with_response_error(self, bquery):
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
