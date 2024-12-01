"""
Unit tests for the plbbg module.
The tests REQUIRE an active Bloomberg Terminal connection.

:author: Marek Ozana
:date: 2024-12-01
"""

from datetime import date
from typing import Generator

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg import BQuery


@pytest.fixture(scope="module")
def bq() -> Generator[BQuery, None, None]:
    with BQuery() as bq_instance:
        yield bq_instance


def test_bdp(bq: BQuery):
    """
    Test the BDP function.
    """
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
    assert (
        df_1.filter(pl.col("security") == "OMX Index")
        .select((pl.col("PX_LAST") - pl.col("CRNCY_ADJ_PX_LAST")).abs().alias("diff"))
        .item()
        < 1e-6
    ), "OMX Index should have PX_LAST same as in SEK"
    assert (
        df_1.filter(pl.col("security") == "SPX Index")
        .select((pl.col("CRNCY_ADJ_PX_LAST") / pl.col("PX_LAST")).alias("ratio"))
        .item()
        > 10
    ), "SPX Index should have PX_LAST 10x larger in USD than in SEK"


def test_bdh(bq: BQuery):
    """
    Test the BDH function.
    """
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


def test_create_request(bq: BQuery):
    """
    Test the _create_request method.
    """
    request = bq._create_request(
        request_type="ReferenceDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
    )
    assert request.getElement("securities").toPy() == ["OMX Index", "SPX Index"]
    assert request.getElement("fields").toPy() == ["PX_LAST"]


def test_create_request_with_overrides(bq: BQuery):
    """
    Test the _create_request method with overrides.
    """
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
    """
    Test the _create_request method with options.
    """
    request = bq._create_request(
        request_type="HistoricalDataRequest",
        securities=["OMX Index", "SPX Index"],
        fields=["PX_LAST"],
        options={"adjustmentSplit": True},
    )
    assert request.getElement("adjustmentSplit").toPy() is True


def test_parse_bdp_responses(bq: BQuery):
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
    print(result)

    # Assert that the parsed result matches the expected output
    assert result == expected_output


def test_parse_bdh_responses(bq: BQuery):
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
