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
from polars_bloomberg.plbbg import SITable


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
    df_lst = bq.bql(query)
    two: Final[int] = 2
    assert len(df_lst) == two

    df = df_lst[0].join(df_lst[1], on="ID")

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
        "{'name': 'CURRENCY', 'type':'STRING','values': ['USD', 'USD']}]}}}",
    ]

    # Expected output after parsing
    exp_data = {
        "ID": ["IBM US Equity", "AAPL US Equity"],
        "px_last": [125.32, 150.75],
        "DATE": [date(2024, 12, 3), date(2024, 12, 3)],
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
@pytest.mark.parametrize(
    "json_file, exp_table_list",
    [
        (
            "tests/data/results_last_px.json",
            [
                SITable(
                    name="px_last",
                    data={
                        "ID": ["IBM US Equity", "AAPL US Equity"],
                        "px_last": [227.02, 241.31],
                        "DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
                        "CURRENCY": ["USD", "USD"],
                    },
                    schema={
                        "ID": pl.Utf8,
                        "px_last": pl.Float64,
                        "DATE": pl.Date,
                        "CURRENCY": pl.Utf8,
                    },
                )
            ],
        ),
        (
            "tests/data/results_dur_zspread.json",
            [
                SITable(
                    name="name()",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                    },
                    schema={"ID": pl.String, "name()": pl.String},
                ),
                SITable(
                    name="#dur",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "#dur": [2.26, 5.36],
                        "DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
                    },
                    schema={"ID": pl.String, "#dur": pl.Float64, "DATE": pl.Date},
                ),
                SITable(
                    name="#zsprd",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "#zsprd": [244.5, 331.1],
                        "DATE": ["2024-12-03T00:00:00Z", "2024-12-03T00:00:00Z"],
                    },
                    schema={"ID": pl.String, "#zsprd": pl.Float64, "DATE": pl.Date},
                ),
            ],
        ),
        (
            "tests/data/results_cpn.json",
            [
                SITable(
                    name="name()",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                    },
                    schema={"ID": pl.String, "name()": pl.String},
                ),
                SITable(
                    name="cpn()",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "cpn()": [6.875, 6.3],
                        "MULTIPLIER": [1.0, 1.0],
                        "CPN_TYP": ["VARIABLE", "VARIABLE"],
                    },
                    schema={
                        "ID": pl.String,
                        "cpn()": pl.Float64,
                        "MULTIPLIER": pl.Float64,
                        "CPN_TYP": pl.String,
                    },
                ),
            ],
        ),
        (
            "tests/data/results_axes.json",
            [
                SITable(
                    name="name()",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "name()": ["SEB 6 ⅞ PERP", "NDAFH 6.3 PERP"],
                    },
                    schema={"ID": pl.String, "name()": pl.String},
                ),
                SITable(
                    name="axes()",
                    data={
                        "ID": ["XS2479344561 Corp", "USX60003AC87 Corp"],
                        "axes()": ["Y", "Y"],
                        "ASK_DEPTH": [3, 1],
                        "BID_DEPTH": [4, 3],
                        "ASK_TOTAL_SIZE": [11200000.0, 2000000.0],
                        "BID_TOTAL_SIZE": [15000000.0, 13000000.0],
                    },
                    schema={
                        "ID": pl.String,
                        "axes()": pl.String,
                        "ASK_DEPTH": pl.Int64,
                        "BID_DEPTH": pl.Int64,
                        "ASK_TOTAL_SIZE": pl.Float64,
                        "BID_TOTAL_SIZE": pl.Float64,
                    },
                ),
            ],
        ),
        (
            "tests/data/results_eps_range.json",
            [
                SITable(
                    name="#eps",
                    data={
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
                        "REVISION_DATE": [
                            "2022-02-22T00:00:00Z",
                            "2023-02-28T00:00:00Z",
                            "2023-02-28T00:00:00Z",
                            "2024-03-18T00:00:00Z",
                            "2024-03-18T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                        ],
                        "AS_OF_DATE": [
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                            "2024-12-07T00:00:00Z",
                        ],
                        "PERIOD_END_DATE": [
                            "2019-12-31T00:00:00Z",
                            "2020-12-31T00:00:00Z",
                            "2021-12-31T00:00:00Z",
                            "2022-12-31T00:00:00Z",
                            "2023-12-31T00:00:00Z",
                            "2024-12-31T00:00:00Z",
                            "2025-12-31T00:00:00Z",
                        ],
                        "CURRENCY": ["USD", "USD", "USD", "USD", "USD", "USD", "USD"],
                    },
                    schema={
                        "ID": pl.String,
                        "#eps": pl.Float64,
                        "REVISION_DATE": pl.Date,
                        "AS_OF_DATE": pl.Date,
                        "PERIOD_END_DATE": pl.Date,
                        "CURRENCY": pl.String,
                    },
                )
            ],
        ),
        (
            "tests/data/results_with_NaN_DOUBLE.json",
            [
                SITable(
                    name="#rets",
                    data={
                        "ID": ["YX231113 Corp", "YX231113 Corp", "YX231113 Corp"],
                        "#rets": ["NaN", 0.000273, -0.000863],
                        "DATE": [
                            "2024-12-07T00:00:00Z",
                            "2024-12-08T00:00:00Z",
                            "2024-12-09T00:00:00Z",
                        ],
                    },
                    schema={"ID": pl.String, "#rets": pl.Float64, "DATE": pl.Date},
                )
            ],
        ),
        (
            "tests/data/results_segment.json",
            [
                SITable(
                    name="#segment",
                    data={
                        "ID": [
                            "SEG0000524428 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000795330 Segment",
                            "SEG8339225113 Segment",
                        ],
                        "#segment": [
                            "Broadcasting",
                            "Production Companies",
                            "Other ",
                            "Adjustment",
                        ],
                        "ORDER": ["1", "2", "3", "4"],
                        "FUNDAMENTAL_TICKER": [
                            "GTN US Equity",
                            "GTN US Equity",
                            "GTN US Equity",
                            "GTN US Equity",
                        ],
                        "AS_OF_DATE": [
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                        ],
                        "ID_DATE": [
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                        ],
                    },
                    schema={
                        "ID": pl.String,
                        "#segment": pl.String,
                        "ORDER": pl.String,
                        "FUNDAMENTAL_TICKER": pl.String,
                        "AS_OF_DATE": pl.Date,
                        "ID_DATE": pl.Date,
                    },
                ),
                SITable(
                    name="#revenue",
                    data={
                        "ID": [
                            "SEG0000524428 Segment",
                            "SEG0000524428 Segment",
                            "SEG0000524428 Segment",
                            "SEG0000524428 Segment",
                            "SEG0000524428 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000524437 Segment",
                            "SEG0000795330 Segment",
                            "SEG0000795330 Segment",
                            "SEG0000795330 Segment",
                            "SEG0000795330 Segment",
                            "SEG0000795330 Segment",
                            "SEG8339225113 Segment",
                            "SEG8339225113 Segment",
                            "SEG8339225113 Segment",
                            "SEG8339225113 Segment",
                            "SEG8339225113 Segment",
                        ],
                        "#revenue": [
                            783000000.0,
                            813000000.0,
                            780000000.0,
                            808000000.0,
                            924000000.0,
                            20000000.0,
                            32000000.0,
                            24000000.0,
                            18000000.0,
                            26000000.0,
                            16000000.0,
                            19000000.0,
                            19000000.0,
                            0.0,
                            17000000.0,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        "REVISION_DATE": [
                            "2023-11-08T00:00:00Z",
                            "2024-02-23T00:00:00Z",
                            "2024-05-07T00:00:00Z",
                            "2024-08-08T00:00:00Z",
                            "2024-11-08T00:00:00Z",
                            "2023-11-08T00:00:00Z",
                            "2024-02-23T00:00:00Z",
                            "2024-05-07T00:00:00Z",
                            "2024-08-08T00:00:00Z",
                            "2024-11-08T00:00:00Z",
                            "2023-11-08T00:00:00Z",
                            "2024-02-23T00:00:00Z",
                            "2024-05-07T00:00:00Z",
                            "2024-08-08T00:00:00Z",
                            "2024-11-08T00:00:00Z",
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        "AS_OF_DATE": [
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                        ],
                        "PERIOD_END_DATE": [
                            "2023-09-30T00:00:00Z",
                            "2023-12-31T00:00:00Z",
                            "2024-03-31T00:00:00Z",
                            "2024-06-30T00:00:00Z",
                            "2024-09-30T00:00:00Z",
                            "2023-09-30T00:00:00Z",
                            "2023-12-31T00:00:00Z",
                            "2024-03-31T00:00:00Z",
                            "2024-06-30T00:00:00Z",
                            "2024-09-30T00:00:00Z",
                            "2023-09-30T00:00:00Z",
                            "2023-12-31T00:00:00Z",
                            "2024-03-31T00:00:00Z",
                            "2024-06-30T00:00:00Z",
                            "2024-09-30T00:00:00Z",
                            "2023-09-30T00:00:00Z",
                            "2023-12-31T00:00:00Z",
                            "2024-03-31T00:00:00Z",
                            "2024-06-30T00:00:00Z",
                            "2024-09-30T00:00:00Z",
                        ],
                        "CURRENCY": [
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                            "USD",
                        ],
                        "ID_DATE": [
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                            "2024-12-10T00:00:00Z",
                        ],
                    },
                    schema={
                        "ID": pl.String,
                        "#revenue": pl.Float64,
                        "REVISION_DATE": pl.Date,
                        "AS_OF_DATE": pl.Date,
                        "PERIOD_END_DATE": pl.Date,
                        "CURRENCY": pl.String,
                        "ID_DATE": pl.Date,
                    },
                ),
            ],
        ),
    ],
)
def test__parse_result(json_file, exp_table_list):
    """Test the _parse_result method with various input files."""
    bq = BQuery()
    with open(json_file) as f:
        result = json.load(f)

    # Call the method to test
    tables = bq._parse_result(result)

    assert len(tables) == len(exp_table_list)
    for i, table in enumerate(tables):
        assert table.data == exp_table_list[i].data
        assert table.schema == exp_table_list[i].schema


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
                {"col1": pl.Utf8, "col2": pl.Float64},
            ),
            (
                {"col1": "INT", "col2": "DATE", "col3": "DOUBLE"},
                {"col1": pl.Int64, "col2": pl.Date, "col3": pl.Float64},
            ),
            (
                {"col1": "UNKNOWN_TYPE"},
                {"col1": pl.Utf8},
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
        ],
    )
    def test__apply_schema(self, data, schema, exp_data, bq: BQuery):
        """Test the _apply_schema method with various data and schema inputs."""
        in_table = SITable(name="test", data=data, schema=schema)
        out_table = bq._apply_schema(in_table)
        assert out_table.data == exp_data
        assert out_table.schema == schema
