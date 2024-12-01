"""
Unit tests for the plbbg module.
the test REQUIRE active Bloomberg Terminal connection.

:author: Marek Ozana
:date: 2024-12-01
"""

from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg import BQuery


def test_bdp():
    """
    Test the BDP function.
    """
    with BQuery() as bq:
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
            .select(
                (pl.col("PX_LAST") - pl.col("CRNCY_ADJ_PX_LAST")).abs().alias("diff")
            )
            .item()
            < 1e-6
        ), "OMX Index should have PX_LAST same as in SEK"
        assert (
            df_1.filter(pl.col("security") == "SPX Index")
            .select((pl.col("CRNCY_ADJ_PX_LAST") / pl.col("PX_LAST")).alias("ratio"))
            .item()
            > 10
        ), "SPX Index should have PX_LAST 10x larger in USD than in SEK"


def test_bdh():
    """
    Test the BDH function.
    """
    with BQuery() as bq:
        # plain vanilla
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

        # with options
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


def test_create_request():
    """
    Test the _create_request method.
    """
    with BQuery() as bq:
        request = bq._create_request(
            request_type="ReferenceDataRequest",
            securities=["OMX Index", "SPX Index"],
            fields=["PX_LAST"],
        )
        assert request.getElement("securities").toPy() == ["OMX Index", "SPX Index"]
        assert request.getElement("fields").toPy() == ["PX_LAST"]


def test_create_request_with_overrides():
    """
    Test the _create_request method with overrides.
    """
    with BQuery() as bq:
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


def test_create_request_with_options():
    """
    Test the _create_request method with options.
    """
    with BQuery() as bq:
        request = bq._create_request(
            request_type="HistoricalDataRequest",
            securities=["OMX Index", "SPX Index"],
            fields=["PX_LAST"],
            options={"adjustmentSplit": True}
        )
        assert request.getElement("adjustmentSplit").toPy() # == True
