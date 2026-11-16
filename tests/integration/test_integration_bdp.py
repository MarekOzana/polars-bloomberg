"""bdp: reference data, like =BDP() in Excel.

bq.bdp(securities, fields) gives one row per security: a `security` column, then one
column per field, typed as Bloomberg sends it (text, number, date, or a list for a
bulk field). Rows may come back in another order than the securities were given.

Run:  pytest --bbg tests/integration/test_integration_bdp.py
"""

import logging
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pytestmark = pytest.mark.requires_bbg


def test_bdp_gives_one_row_per_security_and_one_column_per_field(bq):
    df = bq.bdp(
        ["IBM US Equity", "SEBA SS Equity"],
        ["NAME", "CRNCY", "EQY_INIT_PO_DT"],
    )

    expected = pl.DataFrame(
        [
            ("IBM US Equity", "INTL BUSINESS MACHINES CORP", "USD", date(1915, 11, 11)),
            ("SEBA SS Equity", "SKANDINAVISKA ENSKILDA BAN-A", "SEK", None),
        ],
        schema=["security", "NAME", "CRNCY", "EQY_INIT_PO_DT"],
        orient="row",
    )
    assert_frame_equal(df.sort("security"), expected)


def test_bdp_takes_a_single_security_and_field_as_plain_strings(bq):
    df = bq.bdp("IBM US Equity", "CRNCY")

    assert df.rows() == [("IBM US Equity", "USD")]


def test_bdp_overrides_change_what_bloomberg_computes(bq):
    # When does a USD/SEK spot trade made on 15 July 2020 settle?
    df = bq.bdp(
        "USDSEK Curncy",
        "SETTLE_DT",
        overrides={"REFERENCE_DATE": date(2020, 7, 15)},
    )

    assert df["SETTLE_DT"].item() == date(2020, 7, 17)  # two business days later


def test_bdp_bulk_field_is_a_list_with_one_struct_per_row_of_the_table(bq):
    df = bq.bdp(
        "IBM US Equity",
        "DVD_HIST",
        overrides={"DVD_START_DT": "20240101", "DVD_END_DT": "20241231"},
    )

    dividends = df.explode("DVD_HIST").unnest("DVD_HIST")  # one row per dividend
    assert dividends.select("Ex-Date", "Dividend Amount").sort("Ex-Date").rows() == [
        (date(2024, 2, 8), 1.66),
        (date(2024, 5, 9), 1.67),
        (date(2024, 8, 9), 1.67),
        (date(2024, 11, 12), 1.67),
    ]


def test_bdp_invalid_security_or_field_gives_nulls_and_a_warning(bq, caplog):
    with caplog.at_level(logging.WARNING):
        df = bq.bdp(
            ["IBM US Equity", "NOT A SECURITY Equity"],
            ["NAME", "NOT_A_FIELD"],
        )

    expected = pl.DataFrame(
        [
            ("IBM US Equity", "INTL BUSINESS MACHINES CORP", None),
            ("NOT A SECURITY Equity", None, None),
        ],
        schema=["security", "NAME", "NOT_A_FIELD"],
        orient="row",
    )
    assert_frame_equal(df.sort("security"), expected)
    # Nothing is raised; one warning lists what Bloomberg reported.
    assert "NOT A SECURITY Equity: Unknown/Invalid Security" in caplog.text
    assert "IBM US Equity / NOT_A_FIELD: Field not valid" in caplog.text
