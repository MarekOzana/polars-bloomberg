"""bql: Bloomberg Query Language, like =BQL() in Excel.

bq.bql(query) gives a BqlResult: one DataFrame per item of get(...), in order, and
the item names in result.names. Each frame has an `ID` column, a column named after
the item, and the extra columns BQL sends with it (DATE, CURRENCY, ...).
result.combine(on="ID") joins the frames into one.

Run:  pytest --bbg tests/integration/test_integration_bql.py
"""

import logging
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

pytestmark = pytest.mark.requires_bbg


def test_bql_gives_a_frame_with_id_value_and_extra_columns(bq):
    result = bq.bql(
        "get(px_last(dates=2024-01-02)) for(['IBM US Equity', 'SEBA SS Equity'])"
    )

    assert result.names == ["px_last(dates=2024-01-02)"]
    expected = pl.DataFrame(
        [
            ("IBM US Equity", 161.5, date(2024, 1, 2), "USD"),
            ("SEBA SS Equity", 140.2, date(2024, 1, 2), "SEK"),
        ],
        schema=["ID", "px_last(dates=2024-01-02)", "DATE", "CURRENCY"],
        orient="row",
    )
    assert_frame_equal(result[0].sort("ID"), expected)


def test_bql_combine_joins_the_items_into_one_row_per_security(bq):
    result = bq.bql("get(name, px_last(dates=2024-01-02)) for(['IBM US Equity'])")

    df = result.combine(on="ID")

    assert result.names == ["name", "px_last(dates=2024-01-02)"]
    assert df.columns == ["ID", "name", "px_last(dates=2024-01-02)", "DATE", "CURRENCY"]
    assert df.rows() == [
        (
            "IBM US Equity",
            "International Business Machine",
            161.5,
            date(2024, 1, 2),
            "USD",
        )
    ]


def test_bql_let_names_an_item_and_a_date_range_gives_a_time_series(bq):
    result = bq.bql(
        "let(#px = px_last(dates=range(2024-01-02, 2024-01-05));) "
        "get(#px) for(['IBM US Equity'])"
    )

    assert result.names == ["#px"]
    assert result[0].select("DATE", "#px").rows() == [
        (date(2024, 1, 2), 161.5),
        (date(2024, 1, 3), 160.1),
        (date(2024, 1, 4), 160.86),
        (date(2024, 1, 5), 159.16),
    ]


def test_bql_item_brings_its_own_extra_columns(bq):
    result = bq.bql("get(name(), cpn()) for(['XS2479344561 Corp', 'USX60003AC87 Corp'])")

    names, coupons = result  # a BqlResult unpacks into its frames

    assert names.columns == ["ID", "name()"]
    assert coupons.columns == ["ID", "cpn()", "MULTIPLIER", "CPN_TYP"]
    assert coupons.schema["cpn()"] == pl.Float64
    assert result.combine(on="ID").height == 2


def test_bql_division_by_zero_gives_infinity_and_zero_by_zero_gives_null(bq):
    result = bq.bql(
        "let(#p = 1 / 0; #m = -1 / 0; #n = 0 / 0;) get(#p, #m, #n) for('OMX Index')"
    )

    df = result.combine(on="ID")

    assert df.rows() == [("OMX Index", float("inf"), float("-inf"), None)]


def test_bql_text_keeps_its_apostrophes(bq):
    result = bq.bql("get(name) for(['BFOR US Equity'])")

    assert result[0]["name"].item() == "Barron's 400 ETF"


def test_bql_error_gives_an_empty_result_that_carries_the_message(bq, caplog):
    with caplog.at_level(logging.ERROR):
        result = bq.bql("get(px_last for(['IBM US Equity'])")  # a missing ")"

    # Nothing is raised: the result is empty, and BQL's message is logged and kept.
    assert len(result) == 0
    assert "Unable to parse request" in result.errors[0]
    assert "BQL error:" in caplog.text
    with pytest.raises(ValueError, match="BQL error: Error: Unable to parse request"):
        result.combine()
