import logging

import polars as pl
import pytest

from polars_bloomberg.plbbg import BqlResult

pytestmark = pytest.mark.requires_bbg


def test_bql_live(live_bq):
    query = """
            get(name(), cpn())
            for(['XS2479344561 Corp', 'USX60003AC87 Corp'])
            """
    bql_result = live_bq.bql(query)

    assert isinstance(bql_result, BqlResult)
    assert len(bql_result) == 2
    assert bql_result.names == ["name()", "cpn()"]

    df = bql_result[0].join(bql_result[1], on="ID")
    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["ID", "name()", "cpn()", "MULTIPLIER", "CPN_TYP"]
    assert df.shape[0] == 2


def test_bql_with_single_quote_live(live_bq):
    query = """
            get(px_last)
            for(['IBM US Equity', 'AAPL US Equity'])
            """
    bql_result = live_bq.bql(query)

    assert isinstance(bql_result, BqlResult)
    assert bql_result.names == ["px_last"]
    df = bql_result[0]
    assert df.columns == ["ID", "px_last", "DATE", "CURRENCY"]
    assert df.shape[0] == 2


def test_issue_7_bql_with_single_quote(live_bq):
    """Regression test for apostrophes in BQL results."""
    result = live_bq.bql("for(['BFOR US Equity']) get(name)")
    assert len(result) == 1
    df = result[0]
    assert isinstance(df, pl.DataFrame)
    assert df["name"].item().startswith("Barron")


def test_issue_14_bql_with_infinity(live_bq):
    query = "let(#colP = 1 / 0; #colM = -1 / 0;) get(#colP, #colM) for('OMX Index')"
    res = live_bq.bql(query)

    assert len(res) == 2
    df = res.combine()
    assert df["#colP"].to_list() == [float("inf")]
    assert df["#colM"].to_list() == [float("-inf")]


def test_incorrect_bql_syntax_logs_error(live_bq, caplog):
    incorrect_query = "get(px_last for(['AAPL US Equity'])"
    with caplog.at_level(logging.ERROR):
        res = live_bq.bql(incorrect_query)

    assert len(res) == 0
    assert "BQL error:" in caplog.text
