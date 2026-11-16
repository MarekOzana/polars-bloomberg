"""bsrch: saved searches, like =BSRCH() in Excel.

bq.bsrch(domain, overrides) runs a search saved in SRCH, or one of Bloomberg's
example searches, and gives its grid: one row per record, one column per column
title. The override LIMIT caps the number of rows of a screen.

Run:  pytest --bbg tests/integration/test_integration_bsrch.py
"""

import polars as pl
import pytest

from polars_bloomberg import BloombergError

pytestmark = pytest.mark.requires_bbg


def test_bsrch_runs_a_search_and_limit_caps_its_rows(bq):
    # @COCO is Bloomberg's example search for contingent convertible bonds.
    df = bq.bsrch("FI:SRCHEX.@COCO", overrides={"LIMIT": 2})

    assert df.columns == ["id"]
    assert df.height == 2
    assert df["id"].str.ends_with(" Corp").all()


def test_bsrch_bloomberg_intelligence_table_has_one_column_per_period(bq):
    # A Bloomberg Intelligence (BI) data table, chosen by its BIKEY: credit
    # leverage by rating and by sector. Its rows are fixed; LIMIT does not apply.
    df = bq.bsrch("BI:TPD", overrides={"BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR"})

    labels = ["Main_Category_Custom_Name", "Bclass3_Fin_Nonfin", "Category"]
    periods = df.drop(labels)  # one column per reporting date, e.g. "06/30/2025"
    assert df.columns[:3] == labels
    assert df.height > 0
    assert periods.width > 0
    assert all(dtype == pl.Float64 for dtype in periods.dtypes)


def test_bsrch_unknown_search_raises_bloomberg_error(bq):
    with pytest.raises(BloombergError, match="BSRCH error"):
        bq.bsrch("FI:NOT_A_SEARCH")
