"""bdh() end to end: a scripted Bloomberg answer goes in, a DataFrame comes out."""

from datetime import date

import polars as pl
import pytest
from support import answer, fake_bq, response_event

pytestmark = pytest.mark.no_bbg


def test_bdh_date_column_has_date_dtype():
    payload = {
        "securityData": {
            "security": "OMX Index",
            "fieldData": [{"date": "2024-01-02", "PX_LAST": 2400.5}],
        }
    }
    bq = fake_bq([response_event("HistoricalDataRequest", payload)])

    df = bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 5))

    assert df.schema["date"] == pl.Date
    assert df["date"].to_list() == [date(2024, 1, 2)]


def test_bdh_missing_field_value_is_null():
    payload = {
        "securityData": {
            "security": "OMX Index",
            "fieldData": [
                {"date": date(2024, 1, 2), "PX_LAST": 2400.5},
                {"date": date(2024, 1, 3)},  # Bloomberg leaves out fields with no value
            ],
        }
    }
    bq = fake_bq([answer(payload)])

    df = bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 3))

    assert df["PX_LAST"].to_list() == [2400.5, None]
