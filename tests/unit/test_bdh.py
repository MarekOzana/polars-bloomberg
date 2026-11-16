"""bdh() end to end: a scripted Bloomberg answer goes in, a DataFrame comes out."""

import logging
from datetime import date

import polars as pl
import pytest
from support import answer, fake_bq, replay, response_event

pytestmark = pytest.mark.no_bbg

LOGGER = "polars_bloomberg.plbbg"


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


def test_bdh_weekend_range_returns_empty_frame_with_columns():
    bq, _ = replay("bdh/weekend_range_no_rows")

    df = bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 6), date(2024, 1, 7))

    assert df.shape == (0, 3)
    assert df.columns == ["security", "date", "PX_LAST"]
    assert df.schema["security"] == pl.String
    assert df.schema["date"] == pl.Date


def test_bdh_invalid_security_gives_no_rows_and_warning(caplog):
    bq, _ = replay("bdh/invalid_security_mixed")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        df = bq.bdh(
            ["OMX Index", "NOT A SECURITY Equity"],
            ["PX_LAST"],
            date(2024, 1, 2),
            date(2024, 1, 5),
        )

    assert df["security"].unique().to_list() == ["OMX Index"]
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "NOT A SECURITY Equity: Unknown/Invalid" in caplog.text
    assert "(BAD_SEC)" in caplog.text


def test_bdh_invalid_field_gives_null_column_and_warning(caplog):
    bq, _ = replay("bdh/invalid_field_mixed")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        df = bq.bdh(
            ["OMX Index"], ["PX_LAST", "NOT_A_FIELD"], date(2024, 1, 2), date(2024, 1, 5)
        )

    assert df["NOT_A_FIELD"].null_count() == df.height
    assert len(caplog.records) == 1
    assert "OMX Index / NOT_A_FIELD: Invalid field (BAD_FLD)" in caplog.text


def test_bdh_valid_request_logs_nothing(caplog):
    bq, _ = replay("bdh/two_securities_two_fields")

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        bq.bdh(
            ["OMX Index", "SPX Index"],
            ["PX_LAST", "PX_VOLUME"],
            date(2024, 1, 2),
            date(2024, 1, 5),
        )

    assert caplog.records == []
