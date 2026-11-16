"""bdp() end to end: a scripted Bloomberg answer goes in, a DataFrame comes out."""

import logging

import blpapi
import polars as pl
import pytest
from support import (
    FakeEvent,
    FakeMessage,
    answer,
    fake_bq,
    replay,
    response_event,
)

pytestmark = pytest.mark.no_bbg

LOGGER = "polars_bloomberg.plbbg"


def test_bdp_returns_one_row_per_security_with_requested_columns():
    payload = {
        "securityData": [
            {"security": "IBM US Equity", "fieldData": {"NAME": "IBM"}},
            {"security": "SEBA SS Equity", "fieldData": {"NAME": "SEB"}},
        ]
    }
    bq = fake_bq([answer(payload)])

    df = bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME"])

    assert df.columns == ["security", "NAME"]
    assert df["NAME"].to_list() == ["IBM", "SEB"]


def test_bdp_collects_rows_from_partial_and_final_events():
    def name_of(security, name):
        return {"securityData": [{"security": security, "fieldData": {"NAME": name}}]}

    ibm = name_of("IBM US Equity", "IBM")
    seb = name_of("SEBA SS Equity", "SEB")
    bq = fake_bq(
        [
            FakeEvent(blpapi.Event.PARTIAL_RESPONSE, [FakeMessage(ibm)]),
            FakeEvent(blpapi.Event.RESPONSE, [FakeMessage(seb)]),
        ]
    )

    df = bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME"])

    assert df["security"].to_list() == ["IBM US Equity", "SEBA SS Equity"]
    assert df["NAME"].to_list() == ["IBM", "SEB"]


def test_bdp_bulk_field_becomes_list_of_structs():
    member = "Member Ticker and Exchange Code"
    payload = {
        "securityData": [
            {
                "security": "OMX Index",
                "fieldData": {
                    "INDX_MEMBERS": [{member: "ABB SS"}, {member: "ERICB SS"}]
                },
            }
        ]
    }
    bq = fake_bq([response_event("ReferenceDataRequest", payload)])

    df = bq.bdp(["OMX Index"], ["INDX_MEMBERS"])

    assert df.schema["INDX_MEMBERS"] == pl.List(pl.Struct({member: pl.String}))
    assert df["INDX_MEMBERS"].to_list() == [[{member: "ABB SS"}, {member: "ERICB SS"}]]


def test_bdp_field_first_seen_after_100_rows_is_not_lost():
    securities = [f"SEC{i} Equity" for i in range(101)]
    rows = [{"security": s, "fieldData": {}} for s in securities[:100]]
    rows.append({"security": securities[100], "fieldData": {"DVD_SH_LAST": 2.5}})
    bq = fake_bq([answer({"securityData": rows})])

    df = bq.bdp(securities, ["DVD_SH_LAST"])

    assert df.shape == (101, 2)
    assert df["DVD_SH_LAST"][100] == 2.5


def test_bdp_invalid_security_gives_null_row_and_warning(caplog):
    bq, _ = replay("bdp/invalid_security_mixed")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        df = bq.bdp(["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"])

    assert df["security"].to_list() == ["IBM US Equity", "NOT A SECURITY Equity"]
    assert df["NAME"][1] is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "NOT A SECURITY Equity: Unknown/Invalid Security" in caplog.text
    assert "(BAD_SEC)" in caplog.text


def test_bdp_invalid_field_gives_null_column_and_warning(caplog):
    bq, _ = replay("bdp/invalid_field_mixed")

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        df = bq.bdp(["IBM US Equity"], ["NAME", "NOT_A_FIELD"])

    assert df.columns == ["security", "NAME", "NOT_A_FIELD"]
    assert df["NOT_A_FIELD"].to_list() == [None]
    assert len(caplog.records) == 1
    assert "IBM US Equity / NOT_A_FIELD: Field not valid (BAD_FLD)" in caplog.text


def test_bdp_valid_request_logs_nothing(caplog):
    bq, _ = replay("bdp/static_fields_two_securities")

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME", "CRNCY", "EQY_INIT_PO_DT"])

    assert caplog.records == []


def test_bdp_many_errors_are_logged_as_ten_lines_and_a_count(caplog):
    bad = {"category": "BAD_SEC", "message": "Unknown/Invalid Security"}
    securities = [f"BAD{i} Equity" for i in range(12)]
    rows = [{"security": s, "securityError": bad, "fieldData": {}} for s in securities]
    bq = fake_bq([answer({"securityData": rows})])

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        bq.bdp(securities, ["NAME"])

    assert len(caplog.records) == 1
    assert "BAD9 Equity" in caplog.text
    assert "BAD10 Equity" not in caplog.text
    assert "... and 2 more" in caplog.text


def test_bdp_no_rows_returns_empty_frame_with_columns():
    bq = fake_bq([answer({"securityData": []})])

    df = bq.bdp(["IBM US Equity"], ["NAME", "CRNCY"])

    assert df.columns == ["security", "NAME", "CRNCY"]
    assert df.height == 0
