"""bdp() end to end: a scripted Bloomberg answer goes in, a DataFrame comes out."""

import blpapi
import polars as pl
import pytest
from support import FakeEvent, FakeMessage, answer, fake_bq, response_event

pytestmark = pytest.mark.no_bbg


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
