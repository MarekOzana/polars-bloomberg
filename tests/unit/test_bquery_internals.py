"""What every method shares: the session lifecycle and the debug dump."""

import json
import logging
from datetime import date, datetime, time
from importlib.metadata import version
from unittest.mock import MagicMock, patch

import blpapi
import pytest
from polars.testing import assert_frame_equal
from support import (
    FakeSession,
    _decode,
    answer,
    fake_bq,
    response_event,
    scripted_events,
)

import polars_bloomberg
from polars_bloomberg import BQuery
from polars_bloomberg.plbbg import _json_default

pytestmark = pytest.mark.no_bbg


def test_enter_start_failure():
    session_mock = MagicMock()
    session_mock.start.return_value = False

    with (
        patch("polars_bloomberg.plbbg.blpapi.SessionOptions"),
        patch("polars_bloomberg.plbbg.blpapi.Session", return_value=session_mock),
    ):
        bq = BQuery()
        with pytest.raises(
            ConnectionError, match=r"Failed to start Bloomberg session\."
        ):
            bq.__enter__()


# --- session lifecycle: start(), stop(), services opened on first use ---


@pytest.fixture()
def new_session():
    """The FakeSession that the next blpapi.Session(...) call returns."""
    ibm = {"security": "IBM US Equity", "fieldData": {"NAME": "IBM"}}
    session = FakeSession([answer({"securityData": [ibm]})])
    with patch("polars_bloomberg.plbbg.blpapi.Session", return_value=session):
        yield session


def test_bdp_without_started_session_raises_connection_error():
    bq = BQuery()

    with pytest.raises(ConnectionError, match="session is not started"):
        bq.bdp(["IBM US Equity"], ["NAME"])


def test_start_and_stop_work_without_a_with_block(new_session):
    bq = BQuery().start()
    df = bq.bdp(["IBM US Equity"], ["NAME"])
    bq.stop()

    assert df["NAME"].to_list() == ["IBM"]
    assert new_session.stopped
    assert bq.session is None


def test_stop_twice_is_safe(new_session):
    bq = BQuery().start()

    bq.stop()
    bq.stop()

    assert new_session.stopped


def test_start_twice_keeps_one_session():
    with patch("polars_bloomberg.plbbg.blpapi.Session", lambda options: FakeSession()):
        bq = BQuery().start()
        first = bq.session

        bq.start()  # e.g. a notebook cell run twice

    assert bq.session is first


def test_bdp_opens_only_the_refdata_service():
    bq = fake_bq([answer({"securityData": []})])

    bq.bdp(["IBM US Equity"], ["NAME"])

    assert bq.session.opened == ["//blp/refdata"]


def test_service_is_opened_once_for_two_calls():
    bq = fake_bq([answer({"securityData": []}), answer({"securityData": []})])

    bq.bdp(["IBM US Equity"], ["NAME"])
    bq.bdp(["IBM US Equity"], ["NAME"])

    assert bq.session.opened == ["//blp/refdata"]


def test_bql_raises_connection_error_when_its_service_cannot_be_opened():
    bq = BQuery()
    bq.session = FakeSession(unavailable={"//blp/bqlsvc"})

    with pytest.raises(ConnectionError, match="//blp/bqlsvc"):
        bq.bql("get(name) for(['IBM US Equity'])")


def test_session_is_stopped_when_a_call_fails_inside_a_with_block(new_session):
    new_session.events.clear()  # no answer: the call times out

    with pytest.raises(TimeoutError), BQuery() as bq:
        bq.bdp(["IBM US Equity"], ["NAME"])

    assert new_session.stopped


# --- debug dump: one replayable JSON file per request ---


def test_debug_dump_contains_request_and_events(tmp_path):
    payload = {"securityData": [{"security": "IBM US Equity", "fieldData": {}}]}
    bq = fake_bq([answer(payload)], debug=tmp_path)

    bq.bdp(["IBM US Equity"], ["NAME"])

    (dump_file,) = tmp_path.glob("ReferenceDataRequest_*.json")
    dump = json.loads(dump_file.read_text(encoding="utf-8"))
    assert dump["request"] == {
        "operation": "ReferenceDataRequest",
        "body": {"securities": ["IBM US Equity"], "fields": ["NAME"]},
    }
    assert dump["events"] == [
        {"type": "RESPONSE", "messages": [{"type": "Response", "payload": payload}]}
    ]


def test_debug_dump_is_written_when_the_request_fails(tmp_path):
    bq = fake_bq(debug=tmp_path)  # nothing scripted: the call times out

    with pytest.raises(TimeoutError):
        bq.bdp(["IBM US Equity"], ["NAME"])

    (dump_file,) = tmp_path.glob("ReferenceDataRequest_*.json")
    assert json.loads(dump_file.read_text(encoding="utf-8"))["events"] == []


def test_debug_dump_replays_to_the_same_frame(tmp_path):
    payload = {
        "securityData": {
            "security": "OMX Index",
            "fieldData": [
                {"date": "2024-01-02", "PX_LAST": 2400.5},
                {"date": "2024-01-03", "PX_LAST": 2410.0},
            ],
        }
    }

    def omx_prices(bq):
        return bq.bdh(["OMX Index"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 3))

    df = omx_prices(
        fake_bq([response_event("HistoricalDataRequest", payload)], debug=tmp_path)
    )

    (dump_file,) = tmp_path.glob("HistoricalDataRequest_*.json")
    dump = json.loads(dump_file.read_text(encoding="utf-8"), object_hook=_decode)
    df_replayed = omx_prices(fake_bq(scripted_events(dump)))

    assert_frame_equal(df_replayed, df)


def test_unwritable_debug_directory_does_not_fail_the_request(tmp_path, caplog):
    not_a_directory = tmp_path / "file.txt"
    not_a_directory.write_text("occupied")
    payload = {"securityData": [{"security": "IBM US Equity", "fieldData": {}}]}
    bq = fake_bq([answer(payload)], debug=not_a_directory)

    with caplog.at_level(logging.WARNING):
        df = bq.bdp(["IBM US Equity"], ["NAME"])

    assert df["security"].to_list() == ["IBM US Equity"]
    assert "Could not write debug dump" in caplog.text


def test_debug_true_writes_to_debug_cases_in_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bq = fake_bq([answer({"securityData": []})], debug=True)

    bq.bdp(["IBM US Equity"], ["NAME"])

    assert len(list((tmp_path / "debug_cases").glob("*.json"))) == 1


@pytest.mark.parametrize(
    "value",
    [date(2024, 1, 2), datetime(2024, 1, 2, 9, 30, 15, 250), time(9, 30)],
    ids=["date", "datetime", "time"],
)
def test_dump_dates_and_times_read_back_with_their_types(value):
    text = json.dumps({"value": value}, default=_json_default)

    assert json.loads(text, object_hook=_decode) == {"value": value}


def test_dump_writes_other_unknown_values_as_text():
    text = json.dumps({"name": blpapi.Name("NAME")}, default=_json_default)

    assert json.loads(text) == {"name": "NAME"}


# --- package ---


def test_version_matches_package_metadata():
    assert polars_bloomberg.__version__ == version("polars-bloomberg")
