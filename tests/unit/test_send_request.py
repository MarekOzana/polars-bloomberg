"""The event loop shared by every request: which Bloomberg messages are the answer.

Each test scripts the events the fake session hands out and calls bdp(). The fake
session numbers requests 1, 2, 3, ...; `cid=` says which request a message answers.
"""

import blpapi
import pytest
from support import fake_bq, response_event, status_event

from polars_bloomberg import BloombergError

pytestmark = pytest.mark.no_bbg


def name_answer(security, name, cid=1, event_type=blpapi.Event.RESPONSE):
    """A real bdp answer for one security with its NAME field."""
    payload = {"securityData": [{"security": security, "fieldData": {"NAME": name}}]}
    return response_event("ReferenceDataRequest", payload, event_type, cid)


def test_late_answer_to_timed_out_request_is_not_returned_to_next_request():
    bq = fake_bq()  # nothing scripted: the first call times out
    with pytest.raises(TimeoutError):
        bq.bdp(["IBM US Equity"], ["NAME"])
    bq.session.events += [
        name_answer("IBM US Equity", "IBM", cid=1),  # arrives too late for call 1
        name_answer("SEBA SS Equity", "SEB", cid=2),
    ]

    df = bq.bdp(["SEBA SS Equity"], ["NAME"])

    assert df["security"].to_list() == ["SEBA SS Equity"]


def test_request_failure_raises_bloomberg_error_with_reason():
    reason = {
        "source": "s",
        "errorCode": 1,
        "category": "TIMEOUT",
        "description": "backend timed out",
    }
    failure = status_event(
        blpapi.Event.REQUEST_STATUS, "RequestFailure", {"reason": reason}, cid=1
    )
    bq = fake_bq([failure])

    with pytest.raises(BloombergError, match=r"backend timed out \(TIMEOUT\)"):
        bq.bdp(["IBM US Equity"], ["NAME"])


def test_session_terminated_raises_connection_error():
    bq = fake_bq([status_event(blpapi.Event.SESSION_STATUS, "SessionTerminated")])

    with pytest.raises(ConnectionError, match="session terminated"):
        bq.bdp(["IBM US Equity"], ["NAME"])


def test_status_messages_before_the_answer_are_skipped():
    service_opened = status_event(
        blpapi.Event.SERVICE_STATUS,
        "ServiceOpened",
        {"serviceName": "//blp/refdata"},
    )
    bq = fake_bq([service_opened, name_answer("IBM US Equity", "IBM")])

    df = bq.bdp(["IBM US Equity"], ["NAME"])

    assert df["NAME"].to_list() == ["IBM"]


def test_partial_and_final_messages_are_collected_in_order():
    partial = blpapi.Event.PARTIAL_RESPONSE
    bq = fake_bq(
        [
            name_answer("IBM US Equity", "IBM", event_type=partial),
            name_answer("SEBA SS Equity", "SEB"),
        ]
    )

    df = bq.bdp(["IBM US Equity", "SEBA SS Equity"], ["NAME"])

    assert df["security"].to_list() == ["IBM US Equity", "SEBA SS Equity"]


def test_timeout_cancels_the_request():
    bq = fake_bq()

    with pytest.raises(TimeoutError):
        bq.bdp(["IBM US Equity"], ["NAME"])

    assert len(bq.session.cancelled) == 1


def test_failed_cancel_does_not_hide_the_timeout():
    bq = fake_bq()

    def cancel_on_dead_session(correlation_id):
        raise blpapi.InvalidStateException("session is gone", 0)

    bq.session.cancel = cancel_on_dead_session

    with pytest.raises(TimeoutError):
        bq.bdp(["IBM US Equity"], ["NAME"])


def test_keyboard_interrupt_cancels_the_request_and_next_call_is_correct():
    bq = fake_bq([KeyboardInterrupt(), name_answer("SEBA SS Equity", "SEB", cid=2)])
    with pytest.raises(KeyboardInterrupt):
        bq.bdp(["IBM US Equity"], ["NAME"])

    df = bq.bdp(["SEBA SS Equity"], ["NAME"])

    assert bq.session.cancelled == [blpapi.CorrelationId(1)]
    assert df["security"].to_list() == ["SEBA SS Equity"]


def test_response_error_raises_bloomberg_error_with_category():
    error = {
        "source": "s",
        "code": 1,
        "category": "BAD_ARGS",
        "message": "No securities specified",
    }
    bq = fake_bq([response_event("ReferenceDataRequest", {"responseError": error})])

    with pytest.raises(
        BloombergError, match=r"Response error: No securities specified \(BAD_ARGS\)"
    ):
        bq.bdp([], ["NAME"])
