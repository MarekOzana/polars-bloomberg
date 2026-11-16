"""The requests that bdp, bdh, bdib and bql build before sending them."""

from datetime import UTC, date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import blpapi
import pytest
from support import answer, fake_bq

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def request_sent_by(call):
    """Run `call(bq)` on a fake session and return the request it sent, as a dict.

    The request is built on the reduced test schema of //blp/refdata, so it is checked
    the way Bloomberg would check it. The scripted answer is empty.
    """
    bq = fake_bq([answer({})])
    call(bq)
    return bq.session.sent[0].asElement().toPy()


# --- bdp, bdh, bdib: real requests ---


def test_bdp_request_has_securities_and_fields():
    request = request_sent_by(
        lambda bq: bq.bdp(["OMX Index", "SPX Index"], ["PX_LAST", "NAME"])
    )

    assert request == {
        "securities": ["OMX Index", "SPX Index"],
        "fields": ["PX_LAST", "NAME"],
    }


def test_bdp_accepts_one_security_and_one_field_as_strings():
    request = request_sent_by(lambda bq: bq.bdp("OMX Index", "PX_LAST"))

    assert request == {"securities": ["OMX Index"], "fields": ["PX_LAST"]}


def test_bdh_accepts_one_security_and_one_field_as_strings():
    request = request_sent_by(
        lambda bq: bq.bdh("OMX Index", "PX_LAST", date(2024, 1, 2), date(2024, 1, 5))
    )

    assert request["securities"] == ["OMX Index"]
    assert request["fields"] == ["PX_LAST"]


def test_overrides_accept_pairs():
    request = request_sent_by(
        lambda bq: bq.bdp(
            ["OMX Index"], ["PX_LAST"], overrides=[("EQY_FUND_CRNCY", "SEK")]
        )
    )

    assert request["overrides"] == [{"fieldId": "EQY_FUND_CRNCY", "value": "SEK"}]


def test_overrides_accept_a_dict():
    request = request_sent_by(
        lambda bq: bq.bdh(
            ["OMX Index"],
            ["PX_LAST"],
            date(2024, 1, 2),
            date(2024, 1, 5),
            overrides={"EQY_FUND_CRNCY": "SEK", "BEST_FPERIOD_OVERRIDE": "1BF"},
        )
    )

    assert request["overrides"] == [
        {"fieldId": "EQY_FUND_CRNCY", "value": "SEK"},
        {"fieldId": "BEST_FPERIOD_OVERRIDE", "value": "1BF"},
    ]


def test_override_date_is_sent_as_yyyymmdd():
    request = request_sent_by(
        lambda bq: bq.bdp(
            ["USDSEK Curncy"],
            ["SETTLE_DT"],
            overrides={"REFERENCE_DATE": date(2020, 7, 15)},
        )
    )

    assert request["overrides"] == [{"fieldId": "REFERENCE_DATE", "value": "20200715"}]


def test_override_without_value_raises_type_error():
    bq = fake_bq([answer({})])

    with pytest.raises(TypeError, match="'REFERENCE_DATE' has no value"):
        bq.bdp(["USDSEK Curncy"], ["SETTLE_DT"], overrides={"REFERENCE_DATE": None})

    assert bq.session.sent == []


def test_two_character_override_string_raises_type_error():
    bq = fake_bq([answer({})])

    # One pair without the enclosing list. Unpacking each two-character string
    # would silently send the overrides ("A", "B") and ("C", "D").
    with pytest.raises(TypeError, match=r"sequence of \(name, value\) pairs"):
        bq.bdp(["OMX Index"], ["PX_LAST"], overrides=("AB", "CD"))

    assert bq.session.sent == []


def test_bdp_field_named_security_raises_value_error():
    bq = fake_bq([answer({})])

    with pytest.raises(ValueError, match="'security'"):
        bq.bdp(["OMX Index"], ["PX_LAST", "security"])

    assert bq.session.sent == []


def test_bdh_field_named_date_raises_value_error():
    bq = fake_bq([answer({})])

    with pytest.raises(ValueError, match="'date'"):
        bq.bdh(["OMX Index"], ["date"], date(2024, 1, 2), date(2024, 1, 5))

    assert bq.session.sent == []


def test_bdp_request_has_options():
    request = request_sent_by(
        lambda bq: bq.bdp(["OMX Index"], ["PX_LAST"], options={"returnEids": True})
    )

    assert request["returnEids"] is True


def test_bdh_request_has_dates_as_yyyymmdd_and_options():
    request = request_sent_by(
        lambda bq: bq.bdh(
            ["OMX Index"],
            ["PX_LAST"],
            date(2024, 1, 2),
            date(2024, 3, 28),
            options={"periodicitySelection": "MONTHLY"},
        )
    )

    assert request == {
        "securities": ["OMX Index"],
        "fields": ["PX_LAST"],
        "startDate": "20240102",
        "endDate": "20240328",
        "periodicitySelection": "MONTHLY",
    }


def test_bdh_accepts_iso_date_strings():
    request = request_sent_by(
        lambda bq: bq.bdh(["OMX Index"], ["PX_LAST"], "2024-01-02", "20240105")
    )

    assert request["startDate"] == "20240102"
    assert request["endDate"] == "20240105"


def test_bdib_accepts_a_date():
    request = request_sent_by(
        lambda bq: bq.bdib(
            "OMX Index", "TRADE", 60, date(2024, 1, 2), datetime(2024, 1, 2, 12)
        )
    )

    assert request["startDateTime"] == datetime(2024, 1, 2, 0, 0)
    assert request["endDateTime"] == datetime(2024, 1, 2, 12, 0)


def test_bdib_request_with_naive_datetimes_sends_them_unchanged():
    request = request_sent_by(
        lambda bq: bq.bdib(
            "OMX Index",
            "TRADE",
            60,
            datetime(2024, 1, 2, 8, 0),
            datetime(2024, 1, 2, 12, 0),
            overrides=[("PRICE_OVERRIDE", "SETTLE")],
            options={"gapFillInitialBar": True},
        )
    )

    assert request == {
        "security": "OMX Index",
        "eventType": "TRADE",
        "interval": 60,
        "startDateTime": datetime(2024, 1, 2, 8, 0),
        "endDateTime": datetime(2024, 1, 2, 12, 0),
        "gapFillInitialBar": True,
        "overrides": [{"fieldId": "PRICE_OVERRIDE", "value": "SETTLE"}],
    }


def test_bdib_request_with_aware_datetimes_sends_them_in_utc():
    stockholm_winter = timezone(timedelta(hours=1))

    request = request_sent_by(
        lambda bq: bq.bdib(
            "OMX Index",
            "TRADE",
            60,
            datetime(2024, 1, 2, 9, 0, tzinfo=stockholm_winter),
            datetime(2024, 1, 2, 13, 0, tzinfo=stockholm_winter),
        )
    )

    assert request["startDateTime"] == datetime(2024, 1, 2, 8, 0, tzinfo=UTC)
    assert request["endDateTime"] == datetime(2024, 1, 2, 12, 0, tzinfo=UTC)


# --- bql: the bqlsvc schema is not available offline ---


def test_create_bql_request_sets_client_context(bq: BQuery):
    session_mock = MagicMock()
    service_mock = MagicMock()
    request_mock = MagicMock()
    client_context_mock = MagicMock()

    session_mock.getService.return_value = service_mock
    service_mock.createRequest.return_value = request_mock
    request_mock.getElement.return_value = client_context_mock
    bq.session = session_mock

    expression = "get(px_last) for(['AAPL US Equity'])"
    result = bq._create_bql_request(expression)

    session_mock.getService.assert_called_once_with("//blp/bqlsvc")
    service_mock.createRequest.assert_called_once_with("sendQuery")
    request_mock.set.assert_called_once_with("expression", expression)
    client_context_mock.setElement.assert_called_once_with("appName", "EXCEL")
    assert result is request_mock


def test_create_bql_request_missing_client_context(bq: BQuery):
    session_mock = MagicMock()
    service_mock = MagicMock()
    request_mock = MagicMock()

    session_mock.getService.return_value = service_mock
    service_mock.createRequest.return_value = request_mock
    request_mock.getElement.side_effect = blpapi.NotFoundException(0, "missing")
    bq.session = session_mock

    expression = "get(px_last) for(['AAPL US Equity'])"
    result = bq._create_bql_request(expression)

    request_mock.set.assert_called_once_with("expression", expression)
    assert result is request_mock
