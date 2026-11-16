"""bdib: intraday bars, like =BDIB() in Excel.

bq.bdib(security, event_type, interval, start_datetime, end_datetime) gives one row
per bar of `interval` minutes: open, high, low, close, volume, numEvents and value.
`time` is the start of each bar, in UTC, as a datetime without a time zone. A start
or end without a time zone is read as UTC too.

Bloomberg keeps intraday bars for some months only, so these examples ask for the
last week.

Run:  pytest --bbg tests/integration/test_integration_bdib.py
"""

from datetime import UTC, date, datetime, time, timedelta, timezone

import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg import BloombergError

pytestmark = pytest.mark.requires_bbg

TODAY = datetime.combine(date.today(), time())  # midnight, read as UTC
WEEK_AGO = TODAY - timedelta(days=7)


def test_bdib_gives_one_row_per_bar(bq):
    df = bq.bdib(
        "OMX Index",
        event_type="TRADE",
        interval=60,  # minutes
        start_datetime=WEEK_AGO,
        end_datetime=TODAY,
    )

    assert df.columns == [
        "security",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "numEvents",
        "value",
    ]
    assert df.height > 0  # about nine hourly bars per trading day
    assert df["security"].unique().to_list() == ["OMX Index"]
    assert df["time"].is_sorted()
    assert (df["time"].dt.minute() == 0).all()  # hourly bars start on the hour


def test_bdib_bar_prices_lie_between_low_and_high(bq):
    df = bq.bdib("OMX Index", "TRADE", 60, WEEK_AGO, TODAY)

    assert df.height > 0
    assert (df["low"] <= df["open"]).all()
    assert (df["open"] <= df["high"]).all()
    assert (df["low"] <= df["close"]).all()
    assert (df["close"] <= df["high"]).all()


def test_bdib_converts_a_start_and_end_with_a_time_zone_to_utc(bq):
    stockholm_winter = timezone(timedelta(hours=1))
    start = WEEK_AGO.replace(tzinfo=UTC).astimezone(stockholm_winter)
    end = TODAY.replace(tzinfo=UTC).astimezone(stockholm_winter)

    in_stockholm_time = bq.bdib("OMX Index", "TRADE", 60, start, end)
    in_utc = bq.bdib("OMX Index", "TRADE", 60, WEEK_AGO, TODAY)

    assert in_utc.height > 0
    assert_frame_equal(in_stockholm_time, in_utc)  # the same bars


def test_bdib_unknown_security_raises_bloomberg_error(bq):
    # Unlike bdp and bdh, which give nulls and log a warning.
    with pytest.raises(BloombergError, match="Unknown/Invalid Security"):
        bq.bdib("NOT A SECURITY Equity", "TRADE", 60, WEEK_AGO, TODAY)
