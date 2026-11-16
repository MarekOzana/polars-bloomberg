"""Real-life usage, offline: recorded answers have what consumer code relies on.

tests/usage_checks.py holds one check per `usage_` scenario of tests/recordings.py;
tests/integration/test_integration_usage.py runs the same checks on fresh answers.
The other tests here need the request that was sent or the exact recorded answer.
"""

import json
from datetime import date

import polars as pl
import pytest
from recordings import SCENARIOS
from support import (
    DATA_DIR,
    answer,
    assert_result_matches,
    fake_bq,
    load_recording,
    replay,
    scripted_events,
)
from usage_checks import CHECKS

pytestmark = pytest.mark.no_bbg


@pytest.mark.parametrize("name", list(CHECKS))
def test_recorded_answer_has_what_consumers_rely_on(name):
    bq, _ = replay(name)

    result = SCENARIOS[name](bq)

    CHECKS[name](result)


# --- bdh: dates and options --------------------------------------------------------


def test_bdh_date_objects_give_the_request_recorded_with_string_dates():
    bq, recording = replay("bdh/usage_previous_value")

    df = bq.bdh(
        ["SPX Index"],
        ["PX_LAST"],
        date(2024, 1, 5),
        date(2024, 1, 8),
        options={"nonTradingDayFillMethod": "PREVIOUS_VALUE"},
    )

    assert bq.session.sent[0].asElement().toPy() == recording["request"]["body"]
    assert_result_matches(df, recording["expected"])


def test_bdh_previous_value_fill_alone_gives_no_weekend_rows():
    bq, _ = replay("bdh/usage_previous_value")

    df = SCENARIOS["bdh/usage_previous_value"](bq)

    assert df["date"].to_list() == [date(2024, 1, 5), date(2024, 1, 8)]


def test_bdh_all_calendar_days_option_is_sent_with_the_fill_method():
    bq, recording = replay("bdh/usage_all_calendar_days")

    SCENARIOS["bdh/usage_all_calendar_days"](bq)

    sent = bq.session.sent[0].asElement().toPy()
    assert sent["nonTradingDayFillOption"] == "ALL_CALENDAR_DAYS"
    assert sent["nonTradingDayFillMethod"] == "PREVIOUS_VALUE"
    assert sent == recording["request"]["body"]


# --- bql: grouped statistics -------------------------------------------------------


def test_grouped_statistics_recording_has_two_complete_groups():
    bq, _ = replay("bql/usage_grouped_statistics")

    df = SCENARIOS["bql/usage_grouped_statistics"](bq).combine()

    assert df.select("BB_COMPOSITE()", "#IS_COCO", "#n").rows() == [
        ("BBB", True, 1),
        ("B+", False, 1),
    ]


def _grouped_statistics_with_an_unrated_group():
    """The recorded grouped answer, edited as if the B+ bond had lost its rating."""
    recording = load_recording("bql/usage_grouped_statistics")
    payload = json.loads(recording["events"][0]["messages"][0]["payload"])
    for item in payload["results"].values():
        for column in item["secondaryColumns"]:
            if column["name"] == "BB_COMPOSITE()":
                column["values"][1] = None
    bq = fake_bq([answer(json.dumps(payload))])
    return SCENARIOS["bql/usage_grouped_statistics"](bq)


def test_combine_splits_a_group_whose_rating_is_null():
    result = _grouped_statistics_with_an_unrated_group()

    df = result.combine()  # joins on BB_COMPOSITE() too, and nulls do not match

    unrated = df.filter(pl.col("BB_COMPOSITE()").is_null())
    assert unrated.height == 3  # one row per item instead of one row
    assert df.height == 4


def test_combine_on_id_keeps_one_row_for_a_group_whose_rating_is_null():
    result = _grouped_statistics_with_an_unrated_group()

    df = result.combine(on="ID")

    assert df.height == 2
    assert df.filter(pl.col("ID") == "B+:false").select("#n", "#med", "#q3").row(0) == (
        1,
        pytest.approx(159.87504),
        pytest.approx(159.87504),
    )


# --- bql: axes ---------------------------------------------------------------------


def test_security_axes_combined_on_id_keep_one_row_per_bond_with_quotes():
    # A bond screen with axes() at security grain, recorded before the recordings
    # of tests/data/recordings/ existed; it goes through bql() end to end here.
    fixture = json.loads(
        (DATA_DIR / "bql" / "bql_parse_results_axes_addcolsALL.json").read_text(
            encoding="utf-8"
        )
    )
    payload = {"results": fixture["in_results"], "responseExceptions": None}
    bq = fake_bq([answer(json.dumps(payload))])

    result = bq.bql(fixture["query"])
    df = result.combine(on="ID", how="left")

    assert result.names == ["name", "#ax"]
    assert df.height == result[0].height == 8
    assert df["ID"].is_unique().all()
    assert df.schema["ASK_PRICE"] == pl.Float64
    assert df.schema["ASK_DEPTH"] == pl.Int64
    assert df.schema["ASK_DEALER"] == pl.String
    assert df.schema["ASK_SPREAD_IS_DERIVED"] == pl.Boolean
    assert df["BID_PRICE"].null_count() < df.height


# --- one session, several requests ------------------------------------------------


def test_two_requests_on_one_session_each_get_their_own_answer():
    first, second = "bql/usage_eps_revisions", "bdh/usage_ohlc"
    events = scripted_events(load_recording(first))
    events += scripted_events(load_recording(second))
    bq = fake_bq(events)

    eps = SCENARIOS[first](bq)
    ohlc = SCENARIOS[second](bq)

    assert_result_matches(eps, load_recording(first)["expected"])
    assert_result_matches(ohlc, load_recording(second)["expected"])


def test_result_stays_usable_after_the_session_is_stopped():
    bq, _ = replay("bql/usage_scalar_and_aggregates")
    result = SCENARIOS["bql/usage_scalar_and_aggregates"](bq)

    bq.stop()

    assert bq.session is None
    assert result.combine().height == 1
