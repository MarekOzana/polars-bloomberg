"""The Bloomberg scenarios recorded in tests/data/recordings/, one file per scenario.

Each scenario is one call on a BQuery. `python tests/record.py` runs it against a
Terminal and stores the request, the raw answer and the result; the offline tests
replay the answer and check that the call still gives the same result.
"""

from datetime import date, datetime, timedelta


def _last_weekday_before(day: date) -> date:
    day -= timedelta(days=1)
    while day.weekday() >= 5:  # Saturday or Sunday
        day -= timedelta(days=1)
    return day


def _last_saturday_before(day: date) -> date:
    return day - timedelta(days=(day.weekday() - 5) % 7 or 7)


# Intraday bars reach back only some months, so bdib scenarios use recent days.
D = _last_weekday_before(date.today())
SAT = _last_saturday_before(date.today())


def at(day: date, hour: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour)


US_LARGE_CAPS = (  # noqa: SIM905 - a list of 25 one-word strings reads worse
    "AAPL MSFT AMZN GOOGL META NVDA TSLA JPM V JNJ WMT PG MA HD XOM CVX KO PEP BAC PFE "
    "CSCO ORCL INTC IBM DIS"
).split()

DVD_2024 = [("DVD_START_DT", "20240101"), ("DVD_END_DT", "20241231")]


SCENARIOS = {
    # --- bdp ---
    "bdp/static_fields_two_securities": lambda bq: bq.bdp(
        ["IBM US Equity", "SEBA SS Equity"], ["NAME", "CRNCY", "EQY_INIT_PO_DT"]
    ),
    "bdp/invalid_security_mixed": lambda bq: bq.bdp(
        ["IBM US Equity", "NOT A SECURITY Equity"], ["NAME"]
    ),
    "bdp/invalid_security_only": lambda bq: bq.bdp(["NOT A SECURITY Equity"], ["NAME"]),
    "bdp/invalid_field_mixed": lambda bq: bq.bdp(
        ["IBM US Equity"], ["NAME", "NOT_A_FIELD"]
    ),
    "bdp/field_not_applicable": lambda bq: bq.bdp(
        ["IBM US Equity"], ["NAME", "YAS_ZSPREAD"]
    ),
    "bdp/lowercase_field": lambda bq: bq.bdp(["IBM US Equity"], ["name", "Crncy"]),
    "bdp/bulk_field_with_overrides": lambda bq: bq.bdp(
        ["IBM US Equity"], ["DVD_HIST"], overrides=DVD_2024
    ),
    "bdp/bulk_field_empty_for_one_security": lambda bq: bq.bdp(
        ["IBM US Equity", "BRK/A US Equity"], ["DVD_HIST"], overrides=DVD_2024
    ),
    "bdp/override_reference_date": lambda bq: bq.bdp(
        ["USDSEK Curncy"], ["SETTLE_DT"], overrides=[("REFERENCE_DATE", "20200715")]
    ),
    "bdp/invalid_override": lambda bq: bq.bdp(
        ["IBM US Equity"], ["NAME"], overrides=[("NOT_AN_OVERRIDE", "1")]
    ),
    "bdp/no_securities": lambda bq: bq.bdp([], ["NAME"]),
    "bdp/partial_responses": lambda bq: bq.bdp(
        [ticker + " US Equity" for ticker in US_LARGE_CAPS], ["CRNCY"]
    ),
    # --- bdh ---
    "bdh/two_securities_two_fields": lambda bq: bq.bdh(
        ["OMX Index", "SPX Index"],
        ["PX_LAST", "PX_VOLUME"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    "bdh/weekend_range_no_rows": lambda bq: bq.bdh(
        ["OMX Index"], ["PX_LAST"], date(2024, 1, 6), date(2024, 1, 7)
    ),
    "bdh/invalid_security_mixed": lambda bq: bq.bdh(
        ["OMX Index", "NOT A SECURITY Equity"],
        ["PX_LAST"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    "bdh/invalid_field_mixed": lambda bq: bq.bdh(
        ["OMX Index"], ["PX_LAST", "NOT_A_FIELD"], date(2024, 1, 2), date(2024, 1, 5)
    ),
    "bdh/reference_only_field": lambda bq: bq.bdh(
        ["OMX Index"], ["PX_LAST", "NAME"], date(2024, 1, 2), date(2024, 1, 5)
    ),
    # 15 January 2024 was a US holiday
    "bdh/different_holiday_calendars": lambda bq: bq.bdh(
        ["OMX Index", "SPX Index"], ["PX_LAST"], date(2024, 1, 12), date(2024, 1, 16)
    ),
    "bdh/periodicity_monthly": lambda bq: bq.bdh(
        ["OMX Index"],
        ["PX_LAST"],
        date(2024, 1, 1),
        date(2024, 3, 31),
        options={"periodicitySelection": "MONTHLY"},
    ),
    "bdh/start_after_end": lambda bq: bq.bdh(
        ["OMX Index"], ["PX_LAST"], date(2024, 1, 5), date(2024, 1, 2)
    ),
    "bdh/lowercase_field": lambda bq: bq.bdh(
        ["OMX Index"], ["px_last"], date(2024, 1, 2), date(2024, 1, 5)
    ),
    # --- bdib ---
    "bdib/trade_bars_60min": lambda bq: bq.bdib(
        "OMX Index", "TRADE", 60, at(D, 8), at(D, 12)
    ),
    "bdib/saturday_no_bars": lambda bq: bq.bdib(
        "OMX Index", "TRADE", 60, at(SAT, 8), at(SAT, 12)
    ),
    "bdib/invalid_security": lambda bq: bq.bdib(
        "NOT A SECURITY Equity", "TRADE", 60, at(D, 8), at(D, 12)
    ),
    "bdib/interval_out_of_range": lambda bq: bq.bdib(
        "OMX Index", "TRADE", 2000, at(D, 8), at(D, 12)
    ),
    # --- bql ---
    "bql/px_last_fixed_date": lambda bq: bq.bql(
        "get(px_last(dates=2024-01-02)) for(['IBM US Equity', 'SEBA SS Equity'])"
    ),
    "bql/two_items": lambda bq: bq.bql(
        "get(name, px_last(dates=2024-01-02)) for(['IBM US Equity'])"
    ),
    "bql/time_series": lambda bq: bq.bql(
        "get(px_last(dates=range(2024-01-02, 2024-01-05))) for(['IBM US Equity'])"
    ),
    "bql/syntax_error": lambda bq: bq.bql("get(px_last for(['IBM US Equity'])"),
    "bql/invalid_security_mixed": lambda bq: bq.bql(
        "get(px_last(dates=2024-01-02)) for(['IBM US Equity', 'NOT A SECURITY'])"
    ),
    "bql/invalid_item": lambda bq: bq.bql(
        "get(not_a_real_field) for(['IBM US Equity'])"
    ),
    "bql/valid_and_invalid_item": lambda bq: bq.bql(
        "get(name, not_a_real_field) for(['IBM US Equity'])"
    ),
    "bql/empty_universe": lambda bq: bq.bql(
        "get(name) for(filter(members('OMX Index'), px_last < 0))"
    ),
    "bql/boolean_value": lambda bq: bq.bql(
        "get(px_last(dates=2024-01-02) > 100) for(['IBM US Equity'])"
    ),
    "bql/date_value": lambda bq: bq.bql("get(nxt_call_dt()) for(['USX60003AC87 Corp'])"),
    "bql/int_value": lambda bq: bq.bql(
        "get(count(group(id))) for(['IBM US Equity', 'SEBA SS Equity'])"
    ),
    "bql/nan_and_infinity": lambda bq: bq.bql(
        "let(#p = 1 / 0; #m = -1 / 0; #n = 0 / 0;) get(#p, #m, #n) for('OMX Index')"
    ),
    "bql/item_named_id": lambda bq: bq.bql("get(ID) for(['IBM US Equity'])"),
    "bql/item_named_date": lambda bq: bq.bql(
        "let(#DATE = px_last(dates=2024-01-02);) get(#DATE) for(['IBM US Equity'])"
    ),
    "bql/apostrophe_in_value": lambda bq: bq.bql("get(name) for(['BFOR US Equity'])"),
    # --- bsrch ---
    "bsrch/coco_limit_2": lambda bq: bq.bsrch("FI:SRCHEX.@COCO", {"LIMIT": 2}),
    "bsrch/invalid_domain": lambda bq: bq.bsrch("FI:NOT_A_SEARCH"),
    "bsrch/bi_template_limit_3": lambda bq: bq.bsrch(
        "BI:TPD", {"BIKEY": "DKOCVGXJVU8II8M90W8JSQEKR", "LIMIT": 3}
    ),
}

# Data that differs on every recording: only the schema of the result is checked.
VOLATILE = {
    "bdp/partial_responses",  # Bloomberg returns the securities in a different order
    "bdib/trade_bars_60min",
    "bdib/saturday_no_bars",
    "bsrch/coco_limit_2",
    "bsrch/bi_template_limit_3",
}

# Scenarios that the live contract tests compare with a fresh answer from Bloomberg.
CONTRACT = [
    "bdp/static_fields_two_securities",
    "bdp/invalid_security_mixed",
    "bdp/invalid_field_mixed",
    "bdh/two_securities_two_fields",
    "bdib/trade_bars_60min",
    "bql/px_last_fixed_date",
    "bql/syntax_error",
    "bsrch/coco_limit_2",
]
