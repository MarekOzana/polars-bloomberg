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
    # --- bdh as used in real-life reports and charts (tests/unit/test_usage.py) ---
    "bdh/usage_fund_nav_aum": lambda bq: bq.bdh(
        ["SPY US Equity"],
        ["PX_LAST", "FUND_TOTAL_ASSETS"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    "bdh/usage_weekly_spreads": lambda bq: bq.bdh(
        ["I00423 Index"],
        ["BX218", "BX326"],
        date(2024, 1, 1),
        date(2024, 1, 19),
        options={"periodicitySelection": "WEEKLY"},
    ),
    "bdh/usage_ohlc": lambda bq: bq.bdh(
        ["SPX Index"],
        ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    # 6 and 7 January 2024 were a weekend
    "bdh/usage_previous_value": lambda bq: bq.bdh(
        ["SPX Index"],
        ["PX_LAST"],
        "20240105",
        "20240108",
        options={"nonTradingDayFillMethod": "PREVIOUS_VALUE"},
    ),
    "bdh/usage_all_calendar_days": lambda bq: bq.bdh(
        ["SPX Index"],
        ["PX_LAST"],
        "20240105",
        "20240108",
        options={
            "nonTradingDayFillOption": "ALL_CALENDAR_DAYS",
            "nonTradingDayFillMethod": "PREVIOUS_VALUE",
        },
    ),
    "bdh/usage_index_metrics": lambda bq: bq.bdh(
        ["I30904EU Index"],
        ["INDEX_OAS_TSY", "INDEX_YIELD_TO_WORST"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    "bdh/usage_cds_basis": lambda bq: bq.bdh(
        ["ITRXEBE Index"],
        ["PX_LAST", "CDS_INDEX_INTRINSIC_BASIS_VALUE"],
        date(2024, 1, 2),
        date(2024, 1, 5),
    ),
    "bdh/usage_fiscal_fundamentals": lambda bq: bq.bdh(
        ["IBM US Equity"],
        ["TRAIL_12M_EBITDA", "NET_DEBT"],
        date(2023, 1, 1),
        date(2024, 12, 31),
        options={"periodicitySelection": "YEARLY", "periodicityAdjustment": "FISCAL"},
    ),
    "bdh/usage_fx": lambda bq: bq.bdh(
        ["EURSEK Curncy"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 5)
    ),
    "bdh/usage_fx_hedge_cost": lambda bq: bq.bdh(
        ["FXHCEUUS Index"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 8)
    ),
    "bdh/usage_bank_price_to_book": lambda bq: bq.bdh(
        ["SX7E Index"], ["PX_TO_BOOK_RATIO"], date(2024, 1, 2), date(2024, 1, 5)
    ),
    "bdh/usage_govt_yield": lambda bq: bq.bdh(
        ["GDBR5 Index"], ["PX_LAST"], date(2024, 1, 2), date(2024, 1, 5)
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
    # axes() at security grain: converted from answers of 2024-12-17 (see CONVERTED)
    "bql/axes": lambda bq: bq.bql(
        "let(#ax=axes();) get(security_des, #ax) for(filter(bondsuniv(ACTIVE), "
        "crncy()=='USD' and basel_iii_designation() == 'Additional Tier 1' and "
        "country_iso() == 'SE'))"
    ),
    "bql/axes_addcols_all": lambda bq: bq.bql(
        "let(#ax=axes();) get(name, #ax) for(filter(bondsuniv(ACTIVE), "
        "crncy()=='USD' and basel_iii_designation() == 'Additional Tier 1' and "
        "country_iso() == 'SE' and is_axed('Bid') == True)) preferences(addcols=all)"
    ),
    # --- bql as used in real-life reports and charts (tests/unit/test_usage.py) ---
    "bql/usage_security_snapshot": lambda bq: bq.bql(
        "let(#amt=amt_outstanding(); "
        "#zsprd_EUR=spread(spread_type=Z,currency=EUR).value; "
        "#ret_1d=total_return(calc_interval=range('-1d','0d')).value/100;) "
        "get(#amt, cpn, #zsprd_EUR, #ret_1d) for(['XS2479344561 Corp'])"
    ),
    "bql/usage_grouped_statistics": lambda bq: bq.bql(
        "let(#is_coco=if(capital_contingent_security(),true,false); "
        "#grp=group(spread(spread_type=OAS),by=[bb_composite(),#is_coco]); "
        "#n=count(#grp); #med=median(#grp); #q3=quantile(#grp,0.75);) "
        "get(#n,#med,#q3) for(['XS2479344561 Corp','YX231113 Corp']) "
        "preferences(dropCols=['ORIG_IDS','ID'])"
    ),
    "bql/usage_grouped_monthly_history": lambda bq: bq.bql(
        "let(#is_coco=if(capital_contingent_security(),true,false); "
        "#s=spread(spread_type=OAS,dates=range('2024-01-31','2024-02-29',frq=M),"
        "fill=PREV); "
        "#grp=group(#s,by=[bb_composite(),#is_coco,MONTH(#s().DATE),YEAR(#s().DATE)]); "
        "#med=median(#grp);) "
        "get(#med) for(['XS2479344561 Corp','YX231113 Corp'])"
    ),
    "bql/usage_annual_fundamentals": lambda bq: bq.bql(
        "get(sales_rev_turn,ebitda) for(['IBM US Equity']) "
        "with(fa_period_type=A,fa_adjusted=N,fa_period_offset=range(-1Y,0Y),fill=PREV)"
    ),
    "bql/usage_segment_revenue": lambda bq: bq.bql(
        "let(#segment=segment_name(); "
        "#revenue=sales_Rev_turn(fpt=q,fpr=range(2024Q1,2024Q2));) "
        "get(#segment,#revenue) "
        "for(segments('GTN US Equity',type=reported,hierarchy=PRODUCT,level=1))"
    ),
    "bql/usage_eps_revisions": lambda bq: bq.bql(
        "let(#eps=headline_eps_market(fa_period_offset=1,fa_period_type=A,"
        "as_of_date=range('2024-01-05','2024-01-19',frq=W));) "
        "get(#eps) for(['SPX Index'])"
    ),
    "bql/usage_scalar_and_aggregates": lambda bq: bq.bql(
        "let(#last_price=px_last(); "
        "#12m_fwd_eps=headline_eps_market(fpt=BT,fpo=1); "
        "#avg_pe=avg(headline_pe_ratio(fpt=BT,fpo=1,fill=PREV,"
        "dates=range(-1M,0D,frq=W)));) "
        "get(#last_price,#12m_fwd_eps,#avg_pe) for('SPX Index') "
        "preferences(dropCols=['CURRENCY','DATE','AS_OF_DATE','REVISION_DATE',"
        "'PERIOD_END_DATE'])"
    ),
    "bql/usage_issuer_bonds": lambda bq: bq.bql(
        "get(name,maturity,nxt_call_dt,cpn().value,normalized_payment_rank().value) "
        "for(filter(bondsuniv(ACTIVE,consolidateduplicates=True),ticker in ['GTN']))"
    ),
    "bql/usage_issuer_lineage": lambda bq: bq.bql(
        "get(name,value(bs_tier1_com_equity_ratio,fundamentalticker,mapby=LINEAGE)"
        ".value) for(['XS2479344561 Corp'])"
    ),
    "bql/usage_index_yield_spread_history": lambda bq: bq.bql(
        "get(yield(yield_type=YTW,universe_type=PROJECTED),"
        "spread(spread_type=OAS_SOVEREIGN,universe_type=RETURNS)) "
        "for(['I30904EU Index']) "
        "with(dates=range(2024-01-05,2024-01-19,frq=W),fill=PREV,per=W)"
    ),
    "bql/usage_member_mean_per_date": lambda bq: bq.bql(
        "let(#s=spread(spread_type=Z,dates=range(2024-01-02,2024-01-04)); "
        "#d=#s().DATE; #avg_z=avg(group(#s,by=#d));) "
        "get(#avg_z) for(members('I31415 Index'))"
    ),
    "bql/usage_bank_quarterly": lambda bq: bq.bql(
        "get(bs_tier1_com_equity_ratio(),normalized_roe()) for(['SEBA SS Equity']) "
        "with(fpt='Q',fpr=range(2024Q1,2024Q2)) "
        "preferences(dropCols=['AS_OF_DATE','REVISION_DATE'])"
    ),
    "bql/usage_agency_ratings": lambda bq: bq.bql(
        "let(#sp=rating(rating_source=SP); #moody=rating(rating_source=MOODY);) "
        "get(#sp,#moody) for(['XS2479344561 Corp']) "
        "preferences(dropCols=['NORMALIZED_SCALE','DATE','SOURCE_SCALE','BASE_RATING',"
        "'RATING_SOURCE','EFFECTIVE_DATE','WATCH'])"
    ),
    "bql/usage_macro_surprise": lambda bq: bq.bql(
        "let(#actual=dropna(ACTUAL_RELEASE(dates=range(2024-01-01,2024-03-31))); "
        "#survey=dropna(BN_SURVEY_MEDIAN(dates=range(2024-01-01,2024-03-31))); "
        "#volat=dropna(FORECAST_STANDARD_DEVIATION(dates=range(2024-01-01,2024-03-31)));"
        " #surprise=(#actual - #survey) / #volat;) "
        "get(#actual,#survey,#volat,#surprise) for(['PCE CYOY Index'])"
    ),
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
    # current spreads, ratings, fundamentals, universes, or an AS_OF_DATE of today
    "bql/usage_security_snapshot",
    "bql/usage_grouped_statistics",
    "bql/usage_grouped_monthly_history",
    "bql/usage_annual_fundamentals",
    "bql/usage_segment_revenue",
    "bql/usage_scalar_and_aggregates",
    "bql/usage_issuer_bonds",
    "bql/usage_issuer_lineage",
    "bql/usage_member_mean_per_date",
    "bql/usage_agency_ratings",
}

# Converted from fixtures made before these recordings existed. Their answers (live
# dealer axes of one day) cannot be recorded again, so record.py leaves them alone.
CONVERTED = {"bql/axes", "bql/axes_addcols_all"}

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
    "bdh/usage_previous_value",
    "bql/usage_eps_revisions",
]
