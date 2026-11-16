"""What real-life use of polars-bloomberg relies on, as checks on a call's result.

The `usage_` scenarios of tests/recordings.py are requests that reports, charts and
bond screens send in practice, reduced to one or two public instruments and a few
dates. Each check takes the result of one scenario and asserts what such code
depends on: the names and types of the columns it selects, the keys it joins on and
the relations between values. No check needs exact market data, so the same checks
run offline on the recorded answers (tests/unit/test_usage.py) and live on fresh
ones (tests/integration/test_integration_usage.py).
"""

import math
import re
from datetime import date, timedelta

import polars as pl

from polars_bloomberg import BqlResult

JAN_2, JAN_5, JAN_8 = date(2024, 1, 2), date(2024, 1, 5), date(2024, 1, 8)
BANK_BOND = "XS2479344561 Corp"  # a bank's Additional Tier 1 bond (a CoCo)

# --- bdh --------------------------------------------------------------------------


def history(df: pl.DataFrame, fields: list[str], start: date, end: date) -> None:
    """A bdh frame of one security: typed keys, unique dates in range, values."""
    assert df.columns == ["security", "date", *fields]
    assert df.schema["security"] == pl.String
    assert df.schema["date"] == pl.Date
    assert all(df.schema[field] == pl.Float64 for field in fields)
    assert df.height > 0
    assert df["security"].n_unique() == 1
    assert df.select("security", "date").is_unique().all()
    assert start <= df["date"].min()
    assert df["date"].max() <= end
    for field in fields:
        assert df[field].drop_nulls().len() > 0, f"{field} has no values"


def fund_price_and_assets(df: pl.DataFrame) -> None:
    history(df, ["PX_LAST", "FUND_TOTAL_ASSETS"], JAN_2, JAN_5)
    assert (df["PX_LAST"].drop_nulls() > 0).all()
    assert (df["FUND_TOTAL_ASSETS"].drop_nulls() > 0).all()


def weekly_index_spreads(df: pl.DataFrame) -> None:
    history(df, ["BX218", "BX326"], date(2024, 1, 1), date(2024, 1, 19))
    assert df.height >= 2
    assert (df["date"].diff().drop_nulls().dt.total_days() == 7).all()


def open_high_low_close(df: pl.DataFrame) -> None:
    history(df, ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"], JAN_2, JAN_5)
    complete = df.drop_nulls()
    assert complete.height > 0
    inside = (pl.col("PX_LOW") <= pl.min_horizontal("PX_OPEN", "PX_LAST")) & (
        pl.max_horizontal("PX_OPEN", "PX_LAST") <= pl.col("PX_HIGH")
    )
    assert complete.select(inside).to_series().all()


def all_calendar_days_filled(df: pl.DataFrame) -> None:
    history(df, ["PX_LAST"], JAN_5, JAN_8)
    assert df["date"].to_list() == [JAN_5 + timedelta(days=n) for n in range(4)]
    friday, saturday, sunday, _ = df["PX_LAST"].to_list()
    assert saturday == friday
    assert sunday == friday


def index_spread_and_yield(df: pl.DataFrame) -> None:
    history(df, ["INDEX_OAS_TSY", "INDEX_YIELD_TO_WORST"], JAN_2, JAN_5)
    both = df.drop_nulls()
    assert both.height > 0
    # Both are in percent, and the spread is a part of the yield.
    assert (both["INDEX_OAS_TSY"] > 0).all()
    assert (both["INDEX_OAS_TSY"] < both["INDEX_YIELD_TO_WORST"]).all()


def cds_index_and_basis(df: pl.DataFrame) -> None:
    history(df, ["PX_LAST", "CDS_INDEX_INTRINSIC_BASIS_VALUE"], JAN_2, JAN_5)
    assert df.drop_nulls().height > 0  # the two series share dates


def fiscal_year_fundamentals(df: pl.DataFrame) -> None:
    history(df, ["TRAIL_12M_EBITDA", "NET_DEBT"], date(2023, 1, 1), date(2024, 12, 31))
    assert df["date"].dt.year().to_list() == [2023, 2024]  # one row per fiscal year


# --- bql --------------------------------------------------------------------------


def bql_items(result: BqlResult, names: list[str]) -> None:
    """A BQL answer without errors: the items in order, each a frame with rows."""
    assert result.errors == []
    assert result.names == names
    for name, df in zip(names, result, strict=True):
        assert df.columns[:2] == ["ID", name]
        assert df.schema["ID"] == pl.String
        assert df.height > 0, f"{name} has no rows"


def security_snapshot(result: BqlResult) -> None:
    names = ["#amt", "cpn", "#zsprd_EUR", "#ret_1d"]
    bql_items(result, names)
    df = result.combine(on="ID", how="left")
    assert df["ID"].to_list() == [BANK_BOND]
    assert df.select(names).null_count().row(0) == (0, 0, 0, 0)
    # Both #amt and cpn carry MULTIPLIER; the second keeps it under its item name.
    assert {"CURRENCY_OF_ISSUE", "MULTIPLIER", "MULTIPLIER_cpn", "CPN_TYP"} <= set(
        df.columns
    )
    assert df.schema["CPN_TYP"] == pl.String


GROUP_KEYS = ["BB_COMPOSITE()", "#IS_COCO"]


def grouped_statistics(result: BqlResult) -> None:
    bql_items(result, ["#n", "#med", "#q3"])
    for df in result:
        assert df.schema["DATE"] == pl.Date
        assert df.schema["BB_COMPOSITE()"] == pl.String
        assert df.schema["#IS_COCO"] == pl.Boolean
        assert "ORIG_IDS" not in df.columns
        # dropCols=['ID'] does not drop ID: it holds the group key, not a bond.
        assert not df["ID"].str.ends_with(" Corp").any()
    assert result[0].schema["#n"] == pl.Int64
    df = result.combine()
    assert df.height == result[0].height  # one row per group
    assert df.select(GROUP_KEYS).is_unique().all()
    assert 1 <= df["#n"].sum() <= 2
    both = df.drop_nulls(["#med", "#q3"])
    assert (both["#med"] <= both["#q3"]).all()


def grouped_monthly_history(result: BqlResult) -> None:
    bql_items(result, ["#med"])
    df = result[0]
    month, year = "MONTH(#S().DATE)", "YEAR(#S().DATE)"
    assert df.schema["DATE"] == pl.Date
    assert df.schema[month] == pl.Int64
    assert df.schema[year] == pl.Int64
    assert (df[month] == df["DATE"].dt.month()).all()
    assert (df[year] == df["DATE"].dt.year()).all()
    assert df.select(*GROUP_KEYS, month, year).is_unique().all()
    assert sorted(df[month].unique()) == [1, 2]


def annual_fundamentals(result: BqlResult) -> None:
    bql_items(result, ["sales_rev_turn", "ebitda"])
    revenue, ebitda = (df.sort("PERIOD_END_DATE") for df in result)
    for df in (revenue, ebitda):
        assert df.schema["AS_OF_DATE"] == pl.Date
        assert df.schema["PERIOD_END_DATE"] == pl.Date
        assert df["PERIOD_END_DATE"].n_unique() == 2
    assert revenue["PERIOD_END_DATE"].to_list() == ebitda["PERIOD_END_DATE"].to_list()
    df = result.combine(on=["ID", "PERIOD_END_DATE"]).sort("PERIOD_END_DATE")
    assert df["PERIOD_END_DATE"].to_list() == revenue["PERIOD_END_DATE"].to_list()
    assert df["sales_rev_turn"].to_list() == revenue["sales_rev_turn"].to_list()
    assert df["ebitda"].to_list() == ebitda["ebitda"].to_list()


def segment_revenue(result: BqlResult) -> None:
    bql_items(result, ["#segment", "#revenue"])
    segments, revenue = result
    assert segments["ID"].is_unique().all()
    assert segments["#segment"].null_count() == 0
    assert revenue.schema["#revenue"] == pl.Float64
    assert revenue.schema["PERIOD_END_DATE"] == pl.Date
    quarters = [date(2024, 3, 31), date(2024, 6, 30)]
    periods = revenue.group_by("ID").agg(pl.col("PERIOD_END_DATE").sort())
    assert set(periods["ID"]) == set(segments["ID"])
    assert all(dates == quarters for dates in periods["PERIOD_END_DATE"].to_list())
    for quarter in quarters:
        reported = revenue.filter(pl.col("PERIOD_END_DATE") == quarter)["#revenue"]
        assert reported.drop_nulls().len() > 0
    df = result.combine()
    assert df.height == revenue.height  # one row per segment and quarter
    assert df["#segment"].null_count() == 0


def eps_revisions(result: BqlResult) -> None:
    bql_items(result, ["#eps"])
    df = result[0]
    assert df.schema["#eps"] == pl.Float64
    assert df["#eps"].null_count() == 0
    assert df.schema["AS_OF_DATE"] == pl.Date
    assert df.schema["PERIOD_END_DATE"] == pl.Date
    assert df.height >= 2
    assert df["AS_OF_DATE"].is_unique().all()  # one row per observation date
    assert df["AS_OF_DATE"].min() >= JAN_5
    assert df["AS_OF_DATE"].max() <= date(2024, 1, 19)
    assert df["PERIOD_END_DATE"].n_unique() == 1  # one fiscal year, seen several times


def scalar_and_aggregates(result: BqlResult) -> None:
    names = ["#last_price", "#12m_fwd_eps", "#avg_pe"]
    bql_items(result, names)
    for name, df in zip(names, result, strict=True):
        assert df.columns == ["ID", name]  # dropCols removed the metadata columns
    df = result.combine()
    assert df.columns == ["ID", *names]
    assert df.height == 1
    assert df.null_count().row(0) == (0, 0, 0, 0)


def issuer_bonds(result: BqlResult) -> None:
    types = {
        "name": pl.String,
        "maturity": pl.Date,
        "nxt_call_dt": pl.Date,
        "cpn().value": pl.Float64,
        "normalized_payment_rank().value": pl.String,
    }
    bql_items(result, list(types))
    ids = set(result[0]["ID"])
    for (name, dtype), df in zip(types.items(), result, strict=True):
        assert df.schema[name] == dtype
        assert df["ID"].is_unique().all()
        assert set(df["ID"]) == ids
    df = result.combine()
    assert df.height == len(ids)  # null dates or ranks neither drop nor repeat a bond
    assert df["name"].str.starts_with("GTN").all()


def issuer_lineage(result: BqlResult) -> None:
    cet1 = "value(bs_tier1_com_equity_ratio,fundamentalticker,mapby=LINEAGE).value"
    bql_items(result, ["name", cet1])
    df = result.combine(on="ID")
    assert df["ID"].to_list() == [BANK_BOND]  # the bond, not its issuer
    assert df.schema[cet1] == pl.Float64
    assert 0 < df[cet1].item() < 100  # the issuing bank's CET1 ratio, in percent


def index_yield_and_spread_history(result: BqlResult) -> None:
    ytw = "yield(yield_type=YTW,universe_type=PROJECTED)"
    oas = "spread(spread_type=OAS_SOVEREIGN,universe_type=RETURNS)"
    bql_items(result, [ytw, oas])
    first, second = result
    assert first["DATE"].to_list() == second["DATE"].to_list()
    df = result.combine()
    assert df.height == first.height  # the items line up on ID and DATE
    assert df.select("ID", "DATE").is_unique().all()
    assert df.select(ytw, oas).null_count().row(0) == (0, 0)


def member_mean_per_date(result: BqlResult) -> None:
    bql_items(result, ["#avg_z"])
    df = result[0]
    assert df.schema["DATE"] == pl.Date
    assert df["DATE"].to_list() == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    assert df["#avg_z"].null_count() == 0
    # One aggregate per date; ID holds the date it was grouped by, not a member.
    assert (df["ID"].str.slice(0, 10) == df["DATE"].cast(pl.String)).all()


def bank_quarterly(result: BqlResult) -> None:
    cet1, roe = "bs_tier1_com_equity_ratio()", "normalized_roe()"
    bql_items(result, [cet1, roe])
    for df in result:
        assert "AS_OF_DATE" not in df.columns
        assert "REVISION_DATE" not in df.columns
        assert df.schema["PERIOD_END_DATE"] == pl.Date
    df = result.combine().sort("PERIOD_END_DATE")
    assert df["PERIOD_END_DATE"].to_list() == [date(2024, 3, 31), date(2024, 6, 30)]
    assert df.select(cet1, roe).null_count().row(0) == (0, 0)


def agency_ratings(result: BqlResult) -> None:
    names = ["#sp", "#moody"]
    bql_items(result, names)
    for name, df in zip(names, result, strict=True):
        assert df.columns == ["ID", name]  # dropCols removed the rating metadata
        assert df.schema[name] == pl.String
        assert df["ID"].to_list() == [BANK_BOND]
    df = result.combine()
    assert df.height == 1
    assert df.select(names).null_count().row(0) != (1, 1)  # one agency at least


def macro_surprise(result: BqlResult) -> None:
    bql_items(result, ["#actual", "#survey", "#volat", "#surprise"])
    df = result.combine()  # the items may differ in length; they line up by DATE
    assert df.select("ID", "DATE").is_unique().all()
    rows = df.drop_nulls().filter(pl.col("#volat") != 0)
    assert rows.height > 0
    for actual, survey, volat, surprise in rows.select(
        "#actual", "#survey", "#volat", "#surprise"
    ).rows():
        assert math.isclose(surprise, (actual - survey) / volat, rel_tol=1e-4)


# --- bsrch ------------------------------------------------------------------------


def bi_template_grid(df: pl.DataFrame) -> None:
    assert df.height > 0
    assert df.schema["Main_Category_Custom_Name"] == pl.String
    assert df.schema["Category"] == pl.String
    quarters = [c for c in df.columns if re.fullmatch(r"\d\d/\d\d/\d{4}", c)]
    assert quarters, "no column is named after a quarter end"
    assert all(df.schema[quarter] == pl.Float64 for quarter in quarters)


CHECKS = {
    "bdh/usage_fund_nav_aum": fund_price_and_assets,
    "bdh/usage_weekly_spreads": weekly_index_spreads,
    "bdh/usage_ohlc": open_high_low_close,
    "bdh/usage_previous_value": lambda df: history(df, ["PX_LAST"], JAN_5, JAN_8),
    "bdh/usage_all_calendar_days": all_calendar_days_filled,
    "bdh/usage_index_metrics": index_spread_and_yield,
    "bdh/usage_cds_basis": cds_index_and_basis,
    "bdh/usage_fiscal_fundamentals": fiscal_year_fundamentals,
    "bdh/usage_fx": lambda df: history(df, ["PX_LAST"], JAN_2, JAN_5),
    "bdh/usage_fx_hedge_cost": lambda df: history(df, ["PX_LAST"], JAN_2, JAN_8),
    "bdh/usage_bank_price_to_book": lambda df: history(
        df, ["PX_TO_BOOK_RATIO"], JAN_2, JAN_5
    ),
    "bdh/usage_govt_yield": lambda df: history(df, ["PX_LAST"], JAN_2, JAN_5),
    "bql/usage_security_snapshot": security_snapshot,
    "bql/usage_grouped_statistics": grouped_statistics,
    "bql/usage_grouped_monthly_history": grouped_monthly_history,
    "bql/usage_annual_fundamentals": annual_fundamentals,
    "bql/usage_segment_revenue": segment_revenue,
    "bql/usage_eps_revisions": eps_revisions,
    "bql/usage_scalar_and_aggregates": scalar_and_aggregates,
    "bql/usage_issuer_bonds": issuer_bonds,
    "bql/usage_issuer_lineage": issuer_lineage,
    "bql/usage_index_yield_spread_history": index_yield_and_spread_history,
    "bql/usage_member_mean_per_date": member_mean_per_date,
    "bql/usage_bank_quarterly": bank_quarterly,
    "bql/usage_agency_ratings": agency_ratings,
    "bql/usage_macro_surprise": macro_surprise,
    "bsrch/bi_template_limit_3": bi_template_grid,
}
