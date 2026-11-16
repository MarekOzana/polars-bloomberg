"""Real-life usage, live: fresh answers still have what consumer code relies on.

The checks in tests/usage_checks.py also run offline on the recorded answers
(tests/unit/test_usage.py). Axes are tested live only: which dealers send axes is
specific to the account, so their answers are not recorded.
"""

import polars as pl
import pytest
from recordings import SCENARIOS
from usage_checks import BANK_BOND, CHECKS

pytestmark = pytest.mark.requires_bbg


@pytest.mark.parametrize("name", list(CHECKS))
def test_live_answer_has_what_consumers_rely_on(name, bq):
    result = SCENARIOS[name](bq)

    CHECKS[name](result)


def _axes_or_skip(bq, query):
    result = bq.bql(query)
    assert result.errors == []
    assert result.names == ["security_des", "#ax"]
    if result[1].height == 0:
        pytest.skip(f"no axes visible for {BANK_BOND} at the moment")
    return result


def test_live_dealer_axes_keep_one_row_per_dealer_quote(bq):
    result = _axes_or_skip(
        bq,
        "let(#ax=axes(aggregateby='SECURITY_DEALER');) get(security_des,#ax) "
        f"for(['{BANK_BOND}']) preferences(addcols=all)",
    )
    axes = result[1]

    df = result.combine(on="ID", how="left")

    quote_columns = {"ASK_DEALER", "BID_DEALER", "ASK_PRICE", "BID_PRICE"}
    assert quote_columns <= set(axes.columns)
    assert axes.schema["ASK_TIMESTAMP"] == pl.Datetime  # BQL type DATETIME
    assert axes.schema["BID_TIMESTAMP"] == pl.Datetime
    assert axes["ID"].unique().to_list() == [BANK_BOND]  # repeated, once per quote
    assert df.height == axes.height
    assert df["security_des"].null_count() == 0


def test_live_security_axes_combine_on_id_to_one_row(bq):
    result = _axes_or_skip(
        bq,
        "let(#ax=axes();) get(security_des,#ax) "
        f"for(['{BANK_BOND}']) with(aggregateby=SECURITY) preferences(addcols=all)",
    )

    df = result.combine(on="ID")

    assert df["ID"].to_list() == [BANK_BOND]
    assert {"ASK_PRICE", "BID_PRICE", "ASK_DEPTH", "BID_DEPTH"} <= set(df.columns)


def test_live_capital_stack_requests_in_a_row_get_their_own_answers(bq):
    bonds = bq.bql("get(name) for(bonds('GTN US Equity',consolidateduplicates=True))")
    loans = bq.bql("get(name) for(loans('GTN US Equity'))")
    ebitda = bq.bql("get(ebitda(fpt='LTM').value) for('GTN US Equity') with(ccy=USD)")

    assert bonds.errors == loans.errors == ebitda.errors == []
    assert bonds.names == loans.names == ["name"]
    assert ebitda.names == ["ebitda(fpt='LTM').value"]
    assert bonds[0].height > 0
    assert loans[0].height > 0
    assert set(bonds[0]["ID"]).isdisjoint(loans[0]["ID"])
    assert ebitda[0]["ID"].to_list() == ["GTN US Equity"]
    assert ebitda[0]["ebitda(fpt='LTM').value"].item() > 0
