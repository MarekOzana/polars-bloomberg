import re
from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg.plbbg import BqlResult

pytestmark = pytest.mark.no_bbg


class TestBqlResultContainer:
    def test_initialization(self):
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        names = ["Data1", "Data2"]
        bql_result = BqlResult(dataframes=[df1, df2], names=names)

        assert bql_result.dataframes == [df1, df2]
        assert bql_result.names == names

    def test_getitem(self):
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        assert_frame_equal(df1, bql_result[0])
        assert_frame_equal(df2, bql_result[1])

    def test_len(self):
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value1": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])
        assert len(bql_result) == 2

    def test_iter(self):
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["B"], "Value1": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        dataframes: list[pl.DataFrame] = list(bql_result)
        assert dataframes == [df1, df2]


def _bql_result_with_unrelated_common_columns() -> BqlResult:
    """Create a realistic BQL result where common columns are not all join keys.

    The intended business key is only ID. DATE and MULTIPLIER are repeated metadata
    columns from different BQL items and should not automatically control row matching
    when the caller explicitly asks to combine on ID.
    """
    return BqlResult(
        dataframes=[
            pl.DataFrame(
                {
                    "ID": ["AO132623 Corp", "JK317723 Corp"],
                    "#amt": [1250000000.0, 1900000000.0],
                    "CURRENCY_OF_ISSUE": ["EUR", "EUR"],
                    "MULTIPLIER": [1.0, 1.0],
                    "CURRENCY": ["EUR", "EUR"],
                }
            ),
            pl.DataFrame(
                {
                    "ID": ["AO132623 Corp", "JK317723 Corp"],
                    "cpn": [4.75, 5.88523],
                    "MULTIPLIER": [1.0, 1.0],
                    "CPN_TYP": ["VARIABLE", "VARIABLE"],
                }
            ),
            pl.DataFrame(
                {
                    "ID": ["AO132623 Corp", "JK317723 Corp"],
                    "#zspread": [185.44515182, None],
                    "DATE": [date(2026, 6, 2), date(2026, 6, 2)],
                }
            ),
            pl.DataFrame(
                {
                    "ID": ["AO132623 Corp", "JK317723 Corp"],
                    "#ret_1d": [0.0010506895419868378, 0.0],
                    "DATE": [date(2026, 6, 2), None],
                }
            ),
        ],
        names=["#amt", "cpn", "#zspread", "#ret_1d"],
    )


class TestBqlResultCombine:
    def test_combine_success(self):
        """Legacy combine() joins simple frames on their common ID column."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "B"], "Value1": [1, 2], "Value2": [3, 4]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_no_common_columns(self):
        """Legacy combine() needs at least one inferred common column."""
        df1 = pl.DataFrame({"ID1": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID2": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError, match=re.escape("No common columns found to join on.")
        ):
            bql_result.combine()

    def test_combine_empty_dataframes(self):
        """Legacy combine() raises clearly when there are no result frames."""
        bql_result = BqlResult(dataframes=[], names=[])

        with pytest.raises(ValueError, match=re.escape("No DataFrames to combine.")):
            bql_result.combine()

    def test_combine_multiple_dataframes(self):
        """Legacy combine() joins more than two frames left-to-right."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        df3 = pl.DataFrame({"ID": ["A", "B"], "Value3": [5, 6]})
        bql_result = BqlResult(
            dataframes=[df1, df2, df3], names=["Data1", "Data2", "Data3"]
        )

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "B"], "Value1": [1, 2], "Value2": [3, 4], "Value3": [5, 6]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_with_duplicate_ids(self):
        """Legacy many-to-many joins duplicate rows when inferred keys repeat."""
        df1 = pl.DataFrame({"ID": ["A", "A"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "A"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {
                "ID": ["A", "A", "A", "A"],
                "Value1": [1, 2, 1, 2],
                "Value2": [3, 3, 4, 4],
            }
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_with_different_row_counts(self):
        """Legacy combine() uses a full join, so unmatched rows are retained."""
        df1 = pl.DataFrame({"ID": ["A", "B", "C"], "Value1": [1, 2, 3]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [4, 5]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "B", "C"], "Value1": [1, 2, 3], "Value2": [4, 5, None]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_single_dataframe(self):
        """Legacy combine() is a no-op for a single result frame."""
        df = pl.DataFrame({"ID": ["A", "B", "C"], "Value": [1, 2, 3]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        combined_df = bql_result.combine()
        assert_frame_equal(combined_df, df)

    def test_combine_different_schemas(self):
        """Legacy full joins combine sparse schemas across result frames."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Name": ["Alice", "Bob"]})
        df2 = pl.DataFrame({"ID": ["B", "C"], "Age": [30, 25]})
        df3 = pl.DataFrame({"ID": ["A", "C"], "City": ["New York", "Los Angeles"]})
        bql_result = BqlResult(dataframes=[df1, df2, df3], names=["DF1", "DF2", "DF3"])

        combined_df = bql_result.combine().sort("ID")
        expected_df = pl.DataFrame(
            {
                "ID": ["A", "B", "C"],
                "Name": ["Alice", "Bob", None],
                "Age": [None, 30, 25],
                "City": ["New York", None, "Los Angeles"],
            }
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_with_missing_values(self):
        """Legacy full joins preserve missing values on matched and unmatched rows."""
        df1 = pl.DataFrame({"ID": ["A", "B", "C"], "Value1": [1, None, 3]})
        df2 = pl.DataFrame({"ID": ["B", "C", "D"], "Value2": [None, 4, 5]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["DF1", "DF2"])

        combined_df = bql_result.combine().sort("ID")
        expected_df = pl.DataFrame(
            {
                "ID": ["A", "B", "C", "D"],
                "Value1": [1, None, 3, None],
                "Value2": [None, None, 4, 5],
            }
        )

        assert_frame_equal(combined_df, expected_df)

    def test_bql_result_combine(self):
        """Legacy combine() behavior is preserved, including the known unsafe DATE join.

        This test intentionally documents the old behavior: combine() with no arguments
        joins on every common column it finds. The last two BQL item frames both contain
        DATE, so JK317723 Corp is split into two rows when DATE differs/nulls do not
        match. The resulting 3 rows are backward-compatible legacy behavior, not the
        recommended way to combine this shape of BQL data.
        """
        res = _bql_result_with_unrelated_common_columns()

        # No arguments means legacy mode. This must not change for existing users.
        combined_df = res.combine()

        assert len(res) == 4
        assert res[0].shape == (2, 5)
        assert res[1].shape == (2, 4)
        assert res[2].shape == (2, 3)
        assert res[3].shape == (2, 3)
        expected_df = pl.DataFrame(
            {
                "ID": ["AO132623 Corp", "JK317723 Corp", "JK317723 Corp"],
                "#amt": [1250000000.0, None, 1900000000.0],
                "CURRENCY_OF_ISSUE": ["EUR", None, "EUR"],
                "MULTIPLIER": [1.0, None, 1.0],
                "CURRENCY": ["EUR", None, "EUR"],
                "cpn": [4.75, None, 5.88523],
                "CPN_TYP": ["VARIABLE", None, "VARIABLE"],
                "#zspread": [185.44515182, None, None],
                "DATE": [date(2026, 6, 2), None, date(2026, 6, 2)],
                "#ret_1d": [0.0010506895419868378, 0.0, None],
            }
        )
        # Two instruments become three rows because DATE was inferred as a join key.
        assert_frame_equal(combined_df, expected_df)


    def test_combine_on_id_list_ignores_unrelated_common_columns(self):
        """Use combine(on=["ID"]) when ID is the only intended join key.

        This is the recommended form for BQL item tables that share metadata columns
        such as DATE, CURRENCY, PERIOD, VALUE, or MULTIPLIER. Those columns are retained
        as data columns instead of being used to match rows.
        """
        res = _bql_result_with_unrelated_common_columns()

        # Explicit keys opt out of automatic common-column inference.
        combined_df = res.combine(on=["ID"])

        expected_df = pl.DataFrame(
            {
                "ID": ["AO132623 Corp", "JK317723 Corp"],
                "#amt": [1250000000.0, 1900000000.0],
                "CURRENCY_OF_ISSUE": ["EUR", "EUR"],
                "MULTIPLIER": [1.0, 1.0],
                "CURRENCY": ["EUR", "EUR"],
                "cpn": [4.75, 5.88523],
                "MULTIPLIER_cpn": [1.0, 1.0],
                "CPN_TYP": ["VARIABLE", "VARIABLE"],
                "#zspread": [185.44515182, None],
                "DATE": [date(2026, 6, 2), date(2026, 6, 2)],
                "#ret_1d": [0.0010506895419868378, 0.0],
                "DATE_#ret_1d": [date(2026, 6, 2), None],
            }
        )
        # The two requested instruments stay as two rows.
        assert combined_df.shape[0] == 2
        # Overlapping non-key columns are preserved with source-aware suffixes.
        assert_frame_equal(combined_df, expected_df)


    def test_combine_on_multiple_columns_uses_only_explicit_keys(self):
        """Use combine(on=["ID", "DATE"]) for true compound keys.

        VALUE exists in both item frames, but it is not part of the explicit key. The
        expected result keeps both VALUE columns instead of joining on VALUE.
        """
        df1 = pl.DataFrame(
            {
                "ID": ["A", "A"],
                "DATE": [date(2026, 6, 1), date(2026, 6, 2)],
                "VALUE": [10, 20],
                "LEFT_ONLY": ["x", "y"],
            }
        )
        df2 = pl.DataFrame(
            {
                "ID": ["A", "A"],
                "DATE": [date(2026, 6, 1), date(2026, 6, 2)],
                "VALUE": [100, 200],
                "RIGHT_ONLY": ["m", "n"],
            }
        )
        bql_result = BqlResult(dataframes=[df1, df2], names=["left", "right"])

        # The join key is exactly ID + DATE. VALUE is intentionally not a key.
        combined_df = bql_result.combine(on=["ID", "DATE"])

        expected_df = pl.DataFrame(
            {
                "ID": ["A", "A"],
                "DATE": [date(2026, 6, 1), date(2026, 6, 2)],
                "VALUE": [10, 20],
                "LEFT_ONLY": ["x", "y"],
                "VALUE_right": [100, 200],
                "RIGHT_ONLY": ["m", "n"],
            }
        )
        assert_frame_equal(combined_df, expected_df)


    def test_combine_on_string_is_equivalent_to_single_column_list(self):
        """Users can pass a single key as either "ID" or ["ID"]."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Name": ["Alpha", "Beta"]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Price": [10.0, 20.0]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["name", "price"])

        from_string = bql_result.combine(on="ID")
        from_list = bql_result.combine(on=["ID"])

        assert_frame_equal(from_string, from_list)


    def test_combine_on_how_full_keeps_unmatched_rows(self):
        """Use how="full" when each side may contain different IDs.

        This mirrors Polars join terminology and is the default for explicit combine().
        """
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["B", "C"], "Value2": [20, 30]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["left", "right"])

        combined_df = bql_result.combine(on="ID", how="full").sort("ID")

        expected_df = pl.DataFrame(
            {"ID": ["A", "B", "C"], "Value1": [1, 2, None], "Value2": [None, 20, 30]}
        )
        assert_frame_equal(combined_df, expected_df)


    def test_combine_on_how_inner_keeps_only_matched_rows(self):
        """Use how="inner" when only IDs present in every joined frame should remain."""
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["B", "C"], "Value2": [20, 30]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["left", "right"])

        combined_df = bql_result.combine(on="ID", how="inner")

        expected_df = pl.DataFrame({"ID": ["B"], "Value1": [2], "Value2": [20]})
        assert_frame_equal(combined_df, expected_df)

    def test_combine_without_on_rejects_how(self):
        """Legacy combine() must not silently ignore an explicit join strategy."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape(
                "The 'how' parameter requires explicit join keys via 'on'."
            ),
        ):
            bql_result.combine(how="inner")

    def test_combine_without_on_rejects_allow_common_columns_false(self):
        """Strict non-key overlap handling only makes sense with explicit keys."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape(
                "The 'allow_common_columns' parameter requires explicit join keys "
                "via 'on'."
            ),
        ):
            bql_result.combine(allow_common_columns=False)

    def test_combine_on_rejects_unsupported_how(self):
        """Explicit combine supports normal output-preserving join strategies only."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Unsupported join strategy 'semi'. Supported strategies are: "
                "['full', 'inner', 'left', 'right']."
            ),
        ):
            bql_result.combine(on="ID", how="semi")

    def test_combine_on_outer_is_rejected(self):
        """Use codebase/Polars-current how='full' instead of deprecated 'outer'."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Unsupported join strategy 'outer'. Supported strategies are: "
                "['full', 'inner', 'left', 'right']."
            ),
        ):
            bql_result.combine(on="ID", how="outer")


    def test_combine_on_missing_join_key_raises(self):
        """Explicit combine fails early when a requested key is absent in any frame."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"Security": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape("Join columns not found in DataFrame 1 (Data2): ['ID']"),
        ):
            bql_result.combine(on="ID")

    def test_combine_on_rejects_duplicate_join_keys(self):
        """Duplicate join keys are rejected before Polars raises a lower-level error."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(ValueError, match=re.escape("Join columns must be unique.")):
            bql_result.combine(on=["ID", "ID"])

    def test_combine_on_rejects_non_string_join_keys(self):
        """Join keys must be column names, not arbitrary objects."""
        df1 = pl.DataFrame({"ID": ["A"], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(ValueError, match=re.escape("Join columns must be strings.")):
            bql_result.combine(on=["ID", 1])


    def test_combine_on_empty_join_keys_raises(self):
        """An empty key list is invalid because explicit combine needs a real key."""
        df = pl.DataFrame({"ID": ["A"], "Value": [1]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        with pytest.raises(
            ValueError, match=re.escape("At least one join column must be provided.")
        ):
            bql_result.combine(on=[])


    def test_combine_on_empty_dataframes_keeps_existing_error(self):
        """Explicit combine keeps the same empty-result error as legacy combine()."""
        bql_result = BqlResult(dataframes=[], names=[])

        with pytest.raises(ValueError, match=re.escape("No DataFrames to combine.")):
            bql_result.combine(on="ID")


    def test_combine_on_single_dataframe_returns_dataframe(self):
        """Combining one frame is a no-op once the explicit key is validated."""
        df = pl.DataFrame({"ID": ["A", "B"], "Value": [1, 2]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        combined_df = bql_result.combine(on="ID")

        assert_frame_equal(combined_df, df)


    def test_combine_on_single_dataframe_validates_join_key(self):
        """Even a single-frame explicit combine validates the requested key."""
        df = pl.DataFrame({"ID": ["A", "B"], "Value": [1, 2]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        with pytest.raises(
            ValueError,
            match=re.escape("Join columns not found in DataFrame 0 (Data1): ['DATE']"),
        ):
            bql_result.combine(on="DATE")


    def test_combine_on_overlapping_non_key_columns_can_raise(self):
        """Set allow_common_columns=False to fail on ambiguous non-key overlaps.

        This is useful when a caller wants strict schemas and would rather rename columns
        manually before combining than accept automatic suffixes.
        """
        df1 = pl.DataFrame({"ID": ["A"], "DATE": [date(2026, 6, 1)], "Value1": [1]})
        df2 = pl.DataFrame({"ID": ["A"], "DATE": [date(2026, 6, 2)], "Value2": [2]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError,
            match=re.escape(
                "Overlapping non-key columns found in DataFrame 1 (Data2): ['DATE']"
            ),
        ):
            bql_result.combine(on="ID", allow_common_columns=False)


    def test_combine_on_overlapping_suffix_avoids_existing_column_names(self):
        """Automatic suffixes do not overwrite a column that already exists."""
        df1 = pl.DataFrame({"ID": ["A"], "DATE": [date(2026, 6, 1)]})
        df2 = pl.DataFrame(
            {
                "ID": ["A"],
                "DATE": [date(2026, 6, 2)],
                "DATE_Data2": [date(2026, 6, 3)],
            }
        )
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine(on="ID")

        expected_df = pl.DataFrame(
            {
                "ID": ["A"],
                "DATE": [date(2026, 6, 1)],
                "DATE_1": [date(2026, 6, 2)],
                "DATE_Data2": [date(2026, 6, 3)],
            }
        )
        assert_frame_equal(combined_df, expected_df)


    def test_combine_on_overlapping_suffix_uses_counter_for_repeated_collisions(self):
        """Repeated suffix collisions get a numeric counter instead of failing."""
        df1 = pl.DataFrame(
            {
                "ID": ["A"],
                "DATE": [date(2026, 6, 1)],
                "DATE_Data2": [date(2026, 6, 2)],
                "DATE_1": [date(2026, 6, 3)],
            }
        )
        df2 = pl.DataFrame({"ID": ["A"], "DATE": [date(2026, 6, 4)]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine(on="ID")

        expected_df = pl.DataFrame(
            {
                "ID": ["A"],
                "DATE": [date(2026, 6, 1)],
                "DATE_Data2": [date(2026, 6, 2)],
                "DATE_1": [date(2026, 6, 3)],
                "DATE_1_2": [date(2026, 6, 4)],
            }
        )
        assert_frame_equal(combined_df, expected_df)

