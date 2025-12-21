import re

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_bloomberg.plbbg import BqlResult

pytestmark = pytest.mark.no_bbg


class TestBqlResult:
    def test_initialization(self):
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        names = ["Data1", "Data2"]
        bql_result = BqlResult(dataframes=[df1, df2], names=names)

        assert bql_result.dataframes == [df1, df2]
        assert bql_result.names == names

    def test_combine_success(self):
        df1 = pl.DataFrame({"ID": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "B"], "Value1": [1, 2], "Value2": [3, 4]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_no_common_columns(self):
        df1 = pl.DataFrame({"ID1": ["A", "B"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID2": ["A", "B"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        with pytest.raises(
            ValueError, match=re.escape("No common columns found to join on.")
        ):
            bql_result.combine()

    def test_combine_empty_dataframes(self):
        bql_result = BqlResult(dataframes=[], names=[])

        with pytest.raises(ValueError, match=re.escape("No DataFrames to combine.")):
            bql_result.combine()

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

    def test_combine_multiple_dataframes(self):
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
        df1 = pl.DataFrame({"ID": ["A", "A"], "Value1": [1, 2]})
        df2 = pl.DataFrame({"ID": ["A", "A"], "Value2": [3, 4]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "A", "A", "A"], "Value1": [1, 2, 1, 2], "Value2": [3, 3, 4, 4]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_with_different_row_counts(self):
        df1 = pl.DataFrame({"ID": ["A", "B", "C"], "Value1": [1, 2, 3]})
        df2 = pl.DataFrame({"ID": ["A", "B"], "Value2": [4, 5]})
        bql_result = BqlResult(dataframes=[df1, df2], names=["Data1", "Data2"])

        combined_df = bql_result.combine()
        expected_df = pl.DataFrame(
            {"ID": ["A", "B", "C"], "Value1": [1, 2, 3], "Value2": [4, 5, None]}
        )

        assert_frame_equal(combined_df, expected_df)

    def test_combine_single_dataframe(self):
        df = pl.DataFrame({"ID": ["A", "B", "C"], "Value": [1, 2, 3]})
        bql_result = BqlResult(dataframes=[df], names=["Data1"])

        combined_df = bql_result.combine()
        assert_frame_equal(combined_df, df)

    def test_combine_different_schemas(self):
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
