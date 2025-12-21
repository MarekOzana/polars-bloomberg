from datetime import date

import polars as pl
import pytest

from polars_bloomberg import BQuery
from polars_bloomberg.plbbg import SITable

pytestmark = pytest.mark.no_bbg


class TestSchemaMappingAndDataConversion:
    @pytest.fixture
    def bq(self):
        return BQuery()

    @pytest.mark.parametrize(
        "schema_str, schema_exp",
        [
            (
                {"col1": "STRING", "col2": "DOUBLE"},
                {"col1": pl.String, "col2": pl.Float64},
            ),
            (
                {"col1": "INT", "col2": "DATE", "col3": "DOUBLE"},
                {"col1": pl.Int64, "col2": pl.Date, "col3": pl.Float64},
            ),
            (
                {"col1": "UNKNOWN_TYPE"},
                {"col1": pl.String},
            ),
            (
                {"name": "STRING", "age": "INT"},
                {"name": pl.Utf8, "age": pl.Int64},
            ),
            (
                {"price": "DOUBLE", "date": "DATE"},
                {"price": pl.Float64, "date": pl.Date},
            ),
            (
                {"is_active": "BOOLEAN"},
                {"is_active": pl.Boolean},
            ),
            (
                {"is_active": "boolean"},
                {"is_active": pl.Boolean},
            ),
            (
                {"is_active": "BoOlEaN"},
                {"is_active": pl.Boolean},
            ),
            (
                {"name": "STRING", "is_active": "BOOLEAN", "valid": "BOOLEAN"},
                {"name": pl.Utf8, "is_active": pl.Boolean, "valid": pl.Boolean},
            ),
            (
                {"unknown_field": "UNKNOWN"},
                {"unknown_field": pl.Utf8},
            ),
            (
                {},
                {},
            ),
        ],
    )
    def test__map_types(self, schema_str, schema_exp, bq: BQuery):
        schema = bq._map_types(schema_str)
        assert schema_exp == schema

    @pytest.mark.parametrize(
        "data, schema, exp_data",
        [
            ({}, {}, {}),
            (
                {
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                    "number_col": [1, 2.5],
                },
                {"date_col": pl.Date, "number_col": pl.Float64},
                {
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                    "number_col": [1.0, 2.5],
                },
            ),
            (
                {"date_col": [None], "number_col": ["NaN"]},
                {"date_col": pl.Date, "number_col": pl.Float64},
                {"date_col": [None], "number_col": [None]},
            ),
            (
                {
                    "string_col": ["a", "b"],
                    "int_col": [1, 2],
                    "float_col": [1.1, "NaN"],
                    "bool_col": [True, False],
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                },
                {
                    "string_col": pl.Utf8,
                    "int_col": pl.Int64,
                    "float_col": pl.Float64,
                    "bool_col": pl.Boolean,
                    "date_col": pl.Date,
                },
                {
                    "string_col": ["a", "b"],
                    "int_col": [1, 2],
                    "float_col": [1.1, None],
                    "bool_col": [True, False],
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                },
            ),
            (
                {
                    "date_col": ["2023-01-01T00:00:00Z", "2023-01-02T00:00:00Z"],
                    "number_col": ["NaN", 3.14],
                },
                {"date_col": pl.Date, "number_col": pl.Float64},
                {
                    "date_col": [date(2023, 1, 1), date(2023, 1, 2)],
                    "number_col": [None, 3.14],
                },
            ),
            (
                {
                    "is_verified": [None, True, False, False],
                    "user_role": ["admin", "user", "guest", "user"],
                },
                {"is_verified": pl.Boolean, "user_role": pl.String},
                {
                    "is_verified": [None, True, False, False],
                    "user_role": ["admin", "user", "guest", "user"],
                },
            ),
            (
                {"number_col": ["Infinity", "-Infinity", "NaN", 1.5]},
                {"number_col": pl.Float64},
                {
                    "number_col": [
                        float("inf"),
                        float("-inf"),
                        None,
                        1.5,
                    ]
                },
            ),
        ],
    )
    def test__apply_schema(self, data, schema, exp_data, bq: BQuery):
        in_table = SITable(name="test", data=data, schema=schema)
        out_table = bq._apply_schema(in_table)
        assert out_table.data == exp_data
        assert out_table.schema == schema
