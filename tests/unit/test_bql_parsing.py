import json
import logging
from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import yaml

from polars_bloomberg import BQuery
from polars_bloomberg.plbbg import BqlResult, SITable

pytestmark = pytest.mark.no_bbg


def test_extract_results_with_bql_syntax_error(caplog):
    bq = BQuery()

    with open("tests/data/bql/response_bql_raised_exception.json") as f:
        responses = json.load(f)

    with caplog.at_level(logging.ERROR):
        result = bq._extract_results([responses[-1]])

    assert result == []
    assert "BQL error:" in caplog.text
    assert "Unable to parse request" in caplog.text


class TestExtractResultsBqlErrors:
    def test_multiple_exceptions_concatenated(self, caplog):
        bq = BQuery()
        response = {
            "results": None,
            "responseExceptions": [
                {"message": "First error"},
                {"message": "Second error"},
                {"message": "Third error"},
            ],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == []
        assert "BQL error: First error; Second error; Third error" in caplog.text

    def test_exception_with_missing_message_uses_internal_message(self, caplog):
        bq = BQuery()
        response = {
            "results": None,
            "responseExceptions": [
                {"internalMessage": "Internal error details"},
            ],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == []
        assert "BQL error: Internal error details" in caplog.text

    def test_exception_with_empty_message_uses_internal_message(self, caplog):
        bq = BQuery()
        response = {
            "results": None,
            "responseExceptions": [
                {"message": "", "internalMessage": "Fallback message"},
            ],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == []
        assert "BQL error: Fallback message" in caplog.text

    def test_exception_with_no_message_fields_uses_unknown(self, caplog):
        bq = BQuery()
        response = {
            "results": None,
            "responseExceptions": [
                {"type": "PARTIAL", "messageCategory": "BQL_SYNTAX_ERROR"},
            ],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == []
        assert "BQL error: Unknown error" in caplog.text

    def test_empty_exceptions_array_does_not_log(self, caplog):
        bq = BQuery()
        response = {
            "results": {"data": "some_data"},
            "responseExceptions": [],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == [{"data": "some_data"}]
        assert "BQL error" not in caplog.text

    def test_non_dict_exception_items_are_skipped(self, caplog):
        bq = BQuery()
        response = {
            "results": None,
            "responseExceptions": [
                "invalid string item",
                None,
                {"message": "Valid error"},
                123,
            ],
        }
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([response])

        assert result == []
        assert "BQL error: Valid error" in caplog.text

    def test_non_dict_response_is_skipped(self):
        bq = BQuery()
        responses = [
            "not a json string",
            {"results": {"data": "valid_data"}},
        ]
        result = bq._extract_results(responses)
        assert result == [{"data": "valid_data"}]

    def test_valid_results_extracted_when_no_exceptions(self):
        bq = BQuery()
        responses = [
            {"server": "localhost:8194"},
            {"results": {"field1": "value1"}},
            {"results": {"field2": "value2"}},
        ]
        result = bq._extract_results(responses)
        assert result == [{"field1": "value1"}, {"field2": "value2"}]

    def test_json_string_response_is_parsed(self):
        bq = BQuery()
        json_response = '{"results": {"parsed": true}, "responseExceptions": null}'
        result = bq._extract_results([json_response])
        assert result == [{"parsed": True}]

    def test_json_string_with_exception_logs_error(self, caplog):
        bq = BQuery()
        json_response = (
            '{"results": null, "responseExceptions": [{"message": "Parse error"}]}'
        )
        with caplog.at_level(logging.ERROR):
            result = bq._extract_results([json_response])

        assert result == []
        assert "BQL error: Parse error" in caplog.text


def test_parse_bql_responses():
    bq = BQuery()
    mock_responses = [
        {
            "server": "localhost:8194",
            "serverId": "bbcomm-5CG3290LGQ-3511155",
            "encryptionStatus": "Clear",
            "compressionStatus": "Uncompressed",
        },
        {"initialEndpoints": [{"address": "localhost:8194"}]},
        {"serviceName": "//blp/refdata"},
        {"serviceName": "//blp/bqlsvc"},
        """
        {
            "results": {
                "px_last": {
                    "name": "px_last",
                    "offsets": [
                        0,
                        1
                    ],
                    "namespace": "DATAITEM_DEFAULT",
                    "source": "CR",
                    "idColumn": {
                        "name": "ID",
                        "type": "STRING",
                        "rank": 0,
                        "values": [
                            "IBM US Equity",
                            "AAPL US Equity"
                        ]
                    },
                    "valuesColumn": {
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "rank": 0,
                        "values": [
                            241.28,
                            230.56
                        ]
                    },
                    "secondaryColumns": [
                        {
                            "name": "DATE",
                            "type": "DATE",
                            "rank": 0,
                            "values": [
                                "2025-08-20T00:00:00Z",
                                "2025-08-20T00:00:00Z"
                            ],
                            "defaultDate": true
                        },
                        {
                            "name": "CURRENCY",
                            "type": "STRING",
                            "rank": 0,
                            "values": [
                                "USD",
                                "USD"
                            ]
                        }
                    ],
                    "partialErrorMap": {
                        "errorIterator": null
                    },
                    "responseExceptions": [],
                    "forUniverse": true,
                    "bqlResponseInfo": null,
                    "defaultDateColumnName": null,
                    "itemPreviewStatistics": null,
                    "indexView": null
                }
            },
            "ordering": [
                {
                    "requestIndex": 0,
                    "responseName": "px_last"
                }
            ]
        }
        """,
    ]
    exp_data = {
        "ID": ["IBM US Equity", "AAPL US Equity"],
        "px_last": [241.28, 230.56],
        "DATE": [date(2025, 8, 20), date(2025, 8, 20)],
        "CURRENCY": ["USD", "USD"],
    }
    exp_schema = {
        "ID": pl.String,
        "px_last": pl.Float64,
        "DATE": pl.Date,
        "CURRENCY": pl.String,
    }

    tables: list[SITable] = bq._parse_bql_responses(mock_responses)
    assert len(tables) == 1
    tbl = tables[0]
    assert tbl.data == exp_data
    assert tbl.schema == exp_schema


def test_bql_calls_components():
    bq = BQuery()
    mock_request = MagicMock()
    tables = [
        SITable(
            name="px_last",
            data={"ID": ["IBM US Equity"], "px_last": [123.4]},
            schema={"ID": pl.Utf8, "px_last": pl.Float64},
        )
    ]

    with (
        patch.object(bq, "_create_bql_request", return_value=mock_request)
        as create_mock,
        patch.object(bq, "_send_request", return_value=["raw"]) as send_mock,
        patch.object(bq, "_parse_bql_responses", return_value=tables) as parse_mock,
    ):
        result = bq.bql("get(px_last) for('IBM US Equity')")

    assert isinstance(result, BqlResult)
    assert result.names == ["px_last"]
    assert result[0].shape == (1, 2)
    create_mock.assert_called_once_with("get(px_last) for('IBM US Equity')")
    send_mock.assert_called_once_with(mock_request)
    parse_mock.assert_called_once_with(["raw"])


def test__extract_results():
    bq = BQuery()
    payload = {
        "results": {
            "name": {
                "name": "name",
                "offsets": [0],
                "namespace": "FUNCTION_DEFAULT",
                "source": "BQLAnalyticsEngine",
                "idColumn": {
                    "name": "ID",
                    "type": "STRING",
                    "rank": 0,
                    "values": ["BFOR US Equity"],
                },
                "valuesColumn": {
                    "name": "VALUE",
                    "type": "STRING",
                    "rank": 0,
                    "values": ["Barron's 400 ETF"],
                },
                "secondaryColumns": [],
                "partialErrorMap": {"errorIterator": None},
                "responseExceptions": [],
                "forUniverse": True,
                "bqlResponseInfo": None,
                "defaultDateColumnName": None,
                "itemPreviewStatistics": None,
                "indexView": None,
            }
        },
        "ordering": [{"requestIndex": 0, "responseName": "name"}],
        "responseExceptions": None,
        "screenCounts": None,
        "payloadId": None,
    }
    responses = [
        {
            "server": "localhost:8194",
            "serverId": "bbcomm-5CG3290LGQ-3511155",
            "encryptionStatus": "Clear",
            "compressionStatus": "Uncompressed",
        },
        {"initialEndpoints": [{"address": "localhost:8194"}]},
        {"serviceName": "//blp/refdata"},
        {"serviceName": "//blp/bqlsvc"},
        json.dumps(payload),
    ]

    results = bq._extract_results(responses=responses)
    assert len(results) == 1
    assert results[0]["name"] == {
        "name": "name",
        "offsets": [0],
        "namespace": "FUNCTION_DEFAULT",
        "source": "BQLAnalyticsEngine",
        "idColumn": {
            "name": "ID",
            "type": "STRING",
            "rank": 0,
            "values": ["BFOR US Equity"],
        },
        "valuesColumn": {
            "name": "VALUE",
            "type": "STRING",
            "rank": 0,
            "values": ["Barron's 400 ETF"],
        },
        "secondaryColumns": [],
        "partialErrorMap": {"errorIterator": None},
        "responseExceptions": [],
        "forUniverse": True,
        "bqlResponseInfo": None,
        "defaultDateColumnName": None,
        "itemPreviewStatistics": None,
        "indexView": None,
    }


def test_extract_results_skips_non_dict():
    bq = BQuery()
    results = bq._extract_results([["not", "a", "dict"], {"results": {"ok": True}}])
    assert results == [{"ok": True}]


@pytest.mark.parametrize(
    "case_json_file",
    [
        "tests/data/bql/bql_parse_results_last_px.json",
        "tests/data/bql/bql_parse_results_dur_zspread.json",
        "tests/data/bql/bql_parse_results_axes.json",
        "tests/data/bql/bql_parse_results_segment.json",
        "tests/data/bql/bql_parse_results_eps.json",
        "tests/data/bql/bql_parse_results_rets.json",
        "tests/data/bql/bql_parse_results_axes_addcolsALL.json",
    ],
)
def test__parse_result_replay(case_json_file):
    with open(case_json_file) as f:
        data = json.load(f)

    in_result: dict = data["in_results"]
    out_tables: list[dict] = data["out_tables"]

    bq = BQuery()
    tables = bq._parse_result(in_result)
    for i, table in enumerate(tables):
        assert table.name == out_tables[i]["name"]
        assert table.data == out_tables[i]["data"]
        assert {k: str(v) for k, v in table.schema.items()} == out_tables[i]["schema"]


@pytest.mark.parametrize(
    "yaml_file, exp_df",
    [
        (
            "tests/data/bql/df_lst_name_dur_zspread.yaml",
            {
                "ID": [
                    "YV402592 Corp",
                    "BW924993 Corp",
                    "ZO703956 Corp",
                    "ZO703315 Corp",
                    "ZQ349286 Corp",
                    "YU819930 Corp",
                ],
                "name()": [
                    "SEB Float PERP",
                    "SEB 6 ⅞ PERP",
                    "SHBASS 4 ¾ PERP",
                    "SHBASS 4 ⅜ PERP",
                    "SEB 5 ⅛ PERP",
                    "SEB 6 ¾ PERP",
                ],
                "#dur": [0.21, 2.23, 4.94, 1.95, 0.39, 5.37],
                "DATE": [
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                ],
                "#zsprd": [232.71, 211.55, 255.85, 213.35, 185.98, 308.81],
            },
        ),
        (
            "tests/data/bql/df_lst_name_ema20_ema200_rsi.yaml",
            {
                "ID": [
                    "ERICB SS Equity",
                    "SKFB SS Equity",
                    "SEBA SS Equity",
                    "ASSAB SS Equity",
                    "SWEDA SS Equity",
                ],
                "name()": [
                    "Telefonaktiebolaget LM Ericsso",
                    "SKF AB",
                    "Skandinaviska Enskilda Banken",
                    "Assa Abloy AB",
                    "Swedbank AB",
                ],
                "#ema20": [
                    90.09,
                    214.38,
                    153.68,
                    338.82,
                    217.38,
                ],
                "CURRENCY": ["SEK", "SEK", "SEK", "SEK", "SEK"],
                "DATE": [
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                ],
                "#ema200": [
                    74.91,
                    205.17,
                    150.72,
                    316.82,
                    213.77,
                ],
                "#rsi": [
                    57.45,
                    58.40,
                    57.69,
                    55.46,
                    56.30,
                ],
            },
        ),
        (
            "tests/data/bql/df_lst_name_px_last.yaml",
            {
                "ID": ["IBM US Equity"],
                "name": ["International Business Machine"],
                "CURRENCY": ["USD"],
                "DATE": [date(2024, 12, 14)],
                "px_last": [230.82],
            },
        ),
        (
            "tests/data/bql/df_lst_name_rank_oas_nxtcall.yaml",
            {
                "ID": [
                    "YX231113 Corp",
                    "BS116983 Corp",
                    "AV438089 Corp",
                    "ZO860846 Corp",
                    "LW375188 Corp",
                ],
                "name()": [
                    "GTN 10 ½ 07/15/29",
                    "GTN 5 ⅜ 11/15/31",
                    "GTN 7 05/15/27",
                    "GTN 4 ¾ 10/15/30",
                    "GTN 5 ⅞ 07/15/26",
                ],
                "#rank": [
                    "1st Lien Secured",
                    "Sr Unsecured",
                    "Sr Unsecured",
                    "Sr Unsecured",
                    "Sr Unsecured",
                ],
                "#nxt_call": [
                    date(2026, 7, 15),
                    date(2026, 11, 15),
                    date(2024, 12, 23),
                    date(2025, 10, 15),
                    date(2025, 1, 12),
                ],
                "#oas": [597.32, 1192.83, 391.13, 1232.55, 171.7],
                "DATE": [
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                    date(2024, 12, 14),
                ],
            },
        ),
        (
            "tests/data/bql/df_lst_segment_revenue.yaml",
            {
                "#segment": [
                    "Broadcasting",
                    "Broadcasting",
                    "Production Companies",
                    "Production Companies",
                    "Other ",
                    "Other ",
                    "Adjustment",
                    "Adjustment",
                ],
                "AS_OF_DATE": [
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                ],
                "FUNDAMENTAL_TICKER": [
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                    "GTN US Equity",
                ],
                "ID": [
                    "SEG0000524428 Segment",
                    "SEG0000524428 Segment",
                    "SEG0000524437 Segment",
                    "SEG0000524437 Segment",
                    "SEG0000795330 Segment",
                    "SEG0000795330 Segment",
                    "SEG8339225113 Segment",
                    "SEG8339225113 Segment",
                ],
                "ID_DATE": [
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                    date(2024, 12, 15),
                ],
                "ORDER": ["1", "1", "2", "2", "3", "3", "4", "4"],
                "#revenue": [
                    808000000.0,
                    924000000.0,
                    18000000.0,
                    26000000.0,
                    0.0,
                    17000000.0,
                    None,
                    None,
                ],
                "CURRENCY": ["USD", "USD", "USD", "USD", "USD", "USD", "USD", "USD"],
                "PERIOD_END_DATE": [
                    date(2024, 6, 30),
                    date(2024, 9, 30),
                    date(2024, 6, 30),
                    date(2024, 9, 30),
                    date(2024, 6, 30),
                    date(2024, 9, 30),
                    date(2024, 6, 30),
                    date(2024, 9, 30),
                ],
                "REVISION_DATE": [
                    date(2024, 8, 8),
                    date(2024, 11, 8),
                    date(2024, 8, 8),
                    date(2024, 11, 8),
                    date(2024, 8, 8),
                    date(2024, 11, 8),
                    None,
                    None,
                ],
            },
        ),
    ],
)
def test_real_life_cases(yaml_file, exp_df):
    with open(yaml_file) as f:
        d_lst = yaml.safe_load(f)
    df_lst = [pl.DataFrame(dct) for dct in d_lst]
    names = [str(x) for x in list(range(len(df_lst)))]
    bql_result = BqlResult(df_lst, names=names)

    df = bql_result.combine().to_dict(as_series=False)
    assert df == exp_df
