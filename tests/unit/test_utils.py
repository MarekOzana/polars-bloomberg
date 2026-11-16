from datetime import UTC, datetime

import pytest

from polars_bloomberg import BQuery

pytestmark = pytest.mark.no_bbg


def test_format_datetime_naive():
    dt = datetime(2024, 1, 2, 9, 30)
    assert BQuery._format_datetime(dt) == "2024-01-02T09:30:00"


def test_format_datetime_tzaware():
    dt = datetime(2024, 1, 2, 9, 30, tzinfo=UTC)
    assert BQuery._format_datetime(dt) == "2024-01-02T09:30:00Z"


def test_format_datetime_passthrough_string():
    assert BQuery._format_datetime("2024-01-02T09:30:00") == "2024-01-02T09:30:00"
