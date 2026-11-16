from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--bbg",
        action="store_true",
        help="also run the tests that need a live Bloomberg Terminal",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    if config.getoption("--bbg"):
        return
    skip = pytest.mark.skip(reason="needs a Bloomberg Terminal; run with --bbg")
    for item in items:
        if "requires_bbg" in item.keywords:
            item.add_marker(skip)
