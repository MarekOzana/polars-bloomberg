Tests are split by intent.

Unit tests (offline):
- Run all unit tests: `pytest tests/unit -m no_bbg`

Integration tests (live Bloomberg):
- Run all integration tests: `pytest tests/integration -m requires_bbg`

Markers:
- `no_bbg`: safe to run without Bloomberg.
- `requires_bbg`: requires a live Bloomberg Terminal session.
