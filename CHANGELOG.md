# Changelog

All notable changes to polars-bloomberg are listed here.

## Unreleased

### Added

- `BloombergError`, raised when Bloomberg rejects or fails a request. It derives
  from `ValueError`, so existing `except ValueError` and `except Exception` blocks
  still catch it.

### Changed

- Error messages for a rejected request now include Bloomberg's error category, for
  example `Response error: No securities specified (BAD_ARGS)`.
- A failed request is reported at once as `BloombergError`, and a lost session as
  `ConnectionError`. Both used to surface as a `TimeoutError` after the full wait.
- The `timeout` argument is documented as what it is: the longest wait for the next
  event from Bloomberg, not a limit on the whole request.

### Fixed

- After a `TimeoutError` or an interrupted call (for example "interrupt kernel" in a
  notebook), the next call could return the previous request's data, and every later
  call stayed one answer behind. Each call now accepts only the answer to its own
  request, and an abandoned request is cancelled.
- Session and service status messages are no longer collected as part of an answer.
