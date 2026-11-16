"""Record Bloomberg answers for the scenarios listed in tests/recordings.py.

    python tests/record.py                        # all scenarios (Terminal needed)
    python tests/record.py bdp/ bql/syntax_error  # names starting with a prefix
    python tests/record.py --refresh-expected     # rewrite "expected" offline

A recording is tests/data/recordings/<scenario>.json: the request, the raw messages
Bloomberg sent back, and the result the call gave. --refresh-expected replays the
recorded messages and rewrites only "expected"; use it after an intended change of
output and review `git diff tests/data/recordings` before committing.
"""

import argparse
import ast
import json
import re
import tempfile
from pathlib import Path

import polars as pl
import recordings
from recordings import CONVERTED, SCENARIOS, VOLATILE
from support import DATA_DIR, replay, run

from polars_bloomberg import BqlResult, BQuery
from polars_bloomberg.plbbg import _json_default

RECORDINGS_DIR = DATA_DIR / "recordings"

# Keys of a BQL answer that change on every run and mean nothing to the parser.
BQL_KEYS_TO_DROP = ("clientContext", "responseTiming", "payloadId", "versionInfo")

# Bloomberg names the backend server that answered: in the "source" of its error
# details and inside some messages. The names change on every run and are machine
# names, so they are not committed. These patterns cover the messages.
SERVER_NAMES_IN_TEXT = [
    (re.compile(r"\[nid:\d+\]"), "[nid:<server>]"),
    (re.compile(r"TraceId : [0-9a-f]+"), "TraceId : <trace>"),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("prefixes", nargs="*", help="scenario name prefixes")
    parser.add_argument(
        "--refresh-expected",
        action="store_true",
        help="replay the recordings and rewrite their 'expected' (no Terminal)",
    )
    args = parser.parse_args()
    prefixes = tuple(args.prefixes) or ("",)  # no prefix: every scenario
    names = [name for name in SCENARIOS if name.startswith(prefixes)]
    if args.refresh_expected:
        refresh_expected(names)
    else:
        record_all(names)


def record_all(names: list[str]) -> None:
    """Record the scenarios against a live Terminal."""
    with tempfile.TemporaryDirectory() as dump_dir, BQuery(debug=dump_dir) as bq:
        for name in names:
            if name in CONVERTED:
                print(f"{name:45} kept: converted, cannot be recorded again")
                continue
            recording, _ = record(bq, Path(dump_dir), name)
            write_recording(name, recording)
            print(f"{name:45} {outcome(recording['expected'])}")


def record(bq: BQuery, dump_dir: Path, name: str) -> tuple[dict, object]:
    """Run one scenario live; return its recording and the call's result.

    `bq` must write its debug dumps to `dump_dir`.
    """
    for old_dump in dump_dir.glob("*.json"):
        old_dump.unlink()
    result = run(SCENARIOS[name], bq)
    dumps = list(dump_dir.glob("*.json"))
    if len(dumps) > 1:
        raise RuntimeError(f"{name} sent {len(dumps)} requests; record one per call")
    # No dump: the call raised before it sent anything.
    dump = json.loads(dumps[0].read_text(encoding="utf-8")) if dumps else {}
    _make_bql_payloads_readable(dump.get("events", []))
    recording = {
        "scenario": name,
        "call": CALLS[name],
        **dump,  # format, recorded, request, events
        "events": dump.get("events", []),  # also when nothing was sent
        "expected": describe(result, volatile=name in VOLATILE),
    }
    return recording, result


def refresh_expected(names: list[str]) -> None:
    """Replay each recording offline and rewrite only its 'expected' part."""
    for name in names:
        path = RECORDINGS_DIR / f"{name}.json"
        if not path.exists():
            print(f"{name:45} no recording")
            continue
        recording = json.loads(path.read_text(encoding="utf-8"))  # keeps $ markers
        bq, _ = replay(name)
        result = run(SCENARIOS[name], bq)
        recording["expected"] = describe(result, volatile=name in VOLATILE)
        write_recording(name, recording)
        print(f"{name:45} {outcome(recording['expected'])}")


def describe(result: object, volatile: bool) -> dict:
    """The JSON form of a result: a frame's schema and rows, or the exception."""
    if isinstance(result, Exception):
        return {"raises": type(result).__name__, "message": str(result)}
    if isinstance(result, BqlResult):
        return {
            "names": result.names,
            "frames": [describe(df, volatile=volatile) for df in result],
        }
    assert isinstance(result, pl.DataFrame)
    frame: dict = {"schema": {c: str(dtype) for c, dtype in result.schema.items()}}
    if not volatile:
        frame["rows"] = [list(row) for row in result.rows()]
    return frame


def outcome(expected: dict) -> str:
    """One line for the console: what the scenario gave."""
    if "raises" in expected:
        return f"raises {expected['raises']}: {expected['message']}"
    if "names" in expected:
        return f"BqlResult {expected['names']}"
    rows = expected.get("rows")
    height = "?" if rows is None else len(rows)
    return f"{height} rows x {list(expected['schema'])}"


def write_recording(name: str, recording: dict) -> None:
    path = RECORDINGS_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    _hide_error_sources(recording)
    text = json.dumps(recording, default=_json_default, indent=1, ensure_ascii=False)
    for pattern, placeholder in SERVER_NAMES_IN_TEXT:
        text = pattern.sub(placeholder, text)
    path.write_text(text + "\n", encoding="utf-8", newline="\n")


def _hide_error_sources(value: object) -> None:
    """Replace the server name in every Bloomberg error detail, in place.

    An error detail (responseError, securityError, errorInfo) is a dict with the keys
    source, code, category, message and subcategory.
    """
    if isinstance(value, dict):
        if "source" in value and "category" in value:
            value["source"] = "<server>"
        for item in value.values():
            _hide_error_sources(item)
    elif isinstance(value, list):
        for item in value:
            _hide_error_sources(item)


def _make_bql_payloads_readable(events: list[dict]) -> None:
    """Store BQL's JSON string as JSON, without the keys that change on every run.

    BQL is the only service that answers with a JSON string instead of a structure.
    """
    for event in events:
        for message in event["messages"]:
            try:
                answer = json.loads(message["payload"])
            except (TypeError, json.JSONDecodeError):
                continue  # not a JSON string: keep it as it is
            if not isinstance(answer, dict):
                continue
            for key in BQL_KEYS_TO_DROP:
                answer.pop(key, None)
            message["payload"] = {"$json": answer}


def _call_sources() -> dict[str, str]:
    """Each scenario's call as text, for example "bq.bdp(['IBM US Equity'], ...)"."""
    tree = ast.parse(Path(recordings.__file__).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == "SCENARIOS":
            return {
                ast.literal_eval(key): ast.unparse(value.body)
                for key, value in zip(node.value.keys, node.value.values, strict=True)
            }
    raise LookupError("SCENARIOS not found in recordings.py")


CALLS = _call_sources()

if __name__ == "__main__":
    main()
