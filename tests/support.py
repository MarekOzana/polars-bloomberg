"""Test support: a scripted fake session, real blpapi test events, and recordings."""

import functools
import json
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import MagicMock

import blpapi
import blpapi.test

from polars_bloomberg import BQuery

DATA_DIR = Path(__file__).parent / "data"
OWN = object()  # marks a scripted message as the answer to the request being sent


class FakeMessage:
    """Stand-in for blpapi.Message that returns a given payload from toPy()."""

    def __init__(self, payload, message_type="Response", correlation_id=OWN):
        self.payload = payload
        self.message_type = blpapi.Name(message_type)
        self.correlation_id = correlation_id  # OWN, None (status message) or an id

    def toPy(self):
        return self.payload

    def messageType(self):
        return self.message_type

    def correlationIds(self):
        return [] if self.correlation_id is None else [self.correlation_id]


class FakeEvent:
    """Stand-in for blpapi.Event."""

    def __init__(self, event_type, messages=()):
        self.event_type = event_type
        self.messages = list(messages)

    def eventType(self):
        return self.event_type

    def __iter__(self):
        return iter(self.messages)


class FakeSession:
    """Scripted stand-in for blpapi.Session.

    `events` are returned by nextEvent() in order; an exception instance in the list
    is raised instead. When the list is empty, nextEvent() returns a TIMEOUT event.
    """

    def __init__(self, events=(), unavailable=()):
        self.events = list(events)
        self.unavailable = set(unavailable)  # service names that fail to open
        self.sent, self.cancelled, self.opened = [], [], []
        self.stopped = False
        self.last_cid = None

    def start(self):
        return True

    def stop(self):
        self.stopped = True

    def openService(self, name):
        self.opened.append(name)
        return name not in self.unavailable

    def getService(self, name):
        return refdata_test_service() if name == "//blp/refdata" else MagicMock()

    def sendRequest(self, request, **kwargs):
        self.sent.append(request)
        self.last_cid = blpapi.CorrelationId(len(self.sent))  # 1, 2, 3, ...
        return self.last_cid

    def nextEvent(self, timeout=0):
        if not self.events:
            return FakeEvent(blpapi.Event.TIMEOUT)
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        if isinstance(event, FakeEvent):
            for message in event:
                if message.correlation_id is OWN:
                    message.correlation_id = self.last_cid
        return event

    def cancel(self, correlation_id):
        self.cancelled.append(correlation_id)


def fake_bq(events=(), **bquery_kwargs):
    """Return a BQuery whose session is a FakeSession scripted with `events`."""
    bq = BQuery(**bquery_kwargs)
    bq.session = FakeSession(events)
    return bq


def answer(*payloads):
    """One RESPONSE event of stand-in messages: the usual scripted answer."""
    return FakeEvent(blpapi.Event.RESPONSE, [FakeMessage(p) for p in payloads])


# --- real blpapi objects, built offline with blpapi.test ---------------------------


@functools.cache
def refdata_test_service():
    xml = (DATA_DIR / "schemas" / "refdata_test.xml").read_text(encoding="utf-8")
    return blpapi.test.deserializeService(xml)


def response_event(operation, payload, event_type=blpapi.Event.RESPONSE, cid=1):
    """A real blpapi.Event with one response message of `operation`.

    `cid` must equal the number of the request it answers (FakeSession numbers the
    requests 1, 2, 3, ...). Dates in `payload` are ISO strings; toPy() returns dates.
    """
    definition = (
        refdata_test_service()
        .getOperation(blpapi.Name(operation))
        .getResponseDefinitionAt(0)
    )
    properties = blpapi.test.MessageProperties()
    properties.setCorrelationIds([blpapi.CorrelationId(cid)])
    event = blpapi.test.createEvent(event_type)
    formatter = blpapi.test.appendMessage(event, definition, properties)
    formatter.formatMessageJson(json.dumps(payload))
    return event


def status_event(event_type, message_name, payload=None, cid=None):
    """A real status event, e.g. (REQUEST_STATUS, "RequestFailure", {...}, cid=1)."""
    definition = blpapi.test.getAdminMessageDefinition(blpapi.Name(message_name))
    properties = blpapi.test.MessageProperties()
    if cid is not None:
        properties.setCorrelationIds([blpapi.CorrelationId(cid)])
    event = blpapi.test.createEvent(event_type)
    formatter = blpapi.test.appendMessage(event, definition, properties)
    if payload is not None:
        formatter.formatMessageJson(json.dumps(payload))
    return event


# --- recordings (added in step 4) ---------------------------------------------------


def _decode(obj):
    """json object_hook: restore the typed values written by the debug dump."""
    if len(obj) == 1:
        ((key, value),) = obj.items()
        if key == "$date":
            return date.fromisoformat(value)
        if key == "$datetime":
            return datetime.fromisoformat(value)
        if key == "$time":
            return time.fromisoformat(value)
        if key == "$json":  # a BQL answer: Bloomberg sends it as one JSON string
            return json.dumps(value)
    return obj


def load_recording(name):
    path = DATA_DIR / "recordings" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"), object_hook=_decode)


def replay(name, **bquery_kwargs):
    """Return (bq, recording); bq answers its next request with the recording."""
    recording = load_recording(name)
    events = [
        FakeEvent(
            getattr(blpapi.Event, event["type"]),
            [FakeMessage(m["payload"], m["type"]) for m in event["messages"]],
        )
        for event in recording["events"]
    ]
    return fake_bq(events, **bquery_kwargs), recording
