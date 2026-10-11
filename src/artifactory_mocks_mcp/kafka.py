"""Kafka message mocks in a mock zip's kafka/ folder: read them and find them by topic. No I/O.

The layout, one folder per topic (the topic name with '.' written as '_'):
  kafka/<topic>/<event>.json            the record: {"key":   {"type", "schema": {"file"}, "value"},
                                                     "value": {"type", "schema": {"file"}, "value"}}
  kafka/<topic>/<event>_envelope.json   the same message's value on its own
  kafka/<topic>/*.avsc                  the Avro schemas the records name in schema.file
"""

from __future__ import annotations

import json
import posixpath
import re
from dataclasses import dataclass
from typing import Any

KAFKA_DIR = "kafka/"
ENVELOPE_SUFFIX = "_envelope"


def topic_key(name: str) -> str:
    """Topics compare ignoring case and '.' vs '_': customer.x-v1 is folder customer_x-v1."""
    return re.sub(r"[._]", ".", name.strip().lower())


@dataclass
class KafkaPart:
    """The key or the value of a record."""

    format: str | None  # e.g. avro
    schema_file: str | None  # path in the zip, resolved from schema.file
    value: Any


@dataclass
class KafkaEvent:
    file: str  # e.g. kafka/customer_addresses-event-v1/address_delete_event.json
    folder: str  # the topic folder, '' for a file directly in kafka/
    key: KafkaPart | None
    value: KafkaPart
    envelope_file: str | None = None

    @property
    def name(self) -> str:
        return posixpath.splitext(posixpath.basename(self.file))[0]

    @property
    def metadata(self) -> dict[str, Any]:
        body = self.value.value
        meta = body.get("metadata") if isinstance(body, dict) else None
        return meta if isinstance(meta, dict) else {}

    @property
    def event_type(self) -> str | None:
        value = self.metadata.get("type")
        return value if isinstance(value, str) else None

    @property
    def customer_type(self) -> str | None:
        value = self.metadata.get("customerType")
        return value if isinstance(value, str) else None

    def matches_topic(self, topic: str) -> bool:
        """The folder or the message's metadata.subject names the topic."""
        want = topic_key(topic)
        subject = self.metadata.get("subject")
        return any(isinstance(c, str) and c and topic_key(c) == want for c in (self.folder, subject))

    def matches_event(self, event_type: str) -> bool:
        """metadata.type (DeleteAddress) or the file name (address_delete_event), any case."""
        want = event_type.strip().lower()
        return want in {(self.event_type or "").lower(), self.name.lower()}


def _is_record(doc: Any) -> bool:
    return isinstance(doc, dict) and isinstance(doc.get("value"), dict) and "value" in doc["value"]


def _part(raw: Any, base: str) -> KafkaPart | None:
    if not isinstance(raw, dict) or "value" not in raw:
        return None
    schema = raw.get("schema")
    file = schema.get("file") if isinstance(schema, dict) else None
    path = posixpath.normpath(posixpath.join(base, file)) if isinstance(file, str) and file else None
    fmt = raw.get("type")
    return KafkaPart(fmt if isinstance(fmt, str) else None, path, raw["value"])


def read_kafka(files: dict[str, bytes]) -> tuple[list[KafkaEvent], list[str]]:
    """(events, parse errors) for the JSON files under kafka/.

    A record and its _envelope file make one event. A JSON file that isn't a record, and has no
    record next to it, is taken as a bare message value.
    """
    docs: dict[str, Any] = {}
    errors: list[str] = []
    for name in sorted(files):
        if name.startswith(KAFKA_DIR) and name.lower().endswith(".json"):
            try:
                docs[name] = json.loads(files[name].decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                errors.append(f"{name}: {exc}")

    events: list[KafkaEvent] = []
    for name, doc in docs.items():
        stem, ext = posixpath.splitext(name)
        if stem.endswith(ENVELOPE_SUFFIX) and _is_record(docs.get(stem[: -len(ENVELOPE_SUFFIX)] + ext)):
            continue  # the record next to it holds the same value
        parts = name.split("/")
        folder = parts[1] if len(parts) > 2 else ""
        base = posixpath.dirname(name)
        if _is_record(doc):
            envelope = stem + ENVELOPE_SUFFIX + ext
            value = _part(doc["value"], base)
            assert value is not None  # _is_record checked it
            events.append(
                KafkaEvent(name, folder, _part(doc.get("key"), base), value, envelope if envelope in docs else None)
            )
        else:
            events.append(KafkaEvent(name, folder, None, KafkaPart(None, None, doc)))
    return events, errors
