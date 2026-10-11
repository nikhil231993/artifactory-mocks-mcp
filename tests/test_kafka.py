"""Reading Kafka message mocks from a zip's kafka/ folder."""

import json

import pytest

from artifactory_mocks_mcp.kafka import read_kafka, topic_key


RECORD = {
    "value": {"type": "avro", "schema": {"file": "valueSchema.avsc"}, "value": {"metadata": {"type": "Del"}}},
    "key": {"type": "avro", "schema": {"file": "keySchema.avsc"}, "value": {"cgid": "C1"}},
}


@pytest.mark.parametrize(
    "a, b",
    [
        ("customer.addresses-event-v1", "customer_addresses-event-v1"),
        ("Customer.Addresses-Event-V1", "customer.addresses-event-v1"),
    ],
)
def test_topic_key_ignores_dot_vs_underscore_and_case(a, b):
    assert topic_key(a) == topic_key(b)


def test_record_and_its_envelope_make_one_event():
    events, errors = read_kafka(
        {
            "kafka/t_one-v1/del.json": json.dumps(RECORD).encode(),
            "kafka/t_one-v1/del_envelope.json": json.dumps(RECORD["value"]["value"]).encode(),
            "kafka/t_one-v1/keySchema.avsc": b"{}",
            "mappings/x.json": b"[]",
        }
    )
    assert errors == []
    [event] = events
    assert (event.file, event.folder, event.envelope_file) == (
        "kafka/t_one-v1/del.json",
        "t_one-v1",
        "kafka/t_one-v1/del_envelope.json",
    )
    assert event.event_type == "Del"
    assert event.key.value == {"cgid": "C1"} and event.key.schema_file == "kafka/t_one-v1/keySchema.avsc"
    assert event.value.format == "avro" and event.value.schema_file == "kafka/t_one-v1/valueSchema.avsc"
    assert event.matches_topic("t.one-v1") and not event.matches_topic("t.two-v1")
    assert event.matches_event("del") and event.matches_event("DEL")  # metadata.type or file name


def test_topic_from_metadata_subject_and_bare_values():
    value = {"metadata": {"type": "Ping", "subject": "other.topic-v1"}}
    events, errors = read_kafka(
        {
            "kafka/loose.json": json.dumps(value).encode(),  # no topic folder, not a record
            "kafka/t/broken.json": b"{not json",
        }
    )
    [event] = events
    assert event.folder == "" and event.key is None and event.value.value == value
    assert event.matches_topic("other_topic-v1")
    assert errors and errors[0].startswith("kafka/t/broken.json")


def test_lone_envelope_file_is_kept():
    events, _ = read_kafka({"kafka/t/x_envelope.json": json.dumps({"metadata": {"type": "X"}}).encode()})
    assert [e.name for e in events] == ["x_envelope"]
