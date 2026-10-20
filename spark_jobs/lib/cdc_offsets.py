"""Freeze a local CDC batch's Kafka incarnation and exclusive offset endpoints.

The shared driver directory is a POSIX control store, not an object-store URI.
This protects retries of one batch; it is not a cross-table publication protocol.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile


def _digest(value):
    """SHA-256 hex of a value serialized as sorted, compact JSON."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _validate(snapshot):
    """Reject a boundary without topic ids or with an invalid offset range per partition."""
    for key in ("topic", "cluster_id", "topic_id"):
        if not isinstance(snapshot.get(key), str) or not snapshot[key].strip():
            raise ValueError(f"Missing Kafka {key}")
    starts, ends = snapshot.get("starting_offsets"), snapshot.get("ending_offsets")
    if not isinstance(starts, dict) or not isinstance(ends, dict) or not starts or starts.keys() != ends.keys():
        raise ValueError("Offsets must cover the same nonempty partition set")
    for partition, start in starts.items():
        end = ends[partition]
        if (not isinstance(partition, str) or not partition.isdecimal()
                or str(int(partition)) != partition or type(start) is not int
                or type(end) is not int or not 0 <= start <= end):
            raise ValueError("Invalid Kafka offset range")


def _path(directory, run_id, topic):
    """Return the boundary file path for one run and topic; refuse object-store URIs."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id is required")
    if "://" in str(directory):
        raise ValueError("CDC boundary directory must be a shared POSIX directory")
    return Path(directory) / (_digest([run_id, topic]) + ".json")


def load_boundary(directory, run_id, observed):
    """Read the ingestion boundary and fail if its original source is unavailable."""
    _validate(observed)
    frozen = json.loads(_path(directory, run_id, observed["topic"]).read_text())
    unsigned = {key: value for key, value in frozen.items() if key != "checksum"}
    if frozen.get("checksum") != _digest(unsigned):
        raise ValueError("CDC boundary checksum mismatch")
    _validate(frozen)
    if frozen.get("format_version") != 1 or frozen.get("run_id") != run_id:
        raise ValueError("CDC boundary format/run mismatch")
    for key in ("topic", "cluster_id", "topic_id"):
        if frozen[key] != observed[key]:
            raise ValueError(f"Kafka source incarnation changed: {key}")
    for partition, start in frozen["starting_offsets"].items():
        if (partition not in observed["starting_offsets"]
                or observed["starting_offsets"][partition] > start
                or observed["ending_offsets"][partition] < frozen["ending_offsets"][partition]):
            raise ValueError("Frozen Kafka range lost to retention, reset or truncation")
    return frozen


def freeze_boundary(directory, run_id, observed):
    """First writer wins atomically; retry never advances a run's endpoint."""
    _validate(observed)
    path = _path(directory, run_id, observed["topic"])
    if path.exists():
        return load_boundary(directory, run_id, observed)
    path.parent.mkdir(parents=True, exist_ok=True)
    frozen = {"format_version": 1, "run_id": run_id, **observed}
    frozen["checksum"] = _digest(frozen)
    descriptor, temporary = tempfile.mkstemp(prefix=".cdc-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(frozen, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except FileExistsError:
            pass
    finally:
        os.unlink(temporary)
    return load_boundary(directory, run_id, observed)


def kafka_read_options(boundary):
    """Use explicit partitions, including an empty partition's numeric endpoint."""
    _validate(boundary)
    topic = boundary["topic"]
    return {
        "assign": json.dumps({topic: sorted(map(int, boundary["starting_offsets"]))}),
        "startingOffsets": json.dumps({topic: boundary["starting_offsets"]}),
        "endingOffsets": json.dumps({topic: boundary["ending_offsets"]}),
        "kafka.isolation.level": "read_committed",
        "failOnDataLoss": "true",
    }


def capture_kafka_boundary(spark, bootstrap, topic):
    """Capture cluster/topic identity and committed endpoints using bundled clients.

    No consumer group offsets are changed. Topic UUID requires Kafka 2.8+.
    Source recreation during capture fails rather than mixing incarnations.
    """
    jvm = spark.sparkContext._jvm
    properties = jvm.java.util.Properties()
    for key, value in {
        "bootstrap.servers": bootstrap,
        "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "isolation.level": "read_committed", "enable.auto.commit": "false",
        "allow.auto.create.topics": "false", "default.api.timeout.ms": "30000",
        "request.timeout.ms": "30000",
    }.items():
        properties.setProperty(key, value)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
    consumer = None
    try:
        topics = jvm.java.util.ArrayList()
        topics.add(topic)
        description = admin.describeTopics(topics).allTopicNames().get().get(topic)
        topic_id = str(description.topicId())
        if topic_id == str(jvm.org.apache.kafka.common.Uuid.ZERO_UUID):
            raise ValueError("Kafka broker did not expose a topic UUID")
        cluster_id = str(admin.describeCluster().clusterId().get())
        partitions = jvm.java.util.ArrayList()
        for info in description.partitions():
            partitions.add(jvm.org.apache.kafka.common.TopicPartition(topic, info.partition()))
        consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(properties)
        starts, ends = consumer.beginningOffsets(partitions), consumer.endOffsets(partitions)
        observed = {
            "topic": topic, "cluster_id": cluster_id, "topic_id": topic_id,
            "starting_offsets": {str(p.partition()): int(starts.get(p)) for p in partitions},
            "ending_offsets": {str(p.partition()): int(ends.get(p)) for p in partitions},
        }
        if str(admin.describeTopics(topics).allTopicNames().get().get(topic).topicId()) != topic_id:
            raise ValueError("Kafka topic recreated during boundary capture")
        _validate(observed)
        return observed
    finally:
        if consumer is not None:
            consumer.close()
        admin.close()
