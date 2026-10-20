"""Guard the CDC run's frozen offset vector across retry, producer growth and reset."""

import copy
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from spark_jobs.lib.cdc_offsets import freeze_boundary, kafka_read_options, load_boundary


def source_snapshot(**changes):
    """A captured Kafka incarnation/range, independent of manifest serialization."""
    return {
        "topic": "orders",
        "cluster_id": "cluster-A",
        "topic_id": "topic-A",
        "starting_offsets": {"0": 0, "1": 2},
        "ending_offsets": {"0": 5, "1": 7},
        **changes,
    }


def test_retry_and_validation_keep_the_first_range_when_the_producer_advances(tmp_path):
    """Two tasks must not independently adopt the new latest offset or new partitions."""
    first = freeze_boundary(tmp_path, "run-1", source_snapshot())
    before = next(tmp_path.glob("*.json")).read_bytes()
    newer = source_snapshot(
        starting_offsets={"0": 0, "1": 2, "2": 0},
        ending_offsets={"0": 20, "1": 9, "2": 3},
    )
    assert freeze_boundary(tmp_path, "run-1", newer) == first
    assert load_boundary(tmp_path, "run-1", newer) == first
    assert next(tmp_path.glob("*.json")).read_bytes() == before
    options = kafka_read_options(first)
    assert json.loads(options["assign"]) == {"orders": [0, 1]}
    assert json.loads(options["endingOffsets"]) == {"orders": {"0": 5, "1": 7}}
    assert options["kafka.isolation.level"] == "read_committed"


@pytest.mark.parametrize(
    "changes",
    [
        {"topic_id": "recreated-topic"},
        {"cluster_id": "replacement-cluster"},
        {"starting_offsets": {"0": 1, "1": 2}},
        {"ending_offsets": {"0": 4, "1": 7}},
    ],
)
def test_reset_retention_or_truncation_refuses_replay_without_rewriting_boundary(tmp_path, changes):
    """Offsets alone cannot identify source records across reset or retention."""
    freeze_boundary(tmp_path, "run-1", source_snapshot())
    before = next(tmp_path.glob("*.json")).read_bytes()
    with pytest.raises(ValueError):
        load_boundary(tmp_path, "run-1", source_snapshot(**changes))
    assert next(tmp_path.glob("*.json")).read_bytes() == before


def test_validation_cannot_create_a_missing_ingestion_boundary(tmp_path):
    """Missing metadata must fail, rather than silently validating a fresh latest range."""
    with pytest.raises(FileNotFoundError):
        load_boundary(tmp_path, "run-1", source_snapshot())
    assert not list(tmp_path.iterdir())


def test_manifest_tampering_is_detected(tmp_path):
    """An accidentally edited endpoint cannot change what a run claims it ingested."""
    freeze_boundary(tmp_path, "run-1", source_snapshot())
    path = next(tmp_path.glob("*.json"))
    corrupted = json.loads(path.read_text())
    corrupted["ending_offsets"]["0"] = 6
    path.write_text(json.dumps(corrupted))
    with pytest.raises(ValueError, match="checksum"):
        load_boundary(tmp_path, "run-1", source_snapshot())


def test_run_id_is_not_a_filesystem_path(tmp_path):
    """Airflow run identifiers can contain punctuation but cannot escape the control root."""
    frozen = freeze_boundary(tmp_path, "../../outside/run:1", source_snapshot())
    assert frozen["run_id"] == "../../outside/run:1"
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_competing_first_writers_adopt_one_complete_boundary(tmp_path):
    """Overlapping task retries cannot replace or expose half a JSON manifest."""
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(lambda _: freeze_boundary(tmp_path, "run-1", source_snapshot()), range(8))
        )
    assert all(result == results[0] for result in results)
    assert len(list(tmp_path.iterdir())) == 1


@pytest.mark.parametrize("start,end", [(-1, 5), (3, 2), (True, 5)])
def test_invalid_snapshot_offsets_are_rejected_before_writing(tmp_path, start, end):
    """Do not accept sentinel/latest offsets or malformed ranges in a frozen boundary."""
    snapshot = copy.deepcopy(source_snapshot())
    snapshot["starting_offsets"]["0"] = start
    snapshot["ending_offsets"]["0"] = end
    with pytest.raises(ValueError):
        freeze_boundary(tmp_path, "run-1", snapshot)
    assert not list(tmp_path.iterdir())
