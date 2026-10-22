"""Kiểm CDC v2 không service: seed, contract, đối soát oracle và cấu trúc DAG/harness."""

from __future__ import annotations

import ast
import json
from copy import deepcopy
from pathlib import Path

import pytest

from generator.generate_order_lifecycle_fixture import generate_fixture
from scripts.run.mutate_order_lifecycle_cdc import mutation_plan
from scripts.setup.load_order_lifecycle_walmart import bootstrap_rows
from spark_jobs.lib.data_contracts.registry import Registry
from spark_jobs.lib.data_contracts.schema import validate_record
from spark_jobs.lib.order_lifecycle_cdc import compare_events, payload_sha256

ROOT = Path(__file__).resolve().parents[4]
EPOCH = "cluster/topic-uuid"


@pytest.fixture
def plan(tmp_path):
    """Sinh fixture chuẩn bằng producer rồi dựng chuỗi mutation từ dòng loader."""
    root = tmp_path / "fixture"
    generate_fixture(root)
    sample = bootstrap_rows(root)[0]._asdict()
    return mutation_plan(sample, 11), sample


def bronze_rows(plan):
    """Dựng dữ liệu Bronze sạch theo định dạng thật để kiểm hàm thuần."""
    return [
        {
            **e,
            "payload_after": e["raw_payload"],
            "record_sha256": e["after_sha256"],
            "source_epoch": EPOCH,
            "source_lsn": 100 + n,
            "source_ts": 123456,
            "topic": "topic",
            "kafka_partition": 0,
            "kafka_offset": n,
        }
        for n, e in enumerate(plan)
    ]


def test_seed_and_contract(plan):
    """Cùng seed cho oracle như nhau, seed khác đổi khóa; c/u đúng contract thật."""
    events, sample = plan
    assert events == mutation_plan(sample, 11)
    assert events[0]["purchase_order_id"] != mutation_plan(sample, 12)[0]["purchase_order_id"]
    schema = Registry(ROOT / "contracts").entries.get("sim-native-v2/walmart/1")["schema"]
    for event in events[:2]:
        payload = json.loads(event["raw_payload"])
        assert not validate_record(
            {
                "fetched_at": sample["fetched_at"],
                "purchase_order_id": event["purchase_order_id"],
                "raw_payload": payload,
            },
            schema,
        )
        assert payload["purchaseOrderId"] == event["purchase_order_id"]
        assert event["after_sha256"] == payload_sha256(event["raw_payload"])
    assert events[0]["raw_payload"] != events[1]["raw_payload"]
    assert events[-1]["after_sha256"] is None
    assert [e["commit_order"] for e in events] == [1, 2, 3]


def test_clean_and_arrival_order(plan):
    """Thứ tự collect không có nghĩa; thứ tự Kafka theo offset mới là bằng chứng."""
    events, _ = plan
    rows = bronze_rows(events)
    assert not any(compare_events(events, rows[::-1], EPOCH).values())


@pytest.mark.parametrize(
    "kind", ["missing", "extra", "payload_mismatch", "order_mismatch", "epoch_mismatch"]
)
def test_oracle_detects_errors(plan, kind):
    """Từng lỗi độc lập phải được đối soát phát hiện, kể cả hash cột bị giả mạo."""
    events, _ = plan
    rows = bronze_rows(events)
    if kind == "missing":
        rows.pop(1)
    elif kind == "extra":
        rows.append({**rows[0], "purchase_order_id": "fake", "kafka_offset": 99})
    elif kind == "payload_mismatch":
        rows[0]["payload_after"] = "payload bị sửa nhưng vẫn giữ hash cũ"
    elif kind == "order_mismatch":
        rows[0]["kafka_offset"], rows[1]["kafka_offset"] = 1, 0
    else:
        rows[0]["source_epoch"] = "old-cluster/recreated-topic"
    assert compare_events(events, rows, EPOCH)[kind] == 1


def test_duplicate_delete_payload_and_boundary(plan):
    """Không che dòng ngoài range; lặp offset hoặc after của delete đều bị bắt."""
    events, _ = plan
    rows = bronze_rows(events)
    boundary = {"topic": "topic", "starting_offsets": {"0": 0}, "ending_offsets": {"0": 3}}
    assert not any(compare_events(events, rows, EPOCH, boundary).values())
    duplicate = deepcopy(rows)
    duplicate.append(dict(rows[0]))
    result = compare_events(events, duplicate, EPOCH, boundary)
    assert result["extra"] == result["duplicate_offsets"] == 1
    rows[2]["payload_after"] = "non-null"
    assert compare_events(events, rows, EPOCH)["payload_mismatch"] == 1
    assert compare_events(events, rows, EPOCH)["invalid"] == 1
    rows[2]["kafka_offset"] = 3
    assert compare_events(events, rows, EPOCH, boundary)["outside_boundary"] == 1


def test_contract_rejects_bad_sample(plan):
    """Mutation không được tạo oracle cho payload thiếu trường required."""
    _, sample = plan
    sample = {
        **sample,
        "raw_payload": json.dumps({"purchaseOrderId": "x", "orderLines": {"orderLine": [{}]}}),
    }
    with pytest.raises(ValueError, match="contract"):
        mutation_plan(sample, 11)


def test_dataset_is_part_of_key(plan):
    """Cùng purchase_order_id nhưng dataset khác phải là thiếu và thừa."""
    events, _ = plan
    rows = bronze_rows(events)
    rows[0]["dataset_version"] = "other-dataset"
    result = compare_events(events, rows, EPOCH)
    assert result["missing"] == result["extra"] == 1


def test_dag_ast_and_setup():
    """DAG paused, trigger tay, hai task đúng thứ tự và Variables được setup đủ."""
    source = (ROOT / "dags/dp_order_lifecycle_cdc.py").read_text()
    tree = ast.parse(source)
    dag = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "DAG"
    )
    kwargs = {kw.arg: kw.value for kw in dag.keywords}
    assert ast.literal_eval(kwargs["schedule"]) is None
    assert ast.literal_eval(kwargs["is_paused_upon_creation"]) is True
    dependencies = [
        n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.BinOp)
    ]
    assert any(
        isinstance(n.op, ast.RShift) and n.left.id == "ingest_cdc" and n.right.id == "validate_cdc"
        for n in dependencies
    )
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "OrderLifecycleCdcSparkSubmitOperator"
    ]
    assert len(calls) == 2
    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    for variable in (
        "fsds_order_lifecycle_driver_memory",
        "fsds_order_lifecycle_executor_memory",
        "fsds_order_lifecycle_executor_cores",
        "fsds_order_lifecycle_cdc_spark_packages",
        "fsds_order_lifecycle_cdc_oracle",
        "fsds_order_lifecycle_cdc_control_dir",
    ):
        assert variable in source and f"variables set {variable}" in setup
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    assert all(
        f'"_{field}"' in ast.get_source_segment(source, cls)
        for field in ("driver_memory", "executor_memory", "executor_cores")
    )


def test_all_new_python_functions_have_vietnamese_docstrings():
    """Các hàm mới đều có docstring; parse không cần Airflow/Spark."""
    files = [
        "scripts/setup/setup_order_lifecycle_debezium.py",
        "scripts/run/mutate_order_lifecycle_cdc.py",
        "spark_jobs/lib/order_lifecycle_cdc.py",
        "spark_jobs/cdc/ingest_order_lifecycle_changes.py",
        "spark_jobs/cdc/validate_order_lifecycle_changes.py",
        "dags/dp_order_lifecycle_cdc.py",
        "tests/harness/control_order_lifecycle_cdc.py",
        "tests/unit/spark_jobs/lib/test_order_lifecycle_cdc.py",
    ]
    for name in files:
        tree = ast.parse((ROOT / name).read_text())
        assert ast.get_docstring(tree), name
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert ast.get_docstring(node), (name, node.name)


def test_two_updates_same_key_reordered(plan):
    """Đảo hai UPDATE vẫn phải báo sai thứ tự dù chuỗi op c/u/u/d không đổi."""
    events, _ = plan
    events = deepcopy(events)
    second_update = {
        **events[1],
        "raw_payload": "another payload",
        "after_sha256": payload_sha256("another payload"),
        "commit_order": 3,
    }
    events[-1]["commit_order"] = 4
    events.insert(2, second_update)
    rows = bronze_rows(events)
    rows[1]["kafka_offset"], rows[2]["kafka_offset"] = 2, 1
    assert compare_events(events, rows, EPOCH)["order_mismatch"] == 1
