"""Kiểm receipt/schema offline bằng thư viện chuẩn, không cần Spark hoặc DataHub."""

from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.run.publish_order_lifecycle_datahub import build_mcps, load_receipts, schema_fields
from spark_jobs.lib import receipts as receipt_schemas


TABLE = "lakehouse.gold.lifecycle_fact_order_lines"
SCHEMA = {"type": "struct", "fields": [
    {"name": "line_id", "type": "string", "nullable": False, "metadata": {}},
    {"name": "quantity", "type": "integer", "nullable": True, "metadata": {}},
    {"name": "evidence", "type": {"type": "array", "containsNull": False,
        "elementType": {"type": "struct", "fields": [
            {"name": "observed_at", "type": "timestamp", "nullable": False, "metadata": {}}
        ]}}, "nullable": True, "metadata": {}},
]}


def publisher_args(receipt):
    """Trỏ publisher vào đúng receipt vừa tạo, không dùng schema contract thay thế."""
    return SimpleNamespace(receipt_dir=None, receipt=[f"build_gold={receipt}"],
                           contracts_dir=Path(__file__).resolve().parents[4] / "contracts",
                           source_root=receipt.parent / "no-source", walmart_jdbc=None)


def test_relative_schema_round_trip_and_completed_at(tmp_path, monkeypatch):
    """Receipt tự mang đường dẫn tương đối và giờ hoàn tất, độc lập cwd/mtime."""
    receipt = tmp_path / "run" / "build_gold.json"
    relative = receipt_schemas.write_receipt_schemas({TABLE: SCHEMA}, str(receipt))
    assert relative == "schemas.json"
    assert not Path(relative).is_absolute()
    assert json.loads((receipt.parent / relative).read_text()) == {"tables": {TABLE: SCHEMA}}
    receipt.write_text(json.dumps({"run_id": "original-run", "schema_file": relative,
                                   "completed_at": "2026-10-19T01:02:03Z", "checks": {"duplicate_fact": 0}}))
    monkeypatch.chdir(tmp_path)
    receipts, schemas = load_receipts(publisher_args(receipt))
    assert receipts["build_gold"][0]["run_id"] == "original-run"
    assert schemas == {TABLE: SCHEMA}
    assert [field["fieldPath"] for field in schema_fields(schemas[TABLE])] == [
        "line_id", "quantity", "evidence", "evidence[].observed_at"]
    mcps = build_mcps(publisher_args(receipt))
    schema = next(json.loads(m["aspect"]["value"]) for m in mcps
                  if m["aspectName"] == "schemaMetadata" and TABLE in m["entityUrn"])
    assert schema["fields"] == schema_fields(SCHEMA)
    event = next(json.loads(m["aspect"]["value"]) for m in mcps if m["aspectName"] == "assertionRunEvent")
    assert event["timestampMillis"] == int(datetime(2026, 10, 19, 1, 2, 3, tzinfo=timezone.utc).timestamp() * 1000)
    assert event["result"]["nativeResults"]["timestamp_source"] == "completed_at"


def test_atomic_replace_preserves_other_jobs(tmp_path, monkeypatch):
    """Rename chỉ diễn ra sau khi JSON hoàn chỉnh; giữ schema Bronze khi ghi Gold."""
    bronze = "lakehouse.bronze.lifecycle_raw_records"
    receipt_schemas.write_receipt_schemas({bronze: SCHEMA}, tmp_path / "ingest_bronze.json")
    destination = tmp_path / "schemas.json"
    original = destination.read_bytes()
    replace = receipt_schemas.os.replace
    calls = []

    def inspect_replace(source, target):
        """Quan sát file cũ và file tạm ngay trước thao tác atomic thực tế."""
        assert target == destination
        assert source.parent == target.parent
        assert source != target
        assert destination.read_bytes() == original
        assert json.loads(source.read_text()) == {"tables": {bronze: SCHEMA, TABLE: SCHEMA}}
        calls.append(source)
        replace(source, target)

    monkeypatch.setattr(receipt_schemas.os, "replace", inspect_replace)
    receipt_schemas.write_receipt_schemas({TABLE: SCHEMA}, tmp_path / "build_gold.json")
    assert len(calls) == 1
    assert not calls[0].exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["schemas.json"]


def test_replace_failure_preserves_previous_schema(tmp_path, monkeypatch):
    """Lỗi rename giữ nguyên schema cũ và dọn file tạm."""
    receipt_schemas.write_receipt_schemas({TABLE: SCHEMA}, tmp_path / "receipt.json")
    original = (tmp_path / "schemas.json").read_bytes()

    def fail_replace(*args):
        """Giả lập lỗi đĩa ngay tại ranh giới publish file."""
        raise OSError("rename failed")

    monkeypatch.setattr(receipt_schemas.os, "replace", fail_replace)
    with pytest.raises(OSError, match="rename failed"):
        receipt_schemas.write_receipt_schemas({TABLE: SCHEMA}, tmp_path / "receipt.json")
    assert (tmp_path / "schemas.json").read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["schemas.json"]


@pytest.mark.parametrize("tables, message", [
    ({}, "tables"),
    ({TABLE: {}}, "struct"),
    ({TABLE: {"type": "struct"}}, "fields"),
    ({TABLE: {"type": "struct", "fields": []}}, "fields"),
    ({TABLE: {"type": "struct", "fields": [{"type": "string"}]}}, "name"),
    ({TABLE: {"type": "struct", "fields": [{"name": "id"}]}}, "type"),
    ({TABLE: {"type": "struct", "fields": [{"name": "nested", "type": {"type": "struct"}}]}}, "fields"),
])
def test_refuse_empty_or_incomplete_schema(tmp_path, tables, message):
    """Schema lỗi bị từ chối rõ ràng, không làm hỏng file của job trước."""
    receipt = tmp_path / "build_gold.json"
    receipt_schemas.write_receipt_schemas({TABLE: SCHEMA}, receipt)
    original = (tmp_path / "schemas.json").read_bytes()
    with pytest.raises(ValueError, match=message):
        receipt_schemas.write_receipt_schemas(tables, receipt)
    assert (tmp_path / "schemas.json").read_bytes() == original


def test_completed_at_is_current_utc():
    """Giờ hoàn tất có hậu tố Z và nằm giữa hai lần đọc đồng hồ UTC."""
    before = datetime.now(timezone.utc)
    completed = receipt_schemas.completed_at_utc()
    after = datetime.now(timezone.utc)
    assert completed.endswith("Z")
    assert before <= datetime.fromisoformat(completed.replace("Z", "+00:00")) <= after
