"""Kiểm publisher qua CLI JSON offline bằng receipt mẫu độc lập với Spark."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts/run/publish_order_lifecycle_datahub.py"


def dry_run(tmp_path, extra=()):
    """Dựng receipt mẫu có check sạch/lỗi, chạy CLI không dùng DataHub SDK."""
    for task, checks in {"build_silver": {"orphan_allocations": 0, "native_record_balance": -1},
                         "build_gold": {"duplicate_fact": 0},
                         "validate_gold": {"pit_late_evidence": 2}}.items():
        (tmp_path / f"{task}.json").write_text(json.dumps({
            "run_id": "manual__fixture-original", "completed_at": "2026-10-19T00:00:00Z",
            "checks": checks, "quarantine": {"total": 3},
        }))
    (tmp_path / "sitecustomize.py").write_text(
        '"""Chặn mọi socket trong subprocess dry-run của test."""\n'
        'import socket\n'
        'def forbidden(*args, **kwargs):\n'
        '    """Làm test thất bại nếu publisher thử gọi mạng."""\n'
        '    raise AssertionError("dry-run attempted network")\n'
        'socket.socket.connect = forbidden\n'
        'socket.create_connection = forbidden\n'
        'socket.getaddrinfo = forbidden\n'
    )
    result = subprocess.run([sys.executable, str(SCRIPT), "--dry-run", "--receipt-dir", str(tmp_path),
                             "--source-root", "/data/order_lifecycle", *extra],
                            cwd=tmp_path, text=True, capture_output=True,
                            env={**os.environ, "PYTHONPATH": str(tmp_path)})
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def aspects(mcps, name):
    """Đọc chính phần aspect trong các MCP chuẩn gửi cho REST emitter."""
    return {m["entityUrn"]: json.loads(m["aspect"]["value"]) for m in mcps if m["aspectName"] == name}


def test_offline_complete_inventory_lineage_and_failures(tmp_path):
    """Xuất đúng bảng thật, năm job, cạnh lineage và FAIL có run_id gốc."""
    mcps = dry_run(tmp_path)
    datasets = aspects(mcps, "datasetProperties")
    assert len(datasets) == 41  # 16 feed file + PostgreSQL + 1 Bronze + 18 Silver + 5 Gold.
    v1 = ast.parse((ROOT / "scripts/run/publish_order_snapshots_datahub.py").read_text())
    namespace = {}
    exec(compile(v1, "v1_metadata", "exec"), namespace)
    v1_urns = {f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)" for name in namespace["DATASETS"]}
    assert not v1_urns.intersection(datasets)
    assert any("lakehouse.bronze.lifecycle_raw_records," in urn for urn in datasets)
    assert any("lakehouse.silver.lifecycle_quarantine_records," in urn for urn in datasets)
    assert any("lakehouse.gold.lifecycle_label_segments," in urn for urn in datasets)
    assert any("lakehouse.silver.lifecycle_stg_observed_evidence," in urn for urn in datasets)
    assert all("chưa có schema field" in value["description"] for urn, value in datasets.items()
               if "dataPlatform:iceberg," in urn)
    jobs = aspects(mcps, "dataJobInputOutput")
    assert {urn.rsplit(",", 1)[-1][:-1] for urn in jobs} == {
        "check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"}
    by_task = {urn.rsplit(",", 1)[-1][:-1]: value for urn, value in jobs.items()}
    assert by_task["check_contracts"]["inputDatasets"] == []
    assert len(by_task["ingest_bronze"]["inputDatasets"]) == 16
    assert len(by_task["build_silver"]["outputDatasets"]) == 18
    assert len(by_task["build_gold"]["inputDatasets"]) == 4  # Chỉ Silver, không Bronze.
    assert not any("ingest_bronze" in urn for urn in by_task["build_gold"]["inputDatasets"])
    assert len(by_task["build_gold"]["outputDatasets"]) == 5
    assert len(by_task["validate_gold"]["inputDatasets"]) == 9
    assert by_task["validate_gold"]["outputDatasets"] == []
    lineages = aspects(mcps, "upstreamLineage")
    bronze = next(value for urn, value in lineages.items() if "bronze.lifecycle_raw_records," in urn)
    assert len(bronze["upstreams"]) == 16
    results = aspects(mcps, "assertionRunEvent")
    assert len(results) == 4
    assert sorted(a["result"]["type"] for a in results.values()) == ["FAILURE", "FAILURE", "SUCCESS", "SUCCESS"]
    assert all(urn.endswith("-v2") for urn in results)
    assert all(a["runId"] == "manual__fixture-original" for a in results.values())
    assert all(a["result"]["nativeResults"]["run_id"] == "manual__fixture-original" for a in results.values())
    assert all(a["status"] == "COMPLETE" for a in results.values())
    schemas = aspects(mcps, "schemaMetadata")
    assert schemas and all(s["fields"] for s in schemas.values())
    walmart = dry_run(tmp_path, ["--walmart-jdbc", "walmart_lifecycle.orders"])
    jdbc_job = next(value for urn, value in aspects(walmart, "dataJobInputOutput").items() if urn.endswith(",ingest_bronze)"))
    assert any("dataPlatform:postgres,walmart_lifecycle.orders," in urn for urn in jdbc_job["inputDatasets"])
    assert not any("feed=walmart.orders" in urn for urn in jdbc_job["inputDatasets"])


def test_schema_file_from_receipt_and_original_receipt_cli(tmp_path):
    """Chỉ lấy schema bảng từ file receipt chỉ định; hỗ trợ receipt tên bất kỳ."""
    table = "lakehouse.gold.lifecycle_fact_order_lines"
    (tmp_path / "schema.json").write_text(json.dumps({"tables": {table: {
        "type": "struct", "fields": [{"name": "line_id", "type": "string", "nullable": False},
                                     {"name": "ordered_basis_qty", "type": "integer", "nullable": True}]}}}))
    receipt = tmp_path / "written-receipt.json"
    receipt.write_text(json.dumps({"run_id": "persisted-run", "schema_file": "schema.json",
                                   "checks": {"duplicate_fact": 0}}))
    mcps = dry_run(tmp_path, ["--receipt", f"build_gold={receipt}"])
    schema = next(v for urn, v in aspects(mcps, "schemaMetadata").items() if table in urn)
    assert [f["fieldPath"] for f in schema["fields"]] == ["line_id", "ordered_basis_qty"]
    assert schema["fields"][0]["nullable"] is False
    assert schema["fields"][1]["type"] == {"type": {"com.linkedin.schema.NumberType": {}}}
    original = next(v for urn, v in aspects(mcps, "assertionRunEvent").items() if "-build_gold-" in urn)
    assert original["runId"] == "persisted-run"
    assert original["result"]["nativeResults"]["timestamp_source"] == "receipt_mtime"


def test_refuse_evidence_without_original_run_id(tmp_path):
    """Không tự bịa run_id khi receipt bị thiếu provenance."""
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps({"checks": {"bad": 0}}))
    result = subprocess.run([sys.executable, str(SCRIPT), "--dry-run", "--receipt", f"build_silver={path}"],
                            text=True, capture_output=True)
    assert result.returncode != 0
    assert "run_id gốc" in result.stderr
