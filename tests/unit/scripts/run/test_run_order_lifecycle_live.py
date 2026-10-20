"""Kiểm runbook và route v2 hoàn toàn offline, không cần Docker/Chrome."""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
from urllib.parse import unquote

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
RUNBOOK = ROOT / "scripts/run/run_order_lifecycle_live.sh"


def test_bash_syntax():
    """Kiểm cú pháp Bash của runbook."""
    subprocess.run(["bash", "-n", str(RUNBOOK)], check=True)


@pytest.mark.parametrize("args,code,token", [
    (["--help"], 0, "--phase"),
    (["--phase"], 2, "--phase"),
    (["--phase", "wrong"], 2, "Pha không hợp lệ"),
    (["--unknown"], 2, "Cờ không hợp lệ"),
])
def test_cli_flags_without_docker(args, code, token):
    """Kiểm help và lỗi tham số CLI mà không khởi động Docker."""
    result = subprocess.run(["bash", str(RUNBOOK), *args], capture_output=True, text=True)
    assert result.returncode == code
    assert token in result.stdout + result.stderr


def test_compose_mounts_v2_only_expected_services():
    """Kiểm mount nguồn v2 chỉ xuất hiện ở các service dự kiến."""
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    fixture = '${FSDS_ORDER_LIFECYCLE_DIR:-./out/order_lifecycle_live}:/data/order_lifecycle:ro'
    mounted = {name for name, service in services.items() if fixture in service.get("volumes", [])}
    assert mounted == {"airflow", "spark-master", "spark-worker-1", "spark-worker-2"}
    assert "./out/receipts:/opt/airflow/receipts" in services["airflow"]["volumes"]


def test_capture_routes_without_browser():
    """Kiểm URL và đường dẫn ảnh DataHub mà không mở trình duyệt."""
    result = subprocess.run([sys.executable, str(ROOT / "scripts/evidence/capture_order_lifecycle_datahub_evidence.py"),
                             "--print-urls"], capture_output=True, text=True, check=True)
    targets = json.loads(result.stdout)
    expected = {"lakehouse.gold.lifecycle_fact_order_lines", "lakehouse.silver.lifecycle_stg_line_lifecycle"}
    assert len(targets) == 4
    for name in expected:
        urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,{name},PROD)"
        routes = [t for t in targets if t["urn"] == urn]
        assert {t["route"] for t in routes} == {"Lineage", "Quality"}
        for target in routes:
            assert unquote(target["url"]) == f"http://127.0.0.1:9002/dataset/{urn}/{target['route']}"
            assert "/docs/images/datahub/v2/dp-order-lifecycle" in target["path"]
    assert {Path(t["path"]).name for t in targets} == {
        "dp-order-lifecycle-gold-lineage.png", "dp-order-lifecycle-gold-validation.png",
        "dp-order-lifecycle-silver-lineage.png", "dp-order-lifecycle-silver-validation.png",
    }


def test_quality_accepts_multiple_checks_and_rejects_failure():
    """Kiểm nhiều assertion cùng thành công và phát hiện assertion lỗi."""
    spec = importlib.util.spec_from_file_location("capture_v2", ROOT / "scripts/evidence/capture_order_lifecycle_datahub_evidence.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Page:
        """Trang Playwright giả, trả nội dung cố định."""

        def __init__(self, body):
            """Giữ nội dung trang sẽ trả về."""
            self.body = body

        async def evaluate(self, expression):
            """Bỏ qua biểu thức, trả nội dung đã giữ."""
            return self.body

    import asyncio
    assert asyncio.run(module.wait_for_quality(Page("Quality 19 Passing 0 Failing")))
    with pytest.raises(RuntimeError, match="assertion lỗi"):
        asyncio.run(module.wait_for_quality(Page("Quality 19 Passing 1 Failing")))


@pytest.fixture
def fake_runtime(tmp_path):
    """Docker giả ghi argv, trả state/receipt; không tạo kết nối hay chạy Spark."""
    executable = tmp_path / "docker"
    executable.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if args[0] == "ps":
    pass
elif "python" in args and "-" in args:
    sys.stdin.read()
    print(json.dumps({"state": os.environ.get("FAKE_RUN_STATE", "success"), "directory": "20261019T010203"}))
elif "cp" in args:
    dest = pathlib.Path(args[-1])
    dest.mkdir(parents=True, exist_ok=True)
    for task in ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"):
        (dest / (task + ".json")).write_text(json.dumps({"run_id": os.environ["FSDS_V2_RUN_ID"], "status": "passed"}))
''')
    executable.chmod(0o755)
    log = tmp_path / "docker.jsonl"
    source = tmp_path / "fixture"
    source.mkdir()
    (source / "manifest.json").write_text("{}")
    run_id = "test_live_v2_" + tmp_path.name
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "FAKE_DOCKER_LOG": str(log), "FSDS_ORDER_LIFECYCLE_DIR": str(source),
           "FSDS_V2_RUN_ID": run_id, "FSDS_PYTHON": sys.executable}
    project = tmp_path / "project"
    (project / "scripts").mkdir(parents=True)
    (project / "configs").mkdir()
    for script in ("run/run_order_lifecycle_live.sh", "setup/setup_airflow.sh"):
        (project / "scripts" / script).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / "scripts" / script, project / "scripts" / script)
    shutil.copy(ROOT / "configs/iceberg_maintenance.json", project / "configs")
    env["FAKE_PROJECT"] = str(project)
    yield env, log, run_id


@pytest.mark.parametrize("phase", ["source", "airflow", "datahub", "capture", "all"])
def test_all_phase_flags_parse(fake_runtime, phase):
    # Với source không cần Docker; các pha khác được kiểm sâu riêng.
    """Kiểm mọi tên pha được CLI nhận diện."""
    env, _, _ = fake_runtime
    if phase != "source":
        env["FSDS_V2_TIMEOUT"] = "invalid"
    result = subprocess.run(["bash", str(Path(env["FAKE_PROJECT"]) / "scripts/run/run_order_lifecycle_live.sh"), "--phase", phase],
                            env=env, capture_output=True, text=True)
    assert "Pha không hợp lệ" not in result.stderr
    assert result.returncode == (0 if phase == "source" else 2)


@pytest.mark.parametrize("state", ["success", "failed"])
def test_airflow_copies_run_receipts_before_stop(fake_runtime, state):
    """Kiểm Airflow sao chép receipt của run trước khi dừng service."""
    env, log, run_id = fake_runtime
    env["FAKE_RUN_STATE"] = state
    result = subprocess.run(["bash", str(Path(env["FAKE_PROJECT"]) / "scripts/run/run_order_lifecycle_live.sh"), "--phase", "airflow"],
                            env=env, capture_output=True, text=True)
    assert result.returncode == (0 if state == "success" else 1), result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    trigger = next(c for c in calls if "trigger" in c)
    assert trigger[trigger.index("--run-id") + 1] == run_id
    assert json.loads(trigger[trigger.index("--conf") + 1]) == {
        "source_root": "/data/order_lifecycle", "cutoff": "2026-06-01T00:00:00.000000Z"}
    up = next(c for c in calls if "up" in c)
    assert "generator-batch" not in up
    copy = next(c for c in calls if "cp" in c)
    assert copy[-2] == "airflow:/opt/airflow/receipts/order_lifecycle/20261019T010203/."
    assert copy[-1] == f"out/receipts-v2-run/{run_id}/"
    assert "stop" in calls[-1]
    assert (Path(env["FAKE_PROJECT"]) / copy[-1] / "build_gold.json").is_file()
    if state == "failed":
        assert "không publish" in result.stderr


def test_source_refuses_partial_directory(fake_runtime):
    """Kiểm từ chối thư mục nguồn dở dang để tránh ghi đè dữ liệu."""
    env, _, _ = fake_runtime
    source = Path(env["FSDS_ORDER_LIFECYCLE_DIR"])
    (source / "manifest.json").unlink()
    (source / "partial.jsonl").write_text("keep")
    result = subprocess.run(["bash", str(RUNBOOK), "--phase", "source"],
                            env=env, capture_output=True, text=True)
    assert result.returncode == 1
    assert (source / "partial.jsonl").read_text() == "keep"


@pytest.mark.parametrize("invalid", [False, True])
def test_datahub_phase_stops_spark_then_publishes_same_arguments(fake_runtime, invalid):
    """Kiểm dừng Spark trước khi publish DataHub với đúng tham số run."""
    env, log, run_id = fake_runtime
    project = Path(env["FAKE_PROJECT"])
    receipts = project / "out/receipts-v2-run" / run_id
    receipts.mkdir(parents=True)
    for task in ("check_contracts", "ingest_bronze", "build_silver", "build_gold", "validate_gold"):
        (receipts / f"{task}.json").write_text(json.dumps({
            "run_id": "wrong" if invalid else run_id, "checks": {"missing": 0}}))
    publisher = project.parent / "publisher"
    publisher.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
print("[]")
""")
    publisher.chmod(0o755)
    curl = project.parent / "curl"
    curl.write_text("#!/bin/sh\nexit 0\n")
    curl.chmod(0o755)
    env["DATAHUB_PYTHON"] = str(publisher)
    result = subprocess.run(["bash", str(project / "scripts/run/run_order_lifecycle_live.sh"), "--phase", "datahub"],
                            env=env, capture_output=True, text=True)
    assert result.returncode == (1 if invalid else 0), result.stderr
    if invalid:
        assert "Sai run_id" in result.stderr
        assert not log.exists()
        return
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert "stop" in calls[0] and "spark-master" in calls[0]
    assert "up" in calls[1] and "datahub" in calls[1]
    dry, publish = calls[-2:]
    assert dry[1] == "--dry-run" and publish[1] == "--publish"
    assert dry[2:] == publish[2:]
    assert dry[dry.index("--receipt-dir") + 1] == f"out/receipts-v2-run/{run_id}"
    assert (receipts / "datahub-mcps.json").read_text().strip() == "[]"
