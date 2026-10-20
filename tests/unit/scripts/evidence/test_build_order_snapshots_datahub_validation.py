"""Test parser bằng chứng DataHub và quy tắc publish với log CDC/feature dạng cũ lẫn mới."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = Path(__file__).resolve().parents[4] / "scripts"


def load(name: str):
    """Nạp một script trong scripts/ như module, không cần cài package."""
    spec = importlib.util.spec_from_file_location(name, ROOT / (name.replace(".", "/") + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load("scripts.evidence.build_order_snapshots_datahub_validation")
publisher = load("scripts.run.publish_order_snapshots_datahub")

CDC_V1_LOG = "Kafka=6, Bronze=6, offsets duy nhất=6, thiếu=0, lỗi=0\n"
CDC_V2_LOG = ("Kafka=6, Bronze=6, offsets duy nhất=6, thiếu=0, thừa=0, sai payload=0, lỗi=0\n"
              "Validate CDC Bronze: PASS\n")
FEATURE_V1_LOG = "Gold fact=10, feature=10, feature invalid=0, duplicate_order_line_key=0\n"
FEATURE_V2 = dict(fact_rows=10, feature_rows=10, invalid=0, duplicate_keys=0,
                  source_invalid=0, source_duplicates=0, missing=0, extra=0,
                  wrong_event_time=0)


def feature_v2_log(**changes) -> str:
    """Log giống dòng `print(result)` của spark_jobs/gold/validate_order_snapshot_return_features.py."""
    return f"[2026-10-18] INFO - {FEATURE_V2 | changes}\nValidate feature offline: PASS\n"


def test_cdc_v1_log_keeps_legacy_metrics():
    """Kiểm parser CDC v1 giữ các metric cũ."""
    metrics, _, log_format = builder.metrics_for("dp1_cdc", "validate_walmart_cdc", CDC_V1_LOG)
    assert log_format == "cdc-v1"
    assert metrics == {"source_rows": 6, "dataset_rows": 6, "unique_offsets": 6,
                       "missing_rows": 0, "invalid_rows": 0}


def test_cdc_v2_log_adds_extra_and_payload():
    """Kiểm parser CDC v2 đọc thêm metric extra và payload."""
    metrics, rule, log_format = builder.metrics_for("dp1_cdc", "validate_walmart_cdc", CDC_V2_LOG)
    assert log_format == "cdc-v2"
    assert metrics["extra_rows"] == 0 and metrics["payload_mismatch_rows"] == 0
    assert "payload" in rule


@pytest.mark.parametrize("field", ["thừa", "sai payload"])
def test_cdc_v2_nonzero_failure_is_rejected(field):
    """Kiểm CDC v2 bị từ chối khi có metric lỗi khác không."""
    log = CDC_V2_LOG.replace(f"{field}=0", f"{field}=1")
    with pytest.raises(ValueError, match="Validation failed"):
        builder.metrics_for("dp1_cdc", "validate_walmart_cdc", log)


def test_cdc_log_format_must_match_table():
    """Kiểm định dạng log CDC phải khớp phiên bản bảng."""
    builder.check_cdc_table("cdc-v1", builder.LEGACY_CDC_TABLE)
    builder.check_cdc_table("cdc-v2", "lakehouse.bronze.raw_walmart_cdc_v2")
    with pytest.raises(ValueError):
        builder.check_cdc_table("cdc-v2", builder.LEGACY_CDC_TABLE)
    with pytest.raises(ValueError):
        builder.check_cdc_table("cdc-v1", "lakehouse.bronze.raw_walmart_cdc_v2")


def test_feature_v1_log_still_parses():
    """Kiểm parser tiếp tục nhận log feature v1."""
    metrics, _, log_format = builder.metrics_for("dp3_features", "validate_return_features", FEATURE_V1_LOG)
    assert log_format == "feature-v1"
    assert metrics["source_rows"] == metrics["dataset_rows"] == 10


def test_feature_v2_dict_log_parses_all_checks():
    """Kiểm parser đọc toàn bộ check trong dictionary log feature v2."""
    metrics, _, log_format = builder.metrics_for("dp3_features", "validate_return_features", feature_v2_log())
    assert log_format == "feature-v2"
    assert metrics == {
        "source_rows": 10, "dataset_rows": 10, "invalid_rows": 0,
        "duplicate_order_line_key": 0, "source_invalid_rows": 0, "source_duplicate_keys": 0,
        "missing_rows": 0, "extra_rows": 0, "wrong_event_time_rows": 0,
    }


@pytest.mark.parametrize("field", ["missing", "extra", "wrong_event_time", "source_duplicates"])
def test_feature_v2_nonzero_failure_is_rejected(field):
    """Kiểm feature v2 bị từ chối khi có metric lỗi khác không."""
    with pytest.raises(ValueError, match="Validation failed"):
        builder.metrics_for("dp3_features", "validate_return_features", feature_v2_log(**{field: 1}))


def test_run_override_replaces_only_named_dag():
    """Kiểm override run chỉ thay DAG được chỉ định."""
    runs = builder.parse_runs(["dp1_cdc=manual__new"])
    assert runs["dp1_cdc"] == "manual__new"
    assert runs["dp2_silver"] == builder.RUNS["dp2_silver"]
    with pytest.raises(ValueError):
        builder.parse_runs(["unknown_dag=x"])
    with pytest.raises(ValueError):
        builder.parse_runs(["dp1_cdc"])


def test_publisher_maps_cdc_job_to_v2_and_keeps_v1_dataset():
    """Kiểm publisher chọn job CDC v2 và giữ dataset v1."""
    cdc_jobs = publisher.job_map()["dp1_cdc"]
    assert cdc_jobs["ingest_walmart_cdc"][1] == ["lakehouse.bronze.raw_walmart_cdc_v2"]
    assert publisher.CDC_LEGACY in publisher.DATASETS
    assert "legacy" in publisher.description(publisher.CDC_LEGACY, "iceberg")


def test_assertion_urn_separates_v1_and_v2():
    """Kiểm URN assertion phân biệt phiên bản v1 và v2."""
    base = {"dag_id": "dp1_cdc", "task_id": "validate_walmart_cdc"}
    v1 = publisher.assertion_urn(base | {"dataset": publisher.CDC_LEGACY})
    v2 = publisher.assertion_urn(base | {"dataset": publisher.CDC})
    assert v1 == "urn:li:assertion:fsds-dp1_cdc-validate_walmart_cdc"
    assert v2 == v1 + "-v2"


def test_publisher_rejects_new_failure_metrics():
    """Kiểm publisher từ chối các metric lỗi mới."""
    item = {"dataset": publisher.CDC, "state": "success", "task_id": "validate_walmart_cdc",
            "metrics": {"dataset_rows": 6, "payload_mismatch_rows": 1}}
    with pytest.raises(ValueError, match="không PASS"):
        publisher.check_passed(item)


def test_legacy_manifest_still_publishable():
    """Manifest lịch sử (CDC v1, feature v1) vẫn qua kiểm tra publish, URN không đổi."""
    import json
    manifest = json.loads((SCRIPTS.parent / "docs/evidence/datahub-validation.json").read_text())
    for item in manifest["checks"]:
        publisher.check_passed(item)
        assert not publisher.assertion_urn(item).endswith("-v2")
