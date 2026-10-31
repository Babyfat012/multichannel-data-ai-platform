"""DAG nhãn năm Channel nhận đường đáp án/khóa qua Variable; thiếu Variable thì về đường local cũ."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_dag_passes_ground_truth_and_keys_with_local_defaults():
    """Không có Variable: dùng bind mount local; có Variable (AWS): dùng S3."""
    text = (ROOT / "dags/dp3_return_labels.py").read_text()
    assert "--ground-truth" in text and "--keys-path" in text
    assert "var.value.get('platform_dp3_ground_truth', '/data/ground-truth')" in text
    assert "'/data/ground-truth-bridge/walmart-keys.parquet'" in text
