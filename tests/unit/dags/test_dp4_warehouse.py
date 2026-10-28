"""DAG warehouse: đủ năm Channel, 30 ngày trước cutoff của Gold A; lệch số dòng thì chặn."""

from __future__ import annotations

from pathlib import Path

from spark_jobs.lib.label_channels import CHANNELS, count_mismatches

ROOT = Path(__file__).resolve().parents[3]


def test_dag_exports_thirty_days_before_ml_cutoff():
    """Cùng cutoff với ML (Gold A), cửa sổ 30 ngày, đúng job warehouse."""
    text = (ROOT / "dags/dp4_warehouse.py").read_text()
    assert "/opt/spark_jobs/warehouse/export_order_snapshot_lines.py" in text
    assert "var.value.get('platform_ml_cutoff'" in text
    assert '"--days",\n            "30"' in text
    job = (ROOT / "spark_jobs/warehouse/export_order_snapshot_lines.py").read_text()
    assert 'F.col("channel") == "walmart"' not in job


def test_count_mismatches_blocks_short_or_empty_channels():
    """Khớp đủ thì rỗng; JDBC ghi thiếu một kênh hoặc Gold rỗng một kênh thì bị báo."""
    gold = {c: 10 for c in CHANNELS}
    assert count_mismatches(gold, dict(gold)) == []
    assert count_mismatches(gold, gold | {"shopee": 9}) == ["shopee"]
    assert count_mismatches(gold | {"amazon": 0}, gold | {"amazon": 0}) == ["amazon"]
