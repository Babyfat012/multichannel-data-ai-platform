"""Kiểm ml/drift.summarize: đọc đúng chiều p-value (nhỏ = drift) và khoảng cách (lớn = drift)."""

import pytest

from ml.drift import summarize


def snapshot(*metrics):
    """Hình dạng tối thiểu của Snapshot.dict() mà summarize đọc."""
    return {"metrics": [{"metric_name": n, "value": v} for n, v in metrics]}


@pytest.mark.parametrize(
    ("method", "threshold", "score", "drift"),
    [
        ("chi-square p_value", 0.05, 0.049, True),
        ("chi-square p_value", 0.05, 0.05, False),
        ("Jensen-Shannon distance", 0.2, 0.2, True),
        ("Jensen-Shannon distance", 0.2, 0.199, False),
    ],
)
def test_direction_and_boundary(method, threshold, score, drift):
    """Biên ngưỡng theo từng loại phương pháp."""
    out = summarize(
        snapshot((f"ValueDrift(column=x,method={method},threshold={threshold})", score))
    )
    assert out["columns"]["x"]["drift"] is drift and out["alert"] is drift


def test_alert_when_any_column_drifts_and_ignores_other_metrics():
    """Một cột drift là đủ cảnh báo; metric không phải ValueDrift bị bỏ qua."""
    out = summarize(
        snapshot(
            ("DriftedColumnsCount(drift_share=0.5)", {"count": 1.0, "share": 0.5}),
            ("ValueDrift(column=seller_sku,method=Jensen-Shannon distance,threshold=0.2)", 0.436),
            ("ValueDrift(column=label,method=Jensen-Shannon distance,threshold=0.1)", 0.001),
        )
    )
    assert out["drifted"] == ["seller_sku"] and out["alert"] is True
