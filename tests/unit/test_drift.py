"""Kiểm ml/drift.summarize: đọc đúng chiều p-value (nhỏ = drift) và khoảng cách (lớn = drift)."""

import pandas as pd
import pytest

from ml.drift import comparisons, summarize


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


def test_comparisons_split_by_reference_window_and_channel():
    """N ngày đầu là tham chiếu; channel_shift chỉ giữ phần sau của một kênh; kênh vắng ⇒ báo lỗi rõ."""
    frame = pd.DataFrame(
        {
            "event_timestamp": pd.to_datetime(
                ["2026-01-01", "2026-01-05", "2026-02-10", "2026-02-11", "2026-02-12"], utc=True
            ),
            "channel": ["amazon", "shopee", "amazon", "shopee", "shopee"],
            "seller_sku": ["A", "B", "A", "B", "B"],
            "label": [0, 1, 0, 1, 0],
        }
    )
    pairs = comparisons(frame, reference_days=30, shift_channel="shopee")
    assert [len(x) for x in pairs["stable"]] == [2, 3]
    ref, cur = pairs["channel_shift"]
    assert len(ref) == 2 and set(cur["channel"]) == {"shopee"} and len(cur) == 2
    assert "channel_shift" not in comparisons(frame, 30)
    with pytest.raises(ValueError, match="walmart"):
        comparisons(frame, 30, shift_channel="walmart")
