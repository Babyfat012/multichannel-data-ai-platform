"""Kiểm ml/train_lib.py: split theo thời gian, baseline, đánh giá; train thật LightGBM nếu có."""

import numpy as np
import pandas as pd
import pytest

from ml.train_lib import add_order_features, baseline_scores, evaluate, temporal_split


def frame(days=20, per_day=10):
    """Dữ liệu dựng tay: nhãn 1 khi SKU-A, để model có tín hiệu rõ."""
    rows = []
    for d in range(days):
        for i in range(per_day):
            sku = "A" if i % 3 == 0 else "B"
            rows.append(
                dict(
                    event_timestamp=pd.Timestamp("2026-01-01", tz="UTC")
                    + pd.Timedelta(days=d, hours=i),
                    seller_sku=sku,
                    channel="amazon" if i % 2 else "walmart",
                    country="US",
                    ordered_basis_qty=1 + i % 2,
                    ordered_product_net_minor=1000 * (1 + i % 2),
                    labeled_count_90d=None if d == 0 else 10,
                    return_or_refund_rate_90d=0.9 if sku == "A" else 0.1,
                    approved_returns_24h=None,
                    label=int(sku == "A"),
                )
            )
    return pd.DataFrame(rows)


@pytest.mark.parametrize("test_days", [1, 5, 19])
def test_temporal_split_boundary_has_no_overlap(test_days):
    """Biên: mọi dòng test đều sau mọi dòng train; test đúng `test_days` ngày lịch."""
    train, test = temporal_split(frame(), test_days)
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert test["event_timestamp"].dt.normalize().nunique() == test_days
    assert len(train) + len(test) == 200


def test_split_rejects_whole_range_as_test():
    """Test phủ hết dữ liệu ⇒ train rỗng ⇒ lỗi, không lặng lẽ train trên 0 dòng."""
    with pytest.raises(ValueError):
        temporal_split(frame(days=3), test_days=3)


def test_baseline_fills_missing_with_train_prior():
    """SKU chưa có lịch sử nhận tỷ lệ chung của train, không phải 0."""
    train = pd.DataFrame({"label": [1, 0, 0, 0]})
    test = pd.DataFrame({"return_or_refund_rate_90d": [None, 0.5]})
    assert baseline_scores(train, test).tolist() == [0.25, 0.5]


def test_unit_price_guards_zero_quantity():
    """Số lượng 0 ⇒ đơn giá NaN, không chia cho 0."""
    out = add_order_features(
        pd.DataFrame(
            {
                "ordered_basis_qty": [0, 2],
                "ordered_product_net_minor": [500, 500],
                "channel": ["a", "b"],
                "country": ["x", "y"],
                "labeled_count_90d": [1, 1],
                "return_or_refund_rate_90d": [0.1, 0.1],
                "approved_returns_24h": [None, 1],
            }
        )
    )
    assert np.isnan(out["unit_price_minor"][0]) and out["unit_price_minor"][1] == 250


def test_evaluate_perfect_vs_random():
    """Model xếp đúng hoàn toàn ⇒ PR-AUC 1 và thắng baseline đảo ngược."""
    test = pd.DataFrame({"label": [1, 0, 1, 0]})
    m = evaluate(test, np.array([0.9, 0.1, 0.8, 0.2]), np.array([0.1, 0.9, 0.2, 0.8]))
    assert m["pr_auc_model"] == 1.0 and m["beats_baseline"] and m["prevalence"] == 0.5
