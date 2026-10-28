"""Property-based (hypothesis): model và các bước tiền xử lý cho cùng kết quả khi chạy lại (idempotent).

Hypothesis sinh hàng trăm bộ dữ liệu ngẫu nhiên — kể cả giá 0, số lượng 0, NaN,
kênh lạ — để tìm ca làm kết quả đổi giữa hai lần chạy hoặc làm hỏng bất biến.
"""

import numpy as np
import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ml.train_lib import FEATURES, add_order_features, temporal_split, train_model

CHANNELS = ["amazon", "walmart", "shopify", "tiktok_shop", "shopee"]

order = st.fixed_dictionaries(
    {
        "channel": st.sampled_from(CHANNELS),
        "country": st.sampled_from(["a", "b", "c"]),
        "ordered_basis_qty": st.integers(0, 50),
        "ordered_product_net_minor": st.integers(0, 10_000_000),
        "labeled_count_90d": st.one_of(st.none(), st.integers(0, 500)),
        "return_or_refund_rate_90d": st.one_of(st.none(), st.floats(0, 1)),
        "approved_returns_24h": st.one_of(st.none(), st.integers(0, 20)),
    }
)


def training_frame(n=300, seed=0):
    """Tập train cố định có tín hiệu (kênh tiktok_shop dễ trả hơn)."""
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame(
        {
            "channel": rng.choice(CHANNELS, n),
            "country": rng.choice(["a", "b", "c"], n),
            "ordered_basis_qty": rng.integers(1, 5, n),
            "ordered_product_net_minor": rng.integers(1000, 50000, n),
            "labeled_count_90d": rng.integers(0, 100, n),
            "return_or_refund_rate_90d": rng.random(n),
            "approved_returns_24h": rng.integers(0, 5, n),
        }
    )
    frame["label"] = ((frame["channel"] == "tiktok_shop") | (rng.random(n) < 0.2)).astype(int)
    return add_order_features(frame)


MODEL = train_model(training_frame())


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(order, min_size=1, max_size=20))
def test_prediction_is_idempotent_and_bounded(rows):
    """Cùng đầu vào ⇒ cùng điểm, mọi lần; điểm luôn trong [0, 1] và không NaN."""
    frame = add_order_features(pd.DataFrame(rows))
    first = MODEL.predict_proba(frame[FEATURES])[:, 1]
    second = MODEL.predict_proba(add_order_features(pd.DataFrame(rows))[FEATURES])[:, 1]
    assert np.array_equal(first, second)
    assert np.all((first >= 0) & (first <= 1)) and not np.isnan(first).any()


@settings(max_examples=200, deadline=None)
@given(st.lists(order, min_size=1, max_size=20))
def test_feature_prep_is_idempotent(rows):
    """Chạy add_order_features hai lần = chạy một lần (không cộng dồn, không đổi kiểu)."""
    once = add_order_features(pd.DataFrame(rows))
    twice = add_order_features(once)
    pd.testing.assert_frame_equal(once, twice)


@settings(max_examples=100, deadline=None)
@given(st.integers(1, 30), st.integers(0, 2**32 - 1))
def test_retraining_same_data_gives_same_model(test_days, seed):
    """Train lại trên cùng dữ liệu với cùng seed ⇒ điểm giống hệt (deterministic=True)."""
    frame = training_frame(n=120, seed=seed % 1000)
    a = train_model(frame).predict_proba(frame[FEATURES])[:, 1]
    b = train_model(frame).predict_proba(frame[FEATURES])[:, 1]
    assert np.array_equal(a, b)


@settings(max_examples=200, deadline=None)
@given(st.integers(2, 60), st.integers(1, 59))
def test_temporal_split_never_leaks_future(days, test_days):
    """Mọi cách chia hợp lệ: test luôn sau train, không mất/không trùng dòng."""
    if test_days >= days:
        return
    rows = [
        {
            "event_timestamp": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=d, hours=h),
            "label": int(h == 1),  # mỗi ngày đủ cả nhãn 0 và 1
        }
        for d in range(days)
        for h in (1, 13)
    ]
    train, test = temporal_split(pd.DataFrame(rows), test_days)
    assert train["event_timestamp"].max() < test["event_timestamp"].min()
    assert len(train) + len(test) == len(rows)


def test_temporal_split_rejects_single_class_test():
    """Ca biên hypothesis tìm ra: ngày test chỉ có một nhãn ⇒ từ chối, không đánh giá PR-AUC vô nghĩa."""
    import pytest

    rows = [
        {
            "event_timestamp": pd.Timestamp("2026-01-01", tz="UTC") + pd.Timedelta(days=d),
            "label": d % 2 == 0,
        }
        for d in range(3)
    ]
    rows[-1]["label"] = 0
    with pytest.raises(ValueError, match="một lớp"):
        temporal_split(pd.DataFrame(rows).astype({"label": int}), 1)
