"""Logic train thuần, không phụ thuộc Kubeflow: lấy feature, chia theo thời gian, train, đánh giá.

Mỗi bước KFP trong `ml/training_pipeline.py` chỉ gọi một hàm ở đây, nên test được bằng pytest.
Score là tín hiệu XẾP HẠNG (Q38), không phải xác suất; baseline = tỷ lệ trả 90 ngày của SKU.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FEAST_FEATURES = [
    "sku_return_daily:labeled_count_90d",
    "sku_return_daily:return_or_refund_rate_90d",
    "sku_return_24h:approved_returns_24h",
]
CATEGORICAL = ["channel", "country"]
NUMERIC = [
    "ordered_basis_qty",
    "ordered_product_net_minor",
    "unit_price_minor",
    "labeled_count_90d",
    "return_or_refund_rate_90d",
    "approved_returns_24h",
]
FEATURES = CATEGORICAL + NUMERIC


def add_order_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Thêm đơn giá; ép kiểu categorical cho LightGBM. Không đụng nhãn."""
    out = frame.copy()
    qty = out["ordered_basis_qty"].where(out["ordered_basis_qty"] > 0)
    out["unit_price_minor"] = out["ordered_product_net_minor"] / qty
    for col in CATEGORICAL:
        out[col] = out[col].astype("category")
    for col in NUMERIC:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    return out


def temporal_split(frame: pd.DataFrame, test_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Test = `test_days` ngày đặt đơn cuối cùng; train = phần trước đó. Không chia ngẫu nhiên."""
    ts = pd.to_datetime(frame["event_timestamp"], utc=True)
    boundary = ts.max().normalize() - pd.Timedelta(days=test_days - 1)
    train, test = frame[ts < boundary], frame[ts >= boundary]
    if train.empty or test.empty or test["label"].nunique() < 2:
        raise ValueError(f"split rỗng hoặc test chỉ một lớp (boundary {boundary})")
    return train, test


def baseline_scores(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Baseline Q38: tỷ lệ trả lịch sử của SKU; SKU chưa có lịch sử nhận tỷ lệ chung của train."""
    prior = float(train["label"].mean())
    return test["return_or_refund_rate_90d"].fillna(prior).to_numpy()


def train_model(train: pd.DataFrame, seed: int = 7):
    """LightGBM nhỏ, cố định seed để chạy lại ra cùng model."""
    import lightgbm as lgb

    model = lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=15,
        min_child_samples=20,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        random_state=seed,
        deterministic=True,
        force_row_wise=True,
        verbose=-1,
    )
    model.fit(train[FEATURES], train["label"])
    return model


def evaluate(test: pd.DataFrame, model_scores: np.ndarray, base_scores: np.ndarray) -> dict:
    """PR-AUC (average precision) của model và baseline trên CÙNG tập test, cùng prevalence."""
    from sklearn.metrics import average_precision_score

    y = test["label"].to_numpy()
    prevalence = float(y.mean())
    model_ap = float(average_precision_score(y, model_scores))
    base_ap = float(average_precision_score(y, base_scores))
    return {
        "test_rows": int(len(y)),
        "prevalence": prevalence,
        "pr_auc_model": model_ap,
        "pr_auc_baseline": base_ap,
        "lift_model": model_ap / prevalence,
        "beats_baseline": model_ap > base_ap,
    }
