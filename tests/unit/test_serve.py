"""Kiểm ml/serve.py không cần KServe/Redis thật: ghép feature online, chấm điểm, cờ abstain."""

import sys
import types

import numpy as np
import pandas as pd
import pytest

# kserve/lightgbm có thể không cài trong môi trường test: thay bằng module giả tối thiểu.
sys.modules.setdefault("lightgbm", types.SimpleNamespace(Booster=object))
fake_kserve = types.ModuleType("kserve")
fake_kserve.Model = type("Model", (), {"__init__": lambda self, name: setattr(self, "name", name)})
fake_kserve.ModelServer = object
sys.modules.setdefault("kserve", fake_kserve)
fake_errors = types.ModuleType("kserve.errors")
fake_errors.InvalidInput = type("InvalidInput", (ValueError,), {})
sys.modules.setdefault("kserve.errors", fake_errors)

from ml.serve import ReturnRisk  # noqa: E402


class FakeStore:
    """Feast online giả: trả số line có nhãn theo bảng tra."""

    def __init__(self, history):
        """history: {sku: labeled_count_90d}."""
        self.history = history

    def get_online_features(self, features, entity_rows):
        """Cùng hình dạng kết quả với Feast: đối tượng có to_df()."""
        skus = [r["seller_sku"] for r in entity_rows]
        return types.SimpleNamespace(
            to_df=lambda: pd.DataFrame(
                {
                    "seller_sku": skus,
                    "labeled_count_90d": [self.history.get(s) for s in skus],
                    "return_or_refund_rate_90d": [0.3 for _ in skus],
                    "approved_returns_24h": [None for _ in skus],
                }
            )
        )


class FakeBooster:
    """Booster giả trả điểm hằng số."""

    def predict(self, frame):
        """Điểm hằng số cho mọi dòng."""
        return np.full(len(frame), 0.42)


def model(history):
    """Predictor đã nạp store/booster giả, ngưỡng 20."""
    m = ReturnRisk("return-risk")
    m.min_history, m.version = 20, "7"
    m.store, m.booster = FakeStore(history), FakeBooster()
    return m


ORDER = {
    "channel": "amazon",
    "country": "a",
    "ordered_basis_qty": 3,
    "ordered_product_net_minor": 30000,
}


@pytest.mark.parametrize(
    ("history", "abstain"),
    [(None, True), (0, True), (19, True), (20, False), (500, False)],
)
def test_abstain_boundary(history, abstain):
    """Biên ngưỡng 20: 19 abstain, 20 không; SKU chưa có trong Redis (None) cũng abstain."""
    out = model({"sku-1": history}).predict({"instances": [{"seller_sku": "sku-1", **ORDER}]})
    p = out["predictions"][0]
    assert p["abstain"] is abstain and p["score"] == 0.42 and p["model_version"] == "7"
    assert p["reason"] == ("insufficient_history" if abstain else None)


def test_missing_order_field_is_rejected():
    """Thiếu thuộc tính đơn ⇒ lỗi rõ ràng, không chấm trên NaN."""
    from kserve.errors import InvalidInput

    with pytest.raises(InvalidInput, match="channel"):
        model({}).predict(
            {
                "instances": [
                    {
                        "seller_sku": "sku-1",
                        "country": "a",
                        "ordered_basis_qty": 1,
                        "ordered_product_net_minor": 1,
                    }
                ]
            }
        )


def test_order_of_rows_kept_for_many_skus():
    """Nhiều instance: kết quả giữ đúng thứ tự SKU đầu vào."""
    rows = [{"seller_sku": s, **ORDER} for s in ("b", "a", "c")]
    out = model({"a": 50, "b": 1, "c": 50}).predict({"instances": rows})
    assert [p["seller_sku"] for p in out["predictions"]] == ["b", "a", "c"]
    assert [p["abstain"] for p in out["predictions"]] == [True, False, False]
