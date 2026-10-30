"""KServe predictor return-risk: model nằm sẵn trong image, tự đọc feature SKU từ Feast online.

Người gọi (agent/Web API) chỉ gửi thuộc tính đơn; predictor ghép feature SKU từ Redis rồi chấm.
SKU có ít hơn MIN_HISTORY line đã có nhãn trong 90 ngày ⇒ `abstain` (Q38), vẫn kèm score để tham khảo.
"""

import os
import time

import lightgbm as lgb
import pandas as pd
from kserve import Model, ModelServer
from kserve.errors import InvalidInput
from prometheus_client import Counter, Histogram

from ml.train_lib import FEAST_FEATURES, FEATURES, add_order_features

# Metric nghiệp vụ, lộ ở /metrics cùng metric độ trễ mặc định của KServe (Prometheus kéo về).
PREDICTIONS = Counter("return_risk_predictions_total", "Số dòng đã chấm", ["abstain"])
ERRORS = Counter("return_risk_errors_total", "Số request lỗi", ["reason"])
FEAST_SECONDS = Histogram("return_risk_feast_lookup_seconds", "Thời gian đọc feature online")

ORDER_FIELDS = [
    "seller_sku",
    "channel",
    "country",
    "ordered_basis_qty",
    "ordered_product_net_minor",
]


class ReturnRisk(Model):
    """Model v1 protocol: `{"instances": [...]}` → `{"predictions": [...]}`."""

    def __init__(self, name: str):
        """Khởi tạo cấu hình, phiên bản và trạng thái nạp model dự đoán rủi ro."""
        super().__init__(name)
        self.min_history = int(os.environ.get("MIN_HISTORY", "20"))
        self.version = os.environ.get("MODEL_VERSION", "unknown")
        self.booster = None
        self.store = None

    def load(self):
        """Nạp booster từ image và mở kết nối Feast (registry S3 + Redis)."""
        from feast import FeatureStore

        self.booster = lgb.Booster(model_file=os.environ.get("MODEL_PATH", "/opt/model/model.lgb"))
        self.store = FeatureStore(os.environ.get("FEAST_REPO", "/opt/feature_repo"))
        self.ready = True
        return self.ready

    def score(self, instances: list[dict]) -> list[dict]:
        """Ghép feature online theo SKU, chấm điểm, gắn cờ abstain."""
        orders = pd.DataFrame(instances)
        missing = [c for c in ORDER_FIELDS if c not in orders]
        if missing:
            ERRORS.labels("missing_field").inc()
            raise InvalidInput(f"thiếu trường {missing}")  # KServe trả 400, không phải 500
        started = time.perf_counter()
        online = self.store.get_online_features(
            features=FEAST_FEATURES, entity_rows=[{"seller_sku": s} for s in orders["seller_sku"]]
        ).to_df()
        FEAST_SECONDS.observe(time.perf_counter() - started)
        frame = add_order_features(
            pd.concat([orders.reset_index(drop=True), online.drop(columns="seller_sku")], axis=1)
        )
        scores = self.booster.predict(frame[FEATURES])
        history = frame["labeled_count_90d"].fillna(0)
        for h in history:
            PREDICTIONS.labels(str(h < self.min_history).lower()).inc()
        return [
            {
                "seller_sku": sku,
                "score": float(s),
                "abstain": bool(h < self.min_history),
                "reason": "insufficient_history" if h < self.min_history else None,
                "model_version": self.version,
            }
            for sku, s, h in zip(orders["seller_sku"], scores, history)
        ]

    def predict(self, payload: dict, headers: dict | None = None) -> dict:
        """Điểm vào KServe."""
        return {"predictions": self.score(payload["instances"])}


if __name__ == "__main__":
    model = ReturnRisk(os.environ.get("MODEL_NAME", "return-risk"))
    model.load()
    ModelServer().start([model])
