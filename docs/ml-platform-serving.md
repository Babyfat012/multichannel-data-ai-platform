<!-- Mục đích: phục vụ model return-risk bằng KServe (model-inside-image) -->
# Serve model return-risk — KServe

KServe serve model vừa train, **model-inside-image**; có pod KServe + YAML +
ảnh gọi thử thành công.

## Hợp đồng gọi

`POST http://return-risk-predictor.ml-serving.svc.cluster.local/v1/models/return-risk:predict`

```json
{"instances": [{"seller_sku": "sku-00002", "channel": "tiktok_shop", "account_id": "account-tiktok_shop-1",
                "ordered_basis_qty": 3, "ordered_product_net_minor": 30000}]}
```

Trả về `{"predictions": [{"seller_sku", "score", "abstain", "reason", "model_version"}]}`.
`score` là tín hiệu **xếp hạng**, không phải xác suất (Q38). Người gọi chỉ gửi thuộc tính đơn:
predictor **tự đọc** feature SKU từ Feast online (Redis), nên danh sách feature chỉ nằm một chỗ.
SKU có < 20 line có nhãn trong 90 ngày ⇒ `abstain: true, reason: insufficient_history`.

| Thành phần | File |
|---|---|
| Predictor | [`ml/serve.py`](../ml/serve.py), test [`tests/unit/test_serve.py`](../tests/unit/test_serve.py) |
| Image (model nằm trong image) | [`docker/Dockerfile.serve`](../docker/Dockerfile.serve) |
| InferenceService (RawDeployment) | [`infra/k8s/ml/return-risk.yaml`](../infra/k8s/ml/return-risk.yaml) |
| Deploy: MLflow alias `production` → image → kind → apply | [`scripts/run/deploy_return_risk.sh`](../scripts/run/deploy_return_risk.sh) |

## Cài KServe (local)

RawDeployment: không cần Knative/Istio, chỉ cert-manager + controller.

```bash
K="kubectl --context kind-platform"
$K apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
$K -n cert-manager wait --for=condition=Available deploy --all --timeout=300s
helm --kube-context kind-platform install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.21.0
helm --kube-context kind-platform install kserve oci://ghcr.io/kserve/charts/kserve --version v0.21.0 \
  -n kserve --create-namespace --set kserve.controller.deploymentMode=RawDeployment
scripts/run/deploy_return_risk.sh
```

## Bằng chứng

Deploy 2026-10-21 (kind, KServe v0.21.0 RawDeployment): `return-risk` READY = True, pod
`return-risk-predictor-…` Running, image `platform-return-risk:v3` = MLflow `return-risk` v3 (alias
`production`, PR-AUC 0,341 vs baseline 0,329 vs prevalence 0,312).

```text
GET  /v1/models/return-risk          → {"name":"return-risk","ready":true}
POST /v1/models/return-risk:predict  → sku-00002 0,346 · sku-00099 0,305 · sku-00003 0,254
```

Thứ tự khớp tỷ lệ trả thật trong dữ liệu (sku-00002 ~54%, sku-00003 ~7%). Cả ba `abstain:
insufficient_history` là **đúng**: Redis giữ ảnh chụp ngày 2026-10-13, còn đơn cuối đặt 2026-02-14 ⇒
cửa sổ 90 ngày không còn line có nhãn. Model vẫn trả score để tham khảo nhưng không khẳng định.

Lỗi gặp khi deploy: client MLflow tải artifact bằng presigned URL trỏ `minio:9000` (tên chỉ có trong
mạng Docker) ⇒ script tải `model.lgb` thẳng qua endpoint proxy `/api/2.0/mlflow-artifacts/...`.

## Metric

Pod khai `prometheus.io/scrape|port=8080|path=/metrics` (cờ `enable-prometheus-scraping` của KServe trỏ
cổng 9091 của sidecar gộp metric — RawDeployment không có sidecar đó). Ngoài histogram độ trễ của KServe
(`request_predict_seconds`…), predictor xuất metric nghiệp vụ:

| Metric | Ý nghĩa |
|---|---|
| `return_risk_predictions_total{abstain}` | số dòng đã chấm, tách theo có abstain hay không |
| `return_risk_errors_total{reason}` | request lỗi (vd `missing_field` → HTTP **400**, không phải 500) |
| `return_risk_feast_lookup_seconds` | thời gian đọc feature online |

Kiểm qua Prometheus (route `metrics.`): sau 5 request × 2 dòng + 1 request thiếu trường →
`predictions{abstain="true"} = 10`, `errors{reason="missing_field"} = 1`, `feast_lookup_count = 5`.

**Bẫy:** deploy lại cùng tag `v3` thì node kind giữ image cũ theo tag ⇒ pod chạy code cũ. Script giờ
gắn tag `v<version>-<băm code+model>` (vd `v3-d18e5f42`), mỗi bản build một tag.

<!-- Ảnh pod KServe + curl predict: người vận hành chụp rồi gắn vào đây. -->
