<!-- Mục đích: pipeline train model return-risk trên Kubeflow + version ở MLflow; bằng chứng rubric mle dòng 13–14. -->
# Train model return-risk — Kubeflow Pipelines + MLflow

Rubric `mle` dòng 13: pipeline KFP ≥ 4 bước (load từ offline feature store, split, train, evaluate).
Dòng 14: lưu model + dữ liệu + metadata mỗi run lên MLflow, tự gắn tag `latest` / `production`.

## Đường đi

```
s3://fsds/feature-store/training_lines   (Spark export_training_lines.py: line có nhãn + thuộc tính lúc đặt)
   │ ① load_features: Feast get_historical_features — PIT join sku_return_daily + sku_return_24h
   ▼ ② split: test = `test_days` ngày đặt đơn cuối (không chia ngẫu nhiên)
   ▼ ③ train: LightGBM, seed cố định
   ▼ ④ evaluate: PR-AUC model vs baseline (tỷ lệ trả 90 ngày của SKU) trên CÙNG tập test
   ▼ ⑤ register: MLflow run (param, metric, model, file train/test + sha256, dataset_version)
        → model "return-risk" version mới: tag latest=true; alias + tag production CHỈ khi thắng baseline
```

| Thành phần | File |
|---|---|
| Logic thuần (test được không cần KFP) | [`ml/train_lib.py`](../ml/train_lib.py), test [`tests/unit/test_train_lib.py`](../tests/unit/test_train_lib.py) |
| Pipeline KFP 5 bước | [`ml/training_pipeline.py`](../ml/training_pipeline.py) |
| Image các bước | [`docker/Dockerfile.train`](../docker/Dockerfile.train) (dựng trên `fsds-feast`) |
| Xuất tập train | [`spark_jobs/gold/export_training_lines.py`](../spark_jobs/gold/export_training_lines.py), task `export_training_lines` của DAG `ml_feature_materialize` |
| MLflow server | service `mlflow` (profile `ml`), [`docker/Dockerfile.mlflow`](../docker/Dockerfile.mlflow) |
| Submit | [`scripts/run/run_training_pipeline.sh`](../scripts/run/run_training_pipeline.sh) |

## Quyết định

- **Feature lúc đặt đơn** từ `fact_order_lines` (kênh, account, số lượng, tiền, đơn giá) + feature SKU
  từ Feast. Bỏ mọi cột biết sau khi đặt (giao, hủy, trả, hoàn) — leakage.
- **Gate production**: phải thắng baseline PR-AUC. Thua thì version vẫn được lưu (tag `latest`) nhưng
  không deploy. MLflow cấm alias tên `latest` (`models:/return-risk/latest` đã có sẵn nghĩa đó) nên
  `latest` là **tag**; `production` vừa là alias (để deploy) vừa là tag.
- **MLflow tắt job execution** (`MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`): 7 tiến trình huey ăn ~1 GB
  và từng làm container bị OOM-kill; tracking + registry không cần.

## Phát hiện: dữ liệu v2 mặc định không có tín hiệu

Run đầu (bộ `small`, 3.325 line, test 757 line): PR-AUC model **0,287**, baseline **0,291**, prevalence
**0,291** — cả hai ngang đoán mò. Nguyên nhân: `generator/generate_order_lifecycle.py` rút kịch bản
trả/hoàn **độc lập** với SKU, kênh, giá (mọi line cùng giá, cùng số lượng). Gate chặn đúng.

Sửa: tuỳ chọn `--sku-return-skew S` (mặc định 0 = như cũ, nội dung dữ liệu không đổi — đã đối chiếu
3.631/3.631 line). S > 0: mỗi SKU có hệ số `exp(S·z)`, `z ~ N(0,1)` cố định theo SKU, nhân thêm hệ số
kênh; test `test_sku_return_skew_creates_learnable_signal`. Bộ small với S = 1,0: tỷ lệ trả theo SKU
từ 7% đến 54%, tổng vẫn ~29%.

## Chạy local

```bash
docker compose --profile ml up -d mlflow                     # UI http://localhost:5050
kubectl --context kind-fsds-ml apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=2.17.2"
kubectl --context kind-fsds-ml apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic?ref=2.17.2"
docker build -f docker/Dockerfile.train -t fsds-train:0.1.0 . && kind load docker-image fsds-train:0.1.0 --name fsds-ml
scripts/run/run_training_pipeline.sh train-N                 # UI KFP: kubectl port-forward svc/ml-pipeline-ui 8888:80
```

## Bằng chứng

Run `train-3` (KFP 2.17.2 trong kind, 2026-10-21): **SUCCEEDED** cả 5 bước.

| Mục | Giá trị |
|---|---|
| Tập train / test | 2.568 / 757 line (test = 10 ngày cuối), prevalence test 0,291 |
| PR-AUC model / baseline | 0,287 / 0,291 → `beats_baseline = false` |
| MLflow | model `return-risk` **v2**: tag `latest=true`, `production=false`, không alias production |
| Version dữ liệu (tag run) | `data.dataset_version`, `data.generation_id`, `data.train_sha256`, `data.test_sha256`, `data.entity_path`; artifact `data/train`, `data/test`; dataset input `train` |

Đây là dữ liệu **không tín hiệu** (mục trên) ⇒ gate từ chối đúng. Chạy lại trên bộ có
`--sku-return-skew` để có version `production` cho KServe.

<!-- Ảnh KFP run xanh 5 bước + MLflow model version/tag: người học chụp rồi gắn vào đây. -->
