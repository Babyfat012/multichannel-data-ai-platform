<!-- Mục đích: job đẩy stream feature vào kho OFFLINE và ONLINE của Feast, -->
# Stream feature vào Feast — ML Platform bước 3

deploy các job đẩy **streaming feature** vào kho OFFLINE và ONLINE.

## Đường đi của dữ liệu

```
Kafka order_lifecycle.seller_postsales.return_events     (publisher của Data Platform)
   │ ① Flink SQL sku_return_feature.sql (chạy liên tục)
   │    - bỏ bản retry trùng theo record_sha256
   │    - tra line_id → seller_sku ở PostgreSQL warehouse_lifecycle.fact_order_lines (lookup join, cache)
   │    - HOP 24h trượt 1h theo event time, đếm Return "approved"
   ▼
Kafka ml.sku_return_24h        {seller_sku, event_timestamp = window_end, approved_returns_24h}
   │ ② feast-pusher (push_stream_features.py, service Compose)
   │    store.push(..., to=ONLINE_AND_OFFLINE), commit offset SAU khi push
   ├──► Redis trong kind (ONLINE)          feature view sku_return_24h
   └──► s3://platform/feature-store/sku_return_24h.parquet (OFFLINE, train dùng PIT join)
```

| Thành phần | File |
|---|---|
| Job Flink | [`flink_jobs/ml/sku_return_feature.sql`](../flink_jobs/ml/sku_return_feature.sql) |
| Submit job (điền credential từ `.env`) | [`scripts/run/run_sku_return_feature.sh`](../scripts/run/run_sku_return_feature.sh) |
| Jar JDBC + PostgreSQL cho Flink | [`docker/Dockerfile.flink`](../docker/Dockerfile.flink) |
| Pusher | [`feature_repo/push_stream_features.py`](../feature_repo/push_stream_features.py), image [`docker/Dockerfile.feast`](../docker/Dockerfile.feast), service `feast-pusher` |
| Feature view | `sku_return_24h` trong [`feature_repo/features.py`](../feature_repo/features.py) |

## Vì sao làm vậy

- **Event Return không có SKU**, chỉ có `line_id`. Flink tra SKU từ warehouse PostgreSQL thay vì
  chở cả đơn hàng vào Kafka. Cache 1h vì một line không đổi SKU.
- **Không cho Flink ghi thẳng Redis**: làm vậy bỏ qua Feast, kho offline không có dữ liệu và train
  không PIT join được.
- **At-least-once**: pusher commit offset sau khi push. Chết giữa chừng thì đọc lại, có thể ghi
  trùng dòng offline — cùng giá trị nên PIT join không đổi.

**Giới hạn (đã đánh dấu `ponytail:` trong SQL):** HOP chỉ phát cửa sổ có tin. SKU hết Return thì
Redis giữ số cũ tới khi có tin mới. Push offline đọc + ghi lại cả file mỗi lần — ổn ở cỡ demo.

## Chạy local

```bash
# 0. Đã có Gold (docs/ml-platform-feature-store.md bước 1) và Redis trong kind (setup_kind_feature_store.sh)

# 1. Bảng tra line → SKU: publish warehouse từ Gold hiện tại (run_id = generation_id của Gold).
#    Máy chật RAM thì chạy thẳng job export_order_lifecycle.py bằng Spark local[2] (driver 1g) thay vì DAG.
docker compose --profile airflow --profile spark up -d --wait airflow spark-master spark-worker-1 spark-worker-2
docker compose --profile airflow exec airflow airflow dags unpause dp_order_lifecycle_warehouse
docker compose --profile airflow exec airflow airflow dags trigger dp_order_lifecycle_warehouse \
  --conf '{"run_id": "<generation_id>", "cutoff": "2026-06-01T00:00:00.000000Z"}'
docker compose --profile airflow --profile spark stop airflow spark-master spark-worker-1 spark-worker-2

# 2. Kafka + Flink + pusher
docker compose --profile stream --profile flink --profile ml-stream up -d kafka flink-jobmanager flink-taskmanager feast-pusher

# 3. Phát event vận hành vào Kafka
.venv/bin/python -m generator.publish_order_lifecycle_events --source out/order_lifecycle_live \
  --bootstrap localhost:9094 --epoch 1 --manifest out/stream-ml-1.json

# Lần đầu (làm TRƯỚC bước 3): tạo topic và đăng ký view với Feast
for t in carrier.delivery_events seller_postsales.return_events seller_postsales.refund_events seller_wms.stock_operations; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic order_lifecycle.$t --partitions 3 --replication-factor 1
done
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ml.sku_return_24h --partitions 1 --replication-factor 1
docker compose --profile ml-stream run --rm --no-deps feast-pusher feast apply

# 4. Submit job Flink (UI: http://localhost:8082)
scripts/run/run_sku_return_feature.sh

# 5. Kiểm
docker compose --profile ml-stream logs feast-pusher | grep pushed
kubectl --context kind-platform -n feature-store exec deploy/redis -- redis-cli dbsize
```

## CI/CD

[`ci/jenkins/stream-features.Jenkinsfile`](../ci/jenkins/stream-features.Jenkinsfile): **Test** (ruff, test docstring +
renderer stream, test Spark `check_sku_daily_features.py`) → **Build** (image Flink có jar JDBC, image
Feast) → **Deploy** (Kafka + Flink, `feast apply`, recreate pusher, huỷ job Flink cùng tên rồi submit lại)
→ **Verify** (đúng 1 job `ml-sku-return-24h` RUNNING, pusher running).

Secret không nằm trong repo: Jenkins credential `platform-postgres` và `platform-minio` (username/password),
truyền vào bằng `withCredentials`; [`run_sku_return_feature.sh`](../scripts/run/run_sku_return_feature.sh)
chỉ đọc `.env` khi chạy tay. Jenkins server chưa dựng (việc chung của nhóm CI/CD) — file này chưa chạy.

## Bằng chứng

Lần chạy local 2026-10-21 (fixture 15 đơn / 2 SKU):

- Warehouse: 30 line `line_id → sku-A/sku-B` trong `warehouse_lifecycle.fact_order_lines`.
- Flink job `ml-sku-return-24h`: **RUNNING** (streaming, không tự dừng).
- Pusher: `pushed 72 rows, last 2026-02-05 10:00:00+00:00`; consumer group `feast-pusher` lag = 0.
- Offline: `sku_return_24h.parquet` 72 dòng. Online: `sku-A = 5`, `sku-B = None` (sku-B không có
  Return approved nào — đúng).
- **Đối chiếu độc lập**: đếm lại từ file JSONL nguồn (bỏ trùng theo `source_event_id`, tra SKU từ
  PostgreSQL, cửa sổ `[end−24h, end)`) → 72/72 cửa sổ khớp, 0 sai lệch.
- Cửa sổ cuối dừng ở 02-05 10:00 vì watermark chỉ tiến khi có tin mới (luồng không tự kết thúc).

<!-- Ảnh Flink UI job RUNNING + log pusher: người vận hành chụp rồi gắn vào đây. -->
