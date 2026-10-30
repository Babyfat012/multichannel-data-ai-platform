-- ML Platform stream feature: số Return "approved" của mỗi SKU trong 24h gần nhất (HOP trượt 1h).
-- Kafka return_events → bỏ bản retry trùng → tra line_id→SKU ở PostgreSQL → Kafka ml.sku_return_24h.
-- Chạy liên tục (streaming). @@PG_*@@ do scripts/run/run_sku_return_feature.sh điền lúc submit.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'ml-sku-return-24h';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoint ra đĩa (mount /mnt/platform-data/flink trên EC2): mặc định giữ ở bộ nhớ JobManager, tối đa 5 MB;
-- state bỏ trùng + HOP của stream A ~72 MB ⇒ mọi checkpoint hỏng, job RESTARTING mãi (30/10).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
-- Partition Kafka không có tin thì không giữ watermark lại mãi.
SET 'table.exec.source.idle-timeout' = '10s';

CREATE TABLE return_events (
  record_sha256 STRING,
  effective_at STRING,
  payload ROW<event_type STRING, quantity BIGINT, line_id STRING>,
  dataset_version STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(effective_at, 1, 23), 'T', ' ')),
  proc_time AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' HOUR
) WITH (
  'connector' = 'kafka',
  'topic' = 'order_lifecycle.seller_postsales.return_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ml-sku-return-24h',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Bảng tra do DAG dp_order_lifecycle_warehouse publish; cache để không gọi DB mỗi tin.
CREATE TABLE line_sku (
  dataset_version STRING, line_id STRING, seller_sku STRING,
  PRIMARY KEY (dataset_version, line_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/@@PG_DB@@',
  'table-name' = 'warehouse_lifecycle.fact_order_lines',
  'username' = '@@PG_USER@@',
  'password' = '@@PG_PASSWORD@@',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.expire-after-write' = '1h'
);

CREATE TABLE sku_return_24h (
  seller_sku STRING,
  event_timestamp TIMESTAMP(3),
  approved_returns_24h BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ml.sku_return_24h',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'key.fields' = 'seller_sku',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

-- Producer retry phát lại đúng bytes ⇒ giữ bản đầu theo record_sha256 (giữ được rowtime).
CREATE VIEW approved AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY record_sha256 ORDER BY event_ts) AS rn
  FROM return_events
  WHERE payload.event_type = 'approved' AND payload.quantity > 0
) WHERE rn = 1;

CREATE VIEW approved_sku AS
SELECT a.event_ts, l.seller_sku
FROM approved AS a
JOIN line_sku FOR SYSTEM_TIME AS OF a.proc_time AS l
  ON a.dataset_version = l.dataset_version AND a.payload.line_id = l.line_id;

-- ponytail: HOP chỉ phát cửa sổ CÓ tin; SKU hết Return thì Redis giữ số cũ tới khi có tin mới.
-- Cần về 0 thật thì thêm luồng phát 0 theo lịch.
INSERT INTO sku_return_24h
SELECT seller_sku, window_end, COUNT(*)
FROM TABLE(HOP(TABLE approved_sku, DESCRIPTOR(event_ts), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
GROUP BY seller_sku, window_start, window_end;
