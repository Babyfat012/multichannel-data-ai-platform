-- Cùng một luồng Order UNPAID, hai cửa sổ để so thời điểm phát hiện burst.
-- TUMBLE 5 phút cập nhật mỗi 5 phút; HOP 5 phút cập nhật mỗi 1 phút.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';

CREATE TABLE tiktok_webhooks (
  tts_notification_id STRING,
  `timestamp` BIGINT,
  data ROW<order_id STRING, order_status STRING>,
  event_ts AS TO_TIMESTAMP_LTZ(`timestamp`, 0),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'fsds-flink-window-tiktok',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE VIEW new_orders AS
SELECT tts_notification_id, event_ts
FROM tiktok_webhooks
WHERE data.order_status = 'UNPAID';

CREATE TABLE tiktok_tumble_5m (
  window_start STRING,
  window_end STRING,
  orders BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/tiktok-tumble-5m',
  'format' = 'csv'
);

CREATE TABLE tiktok_hop_5m_1m (
  window_start STRING,
  window_end STRING,
  orders BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/tiktok-hop-5m-1m',
  'format' = 'csv'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO tiktok_tumble_5m
  SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
  FROM TABLE(TUMBLE(TABLE new_orders, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE))
  GROUP BY window_start, window_end;

  INSERT INTO tiktok_hop_5m_1m
  SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
  FROM TABLE(HOP(TABLE new_orders, DESCRIPTOR(event_ts),
                 INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
  GROUP BY window_start, window_end;
END;
