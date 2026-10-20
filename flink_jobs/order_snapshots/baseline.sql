-- Baseline Flink: đọc topic Amazon đã phát, giữ đúng ID và event time.
-- Source bounded tại offset mới nhất khi job bắt đầu để run kết thúc và đếm được.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';

CREATE TABLE amazon_notifications (
  EventTime STRING,
  NotificationMetadata ROW<NotificationId STRING>,
  Payload ROW<OrderChangeNotification ROW<AmazonOrderId STRING>>
) WITH (
  'connector' = 'kafka',
  'topic' = 'amazon.order-notifications',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'fsds-flink-baseline-amazon',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE baseline_amazon (
  notification_id STRING,
  order_id STRING,
  event_time STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/baseline-amazon-proof',
  'format' = 'csv'
);

INSERT INTO baseline_amazon
SELECT NotificationMetadata.NotificationId,
       Payload.OrderChangeNotification.AmazonOrderId,
       EventTime
FROM amazon_notifications;
