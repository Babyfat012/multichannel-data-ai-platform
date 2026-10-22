"""Đọc Kafka `ml.sku_return_24h` (do Flink ghi) rồi push vào Feast: Redis online + Parquet offline.

Commit offset Kafka SAU khi push thành công (at-least-once): chạy lại có thể ghi trùng dòng
offline, nhưng cùng (SKU, event_timestamp, giá trị) nên PIT join vẫn ra đúng số.
"""

import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from confluent_kafka import Consumer
from feast import FeatureStore
from feast.data_source import PushMode

SCHEMA = pa.schema(
    [
        ("seller_sku", pa.string()),
        ("event_timestamp", pa.timestamp("us", tz="UTC")),
        ("approved_returns_24h", pa.int64()),
    ]
)


def ensure_offline_file(path):
    """Feast chỉ append được vào file đã tồn tại; tạo file rỗng đúng schema nếu chưa có."""
    fs = s3fs.S3FileSystem(endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))
    key = path.removeprefix("s3://")
    if not fs.exists(key):
        pq.write_table(SCHEMA.empty_table(), key, filesystem=fs)


def to_frame(messages):
    """Tin JSON của Flink → DataFrame đúng kiểu; timestamp Flink là UTC không múi giờ."""
    rows = [json.loads(m.value()) for m in messages]
    frame = pd.DataFrame(rows, columns=SCHEMA.names)
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    frame["approved_returns_24h"] = frame["approved_returns_24h"].astype("int64")
    return frame


def main():
    """Vòng lặp: gom tối đa 500 tin hoặc 2 giây, push một lần, rồi commit."""
    ensure_offline_file(os.environ["FEAST_SKU_RETURN_24H_PATH"])
    store = FeatureStore(os.path.dirname(os.path.abspath(__file__)))
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "feast-pusher",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.environ.get("FEAST_PUSH_TOPIC", "ml.sku_return_24h")])
    try:
        while True:
            messages = [m for m in consumer.consume(500, timeout=2.0) if not m.error()]
            if not messages:
                continue
            frame = to_frame(messages)
            store.push("sku_return_24h_push", frame, to=PushMode.ONLINE_AND_OFFLINE)
            consumer.commit(asynchronous=False)
            print(f"pushed {len(frame)} rows, last {frame['event_timestamp'].max()}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
