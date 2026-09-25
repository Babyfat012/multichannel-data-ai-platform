"""Chạy phần Stream: phát message sau `cutoff` vào Kafka hoặc ra file.

Hai nơi nhận (sink):
- `kafka`: gửi vào Kafka, khóa message là mã đơn gốc, giá trị là JSON gốc.
- `file`: ghi `stream/<topic>/part-00000.jsonl` dưới `storage.output_root`,
  mỗi dòng gồm `arrive_at`, `key` và `value`. Dùng để test và để đo không cần Kafka.

`speed` chia nhỏ thời gian chờ: 1 là đúng nhịp cấu hình (khoảng 40 phút cho
30 ngày), 60 là nhanh gấp 60 lần, 0 là không chờ.

Sau khi phát xong, ghi `stream_manifest.json` với số đo thật: tỷ lệ trùng,
tỷ lệ trễ, số message lệch thứ tự quá 1 giờ, hệ số burst. Đây là đáp án để đối chiếu
với những gì Flink đo được.
"""

from __future__ import annotations

import json
import statistics
import time
from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any, Protocol

from generator.config import GeneratorConfig
from generator.core.simulate import Simulator
from generator.delivery.stream_events import TOPICS, StreamMessage, stream_messages, wall_offset
from generator.storage import OutputStore


class Sink(Protocol):
    """Nơi nhận message."""

    def send(self, msg: StreamMessage) -> None: ...
    def close(self) -> None: ...


class FileSink:
    """Ghi message ra file JSONL theo topic (giữ trong RAM rồi ghi một lần khi đóng)."""

    def __init__(self, store: OutputStore) -> None:
        self.store = store
        self.rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def send(self, msg: StreamMessage) -> None:
        self.rows[msg.topic].append({"arrive_at": msg.arrive_at.isoformat(), "key": msg.key, "value": msg.value})

    def close(self) -> None:
        for topic, rows in self.rows.items():
            self.store.write_jsonl(f"stream/{topic}/part-00000.jsonl", rows)


class KafkaSink:
    """Gửi message vào Kafka qua `confluent-kafka`."""

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 50})

    def send(self, msg: StreamMessage) -> None:
        self.producer.produce(msg.topic, key=msg.key.encode(), value=json.dumps(msg.value).encode())
        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush()


class StreamStats:
    """Đếm các số đo của Stream trong lúc phát."""

    def __init__(self, config: GeneratorConfig) -> None:
        self.config = config
        self.rows: Counter = Counter()
        self.duplicates: Counter = Counter()
        self.keys: dict[str, set[str]] = defaultdict(set)
        self.delays_min: dict[str, list[float]] = defaultdict(list)
        self.out_of_order: Counter = Counter()
        self.max_event: dict[str, Any] = {}
        self.new_tiktok_orders_by_hour: Counter = Counter()

    def add(self, msg: StreamMessage) -> None:
        t = msg.topic
        self.rows[t] += 1
        self.duplicates[t] += msg.is_duplicate
        self.keys[t].add(msg.dedup_key)
        if not msg.is_duplicate:
            self.delays_min[t].append((msg.arrive_at - msg.event_time).total_seconds() / 60)
        # Lệch thứ tự đáng kể: đến sau một message có event_time muộn hơn quá 1 giờ.
        if t in self.max_event and msg.event_time < self.max_event[t] - timedelta(hours=1):
            self.out_of_order[t] += 1
        self.max_event[t] = max(self.max_event.get(t, msg.event_time), msg.event_time)
        if t == TOPICS["tiktok_shop"] and not msg.is_duplicate and msg.value["data"]["order_status"] == "UNPAID":
            self.new_tiktok_orders_by_hour[msg.event_time.replace(minute=0, second=0, microsecond=0)] += 1

    def summary(self, live_multiplier: float) -> dict[str, Any]:
        topics = {}
        for t, rows in self.rows.items():
            delays = self.delays_min[t]
            topics[t] = {
                "rows": rows,
                "unique_keys": len(self.keys[t]),
                "duplicate_rate": round(1 - len(self.keys[t]) / rows, 6),
                "injected_duplicates": self.duplicates[t],
                "median_delay_minutes": round(statistics.median(delays), 2) if delays else None,
                "share_late_over_1h": round(sum(d > 60 for d in delays) / len(delays), 6) if delays else None,
                "out_of_order_over_1h": self.out_of_order[t],
            }
        return {"topics": topics, "burst": self._burst(live_multiplier)}

    def _burst(self, live_multiplier: float) -> dict[str, Any] | None:
        """So số Order TikTok mới mỗi giờ trong buổi live với cùng giờ đó ở các ngày khác."""
        live = self.config.drift.tiktok_live
        if live is None:
            return None
        first = self.config.scale.days_history + 1
        last = self.config.scale.days_history + self.config.scale.days_stream
        hours = range(int(live.start_hour_utc), int(live.start_hour_utc + live.duration_hours))

        def per_hour(day: int) -> float:
            start = self.config.day_start(day)
            return sum(self.new_tiktok_orders_by_hour[start + timedelta(hours=h)] for h in hours) / len(hours)

        normal_days = [d for d in range(first, last + 1) if d != live.day]
        normal = statistics.mean(per_hour(d) for d in normal_days) if normal_days else 0.0
        during = per_hour(live.day)
        return {
            "live_day": live.day,
            "multiplier_truth": round(live_multiplier, 3),
            "new_orders_per_hour_live": round(during, 1),
            "new_orders_per_hour_normal": round(normal, 1),
            "measured_ratio": round(during / normal, 2) if normal else None,
        }


def run_stream(config: GeneratorConfig, sink: Sink, speed: float = 1.0) -> dict[str, Any]:
    """Phát toàn bộ Stream vào `sink`, trả về manifest với số đo."""
    stats = StreamStats(config)
    started = time.monotonic()
    try:
        for msg in stream_messages(config):
            if speed > 0:
                wait = wall_offset(config, msg.arrive_at) / speed - (time.monotonic() - started)
                if wait > 0:
                    time.sleep(wait)
            sink.send(msg)
            stats.add(msg)
    finally:
        sink.close()

    manifest = {
        "cutoff": config.cutoff.isoformat(),
        "stream_end": config.stream_end.isoformat(),
        "speed": speed,
        **stats.summary(Simulator(config).live_multiplier),
    }
    OutputStore(config.storage.output_root).write_json("stream_manifest.json", manifest)
    return manifest
