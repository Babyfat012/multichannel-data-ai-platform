"""In bằng chứng của ba vấn đề Stream, gọn trong một màn hình để chụp ảnh.

Đây là phần đối xứng của lệnh `python -m generator report` bên Batch: `report`
đọc lại dữ liệu đã ghi ra MinIO và PostgreSQL, còn script này phát lại Stream
trong bộ nhớ (không cần Kafka) và chỉ ra ví dụ cụ thể cho từng vấn đề.

Số tổng của cả lần chạy nằm trong `stream_manifest.json`; script này trả lời câu
hỏi khác: "cho tôi xem một trường hợp thật". Chạy:

    uv run python scripts/stream_evidence.py                      # configs/default.yaml
    uv run python scripts/stream_evidence.py configs/small.yaml
"""

from __future__ import annotations

import collections
import dataclasses
import json
import sys
from datetime import timedelta

from generator.config import load_config
from generator.core.simulate import Simulator
from generator.delivery.stream_events import TOPICS, stream_messages

RULE = "=" * 78


def main(config_path: str) -> None:
    """Quét một lần qua Stream, giữ lại ví dụ tiêu biểu của từng vấn đề."""
    config = load_config(config_path)
    live = config.drift.tiktok_live
    hours = range(int(live.start_hour_utc), int(live.start_hour_utc + live.duration_hours))

    originals: dict[str, object] = {}
    dup_pair = None  # (bản gốc, bản gửi lại)
    worst_late = None  # (message, event_time mới nhất đã đến trước đó)
    newest = None
    orders_by_hour: collections.Counter = collections.Counter()

    for msg in stream_messages(config):
        if msg.topic == TOPICS["tiktok_shop"]:
            if msg.is_duplicate:
                if dup_pair is None:
                    dup_pair = (originals[msg.dedup_key], msg)
            else:
                if dup_pair is None:
                    originals[msg.dedup_key] = msg
                if msg.value["data"]["order_status"] == "UNPAID":
                    orders_by_hour[(msg.event_time.date(), msg.event_time.hour)] += 1
        else:
            if newest is not None:
                lag = newest - msg.event_time
                if worst_late is None or lag > worst_late[1] - worst_late[0].event_time:
                    worst_late = (msg, newest)
            newest = msg.event_time if newest is None else max(newest, msg.event_time)

    _print_duplicate(*dup_pair)
    _print_late(*worst_late)
    _print_burst(config, orders_by_hour, hours)


def _print_duplicate(first, second) -> None:
    """Hai bản của cùng một thông báo: khác lúc đến, giống hệt mọi thứ còn lại."""
    print(RULE)
    print("1. DUPLICATE: bản gửi lại của TikTok Shop giữ nguyên tts_notification_id")
    print(RULE)
    print(f"tts_notification_id : {first.dedup_key}")
    print(f"bản gốc đến lúc     : {first.arrive_at:%Y-%m-%d %H:%M:%S}")
    print(f"bản gửi lại đến lúc : {second.arrive_at:%Y-%m-%d %H:%M:%S}"
          f"   → sau {(second.arrive_at - first.arrive_at).total_seconds() / 60:.0f} phút")
    print(f"nội dung giống hệt  : {first.value == second.value}")
    print(f"payload             : {json.dumps(first.value['data'], separators=(',', ':'))}")


def _print_late(msg, newest) -> None:
    """Tin trễ nặng nhất: đến sau một tin có EventTime muộn hơn nhiều."""
    print()
    print(RULE)
    print("2. LATE + OUT-OF-ORDER: notification Amazon đến sau một tin mới hơn nó")
    print(RULE)
    print(f"EventTime   (lúc xảy ra) : {msg.event_time:%Y-%m-%d %H:%M:%S}")
    print(f"PublishTime (lúc đến)    : {msg.arrive_at:%Y-%m-%d %H:%M:%S}"
          f"   → trễ {(msg.arrive_at - msg.event_time).total_seconds() / 60:.0f} phút")
    print(f"tin mới nhất đã đến      : {newest:%Y-%m-%d %H:%M:%S}"
          f"   → tin này cũ hơn {(newest - msg.event_time).total_seconds() / 3600:.1f} giờ")
    print(f"AmazonOrderId            : {msg.key}")


def _print_burst(config, orders_by_hour, hours) -> None:
    """Order mới mỗi giờ của ngày livestream, đặt cạnh ngày ngay trước đó."""
    live = config.drift.tiktok_live
    live_date = config.day_start(live.day).date()
    prev_date = config.day_start(live.day - 1).date()

    print()
    print(RULE)
    print("3. BURST: Order TikTok Shop mới mỗi giờ, ngày livestream so với ngày trước")
    print(RULE)
    # Hàng nhãn và hàng số phải cùng bề rộng phần đầu, nếu không cột sẽ lệch.
    head = len(f"{'':>11} {live_date}")
    for label, day in (("ngày thường", prev_date), ("LIVESTREAM", live_date)):
        print(f"{label:>11} {day}:" + "".join(f"{orders_by_hour[(day, h)]:>5}" for h in range(24)))
    print(f"{'giờ UTC':>{head}}:" + "".join(f"{h:>5}" for h in range(24)))

    in_window = sum(orders_by_hour[(live_date, h)] for h in hours)
    prev_window = sum(orders_by_hour[(prev_date, h)] for h in hours)
    no_live = dataclasses.replace(config, drift=dataclasses.replace(config.drift, tiktok_live=None))
    base = sum(1 for o in Simulator(no_live).simulate_day(live.day)
               if o.storefront_id == "tiktok_shop_us" and o.created_at.hour in hours)

    span = f"{min(hours):02d}-{max(hours) + 1:02d}h"
    print(f"\nkhung live {span}: {in_window / len(hours):.1f} Order/giờ"
          f"   ngày trước: {prev_window / len(hours):.1f} Order/giờ")
    print(f"chính ngày đó nếu tắt livestream: {base / len(hours):.1f} Order/giờ"
          f"   → tỷ lệ {in_window / base:.2f}   (đáp án {Simulator(config).live_multiplier:.2f})")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "configs/default.yaml")
