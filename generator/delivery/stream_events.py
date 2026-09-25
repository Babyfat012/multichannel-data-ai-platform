"""Lớp giao hàng Stream: biến các lần đổi trạng thái sau `cutoff` thành message.

Mỗi lần một Order Amazon hoặc TikTok Shop đổi Order Status trong khoảng
`[cutoff, stream_end)` sinh ra một message. Lớp này quyết định message **đến lúc
nào** (`arrive_at`) và có bị gửi trùng không. Burst không nằm ở đây: nó đến từ
lõi mô phỏng (livestream làm tăng số Order), lớp này chỉ giao lại.

| Vấn đề | Channel | Cách chèn |
|---|---|---|
| Late | Amazon | `share_over_1h` message trễ 1 đến 6 giờ, còn lại trễ theo phân phối log-normal có trung vị `median_minutes`, luôn dưới 1 giờ |
| Duplicate | TikTok Shop | Với xác suất `p = r / (1 - r)`, message được gửi lại sau 2 hoặc 30 phút (lịch thử lại của TikTok), giữ nguyên `tts_notification_id` |

Công thức `p`: nếu n message gốc, mỗi cái có thêm một bản với xác suất p, thì
tổng là n(1 + p) và tỷ lệ trùng `1 - n / (n(1 + p)) = p / (1 + p)`. Đặt bằng r
rồi giải ra `p = r / (1 - r)`.

Message được trả ra theo thứ tự `arrive_at`, tức thứ tự Kafka nhận được. Message
Amazon trễ vì thế đứng sau những message có `event_time` muộn hơn: đến lệch thứ tự.

Cách giữ RAM thấp: lõi sinh Order theo ngày tạo. Order tạo ngày d chỉ có lần đổi
trạng thái từ ngày d trở đi, và message luôn đến sau lần đổi trạng thái. Nên khi
đã sinh xong ngày d, mọi message đến trước 00:00 ngày d + 1 đã biết đủ và phát ra
được. Hai hàng đợi ưu tiên (heap) chỉ giữ phần chưa tới lượt.
"""

from __future__ import annotations

import heapq
import itertools
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from generator.channels.amazon import AmazonOrderNotificationRenderer
from generator.channels.tiktok import TikTokOrderWebhookRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order, StatusChange
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS

TOPICS = {"amazon": "amazon.order-notifications", "tiktok_shop": "tiktok_shop.order-webhooks"}

# Order sống lâu nhất khoảng 17 ngày (thanh toán, tối đa 3 ngày chuẩn bị, 6 ngày
# vận chuyển, 7 ngày tới hoàn tất). Sinh lại từng ấy ngày trước cutoff là đủ để
# bắt mọi Order còn đổi trạng thái sau cutoff.
LOOKBACK_DAYS = 20
LATE_SIGMA = 0.8  # độ trải của phần trễ ít (log-normal)
LATE_OVER_1H_MINUTES = (60, 360)
TIKTOK_DELAY_SECONDS = 5  # độ trễ trung bình của webhook bình thường
TIKTOK_RETRY_MINUTES = (2, 30)  # hai lần thử lại đầu tiên của TikTok Shop
TIKTOK_RETRY_WEIGHTS = (0.8, 0.2)


@dataclass(frozen=True)
class StreamMessage:
    """Một message sẽ gửi vào Kafka.

    Thuộc tính:
        topic: topic Kafka, theo Channel.
        key: khóa Kafka, là mã đơn gốc, để message của một Order vào cùng partition.
        dedup_key: mã thông báo, khóa để Flink bỏ bản trùng.
        value: nội dung JSON đúng dạng gốc của Channel.
        storefront_id, event_time, arrive_at: dùng để đo, không gửi đi.
        is_duplicate: True nếu là bản gửi lại.
    """

    topic: str
    key: str
    dedup_key: str
    value: dict[str, Any]
    storefront_id: str
    event_time: datetime
    arrive_at: datetime
    is_duplicate: bool = False


def stream_messages(config: GeneratorConfig) -> Iterator[StreamMessage]:
    """Sinh mọi message của Stream, theo thứ tự thời điểm đến."""
    sim = Simulator(config)
    renderers = {"amazon": AmazonOrderNotificationRenderer(config),
                 "tiktok_shop": TikTokOrderWebhookRenderer(config)}
    rng = rng_for(config.seed, "stream_delivery")
    seq = itertools.count()  # phá hòa trong heap, giữ thứ tự tái lập được

    events: list[tuple[datetime, int, Order, StatusChange]] = []
    messages: list[tuple[datetime, int, StreamMessage]] = []

    first_day = max(1, config.scale.days_history - LOOKBACK_DAYS + 1)
    last_day = config.scale.days_history + config.scale.days_stream
    for day in range(first_day, last_day + 1):
        for order in sim.simulate_day(day):
            if STOREFRONTS[order.storefront_id].channel not in renderers:
                continue
            for change in order.timeline:
                if config.cutoff <= change.at < config.stream_end:
                    heapq.heappush(events, (change.at, next(seq), order, change))

        known_until = config.day_start(day + 1)
        while events and events[0][0] < known_until:
            _, _, order, change = heapq.heappop(events)
            for msg in _deliver(config, renderers, rng, order, change):
                heapq.heappush(messages, (msg.arrive_at, next(seq), msg))
        while messages and messages[0][0] < known_until:
            yield heapq.heappop(messages)[2]

    while messages:
        yield heapq.heappop(messages)[2]


def _deliver(config: GeneratorConfig, renderers: dict[str, Any], rng: np.random.Generator,
             order: Order, change: StatusChange) -> list[StreamMessage]:
    """Vẽ một lần đổi trạng thái rồi quyết định lúc đến và bản trùng."""
    channel = STOREFRONTS[order.storefront_id].channel
    rendered = renderers[channel].render_event(order, change)
    if channel == "amazon":
        arrive = change.at + _amazon_delay(config, rng)
        value = rendered.record
        value["NotificationMetadata"]["PublishTime"] = arrive.strftime("%Y-%m-%dT%H:%M:%SZ")
        key = value["Payload"]["OrderChangeNotification"]["AmazonOrderId"]
    else:
        arrive = change.at + timedelta(seconds=float(rng.exponential(TIKTOK_DELAY_SECONDS)))
        value = rendered.record
        key = value["data"]["order_id"]

    original = StreamMessage(TOPICS[channel], key, rendered.key, value, order.storefront_id, change.at, arrive)
    out = [original]
    rate = config.streaming_problems.duplicate_rate.get(order.storefront_id, 0.0)
    if rate and rng.random() < rate / (1 - rate):
        retry = float(rng.choice(TIKTOK_RETRY_MINUTES, p=TIKTOK_RETRY_WEIGHTS))
        out.append(StreamMessage(original.topic, key, rendered.key, value, order.storefront_id,
                                 change.at, arrive + timedelta(minutes=retry), is_duplicate=True))
    return out


def _amazon_delay(config: GeneratorConfig, rng: np.random.Generator) -> timedelta:
    """Độ trễ của một notification Amazon."""
    late = config.streaming_problems.amazon_late
    if rng.random() < late.share_over_1h:
        return timedelta(minutes=float(rng.uniform(*LATE_OVER_1H_MINUTES)))
    minutes = late.median_minutes * float(np.exp(LATE_SIGMA * rng.standard_normal()))
    return timedelta(minutes=min(minutes, 59.9))


def wall_offset(config: GeneratorConfig, t: datetime) -> float:
    """Số giây thật tính từ lúc bắt đầu phát tới khi phát message đến lúc `t`.

    Mỗi ngày mô phỏng chiếm `seconds_per_day(day)` giây thật, nên ngày có
    livestream phát chậm hơn và burst kéo dài đủ lâu để thấy trên Flink UI.
    """
    pacing = config.streaming_problems.pacing
    elapsed = (t - config.cutoff).total_seconds() / 86_400
    first = config.scale.days_history + 1
    whole = int(elapsed)
    total = sum(pacing.seconds_per_day(first + i) for i in range(whole))
    return total + (elapsed - whole) * pacing.seconds_per_day(first + whole)
