"""Test renderer webhook TikTok Shop type 1: đúng envelope, mã cố định, giữ `CANCEL`."""

from __future__ import annotations

from generator.channels.tiktok import TikTokOrderWebhookRenderer
from generator.core.model import CANCELLED
from generator.core.simulate import Simulator


def _tiktok_orders(config, days=range(1, 6)):
    """Generate TikTok orders for the requested days in the tiny test config."""
    sim = Simulator(config)
    return [o for d in days for o in sim.simulate_day(d) if o.storefront_id == "tiktok_shop_us"]


def test_webhook_envelope_matches_docs(tiny_config):
    """Chứng minh envelope webhook type 1 đúng tài liệu: đủ khoá cấp ngoài, đủ khoá `data`, và `timestamp` bằng `update_time`."""
    renderer = TikTokOrderWebhookRenderer(tiny_config)
    order = _tiktok_orders(tiny_config)[0]
    msg = renderer.render_event(order, order.timeline[-1]).record

    assert set(msg) == {"type", "tts_notification_id", "shop_id", "timestamp", "data"}
    assert msg["type"] == 1
    assert set(msg["data"]) == {"order_id", "order_status", "is_on_hold_order", "update_time"}
    assert len(msg["data"]["order_id"]) == 18 and msg["data"]["order_id"].startswith("57")
    assert msg["timestamp"] == msg["data"]["update_time"]


def test_one_notification_id_per_status_change(tiny_config):
    """Chứng minh mỗi lần đổi trạng thái có mã riêng, và vẽ lại cho ra đúng mã cũ (để dedup được)."""
    renderer = TikTokOrderWebhookRenderer(tiny_config)
    order = _tiktok_orders(tiny_config)[0]
    ids = [renderer.render_event(order, c).key for c in order.timeline]
    # Mỗi lần đổi trạng thái có mã riêng, vẽ lại thì ra đúng mã cũ (để dedup được).
    assert len(set(ids)) == len(ids)
    assert ids == [
        TikTokOrderWebhookRenderer(tiny_config).render_event(order, c).key for c in order.timeline
    ]


def test_cancel_uses_webhook_spelling(tiny_config):
    """Chứng minh trạng thái hủy dùng đúng cách viết của webhook (`CANCEL`, không phải `CANCELLED` như API)."""
    renderer = TikTokOrderWebhookRenderer(tiny_config)
    statuses = {
        renderer.render_event(o, c).record["data"]["order_status"]
        for o in _tiktok_orders(tiny_config, days=range(1, 15))
        for c in o.timeline
        if c.status == CANCELLED
    }
    assert statuses <= {"CANCEL"}
