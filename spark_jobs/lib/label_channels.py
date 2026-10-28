"""Kênh của bảng nhãn trả hàng và luật chặn kênh rỗng, tách khỏi pyspark để test được."""

from __future__ import annotations

# Walmart nối bằng bridge riêng (cần customer_order_id); bốn kênh còn lại nối qua sổ khóa
# ground-truth/order_line_keys/<kênh>/ theo channel_order_id + channel_line_id.
KEY_CHANNELS = ("amazon", "tiktok_shop", "shopify", "shopee")
CHANNELS = (*KEY_CHANNELS, "walmart")


def missing_channels(counts: dict[str, int]) -> list[str]:
    """Kênh không có dòng nhãn nào: khóa nối sai thì join ra 0 dòng mà không báo lỗi."""
    return [channel for channel in CHANNELS if counts.get(channel, 0) <= 0]

