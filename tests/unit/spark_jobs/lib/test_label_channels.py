"""Bảng nhãn năm Channel: kênh nối ra 0 dòng phải bị chặn, không lọt im lặng."""

from spark_jobs.lib.label_channels import CHANNELS, missing_channels


def test_every_channel_with_labels_passes():
    """Đủ năm kênh có dòng nhãn thì không thiếu kênh nào."""
    assert missing_channels({channel: 1 for channel in CHANNELS}) == []


def test_empty_or_absent_channel_is_reported():
    """Khóa nối sai ⇒ kênh có 0 dòng hoặc vắng hẳn trong groupBy; cả hai đều bị báo."""
    counts = {channel: 5 for channel in CHANNELS} | {"shopee": 0}
    del counts["amazon"]
    assert missing_channels(counts) == ["amazon", "shopee"]
    assert sorted(CHANNELS) == ["amazon", "shopee", "shopify", "tiktok_shop", "walmart"]
