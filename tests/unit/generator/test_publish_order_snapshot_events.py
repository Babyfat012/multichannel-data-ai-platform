"""Test phần Stream: thứ tự đến, bản trùng, trễ, burst, nhịp phát."""

from __future__ import annotations

from datetime import timedelta

import pytest

from generator.config import parse_config
from generator.delivery.stream_events import TOPICS, stream_messages, wall_offset
from generator.storage import OutputStore
from generator.stream import FileSink, run_stream


@pytest.fixture
def stream_config(tmp_path):
    """12 ngày lịch sử, 6 ngày Stream, livestream ngày 15, trùng và trễ phóng to để đo được ở quy mô nhỏ."""
    return parse_config({
        "seed": 7,
        "scale": {"n_skus": 20, "days_history": 12, "days_stream": 6, "orders_per_day": 400},
        "offline_problems": {"amazon_orders_api_switch_day": 6},
        "streaming_problems": {
            "pacing": {"default_seconds_per_sim_day": 60,
                       "segments": [{"from_day": 15, "to_day": 15, "seconds_per_sim_day": 600}]},
            "duplicate_rate": {"tiktok_shop_us": 0.1},
            "amazon_late": {"median_minutes": 5, "share_over_1h": 0.2},
        },
        "drift": {"tiktok_live": {"day": 15, "start_hour_utc": 12, "duration_hours": 6,
                                  "burst_multiplier": [6, 6]}},
        "storage": {"output_root": str(tmp_path / "out")},
    })


def test_messages_arrive_in_order_and_inside_stream(stream_config):
    """Chứng minh message đến đúng thứ tự thời gian, event nằm trong khoảng cutoff→stream_end, không đến trước khi xảy ra, và phủ đủ mọi topic."""
    msgs = list(stream_messages(stream_config))
    assert [m.arrive_at for m in msgs] == sorted(m.arrive_at for m in msgs)
    assert all(stream_config.cutoff <= m.event_time < stream_config.stream_end for m in msgs)
    assert all(m.arrive_at >= m.event_time for m in msgs)
    assert {m.topic for m in msgs} == set(TOPICS.values())


def test_tiktok_duplicates_keep_notification_id(stream_config):
    """Chứng minh bản trùng TikTok giữ nguyên payload và đến muộn hơn bản gốc, và tỷ lệ trùng khớp cấu hình."""
    tiktok = [m for m in stream_messages(stream_config) if m.topic == TOPICS["tiktok_shop"]]
    dups = [m for m in tiktok if m.is_duplicate]
    assert dups
    originals = {m.dedup_key: m for m in tiktok if not m.is_duplicate}
    for d in dups:
        assert d.value == originals[d.dedup_key].value
        assert d.arrive_at > originals[d.dedup_key].arrive_at
    rate = 1 - len({m.dedup_key for m in tiktok}) / len(tiktok)
    assert rate == pytest.approx(0.1, abs=0.03)


def test_amazon_late_share_and_out_of_order(stream_config):
    """Chứng minh Amazon không có bản trùng, tỷ lệ trễ quá 1 giờ khớp cấu hình, có message lệch thứ tự, và message mang `PublishTime`."""
    amazon = [m for m in stream_messages(stream_config) if m.topic == TOPICS["amazon"]]
    assert not any(m.is_duplicate for m in amazon)
    late = [m for m in amazon if m.arrive_at - m.event_time > timedelta(hours=1)]
    assert len(late) / len(amazon) == pytest.approx(0.2, abs=0.04)
    # Message trễ đến sau message có event_time muộn hơn.
    assert any(a.event_time > b.event_time for a, b in zip(amazon, amazon[1:]))
    assert amazon[0].value["NotificationMetadata"]["PublishTime"]


def test_burst_matches_live_multiplier(stream_config, tmp_path):
    """Chứng minh buổi livestream đẩy số Order TikTok mới lên đúng hệ số cấu hình, và sink ghi ra đúng dạng envelope."""
    manifest = run_stream(stream_config, FileSink(OutputStore(stream_config.storage.output_root)), speed=0)
    burst = manifest["burst"]
    assert burst["multiplier_truth"] == 6
    assert 4 < burst["measured_ratio"] < 8
    rows = list(OutputStore(stream_config.storage.output_root).read_jsonl(
        f"stream/{TOPICS['tiktok_shop']}/part-00000.jsonl"))
    assert rows and rows[0]["value"]["type"] == 1


def test_same_seed_same_stream(stream_config):
    """Chứng minh cùng seed sinh ra đúng cùng một luồng message (khóa và thời điểm đến)."""
    first = [(m.dedup_key, m.arrive_at) for m in stream_messages(stream_config)]
    assert first == [(m.dedup_key, m.arrive_at) for m in stream_messages(stream_config)]


def test_pacing_slows_down_live_day(stream_config):
    """Chứng minh nhịp phát đúng: mốc đầu bằng 0, ngày thường 60 giây/ngày, ngày live chậm gấp 5 lần."""
    start = stream_config.cutoff
    assert wall_offset(stream_config, start) == 0
    assert wall_offset(stream_config, stream_config.day_start(14)) == 60  # ngày 13 bình thường
    live_start = stream_config.day_start(15)
    assert wall_offset(stream_config, live_start + timedelta(hours=12)) - wall_offset(stream_config, live_start) == 300


def test_live_day_must_be_in_stream():
    """Chứng minh cấu hình đặt ngày livestream ngoài phần Stream bị từ chối."""
    with pytest.raises(ValueError, match="tiktok_live.day"):
        parse_config({"scale": {"days_history": 12, "days_stream": 6},
                      "offline_problems": {"amazon_orders_api_switch_day": 6},
                      "drift": {"tiktok_live": {"day": 5}}})
