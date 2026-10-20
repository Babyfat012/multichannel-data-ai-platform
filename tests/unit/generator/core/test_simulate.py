"""Test lõi mô phỏng: tái lập, vòng đời Order và nhãn."""

from __future__ import annotations

import collections
import dataclasses
import re
from datetime import timedelta

import pytest

from generator.config import DriftConfig, PrimeDayConfig
from generator.core.catalog import build_catalog, zipf_popularity
from generator.core.model import CANCELLED, COMPLETED, PENDING_PAYMENT, SHIPPED
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator, postal_code


def _fingerprint(orders):
    """Tóm tắt các Order thành tuple để so sánh hai lần sinh."""
    return [
        (
            o.order_id,
            o.storefront_id,
            o.created_at,
            [(line.sku_id, line.quantity, line.label) for line in o.lines],
        )
        for o in orders
    ]


def test_same_seed_gives_same_orders(tiny_config):
    """Chứng minh cùng seed sinh ra đúng cùng một tập Order (tái lập được)."""
    assert _fingerprint(Simulator(tiny_config).simulate_day(3)) == _fingerprint(
        Simulator(tiny_config).simulate_day(3)
    )


def test_a_day_does_not_depend_on_previous_days(tiny_config):
    """Sinh ngày 5 trực tiếp hay sau khi đã sinh ngày 1 đến 4 đều ra cùng kết quả."""
    warm = Simulator(tiny_config)
    for day in range(1, 5):
        # Phải duyệt hết: `simulate_day` là generator, không duyệt thì không sinh gì
        # và test trở thành rỗng ruột.
        assert list(warm.simulate_day(day))
    assert _fingerprint(warm.simulate_day(5)) == _fingerprint(
        Simulator(tiny_config).simulate_day(5)
    )


def test_orders_are_created_inside_their_day(tiny_config):
    """Chứng minh mọi Order của ngày d có `created_at` nằm trong ngày d, không rơi sang ngày khác."""
    for order in Simulator(tiny_config).simulate_day(4):
        assert tiny_config.day_start(4) <= order.created_at < tiny_config.day_start(5)


def test_timeline_is_ordered_and_ends_in_completed_or_cancelled(tiny_config):
    """Chứng minh timeline tăng dần theo thời gian, mở đầu bằng `PENDING_PAYMENT`, kết thúc ở `COMPLETED` hoặc `CANCELLED`."""
    for order in Simulator(tiny_config).simulate_day(2):
        times = [c.at for c in order.timeline]
        assert times == sorted(times)
        assert order.timeline[0].status == PENDING_PAYMENT
        assert order.timeline[-1].status in (COMPLETED, CANCELLED)


def test_status_at_returns_latest_change_before_time(tiny_config):
    """Chứng minh `status_at` trả về thay đổi mới nhất TRƯỚC mốc thời gian, và báo lỗi khi hỏi trước lúc tạo Order."""
    order = next(o for o in Simulator(tiny_config).simulate_day(1) if o.shipped_at is not None)
    assert order.status_at(order.shipped_at).status == SHIPPED
    assert order.status_at(order.shipped_at - timedelta(seconds=1)).status != SHIPPED
    with pytest.raises(ValueError):
        order.status_at(order.created_at - timedelta(seconds=1))


def test_cancelled_orders_have_no_label_and_delivered_orders_do(tiny_config):
    """Chứng minh Order chưa giao không có nhãn, còn Order đã giao có nhãn 0 hoặc 1."""
    orders = [o for day in range(1, 4) for o in Simulator(tiny_config).simulate_day(day)]
    for order in orders:
        for line in order.lines:
            if order.delivered_at is None:
                assert line.label is None and line.return_requested_at is None
            else:
                assert line.label in (0, 1)


def test_label_is_one_only_when_return_is_within_window(tiny_config):
    """Chứng minh nhãn bằng 1 đúng khi yêu cầu trả rơi trong cửa sổ 30 ngày kể từ ngày giao."""
    window = timedelta(days=tiny_config.labels.window_days)
    orders = [o for day in range(1, 6) for o in Simulator(tiny_config).simulate_day(day)]
    for order in orders:
        for line in order.lines:
            if line.return_requested_at is None:
                assert line.label in (0, None)
            else:
                within = line.return_requested_at - order.delivered_at <= window
                assert line.label == int(within)


def test_zipf_top_share_matches_design_target():
    """Với 2.000 SKU và số mũ 0,8, SKU bán chạy nhất chiếm khoảng 5%."""
    weights = zipf_popularity(2000, 0.8, rng_for(1, "test"))
    assert weights.sum() == pytest.approx(1.0)
    assert 0.045 < weights.max() < 0.06


def test_catalog_has_requested_size_and_unique_ids(tiny_config):
    """Chứng minh catalog có đúng số SKU theo cấu hình và mã SKU không trùng."""
    catalog = build_catalog(tiny_config)
    assert len(catalog.skus) == tiny_config.scale.n_skus
    assert len(catalog.skus_by_id) == tiny_config.scale.n_skus


@pytest.mark.parametrize(
    "country, city, pattern",
    [
        ("US", "New York", r"100\d{2}"),
        ("DE", "Berlin", r"10\d{3}"),
        ("GB", "London", r"SW\d{1,2} \d[A-Z]{2}"),
        ("VN", "Hanoi", r"10\d{3}"),
    ],
)
def test_postal_code_follows_country_format(country, city, pattern):
    """Chứng minh mã bưu chính sinh ra khớp định dạng của từng quốc gia."""
    for suffix in (0, 248, 999):
        assert re.fullmatch(pattern, postal_code(country, city, suffix))


def _sale_config(tiny_config, **overrides):
    """Bản sao của cấu hình với một Sale Event ở ngày 5."""
    prime_day = PrimeDayConfig(day=5, days=2, order_multiplier=4, discount_share=0.7, deal_skus=3)
    return dataclasses.replace(
        tiny_config, drift=DriftConfig(prime_day=dataclasses.replace(prime_day, **overrides))
    )


def test_sale_day_multiplies_amazon_orders_only(tiny_config):
    """Chứng minh ngày sale chỉ nhân số Order Amazon, không đụng tới Channel khác."""
    sim = Simulator(_sale_config(tiny_config))
    sale = collections.Counter(o.storefront_id.split("_")[0] for o in sim.simulate_day(5))
    normal = collections.Counter(o.storefront_id.split("_")[0] for o in sim.simulate_day(8))
    assert sale["amazon"] > 3 * normal["amazon"]
    assert 0.5 * normal["walmart"] <= sale["walmart"] <= 2 * normal["walmart"] + 5


def test_sale_day_discounts_amazon_lines_deeper(tiny_config):
    """Chứng minh ngày sale làm tỷ lệ Order Line Amazon được giảm giá cao hơn hẳn ngày thường."""
    sim = Simulator(_sale_config(tiny_config))

    def discounted_share(day):
        """Tỷ lệ Order Line Amazon có giảm giá trong một ngày."""
        lines = [
            line
            for o in sim.simulate_day(day)
            if o.storefront_id.startswith("amazon")
            for line in o.lines
        ]
        return sum(line.unit_discount > 0 for line in lines) / len(lines)

    assert discounted_share(5) > 0.5
    assert discounted_share(8) < 0.4


def test_days_outside_the_sale_are_untouched(tiny_config):
    """Ngày thường phải ra đúng dữ liệu như khi không cấu hình Sale Event."""
    assert _fingerprint(Simulator(_sale_config(tiny_config)).simulate_day(8)) == _fingerprint(
        Simulator(tiny_config).simulate_day(8)
    )
