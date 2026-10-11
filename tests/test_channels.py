"""Test renderer Amazon và Walmart: đúng dạng gốc, đúng schema theo ngày export."""

from __future__ import annotations

import re

import pytest

from generator.channels.amazon import AmazonOrdersRenderer
from generator.channels.walmart import WalmartOrdersRenderer
from generator.core.model import FULFILLED_BY_CHANNEL
from generator.core.simulate import Simulator
from generator.delivery.batch_exports import end_of_day


def _orders_of(config, storefront_prefix, days=range(1, 4)):
    """Các Order của những Storefront có mã bắt đầu bằng `storefront_prefix`."""
    sim = Simulator(config)
    return [o for d in days for o in sim.simulate_day(d) if o.storefront_id.startswith(storefront_prefix)]


def test_amazon_schema_switches_on_configured_day(tiny_config):
    """Chứng minh đúng ngày chuyển schema: cùng một đơn ra v0 trước ngày đó và 2026 từ ngày đó, với đổi tên, đổi cấu trúc và khoá chỉ có ở một bản."""
    renderer = AmazonOrdersRenderer(tiny_config)
    order = _orders_of(tiny_config, "amazon")[0]
    as_of = end_of_day(tiny_config, 8)
    switch = tiny_config.offline_problems.amazon_orders_api_switch_day

    old = renderer.render(order, as_of, export_day=switch - 1).record
    new = renderer.render(order, as_of, export_day=switch).record

    # Đổi tên: cùng một đơn, khóa khác tên.
    assert old["AmazonOrderId"] == new["orderId"]
    # Field mới chỉ có ở bản mới, field cũ chỉ có ở bản cũ.
    assert "proceeds" in new and "proceeds" not in old
    assert "IsPrime" in old and "IsPrime" not in new
    # Đổi cấu trúc và cách viết giá trị.
    assert new["fulfillment"]["status"] == old["OrderStatus"].upper()


def test_amazon_ids_are_stable_and_well_formed(tiny_config):
    """Chứng minh mã đơn Amazon ổn định qua hai lần dựng renderer và đúng định dạng 3-7-7."""
    renderer = AmazonOrdersRenderer(tiny_config)
    order = _orders_of(tiny_config, "amazon")[0]
    amazon_id = renderer.amazon_order_id(order)
    assert amazon_id == AmazonOrdersRenderer(tiny_config).amazon_order_id(order)
    assert [len(part) for part in amazon_id.split("-")] == [3, 7, 7]


def test_amazon_never_shows_delivered_and_only_mfn_has_delivery_window(tiny_config):
    """Chứng minh Amazon không bao giờ hiện trạng thái 'đã giao', và chỉ đơn Seller tự giao (MFN) có cửa sổ giao dự kiến."""
    renderer = AmazonOrdersRenderer(tiny_config)
    for order in _orders_of(tiny_config, "amazon"):
        record = renderer.render(order, end_of_day(tiny_config, 12), export_day=1).record
        assert record["OrderStatus"] in {"Pending", "Unshipped", "Shipped", "Canceled"}
        has_window = "EarliestDeliveryDate" in record
        assert has_window == (order.fulfillment != FULFILLED_BY_CHANNEL)


def test_walmart_row_has_payload_and_fetched_at(tiny_config):
    """Chứng minh bản ghi Walmart chỉ có 3 cột, khóa bằng `purchaseOrderId`, và payload mang trạng thái Delivered kèm tracking."""
    renderer = WalmartOrdersRenderer(tiny_config)
    delivered = [o for o in _orders_of(tiny_config, "walmart", days=range(1, 6)) if o.delivered_at]
    order = delivered[0]
    as_of = end_of_day(tiny_config, 12)

    rendered = renderer.render(order, as_of, export_day=12)
    row = rendered.record
    assert set(row) == {"purchase_order_id", "raw_payload", "fetched_at"}
    assert rendered.key == row["purchase_order_id"] == row["raw_payload"]["purchaseOrderId"]
    line_status = row["raw_payload"]["orderLines"]["orderLine"][0]["orderLineStatuses"]["orderLineStatus"][0]
    assert line_status["status"] == "Delivered"
    assert "trackingNumber" in line_status["trackingInfo"]


@pytest.mark.parametrize("carrier, pattern", [
    ("UPS", r"1Z[A-Z0-9]{16}"),
    ("FedEx", r"\d{12}"),
    ("USPS", r"94\d{20}"),
])
def test_walmart_tracking_number_matches_carrier(tiny_config, carrier, pattern):
    """Chứng minh mã vận đơn Walmart khớp định dạng của từng hãng vận chuyển."""
    renderer = WalmartOrdersRenderer(tiny_config)
    assert re.fullmatch(pattern, renderer.tracking_number(carrier, "1445238308495"))
