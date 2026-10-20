"""Test renderer Amazon: đúng dạng gốc, đúng schema theo ngày export."""

from __future__ import annotations



from generator.channels.amazon import AmazonOrdersRenderer
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
    # Không có field mới: cột chỉ có ở một dạng thì null ở dạng kia, vì tên khoá đã đổi
    # (`proceeds` là `OrderTotal` đổi tên + lồng một cấp, không phải field mới).
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

