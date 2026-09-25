"""Lõi mô phỏng: sinh các Order của một ngày cùng toàn bộ vòng đời của chúng.

Mỗi lần gọi `Simulator.simulate_day(day)` trả về các Order **được tạo** trong
ngày `day`. Mỗi Order đã có sẵn cả tương lai của nó: lúc thanh toán, lúc gửi,
lúc giao, có bị hủy không, dòng nào bị trả và trả lúc nào. Lõi không quan tâm
tới `cutoff`. Việc cắt "đến hôm nay thấy được gì" là việc của lớp export và
renderer, nhờ hàm `Order.status_at(t)`.

Vòng đời một Order (thời gian tính từ lúc tạo):

    pending_payment ──(vài phút)──► awaiting_shipment ──(0,3 đến 3 ngày)──► shipped
        ──(1 đến 6 ngày)──► delivered ──(3 đến 7 ngày)──► completed

Khoảng 3% Order bị hủy trong vòng 24 giờ sau khi thanh toán, trước khi gửi.
Khi đó vòng đời dừng ở `cancelled` và các Order Line không có nhãn.

Livestream TikTok (sau `cutoff`): trong vài giờ của buổi live, số Order của
TikTok Shop gấp `live_multiplier` lần bình thường. Phần Order thêm rút bằng bộ
sinh riêng, nên các Order còn lại của ngày đó giống hệt khi không có live.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from generator.config import GeneratorConfig
from generator.core.catalog import Catalog, build_catalog
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    FULFILLED_BY_CHANNEL,
    FULFILLED_BY_SELLER,
    PENDING_PAYMENT,
    SHIPPED,
    Buyer,
    Order,
    OrderLine,
    StatusChange,
)
from generator.core.returns import decide_return, return_probability
from generator.core.rngs import rng_for, stable_int
from generator.core.storefronts import CURRENCY_DECIMALS, POSTAL_PREFIX, STOREFRONTS, Storefront

CANCEL_RATE = 0.03
DISCOUNT_RATE = 0.25  # tỷ lệ Order Line có giảm giá
EXTRA_LINES_MEAN = 0.45  # số Order Line thêm trung bình, nên trung bình khoảng 1,45 dòng mỗi Order
MAX_LINES = 5
CITY_ZIPF_EXPONENT = 1.3  # vài thành phố lớn chiếm phần lớn Order (skew địa lý)
BUYERS_PER_ORDER = 0.5  # quy mô tập buyer so với số Order, nhỏ hơn 1 nên có buyer mua lại
GUEST_SHARE = {"shopify": 0.20, "walmart_us": 0.10}  # khách không đăng nhập
DISCOUNT_RANGE = (0.05, 0.30)  # mức giảm giá ngày thường
SALE_DISCOUNT_RANGE = (0.20, 0.40)  # mức giảm giá ngày Sale Event


@dataclass(frozen=True)
class BuyerPool:
    """Tập buyer cố định của một Storefront, sinh một lần lúc khởi tạo.

    Buyer thứ i luôn ở cùng thành phố và mã bưu chính, nên mỗi lần buyer đó mua lại
    thì địa chỉ không đổi. Lưu dạng mảng numpy để gọn bộ nhớ.
    """

    storefront: Storefront
    city_index: np.ndarray
    postal_suffix: np.ndarray
    is_guest: np.ndarray

    def buyer(self, i: int) -> Buyer:
        """Tạo đối tượng `Buyer` cho buyer thứ `i` của Storefront."""
        city = self.storefront.cities[int(self.city_index[i])]
        return Buyer(
            buyer_id=f"{self.storefront.storefront_id}-B{i:07d}",
            country=self.storefront.country,
            city=city,
            postal_code=postal_code(self.storefront.country, city, int(self.postal_suffix[i])),
            is_guest=bool(self.is_guest[i]),
        )


# Chữ cái được dùng ở inward code của Anh (bỏ C, I, K, M, O, V vì dễ nhầm).
_UK_INWARD_LETTERS = "ABDEFGHJLNPQRSTUWXYZ"


def postal_code(country: str, city: str, suffix: int) -> str:
    """Tạo mã bưu chính đúng định dạng của `country`, thuộc vùng của `city`.

    `suffix` (0 đến 999) phân biệt các địa chỉ trong cùng thành phố. Ví dụ với suffix 248:
    New York `10048`, Berlin `10248`, London `SW9 2WT`, Hanoi `10248`.
    """
    prefix = POSTAL_PREFIX[city]
    if country == "US":
        return f"{prefix}{suffix % 100:02d}"
    if country == "GB":
        district = suffix % 20 + 1
        letters = _UK_INWARD_LETTERS
        h = stable_int("postal", city, suffix)
        return f"{prefix}{district} {suffix // 100}{letters[h % 20]}{letters[h // 20 % 20]}"
    # Đức và Việt Nam: 2 số đầu theo vùng, 3 số sau.
    return f"{prefix}{suffix:03d}"


class Simulator:
    """Giữ các thứ cố định của thế giới (SKU, buyer) và sinh Order theo ngày."""

    def __init__(self, config: GeneratorConfig) -> None:
        """Dựng danh mục SKU và tập buyer từ cấu hình."""
        self.config = config
        self.catalog: Catalog = build_catalog(config)
        self.storefronts = [STOREFRONTS[s] for s in config.storefront_weights]
        self.storefront_weights = np.array(list(config.storefront_weights.values()))
        self.buyer_pools = {s.storefront_id: self._build_buyer_pool(s, w)
                            for s, w in zip(self.storefronts, self.storefront_weights)}
        # Sale Event chỉ diễn ra ở các Storefront Amazon.
        self.is_amazon = np.array([s.channel == "amazon" for s in self.storefronts])
        # Hệ số burst của livestream, rút một lần cho cả lần chạy.
        live = config.drift.tiktok_live
        self.live_multiplier = (float(rng_for(config.seed, "tiktok_live").uniform(*live.burst_multiplier))
                                if live is not None else 1.0)

    def is_sale_day(self, day: int) -> bool:
        """Ngày mô phỏng `day` có thuộc một Sale Event (Prime Day) không."""
        sale = self.config.drift.prime_day
        return sale is not None and sale.covers(day)

    def _build_buyer_pool(self, storefront: Storefront, weight: float) -> BuyerPool:
        """Sinh tập buyer của một Storefront, quy mô tỷ lệ với lượng Order dự kiến."""
        rng = rng_for(self.config.seed, "buyers", storefront.storefront_id)
        expected_orders = self.config.scale.orders_per_day * self.config.scale.days_history * weight
        size = max(10, int(expected_orders * BUYERS_PER_ORDER))

        n_cities = len(storefront.cities)
        city_weights = np.arange(1, n_cities + 1, dtype=float) ** (-CITY_ZIPF_EXPONENT)
        city_weights /= city_weights.sum()

        return BuyerPool(
            storefront=storefront,
            city_index=rng.choice(n_cities, size=size, p=city_weights),
            postal_suffix=rng.integers(0, 1000, size=size),
            is_guest=rng.random(size) < GUEST_SHARE.get(storefront.storefront_id, 0.0),
        )

    def simulate_day(self, day: int) -> list[Order]:
        """Sinh mọi Order được tạo trong ngày mô phỏng `day`.

        Các lựa chọn "lớn" (bao nhiêu Order, Storefront nào, SKU nào) rút một lần
        cho cả ngày bằng numpy cho nhanh. Chi tiết từng Order rút trong vòng lặp.
        """
        rng = rng_for(self.config.seed, "orders", day)
        day_start = self.config.day_start(day)
        sale_day = self.is_sale_day(day)

        # Ngày sale: Order của Storefront Amazon nhân lên, nên tổng Order trong ngày
        # tăng theo phần Amazon chiếm, còn tỷ trọng giữa các Storefront lệch hẳn về Amazon.
        weights = self.storefront_weights
        expected = self.config.scale.orders_per_day
        if sale_day:
            weights = weights * np.where(self.is_amazon, self.config.drift.prime_day.order_multiplier, 1.0)
            expected *= weights.sum()
            weights = weights / weights.sum()

        n_orders = int(rng.poisson(expected))
        storefront_idx = rng.choice(len(self.storefronts), size=n_orders, p=weights)
        n_lines = 1 + np.minimum(rng.poisson(EXTRA_LINES_MEAN, size=n_orders), MAX_LINES - 1)
        sku_idx = rng.choice(len(self.catalog.skus), size=int(n_lines.sum()), p=self.catalog.popularity)
        if sale_day:
            # Dòng của Storefront Amazon rút lại theo phân phối SKU của ngày sale.
            on_sale = self.is_amazon[np.repeat(storefront_idx, n_lines)]
            sku_idx[on_sale] = rng.choice(len(self.catalog.skus), size=int(on_sale.sum()),
                                          p=self.catalog.sale_popularity)
        created_offsets = np.sort(rng.uniform(0, 86_400, size=n_orders))

        orders: list[Order] = []
        next_sku = 0
        for i in range(n_orders):
            storefront = self.storefronts[storefront_idx[i]]
            created_at = day_start + timedelta(seconds=float(created_offsets[i]))
            skus_of_order = sku_idx[next_sku: next_sku + n_lines[i]]
            next_sku += n_lines[i]
            orders.append(self._make_order(f"ORD-{day:03d}-{i + 1:05d}", storefront,
                                           created_at, skus_of_order, rng, sale_day))
        live = self.config.drift.tiktok_live
        if live is not None and live.day == day:
            orders += self._live_orders(day, first_number=n_orders + 1)
            orders.sort(key=lambda o: o.created_at)
        return orders

    def _live_orders(self, day: int, first_number: int) -> list[Order]:
        """Order thêm của TikTok Shop trong buổi livestream của ngày `day`.

        Tốc độ thường của TikTok Shop là `orders_per_day * tỷ trọng / 24` mỗi giờ.
        Trong buổi live, thêm `(live_multiplier - 1)` lần tốc độ đó, nên tổng tốc độ
        gấp `live_multiplier` lần.
        """
        live = self.config.drift.tiktok_live
        idx = next((i for i, s in enumerate(self.storefronts) if s.storefront_id == "tiktok_shop_us"), None)
        if idx is None:
            return []
        storefront = self.storefronts[idx]
        rng = rng_for(self.config.seed, "tiktok_live_orders", day)
        per_hour = self.config.scale.orders_per_day * self.storefront_weights[idx] / 24
        n_orders = int(rng.poisson(per_hour * (self.live_multiplier - 1) * live.duration_hours))
        start, _ = live.window(self.config.day_start(day))
        offsets = np.sort(rng.uniform(0, live.duration_hours * 3600, size=n_orders))

        orders = []
        for i in range(n_orders):
            n_lines = 1 + min(int(rng.poisson(EXTRA_LINES_MEAN)), MAX_LINES - 1)
            skus = rng.choice(len(self.catalog.skus), size=n_lines, p=self.catalog.popularity)
            created_at = start + timedelta(seconds=float(offsets[i]))
            orders.append(self._make_order(f"ORD-{day:03d}-{first_number + i:05d}", storefront,
                                           created_at, skus, rng))
        return orders

    def _make_order(
        self,
        order_id: str,
        storefront: Storefront,
        created_at: datetime,
        sku_indices: np.ndarray,
        rng: np.random.Generator,
        sale_day: bool = False,
    ) -> Order:
        """Tạo một Order với vòng đời đầy đủ, rồi quyết định Return cho từng dòng."""
        pool = self.buyer_pools[storefront.storefront_id]
        buyer = pool.buyer(int(rng.integers(len(pool.city_index))))

        by_channel = rng.random() < storefront.channel_fulfilled_share
        fulfillment = FULFILLED_BY_CHANNEL if by_channel else FULFILLED_BY_SELLER
        is_prime = storefront.channel == "amazon" and by_channel and rng.random() < 0.6

        # Ngày giao dự kiến Channel hứa lúc đặt hàng.
        promise_days = (1, 3) if is_prime else (3, 5) if by_channel else (4, 8)
        promised = (created_at + timedelta(days=promise_days[0]), created_at + timedelta(days=promise_days[1]))

        # Vòng đời thật.
        paid_at = created_at + timedelta(minutes=float(rng.exponential(10)))
        timeline = [StatusChange(created_at, PENDING_PAYMENT), StatusChange(paid_at, AWAITING_SHIPMENT)]
        delivered_at = None
        if rng.random() < CANCEL_RATE:
            timeline.append(StatusChange(paid_at + timedelta(hours=float(rng.uniform(0, 24))), CANCELLED))
        else:
            handling = rng.uniform(0.3, 1.5) if by_channel else rng.uniform(0.5, 3.0)
            transit = rng.uniform(1.0, 3.0) if by_channel else rng.uniform(2.0, 6.0)
            shipped_at = paid_at + timedelta(days=float(handling))
            delivered_at = shipped_at + timedelta(days=float(transit))
            completed_at = delivered_at + timedelta(days=float(rng.uniform(3, 7)))
            timeline += [StatusChange(shipped_at, SHIPPED), StatusChange(delivered_at, DELIVERED),
                         StatusChange(completed_at, COMPLETED)]

        order = Order(
            order_id=order_id,
            storefront_id=storefront.storefront_id,
            currency=storefront.currency,
            buyer=buyer,
            created_at=created_at,
            fulfillment=fulfillment,
            is_prime=is_prime,
            promised_delivery=promised,
            delivered_at=delivered_at,
            timeline=timeline,
        )
        for number, idx in enumerate(sku_indices, start=1):
            order.lines.append(self._make_line(order, number, int(idx), storefront, rng, sale_day))
        return order

    def _make_line(
        self,
        order: Order,
        line_number: int,
        sku_index: int,
        storefront: Storefront,
        rng: np.random.Generator,
        sale_day: bool = False,
    ) -> OrderLine:
        """Tạo một Order Line: giá, giảm giá, số lượng, và kết quả Return.

        Trong ngày Sale Event, dòng của Storefront Amazon được giảm giá nhiều hơn
        và sâu hơn. Vì mức giảm giá nằm trong hàm tính xác suất trả, tỷ lệ trả
        của những ngày này cũng nhích lên. Đó chính là covariate drift.
        """
        sku = self.catalog.skus[sku_index]
        decimals = CURRENCY_DECIMALS[storefront.currency]
        quantity = 1 + int(min(rng.poisson(0.2), 3))
        unit_price = round(sku.base_price_usd * storefront.price_factor, decimals)
        on_sale = sale_day and storefront.channel == "amazon"
        share = self.config.drift.prime_day.discount_share if on_sale else DISCOUNT_RATE
        low, high = SALE_DISCOUNT_RANGE if on_sale else DISCOUNT_RANGE
        discount_fraction = float(rng.uniform(low, high)) if rng.random() < share else 0.0

        line = OrderLine(
            order_line_id=f"{order.order_id}-{line_number}",
            line_number=line_number,
            sku_id=sku.sku_id,
            quantity=quantity,
            unit_price=unit_price,
            unit_discount=round(unit_price * discount_fraction, decimals),
        )
        if order.delivered_at is not None:
            p = return_probability(sku, storefront, order.fulfillment, discount_fraction, quantity)
            decide_return(line, p, order.delivered_at, storefront, self.config.labels.window_days, rng)
        return line
