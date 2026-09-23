"""Hằng số mô tả 7 Storefront và 8 danh mục sản phẩm của Seller ACME.

Đây là "địa lý" cố định của thế giới giả lập. Các con số hay đổi
(tỷ trọng Order theo Storefront) nằm trong cấu hình, còn những thứ gắn
với bản chất Storefront (tiền tệ, thành phố, cách giao hàng) nằm ở đây.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Storefront:
    """Một chỗ Seller bán hàng: Channel cộng Marketplace nếu Channel có.

    Thuộc tính:
        storefront_id: mã chuẩn, dùng làm khóa feature (`sku_id` + `storefront_id`).
        channel: tên Channel, quyết định renderer nào sẽ xuất dữ liệu.
        country: mã quốc gia ISO của buyer.
        currency: tiền tệ gốc của giá bán.
        price_factor: giá bán = giá gốc USD của SKU × hệ số này (đã gồm quy đổi tiền).
        channel_fulfilled_share: tỷ lệ Order Line do Channel giao (FBA, WFS, ...).
        return_lag_mean_days: số ngày trung bình từ lúc giao tới lúc buyer yêu cầu trả.
        return_logit_shift: cộng vào logit xác suất trả, thể hiện khác biệt giữa Storefront.
        cities: các thành phố lớn, thành phố đầu chiếm nhiều Order nhất (skew địa lý).
    """

    storefront_id: str
    channel: str
    country: str
    currency: str
    price_factor: float
    channel_fulfilled_share: float
    return_lag_mean_days: float
    return_logit_shift: float
    cities: tuple[str, ...]


STOREFRONTS: dict[str, Storefront] = {
    s.storefront_id: s
    for s in [
        Storefront("amazon_us", "amazon", "US", "USD", 1.00, 0.70, 8.0, 0.00,
                   ("New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Miami", "Denver")),
        Storefront("amazon_uk", "amazon", "GB", "GBP", 0.80, 0.70, 8.0, -0.10,
                   ("London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol")),
        Storefront("amazon_de", "amazon", "DE", "EUR", 0.95, 0.70, 9.0, 0.15,
                   ("Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Stuttgart")),
        Storefront("tiktok_shop_us", "tiktok_shop", "US", "USD", 0.90, 0.40, 6.0, 0.20,
                   ("Los Angeles", "New York", "Houston", "Atlanta", "Dallas", "Chicago")),
        Storefront("shopify", "shopify", "US", "USD", 1.05, 0.00, 10.0, -0.20,
                   ("New York", "San Francisco", "Boston", "Austin", "Seattle", "Chicago")),
        Storefront("walmart_us", "walmart", "US", "USD", 0.95, 0.30, 9.0, 0.00,
                   ("Dallas", "Houston", "Atlanta", "Chicago", "Phoenix", "Orlando")),
        Storefront("shopee_vn", "shopee", "VN", "VND", 17_500.0, 0.00, 5.0, -0.15,
                   ("Ho Chi Minh City", "Hanoi", "Da Nang", "Hai Phong", "Can Tho")),
    ]
}

# Phần đầu mã bưu chính của từng thành phố, theo hệ thống bưu chính thật của nước đó.
# Mỹ: 3 số đầu ZIP. Đức: 2 số đầu PLZ. Anh: vùng (postcode area) của outward code.
# Việt Nam: 2 số đầu mã bưu chính quốc gia 5 số (áp dụng từ 2018).
# Tên thành phố không trùng giữa các nước nên dùng làm khóa được.
POSTAL_PREFIX: dict[str, str] = {
    "New York": "100", "Los Angeles": "900", "Chicago": "606", "Houston": "770", "Phoenix": "850",
    "Seattle": "981", "Miami": "331", "Denver": "802", "Atlanta": "303", "Dallas": "752",
    "San Francisco": "941", "Boston": "021", "Austin": "787", "Orlando": "328",
    "London": "SW", "Manchester": "M", "Birmingham": "B", "Leeds": "LS", "Glasgow": "G", "Bristol": "BS",
    "Berlin": "10", "Hamburg": "20", "Munich": "80", "Cologne": "50", "Frankfurt": "60", "Stuttgart": "70",
    "Ho Chi Minh City": "70", "Hanoi": "10", "Da Nang": "50", "Hai Phong": "04", "Can Tho": "94",
}

# Số chữ số thập phân của từng tiền tệ (VND không có phần lẻ).
CURRENCY_DECIMALS: dict[str, int] = {"USD": 2, "GBP": 2, "EUR": 2, "VND": 0}


@dataclass(frozen=True)
class Category:
    """Một danh mục sản phẩm.

    Thuộc tính:
        code: tiền tố của `sku_id`, ví dụ `APP` trong `APP-00042`.
        sku_share: tỷ lệ SKU thuộc danh mục này trong danh mục SKU.
        return_rate: tỷ lệ trả mục tiêu, dùng làm mức nền của hàm logistic.
        price_range_usd: khoảng giá gốc (USD) của SKU trong danh mục.
    """

    name: str
    code: str
    sku_share: float
    return_rate: float
    price_range_usd: tuple[float, float]


CATEGORIES: tuple[Category, ...] = (
    Category("apparel", "APP", 0.25, 0.22, (15, 80)),
    Category("footwear", "FTW", 0.12, 0.15, (30, 150)),
    Category("accessories", "ACC", 0.12, 0.10, (8, 60)),
    Category("electronics", "ELE", 0.10, 0.08, (20, 300)),
    Category("sports", "SPT", 0.10, 0.08, (10, 120)),
    Category("toys", "TOY", 0.10, 0.06, (8, 70)),
    Category("beauty", "BEA", 0.10, 0.05, (5, 50)),
    Category("home", "HOM", 0.11, 0.04, (10, 200)),
)

# Tỷ trọng Order theo Storefront khi cấu hình không ghi (skew theo Storefront).
DEFAULT_STOREFRONT_WEIGHTS: dict[str, float] = {
    "amazon_us": 0.45,
    "tiktok_shop_us": 0.15,
    "amazon_uk": 0.12,
    "amazon_de": 0.10,
    "shopify": 0.08,
    "walmart_us": 0.07,
    "shopee_vn": 0.03,
}
