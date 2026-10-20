"""Quy luật quyết định Order Line nào bị trả và trả sau bao lâu.

Xác suất trả là hàm logistic của một tổng các "tác động" (xem
generator-design.md, mục 4):

    logit(p) = logit(tỷ lệ trả của danh mục)     mức nền
             + defect_propensity của SKU            ẩn, chỉ suy ra từ lịch sử
             + độ lệch của Storefront
             + tác động của Fulfillment Method
             + tác động của mức giảm giá, giá, số lượng

Hàm logistic đổi tổng này về xác suất trong khoảng (0, 1). Dùng logit thay vì
cộng thẳng xác suất vì cộng nhiều tác động trên thang logit không bao giờ cho
ra xác suất âm hay lớn hơn 1.

Mỗi tác động được **tâm hóa**: trừ đi giá trị trung bình của nó, để tính trung
bình trên mọi Order Line thì tác động gần bằng 0. Nhờ vậy tỷ lệ trả của cả danh
mục vẫn xấp xỉ `return_rate` đã cấu hình, còn từng dòng thì lệch lên hoặc xuống.
Ví dụ giảm giá: khoảng 25% dòng được giảm trung bình 17,5%, tức mức giảm trung
bình khoảng 4,4%. Dòng không giảm giá bị trừ nhẹ, dòng giảm sâu được cộng nhiều.

Mô hình học được bài toán vì mọi tác động (trừ defect_propensity) đều nhìn thấy
trong dữ liệu lúc tạo Order, còn defect_propensity học được qua feature lịch sử
trả hàng theo SKU. Không tác động nào dùng thông tin chỉ có sau khi giao, nên
không có leakage.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta

import numpy as np

from generator.core.model import FULFILLED_BY_CHANNEL, OrderLine, Sku
from generator.core.storefronts import CATEGORIES, Storefront

# Hệ số của từng tác động trên thang logit.
CHANNEL_FULFILLED_SHIFT = 0.25  # hàng do Channel giao trả dễ hơn (nhãn trả trước, tới kho Channel)
DISCOUNT_COEF = 1.2  # giảm 30% làm logit tăng khoảng 0,31 so với mức trung bình
LOG_PRICE_COEF = 0.25  # SKU đắt hơn mức giữa danh mục thì dễ bị trả hơn
EXTRA_QUANTITY_COEF = 0.15  # mua nhiều cái cùng lúc (thử size) thì dễ trả hơn

# Giá trị trung bình dùng để tâm hóa (khớp với cách lõi sinh dữ liệu trong simulate.py).
MEAN_CHANNEL_FULFILLED = 0.6  # tỷ lệ dòng do Channel giao, tính theo tỷ trọng Storefront
MEAN_DISCOUNT_FRACTION = 0.044
MEAN_QUANTITY = 1.2

_CATEGORY_BY_NAME = {c.name: c for c in CATEGORIES}


def return_probability(
    sku: Sku,
    storefront: Storefront,
    fulfillment: str,
    discount_fraction: float,
    quantity: int,
) -> float:
    """Tính xác suất một Order Line bị trả, theo công thức ở đầu file."""
    category = _CATEGORY_BY_NAME[sku.category]
    low, high = category.price_range_usd
    mid_price = (low + high) / 2

    z = _logit(category.return_rate)
    z += sku.defect_propensity
    z += storefront.return_logit_shift
    by_channel = 1.0 if fulfillment == FULFILLED_BY_CHANNEL else 0.0
    z += CHANNEL_FULFILLED_SHIFT * (by_channel - MEAN_CHANNEL_FULFILLED)
    z += DISCOUNT_COEF * (discount_fraction - MEAN_DISCOUNT_FRACTION)
    z += LOG_PRICE_COEF * math.log(sku.base_price_usd / mid_price)
    z += EXTRA_QUANTITY_COEF * (quantity - MEAN_QUANTITY)
    return 1.0 / (1.0 + math.exp(-z))


def decide_return(
    line: OrderLine,
    probability: float,
    delivered_at: datetime,
    storefront: Storefront,
    window_days: int,
    rng: np.random.Generator,
) -> None:
    """Quyết định dòng `line` có bị trả không, rồi ghi kết quả và nhãn vào `line`.

    Độ trễ từ lúc giao tới lúc yêu cầu trả rút từ phân phối Gamma (hình dạng 2),
    trung bình bằng `return_lag_mean_days` của Storefront. Phân phối này có đuôi
    dài, nên thỉnh thoảng có Return tới sau 30 ngày: vẫn là Return thật, nhưng
    nhãn bằng 0 vì nằm ngoài cửa sổ.
    """
    if rng.random() >= probability:
        line.label = 0
        return

    lag_days = rng.gamma(shape=2.0, scale=storefront.return_lag_mean_days / 2.0)
    line.return_requested_at = delivered_at + timedelta(days=float(lag_days))
    rng.uniform(2, 7)  # Rút bỏ đi để giữ nguyên dãy số ngẫu nhiên của dữ liệu đã sinh.
    line.label = 1 if lag_days <= window_days else 0


def _logit(p: float) -> float:
    """Hàm ngược của logistic: đổi xác suất `p` về thang logit."""
    return math.log(p / (1 - p))
