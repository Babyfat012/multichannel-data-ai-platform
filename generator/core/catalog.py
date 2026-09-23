"""Sinh danh mục SKU của Seller và độ phổ biến (Zipf) của từng SKU.

Skew theo SKU: SKU được xếp hạng ngẫu nhiên, SKU hạng r có trọng số r^(-s)
với s = `sku_zipf_exponent`. Với 2.000 SKU và s = 0,8, SKU hạng 1 chiếm
khoảng 5% Order Line. Thứ hạng được xáo trộn nên SKU bán chạy nằm rải rác
ở mọi danh mục, không dồn vào danh mục đầu tiên.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from generator.config import GeneratorConfig, PrimeDayConfig
from generator.core.model import Sku
from generator.core.rngs import rng_for
from generator.core.storefronts import CATEGORIES

# Độ lệch chuẩn của SKU Defect Propensity trên thang logit.
# 0,5 nghĩa là SKU "tệ" (+1 độ lệch chuẩn) có odds trả cao hơn khoảng 1,65 lần.
DEFECT_PROPENSITY_STD = 0.5

# SKU deal được nhân trọng số bấy nhiêu lần trong ngày sale (covariate drift).
DEAL_WEIGHT_MULTIPLIER = 5.0


@dataclass(frozen=True)
class Catalog:
    """Danh mục SKU cùng xác suất mỗi SKU được chọn cho một Order Line.

    `popularity[i]` là xác suất chọn `skus[i]`, tổng bằng 1.
    `skus_by_id` để tra SKU theo `sku_id`.
    `sale_popularity` là phân phối thay thế dùng trong ngày Sale Event.
    """

    skus: list[Sku]
    popularity: np.ndarray
    skus_by_id: dict[str, Sku]
    sale_popularity: np.ndarray


def build_catalog(config: GeneratorConfig) -> Catalog:
    """Sinh `n_skus` SKU chia theo danh mục, kèm độ phổ biến Zipf."""
    rng = rng_for(config.seed, "catalog")
    n = config.scale.n_skus

    # Chia số SKU theo `sku_share` của từng danh mục, phần dư dồn vào danh mục cuối.
    counts = [int(n * c.sku_share) for c in CATEGORIES]
    counts[-1] += n - sum(counts)

    skus: list[Sku] = []
    for category, count in zip(CATEGORIES, counts):
        low, high = category.price_range_usd
        prices = np.round(rng.uniform(low, high, size=count), 2)
        margins = rng.uniform(0.35, 0.6, size=count)
        defects = rng.normal(0.0, DEFECT_PROPENSITY_STD, size=count)
        for i in range(count):
            number = len(skus) + 1
            skus.append(
                Sku(
                    sku_id=f"{category.code}-{number:05d}",
                    name=f"{category.name.title()} item {number}",
                    category=category.name,
                    base_price_usd=float(prices[i]),
                    unit_cost_usd=round(float(prices[i] * (1 - margins[i])), 2),
                    defect_propensity=float(defects[i]),
                )
            )

    popularity = zipf_popularity(n, config.scale.sku_zipf_exponent, rng)
    return Catalog(
        skus=skus,
        popularity=popularity,
        skus_by_id={s.sku_id: s for s in skus},
        sale_popularity=sale_popularity(popularity, config.drift.prime_day, rng),
    )


def sale_popularity(popularity: np.ndarray, sale: PrimeDayConfig | None,
                    rng: np.random.Generator) -> np.ndarray:
    """Độ phổ biến trong ngày sale: SKU deal được nhân trọng số rồi chuẩn hóa lại.

    Không có Sale Event thì trả về chính `popularity`, tức ngày sale không đổi
    phân phối SKU. Tập SKU deal rút một lần theo seed nên cố định giữa các ngày.
    """
    if sale is None:
        return popularity
    deals = rng.choice(len(popularity), size=sale.deal_skus, replace=False)
    weights = popularity.copy()
    weights[deals] *= DEAL_WEIGHT_MULTIPLIER
    return weights / weights.sum()


def zipf_popularity(n: int, exponent: float, rng: np.random.Generator) -> np.ndarray:
    """Trả về mảng xác suất Zipf độ dài `n`, thứ hạng đã xáo trộn.

    Ví dụ n = 3, exponent = 1: trọng số theo hạng là 1, 1/2, 1/3, chuẩn hóa
    thành 0,55 / 0,27 / 0,18, rồi xáo để hạng 1 rơi vào một SKU ngẫu nhiên.
    """
    ranks = np.arange(1, n + 1, dtype=float)
    weights = ranks ** (-exponent)
    weights /= weights.sum()
    return rng.permutation(weights)
