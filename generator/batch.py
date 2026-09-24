"""Chạy phần Batch: nối lõi mô phỏng, lớp export và renderer, rồi ghi ra đĩa.

Luồng xử lý cho mỗi ngày mô phỏng `d` từ 1 tới `days_history`:

    1. Lõi sinh các Order tạo trong ngày d (kèm cả tương lai của chúng).
    2. Lớp export quyết định mỗi Order nằm ở ngày export nào, có bản trùng không.
    3. Renderer của Channel vẽ Order thành bản ghi gốc, bỏ vào bộ đệm theo ngày export.
    4. Ghi ra mọi ngày export <= d, rồi xóa khỏi bộ đệm.

Vì sao bước 4 an toàn: một Order tạo ngày d chỉ xuất hiện ở ngày export >= d
(không thể export một Order trước khi nó tồn tại). Nên khi đã sinh xong ngày d,
các ngày export <= d không còn nhận thêm bản ghi nào. Nhờ vậy bộ đệm chỉ giữ
Order của khoảng hai tuần gần nhất, không phải cả 180 ngày.

Bố cục đầu ra (dưới `storage.output_root`):

    source-exports/channel=amazon/dataset=orders/export_date=YYYY-MM-DD/part-00000.jsonl
    pg-load/walmart.orders/export_date=YYYY-MM-DD/part-00000.jsonl
    pg-load/seller.skus/part-00000.jsonl
    ground-truth/labels/labels.parquet
    ground-truth/order_line_keys/<channel>.parquet
    run_manifest.json

`pg-load/` chứa đúng các dòng sẽ nạp vào PostgreSQL: đơn Walmart (cột
`purchase_order_id`, `raw_payload`, `fetched_at`) và danh mục SKU của Seller.
Bước nạp chạy riêng bằng lệnh `load-postgres` (xem `generator/load_postgres.py`).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pyarrow as pa

from generator.channels.amazon import SCHEMA_2026, AmazonOrdersRenderer
from generator.channels.base import OBJECT_STORE, ChannelRenderer, RenderedRecord
from generator.channels.walmart import WalmartOrdersRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS
from generator.delivery.batch_exports import PRIMARY, plan_exports
from generator.storage import OutputStore

LABELS_SCHEMA = pa.schema([
    ("order_line_id", pa.string()),
    ("label", pa.int8()),
    ("delivered_at", pa.timestamp("us", tz="UTC")),
    ("label_available_at", pa.timestamp("us", tz="UTC")),
])
LINE_KEYS_SCHEMA = pa.schema([
    ("order_line_id", pa.string()),
    ("storefront_id", pa.string()),
    ("channel_order_id", pa.string()),
    ("channel_line_id", pa.string()),
])


@dataclass
class DatasetStats:
    """Số liệu thật của một dataset, ghi vào manifest làm đáp án đối chiếu."""

    rows: int = 0
    keys: set[str] = field(default_factory=set)
    placements: Counter = field(default_factory=Counter)
    files: int = 0
    bytes: int = 0

    def summary(self) -> dict[str, Any]:
        """Tóm tắt thành dict để ghi JSON (không ghi cả tập khóa)."""
        unique = len(self.keys)
        return {
            "rows": self.rows,
            "unique_keys": unique,
            "duplicate_rate": round(1 - unique / self.rows, 6) if self.rows else 0.0,
            "rows_by_placement": dict(self.placements),
            "files": self.files,
            "bytes": self.bytes,
        }


class BatchRun:
    """Một lần sinh Batch, từ cấu hình tới các file đầu ra và manifest."""

    def __init__(self, config: GeneratorConfig) -> None:
        """Chuẩn bị lõi mô phỏng, renderer theo Channel và nơi ghi."""
        self.config = config
        self.simulator = Simulator(config)
        self.store = OutputStore(config.storage.output_root)
        # Channel chưa có renderer (TikTok Shop, Shopify, Shopee) vẫn được mô phỏng
        # và có nhãn, chỉ chưa xuất dữ liệu gốc.
        self.renderers: dict[str, ChannelRenderer] = {
            "amazon": AmazonOrdersRenderer(config),
            "walmart": WalmartOrdersRenderer(config),
        }
        self.buffers: dict[tuple[str, int], list[RenderedRecord]] = defaultdict(list)
        self.stats: dict[str, DatasetStats] = defaultdict(DatasetStats)
        self.labels: dict[str, list[Any]] = {"order_line_id": [], "label": [], "delivered_at": [],
                                             "label_available_at": []}
        self.line_keys: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.category_labels: dict[str, Counter] = defaultdict(Counter)
        self.orders_by_storefront: Counter = Counter()
        # Số liệu Sale Event, tách theo ngày sale và ngày thường (khóa True/False).
        self.sale_days = 0
        self.amazon_orders: Counter = Counter()
        self.amazon_lines: Counter = Counter()
        self.amazon_discount_sum: dict[bool, float] = {True: 0.0, False: 0.0}
        self.amazon_labels: dict[bool, Counter] = {True: Counter(), False: Counter()}

    def run(self) -> dict[str, Any]:
        """Sinh toàn bộ Batch, ghi file và trả về manifest."""
        for day in range(1, self.config.scale.days_history + 1):
            rng = rng_for(self.config.seed, "batch_exports", day)
            sale_day = self.simulator.is_sale_day(day)
            self.sale_days += sale_day
            for order in self.simulator.simulate_day(day):
                self._handle_order(order, rng, sale_day)
            self._flush(up_to_day=day)
        self._write_ground_truth()
        self._write_skus()
        manifest = self._manifest()
        self.store.write_json("run_manifest.json", manifest)
        return manifest

    def _handle_order(self, order: Order, rng: np.random.Generator, sale_day: bool = False) -> None:
        """Xử lý một Order: đặt vào ngày export, vẽ bản ghi, gom nhãn."""
        self.orders_by_storefront[order.storefront_id] += 1
        self._collect_labels(order, sale_day)
        if STOREFRONTS[order.storefront_id].channel == "amazon":
            self._collect_sale_stats(order, sale_day)

        renderer = self.renderers.get(STOREFRONTS[order.storefront_id].channel)
        if renderer is None:
            return
        self.line_keys[renderer.channel].extend(
            {**keys, "storefront_id": order.storefront_id} for keys in renderer.line_keys(order))
        for placement in plan_exports(order, self.config, rng):
            rendered = renderer.render(order, placement.as_of, placement.export_day)
            self.buffers[(renderer.channel, placement.export_day)].append(rendered)
            stats = self.stats[f"{renderer.channel}.{renderer.dataset}"]
            stats.rows += 1
            stats.keys.add(rendered.key)
            stats.placements[placement.kind] += 1

    def _collect_sale_stats(self, order: Order, sale_day: bool) -> None:
        """Đếm Order và mức giảm giá của Amazon, tách ngày sale với ngày thường."""
        self.amazon_orders[sale_day] += 1
        self.amazon_lines[sale_day] += len(order.lines)
        self.amazon_discount_sum[sale_day] += sum(
            line.unit_discount / line.unit_price for line in order.lines if line.unit_price)

    def _collect_labels(self, order: Order, sale_day: bool = False) -> None:
        """Gom nhãn của các Order Line mà tới `cutoff` đã biết nhãn."""
        if order.delivered_at is None:
            return
        available_at = order.delivered_at + timedelta(days=self.config.labels.window_days)
        if available_at > self.config.cutoff:
            return
        for line in order.lines:
            self.labels["order_line_id"].append(line.order_line_id)
            self.labels["label"].append(line.label)
            self.labels["delivered_at"].append(order.delivered_at)
            self.labels["label_available_at"].append(available_at)
            category = self.simulator.catalog.skus_by_id[line.sku_id].category
            self.category_labels[category][line.label] += 1
            if STOREFRONTS[order.storefront_id].channel == "amazon":
                self.amazon_labels[sale_day][line.label] += 1

    def _flush(self, up_to_day: int) -> None:
        """Ghi ra đĩa mọi ngày export <= `up_to_day` và xóa chúng khỏi bộ đệm."""
        ready = [k for k in self.buffers if k[1] <= up_to_day]
        for channel, export_day in sorted(ready):
            renderer = self.renderers[channel]
            records = self.buffers.pop((channel, export_day))
            export_date = self.config.day_start(export_day).date().isoformat()
            if renderer.storage == OBJECT_STORE:
                relative = (f"source-exports/channel={channel}/dataset={renderer.dataset}/"
                            f"export_date={export_date}/part-00000.jsonl")
            else:
                relative = f"pg-load/{channel}.{renderer.dataset}/export_date={export_date}/part-00000.jsonl"
            stats = self.stats[f"{channel}.{renderer.dataset}"]
            stats.bytes += self.store.write_jsonl(relative, (r.record for r in records))
            stats.files += 1

    def _write_skus(self) -> None:
        """Ghi danh mục SKU của Seller, sẽ nạp vào bảng `seller.skus`.

        Đây là hệ thống của chính Seller nên có danh mục (`category`), thứ mà dữ liệu
        đơn của Channel không có. Báo cáo skew theo danh mục join vào bảng này.
        SKU Defect Propensity là giá trị ẩn nên không ghi ra.
        """
        rows = [
            {"sku": sku.sku_id, "name": sku.name, "category": sku.category,
             "base_price_usd": sku.base_price_usd, "unit_cost_usd": sku.unit_cost_usd}
            for sku in self.simulator.catalog.skus
        ]
        stats = self.stats["seller.skus"]
        stats.rows = len(rows)
        stats.keys = {row["sku"] for row in rows}
        stats.bytes = self.store.write_jsonl("pg-load/seller.skus/part-00000.jsonl", rows)
        stats.files = 1

    def _write_ground_truth(self) -> None:
        """Ghi bảng nhãn và bảng ánh xạ mã Order Line sang mã gốc của Channel."""
        self.store.write_parquet("ground-truth/labels/labels.parquet", self.labels, LABELS_SCHEMA)
        for channel, rows in self.line_keys.items():
            columns = {name: [row[name] for row in rows] for name in LINE_KEYS_SCHEMA.names}
            self.store.write_parquet(f"ground-truth/order_line_keys/{channel}.parquet", columns, LINE_KEYS_SCHEMA)

    def _prime_day_summary(self) -> dict[str, Any] | None:
        """Số đo thật của Sale Event, đặt cạnh cấu hình để đối chiếu.

        Mỗi chỉ số có hai giá trị: trong ngày sale (`sale`) và ngoài ngày sale
        (`normal`). Tỷ lệ trả tính trên các dòng đã biết nhãn trước `cutoff`, nên
        bằng `null` nếu đợt sale nằm trong 30 ngày cuối của lịch sử.
        """
        sale = self.config.drift.prime_day
        if sale is None or not self.sale_days:
            return None
        normal_days = self.config.scale.days_history - self.sale_days

        def per_day(on_sale: bool) -> float:
            days = self.sale_days if on_sale else normal_days
            return round(self.amazon_orders[on_sale] / days, 1) if days else 0.0

        def mean_discount(on_sale: bool) -> float:
            lines = self.amazon_lines[on_sale]
            return round(self.amazon_discount_sum[on_sale] / lines, 4) if lines else 0.0

        def positive_rate(on_sale: bool) -> float | None:
            """None khi chưa dòng nào của nhóm này biết nhãn trước `cutoff`."""
            counts = self.amazon_labels[on_sale]
            total = counts[0] + counts[1]
            return round(counts[1] / total, 6) if total else None

        return {
            "days": [self.config.day_start(sale.day + i).date().isoformat() for i in range(sale.days)],
            "amazon_orders_per_day": {"sale": per_day(True), "normal": per_day(False)},
            "amazon_mean_discount_fraction": {"sale": mean_discount(True), "normal": mean_discount(False)},
            "amazon_positive_rate": {"sale": positive_rate(True), "normal": positive_rate(False)},
        }

    def _manifest(self) -> dict[str, Any]:
        """Tập hợp đáp án của lần chạy: cấu hình, số dòng, lỗi đã chèn, nhãn."""
        n_labels = len(self.labels["label"])
        positives = sum(self.labels["label"])
        switch_day = self.config.offline_problems.amazon_orders_api_switch_day
        return {
            "config": self.config.to_dict(),
            "cutoff": self.config.cutoff.isoformat(),
            "orders_by_storefront": dict(self.orders_by_storefront),
            "datasets": {name: s.summary() for name, s in sorted(self.stats.items())},
            "schema_evolution": {
                "amazon.orders": {
                    "new_schema": SCHEMA_2026,
                    "first_export_date": self.config.day_start(switch_day).date().isoformat(),
                },
            },
            "labels": {
                "rows": n_labels,
                "positive_rate": round(positives / n_labels, 6) if n_labels else 0.0,
                "positive_rate_by_category": {
                    c: round(counts[1] / (counts[0] + counts[1]), 6)
                    for c, counts in sorted(self.category_labels.items())
                },
            },
            "drift": {"prime_day": self._prime_day_summary()},
            "not_rendered_yet": sorted({s.channel for s in STOREFRONTS.values()} - set(self.renderers)),
            "placement_kinds": {PRIMARY: "bản chính", "identical_copy": "bản trùng y hệt",
                                "older_version": "bản cũ hơn cùng khóa"},
        }
