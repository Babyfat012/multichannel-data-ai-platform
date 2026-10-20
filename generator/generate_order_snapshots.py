"""Chạy phần Batch: lõi mô phỏng -> lớp export -> renderer -> file.

Mỗi ngày mô phỏng `d`:
    1. Lõi sinh các Order tạo trong ngày d (kèm cả tương lai của chúng).
    2. Lớp export chọn ngày export cho từng Order và chèn bản trùng.
    3. Renderer ghi bản ghi gốc vào `export_date=.../part-sim<d>.jsonl`.
    4. Đóng mọi file của ngày d, rồi ghi tem `_checkpoint/day-<d>.json`.

Mỗi ngày mô phỏng ghi file riêng vì một Order có thể được export muộn hơn ngày tạo tới
~20 ngày; file dùng chung thì không bao giờ nói được "ngày d đã xong". Bên đọc glob cả
thư mục `export_date=` nên tập dòng không đổi.

Bố cục đầu ra (dưới `storage.output_root`):

    source-exports/channel=<channel>/dataset=orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/walmart.orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/seller.skus/part-00000.jsonl
    ground-truth/labels/part-simNNNNN.parquet
    ground-truth/order_line_keys/<channel>/part-simNNNNN.parquet
    _checkpoint/day-NNNNN.json
    run_manifest.json

Chạy tiếp sau khi bị cắt: tem ngày d chứa mọi bộ đếm tới hết ngày d. Tem chỉ được ghi
sau khi file đã `fsync`, bằng ghi-tạm-rồi-đổi-tên. Khi khởi động, tìm d lớn nhất mà tem
1..d đều có, xoá file của các ngày sau d, rồi chạy từ d+1. `--restart` làm lại từ đầu.

`pg-load/` được nạp vào PostgreSQL bằng lệnh `load-postgres` (xem `load_postgres.py`).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pyarrow as pa

from generator.channels.amazon import SCHEMA_2026, AmazonOrdersRenderer
from generator.channels.base import OBJECT_STORE, ChannelRenderer
from generator.channels.shopee import ShopeeOrdersRenderer
from generator.channels.shopify import ShopifyOrdersRenderer
from generator.channels.tiktok_batch import TikTokOrdersRenderer
from generator.channels.walmart import WalmartOrdersRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS
from generator.dataset_identity import build_dataset_identity
from generator.delivery.batch_exports import PRIMARY, plan_exports
from generator.storage import JsonlWriterPool, OutputStore

CHECKPOINT_DIR = "_checkpoint"
DATASET_IDENTITY_PATH = "_dataset_identity.json"
# Mọi file một ngày mô phỏng ghi ra, liệt kê tường minh để không xoá nhầm thứ khác.
DAY_OWNED_PATTERNS = (
    "source-exports/channel=*/dataset=*/export_date=*/part-sim*.jsonl",
    "pg-load/*/export_date=*/part-sim*.jsonl",
    "ground-truth/labels/part-sim*.parquet",
    "ground-truth/order_line_keys/*/part-sim*.parquet",
    f"{CHECKPOINT_DIR}/day-*.json",
)
SIM_DAY_IN_NAME = re.compile(r"(?:part-sim|day-)(\d+)\.")


def _checkpoint_relative(day: int) -> str:
    """Đường dẫn tem của một ngày mô phỏng."""
    return f"{CHECKPOINT_DIR}/day-{day:05d}.json"


LABELS_SCHEMA = pa.schema(
    [
        ("order_line_id", pa.string()),
        ("label", pa.int8()),
        ("delivered_at", pa.timestamp("us", tz="UTC")),
        ("label_available_at", pa.timestamp("us", tz="UTC")),
    ]
)
LINE_KEYS_SCHEMA = pa.schema(
    [
        ("order_line_id", pa.string()),
        ("storefront_id", pa.string()),
        ("channel_order_id", pa.string()),
        ("channel_line_id", pa.string()),
    ]
)


@dataclass
class DatasetStats:
    """Số liệu thật của một dataset, ghi vào manifest làm đáp án đối chiếu."""

    rows: int = 0
    unique_keys: int = 0
    placements: Counter = field(default_factory=Counter)
    files: int = 0
    bytes: int = 0

    def summary(self) -> dict[str, Any]:
        """Tóm tắt thành dict để ghi JSON."""
        return {
            "rows": self.rows,
            "unique_keys": self.unique_keys,
            "duplicate_rate": round(1 - self.unique_keys / self.rows, 6) if self.rows else 0.0,
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
        self.dataset_identity = build_dataset_identity(config)
        # Mỗi Channel có renderer nguồn Batch. Nhãn thật vẫn ở ground truth riêng.
        self.renderers: dict[str, ChannelRenderer] = {
            "amazon": AmazonOrdersRenderer(config),
            "tiktok_shop": TikTokOrdersRenderer(config),
            "shopify": ShopifyOrdersRenderer(config),
            "walmart": WalmartOrdersRenderer(config),
            "shopee": ShopeeOrdersRenderer(config),
        }
        # Mỗi ngày export đang chờ dòng có sẵn một file mở, nên RAM không tăng theo số đơn.
        self.jsonl: JsonlWriterPool | None = None
        self.stats: dict[str, DatasetStats] = defaultdict(DatasetStats)
        # Bộ đệm của một ngày mô phỏng; bộ đếm dồn phải nằm trong tem (`_counters`).
        self.labels: dict[str, list[Any]] = self._empty_labels()
        self.line_keys: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.label_rows = 0
        self.label_positives = 0
        self.category_labels: dict[str, Counter] = defaultdict(Counter)
        self.orders_by_storefront: Counter = Counter()
        # Số liệu Sale Event, tách theo ngày sale và ngày thường (khóa True/False).
        self.sale_days = 0
        self.amazon_orders: Counter = Counter()
        self.amazon_lines: Counter = Counter()
        self.amazon_discount_sum: dict[bool, float] = {True: 0.0, False: 0.0}
        self.amazon_labels: dict[bool, Counter] = {True: Counter(), False: Counter()}

    def run(self, restart: bool = False) -> dict[str, Any]:
        """Sinh toàn bộ Batch và trả manifest; mặc định chạy tiếp từ tem gần nhất.

        `restart=True` xoá đầu ra cũ và làm lại từ ngày 1. Kết quả không phụ thuộc việc có bị cắt
        hay không, vì mỗi ngày rút số ngẫu nhiên riêng theo `(seed, "batch_exports", day)`.
        """
        self._check_resume_identity(restart)
        resume_from = 0 if restart else self._last_complete_day()
        if resume_from:
            self._restore(self.store.read_json(_checkpoint_relative(resume_from)))
        self._discard_days_after(resume_from)
        if restart:
            # Đang chạy dở thì không được để manifest cũ trông như đã xong.
            self.store.remove("run_manifest.json")
        self.store.write_json_atomic(DATASET_IDENTITY_PATH, self.dataset_identity)
        with self.store.jsonl_writers() as jsonl:
            self.jsonl = jsonl
            for day in range(resume_from + 1, self.config.scale.days_history + 1):
                rng = rng_for(self.config.seed, "batch_exports", day)
                sale_day = self.simulator.is_sale_day(day)
                self.sale_days += sale_day
                for order in self.simulator.simulate_day(day):
                    self._handle_order(order, rng, sale_day, day)
                self._flush(day)
        self._write_skus()
        manifest = self._manifest()
        self.store.write_json("run_manifest.json", manifest)
        return manifest

    def _check_resume_identity(self, restart: bool) -> None:
        """Từ chối chạy tiếp trên đầu ra cũ hoặc khác thế giới, trước khi sửa gì.

        `--restart` vẫn được sinh lại. Chỉ giả định một người ghi, không phải khoá phân tán.
        """
        if restart:
            return
        if not self.store.exists(DATASET_IDENTITY_PATH):
            owned = ("run_manifest.json", "pg-load/seller.skus/part-00000.jsonl")
            if any(self.store.exists(path) for path in owned) or any(
                self.store.glob(pattern) for pattern in DAY_OWNED_PATTERNS
            ):
                raise ValueError(
                    "Output has no dataset identity; use a new output_root or "
                    "explicit --restart to regenerate legacy data."
                )
            return
        previous = self.store.read_json(DATASET_IDENTITY_PATH)
        if previous != self.dataset_identity:
            raise ValueError(
                "Dataset identity mismatch (config/code/runtime changed); use a "
                "new output_root or explicit --restart to regenerate."
            )
        version = self.dataset_identity["dataset_version"]
        for path in self.store.glob(f"{CHECKPOINT_DIR}/day-*.json"):
            if self.store.read_json(path).get("dataset_version") != version:
                raise ValueError(f"Checkpoint dataset identity mismatch: {path}")
        if self.store.exists("run_manifest.json") and (
            self.store.read_json("run_manifest.json").get("dataset_version") != version
        ):
            raise ValueError("Manifest dataset identity mismatch")

    def _handle_order(
        self, order: Order, rng: np.random.Generator, sale_day: bool, sim_day: int
    ) -> None:
        """Xử lý một Order: đặt vào ngày export, ghi bản ghi, gom nhãn."""
        self.orders_by_storefront[order.storefront_id] += 1
        self._collect_labels(order, sale_day)
        if STOREFRONTS[order.storefront_id].channel == "amazon":
            self._collect_sale_stats(order, sale_day)

        renderer = self.renderers.get(STOREFRONTS[order.storefront_id].channel)
        if renderer is None:
            return
        self.line_keys[renderer.channel].extend(
            {**keys, "storefront_id": order.storefront_id} for keys in renderer.line_keys(order)
        )
        # Channel mới dùng RNG riêng theo Order để không đổi dãy số của Amazon/Walmart.
        placement_rng = (
            rng
            if renderer.channel in ("amazon", "walmart")
            else rng_for(
                self.config.seed,
                "batch_exports_new_channel",
                sim_day,
                renderer.channel,
                order.order_id,
            )
        )
        for placement in plan_exports(order, self.config, placement_rng):
            rendered = renderer.render(order, placement.as_of, placement.export_day)
            key = (renderer.channel, placement.export_day, sim_day)
            self.jsonl.write(
                key, self._export_path(renderer, placement.export_day, sim_day), rendered.record
            )
            stats = self.stats[f"{renderer.channel}.{renderer.dataset}"]
            stats.rows += 1
            # Mỗi Order có đúng một bản `primary`, nên đếm bản chính là đếm khoá duy nhất
            # mà không phải giữ một set khoá rất lớn trong RAM.
            stats.unique_keys += placement.kind == PRIMARY
            stats.placements[placement.kind] += 1

    def _collect_sale_stats(self, order: Order, sale_day: bool) -> None:
        """Đếm Order và mức giảm giá của Amazon, tách ngày sale với ngày thường."""
        self.amazon_orders[sale_day] += 1
        self.amazon_lines[sale_day] += len(order.lines)
        self.amazon_discount_sum[sale_day] += sum(
            line.unit_discount / line.unit_price for line in order.lines if line.unit_price
        )

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
            self.label_rows += 1
            self.label_positives += line.label
            category = self.simulator.catalog.skus_by_id[line.sku_id].category
            self.category_labels[category][line.label] += 1
            if STOREFRONTS[order.storefront_id].channel == "amazon":
                self.amazon_labels[sale_day][line.label] += 1

    def _export_path(self, renderer: ChannelRenderer, export_day: int, sim_day: int) -> str:
        """Đường dẫn file: thư mục là `export_date=`, tên file là ngày mô phỏng đã ghi nó."""
        export_date = self.config.day_start(export_day).date().isoformat()
        part = f"part-sim{sim_day:05d}.jsonl"
        if renderer.storage == OBJECT_STORE:
            return (
                f"source-exports/channel={renderer.channel}/dataset={renderer.dataset}/"
                f"export_date={export_date}/{part}"
            )
        return f"pg-load/{renderer.channel}.{renderer.dataset}/export_date={export_date}/{part}"

    def _flush(self, day: int) -> None:
        """Kết thúc ngày `day`: đóng mọi file của nó, ghi Parquet, rồi dán tem.

        Tem phải là bước cuối: mọi thứ nó báo "xong" đã được `fsync` trước đó.
        """
        for key in sorted(self.jsonl.keys()):
            channel = key[0]
            renderer = self.renderers[channel]
            _, written = self.jsonl.close(key)
            stats = self.stats[f"{channel}.{renderer.dataset}"]
            stats.bytes += written
            stats.files += 1
        self._flush_ground_truth(day)
        self.store.write_json_atomic(_checkpoint_relative(day), self._counters())

    def _flush_ground_truth(self, day: int) -> None:
        """Ghi nhãn và ánh xạ khoá của ngày `day` thành một file Parquet riêng.

        Một file mỗi ngày vì footer Parquet chỉ ghi lúc đóng; file mở suốt kỳ sẽ mất trắng khi
        bị cắt. Ngày không có dòng thì không tạo file.
        """
        if self.labels["order_line_id"]:
            with self.store.parquet_writer(
                f"ground-truth/labels/part-sim{day:05d}.parquet", LABELS_SCHEMA
            ) as writer:
                writer.write(self.labels)
        self.labels = self._empty_labels()
        for channel, rows in self.line_keys.items():
            if not rows:
                continue
            columns = {name: [row[name] for row in rows] for name in LINE_KEYS_SCHEMA.names}
            with self.store.parquet_writer(
                f"ground-truth/order_line_keys/{channel}/part-sim{day:05d}.parquet",
                LINE_KEYS_SCHEMA,
            ) as writer:
                writer.write(columns)
        self.line_keys.clear()

    @staticmethod
    def _empty_labels() -> dict[str, list[Any]]:
        """Bộ đệm nhãn rỗng, một list cho mỗi cột của `LABELS_SCHEMA`."""
        return {name: [] for name in LABELS_SCHEMA.names}

    def _last_complete_day(self) -> int:
        """Ngày `d` lớn nhất mà tem của mọi ngày 1..d đều có.

        Tem lẻ sau một khoảng trống không chứng minh được các ngày trước đã xong.
        """
        present = {
            int(match.group(1))
            for path in self.store.glob(f"{CHECKPOINT_DIR}/day-*.json")
            if (match := SIM_DAY_IN_NAME.search(path))
        }
        day = 0
        while day + 1 in present:
            day += 1
        return day

    def _discard_days_after(self, day: int) -> list[str]:
        """Xoá mọi file của ngày mô phỏng > `day` (kể cả `.tmp` sót), trả danh sách đã xoá.

        Các file đó chưa có tem nên không biết đủ hay thiếu; sinh lại cho ra đúng dữ liệu đó.
        """
        removed = []
        for pattern in DAY_OWNED_PATTERNS:
            for path in self.store.glob(pattern):
                match = SIM_DAY_IN_NAME.search(path)
                if match and int(match.group(1)) > day:
                    self.store.remove(path)
                    removed.append(path)
        for path in self.store.glob(f"{CHECKPOINT_DIR}/*.tmp"):
            self.store.remove(path)
            removed.append(path)
        return removed

    def _counters(self) -> dict[str, Any]:
        """Mọi bộ đếm dồn, dạng JSON được, để ghi vào tem.

        Phải đủ mọi thứ `_manifest` đọc, nếu không lần chạy tiếp ra manifest thiếu số mà không
        báo lỗi. Khoá bool ghi thành "sale"/"normal", khoá số thành chuỗi.
        """
        return {
            "dataset_version": self.dataset_identity["dataset_version"],
            "datasets": {
                name: {
                    "rows": st.rows,
                    "unique_keys": st.unique_keys,
                    "placements": dict(st.placements),
                    "files": st.files,
                    "bytes": st.bytes,
                }
                for name, st in sorted(self.stats.items())
            },
            "label_rows": self.label_rows,
            "label_positives": self.label_positives,
            "category_labels": {
                c: dict(counts) for c, counts in sorted(self.category_labels.items())
            },
            "orders_by_storefront": dict(self.orders_by_storefront),
            "sale_days": self.sale_days,
            "amazon": {
                name: {
                    "orders": self.amazon_orders[flag],
                    "lines": self.amazon_lines[flag],
                    "discount_sum": self.amazon_discount_sum[flag],
                    "labels": dict(self.amazon_labels[flag]),
                }
                for name, flag in (("sale", True), ("normal", False))
            },
        }

    def _restore(self, data: dict[str, Any]) -> None:
        """Nạp lại các bộ đếm từ tem, nghịch đảo của `_counters`."""
        for name, values in data["datasets"].items():
            stats = self.stats[name]
            stats.rows = values["rows"]
            stats.unique_keys = values["unique_keys"]
            stats.placements = Counter(values["placements"])
            stats.files = values["files"]
            stats.bytes = values["bytes"]
        self.label_rows = data["label_rows"]
        self.label_positives = data["label_positives"]
        self.category_labels = defaultdict(
            Counter,
            {
                category: Counter({int(label): n for label, n in counts.items()})
                for category, counts in data["category_labels"].items()
            },
        )
        self.orders_by_storefront = Counter(data["orders_by_storefront"])
        self.sale_days = data["sale_days"]
        for name, flag in (("sale", True), ("normal", False)):
            values = data["amazon"][name]
            self.amazon_orders[flag] = values["orders"]
            self.amazon_lines[flag] = values["lines"]
            self.amazon_discount_sum[flag] = values["discount_sum"]
            self.amazon_labels[flag] = Counter(
                {int(label): n for label, n in values["labels"].items()}
            )

    def _write_skus(self) -> None:
        """Ghi danh mục SKU của Seller (có `category`) để nạp vào `seller.skus`."""
        rows = [
            {
                "sku": sku.sku_id,
                "name": sku.name,
                "category": sku.category,
                "base_price_usd": sku.base_price_usd,
                "unit_cost_usd": sku.unit_cost_usd,
            }
            for sku in self.simulator.catalog.skus
        ]
        stats = self.stats["seller.skus"]
        stats.rows = len(rows)
        stats.unique_keys = len({row["sku"] for row in rows})
        stats.bytes = self.store.write_jsonl("pg-load/seller.skus/part-00000.jsonl", rows)
        stats.files = 1

    def _prime_day_summary(self) -> dict[str, Any] | None:
        """Số đo thật của Sale Event, chia ngày sale / ngày thường, để đối chiếu với cấu hình.

        Tỷ lệ trả là `null` nếu đợt sale nằm trong 30 ngày cuối của lịch sử.
        """
        sale = self.config.drift.prime_day
        if sale is None or not self.sale_days:
            return None
        normal_days = self.config.scale.days_history - self.sale_days

        def per_day(on_sale: bool) -> float:
            """Số Order Amazon trung bình mỗi ngày của nhóm ngày sale hoặc ngày thường."""
            days = self.sale_days if on_sale else normal_days
            return round(self.amazon_orders[on_sale] / days, 1) if days else 0.0

        def mean_discount(on_sale: bool) -> float:
            """Tỷ lệ giảm giá trung bình trên mỗi Order Line của nhóm."""
            lines = self.amazon_lines[on_sale]
            return round(self.amazon_discount_sum[on_sale] / lines, 4) if lines else 0.0

        def positive_rate(on_sale: bool) -> float | None:
            """None khi chưa dòng nào của nhóm này biết nhãn trước `cutoff`."""
            counts = self.amazon_labels[on_sale]
            total = counts[0] + counts[1]
            return round(counts[1] / total, 6) if total else None

        return {
            "days": [
                self.config.day_start(sale.day + i).date().isoformat() for i in range(sale.days)
            ],
            "amazon_orders_per_day": {"sale": per_day(True), "normal": per_day(False)},
            "amazon_mean_discount_fraction": {
                "sale": mean_discount(True),
                "normal": mean_discount(False),
            },
            "amazon_positive_rate": {"sale": positive_rate(True), "normal": positive_rate(False)},
        }

    def _manifest(self) -> dict[str, Any]:
        """Tập hợp đáp án của lần chạy: cấu hình, số dòng, lỗi đã chèn, nhãn."""
        n_labels = self.label_rows
        positives = self.label_positives
        switch_day = self.config.offline_problems.amazon_orders_api_switch_day
        return {
            "dataset_version": self.dataset_identity["dataset_version"],
            "generator_sha256": self.dataset_identity["generator_sha256"],
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
            "not_rendered_yet": sorted(
                {s.channel for s in STOREFRONTS.values()} - set(self.renderers)
            ),
            "placement_kinds": {
                PRIMARY: "bản chính",
                "identical_copy": "bản trùng y hệt",
                "older_version": "bản cũ hơn cùng khóa",
            },
        }
