"""Chạy phần Batch: nối lõi mô phỏng, lớp export và renderer, rồi ghi ra đĩa.

Luồng xử lý cho mỗi ngày mô phỏng `d` từ 1 tới `days_history`:

    1. Lõi sinh các Order tạo trong ngày d (kèm cả tương lai của chúng).
    2. Lớp export quyết định mỗi Order nằm ở ngày export nào, có bản trùng không.
    3. Renderer của Channel vẽ Order thành bản ghi gốc, ghi thẳng vào file của ngày
       export — nhưng vào file RIÊNG của ngày d, tên `part-sim<d>.jsonl`.
    4. Đóng mọi file ngày d vừa mở, rồi ghi tem `_checkpoint/day-<d>.json`.

Vì sao mỗi ngày mô phỏng có file riêng: một Order tạo ngày d được export vào ngày
nó đổi trạng thái lần cuối, muộn hơn d tới ~20 ngày (đo được: tối đa 18 trên
736.129 Order, trần cứng từ hằng số vòng đời là 16 ngày + `overlap_days`). Nên nếu
mỗi thư mục `export_date=` chỉ có một file dùng chung, file đó phải mở suốt ~20
ngày mô phỏng và nhận dòng từ cả 20 ngày đó. Lúc ấy không có thời điểm nào nói được
"ngày d đã xong và nằm trọn trên đĩa", nên không chạy tiếp được sau khi bị cắt.
Cho mỗi ngày một file thì hết ngày d là đóng được hết phần của nó.

Phân vùng KHÔNG đổi: thư mục vẫn là `export_date=`, tức ngày Channel giao file.
Chỉ có tên file bên trong thư mục là mới, và mọi bên đọc đều dùng glob cả thư mục
(`generator/report.py`, `generator/load_postgres.py`), nên tập dòng nhận được y như cũ.

Vì sao ghi thẳng thay vì gom theo ngày: một Order có thể chỉ export sau hai tuần
(lúc giao xong), nên gom lại thì RAM phải giữ hai tuần bản ghi — tỉ lệ với số đơn
mỗi ngày, tức ~33 GB ở quy mô 100 GB. Giữ sẵn ~14 file mở thì RAM không đổi.

Bố cục đầu ra (dưới `storage.output_root`):

    source-exports/channel=amazon/dataset=orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/walmart.orders/export_date=YYYY-MM-DD/part-simNNNNN.jsonl
    pg-load/seller.skus/part-00000.jsonl
    ground-truth/labels/part-simNNNNN.parquet
    ground-truth/order_line_keys/<channel>/part-simNNNNN.parquet
    _checkpoint/day-NNNNN.json
    run_manifest.json

Chạy tiếp sau khi bị cắt (mất điện, tắt máy giữa lần chạy 100 GB ~3,2 giờ):

    `_checkpoint/day-<d>.json` vừa là tem "ngày d xong" vừa là chỗ chứa mọi con số
    dồn tới hết ngày d. Gộp làm một file nên tem và số không thể lệch nhịp nhau.

    Thứ tự ghi là điều kiện sống còn: đóng file (có `fsync`) -> rồi mới ghi tem,
    bằng ghi-tạm-rồi-đổi-tên. Làm ngược lại thì mất điện có thể để lại tem nói
    "ngày d xong" trong khi file ngày d thiếu đuôi, và lần chạy sau sẽ bỏ qua ngày
    d — mất dữ liệu mà manifest vẫn ghi đủ số và test vẫn xanh.

    Khi khởi động: tìm `d` lớn nhất mà tem 1..d đều có, nạp số từ tem đó, xóa mọi
    `part-sim<k>` với k > d (đó là file của ngày bị cắt, ghi dở), rồi chạy từ d+1.
    `--restart` bỏ qua tem và làm lại từ ngày 1.

`pg-load/` chứa đúng các dòng sẽ nạp vào PostgreSQL: đơn Walmart (cột
`purchase_order_id`, `raw_payload`, `fetched_at`) và danh mục SKU của Seller.
Bước nạp chạy riêng bằng lệnh `load-postgres` (xem `generator/load_postgres.py`).
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
from generator.channels.walmart import WalmartOrdersRenderer
from generator.config import GeneratorConfig
from generator.core.model import Order
from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.core.storefronts import STOREFRONTS
from generator.delivery.batch_exports import PRIMARY, plan_exports
from generator.storage import JsonlWriterPool, OutputStore

CHECKPOINT_DIR = "_checkpoint"
# Mọi file do một ngày mô phỏng ghi ra, và tem của ngày đó. Dùng để dọn file ghi dở
# của ngày bị cắt. Liệt kê tường minh thay vì `**/part-sim*` để không vô tình xóa
# thứ gì khác nằm dưới `output_root`.
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
        # Channel chưa có renderer (TikTok Shop, Shopify, Shopee) vẫn được mô phỏng
        # và có nhãn, chỉ chưa xuất dữ liệu gốc.
        self.renderers: dict[str, ChannelRenderer] = {
            "amazon": AmazonOrdersRenderer(config),
            "walmart": WalmartOrdersRenderer(config),
        }
        # Không có bộ đệm bản ghi: `jsonl` giữ sẵn một file mở cho mỗi ngày export
        # đang chờ dòng mới, nên RAM không tỉ lệ với số đơn mỗi ngày.
        self.jsonl: JsonlWriterPool | None = None
        self.stats: dict[str, DatasetStats] = defaultdict(DatasetStats)
        # `labels` và `line_keys` là bộ đệm của một ngày mô phỏng, không phải của cả
        # lần chạy: `_flush` ghi chúng ra Parquet của riêng ngày đó rồi xóa. Tổng số
        # nhãn phải đếm dần vì bộ đệm không còn giữ lịch sử — và mọi bộ đếm dồn kiểu
        # này đều phải nằm trong tem, xem `_counters`.
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
        """Sinh toàn bộ Batch, ghi file và trả về manifest.

        Mặc định chạy tiếp từ tem gần nhất nếu `output_root` đã có tem của một lần
        chạy bị cắt. `restart=True` bỏ qua tem và làm lại từ ngày 1, xóa mọi file
        của lần chạy cũ.

        Kết quả không phụ thuộc việc có bị cắt hay không: mỗi ngày mô phỏng rút số
        ngẫu nhiên riêng theo `(seed, "batch_exports", day)` và không đọc trạng thái
        của ngày trước (xem `tests/test_core.py`), nên ngày d cho ra cùng dữ liệu dù
        chạy từ ngày 1 hay nhảy vào từ ngày d.
        """
        resume_from = 0 if restart else self._last_complete_day()
        if resume_from:
            self._restore(self.store.read_json(_checkpoint_relative(resume_from)))
        self._discard_days_after(resume_from)
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

    def _handle_order(self, order: Order, rng: np.random.Generator, sale_day: bool, sim_day: int) -> None:
        """Xử lý một Order: đặt vào ngày export, vẽ bản ghi, gom nhãn.

        `sim_day` là ngày mô phỏng đang chạy. Nó đi vào tên file để mỗi ngày ghi vào
        file riêng của mình, kể cả khi hai ngày cùng rót vào một `export_date=`.
        """
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
            key = (renderer.channel, placement.export_day, sim_day)
            self.jsonl.write(key, self._export_path(renderer, placement.export_day, sim_day), rendered.record)
            stats = self.stats[f"{renderer.channel}.{renderer.dataset}"]
            stats.rows += 1
            # Đếm khóa duy nhất bằng số bản chính, không bằng một set khóa: mỗi Order
            # có đúng một placement `primary` (xem `plan_exports`) và `rendered.key`
            # chỉ phụ thuộc Order, nên bản trùng luôn lặp lại khóa của bản chính.
            # Set khóa ở quy mô 100 GB là ~117 triệu string trong RAM, chỉ để lấy
            # `len()`. `tests/test_batch.py` khóa bất biến này bằng một lần đối chiếu.
            stats.unique_keys += placement.kind == PRIMARY
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
            self.label_rows += 1
            self.label_positives += line.label
            category = self.simulator.catalog.skus_by_id[line.sku_id].category
            self.category_labels[category][line.label] += 1
            if STOREFRONTS[order.storefront_id].channel == "amazon":
                self.amazon_labels[sale_day][line.label] += 1

    def _export_path(self, renderer: ChannelRenderer, export_day: int, sim_day: int) -> str:
        """Đường dẫn file của một ngày export, theo nơi lưu mà Channel dùng.

        Thư mục là `export_date=` (ngày Channel giao file) và tên file là ngày mô
        phỏng đã ghi nó. Một thư mục có nhiều file part, mỗi file của một ngày mô
        phỏng; Spark, Iceberg và DuckDB đều đọc cả thư mục nên điều này không đổi
        tập dòng mà bên đọc nhận được.
        """
        export_date = self.config.day_start(export_day).date().isoformat()
        part = f"part-sim{sim_day:05d}.jsonl"
        if renderer.storage == OBJECT_STORE:
            return (f"source-exports/channel={renderer.channel}/dataset={renderer.dataset}/"
                    f"export_date={export_date}/{part}")
        return f"pg-load/{renderer.channel}.{renderer.dataset}/export_date={export_date}/{part}"

    def _flush(self, day: int) -> None:
        """Kết thúc ngày `day`: đóng hết file của nó, ghi Parquet, rồi dán tem.

        Đóng được HẾT, không lọc theo ngày export, vì mọi file đang mở đều là file
        của ngày `day` (tên chứa `sim<day>`) và không ngày nào khác ghi vào chúng.
        Đóng cũng là lúc duy nhất biết số byte thật của một file.

        Tem là việc CUỐI CÙNG. Mọi thứ nó nói "đã xong" phải đã nằm trên đĩa trước
        đó: `JsonlAppender.close` và `OutputStore.parquet_writer` đều `fsync`.
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
        """Ghi bộ đệm nhãn và ánh xạ khóa của ngày `day` thành file Parquet riêng.

        Một file mỗi ngày mô phỏng, một row group mỗi file. Trước đây là một file cho
        cả lần chạy với một row group mỗi ngày; đổi vì footer Parquet chỉ được ghi lúc
        đóng, nên một file mở suốt 180 ngày là thứ chắc chắn mất trắng khi bị cắt.

        Kích cỡ row group không đổi: vẫn là dữ liệu của đúng một ngày mô phỏng, khoảng
        một triệu dòng ở quy mô 100 GB, đúng vùng Spark đọc hiệu quả.

        Ngày không có dòng nào thì không tạo file, để khỏi sinh file rỗng vô ích.
        Không cần tạo: tem mới là thứ nói ngày đó đã xong, không phải sự có mặt của file.
        """
        if self.labels["order_line_id"]:
            with self.store.parquet_writer(
                    f"ground-truth/labels/part-sim{day:05d}.parquet", LABELS_SCHEMA) as writer:
                writer.write(self.labels)
        self.labels = self._empty_labels()
        for channel, rows in self.line_keys.items():
            if not rows:
                continue
            columns = {name: [row[name] for row in rows] for name in LINE_KEYS_SCHEMA.names}
            with self.store.parquet_writer(
                    f"ground-truth/order_line_keys/{channel}/part-sim{day:05d}.parquet",
                    LINE_KEYS_SCHEMA) as writer:
                writer.write(columns)
        self.line_keys.clear()

    @staticmethod
    def _empty_labels() -> dict[str, list[Any]]:
        """Bộ đệm nhãn rỗng, một list cho mỗi cột của `LABELS_SCHEMA`."""
        return {name: [] for name in LABELS_SCHEMA.names}

    def _last_complete_day(self) -> int:
        """Ngày `d` lớn nhất mà tem của MỌI ngày 1..d đều có mặt.

        Lấy `d` lớn nhất liên tục chứ không lấy số lớn nhất tìm được: một tem đứng
        lẻ sau một khoảng trống (ví dụ còn sót của lần chạy trước có cấu hình khác)
        không chứng minh được các ngày trước nó đã xong. `_discard_days_after` sẽ
        dọn mấy tem lẻ đó đi.
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
        """Xóa mọi file thuộc ngày mô phỏng > `day`, trả về danh sách đã xóa.

        Đây là các file của ngày bị cắt giữa đường: đã có dòng trên đĩa nhưng chưa
        có tem, nên không biết chúng đủ hay thiếu. Rẻ hơn và chắc hơn là bỏ đi rồi
        sinh lại — ngày mô phỏng là tiền định nên sinh lại cho ra đúng dữ liệu đó.

        Cũng xóa file `.tmp` còn sót: đó là tem đang ghi dở lúc bị cắt.
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
        """Mọi bộ đếm dồn theo thời gian, dạng JSON được, để ghi vào tem.

        Phải có ĐỦ mọi thứ mà `_manifest` đọc và `_handle_order`/`_collect_*` cộng
        dồn. Thiếu một cái thì lần chạy tiếp cho ra manifest thiếu số, mà không có
        lỗi nào báo. `tests/test_batch.py` khóa chuyện này bằng cách so manifest của
        một lần chạy liền mạch với một lần chạy bị cắt.

        Khóa `bool` của `amazon_*` ghi thành "sale"/"normal" vì JSON chỉ có khóa
        chuỗi; khóa số của các `Counter` nhãn ghi thành "0"/"1" rồi đổi lại khi nạp.
        """
        return {
            "datasets": {
                name: {"rows": st.rows, "unique_keys": st.unique_keys,
                       "placements": dict(st.placements), "files": st.files, "bytes": st.bytes}
                for name, st in sorted(self.stats.items())
            },
            "label_rows": self.label_rows,
            "label_positives": self.label_positives,
            "category_labels": {c: dict(counts) for c, counts in sorted(self.category_labels.items())},
            "orders_by_storefront": dict(self.orders_by_storefront),
            "sale_days": self.sale_days,
            "amazon": {
                name: {"orders": self.amazon_orders[flag], "lines": self.amazon_lines[flag],
                       "discount_sum": self.amazon_discount_sum[flag],
                       "labels": dict(self.amazon_labels[flag])}
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
        self.category_labels = defaultdict(Counter, {
            category: Counter({int(label): n for label, n in counts.items()})
            for category, counts in data["category_labels"].items()
        })
        self.orders_by_storefront = Counter(data["orders_by_storefront"])
        self.sale_days = data["sale_days"]
        for name, flag in (("sale", True), ("normal", False)):
            values = data["amazon"][name]
            self.amazon_orders[flag] = values["orders"]
            self.amazon_lines[flag] = values["lines"]
            self.amazon_discount_sum[flag] = values["discount_sum"]
            self.amazon_labels[flag] = Counter({int(label): n for label, n in values["labels"].items()})

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
        stats.unique_keys = len({row["sku"] for row in rows})
        stats.bytes = self.store.write_jsonl("pg-load/seller.skus/part-00000.jsonl", rows)
        stats.files = 1

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
        n_labels = self.label_rows
        positives = self.label_positives
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
