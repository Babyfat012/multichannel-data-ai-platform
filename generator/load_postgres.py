"""Nạp `pg-load/` vào PostgreSQL, mô phỏng hệ thống của phòng ban khác.

Lệnh `batch` chỉ ghi file. Các dòng dành cho PostgreSQL nằm ở `pg-load/` (thư mục
local hoặc MinIO), và module này đưa chúng vào bảng:

    walmart.orders   một dòng mỗi lần hệ thống kia kéo đơn từ Walmart, giữ nguyên body API
    seller.skus      danh mục SKU của Seller

`walmart.orders` cố ý không có khóa chính: export chồng lấn tạo bản trùng theo
`purchase_order_id`, và việc dedup thuộc về pipeline phía sau (Spark), không phải
hệ thống nguồn. Cột `export_date` lấy từ tên thư mục, để DP1 kéo theo từng ngày.

Mỗi lần chạy xóa sạch bảng rồi nạp lại trong cùng một transaction, nên chạy lại
bao nhiêu lần cũng ra cùng kết quả. Dữ liệu vào bằng `COPY`, nhanh hơn nhiều so
với `INSERT` từng dòng.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from generator.storage import OutputStore

DDL = """
CREATE SCHEMA IF NOT EXISTS walmart;
CREATE SCHEMA IF NOT EXISTS seller;

CREATE TABLE IF NOT EXISTS walmart.orders (
    purchase_order_id TEXT        NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL,
    export_date       DATE        NOT NULL,
    raw_payload       JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS seller.skus (
    sku            TEXT PRIMARY KEY,
    name           TEXT          NOT NULL,
    category       TEXT          NOT NULL,
    base_price_usd NUMERIC(10,2) NOT NULL,
    unit_cost_usd  NUMERIC(10,2) NOT NULL
);
"""


@dataclass(frozen=True)
class TableLoad:
    """Cách nạp một bảng: đọc file nào dưới `pg-load/`, đổi mỗi bản ghi thành dòng ra sao."""

    table: str
    columns: tuple[str, ...]
    pattern: str
    to_row: Callable[[dict[str, Any], str], tuple[Any, ...]]


def _walmart_order_row(record: dict[str, Any], path: str) -> tuple[Any, ...]:
    """Một bản ghi Walmart thành dòng; `export_date` lấy từ thư mục chứa file."""
    export_date = path.split("export_date=", 1)[1].split("/", 1)[0]
    return (record["purchase_order_id"], record["fetched_at"], export_date,
            json.dumps(record["raw_payload"], ensure_ascii=False, separators=(",", ":")))


def _sku_row(record: dict[str, Any], path: str) -> tuple[Any, ...]:
    """Một SKU thành dòng của `seller.skus`."""
    return (record["sku"], record["name"], record["category"], record["base_price_usd"], record["unit_cost_usd"])


TABLES = (
    TableLoad("walmart.orders", ("purchase_order_id", "fetched_at", "export_date", "raw_payload"),
              "pg-load/walmart.orders/export_date=*/part-*.jsonl", _walmart_order_row),
    TableLoad("seller.skus", ("sku", "name", "category", "base_price_usd", "unit_cost_usd"),
              "pg-load/seller.skus/part-*.jsonl", _sku_row),
)


def iter_rows(store: OutputStore, load: TableLoad) -> Iterator[tuple[Any, ...]]:
    """Đọc mọi file của `load` dưới `store` và trả về từng dòng sẵn sàng cho `COPY`."""
    for path in store.glob(load.pattern):
        for record in store.read_jsonl(path):
            yield load.to_row(record, path)


def load_postgres(source_root: str, dsn: str) -> dict[str, int]:
    """Tạo bảng nếu chưa có, xóa dữ liệu cũ rồi nạp lại. Trả về số dòng theo bảng."""
    import psycopg  # nhập muộn để test phần đọc file không cần thư viện PostgreSQL

    store = OutputStore(source_root)
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        for load in TABLES:
            cur.execute(f"TRUNCATE {load.table}")
            n = 0
            with cur.copy(f"COPY {load.table} ({', '.join(load.columns)}) FROM STDIN") as copy:
                for row in iter_rows(store, load):
                    copy.write_row(row)
                    n += 1
            counts[load.table] = n
    return counts
