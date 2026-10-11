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

`sample_days` cho nạp một tập ngày export rải đều thay vì cả dải. Hai lý do:

  * Chạy thử rẻ. Nguồn thật là 13 GB / 180 ngày; 20 ngày rải đều chỉ ~1,4 GB. Volume
    PostgreSQL nằm trên cùng phân vùng `/` với file nguồn, nên nạp cả dải là ăn chỗ hai
    lần trên một phân vùng đang chật.
  * Airflow (DP1) rồi cũng cần kéo theo từng ngày — cột `export_date` tồn tại sẵn cho
    việc đó. Nên đây là việc phải làm, không phải cầu tạm cho một lần chạy thử.

`seller.skus` không chia theo ngày nên cờ này không áp vào nó; xem `filter_to_sample_days`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from generator.day_selection import filter_to_sample_days
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


def iter_rows(store: OutputStore, load: TableLoad, sample_days: int = 0) -> Iterator[tuple[Any, ...]]:
    """Đọc file của `load` dưới `store` và trả về từng dòng sẵn sàng cho `COPY`.

    `sample_days > 0` thì chỉ đọc file thuộc N ngày export rải đều. Mặc định 0 = đọc hết,
    nên hành vi cũ không đổi.
    """
    for path in filter_to_sample_days(store.glob(load.pattern), sample_days):
        for record in store.read_jsonl(path):
            yield load.to_row(record, path)


def load_postgres(source_root: str, dsn: str, sample_days: int = 0) -> dict[str, int]:
    """Tạo bảng nếu chưa có, xóa dữ liệu cũ rồi nạp lại. Trả về số dòng theo bảng.

    `sample_days > 0` chỉ nạp N ngày export rải đều. TRUNCATE vẫn xoá sạch bảng trước, nên
    kết quả luôn là "đúng N ngày đó và không gì khác", chạy lại bao nhiêu lần cũng vậy.
    """
    import psycopg  # nhập muộn để test phần đọc file không cần thư viện PostgreSQL

    store = OutputStore(source_root)
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        for load in TABLES:
            cur.execute(f"TRUNCATE {load.table}")
            n = 0
            with cur.copy(f"COPY {load.table} ({', '.join(load.columns)}) FROM STDIN") as copy:
                for row in iter_rows(store, load, sample_days):
                    copy.write_row(row)
                    n += 1
            counts[load.table] = n
    return counts
