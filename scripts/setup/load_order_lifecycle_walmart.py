"""Nạp đơn Walmart bootstrap của nguồn v2 vào PostgreSQL `walmart_lifecycle.orders` (mô phỏng DB phòng ban khác).

Sau bước này job `bronze/ingest_order_lifecycle.py --walmart-jdbc walmart_lifecycle.orders` đọc Walmart qua JDBC,
đúng boundary manifest khai. Bảng v1 `walmart.orders` không bị đụng.

`raw_payload` lưu TEXT chứ không JSONB: JSONB sắp lại khóa và bỏ khoảng trắng, tức DB "sửa"
chuỗi gốc trước khi Bronze chép. Nạp lại cùng dataset_version thì xóa rồi chèn trong MỘT
transaction, nên chạy lại không nhân đôi dòng.

    .venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root out/<fixture>/default
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import NamedTuple

DDL = """
CREATE SCHEMA IF NOT EXISTS walmart_lifecycle;
CREATE TABLE IF NOT EXISTS walmart_lifecycle.orders (
    dataset_version   TEXT        NOT NULL,
    purchase_order_id TEXT        NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL,
    raw_payload       TEXT        NOT NULL,
    PRIMARY KEY (dataset_version, purchase_order_id)
);
"""


class Row(NamedTuple):
    """Một đơn Walmart sẵn sàng chèn."""
    dataset_version: str
    purchase_order_id: str
    fetched_at: str
    raw_payload: str


def bootstrap_rows(root: Path) -> list[Row]:
    """Đọc file bootstrap Walmart theo manifest; payload giữ đúng chuỗi JSON canonical của file."""
    manifest = json.loads((root / "manifest.json").read_text())
    files = [f for f in manifest["files"] if f["feed"] == "walmart.orders"]
    if len(files) != 1:
        raise ValueError(f"Cần đúng một file walmart.orders trong manifest, có {len(files)}")
    rows = []
    for line in (root / files[0]["path"]).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        payload = json.dumps(record["raw_payload"], sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False)
        rows.append(Row(manifest["dataset_version"], record["purchase_order_id"], record["fetched_at"], payload))
    if len(rows) != files[0]["rows"]:
        raise ValueError("Số dòng file Walmart khác manifest")
    return rows


def load(rows: list[Row], settings: dict[str, str]) -> int:
    """Thay toàn bộ dòng của dataset_version này trong một transaction; trả số dòng đã chèn."""
    import psycopg  # nhập muộn để test phần đọc file không cần thư viện PostgreSQL

    with psycopg.connect(host="127.0.0.1", port=int(settings.get("POSTGRES_PORT", "5432")),
                         dbname=settings["POSTGRES_DB"], user=settings["POSTGRES_USER"],
                         password=settings["POSTGRES_PASSWORD"]) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("DELETE FROM walmart_lifecycle.orders WHERE dataset_version = %s", (rows[0].dataset_version,))
        cur.executemany(
            "INSERT INTO walmart_lifecycle.orders (dataset_version, purchase_order_id, fetched_at, raw_payload) "
            "VALUES (%s, %s, %s, %s)", rows)
        cur.execute("SELECT count(*) FROM walmart_lifecycle.orders WHERE dataset_version = %s", (rows[0].dataset_version,))
        return cur.fetchone()[0]


def main() -> int:
    """Nạp bootstrap Walmart của một biến thể fixture."""
    from scripts.setup.setup_order_snapshots_debezium import load_settings

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    rows = bootstrap_rows(args.root)
    count = load(rows, load_settings(args.env_file))
    print(f"walmart_lifecycle.orders: dataset_version={rows[0].dataset_version}, file={len(rows)}, DB={count}")
    return 0 if count == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
