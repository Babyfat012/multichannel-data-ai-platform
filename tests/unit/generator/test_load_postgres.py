"""Test phần đọc `pg-load/` của lệnh nạp PostgreSQL (không cần PostgreSQL chạy)."""

from __future__ import annotations

import json

from generator.batch import BatchRun
from generator.load_postgres import TABLES, iter_rows
from generator.storage import OutputStore


def test_rows_match_manifest_and_columns(tiny_config):
    """Chứng minh số dòng đọc ra khớp manifest và mỗi dòng đủ số cột khai báo cho từng bảng."""
    manifest = BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    for load in TABLES:
        rows = list(iter_rows(store, load))
        assert len(rows) == manifest["datasets"][load.table]["rows"]
        assert all(len(row) == len(load.columns) for row in rows)


def test_walmart_row_takes_export_date_from_folder(tiny_config):
    """Chứng minh `export_date` của dòng Walmart lấy từ tên thư mục, và payload khớp `purchase_order_id`."""
    BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    walmart = next(load for load in TABLES if load.table == "walmart.orders")
    path = store.glob(walmart.pattern)[0]
    folder_date = path.split("export_date=")[1][:10]
    po_id, _, export_date, payload = next(iter_rows(store, walmart))
    assert export_date == folder_date
    assert json.loads(payload)["purchaseOrderId"] == po_id


def test_sample_days_gioi_han_so_ngay_walmart(tiny_config):
    """`sample_days=3` chỉ đọc 3 ngày export, và vẫn đọc đủ `seller.skus`.

    Chạy trên dữ liệu thật do generator sinh, không phải đường dẫn bịa, nên test này bắt
    được cả trường hợp quy ước tên thư mục đổi.
    """
    BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    walmart = next(load for load in TABLES if load.table == "walmart.orders")
    skus = next(load for load in TABLES if load.table == "seller.skus")

    het = {row[2] for row in iter_rows(store, walmart)}
    ba = {row[2] for row in iter_rows(store, walmart, sample_days=3)}
    assert len(het) > 3, "dữ liệu test phải có hơn 3 ngày mới kiểm được việc lọc"
    assert len(ba) == 3
    assert ba <= het

    assert len(list(iter_rows(store, skus, sample_days=3))) == len(list(iter_rows(store, skus)))
