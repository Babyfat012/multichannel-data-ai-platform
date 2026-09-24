"""Test phần đọc `pg-load/` của lệnh nạp PostgreSQL (không cần PostgreSQL chạy)."""

from __future__ import annotations

import json

from generator.batch import BatchRun
from generator.load_postgres import TABLES, iter_rows
from generator.storage import OutputStore


def test_rows_match_manifest_and_columns(tiny_config):
    manifest = BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    for load in TABLES:
        rows = list(iter_rows(store, load))
        assert len(rows) == manifest["datasets"][load.table]["rows"]
        assert all(len(row) == len(load.columns) for row in rows)


def test_walmart_row_takes_export_date_from_folder(tiny_config):
    BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    walmart = next(load for load in TABLES if load.table == "walmart.orders")
    path = store.glob(walmart.pattern)[0]
    folder_date = path.split("export_date=")[1][:10]
    po_id, _, export_date, payload = next(iter_rows(store, walmart))
    assert export_date == folder_date
    assert json.loads(payload)["purchaseOrderId"] == po_id
