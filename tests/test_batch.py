"""Test chạy trọn một Batch nhỏ: file đầu ra, manifest và bảng nhãn."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pyarrow.parquet as pq

from generator.batch import BatchRun


def test_batch_writes_expected_layout_and_manifest(tiny_config):
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)

    amazon_parts = sorted(root.glob("source-exports/channel=amazon/dataset=orders/export_date=*/part-00000.jsonl"))
    walmart_parts = sorted(root.glob("pg-load/walmart.orders/export_date=*/part-00000.jsonl"))
    assert amazon_parts and walmart_parts
    assert json.loads((root / "run_manifest.json").read_text()) == manifest

    # Số dòng trong manifest khớp với số dòng thật trong file.
    amazon_rows = sum(len(p.read_text().splitlines()) for p in amazon_parts)
    assert manifest["datasets"]["amazon.orders"]["rows"] == amazon_rows
    assert manifest["datasets"]["amazon.orders"]["files"] == len(amazon_parts)


def test_amazon_partitions_before_switch_use_v0_and_after_use_new_schema(tiny_config):
    BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    switch_date = tiny_config.day_start(tiny_config.offline_problems.amazon_orders_api_switch_day).date().isoformat()
    for part in root.glob("source-exports/channel=amazon/dataset=orders/export_date=*/part-00000.jsonl"):
        export_date = part.parent.name.removeprefix("export_date=")
        first = json.loads(part.read_text().splitlines()[0])
        expected_key = "AmazonOrderId" if export_date < switch_date else "orderId"
        assert expected_key in first


def test_no_labels_yet_when_history_is_shorter_than_window(tiny_config):
    """12 ngày lịch sử ngắn hơn cửa sổ 30 ngày, nên chưa dòng nào biết nhãn."""
    BatchRun(tiny_config).run()
    table = pq.read_table(Path(tiny_config.storage.output_root) / "ground-truth/labels/labels.parquet")
    assert table.num_rows == 0
    assert str(table.schema.field("label").type) == "int8"


def test_labels_only_include_lines_known_before_cutoff(tiny_config):
    scale = dataclasses.replace(tiny_config.scale, days_history=40, orders_per_day=20)
    config = dataclasses.replace(tiny_config, scale=scale)
    BatchRun(config).run()
    table = pq.read_table(Path(config.storage.output_root) / "ground-truth/labels/labels.parquet")
    assert table.num_rows > 0
    assert set(table.column("label").to_pylist()) <= {0, 1}
    assert max(table.column("label_available_at").to_pylist()) <= config.cutoff


def test_same_config_produces_identical_files(tiny_config, tmp_path):
    other = dataclasses.replace(tiny_config, storage=dataclasses.replace(tiny_config.storage,
                                                                         output_root=str(tmp_path / "again")))
    BatchRun(tiny_config).run()
    BatchRun(other).run()
    first = Path(tiny_config.storage.output_root)
    second = Path(other.storage.output_root)
    for part in first.glob("source-exports/**/*.jsonl"):
        assert part.read_bytes() == (second / part.relative_to(first)).read_bytes()
