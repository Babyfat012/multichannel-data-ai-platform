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


def test_seller_skus_cover_catalog_without_hidden_propensity(tiny_config):
    manifest = BatchRun(tiny_config).run()
    part = Path(tiny_config.storage.output_root) / "pg-load/seller.skus/part-00000.jsonl"
    rows = [json.loads(line) for line in part.read_text().splitlines()]
    assert len(rows) == tiny_config.scale.n_skus == manifest["datasets"]["seller.skus"]["rows"]
    assert {"sku", "category", "base_price_usd"} <= set(rows[0])
    assert "defect_propensity" not in rows[0]


def test_unique_keys_equals_primary_placements_so_the_key_set_is_unneeded(tiny_config):
    """Khóa bất biến cho phép `DatasetStats` đếm khóa duy nhất mà không giữ set khóa.

    `BatchRun` không còn gom `rendered.key` vào một set (ở quy mô 100 GB là ~117
    triệu string trong RAM) mà lấy `unique_keys` từ số placement `primary`. Test này
    dựng lại tập khóa thật từ file đầu ra và đối chiếu, nên nếu một Channel sau này
    sinh khóa phụ thuộc ngày export, hoặc `plan_exports` cho hơn một bản chính, thì
    test đổ chứ không phải `duplicate_rate` trong manifest âm thầm sai.
    """
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    key_fields = {"amazon.orders": ("AmazonOrderId", "orderId"), "walmart.orders": ("purchase_order_id",)}

    for dataset, pattern in [
        ("amazon.orders", "source-exports/channel=amazon/dataset=orders/export_date=*/part-00000.jsonl"),
        ("walmart.orders", "pg-load/walmart.orders/export_date=*/part-00000.jsonl"),
    ]:
        summary = manifest["datasets"][dataset]
        keys = {
            next(record[f] for f in key_fields[dataset] if f in record)
            for part in sorted(root.glob(pattern))
            for record in map(json.loads, part.read_text().splitlines())
        }
        assert len(keys) == summary["rows_by_placement"]["primary"], dataset
        assert len(keys) == summary["unique_keys"], dataset


def test_ground_truth_files_have_one_row_group_per_simulated_day_with_rows(tiny_config):
    """Nhãn và ánh xạ khóa được ghi dần, nên RAM theo một ngày chứ theo cả lần chạy."""
    BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)

    line_keys = pq.ParquetFile(root / "ground-truth/order_line_keys/amazon.parquet")
    assert 1 < line_keys.num_row_groups <= tiny_config.scale.days_history
    assert line_keys.metadata.num_rows > 0


def test_manifest_bytes_match_the_real_size_of_the_export_files(tiny_config):
    """`datasets[*].bytes` là thước đo ngưỡng 100 GB, nên phải bằng byte thật trên đĩa.

    Số byte giờ đến từ `JsonlWriterPool.close()` lúc đóng file, không còn từ một lần
    `write_jsonl` gom cả ngày. Test này là lý do dám dùng tổng đó làm bằng chứng
    đạt ngưỡng thay vì `du`.
    """
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    patterns = {
        "amazon.orders": "source-exports/channel=amazon/dataset=orders/export_date=*/part-00000.jsonl",
        "walmart.orders": "pg-load/walmart.orders/export_date=*/part-00000.jsonl",
        "seller.skus": "pg-load/seller.skus/part-00000.jsonl",
    }
    for dataset, pattern in patterns.items():
        parts = sorted(root.glob(pattern))
        assert parts, dataset
        assert manifest["datasets"][dataset]["bytes"] == sum(p.stat().st_size for p in parts), dataset
        assert manifest["datasets"][dataset]["files"] == len(parts), dataset
