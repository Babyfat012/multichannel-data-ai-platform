"""Test báo cáo chất lượng: đo lại từ file phải khớp đáp án trong manifest."""

from __future__ import annotations

import pytest

from generator.generate_order_snapshots import BatchRun
from generator.report import QualityReport, render_markdown, render_text


@pytest.fixture
def report(tiny_config):
    """Báo cáo trên một Batch nhỏ, đọc từ thư mục local (không cần MinIO hay PostgreSQL)."""
    BatchRun(tiny_config).run()
    return QualityReport(tiny_config.storage.output_root)


def test_duplicate_rate_matches_manifest_and_is_zero_after_dedup(report):
    """Chứng minh tỷ lệ trùng đo lại khớp đáp án trong manifest, và về 0 sau khi giữ bản mới nhất theo khóa."""
    for row in report.duplicates().as_dicts():
        assert row["dup_pct_before"] == row["dup_pct_truth"]
        assert row["dup_pct_after"] == 0
        assert row["rows_after"] == row["distinct_keys"]


def test_exported_orders_match_truth_for_rendered_storefronts(report):
    """Chứng minh số Order xuất ra khớp số thật của lõi với các Storefront đã có renderer."""
    for row in report.storefront_skew().as_dicts():
        if row["orders_exported"] is not None:
            assert row["orders_exported"] == row["orders_truth"]


def test_schema_evolution_nulls_flip_at_switch(report):
    """Chứng minh cột `proceeds` và `IsPrime` null đảo chiều đúng tại mốc đổi schema."""
    before, after = sorted(report.schema_evolution().as_dicts(), key=lambda r: r["period"], reverse=True)
    assert before["pct_null_proceeds"] == 100 and before["pct_null_IsPrime"] == 0
    assert after["pct_null_proceeds"] == 0 and after["pct_null_IsPrime"] == 100


def test_category_shares_sum_to_100(report):
    """Chứng minh tỷ lệ Order theo danh mục cộng lại đúng 100%."""
    assert sum(r["pct"] for r in report.category_skew().as_dicts()) == pytest.approx(100, abs=0.1)


def test_every_table_renders(report):
    """Chứng minh mọi bảng báo cáo render được ra cả text lẫn markdown."""
    for table in report.all_tables():
        assert table.title in render_text(table)
        assert render_markdown(table).startswith(f"#### {table.title}")
