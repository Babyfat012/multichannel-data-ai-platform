"""Fixture dùng chung cho các test: cấu hình rất nhỏ để test chạy trong vài giây."""

from __future__ import annotations

import pytest

from generator.config import GeneratorConfig, parse_config


@pytest.fixture
def tiny_config(tmp_path) -> GeneratorConfig:
    """Cấu hình 20 SKU, 12 ngày, 60 Order mỗi ngày, ghi vào thư mục tạm của test."""
    return parse_config({
        "seed": 7,
        "scale": {"n_skus": 20, "days_history": 12, "orders_per_day": 60},
        "offline_problems": {"duplicate_rate": 0.02, "overlap_days": 3, "amazon_orders_api_switch_day": 6},
        "storage": {"output_root": str(tmp_path / "out")},
    })
