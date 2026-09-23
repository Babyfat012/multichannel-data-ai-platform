"""Test đọc và kiểm tra cấu hình."""

from __future__ import annotations

from pathlib import Path

import pytest

from generator.config import load_config, parse_config


def test_empty_config_uses_design_defaults():
    config = parse_config({})
    assert config.scale.n_skus == 2000
    assert config.storefront_weights["amazon_us"] == 0.45
    assert sum(config.storefront_weights.values()) == pytest.approx(1.0)


def test_unknown_key_is_rejected():
    with pytest.raises(ValueError, match="order_per_day"):
        parse_config({"scale": {"order_per_day": 10}})


def test_storefront_weights_must_sum_to_one():
    with pytest.raises(ValueError, match="cộng lại bằng 1"):
        parse_config({"storefront_weights": {"amazon_us": 0.5, "walmart_us": 0.2}})


def test_unknown_storefront_is_rejected():
    with pytest.raises(ValueError, match="không tồn tại"):
        parse_config({"storefront_weights": {"ebay_us": 1.0}})


def test_cutoff_is_midnight_after_last_history_day():
    config = parse_config({"scale": {"days_history": 180}})
    assert config.cutoff.isoformat() == "2026-06-30T00:00:00+00:00"


@pytest.mark.parametrize("name", ["small.yaml", "default.yaml"])
def test_shipped_config_files_are_valid(name):
    load_config(Path(__file__).parent.parent / "configs" / name)


@pytest.mark.parametrize("prime_day, message", [
    ({"day": 180, "days": 2}, "ngày lịch sử"),
    ({"day": 0}, "ngày lịch sử"),
    ({"day": 10, "discount_share": 1.5}, "discount_share"),
    ({"day": 10, "deal_skus": 5000}, "deal_skus"),
    ({"day": 10, "dealskus": 5}, "drift.prime_day"),
])
def test_invalid_prime_day_is_rejected(prime_day, message):
    with pytest.raises(ValueError, match=message):
        parse_config({"scale": {"days_history": 180}, "drift": {"prime_day": prime_day}})


def test_drift_is_optional_and_defaults_to_no_sale():
    assert parse_config({}).drift.prime_day is None
