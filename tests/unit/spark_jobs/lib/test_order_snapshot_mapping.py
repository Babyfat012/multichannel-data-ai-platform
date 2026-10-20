"""Test bảng ánh xạ của tầng Silver, không cần cụm Spark.

Job Spark tra các dict này bằng `create_map`, nên `dict.get` ở đây đúng là phép tra của job.
"""

from __future__ import annotations

import pytest

from spark_jobs.lib.order_snapshot_mapping import (
    AMAZON_2026_STATUS,
    AMAZON_FULFILLMENT,
    AMAZON_V0_STATUS,
    COUNTRY_ALIASES,
    WALMART_STATUS,
)


class TestTrangThaiAmazon:
    """Hai dạng schema của Amazon có hai bảng trạng thái riêng."""

    def test_v0_va_2026_cung_bon_nghia(self):
        """Hai bảng phủ đúng cùng một tập trạng thái chuẩn."""
        assert set(AMAZON_V0_STATUS.values()) == set(AMAZON_2026_STATUS.values())

    def test_ep_chu_hoa_khong_gop_duoc_hai_cach_viet(self):
        """`Canceled` (v0) viết hoa thành `CANCELED`, vẫn khác `CANCELLED` (2026)."""
        assert "Canceled" in AMAZON_V0_STATUS
        assert "CANCELLED" in AMAZON_2026_STATUS
        assert "Canceled".upper() not in AMAZON_2026_STATUS

    @pytest.mark.parametrize(
        "table,raw,expected",
        [
            (AMAZON_V0_STATUS, "Pending", "pending_payment"),
            (AMAZON_V0_STATUS, "Unshipped", "awaiting_shipment"),
            (AMAZON_V0_STATUS, "Shipped", "shipped"),
            (AMAZON_V0_STATUS, "Canceled", "cancelled"),
            (AMAZON_2026_STATUS, "PENDING", "pending_payment"),
            (AMAZON_2026_STATUS, "UNSHIPPED", "awaiting_shipment"),
            (AMAZON_2026_STATUS, "SHIPPED", "shipped"),
            (AMAZON_2026_STATUS, "CANCELLED", "cancelled"),
        ],
    )
    def test_trang_thai_chuan(self, table, raw, expected):
        """Mỗi trạng thái Amazon ra đúng trạng thái chuẩn."""
        assert table[raw] == expected

    def test_amazon_khong_bao_gio_ra_delivered(self):
        """Amazon không báo "đã giao": `Shipped` gộp shipped/delivered/completed."""
        for table in (AMAZON_V0_STATUS, AMAZON_2026_STATUS):
            assert "delivered" not in table.values()
            assert "completed" not in table.values()


class TestTrangThaiWalmart:
    """Walmart có `Delivered` thật, nhưng nó gộp hai trạng thái lõi."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Created", "pending_payment"),
            ("Acknowledged", "awaiting_shipment"),
            ("Shipped", "shipped"),
            ("Delivered", "delivered"),
            ("Cancelled", "cancelled"),
        ],
    )
    def test_trang_thai_chuan(self, raw, expected):
        """Mỗi trạng thái Walmart ra đúng trạng thái chuẩn."""
        assert WALMART_STATUS[raw] == expected

    def test_delivered_chon_trang_thai_som_hon(self):
        """`Delivered` gộp delivered + completed; chọn `delivered` vì luôn đúng."""
        assert "completed" not in WALMART_STATUS.values()


def test_fulfillment_hai_dang_ve_mot_bo_gia_tri():
    """AFN/AMAZON là Channel giao, MFN/MERCHANT là Seller tự giao."""
    assert AMAZON_FULFILLMENT == {
        "AFN": "channel",
        "AMAZON": "channel",
        "MFN": "seller",
        "MERCHANT": "seller",
    }


def test_ma_quoc_gia():
    """`USA` quy về `US`; mã khác không có trong bảng nên job giữ nguyên."""
    assert COUNTRY_ALIASES == {"USA": "US"}
