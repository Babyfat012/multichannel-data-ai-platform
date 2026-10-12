"""Test bảng ánh xạ của tầng Silver, không cần cụm Spark.

`spark_jobs/silver_mapping.py` cố tình không import pyspark, nên các quyết định nghiệp vụ
của Silver (trạng thái chuẩn, fulfillment method, mã quốc gia) kiểm được ngay trên máy host.

Mỗi test dưới đây gắn với một quyết định có ghi trong `docs/silver.md`, và chỗ nào nói về
dữ liệu nguồn thì đối chiếu thẳng với bảng ánh xạ trong `generator/channels/`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# `spark_jobs/` không phải package cài được, nên thêm vào sys.path như spark-submit làm.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))

from silver_mapping import (  # noqa: E402
    AMAZON_2026_STATUS,
    AMAZON_V0_STATUS,
    WALMART_STATUS,
    amazon_fulfillment_method,
    amazon_order_status,
    normalize_country,
    walmart_order_status,
)


class TestTrangThaiAmazon:
    """Hai dạng schema của Amazon có hai bảng trạng thái riêng, không gộp được."""

    def test_v0_va_2026_cung_bon_nghia(self):
        """Hai bảng phải phủ ĐÚNG cùng một tập trạng thái chuẩn."""
        assert set(AMAZON_V0_STATUS.values()) == set(AMAZON_2026_STATUS.values())

    def test_ep_chu_hoa_khong_gop_duoc_hai_cach_viet(self):
        """`Canceled` (v0, một chữ l) và `CANCELLED` (2026, hai chữ l) khác nhau.

        Đây là lý do phải có HAI bảng chứ không một bảng rồi `.upper()`: ép chữ hoa biến
        `Canceled` thành `CANCELED`, vẫn không khớp `CANCELLED`.
        """
        assert "Canceled" in AMAZON_V0_STATUS
        assert "CANCELLED" in AMAZON_2026_STATUS
        assert "Canceled".upper() not in AMAZON_2026_STATUS

    @pytest.mark.parametrize(
        "v0,expected",
        [
            ("Pending", "pending_payment"),
            ("Unshipped", "awaiting_shipment"),
            ("Shipped", "shipped"),
            ("Canceled", "cancelled"),
        ],
    )
    def test_dang_v0(self, v0, expected):
        assert amazon_order_status(v0, None) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("PENDING", "pending_payment"),
            ("UNSHIPPED", "awaiting_shipment"),
            ("SHIPPED", "shipped"),
            ("CANCELLED", "cancelled"),
        ],
    )
    def test_dang_2026(self, raw, expected):
        assert amazon_order_status(None, raw) == expected

    def test_amazon_khong_bao_gio_ra_delivered(self):
        """Amazon KHÔNG công bố 'đã giao': `Shipped` gộp shipped/delivered/completed.

        Khớp `generator/channels/amazon.py` bảng `STATUS_V0` và `STATUS_2026`, nơi cả ba
        trạng thái lõi đều ánh xạ về cùng một chữ. Hệ quả đã ghi ở `docs/silver.md` mục 6.
        """
        for table in (AMAZON_V0_STATUS, AMAZON_2026_STATUS):
            assert "delivered" not in table.values()
            assert "completed" not in table.values()

    def test_chu_la_thanh_none_chu_khong_nem_loi(self):
        """Chữ lạ phải hiện ra thành null để đếm được, không làm chết job."""
        assert amazon_order_status("KhongCoThat", None) is None
        assert amazon_order_status(None, "KHONG_CO_THAT") is None
        assert amazon_order_status(None, None) is None


class TestTrangThaiWalmart:
    """Walmart có `Delivered` thật, nhưng nó gộp hai trạng thái lõi."""

    def test_delivered_gop_hai_nghia_nen_chon_trang_thai_som_hon(self):
        """Nguồn ánh xạ cả DELIVERED và COMPLETED thành `"Delivered"`.

        `generator/channels/walmart.py` bảng `STATUS` có hai khoá lõi trỏ cùng một chữ, nên
        từ payload không phân biệt được. Chọn `delivered` vì mọi đơn `completed` đều đã đi
        qua `delivered` — nói `delivered` là nói một điều luôn đúng.
        """
        assert walmart_order_status("Delivered") == "delivered"
        assert "completed" not in WALMART_STATUS.values()

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Created", "pending_payment"),
            ("Acknowledged", "awaiting_shipment"),
            ("Shipped", "shipped"),
            ("Cancelled", "cancelled"),
        ],
    )
    def test_cac_trang_thai_con_lai(self, raw, expected):
        assert walmart_order_status(raw) == expected

    def test_chu_la_thanh_none(self):
        assert walmart_order_status("khong-co-that") is None
        assert walmart_order_status(None) is None


class TestFulfillmentMethod:
    """Một cột nên phải một bộ giá trị, dù hai dạng nguồn dùng hai bộ chữ."""

    @pytest.mark.parametrize("raw", ["AFN", "AMAZON"])
    def test_channel_giao(self, raw):
        assert amazon_fulfillment_method(raw) == "channel"

    @pytest.mark.parametrize("raw", ["MFN", "MERCHANT"])
    def test_seller_tu_giao(self, raw):
        assert amazon_fulfillment_method(raw) == "seller"

    def test_bon_chu_nguon_khong_trung_nghia_nhau(self):
        """Gộp hai dạng vào MỘT bảng chỉ an toàn khi không chữ nào trùng nghĩa khác."""
        assert amazon_fulfillment_method("khong-co-that") is None
        assert amazon_fulfillment_method(None) is None


class TestMaQuocGia:
    """`country` là cột chuẩn duy nhất, không có cột gốc đi kèm."""

    def test_usa_ve_us(self):
        assert normalize_country("USA") == "US"

    def test_ma_da_dung_thi_giu_nguyen(self):
        assert normalize_country("US") == "US"
        assert normalize_country("DE") == "DE"

    def test_ma_la_GIU_NGUYEN_chu_khong_thanh_none(self):
        """Khác trạng thái: ở đây giá trị lạ phải đi tiếp được và nhìn thấy được.

        Trạng thái có hai cột nên cột gốc còn chỗ trung thực; `country` chỉ có một cột, nên
        biến mã lạ thành null là mất luôn dữ liệu.
        """
        assert normalize_country("XX") == "XX"

    def test_none_van_la_none(self):
        assert normalize_country(None) is None
