"""Test cho `generator/lib/day_selection.py` — lọc file nguồn xuống N ngày export.

Vì sao test phần này: nó đứng giữa 13 GB và 1,4 GB dữ liệu đưa vào PostgreSQL. Hai hành vi
dễ làm sai mà hậu quả lặng lẽ:

  * Lọc sai hướng -> `seller.skus` bị loại sạch, bảng SKU rỗng, và không có lỗi nào báo.
  * Lấy N ngày liền nhau ở cuối thay vì rải đều -> tập mẫu toàn ngày to, không đại diện.

`spark_jobs/lib/day_selection.py` có bản `pick_sample_days` trùng nội dung, test riêng ở
`tests/unit/spark_jobs/lib/test_day_selection.py`. Lý do trùng ghi trong docstring của module.
"""

from __future__ import annotations

import pytest

from generator.day_selection import day_of, filter_to_sample_days, pick_sample_days

DAYS = [f"2026-{1 + i // 30:02d}-{1 + i % 30:02d}" for i in range(180)]
WALMART = [
    f"pg-load/walmart.orders/export_date={d}/part-sim{i:05d}.jsonl" for i, d in enumerate(DAYS)
]
SKUS = ["pg-load/seller.skus/part-00000.jsonl"]


def test_pick_khop_voi_ban_trong_spark_jobs():
    """Hai bản trùng code phải cho cùng kết quả, nếu không lệch âm thầm giữa hai container."""
    from spark_jobs.lib.day_selection import pick_sample_days as spark_pick

    for n in (0, 1, 2, 20, 180, 500):
        assert pick_sample_days(DAYS, n) == spark_pick(DAYS, n), n


def test_day_of_lay_ngay_tu_ten_thu_muc():
    """Ngày nằm trong tên thư mục `export_date=...`, không phải trong nội dung file."""
    assert day_of(WALMART[0]) == DAYS[0]
    assert day_of(SKUS[0]) is None


def test_sample_days_0_giu_het():
    """Mặc định 0 là 'nạp hết' — hành vi cũ không được đổi."""
    paths = WALMART + SKUS
    assert filter_to_sample_days(paths, 0) == paths
    assert filter_to_sample_days(paths, -3) == paths


def test_giu_dung_n_ngay_phan_biet():
    """20 ngày yêu cầu phải ra đúng 20 ngày khác nhau trong số file còn lại."""
    kept = filter_to_sample_days(WALMART, 20)
    assert len({day_of(p) for p in kept}) == 20


def test_luon_con_ngay_dau_va_ngay_cuoi():
    """Ngày đầu nhỏ nhất (~872 KB) và ngày cuối lớn nhất (~261 MB) — cần cả hai đầu dải."""
    kept_days = {day_of(p) for p in filter_to_sample_days(WALMART, 20)}
    assert DAYS[0] in kept_days
    assert DAYS[-1] in kept_days


def test_seller_skus_khong_bao_gio_bi_loai():
    """File không có `export_date=` luôn được giữ, kể cả khi lọc rất chặt.

    Nếu test này vỡ thì bảng `seller.skus` sẽ rỗng sau khi nạp, mà không có lỗi nào báo.
    """
    for n in (1, 2, 20):
        assert filter_to_sample_days(WALMART + SKUS, n)[-1:] == SKUS, n


def test_chi_loai_bo_chu_khong_them_duong_dan_moi():
    """Hàm chỉ được lọc; không tự sinh đường dẫn, không đổi thứ tự các file còn lại."""
    paths = WALMART + SKUS
    kept = filter_to_sample_days(paths, 20)
    assert set(kept) <= set(paths)
    assert kept == [p for p in paths if p in set(kept)]


@pytest.mark.parametrize("n", [1, 2, 7, 20, 60])
def test_khong_bao_gio_giu_nhieu_hon_ca_dai(n):
    """Số file giữ lại không được vượt số file ban đầu, với mọi n."""
    assert len(filter_to_sample_days(WALMART, n)) <= len(WALMART)


def test_khong_co_ngay_nao_thi_giu_het():
    """Nguồn không chia theo ngày (chỉ có seller.skus) thì lọc là no-op, không trả rỗng."""
    assert filter_to_sample_days(SKUS, 20) == SKUS
