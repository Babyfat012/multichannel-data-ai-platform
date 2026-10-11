"""Test cho `spark_jobs/day_selection.py` — cách chọn tập ngày export để nạp thử.

Vì sao test phần này: nó quyết định ảnh Spark UI có lộ skew hay không. Lấy ngày liền nhau
ở cuối dải thì mọi task to gần bằng nhau và bằng chứng skew biến mất, nên "rải đều" là
hành vi phải khoá lại bằng test, không phải chi tiết tuỳ ý.
"""

from __future__ import annotations

import pytest

from spark_jobs.day_selection import pick_sample_days

# 180 ngày giống thật: 2026-01-01 .. 2026-06-29.
DAYS = [f"d{i:03d}" for i in range(180)]


def test_lay_het_khi_n_bang_0_hoac_qua_lon():
    """n <= 0 và n >= số ngày đều nghĩa là 'nạp hết', trả về chính danh sách đầu vào."""
    assert pick_sample_days(DAYS, 0) == DAYS
    assert pick_sample_days(DAYS, -5) == DAYS
    assert pick_sample_days(DAYS, 180) == DAYS
    assert pick_sample_days(DAYS, 500) == DAYS


def test_dung_so_ngay_va_khong_trung_lap():
    """Lấy n ngày thì phải ra đúng n ngày khác nhau, giữ nguyên thứ tự tăng."""
    picked = pick_sample_days(DAYS, 20)
    assert len(picked) == 20
    assert len(set(picked)) == 20
    assert picked == sorted(picked)


def test_luon_gom_ngay_dau_va_ngay_cuoi():
    """Đây là điều kiện để skew lộ ra: ngày đầu dải nhỏ nhất, ngày cuối lớn nhất."""
    for n in (2, 3, 7, 20, 179):
        picked = pick_sample_days(DAYS, n)
        assert picked[0] == DAYS[0], n
        assert picked[-1] == DAYS[-1], n


def test_rai_deu_chu_khong_don_ve_mot_dau():
    """Khoảng cách giữa các ngày được chọn phải xấp xỉ nhau, lệch nhiều nhất 1 bậc.

    Nếu hàm đổi thành 'n ngày cuối' thì test này vỡ: mọi khoảng cách sẽ là 1 trừ một
    khoảng khổng lồ ở đầu.
    """
    picked = pick_sample_days(DAYS, 20)
    idx = [DAYS.index(d) for d in picked]
    gaps = [b - a for a, b in zip(idx, idx[1:])]
    assert max(gaps) - min(gaps) <= 1, gaps


def test_n_bang_1_lay_ngay_lon_nhat():
    """Chỉ một ngày thì lấy ngày cuối, vì đó là ngày nhiều dữ liệu nhất."""
    assert pick_sample_days(DAYS, 1) == [DAYS[-1]]


@pytest.mark.parametrize("n", [2, 5, 50])
def test_moi_ngay_duoc_chon_la_ngay_co_that(n):
    """Hàm chỉ được chọn lại từ danh sách đầu vào, không tự sinh ngày mới."""
    assert set(pick_sample_days(DAYS, n)) <= set(DAYS)
