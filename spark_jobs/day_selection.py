"""Chọn tập ngày export để nạp — phần logic thuần, không phụ thuộc Spark.

Tách khỏi `01_bronze_baseline.py` vì file job import `pyspark`, mà pyspark chỉ có trong
container Spark. Tách ra thì `uv run pytest` trên máy host test được phần quyết định này.
"""

from __future__ import annotations


def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Chọn `n` ngày rải đều trên danh sách ngày đã sắp xếp, luôn gồm ngày đầu và ngày cuối.

    Rải đều là có chủ đích. Dữ liệu Amazon lớn dần theo thời gian: ngày đầu dải ~7 MB,
    ngày cuối ~1,64 GB. Nếu lấy `n` ngày liền nhau ở cuối thì mọi task to gần bằng nhau và
    biểu đồ Spark UI KHÔNG lộ skew — mất đúng thứ mình cần chụp làm bằng chứng.

    `n <= 0` hoặc `n >= len(all_days)` nghĩa là lấy hết.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def pick_window_days(
    all_days: list[str], n: int, start: str | None = None
) -> list[str]:
    """Chọn `n` ngày LIỀN NHAU trên danh sách ngày đã sắp xếp.

    Ngược hẳn mục đích của `pick_sample_days`, và hai mục đích đó loại trừ nhau:

      * **rải đều** để thấy SKEW — cần cả ngày bé lẫn ngày to trong cùng một lần chạy;
      * **liền nhau** để thấy BẢN TRÙNG — generator xuất lại một Order ở các ngày trong
        khoảng `overlap_days = 3`, nên hai ngày cách nhau 9-10 ngày KHÔNG BAO GIỜ chứa cùng
        một Order. Đã đo: 20 ngày rải đều cho dedup bắt được đúng 0 bản trùng
        (`docs/silver.md` mục 10.3).

    Vì vậy muốn chứng minh dedup thì `n` phải lớn hơn cửa sổ chồng lấn: `n >= 7` phủ được
    `±3` ngày quanh một ngày ở giữa.

    `start` là ngày bắt đầu; để `None` thì lấy cửa sổ ở GIỮA dải. Lấy giữa chứ không lấy
    đầu hay cuối vì hai đầu dải là ca biên: đầu dải ngày quá bé (ít Order nên ít bản trùng
    tuyệt đối), cuối dải ngày quá to (tốn đĩa mà không thêm thông tin gì về bản trùng).

    Ném `ValueError` khi `start` không có trong danh sách — khác `pick_sample_days` vốn
    không có tham số nào sai được. Lý do ném chứ không lặng lẽ chọn ngày gần nhất: gõ sai
    ngày rồi chạy 20 phút ra bảng không như mong đợi thì tệ hơn là dừng ngay.

    >>> days = [f"2026-01-{d:02d}" for d in range(1, 11)]
    >>> pick_window_days(days, 3, start="2026-01-05")
    ['2026-01-05', '2026-01-06', '2026-01-07']
    >>> pick_window_days(days, 4)
    ['2026-01-04', '2026-01-05', '2026-01-06', '2026-01-07']
    >>> pick_window_days(days, 0)
    []
    >>> pick_window_days(days, 99) == days
    True
    """
    if n <= 0:
        return []
    if n >= len(all_days):
        return all_days
    if start is None:
        first = (len(all_days) - n) // 2
    else:
        if start not in all_days:
            raise ValueError(
                f"ngày bắt đầu {start!r} không có trong danh sách ngày trên đĩa"
            )
        first = all_days.index(start)
        # Cửa sổ chạm cuối dải thì lùi lại cho đủ `n` ngày, thay vì trả về ít hơn yêu cầu.
        first = min(first, len(all_days) - n)
    return all_days[first : first + n]
