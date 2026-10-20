"""Chọn ngày export để nạp; logic thuần, không phụ thuộc Spark nên test được trên host."""

from __future__ import annotations


def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Chọn `n` ngày rải đều, luôn gồm ngày đầu và ngày cuối; `n <= 0` hoặc quá dài là lấy hết.

    Rải đều để một lần chạy có cả ngày bé lẫn ngày to, nhờ đó Spark UI lộ skew.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def pick_window_days(all_days: list[str], n: int, start: str | None = None) -> list[str]:
    """Chọn `n` ngày liền nhau; `start=None` lấy cửa sổ ở giữa dải.

    Dùng để kiểm dedup: bản trùng chỉ nằm trong khoảng `overlap_days` quanh nhau, nên ngày rải
    đều không bắt được bản trùng nào. Cần `n >= 7`. `start` không có trong danh sách thì
    ném `ValueError`.

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
            raise ValueError(f"ngày bắt đầu {start!r} không có trong danh sách ngày trên đĩa")
        first = all_days.index(start)
        # Cửa sổ chạm cuối dải thì lùi lại cho đủ `n` ngày, thay vì trả về ít hơn yêu cầu.
        first = min(first, len(all_days) - n)
    return all_days[first : first + n]
