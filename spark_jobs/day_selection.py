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
