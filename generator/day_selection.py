"""Chọn ngày export để nạp; logic thuần, không chạm PostgreSQL hay fsspec.

`pick_sample_days` trùng với `spark_jobs/lib/day_selection.py` có chủ ý: container Spark
không cài package `generator`, container generator không mount `spark_jobs/`. Sửa một bên
thì sửa cả bên kia; hai bên đều có test.
"""

from __future__ import annotations

DAY_MARKER = "export_date="


# ponytail: bản sao của spark_jobs/lib/day_selection.pick_sample_days (hai image không thấy nhau).
# Gộp khi generator và job Spark dùng chung một package.
def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Chọn `n` ngày rải đều, luôn gồm ngày đầu và ngày cuối; `n <= 0` hoặc quá dài là lấy hết.

    Rải đều vì dữ liệu lớn dần theo thời gian: lấy các ngày liền nhau ở cuối thì mẫu không
    đại diện cho cả dải.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def day_of(path: str) -> str | None:
    """Giá trị `export_date=` trong đường dẫn, hoặc None."""
    if DAY_MARKER not in path:
        return None
    return path.split(DAY_MARKER, 1)[1].split("/", 1)[0]


def filter_to_sample_days(paths: list[str], n: int) -> list[str]:
    """Giữ các file thuộc `n` ngày export rải đều; `n <= 0` là giữ hết.

    Lọc theo đường dẫn nên không phải mở file. Đường dẫn không có `export_date=` (vd
    `seller.skus`) luôn được giữ.
    """
    if n <= 0:
        return paths
    days = sorted({d for p in paths if (d := day_of(p)) is not None})
    if not days:
        return paths
    keep = set(pick_sample_days(days, n))
    return [p for p in paths if (d := day_of(p)) is None or d in keep]
