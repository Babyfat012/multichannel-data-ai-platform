"""Chọn tập ngày export để nạp — phần logic thuần, không chạm PostgreSQL hay fsspec.

Vì sao file này TRÙNG `pick_sample_days` với `spark_jobs/day_selection.py`:
hai bên không import được nhau, dù code giống nhau.

  * `spark_jobs/` chỉ được **mount** vào container Spark (`/opt/spark_jobs:ro`) và container
    đó KHÔNG cài package `generator`. Cố ý vậy: job Spark là mấy file `.py` trần, không có
    gì để build, và mount thì sửa xong chạy ngay thay vì chờ build image 40 giây.
  * Ngược lại, container `generator-load` cài package `generator` nhưng KHÔNG mount
    `spark_jobs/`. Nên `from spark_jobs...` ở đây sẽ chạy được trên máy host mà vỡ trong
    Docker — kiểu lỗi chỉ lộ ra lúc deploy.

Sáu dòng trùng rẻ hơn một phụ thuộc vòng giữa hai thứ được đóng gói khác nhau. Đổi hành vi
ở một bên thì phải đổi bên kia; cả hai bên đều có test khoá hành vi lại.
"""

from __future__ import annotations

DAY_MARKER = "export_date="


def pick_sample_days(all_days: list[str], n: int) -> list[str]:
    """Chọn `n` ngày rải đều trên danh sách ngày đã sắp xếp, luôn gồm ngày đầu và ngày cuối.

    Rải đều là có chủ đích. Dữ liệu lớn dần theo thời gian: ngày Walmart đầu dải ~872 KB,
    ngày cuối ~261 MB. Nếu lấy `n` ngày liền nhau ở cuối thì mọi ngày to gần bằng nhau và
    tập mẫu KHÔNG còn đại diện cho dải thật — mất đúng thứ cần để đo.

    `n <= 0` hoặc `n >= len(all_days)` nghĩa là lấy hết.
    """
    if n <= 0 or n >= len(all_days):
        return all_days
    if n == 1:
        return [all_days[-1]]
    step = (len(all_days) - 1) / (n - 1)
    return [all_days[round(i * step)] for i in range(n)]


def day_of(path: str) -> str | None:
    """Giá trị `export_date` nằm trong đường dẫn, hoặc None nếu đường dẫn không có mốc đó.

    Quy ước thư mục `cột=giá trị` là của Hive/Spark, và generator ghi theo đúng quy ước đó,
    nên ngày nằm ngay trong tên thư mục chứ không cần mở file ra đọc.
    """
    if DAY_MARKER not in path:
        return None
    return path.split(DAY_MARKER, 1)[1].split("/", 1)[0]


def filter_to_sample_days(paths: list[str], n: int) -> list[str]:
    """Giữ lại các file thuộc `n` ngày export rải đều; `n <= 0` là giữ hết.

    Lọc ở mức ĐƯỜNG DẪN, không ở mức dòng: ngày nằm trong tên thư mục nên loại được cả
    file mà không cần mở ra đọc. Với 180 ngày / 3.256 file thì đây là chênh lệch giữa
    đọc 1,4 GB và đọc 13 GB.

    Đường dẫn KHÔNG chứa `export_date=` thì luôn được giữ. Đó là chủ ý: `seller.skus` là
    danh mục SKU, không chia theo ngày, và lọc nó đi sẽ làm bảng SKU rỗng.
    """
    if n <= 0:
        return paths
    days = sorted({d for p in paths if (d := day_of(p)) is not None})
    if not days:
        return paths
    keep = set(pick_sample_days(days, n))
    return [p for p in paths if (d := day_of(p)) is None or d in keep]
