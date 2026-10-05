"""Lớp giao hàng của Batch: Order nào nằm ở ngày export nào, và chèn duplicate.

Quy tắc cơ bản (chưa có lỗi): mỗi Order xuất hiện **đúng một lần**, ở ngày
export chứa lần cập nhật cuối cùng của nó trước `cutoff`, với trạng thái lúc đó.
Nhờ vậy khi chưa chèn lỗi, tỷ lệ trùng theo khóa bằng 0.

Chèn duplicate (mô phỏng export chồng lấn: mỗi lần export lấy lại vài ngày gần
nhất để bắt cập nhật). Với xác suất `p`, Order có thêm **một** bản nữa:

- **Bản y hệt**: nằm ở ngày export muộn hơn 1 đến `overlap_days` ngày. Trạng thái
  không đổi vì lần cập nhật cuối đã xảy ra rồi, nên nội dung giống hệt.
- **Bản cũ hơn**: nằm ở ngày export sớm hơn 1 đến `overlap_days` ngày, chụp lúc
  Order còn ở trạng thái trước. Cùng khóa với bản chính nhưng thời điểm cập nhật
  cũ hơn. Dedup đúng phải giữ bản mới nhất theo thời điểm cập nhật, chứ không chỉ
  bỏ dòng giống hệt.

Chọn `p` thế nào để tỷ lệ trùng đo được bằng `r` (cấu hình `duplicate_rate`):
tỷ lệ trùng đo bằng `1 - số khóa khác nhau / số dòng`. Nếu N Order có thêm pN bản,
tỷ lệ là pN / (N + pN) = p / (1 + p). Muốn bằng r thì p = r / (1 - r).
Ví dụ r = 2% thì p ≈ 2,04%.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from generator.config import GeneratorConfig
from generator.core.model import Order

PRIMARY = "primary"
IDENTICAL_COPY = "identical_copy"
OLDER_VERSION = "older_version"


@dataclass(frozen=True)
class ExportPlacement:
    """Một lần Order xuất hiện trong dữ liệu Batch.

    Thuộc tính:
        export_day: ngày mô phỏng của lần export, quyết định partition `export_date`.
        as_of: thời điểm chụp trạng thái Order (cuối ngày export, không quá `cutoff`).
        kind: `primary`, `identical_copy` hoặc `older_version`.
    """

    export_day: int
    as_of: datetime
    kind: str


def day_of(config: GeneratorConfig, t: datetime) -> int:
    """Đổi một thời điểm UTC thành số thứ tự ngày mô phỏng (ngày 1 là `start_date`)."""
    return (t.date() - config.scale.start_date).days + 1


def end_of_day(config: GeneratorConfig, day: int) -> datetime:
    """Thời điểm cuối ngày `day` (23:59:59 UTC), là lúc chạy export của ngày đó."""
    return config.day_start(day + 1) - timedelta(seconds=1)


def plan_exports(order: Order, config: GeneratorConfig, rng: np.random.Generator) -> list[ExportPlacement]:
    """Trả về các lần Order xuất hiện trong Batch: bản chính và có thể một bản trùng.

    Ảnh chụp của Batch lấy ở giây cuối trước `cutoff`. Order tạo sau thời điểm đó
    chưa tồn tại lúc chụp nên không thuộc Batch, trả về danh sách rỗng.

    Mốc loại bỏ phải là `cutoff - 1s`, không phải `cutoff`: Order tạo trong giây
    cuối cùng của lịch sử nằm giữa hai mốc đó, lọt qua cửa rồi làm `status_at` ném
    lỗi "chưa tồn tại". Xác suất mỗi Order là 1/86.400 nên ở 4.000 Order/ngày hầu
    như không gặp, nhưng ở 852.099 Order/ngày thì trung bình ~10 Order mỗi lần chạy,
    tức lần chạy 100 GB gần như chắc chắn chết.
    """
    as_of = config.cutoff - timedelta(seconds=1)
    if order.created_at > as_of:
        return []

    last_change = order.status_at(as_of)
    primary_day = day_of(config, last_change.at)
    placements = [ExportPlacement(primary_day, end_of_day(config, primary_day), PRIMARY)]

    rate = config.offline_problems.duplicate_rate
    if rng.random() < rate / (1 - rate):
        duplicate = _pick_duplicate(order, primary_day, config, rng)
        if duplicate is not None:
            placements.append(duplicate)
    return placements


def _pick_duplicate(
    order: Order,
    primary_day: int,
    config: GeneratorConfig,
    rng: np.random.Generator,
) -> ExportPlacement | None:
    """Chọn chỗ đặt bản trùng: thử kiểu được bốc trước, không hợp lệ thì thử kiểu còn lại.

    Bản muộn hơn không được vượt quá ngày lịch sử cuối cùng. Bản sớm hơn không được
    trước ngày tạo Order. Nếu cả hai kiểu đều không đặt được thì bỏ qua, và
    manifest sẽ ghi số bản trùng thật sự đã chèn.
    """
    shift = int(rng.integers(1, config.offline_problems.overlap_days + 1))
    first_day = day_of(config, order.created_at)
    last_day = config.scale.days_history

    later_day = primary_day + shift if primary_day + shift <= last_day else None
    earlier_day = primary_day - shift if primary_day - shift >= first_day else None
    candidates = [later_day, earlier_day] if rng.random() < 0.5 else [earlier_day, later_day]

    for day in candidates:
        if day is None:
            continue
        as_of = end_of_day(config, day)
        same_state = order.status_at(as_of) == order.status_at(end_of_day(config, primary_day))
        return ExportPlacement(day, as_of, IDENTICAL_COPY if same_state else OLDER_VERSION)
    return None
