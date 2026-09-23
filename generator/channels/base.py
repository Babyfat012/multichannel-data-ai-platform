"""Phần dùng chung cho mọi renderer Channel.

Renderer làm một việc: nhận một `Order` của lõi cùng thời điểm chụp `as_of`,
và trả về bản ghi đúng dạng gốc của Channel tại thời điểm đó. Renderer không
rút số ngẫu nhiên. Mọi thứ "trông ngẫu nhiên" như mã đơn Amazon đều suy ra từ
mã chuẩn bằng hàm băm, nên cùng một Order luôn có cùng mã gốc ở mọi lần export,
ở cả Batch lẫn Stream.
"""

from __future__ import annotations

import string
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from generator.core.model import Order
from generator.core.rngs import stable_int

OBJECT_STORE = "object_store"  # MinIO, bucket source-exports
POSTGRES = "postgres"  # PostgreSQL mô phỏng hệ thống của phòng ban khác


@dataclass(frozen=True)
class RenderedRecord:
    """Một bản ghi đã ở dạng gốc của Channel.

    Thuộc tính:
        key: khóa dedup của Channel (ví dụ `AmazonOrderId`), dùng để đếm tỷ lệ trùng.
        record: nội dung sẽ ghi ra, là dict chuyển được sang JSON.
    """

    key: str
    record: dict[str, Any]


class ChannelRenderer(Protocol):
    """Giao diện mà renderer của mỗi Channel phải có."""

    channel: str
    dataset: str
    storage: str

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Vẽ `order` như Channel cho Seller thấy tại thời điểm `as_of`."""
        ...

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Trả về ánh xạ từ `order_line_id` chuẩn sang mã gốc của Channel."""
        ...


def native_digits(seed: int, kind: str, source_id: str, length: int) -> str:
    """Sinh chuỗi `length` chữ số cố định cho `source_id`.

    Ví dụ `native_digits(42, "amazon_order", "ORD-001-00001", 7)` luôn ra cùng
    7 chữ số. `kind` giúp hai loại mã sinh từ cùng một Order không trùng nhau.
    """
    value = stable_int(seed, kind, source_id)
    return str(value % 10**length).zfill(length)


def native_code(seed: int, kind: str, source_id: str, length: int) -> str:
    """Giống `native_digits` nhưng dùng cả chữ in hoa và số (ví dụ phần đuôi ASIN)."""
    alphabet = string.ascii_uppercase + string.digits
    value = stable_int(seed, kind, source_id)
    chars = []
    for _ in range(length):
        value, i = divmod(value, len(alphabet))
        chars.append(alphabet[i])
    return "".join(chars)


def iso_z(t: datetime) -> str:
    """Định dạng thời điểm UTC kiểu ISO 8601 có đuôi Z, ví dụ `2026-01-01T08:30:00Z`."""
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_millis(t: datetime) -> int:
    """Đổi thời điểm thành số mili giây kể từ 1970-01-01 UTC (kiểu Walmart dùng)."""
    return int(t.timestamp() * 1000)


def money(amount: float, decimals: int) -> str:
    """Định dạng số tiền thành chuỗi với số chữ số thập phân của tiền tệ."""
    return f"{amount:.{decimals}f}"
