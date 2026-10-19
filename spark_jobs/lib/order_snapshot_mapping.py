"""Bảng ánh xạ của tầng Silver — cố tình KHÔNG import pyspark để test được trên máy host.

Cùng thủ pháp với `day_selection.py`: mọi quyết định *nghiệp vụ* nào diễn tả được bằng
Python thuần thì để ở đây, còn file job chỉ còn phần Spark. Nhờ vậy `uv run pytest` kiểm
được các bảng ánh xạ mà không cần dựng cụm Spark.

Nội dung file này là phần "quy về ngôn ngữ chung" của `docs/silver.md`:
  * mục 6 — trạng thái có HAI cột: `channel_status` giữ y nguyên chữ nguồn, `order_status`
    là trạng thái chuẩn theo `CONTEXT.md:44`;
  * mục 8.3 — `fulfillment_method` là MỘT cột nên giá trị phải quy về một bộ chung;
  * mục 4.4 — `country` là MỘT cột nên Walmart `"USA"` ép về `"US"`.

Nguyên tắc chung cho cả ba: cột nào chỉ có MỘT bản thì cột đó phải là cột chuẩn. Trạng thái
là ngoại lệ duy nhất, vì nó có hai cột nên cột gốc còn chỗ để trung thực.
"""

from __future__ import annotations

# --- Trạng thái chuẩn, đúng danh sách CONTEXT.md:44 --------------------------------
PENDING_PAYMENT = "pending_payment"
AWAITING_SHIPMENT = "awaiting_shipment"
SHIPPED = "shipped"
DELIVERED = "delivered"
COMPLETED = "completed"
CANCELLED = "cancelled"
RETURNED = "returned"

#: Trạng thái gốc Amazon dạng v0 (`OrderStatus`) -> trạng thái chuẩn.
#: Nguồn: `generator/channels/amazon.py` bảng `STATUS_V0`.
#: Chú ý `"Shipped"` gộp ba trạng thái lõi (shipped / delivered / completed) — Amazon KHÔNG
#: công bố "đã giao" cho Seller, nên từ payload Amazon không bao giờ suy ra được `delivered`.
AMAZON_V0_STATUS: dict[str, str] = {
    "Pending": PENDING_PAYMENT,
    "Unshipped": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Canceled": CANCELLED,
}

#: Trạng thái gốc Amazon dạng 2026 (`fulfillment.status`) -> trạng thái chuẩn.
#: Nguồn: `generator/channels/amazon.py` bảng `STATUS_2026`.
#: Cùng bốn nghĩa như v0 nhưng viết CHỮ HOA, và `Canceled` -> `CANCELLED` (hai chữ `l`).
#: Ép chữ hoa KHÔNG gộp được hai cách viết này, nên phải là hai bảng riêng.
AMAZON_2026_STATUS: dict[str, str] = {
    "PENDING": PENDING_PAYMENT,
    "UNSHIPPED": AWAITING_SHIPMENT,
    "SHIPPED": SHIPPED,
    "CANCELLED": CANCELLED,
}

#: Trạng thái gốc Walmart (`orderLines.orderLine[].orderLineStatuses...status`) -> chuẩn.
#: Nguồn: `generator/channels/walmart.py` bảng `STATUS`.
#: GIỚI HẠN đã khai ở `docs/silver.md` mục 8.2: nguồn ánh xạ CẢ `DELIVERED` lẫn `COMPLETED`
#: thành cùng chữ `"Delivered"`, nên từ payload KHÔNG phân biệt được hai trạng thái đó.
#: Ở đây chọn `delivered` (trạng thái sớm hơn) vì mọi đơn `completed` đều đã đi qua
#: `delivered`, nên nói `delivered` là nói một điều luôn đúng; nói `completed` thì sai với
#: các đơn mới chỉ giao xong.
WALMART_STATUS: dict[str, str] = {
    "Created": PENDING_PAYMENT,
    "Acknowledged": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Delivered": DELIVERED,
    "Cancelled": CANCELLED,
}

# --- Fulfillment Method: một cột, một bộ giá trị ------------------------------------
#: Bên chịu trách nhiệm giao hàng, theo định nghĩa `CONTEXT.md:55-57`.
BY_CHANNEL = "channel"  # Channel giao (Amazon FBA / AFN)
BY_SELLER = "seller"  # Seller tự giao (Amazon FBM / MFN)

#: Hai dạng Amazon dùng HAI bộ chữ khác nhau cho cùng một nghĩa:
#:   v0   `FulfillmentChannel`      : "AFN" / "MFN"      (`amazon.py:135`)
#:   2026 `fulfillment.fulfilledBy` : "AMAZON" / "MERCHANT" (`amazon.py:178`)
#: Gộp vào MỘT bảng được vì bốn chữ này đều khác nhau, không chữ nào trùng nghĩa khác.
AMAZON_FULFILLMENT: dict[str, str] = {
    "AFN": BY_CHANNEL,
    "AMAZON": BY_CHANNEL,
    "MFN": BY_SELLER,
    "MERCHANT": BY_SELLER,
}

# --- country: một cột nên phải chuẩn (docs/silver.md mục 4.4) -----------------------
#: Mã quốc gia nguồn -> ISO 3166-1 alpha-2. Walmart ghi `"USA"` (`walmart.py:121`),
#: Amazon đã dùng mã 2 chữ sẵn.
COUNTRY_ALIASES: dict[str, str] = {"USA": "US"}


def normalize_country(raw: str | None) -> str | None:
    """Quy mã quốc gia về ISO 3166-1 alpha-2.

    Giá trị không có trong bảng alias thì trả về NGUYÊN VĂN, không đoán và không ném lỗi:
    một mã lạ đi tiếp được và nhìn thấy được ở Silver, còn ném lỗi thì cả job chết vì một
    dòng. Chỗ phát hiện mã lạ là kiểm tra chất lượng dữ liệu, không phải chỗ ép kiểu.

    >>> normalize_country("USA")
    'US'
    >>> normalize_country("US")
    'US'
    >>> normalize_country(None) is None
    True
    """
    if raw is None:
        return None
    return COUNTRY_ALIASES.get(raw, raw)


def amazon_order_status(v0_status: str | None, status_2026: str | None) -> str | None:
    """Trả trạng thái chuẩn cho một dòng Amazon, nhận cả hai dạng schema.

    Một dòng Bronze chỉ thuộc MỘT dạng, nên đúng một trong hai tham số có giá trị. Hàm
    nhận cả hai để chỗ gọi không phải biết dòng đó dạng nào.

    Trả `None` khi chữ nguồn không có trong bảng — cố ý không ném lỗi, vì một chữ lạ phải
    hiện ra ở cột `order_status` dưới dạng null để đếm được, chứ không làm chết job.

    >>> amazon_order_status("Unshipped", None)
    'awaiting_shipment'
    >>> amazon_order_status(None, "CANCELLED")
    'cancelled'
    >>> amazon_order_status("Canceled", None)
    'cancelled'
    >>> amazon_order_status(None, None) is None
    True
    """
    if v0_status is not None:
        return AMAZON_V0_STATUS.get(v0_status)
    if status_2026 is not None:
        return AMAZON_2026_STATUS.get(status_2026)
    return None


def walmart_order_status(raw: str | None) -> str | None:
    """Trả trạng thái chuẩn cho một dòng Walmart. `None` khi chữ nguồn lạ.

    >>> walmart_order_status("Acknowledged")
    'awaiting_shipment'
    >>> walmart_order_status("Delivered")
    'delivered'
    >>> walmart_order_status("khong-co-that") is None
    True
    """
    if raw is None:
        return None
    return WALMART_STATUS.get(raw)


def amazon_fulfillment_method(raw: str | None) -> str | None:
    """Trả `channel` / `seller` từ chữ gốc của một trong hai dạng Amazon.

    >>> amazon_fulfillment_method("AFN")
    'channel'
    >>> amazon_fulfillment_method("MERCHANT")
    'seller'
    >>> amazon_fulfillment_method(None) is None
    True
    """
    if raw is None:
        return None
    return AMAZON_FULFILLMENT.get(raw)
