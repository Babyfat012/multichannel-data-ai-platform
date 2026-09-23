"""Tạo bộ sinh số ngẫu nhiên tái lập được cho từng phần của generator.

Vấn đề: nếu cả chương trình dùng chung một `np.random.Generator`, thì chỉ cần
thêm một lần rút số ở phần A là mọi số ở phần B phía sau đổi theo. Khi đó cùng
seed nhưng sửa code một chút là ra dữ liệu khác hẳn, khó so sánh.

Cách làm: mỗi phần xin một bộ sinh riêng, đặt tên theo phần đó, ví dụ
`rng_for(seed, "orders", 17)` cho các Order của ngày 17. Bộ sinh được tạo từ
(seed, tên), nên:
- Cùng seed và cùng tên luôn cho cùng dãy số.
- Ngày 17 không phụ thuộc ngày 16 đã rút bao nhiêu số, nên sinh từng ngày
  độc lập được (ít tốn RAM, chạy song song được về sau).
"""

from __future__ import annotations

import hashlib

import numpy as np


def rng_for(seed: int, *names: str | int) -> np.random.Generator:
    """Trả về bộ sinh số riêng cho phần có tên `names`, suy ra từ `seed`.

    Tên được băm thành số nguyên rồi ghép với seed, vì numpy chỉ nhận số làm seed.
    """
    return np.random.default_rng([seed, stable_int(*names)])


def stable_int(*names: str | int) -> int:
    """Băm các tên thành một số nguyên 64 bit, giống nhau giữa các lần chạy.

    Không dùng `hash()` của Python vì hàm đó đổi giá trị mỗi lần chạy với chuỗi.
    """
    key = "/".join(str(n) for n in names).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")
