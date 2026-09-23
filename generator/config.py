"""Đọc và kiểm tra file cấu hình YAML của generator.

Mỗi mục trong YAML (`scale`, `labels`, ...) ứng với một dataclass ở đây.
Đọc vào dataclass thay vì dùng dict thô có hai lợi ích:
- Gõ sai tên khóa (ví dụ `order_per_day`) sẽ báo lỗi ngay, không bị bỏ qua âm thầm.
- Code phía sau viết `config.scale.orders_per_day`, dễ đọc và IDE gợi ý được.

Chỉ những khóa generator đã dùng mới có mặt. Khóa của phần chưa làm
(streaming, drift) sẽ thêm khi làm tới phần đó.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from generator.core.storefronts import DEFAULT_STOREFRONT_WEIGHTS, STOREFRONTS


@dataclass(frozen=True)
class ScaleConfig:
    """Quy mô dữ liệu: số SKU, số ngày, số Order mỗi ngày."""

    n_skus: int = 2000
    start_date: date = date(2026, 1, 1)
    days_history: int = 180
    orders_per_day: int = 4000
    sku_zipf_exponent: float = 0.8


@dataclass(frozen=True)
class LabelConfig:
    """Tham số sinh nhãn Return/Refund."""

    window_days: int = 30


@dataclass(frozen=True)
class OfflineProblemsConfig:
    """Các vấn đề dữ liệu chèn vào Batch."""

    duplicate_rate: float = 0.02
    overlap_days: int = 3
    amazon_orders_api_switch_day: int = 100


@dataclass(frozen=True)
class PrimeDayConfig:
    """Một Sale Event kiểu Prime Day trên các Storefront Amazon.

    Thuộc tính:
        day: ngày mô phỏng bắt đầu sale.
        days: sale kéo dài mấy ngày.
        order_multiplier: số Order của Storefront Amazon gấp mấy lần ngày thường.
        discount_share: tỷ lệ Order Line Amazon được giảm giá trong ngày sale.
        deal_skus: số SKU được đẩy mạnh, hút phần lớn nhu cầu trong ngày sale.
    """

    day: int = 120
    days: int = 2
    order_multiplier: float = 4.0
    discount_share: float = 0.7
    deal_skus: int = 50

    def covers(self, day: int) -> bool:
        """Ngày mô phỏng `day` có nằm trong đợt sale không."""
        return self.day <= day < self.day + self.days


@dataclass(frozen=True)
class DriftConfig:
    """Các sự kiện làm đổi tham số của lõi theo lịch. `None` nghĩa là không có."""

    prime_day: PrimeDayConfig | None = None


@dataclass(frozen=True)
class StorageConfig:
    """Nơi ghi dữ liệu.

    `output_root` là URL của fsspec: đường dẫn thư mục local (`./out`)
    hoặc bucket S3/MinIO (`s3://...`). Code ghi file không cần biết là loại nào.
    """

    output_root: str = "./out"


@dataclass(frozen=True)
class GeneratorConfig:
    """Toàn bộ cấu hình của một lần chạy."""

    seed: int = 42
    scale: ScaleConfig = field(default_factory=ScaleConfig)
    storefront_weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_STOREFRONT_WEIGHTS))
    labels: LabelConfig = field(default_factory=LabelConfig)
    offline_problems: OfflineProblemsConfig = field(default_factory=OfflineProblemsConfig)
    drift: DriftConfig = field(default_factory=DriftConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    # --- Các mốc thời gian suy ra từ cấu hình -----------------------------

    def day_start(self, day: int) -> datetime:
        """Trả về 00:00 UTC của ngày mô phỏng `day` (ngày 1 là `start_date`)."""
        d = self.scale.start_date + timedelta(days=day - 1)
        return datetime.combine(d, time(0, 0), tzinfo=timezone.utc)

    @property
    def cutoff(self) -> datetime:
        """Mốc "hôm nay": hết ngày cuối cùng của lịch sử, tức 00:00 UTC ngày kế tiếp."""
        return self.day_start(self.scale.days_history + 1)

    def to_dict(self) -> dict[str, Any]:
        """Chuyển cấu hình về dict thuần để ghi vào `run_manifest.json`."""
        return _jsonable(dataclasses.asdict(self))


def load_config(path: str | Path) -> GeneratorConfig:
    """Đọc file YAML và trả về `GeneratorConfig` đã kiểm tra.

    Khóa nào không có trong YAML thì lấy giá trị mặc định của dataclass.
    Khóa lạ gây `ValueError` để phát hiện lỗi gõ sớm.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> GeneratorConfig:
    """Tạo `GeneratorConfig` từ dict (đã đọc từ YAML), rồi kiểm tra giá trị."""
    sections = {
        "scale": ScaleConfig,
        "labels": LabelConfig,
        "offline_problems": OfflineProblemsConfig,
        "storage": StorageConfig,
    }
    _reject_unknown(raw, {"seed", "storefront_weights", "drift", *sections}, where="gốc")

    kwargs: dict[str, Any] = {}
    if "seed" in raw:
        kwargs["seed"] = int(raw["seed"])
    for name, cls in sections.items():
        if name in raw:
            section = raw[name] or {}
            _reject_unknown(section, {f.name for f in dataclasses.fields(cls)}, where=name)
            kwargs[name] = cls(**section)
    if "storefront_weights" in raw:
        kwargs["storefront_weights"] = {k: float(v) for k, v in raw["storefront_weights"].items()}
    if "drift" in raw:
        kwargs["drift"] = _parse_drift(raw["drift"] or {})

    config = GeneratorConfig(**kwargs)
    _validate(config)
    return config


def _parse_drift(raw: dict[str, Any]) -> DriftConfig:
    """Đọc mục `drift`, mục duy nhất có dataclass lồng bên trong."""
    _reject_unknown(raw, {"prime_day"}, where="drift")
    prime_day = raw.get("prime_day")
    if prime_day is None:
        return DriftConfig()
    _reject_unknown(prime_day, {f.name for f in dataclasses.fields(PrimeDayConfig)}, where="drift.prime_day")
    return DriftConfig(prime_day=PrimeDayConfig(**prime_day))


def _reject_unknown(section: dict[str, Any], allowed: set[str], where: str) -> None:
    """Báo lỗi nếu `section` có khóa không nằm trong `allowed`."""
    unknown = set(section) - allowed
    if unknown:
        raise ValueError(f"Khóa không hợp lệ ở mục '{where}': {sorted(unknown)}")


def _validate(config: GeneratorConfig) -> None:
    """Kiểm tra các ràng buộc giữa các giá trị cấu hình."""
    weights = config.storefront_weights
    unknown = set(weights) - set(STOREFRONTS)
    if unknown:
        raise ValueError(f"Storefront không tồn tại: {sorted(unknown)}")
    if abs(sum(weights.values()) - 1.0) > 1e-6:
        raise ValueError(f"storefront_weights phải cộng lại bằng 1, đang là {sum(weights.values())}")
    if not 0 <= config.offline_problems.duplicate_rate < 0.5:
        raise ValueError("duplicate_rate phải nằm trong [0, 0.5)")
    if not 1 <= config.offline_problems.amazon_orders_api_switch_day <= config.scale.days_history + 1:
        raise ValueError("amazon_orders_api_switch_day phải nằm trong khoảng ngày lịch sử")

    sale = config.drift.prime_day
    if sale is not None:
        if sale.days < 1 or not 1 <= sale.day <= config.scale.days_history - sale.days + 1:
            raise ValueError("drift.prime_day phải nằm trọn trong khoảng ngày lịch sử")
        if sale.order_multiplier < 1:
            raise ValueError("drift.prime_day.order_multiplier phải từ 1 trở lên")
        if not 0 <= sale.discount_share <= 1:
            raise ValueError("drift.prime_day.discount_share phải nằm trong [0, 1]")
        if not 1 <= sale.deal_skus <= config.scale.n_skus:
            raise ValueError("drift.prime_day.deal_skus phải từ 1 tới n_skus")


def _jsonable(value: Any) -> Any:
    """Đổi `date`/`datetime` thành chuỗi ISO để `json.dumps` ghi được."""
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
