"""Khóa phiên bản Order Amazon dùng chung giữa dim SCD2 và Gold fact."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def amazon_order_version_key(order_id: Column, changed_at: Column) -> Column:
    """Băm đúng bộ mã Order và thời điểm trạng thái đổi trong UTC."""
    identity = F.struct(order_id.alias("order_id"), changed_at.alias("valid_from_ts"))
    return F.when(order_id.isNotNull() & changed_at.isNotNull(),
                  F.sha2(F.to_json(identity), 256))
