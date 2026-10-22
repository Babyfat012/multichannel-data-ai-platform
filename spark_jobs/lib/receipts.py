"""Ghi schema thực tế và thời gian hoàn tất cho receipt, không phụ thuộc Spark."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def completed_at_utc() -> str:
    """Trả thời điểm hiện tại theo ISO-8601 UTC, kết thúc bằng Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _validate_type(schema, location):
    """Từ chối schema rỗng hoặc thiếu tên/kiểu field trước khi thay file cũ."""
    if isinstance(schema, str) and schema:
        return
    if not isinstance(schema, dict) or not schema.get("type"):
        raise ValueError(f"Schema thiếu type: {location}")
    kind = schema["type"]
    if kind == "struct":
        fields = schema.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ValueError(f"Schema struct cần fields không rỗng: {location}")
        for field in fields:
            if (
                not isinstance(field, dict)
                or not isinstance(field.get("name"), str)
                or not field["name"]
            ):
                raise ValueError(f"Schema field thiếu name: {location}")
            _validate_type(field.get("type"), f"{location}.{field['name']}")
    elif kind == "array":
        _validate_type(schema.get("elementType"), f"{location}[]")
    elif kind == "map":
        _validate_type(schema.get("keyType"), f"{location}.key")
        _validate_type(schema.get("valueType"), f"{location}.value")
    elif not isinstance(kind, str):
        raise ValueError(f"Schema type không hợp lệ: {location}")


def write_receipt_schemas(tables: dict, receipt_path: Path | str) -> str:
    """Ghi schemas.json cạnh receipt bằng rename atomic; giữ schema các job cùng run.

    Các job trong một thư mục receipt chạy tuần tự. Mỗi job truyền đúng các bảng
    vừa ghi dưới dạng {tên đầy đủ: df.schema.jsonValue()}, không lấy từ contract.
    """
    if not isinstance(tables, dict) or not tables:
        raise ValueError("Schema tables cần object không rỗng")
    for name, schema in tables.items():
        if not isinstance(name, str) or not name:
            raise ValueError("Schema bảng thiếu tên đầy đủ")
        if not isinstance(schema, dict) or schema.get("type") != "struct":
            raise ValueError(f"Schema bảng cần type struct: {name}")
        _validate_type(schema, name)

    destination = Path(receipt_path).parent / "schemas.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    previous = (
        json.loads(destination.read_text(encoding="utf-8"))["tables"]
        if destination.exists()
        else {}
    )
    document = {"tables": {**previous, **tables}}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".schemas-",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(document, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination.name
