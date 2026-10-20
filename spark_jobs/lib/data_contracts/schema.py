"""Suy luận và kiểm tra tập con JSON Schema của registry."""


def _type(value):
    """Tên kiểu JSON, tránh coi boolean là integer."""
    return {
        type(None): "null",
        bool: "boolean",
        int: "integer",
        float: "number",
        str: "string",
        list: "array",
        dict: "object",
    }[type(value)]


def infer_schema(records):
    """Hợp kiểu quan sát; required là giao các khóa trong mọi object quan sát."""
    values = list(records)
    if not values:
        return {}
    types = sorted({_type(v) for v in values})
    result = {"type": types[0] if len(types) == 1 else types}
    objects = [v for v in values if isinstance(v, dict)]
    if objects:
        keys = sorted(set().union(*(v.keys() for v in objects)))
        result["properties"] = {k: infer_schema(v[k] for v in objects if k in v) for k in keys}
        result["required"] = sorted(set.intersection(*(set(v) for v in objects)))
    arrays = [v for v in values if isinstance(v, list)]
    if arrays:
        result["items"] = infer_schema(item for array in arrays for item in array)
    return result


def validate_record(record, schema, path="$"):
    """Kiểm mọi phần tử, trả đường dẫn thiếu required hoặc sai kiểu."""
    errors = []
    types = schema.get("type", [])
    types = [types] if isinstance(types, str) else types
    actual = _type(record)
    if types and actual not in types and not (actual == "integer" and "number" in types):
        return [f"{path}: type {actual}, expected {types}"]
    if isinstance(record, dict):
        errors.extend(
            f"{path}.{k}: missing required" for k in schema.get("required", []) if k not in record
        )
        for key, child in schema.get("properties", {}).items():
            if key in record:
                errors.extend(validate_record(record[key], child, f"{path}.{key}"))
    if isinstance(record, list):
        for i, value in enumerate(record):
            errors.extend(validate_record(value, schema.get("items", {}), f"{path}[{i}]"))
    return errors


def required_json_paths(schema, path="$"):
    """Đường dẫn bắt buộc cho Spark; mảng chỉ kiểm phần tử đầu ([0]).

    Không đi vào object tùy chọn hoặc trường cho phép null; empty array không đòi item.
    Spark dùng thêm điều kiện array rỗng để tránh coi mảng hợp lệ là thiếu phần tử.
    """
    paths = []
    for key in schema.get("required", []):
        child = schema.get("properties", {}).get(key, {})
        child_path = f"{path}.{key}"
        types = child.get("type", [])
        if types == "null" or "null" in (types if isinstance(types, list) else []):
            continue
        paths.append(child_path)
        paths.extend(required_json_paths(child, child_path))
    if "items" in schema:
        paths.extend(required_json_paths(schema["items"], path + "[0]"))
    return paths
