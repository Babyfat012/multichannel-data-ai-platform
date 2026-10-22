"""Chính sách Iceberg thuần Python: kế hoạch, ngưỡng, retention và retry có receipt."""

from __future__ import annotations

import json
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spark_jobs.lib.return_label import utc

DEFAULTS = {
    "min_input_files": 5,
    "small_file_ratio": 0.5,
    "target_file_size_bytes": 134217728,
    "zorder_columns": [],
    "zorder_min_new_files": 20,
    "max_snapshot_age_hours": 168,
    "retain_last": 3,
    "generation_tag_prefix": "gen-",
    "orphans_dry_run": True,
    "orphan_age_hours": 72,
    "max_attempts": 3,
    "backoff_seconds": 1,
    "max_backoff_seconds": 8,
    "probe_predicates": [],
}


def validated_config(config):
    """Chuẩn hoá cấu hình, từ chối retention/retry không an toàn và cột không hợp lệ."""
    result = {**DEFAULTS, **config}
    for key in (
        "min_input_files",
        "target_file_size_bytes",
        "zorder_min_new_files",
        "retain_last",
        "max_attempts",
    ):
        if type(result[key]) is not int or result[key] < 1:
            raise ValueError(f"{key} phải là số nguyên dương")
    for key in (
        "small_file_ratio",
        "max_snapshot_age_hours",
        "orphan_age_hours",
        "backoff_seconds",
        "max_backoff_seconds",
    ):
        if (
            type(result[key]) not in (int, float)
            or not math.isfinite(result[key])
            or result[key] < 0
        ):
            raise ValueError(f"{key} không hợp lệ")
    if not 0 < result["small_file_ratio"] <= 1 or result["orphan_age_hours"] < 72:
        raise ValueError("ratio phải trong (0,1]; orphan_age_hours phải >=72")
    if type(result["orphans_dry_run"]) is not bool:
        raise ValueError("orphans_dry_run phải là boolean")
    if not isinstance(result["generation_tag_prefix"], str) or not result["generation_tag_prefix"]:
        raise ValueError("generation_tag_prefix phải khác rỗng")
    if not isinstance(result["zorder_columns"], list) or any(
        not isinstance(column, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column)
        for column in result["zorder_columns"]
    ):
        raise ValueError("zorder_columns phải là danh sách tên cột đơn")
    if not isinstance(result["probe_predicates"], list):
        raise ValueError("probe_predicates phải là danh sách")
    for probe in result["probe_predicates"]:
        if not isinstance(probe, dict) or not probe.get("name") or not probe.get("ranges"):
            raise ValueError("probe cần name và ranges (AND các khoảng đóng)")
        for column, bounds in probe["ranges"].items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) or len(bounds) != 2:
                raise ValueError("Khoảng probe không hợp lệ")
            if bounds[0] is not None and bounds[1] is not None and bounds[0] > bounds[1]:
                raise ValueError("Khoảng probe bị đảo")
    return result


def load_config(path):
    """Đọc JSON cấu hình theo bảng; không suy đoán bảng thật khi thiếu cấu hình."""
    data = json.loads(Path(path).read_text())
    if not isinstance(data.get("tables"), dict) or not data["tables"]:
        raise ValueError("Cấu hình cần tables khác rỗng")
    return {table: validated_config(config) for table, config in data["tables"].items()}


def plan_maintenance(metadata, config, now=None):
    """Trả hành động có lý do; orphan dry-run luôn có, no_op chỉ xét thay đổi bảng.

    Brief đồng thời yêu cầu orphan luôn lập kế hoạch và actions lần hai rỗng.
    Giữ nguyên luật an toàn orphan: no_op=true nghĩa là không có mutation,
    actions vẫn chứa đúng cuộc kiểm kê orphan dry-run.
    """
    config = validated_config(config)
    now = utc(now or datetime.now(timezone.utc))
    actions = []
    grouped = defaultdict(list)
    for file in metadata.get("files", []):
        if file.get("content", 0) == 0:
            grouped[json.dumps(file.get("partition", {}), sort_keys=True, default=str)].append(file)
    partitions = [
        json.loads(key)
        for key, files in grouped.items()
        if len(files) >= config["min_input_files"]
        and sum(file["file_size_in_bytes"] for file in files) / len(files)
        < config["small_file_ratio"] * config["target_file_size_bytes"]
    ]
    if partitions:
        actions.append(
            {
                "kind": "compact",
                "partitions": partitions,
                "target_file_size_bytes": config["target_file_size_bytes"],
                "min_input_files": config["min_input_files"],
                "reason": "Partition có đủ nhiều file và size trung bình dưới ngưỡng",
            }
        )
    snapshots = sorted(metadata.get("snapshots", []), key=lambda s: utc(s["committed_at"]))
    marker = metadata.get("properties", {}).get("fsds.maintenance.last_zorder_snapshot_id")
    by_id = {str(s["snapshot_id"]): s for s in snapshots}
    # Đi theo parent để không đếm file trên nhánh không thuộc trạng thái hiện hành.
    cursor = str(
        metadata.get("current_snapshot_id", snapshots[-1]["snapshot_id"] if snapshots else "")
    )
    added, seen = 0, set()
    while cursor in by_id and cursor != str(marker) and cursor not in seen:
        seen.add(cursor)
        snapshot = by_id[cursor]
        added += int(snapshot.get("summary", {}).get("added-data-files", 0))
        cursor = str(snapshot.get("parent_id"))
    marker_missing = marker is None or cursor != str(marker)
    if config["zorder_columns"] and (marker_missing or added >= config["zorder_min_new_files"]):
        actions.append(
            {
                "kind": "zorder",
                "columns": config["zorder_columns"],
                "target_file_size_bytes": config["target_file_size_bytes"],
                "new_data_files": added,
                "reason": "Chưa có mốc z-order/không còn ancestry mốc"
                if marker_missing
                else "Đủ data file mới từ mốc z-order",
            }
        )
    protected = sorted({int(ref["snapshot_id"]) for ref in metadata.get("refs", [])})
    recent = {s["snapshot_id"] for s in snapshots[-config["retain_last"] :]}
    cutoff = now - timedelta(hours=config["max_snapshot_age_hours"])
    expired = [
        s["snapshot_id"]
        for s in snapshots
        if utc(s["committed_at"]) < cutoff
        and s["snapshot_id"] not in recent
        and int(s["snapshot_id"]) not in protected
    ]
    if expired:
        actions.append(
            {
                "kind": "expire_snapshots",
                "snapshot_ids": expired,
                "older_than": cutoff.isoformat(),
                "retain_last": config["retain_last"],
                "reason": "Snapshot quá tuổi, ngoài retain_last và không bị ref bảo vệ",
            }
        )
    actions.append(
        {
            "kind": "remove_orphans",
            "dry_run": config["orphans_dry_run"],
            "older_than": (now - timedelta(hours=config["orphan_age_hours"])).isoformat(),
            "reason": "Kiểm kê orphan mặc định không xoá; cutoff ít nhất 72 giờ",
        }
    )
    no_op = all(a["kind"] == "remove_orphans" and a["dry_run"] for a in actions)
    return {
        "actions": actions,
        "protected_snapshot_ids": protected,
        "generation_tags": [
            r["name"]
            for r in metadata.get("refs", [])
            if r.get("type", "").lower() == "tag"
            and r["name"].startswith(config["generation_tag_prefix"])
        ],
        "no_op": no_op,
        "reason": "no-op: không hành động thay đổi bảng; chỉ kiểm kê orphan"
        if no_op
        else "Có ngưỡng bảo trì bị vượt",
    }


def is_transient(error):
    """Chỉ retry commit conflict và ValidationException có dấu hiệu ghi đồng thời."""
    message = f"{type(error).__name__}: {error}"
    return "CommitFailedException" in message or (
        "ValidationException" in message
        and any(
            token in message.lower()
            for token in (
                "concurrent",
                "conflicting",
                "found new",
                "cannot commit",
                "changed",
                "already been committed",
            )
        )
    )


def execute_actions(actions, execute, config, attempts, sleep=time.sleep):
    """Thi hành tuần tự; ghi mọi lần thử kể cả lỗi rồi ném lại và dừng ngay."""
    config = validated_config(config)
    for action in actions:
        for number in range(1, config["max_attempts"] + 1):
            attempt = {"kind": action["kind"], "attempt": number}
            attempts.append(attempt)
            try:
                result = execute(action)
                attempt.update(status="success", result=result)
                break
            except Exception as error:
                transient = is_transient(error)
                attempt.update(
                    status="failed", transient=transient, error=f"{type(error).__name__}: {error}"
                )
                if not transient or number == config["max_attempts"]:
                    raise
                delay = min(
                    config["max_backoff_seconds"], config["backoff_seconds"] * 2 ** (number - 1)
                )
                attempt["backoff_seconds"] = delay
                sleep(delay)


def count_probe_files(files, ranges):
    """Đếm file có giao tất cả khoảng đóng; thiếu metrics thì luôn phải đọc.

    Đây là ước lượng bằng min/max, không phải số task Spark hay số file đọc thực tế.
    Ngày/giờ cấu hình ISO được đổi cùng kiểu với metrics trước khi so sánh.
    """
    count = 0
    for file in files:
        overlaps = True
        metrics = file.get("readable_metrics") or {}
        for column, (low, high) in ranges.items():
            metric = metrics.get(column) or {}
            lower, upper = metric.get("lower_bound"), metric.get("upper_bound")
            example = lower if lower is not None else upper
            if isinstance(example, datetime):
                lower = utc(lower) if lower is not None else None
                upper = utc(upper) if upper is not None else None
                low = utc(low) if low is not None else None
                high = utc(high) if high is not None else None
            elif isinstance(example, date):
                low = date.fromisoformat(low) if isinstance(low, str) else low
                high = date.fromisoformat(high) if isinstance(high, str) else high
            if (lower is not None and high is not None and lower > high) or (
                upper is not None and low is not None and upper < low
            ):
                overlaps = False
                break
        count += int(overlaps)
    return count


def ensure_refs_survive_expiration(metadata, now=None):
    """Từ chối expiry nếu TTL sẽ làm Iceberg bỏ một ref mà policy đang bảo vệ.

    Iceberg có thể tự bỏ ref quá tuổi trong expire_snapshots. Không chỉ dựa
    vào hậu kiểm: kiểm retention tường minh trước khi gọi procedure xoá.
    """
    now = utc(now or datetime.now(timezone.utc))
    snapshots = {int(s["snapshot_id"]): s for s in metadata.get("snapshots", [])}
    default_age = int(
        metadata.get("properties", {}).get("history.expire.max-ref-age-ms", 9223372036854775807)
    )
    for ref in metadata.get("refs", []):
        if ref["name"] == "main":
            continue
        age_limit = ref.get("max_reference_age_in_ms")
        age_limit = default_age if age_limit is None else int(age_limit)
        snapshot = snapshots.get(int(ref["snapshot_id"]))
        if snapshot is None:
            raise ValueError(f"Ref {ref['name']} trỏ snapshot không đọc được")
        age_ms = (now - utc(snapshot["committed_at"])).total_seconds() * 1000
        if age_ms > age_limit:
            raise ValueError(
                f"Ref {ref['name']} đã quá TTL; dừng trước expiry để bảo vệ generation"
            )
