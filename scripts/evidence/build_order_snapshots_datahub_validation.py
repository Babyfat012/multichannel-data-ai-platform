"""Build DataHub validation evidence from successful Airflow task logs.

Mount the FSDS Airflow volume read-only at ``--logs-root``. Every check is
parsed from the successful attempt of a specific documented DAG run.

Log của hai validator có hai dạng. Dạng cũ (run 2026-10-13/14) là bằng chứng
lịch sử của CDC v1 và feature validator cũ. Dạng mới in thêm `thừa`/`sai payload`
(CDC v2) và một dict khóa/thời gian (feature). Parser đọc cả hai và gắn dạng log
vào metrics; muốn dùng run mới thì truyền ``--run`` và ``--output`` riêng để
không ghi đè manifest lịch sử.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from datetime import UTC, datetime
from pathlib import Path

RUNS = {
    "dp1_bronze": "manual__2026-10-13T15:30:09+00:00",
    "dp1_cdc": "manual__2026-10-13T22:34:22+00:00",
    "dp2_silver": "manual__2026-10-14T03:53:52+00:00",
    "dp3_features": "manual__2026-10-14T04:11:15+00:00",
    "dp3_walmart_labels": "manual__2026-10-14T04:15:18+00:00",
}
CHANNELS = ("amazon", "walmart", "tiktok_shop", "shopify", "shopee")
LEGACY_CDC_TABLE = "lakehouse.bronze.raw_walmart_cdc"
# Metric có tên bắt đầu bằng các tiền tố này phải bằng 0 thì check mới PASS.
FAILURE_PREFIXES = (
    "invalid",
    "duplicate",
    "missing",
    "extra",
    "bad_",
    "multiple_current",
    "payload_mismatch",
    "wrong_",
    "source_invalid",
    "source_duplicate",
)


def number(pattern: str, log: str) -> int:
    """Lấy một số nguyên có dấu phẩy ngăn cách từ log task."""
    match = re.search(pattern, log)
    if match is None:
        raise ValueError(f"Missing metric: {pattern}")
    return int(match.group(1).replace(",", ""))


def success_log(root: Path, dag: str, task: str, run_id: str) -> tuple[str, str, str]:
    """Lần chạy thành công gần nhất: đường dẫn tương đối và giờ kết thúc."""
    directory = root / f"dag_id={dag}" / f"run_id={run_id}" / f"task_id={task}"
    attempts = sorted(
        directory.glob("attempt=*.log"),
        key=lambda path: int(path.stem.partition("=")[2]),
        reverse=True,
    )
    for path in attempts:
        log = path.read_text(errors="replace")
        match = re.search(r"Marking task as SUCCESS\..*end_date=(\d{8}T\d{6})", log)
        if match:
            completed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
            return (
                log,
                path.relative_to(root).as_posix(),
                completed.replace(tzinfo=UTC).isoformat(),
            )
    raise ValueError(f"No successful attempt for {dag}.{task} in {directory}")


def cdc_metrics(log: str) -> tuple[dict[str, int], str, str]:
    """Parse validator CDC; dạng v2 có thêm offset thừa và payload lệch."""
    metrics = {
        "source_rows": number(r"Kafka=([\d,]+)", log),
        "dataset_rows": number(r"Bronze=([\d,]+)", log),
        "unique_offsets": number(r"offsets duy nhất=([\d,]+)", log),
        "missing_rows": number(r"thiếu=([\d,]+)", log),
        "invalid_rows": number(r"lỗi=([\d,]+)", log),
    }
    if "thừa=" not in log:
        return metrics, "Kafka = Bronze = số offset duy nhất; không thiếu và không lỗi", "cdc-v1"
    metrics["extra_rows"] = number(r"thừa=([\d,]+)", log)
    metrics["payload_mismatch_rows"] = number(r"sai payload=([\d,]+)", log)
    rule = (
        "Trong khoảng offset đã đóng băng của run và đúng source epoch: Kafka = Bronze "
        "= số offset duy nhất; không thiếu, không thừa, payload khớp, không lỗi"
    )
    return metrics, rule, "cdc-v2"


def feature_metrics(log: str) -> tuple[dict[str, int], str, str]:
    """Parse validator feature; dạng mới in một dict so khóa và event time với Gold."""
    match = re.search(r"(\{'fact_rows': [^\n]+?\})", log)
    if match is None:
        metrics = {
            "source_rows": number(r"Gold fact=([\d,]+)", log),
            "dataset_rows": number(r"feature=([\d,]+)", log),
            "invalid_rows": number(r"feature invalid=([\d,]+)", log),
            "duplicate_order_line_key": number(r"duplicate_order_line_key=([\d,]+)", log),
        }
        return (
            metrics,
            "Feature bằng Gold; không lỗi, không trùng khóa, không rò rỉ nhãn",
            "feature-v1",
        )
    raw = ast.literal_eval(match.group(1))
    metrics = {
        "source_rows": raw["fact_rows"],
        "dataset_rows": raw["feature_rows"],
        "invalid_rows": raw["invalid"],
        "duplicate_order_line_key": raw["duplicate_keys"],
        "source_invalid_rows": raw["source_invalid"],
        "source_duplicate_keys": raw["source_duplicates"],
        "missing_rows": raw["missing"],
        "extra_rows": raw["extra"],
        "wrong_event_time_rows": raw["wrong_event_time"],
    }
    rule = (
        "Feature và Gold cùng tập khóa Order Line (không thiếu, không thừa), không trùng "
        "khóa, event_timestamp = created_at của Gold, không rò rỉ nhãn. Chưa phải PIT đầy đủ"
    )
    return metrics, rule, "feature-v2"


def metrics_for(dag: str, task: str, log: str) -> tuple[dict[str, int], str, str]:
    """Đọc số đếm, luật kiểm và dạng log của một lần validate Spark."""
    log_format = "v1"
    if dag == "dp1_bronze":
        if task in {"validate_amazon", "validate_walmart"}:
            metrics = {
                "source_rows": number(r"nguồn=([\d,]+)", log),
                "dataset_rows": number(r"Bronze=([\d,]+)", log),
                "invalid_rows": number(r"lỗi=([\d,]+) bảng=", log),
            }
        else:
            metrics = {
                "source_rows": number(r"dòng nguồn\s*=\s*([\d,]+)", log),
                "dataset_rows": number(r"dòng Bronze\s*=\s*([\d,]+)", log),
                "invalid_rows": number(r"dòng lỗi\s*=\s*([\d,]+)", log),
            }
        rule = "Nguồn có dòng; số dòng nguồn = Bronze; dữ liệu và provenance hợp lệ"
        if metrics["source_rows"] <= 0 or metrics["source_rows"] != metrics["dataset_rows"]:
            raise ValueError(f"Bronze row mismatch: {task}")
    elif dag == "dp1_cdc":
        metrics, rule, log_format = cdc_metrics(log)
        if not metrics["source_rows"] == metrics["dataset_rows"] == metrics["unique_offsets"]:
            raise ValueError("CDC row mismatch")
    elif dag == "dp2_silver" and task.endswith("_silver"):
        source_pattern = r"Order nguồn(?: duy nhất)?\s*=\s*([\d,]+)"
        target_pattern = r"Order Line Silver\s*=\s*([\d,]+)"
        duplicate_match = re.search(r"có (?:khóa Order Line|khoá) trùng\s*=\s*(True|False)", log)
        if duplicate_match is None:
            raise ValueError(f"Missing duplicate-key result: {task}")
        metrics = {
            "source_orders": number(source_pattern, log),
            "dataset_rows": number(target_pattern, log),
            "invalid_rows": number(r"dòng (?:không hợp lệ|lỗi)\s*=\s*([\d,]+)", log),
            "duplicate_keys": int(duplicate_match.group(1) == "True"),
        }
        rule = "Order Line >= Order nguồn; cột bắt buộc, run_id và khóa hợp lệ"
        if metrics["source_orders"] <= 0 or metrics["dataset_rows"] < metrics["source_orders"]:
            raise ValueError(f"Silver row mismatch: {task}")
    elif task == "validate_gold_order_lines":
        match = re.search(r"Silver rows: (\{[^\n]+?\}); Gold rows: (\{[^\n]+?\})", log)
        if match is None:
            raise ValueError("Missing Gold channel counts")
        silver, gold = ast.literal_eval(match.group(1)), ast.literal_eval(match.group(2))
        if silver != gold:
            raise ValueError("Gold channel mismatch")
        metrics = {
            "source_rows": sum(silver.values()),
            "dataset_rows": sum(gold.values()),
            "invalid_rows": number(r"Gold invalid=([\d,]+)", log),
            "duplicate_order_line_key": number(r"duplicate_order_line_key=([\d,]+)", log),
        }
        rule = "Gold khớp Silver theo từng Channel; không lỗi và không trùng khóa"
    elif task == "validate_amazon_order_dim":
        current = number(r"is_current=True, count=([\d,]+)", log)
        historic = number(r"is_current=False, count=([\d,]+)", log)
        metrics = {"dataset_rows": current + historic}
        for key in ("invalid", "duplicate", "multiple_current", "bad_fact", "missing_version"):
            metrics[f"{key}_rows"] = number(rf"{key}=([\d,]+)", log)
        rule = "Một phiên bản hiện hành mỗi Order; khóa fact trỏ đúng phiên bản; không lỗi"
    elif task == "validate_return_features":
        metrics, rule, log_format = feature_metrics(log)
        if metrics["source_rows"] != metrics["dataset_rows"]:
            raise ValueError("Feature row mismatch")
    elif task == "build_and_validate_walmart_labels":
        metrics = {
            "dataset_rows": number(r"Bridge Walmart: rows=([\d,]+)", log),
            "duplicate_keys": number(r"Bridge Walmart: rows=[\d,]+, duplicate=([\d,]+)", log),
            "invalid_rows": number(
                r"Bridge Walmart: rows=[\d,]+, duplicate=[\d,]+, invalid=([\d,]+)", log
            ),
        }
        rule = "Bridge nhãn Walmart có dòng; không lỗi và không trùng khóa"
    else:
        raise ValueError(f"Unknown validation task: {dag}.{task}")
    if metrics.get("dataset_rows", 0) <= 0 or any(
        value != 0 for key, value in metrics.items() if key.startswith(FAILURE_PREFIXES)
    ):
        raise ValueError(f"Validation failed: {dag}.{task}: {metrics}")
    return metrics, rule, log_format


def check_cdc_table(log_format: str, table: str) -> None:
    """Chặn gắn log CDC v2 vào bảng v1 (hoặc ngược lại) trên DataHub."""
    if (log_format == "cdc-v2") == (table == LEGACY_CDC_TABLE):
        raise ValueError(f"Log {log_format} không khớp bảng CDC {table}")


def parse_runs(overrides: list[str]) -> dict[str, str]:
    """Ghép run mặc định với các ``dag=run_id`` truyền vào."""
    runs = dict(RUNS)
    for item in overrides:
        dag, sep, run_id = item.partition("=")
        if not sep or dag not in RUNS or not run_id:
            raise ValueError(
                f"--run phải có dạng dag=run_id với dag thuộc {sorted(RUNS)}: {item!r}"
            )
        runs[dag] = run_id
    return runs


def main() -> None:
    """Chỉ ghi manifest khi mọi check được chọn đều đọc được và PASS."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-root", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/evidence/datahub-validation.json")
    )
    parser.add_argument(
        "--run",
        action="append",
        default=[],
        metavar="DAG=RUN_ID",
        help="Thay run mặc định của một DAG; lặp lại cho nhiều DAG",
    )
    parser.add_argument(
        "--cdc-table",
        default=LEGACY_CDC_TABLE,
        help="Bảng CDC mà run dp1_cdc đã ghi (Airflow Variable fsds_dp1_cdc_table)",
    )
    args = parser.parse_args()
    runs = parse_runs(args.run)
    tasks = (
        [
            ("dp1_bronze", f"validate_{channel}", f"lakehouse.bronze.raw_{channel}_orders_dp1")
            for channel in CHANNELS
        ]
        + [("dp1_cdc", "validate_walmart_cdc", args.cdc_table)]
        + [
            (
                "dp2_silver",
                f"validate_{channel}_silver",
                f"lakehouse.silver.stg_{channel}_order_lines_dp2",
            )
            for channel in CHANNELS
        ]
        + [("dp2_silver", "validate_amazon_order_dim", "lakehouse.gold.dim_amazon_order_dp2")]
        + [("dp2_silver", "validate_gold_order_lines", "lakehouse.gold.fact_order_lines_dp2")]
        + [
            (
                "dp3_features",
                "validate_return_features",
                "lakehouse.gold.feat_order_line_return_dp3",
            )
        ]
        + [
            (
                "dp3_walmart_labels",
                "build_and_validate_walmart_labels",
                "lakehouse.gold.fact_walmart_return_labels_dp3",
            )
        ]
    )
    checks = []
    for dag, task, dataset in tasks:
        log, source_log, completed_at = success_log(args.logs_root, dag, task, runs[dag])
        metrics, rule, log_format = metrics_for(dag, task, log)
        if dag == "dp1_cdc":
            check_cdc_table(log_format, dataset)
        checks.append(
            {
                "dag_id": dag,
                "task_id": task,
                "dataset": dataset,
                "run_id": runs[dag],
                "state": "success",
                "completed_at": completed_at,
                "source_log": source_log,
                "rule": rule,
                "log_format": log_format,
                "metrics": metrics,
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"checks": checks}, ensure_ascii=False, indent=2) + "\n")
    print(f"Wrote {len(checks)} successful checks to {args.output}")


if __name__ == "__main__":
    main()
