"""DE-N2: đọc metadata, lập/thi hành kế hoạch, kiểm dữ liệu và lưu receipt JSON.

Fingerprint = (count(*), sum(cast(xxhash64(tất cả cột theo schema) AS decimal(38,0)))).
Tổng không phụ thuộc thứ tự dòng/file, không tràn bigint; đây là checksum xác suất,
không phải chứng minh toán học rằng hai multiset bằng nhau. Không dùng count metadata.
Module chỉ import pyspark khi dựng session; có thể nạp CLI/help không cần Spark.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time

from spark_jobs.lib.maintenance_policy import (count_probe_files, ensure_refs_survive_expiration, execute_actions, load_config,
                                plan_maintenance, validated_config)

ROOT = Path(__file__).resolve().parents[2]


def identifier(value):
    """Quote từng phần định danh; bắt buộc catalog.namespace.table tường minh."""
    parts = value.split(".")
    if len(parts) < 3 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts):
        raise ValueError(f"Tên bảng không hợp lệ: {value}")
    return ".".join(f"`{part}`" for part in parts)


def column_name(value):
    """Quote tên cột Spark, kể cả tên có dấu backtick."""
    return "`" + value.replace("`", "``") + "`"


def literal(value):
    """Sinh literal Spark SQL; từ chối chuỗi có backslash để tránh escape nhập nhằng."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    if isinstance(value, date):
        return "DATE '" + value.isoformat() + "'"
    value = str(value)
    if "\\" in value:
        raise ValueError("SQL literal không hỗ trợ backslash")
    return "'" + value.replace("'", "\\'") + "'"


def rows(spark, query):
    """Thu kết quả metadata/procedure thành dict JSON được."""
    return [row.asDict(recursive=True) for row in spark.sql(query).collect()]


def snapshot_id(spark, table):
    """Đọc snapshot hiện hành từ ref main, không đoán theo timestamp."""
    current = rows(spark, f"SELECT snapshot_id FROM {identifier(table)}.refs WHERE name = 'main'")
    return current[0]["snapshot_id"] if current else None


def read_metadata(spark, table):
    """Chụp files/snapshots/refs/partitions/properties cho policy thuần Python."""
    name = identifier(table)
    files = rows(spark, f"SELECT * FROM {name}.files WHERE content = 0")
    return {"files": files, "snapshots": rows(spark, f"SELECT * FROM {name}.snapshots"),
            "refs": rows(spark, f"SELECT * FROM {name}.refs"),
            "partitions": rows(spark, f"SELECT * FROM {name}.partitions"),
            "properties": {row["key"]: row["value"] for row in rows(spark, f"SHOW TBLPROPERTIES {name}")},
            "current_snapshot_id": snapshot_id(spark, table)}


def fingerprint(spark, table, version=None):
    """Đếm dòng logic và tổng decimal của xxhash64 mọi cột, dùng cùng schema."""
    relation = identifier(table)
    if version is not None:
        relation += " VERSION AS OF " + literal(version)
    frame = spark.sql(f"SELECT * FROM {relation}")
    columns = ", ".join(column_name(column) for column in frame.columns)
    result = frame.selectExpr("count(*) AS rows",
                              f"coalesce(sum(cast(xxhash64({columns}) AS decimal(38,0))), 0) AS fingerprint").first()
    return {"rows": result["rows"], "fingerprint": str(result["fingerprint"]), "columns": frame.columns}


def probe_where(probe):
    """Chuyển khoảng đóng AND của probe thành biểu thức Spark SQL."""
    clauses = []
    for column, (low, high) in probe["ranges"].items():
        if low is not None:
            clauses.append(f"{column_name(column)} >= {literal(low)}")
        if high is not None:
            clauses.append(f"{column_name(column)} <= {literal(high)}")
    return " AND ".join(clauses) or "true"


def measure(spark, table, config, metadata=None):
    """Đo checksum, file/byte/snapshot và probe: 1 warmup + 3 lượt, n=3."""
    metadata = metadata or read_metadata(spark, table)
    result = {**fingerprint(spark, table), "data_files": len(metadata["files"]),
              "total_bytes": sum(file["file_size_in_bytes"] for file in metadata["files"]),
              "snapshots": len(metadata["snapshots"]), "probes": {}}
    if config["zorder_columns"]:
        for probe in config["probe_predicates"]:
            query = f"SELECT count(*) FROM {identifier(table)} WHERE {probe_where(probe)}"
            spark.sql(query).collect()
            samples = []
            for _ in range(3):
                started = time.perf_counter()
                spark.sql(query).collect()
                samples.append(time.perf_counter() - started)
            result["probes"][probe["name"]] = {
                "candidate_data_files": count_probe_files(metadata["files"], probe["ranges"]),
                "seconds": samples, "n": 3, "warmup": 1}
    return result


def assert_same(before, after):
    """Fail khi thay số dòng, checksum hoặc thứ tự/schema cột."""
    for key in ("rows", "fingerprint", "columns"):
        if before[key] != after[key]:
            raise ValueError(f"Kiểm tra dữ liệu thất bại: {key}: {before[key]} != {after[key]}")


def partition_where(spark, table, partitions):
    """Dịch partition identity/time thành predicate cột nguồn; fail closed với spec khác.

    Không dùng tên partition transform trực tiếp làm tên cột. Bảng có evolution spec
    khác hiện hành hoặc bucket/truncate bị từ chối trước mọi procedure.
    """
    jtable = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    spec = jtable.spec()
    if jtable.specs().size() != 1:
        raise ValueError("Bảng có partition spec evolution; cần mapping riêng trước compaction")
    fields = list(spec.fields())
    clauses = []
    for partition in partitions:
        if not partition:
            return "true"
        predicates = []
        if set(partition) != {field.name() for field in fields}:
            raise ValueError("Partition spec evolution cần mapping riêng; dừng an toàn")
        for field in fields:
            value = partition[field.name()]
            source_name = str(jtable.schema().findColumnName(field.sourceId()))
            if "." in source_name:
                raise ValueError("Partition theo cột lồng nhau cần mapping riêng")
            column = column_name(source_name)
            transform = str(field.transform())
            if value is None:
                predicates.append(f"{column} IS NULL")
            elif transform == "identity":
                predicates.append(f"{column} = {literal(value)}")
            elif transform in ("year", "month", "day", "hour"):
                epoch = datetime(1970, 1, 1)
                if transform == "year":
                    start, end = datetime(1970 + value, 1, 1), datetime(1971 + value, 1, 1)
                elif transform == "month":
                    year, month = 1970 + value // 12, value % 12 + 1
                    start = datetime(year, month, 1)
                    end = datetime(year + (month == 12), month % 12 + 1, 1)
                else:
                    delta = timedelta(days=1) if transform == "day" else timedelta(hours=1)
                    start, end = epoch + value * delta, epoch + (value + 1) * delta
                predicates.append(f"({column} >= {literal(start)} AND {column} < {literal(end)})")
            else:
                raise ValueError(f"Partition transform chưa hỗ trợ: {transform}")
        clauses.append("(" + " AND ".join(predicates) + ")")
    return " OR ".join(clauses)


def action_sql(spark, table, action):
    """Sinh procedure Iceberg 1.11; quote where như một tham số string."""
    name = identifier(table)
    catalog, short = table.split(".", 1)
    system = f"`{catalog}`.system"
    kind = action["kind"]
    if kind in ("compact", "zorder"):
        options = {"target-file-size-bytes": str(action["target_file_size_bytes"])}
        if kind == "compact":
            options["min-input-files"] = str(action["min_input_files"])
            extra = ", where => " + literal(partition_where(spark, table, action["partitions"]))
            strategy = "binpack"
        else:
            # rewrite-all bắt buộc: sau binpack file có thể đã đủ lớn nhưng chưa z-order.
            options["rewrite-all"] = "true"
            extra = ", sort_order => " + literal("zorder(" + ",".join(action["columns"]) + ")")
            strategy = "sort"
        mapping = ", ".join(f"{literal(key)}, {literal(value)}" for key, value in options.items())
        return (f"CALL {system}.rewrite_data_files(table => {literal(short)}, strategy => {literal(strategy)}"
                f"{extra}, options => map({mapping}))")
    if kind == "expire_snapshots":
        return (f"CALL {system}.expire_snapshots(table => {literal(short)}, "
                f"older_than => {literal(datetime.fromisoformat(action['older_than']))}, "
                f"retain_last => {action['retain_last']})")
    if kind == "remove_orphans":
        return (f"CALL {system}.remove_orphan_files(table => {literal(short)}, "
                f"older_than => {literal(datetime.fromisoformat(action['older_than']))}, "
                f"dry_run => {literal(action['dry_run'])})")
    raise ValueError(f"Hành động không biết: {kind} trên {name}")


def code_hashes():
    """Gắn sha256 code/config vào receipt để truy nguồn phiên bản đã chạy."""
    paths = [Path(__file__), ROOT / "spark_jobs/lib/maintenance_policy.py"]
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def write_receipt(path, receipt):
    """Ghi JSON atomic cùng thư mục, kể cả receipt lỗi."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, default=str) + "\n")
    temp.replace(path)


def check_tags(spark, table, tags):
    """Đọc lại từng generation tag bằng VERSION AS OF và so fingerprint trước."""
    for tag, expected in tags.items():
        assert_same(expected, fingerprint(spark, table, tag))


def run_table(spark, table, config, receipt, execute=False, transient_failures=0, fatal_failure=False):
    """Chạy một bảng; receipt được cập nhật tại chỗ ngay cả khi lỗi giữa chừng."""
    config = validated_config(config)
    spark.conf.set("spark.sql.parser.escapedStringLiterals", "false")
    receipt.update(table=table, config=config, attempts=[], status="running")
    try:
        metadata = read_metadata(spark, table)
        plan = plan_maintenance(metadata, config)
        receipt["plan"] = plan
        if any(action["kind"] == "expire_snapshots" for action in plan["actions"]):
            ensure_refs_survive_expiration(metadata)
        # Preflight mọi SQL để partition không hỗ trợ không gây thực thi dở dang.
        statements = [action_sql(spark, table, action) for action in plan["actions"]]
        receipt["sql"] = statements
        if not execute:
            receipt["status"] = "planned"
            return receipt
        receipt["before"] = measure(spark, table, config, metadata)
        receipt["generation_before"] = {tag: fingerprint(spark, table, tag) for tag in plan["generation_tags"]}
        injected = 0
        first = plan["actions"][0] if plan["actions"] else None

        def perform(action):
            """Thực thi procedure, injection trước CALL và ghi mốc z-order sau commit."""
            nonlocal injected
            if action is first:
                if fatal_failure:
                    raise ValueError("Injected fatal failure trước procedure")
                if injected < transient_failures:
                    injected += 1
                    raise RuntimeError("CommitFailedException: injected transient failure trước procedure")
            if action["kind"] == "expire_snapshots":
                # Kiểm lại sát CALL vì compact/zorder có thể kéo dài qua TTL của ref.
                ensure_refs_survive_expiration(read_metadata(spark, table))
            result = rows(spark, action_sql(spark, table, action))
            if action["kind"] == "zorder":
                current = snapshot_id(spark, table)
                spark.sql(f"ALTER TABLE {identifier(table)} SET TBLPROPERTIES "
                          f"('fsds.maintenance.last_zorder_snapshot_id' = '{current}')").collect()
            if action["kind"] == "expire_snapshots":
                check_tags(spark, table, receipt["generation_before"])
            return result

        execute_actions(plan["actions"], perform, config, receipt["attempts"])
        receipt["after"] = measure(spark, table, config)
        assert_same(receipt["before"], receipt["after"])
        check_tags(spark, table, receipt["generation_before"])
        receipt["generation_after"] = {tag: fingerprint(spark, table, tag) for tag in plan["generation_tags"]}
        receipt["status"] = "success"
        return receipt
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise


def validate_receipt(spark, receipt):
    """Session mới phải đọc được cùng dữ liệu hiện hành và generation của receipt."""
    if receipt.get("status") != "success" or not receipt.get("tables"):
        raise ValueError("Receipt không phải lần execute thành công")
    for entry in receipt["tables"]:
        if entry.get("status") != "success":
            raise ValueError("Bảng chưa bảo trì thành công")
        assert_same(entry["before"], entry["after"])
        assert_same(entry["after"], fingerprint(spark, entry["table"]))
        check_tags(spark, entry["table"], entry["generation_before"])
    return {"status": "validated", "validated_at": datetime.now(timezone.utc).isoformat()}


def arguments():
    """CLI dùng chung plan/execute/validate và catalog local không nối mạng."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--validate-receipt")
    parser.add_argument("--config", default=str(ROOT / "configs/iceberg_maintenance.json"))
    parser.add_argument("--config-json", help="Bản JSON từ Airflow Variable, cùng schema với --config")
    parser.add_argument("--tables", help="JSON array hoặc danh sách bảng phân cách bằng dấu phẩy")
    parser.add_argument("--receipt", default="/tmp/fsds-maintenance-receipt.json")
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--local-warehouse", help="Dùng Hadoop local catalog thay vì build_spark REST")
    parser.add_argument("--inject-transient-failures", type=int, default=0)
    parser.add_argument("--inject-fatal-failure", action="store_true")
    return parser.parse_args()


def build_session(args):
    """Production dùng helper có sẵn; Hadoop local không lấy packages qua mạng."""
    if args.local_warehouse:
        from pyspark.sql import SparkSession
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.catalog):
            raise ValueError("Catalog không hợp lệ")
        return (SparkSession.builder.master("local[2]").appName("fsds-maintenance-local")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
                .config(f"spark.sql.catalog.{args.catalog}.type", "hadoop")
                .config(f"spark.sql.catalog.{args.catalog}.warehouse", args.local_warehouse)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    from spark_jobs.lib.iceberg_session import build_spark
    return build_spark("fsds-iceberg-maintenance", shuffle_partitions=4)


def main():
    """Ghi receipt lỗi và exit khác 0; validate không ghi đè receipt execute."""
    args = arguments()
    receipt = {"status": "running", "started_at": datetime.now(timezone.utc).isoformat(),
               "mode": "execute" if args.execute else "plan", "code_sha256": code_hashes(), "tables": []}
    spark = None
    try:
        if args.inject_transient_failures < 0:
            raise ValueError("Số lỗi injection phải >=0")
        if args.validate_receipt:
            spark = build_session(args)
            original = json.loads(Path(args.validate_receipt).read_text())
            print(json.dumps(validate_receipt(spark, original)))
            return
        if args.config_json:
            payload = args.config_json
            data = json.loads(payload)
            if not isinstance(data.get("tables"), dict) or not data["tables"]:
                raise ValueError("Cấu hình cần tables khác rỗng")
            configs = {table: validated_config(config) for table, config in data["tables"].items()}
        else:
            payload = Path(args.config).read_text()
            configs = load_config(args.config)
        receipt["config_sha256"] = hashlib.sha256(payload.encode()).hexdigest()
        tables = (json.loads(args.tables) if args.tables and args.tables.lstrip().startswith("[")
                  else args.tables.split(",") if args.tables else list(configs))
        if not isinstance(tables, list) or not tables:
            raise ValueError("Danh sách bảng phải khác rỗng")
        for table in tables:
            identifier(table)
            if table.split(".")[0] != args.catalog or table not in configs:
                raise ValueError(f"Bảng chưa có cấu hình hoặc khác catalog: {table}")
        spark = build_session(args)
        for index, table in enumerate(tables):
            entry = {}
            receipt["tables"].append(entry)
            run_table(spark, table, configs[table], entry, args.execute,
                      args.inject_transient_failures if index == 0 else 0,
                      args.inject_fatal_failure if index == 0 else False)
        receipt["status"] = "success" if args.execute else "planned"
    except Exception as error:
        receipt.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        if not args.validate_receipt:
            receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
            write_receipt(args.receipt, receipt)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
