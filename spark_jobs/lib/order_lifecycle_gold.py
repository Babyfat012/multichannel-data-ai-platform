"""Gold v2: DataFrame Silver vào, DataFrame Gold ra; chỉ đọc Silver.

Luật nhãn nằm trọn ở `return_label`; Spark chỉ chuyển bằng chứng và gọi luật đó trong worker.
Hai lựa chọn giữ chi phí tuyến tính thay vì bậc hai:

- Bằng chứng mỗi line lấy theo khoá từ `silver.lifecycle_stg_observed_evidence` (line, order,
  correction của sự kiện thuộc line). Chỉ bản ghi không khoá (coverage, scope cả Channel) được
  gửi một lần tới mỗi worker. Digest coverage được Silver kiểm một lần.
- Feature PIT dùng `return_label.label_segments`: nhãn của line chỉ đổi ở thời điểm quan sát
  của chính nó hoặc lúc hết hạn chờ, nên đếm theo SKU là một lần quét các cửa sổ nửa mở.

`tests/spark/check_order_lifecycle_gold_equivalence.py` kiểm kết quả giống bản bậc hai cũ.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG
from spark_jobs.lib.package_distribution import distribute_python_packages
from spark_jobs.lib.receipts import completed_at_utc
from spark_jobs.lib.return_label import LABEL_VERSION, label_line, label_segments, source_line, utc

SILVER_TABLES = ("line_lifecycle", "native_order_lines", "observed_evidence", "coverage_validity")
GOLD_TABLES = (
    "fact_order_lines",
    "label_return_v2",
    "feat_sku_return_rate_90d",
    "label_segments",
    "label_inputs",
)


def read_silver(spark, cutoff):
    """Đọc Silver đã publish; từ chối ảnh chụp không dựng với `as_of == cutoff`."""
    info = spark.table(f"{CATALOG}.silver.lifecycle_stg_build_info").collect()
    if len(info) != 1 or info[0]["as_of"] is None or utc(info[0]["as_of"]) != utc(cutoff):
        raise ValueError(
            f"Silver as_of={[r['as_of'] for r in info]} != cutoff {utc(cutoff).isoformat()}; "
            "run job 21 --as-of <cutoff> first"
        )
    silver = {name: spark.table(f"{CATALOG}.silver.lifecycle_stg_{name}") for name in SILVER_TABLES}
    late = (
        silver["observed_evidence"]
        .filter(F.col("source_observed_at") >= F.lit(utc(cutoff)))
        .limit(1)
        .count()
    )
    if late:
        raise ValueError(
            "observed_evidence holds observations at/after cutoff; Silver snapshot is not as_of cutoff"
        )
    return silver


KEY = ["dataset_version", "line_id"]
IDENTITY = KEY + ["channel", "account_id", "native_order_id", "native_line_id", "seller_sku"]
LABEL_DDL = (
    "dataset_version string, line_id string, cutoff timestamp, generation_id string, computed_at timestamp, "
    "state string, label int, cohort_date date, maturity_end timestamp, qualifying_event_id string, "
    "reasons array<string>, label_version string, decision_observed_at timestamp, source_event_refs array<string>, input_json string, "
    "positive_kind string"
)
INPUT_DDL = "dataset_version string, line_id string, input_json string, created_at timestamp"
SEGMENT_DDL = (
    "dataset_version string, line_id string, start_exclusive timestamp, end_inclusive timestamp, label int, "
    "cohort_date date, maturity_end timestamp, decision_observed_at timestamp, qualifying_event_id string"
)
# Giới hạn driver: 39 tuần × 9 feeds = 351 complete envelope/dataset, dưới 10.000.
# source_line lọc interval tại worker trước khi tạo input_json; không nhân tuần vào từng line.
MAX_KEYLESS_EVIDENCE = 10_000
# Chỉ dùng để Silver kiểm coverage (`coverage_validity`); khi đã có `coverage_status`, return_label không
# đọc. Trường này dài theo dữ liệu (hàng trăm KB ở scale), nên bỏ trước khi gửi tới từng line.
VERIFICATION_ONLY_FIELDS = ("source_sequence_ranges",)


def _slim(raw_json: str) -> str:
    """Bỏ trường chỉ phục vụ kiểm coverage khỏi một bản ghi keyless."""
    record = json.loads(raw_json)
    for name in VERIFICATION_ONLY_FIELDS:
        record.pop(name, None)
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _input_row(row, statuses, keyless=None):
    """Normalize one line's candidate observations in a Spark worker (exact filtering is in return_label).

    `keyless` = {dataset_version: [raw_json]}: bản ghi không gắn line/Order, gửi một lần cho cả
    worker thay vì nhân bản theo từng line trong Spark (chi phí số line × kích thước bản ghi).
    """
    identity = {key: row[key] for key in IDENTITY}
    raws = [r for r in row["raw_records"] if r is not None]
    raws += (keyless or {}).get(row["dataset_version"], [])
    data = source_line(identity, raws, coverage_status=statuses.get(row["dataset_version"], {}))
    return (
        row["dataset_version"],
        row["line_id"],
        json.dumps(data, default=str, sort_keys=True),
        data["created_at"],
    )


def _label_row(row, cutoff, generation, computed):
    """Gọi luật nhãn thuần duy nhất; giữ mọi cột bằng chứng."""
    data = json.loads(row["input_json"])
    result = asdict(label_line(data, cutoff))
    return dict(
        dataset_version=row["dataset_version"],
        line_id=row["line_id"],
        cutoff=utc(cutoff),
        generation_id=generation,
        computed_at=computed,
        input_json=row["input_json"],
        **result,
    )


def _segment_rows(row):
    """Mọi đoạn cutoff có nhãn của một line (`return_label.label_segments`)."""
    for segment in label_segments(json.loads(row["input_json"])):
        yield (
            row["dataset_version"],
            row["line_id"],
            segment["start_exclusive"],
            segment["end_inclusive"],
            segment["label"],
            segment["cohort_date"],
            segment["maturity_end"],
            segment["decision_observed_at"],
            segment["qualifying_event_id"],
        )


def label_inputs(inputs: DataFrame, cutoff, run_id: str, computed_at=None) -> DataFrame:
    """Tính một nhãn cho mỗi line đầu vào tại một ảnh chụp quan sát."""
    computed = computed_at or datetime.now(timezone.utc)
    return inputs.sparkSession.createDataFrame(
        inputs.rdd.map(lambda row: _label_row(row, cutoff, run_id, computed)), LABEL_DDL
    )


def keyless_evidence(evidence: DataFrame) -> dict:
    """{dataset_version: [raw_json đã bỏ trường kiểm coverage]} cho bản ghi đi tới MỌI line.

    Gồm bản ghi không có scope line/Order, và correction của chính các bản ghi đó (vì bản bị
    sửa nằm ở mọi line). Thu về driver một lần; số lượng bị chặn bởi MAX_KEYLESS_EVIDENCE.
    """
    has_line = F.col("ev_line_id").isNotNull() | F.col("ev_line_ids").isNotNull()
    has_order = F.col("ev_order_id").isNotNull() | F.col("ev_order_ids").isNotNull()
    keyless = evidence.filter(~has_line & ~has_order).select(
        "dataset_version", "record_sha256", "raw_json", "evidence_event_id"
    )
    if keyless.count() > MAX_KEYLESS_EVIDENCE:
        raise ValueError(
            f"More than {MAX_KEYLESS_EVIDENCE} keyless evidence records; scope extraction is wrong"
        )
    held = keyless.select(
        "dataset_version", F.col("evidence_event_id").alias("corrects_source_event_id")
    )
    corrections = evidence.join(
        held, ["dataset_version", "corrects_source_event_id"], "left_semi"
    ).select("dataset_version", "record_sha256", "raw_json")
    rows = (
        keyless.select("dataset_version", "record_sha256", "raw_json")
        .unionByName(corrections)
        .dropDuplicates(["dataset_version", "record_sha256"])
        .collect()
    )
    out = {}
    for row in sorted(rows, key=lambda r: (r["dataset_version"], r["record_sha256"])):
        out.setdefault(row["dataset_version"], []).append(_slim(row["raw_json"]))
    return out


def line_evidence(lines: DataFrame, evidence: DataFrame) -> DataFrame:
    """Cặp (line, raw_json) cho bản ghi CÓ khoá; là tập cha của thứ `visible_events` giữ.

    Khoá giống `scope_matches`: bản ghi gắn line đi tới line đó, gắn order đi tới mọi line của
    order, correction đi thêm tới line chứa bản ghi nó sửa. Bản ghi không khoá đi qua
    `keyless_evidence`, không join lines × records.
    """
    ev = evidence.select(
        "dataset_version",
        "record_sha256",
        "raw_json",
        "evidence_event_id",
        "corrects_source_event_id",
        "ev_line_id",
        "ev_line_ids",
        "ev_order_id",
        "ev_order_ids",
    )
    line_ids = lines.select("dataset_version", "line_id")
    has_line = F.col("ev_line_id").isNotNull() | F.col("ev_line_ids").isNotNull()
    has_order = F.col("ev_order_id").isNotNull() | F.col("ev_order_ids").isNotNull()
    by_line = (
        ev.filter(has_line)
        .withColumn(
            "line_id",
            F.explode(
                F.when(F.col("ev_line_id").isNotNull(), F.array("ev_line_id")).otherwise(
                    F.col("ev_line_ids")
                )
            ),
        )
        .join(line_ids, ["dataset_version", "line_id"], "left_semi")
    )
    by_order = (
        ev.filter(~has_line & has_order)
        .withColumn(
            "native_order_id",
            F.explode(
                F.when(F.col("ev_order_id").isNotNull(), F.array("ev_order_id")).otherwise(
                    F.col("ev_order_ids")
                )
            ),
        )
        .join(
            lines.select("dataset_version", "native_order_id", "line_id"),
            ["dataset_version", "native_order_id"],
        )
        .drop("native_order_id")
    )
    columns = ["dataset_version", "line_id", "record_sha256", "raw_json", "evidence_event_id"]
    direct = by_line.select(*columns).unionByName(by_order.select(*columns))
    held = direct.select(
        "dataset_version", "line_id", F.col("evidence_event_id").alias("corrects_source_event_id")
    )
    corrections = (
        ev.filter(F.col("corrects_source_event_id").isNotNull())
        .join(held, ["dataset_version", "corrects_source_event_id"])
        .select(*columns)
    )
    return direct.unionByName(corrections).dropDuplicates(
        ["dataset_version", "line_id", "record_sha256"]
    )


def _feature_windows(segments: DataFrame, skus: DataFrame) -> DataFrame:
    """Khoảng thời điểm đặt [a, b) mà một đoạn có nhãn được đếm (giống `return_label.feature_window`)."""
    micro = F.expr("INTERVAL 1 MICROSECOND")
    begin = F.greatest(
        F.col("start_exclusive") + micro, F.to_timestamp(F.date_add("cohort_date", 1))
    )
    end = F.least(F.col("end_inclusive") + micro, F.to_timestamp(F.date_add("cohort_date", 91)))
    return (
        segments.join(skus, KEY)
        .select(*KEY, "seller_sku", "label", begin.alias("a"), end.alias("b"))
        .filter(F.col("a") < F.col("b"))
    )


def _sweep(windows: DataFrame, queries: DataFrame) -> DataFrame:
    """Tổng dồn (n, p) của các cửa sổ đang mở tại mỗi truy vấn (dataset, SKU, time, line_id).

    +1/+nhãn khi cửa sổ mở, −1/−nhãn khi đóng. Cùng timestamp thì đổi cửa sổ xếp trước truy
    vấn (a ≤ t được đếm, b ≤ t không).
    """
    group = ["dataset_version", "seller_sku"]
    opens = windows.select(
        *group,
        F.col("a").alias("time"),
        F.lit(0).alias("kind"),
        F.lit(1).alias("dc"),
        F.col("label").alias("dp"),
        F.lit(None).cast("string").alias("line_id"),
    )
    closes = windows.select(
        *group,
        F.col("b").alias("time"),
        F.lit(0).alias("kind"),
        F.lit(-1).alias("dc"),
        (-F.col("label")).alias("dp"),
        F.lit(None).cast("string").alias("line_id"),
    )
    queries = queries.select(
        *group,
        "time",
        F.lit(1).alias("kind"),
        F.lit(0).alias("dc"),
        F.lit(0).alias("dp"),
        "line_id",
    )
    running = (
        Window.partitionBy(*group).orderBy("time", "kind").rowsBetween(Window.unboundedPreceding, 0)
    )
    return (
        opens.unionByName(closes)
        .unionByName(queries)
        .withColumn("n", F.sum("dc").over(running))
        .withColumn("p", F.sum("dp").over(running))
        .filter(F.col("kind") == 1)
        .select(*group, "time", "line_id", "n", "p")
    )


def _with_rate(frame: DataFrame) -> DataFrame:
    """Tỷ lệ = dương / có nhãn; NULL khi chưa có line nào có nhãn."""
    return frame.withColumn(
        "return_or_refund_rate_90d",
        F.when(
            F.col("labeled_count_90d") > 0,
            F.col("positive_count_90d") / F.col("labeled_count_90d"),
        ),
    )


def sku_features(fact: DataFrame, segments: DataFrame) -> DataFrame:
    """Feature mỗi line: lịch sử có nhãn của cùng SKU, biết tại thời điểm đặt, 90 ngày cohort.

    Đọc tổng dồn của `_sweep` tại thời điểm đặt rồi trừ đi cửa sổ của chính line.
    """
    windows = _feature_windows(segments, fact.select(*KEY, "seller_sku"))
    queries = fact.filter(F.col("created_at").isNotNull()).select(
        "dataset_version", "seller_sku", F.col("created_at").alias("time"), "line_id"
    )
    swept = _sweep(windows, queries).select("dataset_version", "line_id", "n", "p")
    own = (
        windows.join(fact.select(*KEY, "created_at"), KEY)
        .filter((F.col("a") <= F.col("created_at")) & (F.col("created_at") < F.col("b")))
        .groupBy(*KEY)
        .agg(F.count("*").alias("own_n"), F.sum("label").alias("own_p"))
    )
    counts = (
        swept.join(own, KEY, "left")
        .fillna(0, subset=["own_n", "own_p"])
        .select(
            *KEY,
            (F.col("n") - F.col("own_n")).cast("bigint").alias("labeled_count_90d"),
            (F.col("p") - F.col("own_p")).cast("bigint").alias("positive_count_90d"),
        )
    )
    return _with_rate(
        fact.select(*KEY, "seller_sku", "created_at")
        .join(counts, KEY, "left")
        .fillna(0, subset=["labeled_count_90d", "positive_count_90d"])
    )


def sku_daily_features(fact: DataFrame, segments: DataFrame, cutoff) -> DataFrame:
    """Ảnh chụp feature SKU tại 00:00 UTC mỗi ngày, từ ngày đặt đầu tiên của SKU tới cutoff.

    Cùng định nghĩa với `sku_features` (cùng cửa sổ, cùng `_sweep`) nhưng hỏi theo ngày thay vì
    theo line, nên Feast dùng được cho cả train (PIT join) lẫn serve (bản mới nhất). Không có
    "line của chính mình" để trừ. `event_timestamp` = mốc ảnh chụp.
    """
    windows = _feature_windows(segments, fact.select(*KEY, "seller_sku"))
    days = (
        fact.filter(F.col("created_at").isNotNull())
        .groupBy("dataset_version", "seller_sku")
        .agg(F.to_date(F.min("created_at")).alias("first_day"))
        .select(
            "dataset_version",
            "seller_sku",
            F.explode(
                F.sequence("first_day", F.to_date(F.lit(utc(cutoff))), F.expr("INTERVAL 1 DAY"))
            ).alias("day"),
        )
        .select(
            "dataset_version",
            "seller_sku",
            F.to_timestamp("day").alias("time"),
            F.lit(None).cast("string").alias("line_id"),
        )
    )
    return _with_rate(
        _sweep(windows, days).select(
            "dataset_version",
            "seller_sku",
            F.col("time").alias("event_timestamp"),
            F.col("n").cast("bigint").alias("labeled_count_90d"),
            F.col("p").cast("bigint").alias("positive_count_90d"),
        )
    )


def build_gold(silver: dict, cutoff, run_id: str, computed_at=None) -> dict:
    """Dựng fact, nhãn ảnh chụp, đoạn nhãn và feature PIT, chỉ từ Silver.

    Cần `line_lifecycle`, `native_order_lines`, `observed_evidence`, `coverage_validity` (do
    `order_lifecycle_lakehouse.build_silver` tạo với `as_of = cutoff`).
    """
    spark = silver["line_lifecycle"].sparkSession
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    distribute_python_packages(spark.sparkContext)
    computed = computed_at or datetime.now(timezone.utc)
    native = silver["native_order_lines"].select(
        "dataset_version", "channel", "native_order_id", "native_line_id", "seller_sku"
    )
    native_key = ["dataset_version", "channel", "native_order_id", "native_line_id"]
    # Ánh xạ mơ hồ thì báo lỗi, không lặng lẽ bỏ hay gộp line nguồn.
    if native.groupBy(*native_key).count().filter("count != 1").limit(1).count():
        raise ValueError("Ambiguous native line identity")
    base = silver["line_lifecycle"].join(native, native_key, "left")
    statuses = {}
    for row in silver["coverage_validity"].collect():
        statuses.setdefault(row["dataset_version"], {})[row["evidence_event_id"]] = row[
            "receipt_status"
        ]
    pairs = line_evidence(base.select(*IDENTITY), silver["observed_evidence"])
    keyless = keyless_evidence(silver["observed_evidence"])
    grouped = (
        base.select(*IDENTITY)
        .join(pairs.select(*KEY, "raw_json"), KEY, "left")
        .groupBy(*IDENTITY)
        .agg(F.collect_list("raw_json").alias("raw_records"))
    )
    inputs = spark.createDataFrame(
        grouped.rdd.map(lambda row: _input_row(row, statuses, keyless)), INPUT_DDL
    ).cache()
    fact = base.join(inputs.select(*KEY, "created_at"), KEY, "left").withColumn(
        "generation_id", F.lit(run_id)
    )
    labels = label_inputs(inputs, cutoff, run_id, computed).cache()
    segments = (
        spark.createDataFrame(inputs.rdd.flatMap(_segment_rows), SEGMENT_DDL)
        .withColumn("generation_id", F.lit(run_id))
        .cache()
    )
    features = (
        sku_features(fact, segments)
        .withColumn("event_timestamp", F.col("created_at"))
        .withColumn("created", F.lit(computed).cast("timestamp"))
        .withColumn("label_version", F.lit(LABEL_VERSION))
        .withColumn("generation_id", F.lit(run_id))
    )
    return dict(
        fact_order_lines=fact,
        label_return_v2=labels,
        feat_sku_return_rate_90d=features,
        label_segments=segments,
        label_inputs=inputs,
    )


def _recompute_feature(target, history_inputs):
    """Oracle độc lập theo định nghĩa gốc: `label_line` tại thời điểm đặt của line đích."""
    timestamp = utc(target["created_at"])
    n = p = 0
    for candidate in history_inputs:
        if candidate["line_id"] == target["line_id"]:
            continue
        result = label_line(json.loads(candidate["input_json"]), timestamp)
        if (
            result.state == "labeled"
            and result.maturity_end < timestamp
            and result.decision_observed_at < timestamp
            and timestamp.date() - timedelta(days=90) <= result.cohort_date < timestamp.date()
        ):
            n += 1
            p += result.label
    return n, p


def validate_gold(
    silver: dict, gold: dict, cutoff, sample_limit: int = 200, feature_sample: int = 50
) -> dict:
    """Đếm vi phạm; feature PIT của một mẫu được tính lại bằng `label_line` thuần.

    Oracle mẫu không dùng chung code với đường quét/đoạn, nên feature đếm nhầm một nhãn chưa
    biết lúc đặt (hoặc bỏ sót) sẽ hiện thành `pit_feature_mismatch`.
    """
    fact, labels, feat = (
        gold[name] for name in ("fact_order_lines", "label_return_v2", "feat_sku_return_rate_90d")
    )
    spark = fact.sparkSession
    distribute_python_packages(spark.sparkContext)
    checks = {}
    checks["fact_line_count_delta"] = abs(fact.count() - silver["line_lifecycle"].count())

    def total(frame):
        """So tổng tiền (đơn vị nhỏ nhất), kể cả khi dataset rỗng."""
        return frame.agg(F.coalesce(F.sum("headline_product_net_minor"), F.lit(0))).first()[0]

    checks["fact_headline_delta_minor"] = abs(total(fact) - total(silver["line_lifecycle"]))
    checks["duplicate_fact_lines"] = fact.groupBy(*KEY).count().filter("count != 1").count()
    expected_keys = fact.select(*KEY)
    checks["missing_or_extra_label_lines"] = (
        expected_keys.join(labels.select(*KEY), KEY, "left_anti").count()
        + labels.select(*KEY).join(expected_keys, KEY, "left_anti").count()
    )
    checks["duplicate_labels"] = labels.groupBy(*KEY, "cutoff").count().filter("count != 1").count()
    checks["invalid_label_state"] = labels.filter(
        (~F.col("state").isin("labeled", "unknown", "immature", "ineligible"))
        | F.col("state").isNull()
        | ((F.col("state") == "labeled") & (F.col("label").isNull() | ~F.col("label").isin(0, 1)))
        | ((F.col("state") != "labeled") & F.col("label").isNotNull())
        | (
            F.col("label").eqNullSafe(F.lit(1))
            & ~F.coalesce(
                F.col("positive_kind").isin("return_and_refund", "refund_only", "return_only"),
                F.lit(False),
            )
        )
        | (~F.col("label").eqNullSafe(F.lit(1)) & F.col("positive_kind").isNotNull())
    ).count()
    checks["labeled_not_mature"] = labels.filter(
        (F.col("state") == "labeled")
        & (F.col("maturity_end").isNull() | (F.col("maturity_end") >= F.col("cutoff")))
    ).count()
    generations = [r[0] for r in labels.select("generation_id").distinct().collect()]
    checks["generation_mismatch"] = (
        int(len(generations) != 1 or generations[0] is None) if generations else 1
    )
    if len(generations) == 1 and generations[0] is not None:
        for frame in (fact, feat, gold["label_segments"]):
            checks["generation_mismatch"] += frame.filter(
                ~F.col("generation_id").eqNullSafe(F.lit(generations[0]))
            ).count()
    checks["wrong_cutoff"] = labels.filter(
        F.col("cutoff").isNull() | (F.col("cutoff") != F.lit(utc(cutoff)))
    ).count()
    checks["missing_feature_clock_or_sku"] = feat.filter(
        F.col("event_timestamp").isNull()
        | F.col("created").isNull()
        | F.col("seller_sku").isNull()
        | (F.col("event_timestamp") != F.col("created_at"))
    ).count()
    checks["future_created_lines"] = fact.filter(F.col("created_at") >= F.lit(utc(cutoff))).count()
    checks["feature_grain_mismatch"] = (
        abs(feat.count() - fact.count()) + feat.groupBy(*KEY).count().filter("count != 1").count()
    )
    checks["wrong_label_version"] = (
        labels.filter(
            F.col("label_version").isNull() | (F.col("label_version") != LABEL_VERSION)
        ).count()
        + feat.filter(
            F.col("label_version").isNull() | (F.col("label_version") != LABEL_VERSION)
        ).count()
    )
    checks["feature_rate_mismatch"] = feat.filter(
        F.col("labeled_count_90d").isNull()
        | F.col("positive_count_90d").isNull()
        | (F.col("positive_count_90d") > F.col("labeled_count_90d"))
        | (F.col("positive_count_90d") < 0)
        | ((F.col("labeled_count_90d") == 0) & F.col("return_or_refund_rate_90d").isNotNull())
        | (
            (F.col("labeled_count_90d") > 0)
            & ~F.col("return_or_refund_rate_90d").eqNullSafe(
                F.col("positive_count_90d") / F.col("labeled_count_90d")
            )
        )
    ).count()
    # PIT: đoạn chỉ mở khi đã qua cả thời điểm quyết định lẫn hạn chờ.
    checks["pit_segment_before_known"] = (
        gold["label_segments"]
        .filter(
            F.col("decision_observed_at").isNull()
            | F.col("maturity_end").isNull()
            | (F.col("start_exclusive") < F.col("decision_observed_at"))
            | (F.col("start_exclusive") < F.col("maturity_end"))
        )
        .count()
    )
    # Tính lại feature PIT trên mẫu, độc lập với code đoạn/quét.
    targets = (
        feat.filter(F.col("created_at").isNotNull())
        .withColumn("h", F.xxhash64(*KEY))
        .orderBy("h", *KEY)
        .limit(feature_sample)
        .select(*KEY, "seller_sku", "created_at", "labeled_count_90d", "positive_count_90d")
    )
    history = (
        gold["label_inputs"]
        .join(fact.select(*KEY, "seller_sku"), KEY)
        .select(
            "dataset_version", "seller_sku", F.col("line_id").alias("history_line_id"), "input_json"
        )
    )
    # Gom theo (dataset, SKU): lịch sử một SKU nạp MỘT lần cho mọi target của SKU đó. Gom theo
    # từng target thì SKU nóng bị chép lại cho mỗi target (từng gây OOM).
    history = history.join(
        targets.select("dataset_version", "seller_sku").distinct(),
        ["dataset_version", "seller_sku"],
        "left_semi",
    )

    def sku_key(row):
        """Khoá gom của một dòng target hoặc lịch sử."""
        return (row["dataset_version"], row["seller_sku"])

    def oracle(group):
        """Group = (SKU key, (targets, history)); đếm target có feature lệch định nghĩa gốc."""
        _, (target_rows, history_rows) = group
        inputs = [
            dict(line_id=r["history_line_id"], input_json=r["input_json"])
            for r in history_rows
            if r["input_json"]
        ]
        return sum(
            int(
                _recompute_feature(dict(line_id=t["line_id"], created_at=t["created_at"]), inputs)
                != (t["labeled_count_90d"], t["positive_count_90d"])
            )
            for t in target_rows
        )

    mismatches = (
        (targets.rdd.keyBy(sku_key).cogroup(history.rdd.keyBy(sku_key)).map(oracle).sum())
        if targets.limit(1).count()
        else 0
    )
    checks["pit_feature_mismatch"] = int(mismatches)
    sample = gold["label_inputs"].orderBy(*KEY).limit(sample_limit)
    inputs = {tuple(row[k] for k in KEY): row["input_json"] for row in sample.collect()}
    rows = labels.join(sample.select(*KEY), KEY).collect()
    mismatch = 0
    for row in rows:
        pure = asdict(label_line(json.loads(inputs[tuple(row[k] for k in KEY)]), cutoff))
        for name, value in pure.items():
            actual_value = row[name]
            if isinstance(value, datetime):
                actual_value = utc(actual_value) if actual_value else None
            if actual_value != value:
                mismatch += 1
                break
    checks["python_spark_label_mismatch"] = mismatch
    return {
        "checks": checks,
        "info": {
            "fact_rows": fact.count(),
            "sampled_labels": len(rows),
            "sampled_features": min(feature_sample, feat.count()),
        },
    }


def write_receipt(path, receipt):
    """In kết quả kiểm và, nếu có đường dẫn, ghi receipt để xem lại."""
    if path:
        receipt.setdefault("completed_at", completed_at_utc())
    text = json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2)
    print(text)
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n")
