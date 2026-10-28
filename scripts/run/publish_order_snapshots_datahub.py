"""Đưa bảng, Airflow job, lineage và kết quả Validate đã chạy lên DataHub.

Chạy sau khi DataHub GMS sẵn sàng. Không chạy lại Spark/Airflow; dữ liệu kiểm
được đọc từ manifest bằng chứng, mỗi assertion giữ run_id gốc trong properties.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

CHANNELS = ("amazon", "walmart", "tiktok_shop", "shopify", "shopee")
BRONZE = {c: f"lakehouse.bronze.raw_{c}_orders_dp1" for c in CHANNELS}
SILVER = {c: f"lakehouse.silver.stg_{c}_order_lines_dp2" for c in CHANNELS}
FACT = "lakehouse.gold.fact_order_lines_dp2"
DIM = "lakehouse.gold.dim_amazon_order_dp2"
FEATURE = "lakehouse.gold.feat_order_line_return_dp3"
LABELS = "lakehouse.gold.fact_return_labels_dp3"
# Bảng nhãn chỉ-Walmart (DAG dp3_walmart_labels cũ) giữ trên DataHub như bằng chứng lịch sử;
# DAG dp3_return_labels nay ghi một bảng nhãn cho cả năm Channel.
LABELS_LEGACY = "lakehouse.gold.fact_walmart_return_labels_dp3"
# DAG dp1_cdc hiện ghi bảng v2 (có source_epoch). Bảng v1 giữ trên DataHub như bằng chứng
# lịch sử của run 2026-10-13; không còn job nào ghi vào nó.
CDC = "lakehouse.bronze.raw_walmart_cdc_v2"
CDC_LEGACY = "lakehouse.bronze.raw_walmart_cdc"
DATASETS = (*BRONZE.values(), *SILVER.values(), FACT, DIM, FEATURE, LABELS, LABELS_LEGACY, CDC, CDC_LEGACY)
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
RAW_AMAZON = "/data/raw/source-exports/channel=amazon/dataset=orders"
RAW_FILES = {
    channel: f"/data/channels-small/source-exports/channel={channel}/dataset=orders"
    for channel in ("tiktok_shop", "shopify", "shopee")
}
RAW_WALMART = "walmart.orders"
KAFKA_WALMART_CDC = "platform_cdc.walmart.orders"
SOURCE_DATASETS = {
    RAW_AMAZON: "file",
    **{name: "file" for name in RAW_FILES.values()},
    RAW_WALMART: "postgres",
    KAFKA_WALMART_CDC: "kafka",
}


def raw_sources(raw_root: str | None) -> dict[str, str]:
    """Đường nguồn file theo Channel; ``raw_root`` (vd. s3a://bucket/.../source-exports) thay bộ local."""
    if raw_root is None:
        return {"amazon": RAW_AMAZON, **RAW_FILES}
    return {
        c: f"{raw_root.rstrip('/')}/channel={c}/dataset=orders"
        for c in ("amazon", "tiktok_shop", "shopify", "shopee")
    }


def job_map(
    raw_root: str | None = None, dags: tuple[str, ...] | None = None
) -> dict[str, dict[str, tuple[list[str], list[str]]]]:
    """Airflow task -> bảng đọc/ghi thật theo DAG của dự án; chỉ giữ ``dags`` nếu có."""
    files = raw_sources(raw_root)
    dp1: dict[str, tuple[list[str], list[str]]] = {}
    dp2: dict[str, tuple[list[str], list[str]]] = {}
    for channel in CHANNELS:
        source = RAW_WALMART if channel == "walmart" else files[channel]
        dp1[f"ingest_{channel}"] = ([source], [BRONZE[channel]])
        dp1[f"validate_{channel}"] = ([BRONZE[channel]], [])
        dp2[f"ingest_{channel}_silver"] = ([BRONZE[channel]], [SILVER[channel]])
        dp2[f"validate_{channel}_silver"] = ([SILVER[channel]], [])
    dp2["build_gold_order_lines"] = (list(SILVER.values()), [FACT])
    dp2["build_amazon_order_dim"] = ([BRONZE["amazon"]], [DIM])
    dp2["validate_amazon_order_dim"] = ([DIM, FACT], [])
    dp2["validate_gold_order_lines"] = ([FACT], [])
    jobs = {
        "dp1_bronze": dp1,
        "dp1_cdc": {
            "ingest_walmart_cdc": ([KAFKA_WALMART_CDC], [CDC]),
            "validate_walmart_cdc": ([CDC], []),
        },
        "dp2_silver": dp2,
        "dp3_features": {
            "build_return_features": ([FACT], [FEATURE]),
            "validate_return_features": ([FEATURE], []),
        },
        "dp3_return_labels": {
            "build_and_validate_return_labels": ([FACT], [LABELS]),
        },
    }
    return {dag: tasks for dag, tasks in jobs.items() if dags is None or dag in dags}


def datasets_of(jobs: dict[str, dict[str, tuple[list[str], list[str]]]]) -> list[str]:
    """Bảng/nguồn mà các job đã chọn đọc hoặc ghi, giữ thứ tự xuất hiện."""
    names = (n for tasks in jobs.values() for io in tasks.values() for side in io for n in side)
    return list(dict.fromkeys(names))


def platform_of(name: str) -> str:
    """Nền tảng DataHub của một dataset theo tiền tố/tên của nó."""
    if name.startswith(("s3a://", "s3://")):
        return "s3"
    return SOURCE_DATASETS.get(name, "iceberg")


def description(name: str, platform: str) -> str:
    """Mô tả dataset; bảng CDC v1 ghi rõ là bản lịch sử."""
    if name == CDC_LEGACY:
        return f"PLATFORM {platform}: {name} (legacy CDC v1, bằng chứng run 2026-10-13; DAG hiện ghi {CDC})"
    if name == LABELS_LEGACY:
        return f"PLATFORM {platform}: {name} (legacy nhãn chỉ Walmart; DAG hiện ghi {LABELS})"
    return f"PLATFORM {platform}: {name}"


def assertion_urn(item: dict) -> str:
    """URN assertion; check trên bảng v2 có hậu tố riêng để không đè lịch sử của bảng v1."""
    suffix = "-v2" if item["dataset"].endswith("_v2") else ""
    return f"urn:li:assertion:platform-{item['dag_id']}-{item['task_id']}{suffix}"


def check_passed(item: dict) -> None:
    """Từ chối publish SUCCESS khi task chưa thành công hoặc có metric lỗi khác 0."""
    if item["dataset"] not in DATASETS:
        raise ValueError(f"Bảng assertion không thuộc mapping: {item['dataset']}")
    if item["state"] != "success":
        raise ValueError(f"Task Validate chưa thành công: {item['task_id']}")
    if any(
        value != 0 for key, value in item["metrics"].items() if key.startswith(FAILURE_PREFIXES)
    ):
        raise ValueError(f"Số liệu Validate không PASS: {item['task_id']}")


def main() -> None:
    """Publish dataset lineage and validation assertions to DataHub GMS."""
    parser = argparse.ArgumentParser(
        description="Publish PLATFORM lineage and validation to DataHub"
    )
    parser.add_argument("--server", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"))
    parser.add_argument(
        "--evidence", type=Path, default=Path("docs/evidence/datahub-validation.json")
    )
    parser.add_argument(
        "--raw-root",
        help="Gốc nguồn file thật, vd. s3a://<raw-bucket>/order_snapshots/source-exports (AWS)",
    )
    parser.add_argument(
        "--dags", nargs="+", choices=sorted(job_map()), help="Chỉ publish các DAG đã chạy thật"
    )
    args = parser.parse_args()
    jobs = job_map(args.raw_root, tuple(args.dags) if args.dags else None)
    # Không chọn DAG: giữ cả bảng lịch sử không còn job ghi (CDC v1) như trước.
    names = datasets_of(jobs) + ([] if args.dags else list(DATASETS))
    names = list(dict.fromkeys(names))
    evidence = json.loads(args.evidence.read_text())

    # Import tại điểm chạy để file vẫn đọc được khi client chưa được cài.
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
    from datahub.metadata.urns import DatasetUrn
    from datahub.sdk import DataFlow, DataHubClient, DataJob, Dataset

    token = os.getenv("DATAHUB_GMS_TOKEN")
    client = DataHubClient(server=args.server, token=token)
    graph = DataHubGraph(config=DatahubClientConfig(server=args.server, token=token))

    def urn(name: str) -> DatasetUrn:
        """Resolve a dataset URN using its actual storage platform."""
        return DatasetUrn(platform=platform_of(name), name=name, env="PROD")

    for name in names:
        platform = platform_of(name)
        client.entities.upsert(
            Dataset(
                platform=platform,
                name=name,
                description=description(name, platform),
            )
        )

    for dag_id, tasks in jobs.items():
        flow = DataFlow(
            platform="airflow", name=dag_id, description=f"PLATFORM Airflow DAG {dag_id}"
        )
        client.entities.upsert(flow)
        for task_id, (inputs, outputs) in tasks.items():
            client.entities.upsert(
                DataJob(
                    name=task_id,
                    flow=flow,
                    inlets=[urn(x) for x in inputs],
                    outlets=[urn(x) for x in outputs],
                )
            )
            for source in inputs:
                for target in outputs:
                    client.lineage.add_lineage(upstream=urn(source), downstream=urn(target))

    for item in evidence["checks"]:
        check_passed(item)
        if item["dataset"] not in names:
            raise ValueError(f"Assertion trên bảng ngoài các DAG đã chọn: {item['dataset']}")
        dataset = item["dataset"]
        metrics = item["metrics"]
        urn_text = assertion_urn(item)
        graph.upsert_custom_assertion(
            urn=urn_text,
            entity_urn=str(urn(dataset)),
            type="Airflow Spark Validate",
            description=f"{item['dag_id']}.{item['task_id']} kiểm {dataset}",
            platform_name="PLATFORM Airflow",
            logic=item["rule"],
        )
        completed = datetime.fromisoformat(item["completed_at"].replace("Z", "+00:00"))
        graph.report_assertion_result(
            urn=urn_text,
            timestamp_millis=int(completed.timestamp() * 1000),
            type="SUCCESS",
            properties=[
                {"key": "run_id", "value": item["run_id"]},
                *({"key": key, "value": str(value)} for key, value in metrics.items()),
            ],
        )
    print(
        f"DataHub: {len(names)} datasets, "
        f"{sum(map(len, jobs.values()))} jobs, "
        f"{len(evidence['checks'])} kết quả Validate"
    )


if __name__ == "__main__":
    main()
