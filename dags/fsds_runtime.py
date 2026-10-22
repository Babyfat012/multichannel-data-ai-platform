"""Pure mapping from Airflow metadata to environment variables for Spark jobs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

_GROUPS = {
    "iceberg": "fsds_minio",
    "postgres": "fsds_postgres",
    "kafka": "fsds_kafka",
}


def resolve_runtime_env(
    connections: Mapping[str, Mapping[str, object]],
    variables: Mapping[str, str],
    groups: Sequence[str],
    *,
    storage: str = "minio",
) -> dict[str, str]:
    """Build a minimal Spark environment from selected Airflow connection groups."""
    unknown = set(groups) - _GROUPS.keys()
    if unknown:
        raise ValueError(f"unknown runtime connection group(s): {', '.join(sorted(unknown))}")
    if storage not in {"minio", "s3"}:
        raise ValueError(f"unknown runtime storage: {storage!r}")

    env: dict[str, str] = {}
    if "iceberg" in groups:
        env.update(
            {
                "WAREHOUSE_NAME": variables["fsds_warehouse_name"],
                "LAKEKEEPER_CATALOG_URI": variables["fsds_lakekeeper_catalog_uri"],
            }
        )
        if storage == "minio":
            conn = connections[_GROUPS["iceberg"]]
            env.update(
                {
                    "AWS_ACCESS_KEY_ID": str(conn["login"]),
                    "AWS_SECRET_ACCESS_KEY": str(conn["password"]),
                    "MINIO_INTERNAL_ENDPOINT": variables["fsds_minio_internal_endpoint"],
                }
            )
    if "postgres" in groups:
        conn = connections[_GROUPS["postgres"]]
        env.update(
            {
                "POSTGRES_HOST": str(conn["host"]),
                "POSTGRES_PORT": str(conn["port"] or 5432),
                "POSTGRES_DB": str(conn["schema"]),
                "POSTGRES_USER": str(conn["login"]),
                "POSTGRES_PASSWORD": str(conn["password"]),
            }
        )
    if "kafka" in groups:
        conn = connections[_GROUPS["kafka"]]
        env.update(
            {
                "KAFKA_BOOTSTRAP_SERVERS": f"{conn['host']}:{conn['port'] or 9092}",
                "KAFKA_TOPIC": variables["fsds_dp1_cdc_topic"],
            }
        )
    return env
