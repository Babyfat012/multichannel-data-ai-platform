"""Validate the actual merged 32-GiB Compose profile without running cloud workloads."""

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def test_performance_merge_fits_vm_and_keeps_cloud_auth(tmp_path):
    """Spark receives extra capacity, one driver at a time, and no MinIO keys leak into S3 jobs."""
    envfile = tmp_path / ".env"
    envfile.write_text(
        (ROOT / ".env.example").read_text()
        + "\nAWS_REGION=ap-southeast-2\nRAW_BUCKET=test-raw\nLAKEHOUSE_BUCKET=test-lake\n"
        + "PLATFORM_SPARK_PACKAGES_AWS=org.example:test:1\n"
    )
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--project-directory",
            str(tmp_path),
            "--env-file",
            str(envfile),
            "-f",
            str(ROOT / "docker-compose.yml"),
            "-f",
            str(ROOT / "docker-compose.aws.yml"),
            "-f",
            str(ROOT / "docker-compose.aws-performance.yml"),
            "--profile",
            "airflow",
            "--profile",
            "spark",
            "config",
            "--format",
            "json",
        ],
        env={
            **os.environ,
            "AWS_REGION": "ap-southeast-2",
            "RAW_BUCKET": "test-raw",
            "LAKEHOUSE_BUCKET": "test-lake",
            "PLATFORM_SPARK_PACKAGES_AWS": "org.example:test:1",
        },
        text=True,
        capture_output=True,
        check=True,
    )
    services = json.loads(result.stdout)["services"]
    runtime = [
        services[name]
        for name in [
            "postgres",
            "lakekeeper",
            "spark-master",
            "spark-worker-1",
            "spark-worker-2",
            "airflow",
        ]
    ]
    assert sum(float(svc["cpus"]) for svc in runtime) <= 8
    assert sum(int(svc["mem_limit"]) for svc in runtime) < 30 * 1024**3
    for name in ["spark-master", "spark-worker-1", "spark-worker-2", "airflow"]:
        env = services[name]["environment"]
        assert env["PLATFORM_STORAGE"] == "s3"
        assert "AWS_ACCESS_KEY_ID" not in env and "AWS_SECRET_ACCESS_KEY" not in env
    worker = services["spark-worker-1"]
    assert int(worker["environment"]["SPARK_WORKER_CORES"]) >= 2
    assert int(worker["mem_limit"]) > 5 * 1024**3
    assert services["airflow"]["environment"]["AIRFLOW__CORE__PARALLELISM"] == "1"
