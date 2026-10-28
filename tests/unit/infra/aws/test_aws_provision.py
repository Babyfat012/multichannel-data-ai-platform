# Mục đích: kiểm offline các chốt an toàn của provision AWS (Terraform, Ansible, Compose, Spark).
"""Kiểm offline Terraform/Ansible/Compose AWS bằng text và YAML, không gọi Cloud."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from spark_jobs.lib.iceberg_session import catalog_conf

ROOT = Path(__file__).resolve().parents[4]
TF_DIR = ROOT / "infra/aws/terraform"
ANSIBLE = ROOT / "infra/aws/ansible"
CODE = re.sub(
    r"(?m)^\s*#.*$", "", "\n".join(p.read_text(encoding="utf-8") for p in TF_DIR.glob("*.tf"))
)


class ComposeLoader(yaml.SafeLoader):
    """Đọc được tag `!override` của Compose."""


ComposeLoader.add_constructor(
    "!override",
    lambda loader, node: (
        loader.construct_mapping(node)
        if isinstance(node, yaml.MappingNode)
        else loader.construct_sequence(node)
    ),
)


def block(kind: str, name: str) -> str:
    """Lấy đúng một block cấp cao theo loại và tên."""
    found = re.findall(rf'(?ms)^{kind}\s+"{re.escape(name)}"[^\n]*\{{\n.*?^\}}', CODE)
    assert len(found) == 1, (kind, name)
    return found[0]


def test_security_group_has_no_ingress_and_no_access_keys():
    """SSH đi qua SSM: không ingress, không access key, không user_data cài runtime."""
    sg = block("resource", "aws_security_group")
    assert not re.search(r"ingress\s*\{", sg)
    assert "aws_vpc_security_group_ingress_rule" not in CODE
    assert "aws_iam_access_key" not in CODE
    assert "user_data" not in CODE
    assert "AmazonSSMManagedInstanceCore" in CODE


def test_instance_uses_imdsv2_hop_two_and_encrypted_disks():
    """Container lấy được credential (hop 2); IMDSv1 bị tắt; ổ được mã hóa."""
    vm = block("resource", "aws_instance")
    assert re.search(r'http_tokens\s*=\s*"required"', vm)
    assert re.search(r"http_put_response_hop_limit\s*=\s*2", vm)
    assert re.search(r"associate_public_ip_address\s*=\s*true", vm)
    assert len(re.findall(r"encrypted\s*=\s*true", CODE)) == 2
    assert not re.search(r'resource\s+"aws_nat_gateway"', CODE)


def test_iam_only_touches_the_two_buckets():
    """Policy chỉ trỏ vào ARN của bucket do Terraform tạo, không có `*` toàn cục."""
    policy = re.search(r'(?ms)^data "aws_iam_policy_document" "buckets".*?^\}', CODE)[0]
    assert 'resources = ["*"]' not in policy
    assert "aws_s3_bucket.data" in policy


def test_buckets_private_versioned_with_bounded_lifecycle():
    """Chặn public, tắt ACL, bật versioning, chỉ dọn bản cũ và prefix tmp/."""
    assert re.search(r'for_each\s*=\s*toset\(\["raw", "lakehouse"\]\)', CODE)
    pab = block("resource", "aws_s3_bucket_public_access_block")
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{flag}\s*=\s*true", pab)
    assert '"BucketOwnerEnforced"' in CODE
    assert re.search(r'status\s*=\s*"Enabled"', block("resource", "aws_s3_bucket_versioning"))
    life = block("resource", "aws_s3_bucket_lifecycle_configuration")
    assert 'prefix = "tmp/"' in life
    assert "noncurrent_days = var.noncurrent_retention_days" in life
    assert "force_destroy = var.force_destroy" in CODE


def test_budget_optional_excludes_credits_and_needs_email():
    """Budget tắt mặc định, không trừ credit, 3 ngưỡng, bắt buộc có email."""
    budget = block("resource", "aws_budgets_budget")
    assert re.search(r"count\s*=\s*var.enable_budget\s*\?\s*1\s*:\s*0", budget)
    assert re.search(r"include_credit\s*=\s*false", budget)
    assert "[50, 90, 100]" in budget
    assert "var.budget_email != null" in budget


@pytest.mark.parametrize(
    ("name", "default"),
    [
        ("region", '"ap-southeast-1"'),
        ("instance_type", '"t3.xlarge"'),
        ("cpu_credits", '"standard"'),
        ("enable_budget", "false"),
        ("force_destroy", "false"),
    ],
)
def test_defaults(name, default):
    """Mặc định rẻ và an toàn cho người dùng free tier."""
    assert re.search(rf"(?m)^\s*default\s*=\s*{re.escape(default)}\s*$", block("variable", name))


def test_no_default_ssh_key_and_outputs_for_ansible():
    """Không có khóa mặc định; đủ output Ansible cần."""
    assert "default" not in block("variable", "ssh_public_key")
    for name in ("raw_bucket", "lakehouse_bucket", "instance_id", "data_volume_id", "region"):
        block("output", name)
    ports = block("output", "port_forward_command")
    assert "AWS-StartSSHSession" in ports
    assert "-i ~/.ssh/platform_aws" in ports
    for port in (8080, 8081, 8181, 9002, 8089, 8082):
        assert f"-L 127.0.0.1:{port}:127.0.0.1:{port}" in ports


def test_region_and_machine_size_match_the_runtime():
    """Chặn AZ khác region và máy 8 GiB không đủ cho overlay 14 GiB."""
    assert 'startswith(var.availability_zone, "${var.region}")' in CODE
    assert '"t3.large"' not in block("variable", "instance_type")


def test_overlay_fits_c6i_2xlarge_without_keys_or_gcs():
    """Service chạy thường trực vừa 8 vCPU/16 GiB (chừa 2 vCPU), port loopback, không key, không GCS."""
    text = (ROOT / "docker-compose.aws.yml").read_text()
    services = yaml.load(text, Loader=ComposeLoader)["services"]
    for svc in services.values():
        assert all(port.startswith("127.0.0.1:") for port in svc.get("ports", []))
    assert "AWS_ACCESS_KEY_ID" not in text and "gcs" not in text.lower()
    running = [
        "postgres",
        "lakekeeper",
        "spark-master",
        "spark-worker-1",
        "spark-worker-2",
        "airflow",
    ]

    def gib(value: str) -> float:
        """Đổi mem_limit Compose (g/m) ra GiB."""
        return float(value[:-1]) / (1024 if value.endswith("m") else 1)

    assert sum(float(services[n]["cpus"]) for n in running) <= 6
    assert sum(gib(str(services[n]["mem_limit"])) for n in running) <= 15
    for name in ("minio", "kafka", "flink-jobmanager", "trino"):
        assert services[name]["profiles"] == ["disabled-aws"]
    assert services["airflow"]["environment"][
        "AIRFLOW_VAR_PLATFORM_ORDER_LIFECYCLE_SOURCE_ROOT"
    ].startswith("s3a://")


def test_ansible_finds_ebs_by_volume_id_and_guards_mkfs():
    """Đường dẫn nvme theo volume ID; mkfs chỉ chạy khi blkid báo trống."""
    path = yaml.safe_load((ANSIBLE / "roles/data_disk/vars/main.yml").read_text())["data_disk_path"]
    assert path.startswith("/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_")
    tasks = yaml.safe_load((ANSIBLE / "roles/data_disk/tasks/main.yml").read_text())
    fmt = next(t for t in tasks if "mkfs.ext4" in str(t.get("ansible.builtin.command", "")))
    assert fmt["when"] == "disk_blkid.rc == 2"
    variables = yaml.safe_load((ANSIBLE / "group_vars/all.example.yml").read_text())
    assert all(len(p.split(":")) == 3 for p in variables["spark_packages_aws"].split(","))
    for path in ANSIBLE.rglob("*"):
        if path.is_file():
            assert not re.search(r"gcloud|gs://|google-", path.read_text()), path


def test_s3_catalog_has_region_and_no_static_keys():
    """Nhánh s3: S3FileIO + region, không endpoint MinIO, không access key."""
    conf = catalog_conf({"PLATFORM_STORAGE": "s3", "AWS_REGION": "ap-southeast-1"})
    p = "spark.sql.catalog.lakehouse"
    assert conf[p + ".io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf[p + ".client.region"] == "ap-southeast-1"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-southeast-1.amazonaws.com"
    assert p + ".s3.endpoint" not in conf
    assert not any("key" in k.lower() for k in conf)
    with pytest.raises(KeyError):
        catalog_conf({"PLATFORM_STORAGE": "s3"})


def test_datahub_is_prepared_without_starting_and_all_ports_are_loopback():
    """SDK/Compose cho người vận hành, secret tạo một lần; không chạy pha B lúc provision."""
    site = yaml.safe_load((ANSIBLE / "site.yml").read_text())
    role = next(r for r in site[0]["roles"] if r["role"] == "datahub")
    assert "enable_datahub" in role["when"]
    variables = yaml.safe_load((ANSIBLE / "group_vars/all.example.yml").read_text())
    assert variables["enable_datahub"] is True
    tasks = yaml.safe_load((ANSIBLE / "roles/datahub/tasks/main.yml").read_text())
    assert "acryl-datahub==1.7.0.14" in str(tasks)
    assert not any("docker compose" in str(t.get("ansible.builtin.command", "")) for t in tasks)
    secret = next(t for t in tasks if t.get("no_log"))
    assert secret["ansible.builtin.command"]["creates"].endswith(".local-secrets.env")
    services = yaml.load(
        (ROOT / "docker/datahub-compose.override.yml").read_text(), Loader=ComposeLoader
    )["services"]
    for name in (
        "datahub-gms-quickstart",
        "frontend-quickstart",
        "mysql",
        "kafka-broker",
        "opensearch",
    ):
        assert all(p.startswith("127.0.0.1:") for p in services[name]["ports"])


def test_stream_overlay_only_enables_kafka_flink_on_loopback():
    """Overlay Stream bật đúng Kafka + Flink, cổng loopback, đầu ra trên EBS; không bật MinIO."""
    services = yaml.load(
        (ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader
    )["services"]
    assert set(services) == {"kafka", "flink-jobmanager", "flink-taskmanager", "debezium-connect"}
    for svc in services.values():
        assert all(port.startswith("127.0.0.1:") for port in svc.get("ports", []))
    # File gốc mở 0.0.0.0 cho Kafka và Flink UI: overlay phải ghi đè cả hai.
    assert services["kafka"]["ports"] and services["flink-jobmanager"]["ports"]
    for name in ("flink-jobmanager", "flink-taskmanager"):
        assert "/mnt/platform-data/flink:/opt/flink/output" in services[name]["volumes"]


def test_aws_overlay_points_walmart_labels_at_s3():
    """dp3_walmart_labels trên AWS: executor không có /data local nên đáp án và khóa phải ở S3."""
    env = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["environment"]
    assert env["AIRFLOW_VAR_PLATFORM_DP3_GROUND_TRUTH"].startswith("s3a://")
    assert env["AIRFLOW_VAR_PLATFORM_DP3_WALMART_KEYS"].startswith("s3a://")


def test_docker_storage_moves_to_ebs_without_silent_empty_store():
    """Kho Docker và containerd bind sang EBS; systemd chờ mount; chép không --delete, giữ bản cũ."""
    tasks = yaml.safe_load((ANSIBLE / "roles/data_disk/tasks/main.yml").read_text())
    text = (ANSIBLE / "roles/data_disk/tasks/main.yml").read_text()
    binds = [t for t in tasks if t.get("ansible.posix.mount", {}).get("fstype") == "none"]
    assert len(binds) == 1 and binds[0]["loop"] == ["containerd", "docker"]
    assert (
        "x-systemd.requires-mounts-for=/mnt/platform-data"
        in binds[0]["ansible.posix.mount"]["opts"]
    )
    assert "RequiresMountsFor=/var/lib/containerd /var/lib/docker" in text
    move = next(t for t in tasks if "block" in t)
    rsync = next(
        t["ansible.builtin.command"]["argv"]
        for t in move["block"]
        if t.get("ansible.builtin.command", {}).get("argv", [""])[0] == "rsync"
    )
    assert "--delete" not in rsync and "-aHAXx" in rsync
    commands = [
        t["ansible.builtin.command"] for t in move["block"] if "ansible.builtin.command" in t
    ]
    mv = next(c for c in commands if c["argv"][0] == "mv")
    assert mv["creates"].endswith(".pre-ebs")
    # Thứ tự: mount EBS → dời kho → bind mount.
    names = [t["name"] for t in tasks]
    assert (
        names.index("Ghi mount UUID và mount nếu chưa có")
        < names.index(move["name"])
        < names.index(binds[0]["name"])
    )


def test_cdc_on_aws_enables_debezium_and_loads_dp1_cdc_variables():
    """dp1_cdc trên EC2: Kafka thuộc cả profile cdc, Debezium bật ở loopback, Airflow nạp Variable CDC."""
    services = yaml.load(
        (ROOT / "docker-compose.aws-stream.yml").read_text(), Loader=ComposeLoader
    )["services"]
    assert "cdc" in services["kafka"]["profiles"]
    assert services["debezium-connect"]["profiles"] == ["cdc"]
    assert services["debezium-connect"]["scale"] == 1
    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    s3_branch = setup[setup.index("= s3 ]; then") :]
    assert "dp1_cdc" in s3_branch.split("done")[0]
    assert "spark-sql-kafka-0-10" in s3_branch and "PLATFORM_SPARK_PACKAGES_AWS" in s3_branch


def _mb(value: str) -> int:
    """'2560m' / '3g' -> MB."""
    return int(value[:-1]) * (1024 if value.endswith("g") else 1)


def test_spark_worker_promise_fits_container_limit():
    """Worker không được hứa RAM vượt trần container: executor nhỏ nhất (1536m) xếp đầy + overhead + JVM worker."""
    services = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]
    for name in ("spark-worker-1", "spark-worker-2"):
        worker = _mb(services[name]["environment"]["SPARK_WORKER_MEMORY"])
        executors = worker // 1536
        assert worker + executors * 384 + 512 <= _mb(services[name]["mem_limit"]), name
