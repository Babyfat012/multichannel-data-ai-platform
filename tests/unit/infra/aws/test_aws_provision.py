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
    # Ngoại lệ duy nhất: MLflow nghe thêm IP riêng VPC cho pod EKS (SG chỉ mở 5050 từ SG cluster).
    private = "${PLATFORM_PRIVATE_IP:?PLATFORM_PRIVATE_IP_required}:${MLFLOW_PORT:-5050}:5000"
    for name, svc in services.items():
        ports = [p for p in svc.get("ports", []) if not (name == "mlflow" and p == private)]
        assert all(port.startswith("127.0.0.1:") for port in ports), name
    assert "AWS_ACCESS_KEY_ID" not in text and "gcs" not in text.lower()
    running = [
        "postgres",
        "lakekeeper",
        "spark-master",
        "spark-worker-1",
        "spark-worker-2",
        "airflow",
        "mlflow",
    ]

    def gib(value: str) -> float:
        """Đổi mem_limit Compose (g/m) ra GiB."""
        return float(value[:-1]) / (1024 if value.endswith("m") else 1)

    # cpus là trần, không giữ chỗ: 6 cho Data Platform + 0,25 MLflow (rảnh trừ lúc KFP register model).
    # Từ 29/10 EC2 là r6i.xlarge (4 vCPU/32 GiB): trần CPU cộng lại vượt 4 là có chủ đích (Spark nặng đã
    # chạy xong; job còn lại nhỏ), RAM thì dư gấp đôi.
    assert sum(float(services[n]["cpus"]) for n in running) <= 6.25
    assert sum(gib(str(services[n]["mem_limit"])) for n in running) <= 15
    for name in ("minio", "kafka", "flink-jobmanager"):
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


def test_aws_overlay_points_return_labels_at_s3():
    """dp3_return_labels trên AWS: executor không có /data local nên đáp án và khóa phải ở S3."""
    env = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["environment"]
    assert env["AIRFLOW_VAR_PLATFORM_DP3_GROUND_TRUTH"].startswith("s3a://")
    assert env["AIRFLOW_VAR_PLATFORM_DP3_WALMART_KEYS"].startswith("s3a://")


def test_aws_feast_uses_s3_iam_and_eks_redis():
    """Feast trên AWS: kho S3 thật + Redis EKS, không còn MinIO/kind; key MinIO chỉ ở nhánh local."""
    env = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["environment"]
    assert env["AIRFLOW_VAR_PLATFORM_FEAST_ROOT"] == "s3://${LAKEHOUSE_BUCKET}/feature-store"
    assert env["AIRFLOW_VAR_PLATFORM_FEAST_REDIS"] == "${PLATFORM_FEAST_REDIS:-}"
    assert env["AIRFLOW_VAR_PLATFORM_ML_CUTOFF"] == "2026-06-30T00:00:00Z"
    assert env["PLATFORM_STORAGE"] == "s3"
    volumes = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["airflow"]["volumes"]
    assert "./feature_repo:/opt/feature_repo:ro" in volumes  # volumes !override từng làm mất mount
    template = (ANSIBLE / "roles/app/templates/env.j2").read_text()
    assert "PLATFORM_FEAST_REDIS={{ feast_redis | default('10.42.0.121:30379')" in template
    dag = (ROOT / "dags/ml_feature_materialize.py").read_text()
    branch = dag.index('if os.environ.get("PLATFORM_STORAGE", "minio") != "s3":')
    assert branch < dag.index('get_connection("platform_minio")') < dag.index("env |= {")


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


def test_aws_maintenance_targets_a_configured_real_table():
    """Nhánh S3 thay bảng demo: mọi bảng trong platform_maintenance_tables phải có chính sách trong file AWS."""
    import json

    from spark_jobs.lib.maintenance_policy import validated_config

    setup = (ROOT / "scripts/setup/setup_airflow.sh").read_text()
    s3_branch = setup[setup.index("= s3 ]; then") :].split("\nfi\n")[0]
    assert "< configs/iceberg_maintenance_aws.json" in s3_branch
    tables = json.loads(re.search(r"platform_maintenance_tables '(\[.*?\])'", s3_branch).group(1))
    policies = json.loads((ROOT / "configs/iceberg_maintenance_aws.json").read_text())["tables"]
    assert tables and set(tables) <= set(policies)
    for name in tables:
        assert not name.startswith("lakehouse.demo.")
        config = validated_config(policies[name])
        assert config["orphans_dry_run"] is True and config["zorder_columns"] == []


def test_docker_log_size_is_capped_before_containers_start():
    """Mỗi container giữ tối đa 300 MB log; áp trước role tạo container, đổi thì restart Docker."""
    import json

    tasks = yaml.safe_load((ANSIBLE / "roles/base/tasks/main.yml").read_text())
    daemon = next(
        t
        for t in tasks
        if t.get("ansible.builtin.copy", {}).get("dest") == "/etc/docker/daemon.json"
    )
    options = json.loads(daemon["ansible.builtin.copy"]["content"])
    assert options == {"log-driver": "json-file", "log-opts": {"max-size": "100m", "max-file": "3"}}
    restart = next(
        t for t in tasks if t.get("ansible.builtin.service", {}).get("state") == "restarted"
    )
    assert restart["when"] == f"{daemon['register']}.changed"
    roles = [r["role"] for r in yaml.safe_load((ANSIBLE / "site.yml").read_text())[0]["roles"]]
    assert roles.index("base") < roles.index("app") < roles.index("datahub")


def test_mlflow_on_aws_uses_s3_iam_and_private_listener():
    """MLflow EC2: không MinIO, artifact S3 thật, nghe loopback + IP riêng; Ansible bật đúng mlflow."""
    services = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]
    mlflow = services["mlflow"]
    assert "minio" not in mlflow["depends_on"]
    assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "MLFLOW_S3_ENDPOINT_URL"} & set(
        mlflow["environment"]
    )
    command = mlflow["command"]
    assert (
        command[command.index("--artifacts-destination") + 1] == "s3://${LAKEHOUSE_BUCKET}/mlflow"
    )
    assert mlflow["ports"] == [
        "127.0.0.1:${MLFLOW_PORT:-5050}:5000",
        "${PLATFORM_PRIVATE_IP:?PLATFORM_PRIVATE_IP_required}:${MLFLOW_PORT:-5050}:5000",
    ]
    assert (
        "PLATFORM_PRIVATE_IP={{ ansible_default_ipv4.address"
        in (ANSIBLE / "roles/app/templates/env.j2").read_text()
    )
    tasks = (ANSIBLE / "roles/app/tasks/main.yml").read_text()
    assert "docker compose --profile ml up -d --wait --wait-timeout 180 mlflow" in tasks


def test_kfp_on_eks_reads_s3_by_pod_identity_without_minio():
    """Image train không ghi cứng MinIO; pipeline-runner chỉ đọc feature-store/*; bản aws bỏ Secret."""
    dockerfile = (ROOT / "docker/Dockerfile.train").read_text()
    assert "minio:9000" not in dockerfile and "s3://platform/" not in dockerfile
    identity = (ROOT / "infra/aws/kubernetes/identity.tf").read_text()
    assert (
        'pipeline_reader = { namespace = "kubeflow", service_account = "pipeline-runner" }'
        in identity
    )
    block = identity[identity.index('resource "aws_iam_role_policy" "pipeline_reader"') :]
    assert "data.aws_iam_policy_document.feature_reader.json" in block.split("}")[0]
    reader = identity[identity.index('data "aws_iam_policy_document" "feature_reader"') :]
    reader = reader[: reader.index('resource "aws_iam_role_policy" "feature_reader"')]
    assert '"s3:GetObject"' in reader and "Put" not in reader and "Delete" not in reader
    pipeline = (ROOT / "ml/training_pipeline.py").read_text()
    assert 'SECRET = "platform-minio" if TARGET == "kind" else None' in pipeline
    script = (ROOT / "scripts/run/run_training_pipeline.sh").read_text()
    assert 'if [[ "$PIPELINE_TARGET" == aws ]]' in script and "REGISTRY phải là ECR" in script


def test_kserve_on_eks_reads_registry_by_pod_identity():
    """return-risk: SA riêng có role chỉ-đọc feature-store; Secret MinIO không bắt buộc; nhánh s3 không tạo Secret."""
    docs = list(yaml.safe_load_all((ROOT / "infra/k8s/ml/return-risk.yaml").read_text()))
    account = next(d for d in docs if d["kind"] == "ServiceAccount")
    service = next(d for d in docs if d["kind"] == "InferenceService")
    predictor = service["spec"]["predictor"]
    assert predictor["serviceAccountName"] == account["metadata"]["name"] == "return-risk"
    container = predictor["containers"][0]
    assert {"secretRef": {"name": "platform-minio", "optional": True}} in container["envFrom"]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["FEAST_REGISTRY"] == "@@FEAST_ROOT@@/registry.db"
    identity = (ROOT / "infra/aws/kubernetes/identity.tf").read_text()
    assert (
        'model_reader    = { namespace = "ml-serving", service_account = "return-risk" }'
        in identity
    )
    block = identity[identity.index('resource "aws_iam_role_policy" "model_reader"') :]
    assert "data.aws_iam_policy_document.feature_reader.json" in block.split("}")[0]
    script = (ROOT / "scripts/run/deploy_return_risk.sh").read_text()
    s3 = script[script.index("== s3 ]]; then") : script.index("\nelse\n")]
    assert "platform-minio" not in s3 and "REGISTRY phải là ECR" in s3


def test_training_sample_and_kfp_memory_fit_eks_nodes():
    """Tập train 2‰ (1% từng OOM node 8 GB); mọi bước KFP khai RAM, load nhiều nhất."""
    dag = (ROOT / "dags/ml_feature_materialize.py").read_text()
    assert '"--sample-per-mille",\n            "2",' in dag
    pipeline = (ROOT / "ml/training_pipeline.py").read_text()
    assert 'loaded.set_memory_request("2G").set_memory_limit("4G")' in pipeline
    assert 'task.set_memory_request("1G").set_memory_limit("3G")' in pipeline


def test_ec2_allows_memory_optimized_r6i_within_quota():
    """r6i.xlarge (4 vCPU/32 GiB) được phép: cùng quota vCPU, gấp đôi RAM cho DataHub/Trino/Kafka cùng lúc."""
    text = (ROOT / "infra/aws/terraform/variables.tf").read_text()
    block = text[text.index('variable "instance_type"') :]
    assert '"r6i.xlarge"' in block[: block.index("}\n}")]


def test_trino_on_aws_uses_iam_catalog_on_loopback_only_when_measuring():
    """Trino AWS: catalog S3 không key/endpoint MinIO, cổng loopback, chỉ bật theo profile query."""
    trino = yaml.load((ROOT / "docker-compose.aws.yml").read_text(), Loader=ComposeLoader)[
        "services"
    ]["trino"]
    assert trino["profiles"] == ["query"]
    assert trino["volumes"] == ["./trino/catalog-aws:/etc/trino/catalog:ro"]
    assert trino["ports"] == ["127.0.0.1:${TRINO_PORT:-8083}:8080"]
    assert "minio" not in trino["depends_on"]
    lake = (ROOT / "trino/catalog-aws/lakehouse.properties").read_text()
    assert "aws-access-key" not in lake and "minio" not in lake and "s3.endpoint" not in lake
    assert "iceberg.rest-catalog.warehouse=${ENV:WAREHOUSE_NAME}" in lake
