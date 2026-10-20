# Mục đích: kiểm các ràng buộc an toàn Terraform K1 bằng text/regex, hoàn toàn offline.
"""Kiểm ràng buộc an toàn và hợp đồng Terraform GCP hoàn toàn offline."""
from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
TF_DIR = ROOT / "infra/gcp/terraform"
TF_FILES = sorted(TF_DIR.glob("*.tf"))
TF = "\n".join(path.read_text(encoding="utf-8") for path in TF_FILES)
# Chỉ bỏ comment nguyên dòng; không cần parser HCL/provider cho kiểm smoke này.
CODE = re.sub(r"(?m)^\s*#.*$", "", TF)


def blocks(kind: str, name: str, text: str = CODE) -> list[str]:
    """Lấy block cấp cao, dùng dấu } ở đầu dòng để giữ cả các block lồng nhau."""
    return re.findall(
        rf'(?ms)^{kind}\s+"{re.escape(name)}"[^\n]*\{{\n.*?^\}}', text
    )


def variable(name: str) -> str:
    """Lấy nội dung duy nhất của biến Terraform theo tên."""
    found = blocks("variable", name)
    assert len(found) == 1, f"Thiếu/trùng biến {name}"
    return found[0]


def test_firewall_only_allows_iap_ssh() -> None:
    """Kiểm firewall chỉ cho phép SSH qua dải địa chỉ IAP."""
    firewalls = blocks("resource", "google_compute_firewall")
    assert len(firewalls) == 1
    rule = firewalls[0]
    assert 'direction               = "INGRESS"' in rule
    assert re.search(r'source_ranges\s*=\s*\["35\.235\.240\.0/20"\]', rule)
    assert re.search(r'ports\s*=\s*\["22"\]', rule)
    assert re.search(r'protocol\s*=\s*"tcp"', rule)
    assert "target_service_accounts" in rule
    assert not re.search(r"0\.0\.0\.0/0|::/0", "\n".join(firewalls))


def test_no_service_account_keys_or_broad_vm_storage_iam() -> None:
    """Kiểm không tạo khóa service account và giới hạn quyền storage của VM."""
    assert "google_service_account_key" not in CODE
    project_roles = blocks("resource", "google_project_iam_member")
    assert len(project_roles) == 1
    assert set(re.findall(r'"(roles/[^\"]+)"', project_roles[0])) == {
        "roles/logging.logWriter", "roles/monitoring.metricWriter"
    }
    assert not blocks("resource", "google_project_iam_binding")
    assert not blocks("resource", "google_project_iam_policy")
    bucket_roles = blocks("resource", "google_storage_bucket_iam_member")
    assert len(bucket_roles) == 2
    assert set(re.findall(r'role\s*=\s*"([^\"]+)"', "\n".join(bucket_roles))) == {
        "roles/storage.objectAdmin", "roles/storage.legacyBucketReader"
    }
    for role in bucket_roles:
        assert "for_each = google_storage_bucket.data" in role
        assert "google_service_account.vm.email" in role


def test_two_private_regional_buckets_and_limited_lifecycle() -> None:
    """Kiểm hai bucket riêng tư cùng vùng và lifecycle có phạm vi giới hạn."""
    found = blocks("resource", "google_storage_bucket")
    assert len(found) == 1
    bucket = found[0]
    assert re.search(r'for_each\s*=\s*toset\(\["raw", "lakehouse"\]\)', bucket)
    assert re.search(r'public_access_prevention\s*=\s*"enforced"', bucket)
    assert re.search(r"uniform_bucket_level_access\s*=\s*true", bucket)
    assert re.search(r"location\s*=\s*var.region", bucket)
    assert 'name                        = "${var.name_prefix}-${var.project_id}-${each.value}"' in bucket
    assert re.search(r"force_destroy\s*=\s*var.force_destroy", bucket)
    assert re.search(r'with_state\s*=\s*"ARCHIVED"', bucket)
    assert re.search(r"days_since_noncurrent_time\s*=\s*var.noncurrent_retention_days", bucket)
    assert re.search(r'matches_prefix\s*=\s*\["tmp/"\]', bucket)
    assert re.search(r"age\s*=\s*var.temp_retention_days", bucket)
    assert re.search(r"versioning\s*\{\s*enabled\s*=\s*true", bucket)
    assert re.search(r"retention_duration_seconds\s*=\s*0", bucket)
    assert re.search(r"default\s*=\s*false", variable("force_destroy"))


def test_machine_default_and_every_allowed_type_fit_trial_limit() -> None:
    # Bảng CPU độc lập: E2 shared-core vẫn có 2 vCPU nhìn thấy, standard/high có suffix CPU.
    """Kiểm mọi machine type cho phép nằm trong giới hạn CPU trial."""
    cpus = {"e2-micro": 2, "e2-small": 2, "e2-medium": 2}
    cpus.update({f"e2-{family}-{n}": n for family in ("standard", "highmem", "highcpu") for n in (2, 4, 8)})
    config = variable("machine_type")
    default = re.search(r'default\s*=\s*"([^\"]+)"', config)
    assert default is not None
    assert default[1] == "e2-standard-8"
    assert cpus[default[1]] <= 8
    allowed = re.search(r"(?s)contains\(\[(.*?)\],\s*var.machine_type\)", config)
    assert allowed is not None
    names = re.findall(r'"([^\"]+)"', allowed[1])
    assert names
    assert default[1] in names
    assert all(name in cpus and cpus[name] <= 8 for name in names)


@pytest.mark.parametrize(
    ("name", "default"),
    [("region", '"asia-southeast1"'), ("zone", '"asia-southeast1-b"'),
     ("name_prefix", '"fsds"'), ("boot_disk_gb", "50"), ("data_disk_gb", "200"),
     ("enable_budget", "false"), ("billing_account_id", "null"), ("budget_amount_usd", "50")],
)
def test_brief_defaults(name: str, default: str) -> None:
    """Kiểm giá trị mặc định khớp brief Terraform."""
    assert re.search(rf"(?m)^\s*default\s*=\s*{re.escape(default)}\s*$", variable(name))


def test_no_real_project_or_billing_values_in_deliverables() -> None:
    """Kiểm deliverable không chứa project hoặc thông tin billing thật."""
    assert not re.search(r"\bdefault\s*=", variable("project_id"))
    # Chỉ kiểm file K1 sở hữu, không suy ra repo lịch sử hoặc file K2 không có ID thật.
    paths = [*TF_FILES, TF_DIR / "terraform.tfvars.example", TF_DIR / "README.md", ROOT / "docs/gcp-deploy.md"]
    for path in paths:
        content = path.read_text(encoding="utf-8")
        assert not re.search(r"\b[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}\b", content)
        for name, literal in re.findall(r'\b(project_id|billing_account(?:_id)?)\s*=\s*"([^\"]+)"', content):
            # ${...} là tham chiếu Terraform; YOUR_... là placeholder cố ý không hợp lệ.
            assert "${" in literal or literal.startswith("YOUR_"), (path, name, literal)
        assert "BEGIN PRIVATE KEY" not in content
        assert not re.search(r'"type"\s*:\s*"service_account"', content)
    example = (TF_DIR / "terraform.tfvars.example").read_text(encoding="utf-8")
    assert 'project_id = "YOUR_PROJECT_ID"' in example
    # project trong labels chỉ là nhãn; chỉ kiểm literal project trên resource/data.
    resource_text = "\n".join(re.findall(r'(?ms)^(?:resource|data)\s+"[^\"]+"[^\n]*\{\n.*?^\}', CODE))
    assert not re.search(r'(?m)^\s*project\s*=\s*"', resource_text)


@pytest.mark.parametrize("name", ["terraform.tfstate", "terraform.tfstate.backup", "other.tfstate.123", "terraform.tfvars", "secret.tfvars", "secret.tfvars.json", ".terraform/providers/registry/plugin", "de.tfplan"])
def test_gitignore_blocks_local_state_vars_and_provider_cache(name: str) -> None:
    """Kiểm gitignore loại bỏ state, biến local và cache provider."""
    patterns = [line.strip() for line in (TF_DIR / ".gitignore").read_text(encoding="utf-8").splitlines() if line.strip() and not line.startswith("#")]
    assert any(
        name.startswith(pattern.rstrip("/") + "/") if pattern.endswith("/")
        else fnmatch.fnmatchcase(name, pattern)
        for pattern in patterns
    ), name
    assert not any(fnmatch.fnmatchcase("terraform.tfvars.example", pattern) for pattern in patterns)


def test_vm_security_persistent_disk_and_no_startup_install() -> None:
    """Kiểm cấu hình bảo mật VM, persistent disk và không cài qua startup script."""
    vms = blocks("resource", "google_compute_instance")
    assert len(vms) == 1
    vm = vms[0]
    assert '"debian-cloud/debian-12"' in vm
    assert re.search(r"allow_stopping_for_update\s*=\s*true", vm)
    for flag in ("enable_secure_boot", "enable_vtpm", "enable_integrity_monitoring"):
        assert re.search(rf"{flag}\s*=\s*true", vm)
    assert re.search(r'enable-oslogin\s*=\s*"TRUE"', vm)
    assert re.search(r'scopes\s*=\s*\["cloud-platform"\]', vm)
    assert "startup-script" not in vm
    assert "metadata_startup_script" not in vm
    assert re.search(r"access_config\s*\{\s*\}", vm)
    attached = re.search(r"(?s)attached_disk\s*\{(.*?)\}", vm)
    assert attached is not None
    assert "google_compute_disk.data.id" in attached[1]
    # Schema attached_disk không nhận auto_delete; disk độc lập giữ vòng đời ngoài VM.
    assert "auto_delete" not in attached[1]
    assert len(blocks("resource", "google_compute_disk")) == 1
    assert len(re.findall(r'type\s*=\s*"pd-balanced"', CODE)) == 2
    assert not blocks("resource", "google_compute_address")
    assert not blocks("resource", "google_compute_router_nat")


def test_apis_and_optional_project_filtered_budget() -> None:
    """Kiểm API cần thiết và budget tùy chọn được lọc theo project."""
    for api in ("compute", "storage", "iam", "iap"):
        assert f'"{api}.googleapis.com"' in CODE
    assert 'var.enable_budget ? ["billingbudgets.googleapis.com"] : []' in CODE
    assert re.search(r"disable_on_destroy\s*=\s*false", CODE)
    budget = blocks("resource", "google_billing_budget")
    assert len(budget) == 1
    assert re.search(r"count\s*=\s*var.enable_budget\s*\?\s*1\s*:\s*0", budget[0])
    assert "var.billing_account_id != null" in budget[0]
    assert '"projects/${data.google_project.budget[0].number}"' in budget[0]
    assert set(re.findall(r"threshold_percent\s*=\s*([0-9.]+)", budget[0])) == {"0.5", "0.9", "1.0"}
    assert '"EXCLUDE_ALL_CREDITS"' in budget[0]


def test_provider_pinned_local_backend_and_k2_output_contract() -> None:
    """Kiểm pin provider, backend local và hợp đồng output cho K2."""
    versions = (TF_DIR / "versions.tf").read_text(encoding="utf-8")
    assert 'source  = "hashicorp/google"' in versions
    assert re.search(r'version\s*=\s*"6\.\d+\.\d+"', versions)
    assert re.search(r'required_version\s*=\s*">= 1\.6\.0', versions)
    assert not re.search(r'backend\s+"', CODE)
    for output in ("raw_bucket", "lakehouse_bucket", "vm_name", "zone", "service_account_email", "raw_bucket_uri", "lakehouse_bucket_uri", "ssh_command", "port_forward_command"):
        assert len(blocks("output", output)) == 1
    ports = blocks("output", "port_forward_command")[0]
    assert "--tunnel-through-iap" in ports
    for port in (8080, 8081, 8181):
        assert f"-L 127.0.0.1:{port}:127.0.0.1:{port}" in ports


def test_every_new_file_starts_with_vietnamese_purpose_comment() -> None:
    """Kiểm file hạ tầng mới mở đầu bằng chú thích mục đích tiếng Việt."""
    paths = [*TF_FILES, TF_DIR / ".gitignore", TF_DIR / "terraform.tfvars.example", TF_DIR / "README.md", ROOT / "docs/gcp-deploy.md", Path(__file__)]
    for path in paths:
        first = path.read_text(encoding="utf-8").splitlines()[0]
        assert first.startswith(("# ", "<!-- ")), path
