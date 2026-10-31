"""Vault: key agentgateway chép từ Secret EKS cả khi chạy trên EC2 (host không có kubeconfig)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (ROOT / "scripts/setup/setup_vault.sh").read_text()


def test_agentgateway_key_is_read_through_jenkins_kubeconfig_on_ec2():
    """Thiếu nhánh s3 ⇒ kubectl host trỏ kind, lỗi bị nuốt ⇒ Vault không có key, eval CI 404."""
    s3 = SCRIPT[SCRIPT.index('if [[ "${PLATFORM_STORAGE:-minio}" == s3 ]]; then') :].split("else")[
        0
    ]
    assert "docker compose exec -T jenkins kubectl --kubeconfig /run/secrets/kubeconfig" in s3
    assert 'if agw=$("${KC[@]}" -n agentgateway get secret agentgateway-keys' in SCRIPT
