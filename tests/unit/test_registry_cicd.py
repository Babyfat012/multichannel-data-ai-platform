"""Kiểm hợp đồng phát hành image và cổng CI, hoàn toàn offline."""

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    """Đọc cấu hình trong repo."""
    return (ROOT / path).read_text()


def test_no_direct_kind_image_loading():
    """Các script/CI phải phát hành image, không nạp trực tiếp vào node."""
    for folder in ("scripts", "ci"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix not in (".sh", ".Jenkinsfile"):
                continue
            for line in path.read_text().splitlines():
                if not line.lstrip().startswith(("#", "//")):
                    assert not re.search(r"\bkind\s+load\b", line), path


def test_build_pipelines_publish_before_deploy():
    """Mọi pipeline build phải có Push trước deploy đầu tiên."""
    for path in (ROOT / "ci/jenkins").glob("*.Jenkinsfile"):
        text = path.read_text()
        stages = re.findall(r"stage\('([^']+)'\)", text)
        if "Build" in stages:
            deploy = next(i for i, name in enumerate(stages) if name.startswith("Deploy"))
            assert stages.index("Build") < stages.index("Push") < deploy, path
            assert "scripts/run/push_images.sh" in text
    for name, deploy in (("feature-api-agents", "Deploy Agent"), ("coordinator", "Deploy")):
        text = read(f"ci/jenkins/{name}.Jenkinsfile")
        stages = re.findall(r"stage\('([^']+)'\)", text)
        assert stages.index("Eval agent") > stages.index(deploy)
        assert "eval_agent_ci.sh" in text
        assert "archiveArtifacts" in text and "out/agent-eval*.json*" in text


def test_registry_and_pull_configuration():
    """Registry chỉ bind loopback, có volume, node pull qua địa chỉ chuẩn."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    registry = compose["services"]["registry"]
    assert registry["image"] == "registry:2.8.3"
    assert registry["ports"] == ["127.0.0.1:5001:5000"]
    assert "registry-data:/var/lib/registry" in registry["volumes"]
    assert compose["networks"]["kind"]["external"] is True
    values = yaml.safe_load(read("infra/helm/feature-api/values.yaml"))
    assert values["image"]["repository"] == "localhost:5001/platform-feature-api"
    assert values["image"]["pullPolicy"] == "IfNotPresent"
    assert "localhost:5001" in read("ml/training_pipeline.py")
    assert 'set_image_pull_policy(task, "IfNotPresent")' in read("ml/training_pipeline.py")
    assert "imagePullPolicy: IfNotPresent" in read("infra/k8s/ml/return-risk.yaml")
    setup = read("scripts/setup/setup_kind_registry.sh")
    assert "/etc/containerd/certs.d/localhost:5001/hosts.toml" in setup
    assert 'server = "http://registry:5000"' in setup
    assert "namespace: kube-public" in setup
    assert "kind create" not in setup and "kind delete" not in setup


@pytest.mark.parametrize(
    "script",
    [
        "scripts/setup/setup_kind_registry.sh",
        "scripts/setup/setup_letsencrypt.sh",
        "scripts/run/push_images.sh",
        "scripts/run/compose_release_images.sh",
        "scripts/run/eval_agent_ci.sh",
        "scripts/setup/deploy_kind_feature_api.sh",
        "scripts/run/deploy_return_risk.sh",
        "scripts/run/run_training_pipeline.sh",
        "scripts/setup/up_ml_base.sh",
        "scripts/setup/vault_env.sh",
    ],
)
def test_shell_syntax(script):
    """Parse bash, không chạy hạ tầng hay mạng."""
    subprocess.run(["bash", "-n", str(ROOT / script)], check=True)


def test_letsencrypt_disabled_without_domain():
    """Nhánh mặc định vẫn chạy khi không có công cụ hạ tầng trong PATH."""
    env = {**os.environ, "DUCKDNS_DOMAIN": "", "PATH": "/nonexistent"}
    result = subprocess.run(
        ["/bin/bash", str(ROOT / "scripts/setup/setup_letsencrypt.sh")],
        env=env,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0
    assert "Giữ CA nội bộ" in result.stdout
    assert not result.stderr


def test_push_retags_to_immutable_release(tmp_path):
    """Docker giả ghi lại lệnh: inspect → tag release SHA → push cùng địa chỉ."""
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$DOCKER_LOG"\n')
    docker.chmod(0o755)
    log = tmp_path / "docker.log"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "DOCKER_LOG": str(log),
        "TAG": "abc123",
        "REGISTRY": "localhost:5001",
    }
    subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        check=True,
    )
    assert log.read_text().splitlines() == [
        "image inspect platform-feast:0.66.0",
        "tag platform-feast:0.66.0 localhost:5001/platform-feast:abc123",
        "push localhost:5001/platform-feast:abc123",
    ]
    log.unlink()
    env["TAG"] = "latest"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/run/push_images.sh"), "platform-feast:0.66.0"],
        env=env,
        capture_output=True,
    )
    assert result.returncode != 0
    assert not log.exists()


def test_compose_release_matches_published_tag():
    """Các service dùng chung image nhận cùng địa chỉ đã push, kể cả taskmanager."""
    result = subprocess.run(
        [
            "bash",
            "scripts/run/compose_release_images.sh",
            "flink-jobmanager=platform-flink",
            "flink-taskmanager=platform-flink",
        ],
        cwd=ROOT,
        env={**os.environ, "REGISTRY": "localhost:5001", "TAG": "abc123"},
        text=True,
        capture_output=True,
        check=True,
    )
    services = yaml.safe_load(result.stdout)["services"]
    assert {v["image"] for v in services.values()} == {"localhost:5001/platform-flink:abc123"}


def test_eval_gate_propagates_failure_and_archives_summary_contract():
    """pipefail giữ exit 1 của harness qua tee; ngưỡng CI được ghi tường minh."""
    text = read("scripts/run/eval_agent_ci.sh")
    assert "set -euo pipefail" in text
    assert "--min-accuracy 0.95" in text
    assert "tee out/agent-eval-summary.json" in text
    assert "flushall" not in text


def test_registry_setup_repeated_on_existing_nodes(tmp_path):
    """Hai lượt setup ghi cùng hosts/ConfigMap trên cả hai node, không tạo lại cluster."""
    import json
    import sys

    fake = tmp_path / "fake.py"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys\n"
        "tool = pathlib.Path(sys.argv[0]).name\n"
        "args = sys.argv[1:]\n"
        'body = ""\n'
        'if tool == "kind":\n'
        '    assert args == ["get", "nodes", "--name", "platform"]\n'
        '    print("platform-control-plane\\nplatform-worker")\n'
        'elif "containerd" in args:\n'
        '    print("config_path = \\"/etc/containerd/certs.d\\"")\n'
        'elif tool == "kubectl" or "cp" in args:\n'
        "    body = sys.stdin.read()\n"
        'with open(os.environ["CALL_LOG"], "a") as f:\n'
        '    f.write(json.dumps([tool, args, body]) + "\\n")\n'
    )
    fake.chmod(0o755)
    for tool in ("docker", "kind", "kubectl"):
        (tmp_path / tool).symlink_to(fake)
    log = tmp_path / "calls.jsonl"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALL_LOG": str(log),
        "KIND_CLUSTER": "platform",
    }
    for _ in range(2):
        subprocess.run(
            ["bash", str(ROOT / "scripts/setup/setup_kind_registry.sh")],
            env=env,
            check=True,
            capture_output=True,
        )
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    hosts = [call for call in calls if call[0] == "docker" and "cp" in call[1]]
    assert len(hosts) == 4
    assert len({call[2] for call in hosts}) == 1
    assert {call[1][2] for call in hosts} == {"platform-control-plane", "platform-worker"}
    assert 'server = "http://registry:5000"' in hosts[0][2]
    configs = [yaml.safe_load(call[2]) for call in calls if call[0] == "kubectl"]
    assert configs[0] == configs[1]
    assert configs[0]["metadata"] == {"name": "local-registry-hosting", "namespace": "kube-public"}
