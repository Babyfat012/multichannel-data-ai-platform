"""Langfuse và agentregistry dùng DB trên pgvector ⇒ tài khoản phải lấy từ Secret rag-pg, không từ .env laptop."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize("script", ["deploy_kind_langfuse.sh", "deploy_kind_agentregistry.sh"])
def test_db_credentials_come_from_pgvector_secret(tmp_path, script):
    """30/10: .env lệch Vault ⇒ Langfuse/agentregistry ghi URL DB sai. Biến shell cũ cũng không được thắng Secret."""
    log = tmp_path / "calls.log"
    fake = tmp_path / "kubectl"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import base64,os,sys\n"
        "args=sys.argv[1:]\n"
        "open(os.environ['CALL_LOG'],'a').write(' '.join(args)+' STDIN:'+(sys.stdin.read() if '-f' in args else '')+'\\n')\n"
        "if 'rag-pg' in args:\n"
        "    key=[a for a in args if a.startswith('jsonpath=')][0].split('.')[-1].rstrip('}')\n"
        "    print(base64.b64encode(('secret-'+key).encode()).decode())\n"
        "sys.exit(1 if 'rollout' in args else 0)\n"
    )
    fake.chmod(0o755)
    # Dừng script sau bước ghi Secret DB: không chạm helm/rollout/seed thật.
    (tmp_path / "helm").write_text("#!/bin/sh\nexit 1\n")
    (tmp_path / "helm").chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALL_LOG": str(log),
        "KCTX": "fake",
        "POSTGRES_USER": "stale-user",
        "POSTGRES_PASSWORD": "stale-pass",
    }
    subprocess.run(
        ["bash", str(ROOT / "scripts/setup" / script)], env=env, capture_output=True, cwd=tmp_path
    )
    calls = log.read_text()
    assert "secret-POSTGRES_PASSWORD@pgvector" in calls
    assert "stale" not in calls
