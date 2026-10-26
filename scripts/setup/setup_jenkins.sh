#!/usr/bin/env bash
# Khởi động Jenkins (profile ci): mật khẩu admin sinh một lần và cất ở Vault (secret/platform/jenkins),
# token Vault chỉ-đọc cho credential `vault-ci-token`, kubeconfig của kind cho `kind-kubeconfig`.
# Cần Vault đã setup (scripts/setup/setup_vault.sh). UI: http://localhost:8090 (user admin).
set -euo pipefail
cd "$(dirname "$0")/../.."
export VAULT_ADDR="http://localhost:${VAULT_PORT:-8200}"
ROOT=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')
api() { curl -fsS -H "X-Vault-Token: $ROOT" "$@"; }
if ! api "$VAULT_ADDR/v1/secret/data/platform/jenkins" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(18))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/jenkins" -d "{\"data\":{\"admin_password\":\"$pw\"}}" >/dev/null
fi
export JENKINS_ADMIN_PASSWORD=$(api "$VAULT_ADDR/v1/secret/data/platform/jenkins" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["admin_password"])')
export VAULT_CI_TOKEN=$(cat out/vault/ci-token)
export DOCKER_GID=$(getent group docker | cut -d: -f3) PWD
mkdir -p out/jenkins out/jenkins_home
kind get kubeconfig --name platform > out/jenkins/kubeconfig && chmod 600 out/jenkins/kubeconfig
docker compose --profile ci up -d --build jenkins
for _ in $(seq 1 90); do curl -s -o /dev/null -w '%{http_code}' localhost:8090/login | grep -q 200 && break; sleep 2; done
printf 'Jenkins: http://localhost:8090  user admin, mật khẩu ở Vault secret/platform/jenkins\n'
