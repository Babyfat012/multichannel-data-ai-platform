#!/usr/bin/env bash
# In lệnh export credential đọc từ Vault, để script khác dùng thay .env:
#   eval "$(scripts/setup/vault_env.sh postgres)"   → POSTGRES_USER/PASSWORD/DB
#   eval "$(scripts/setup/vault_env.sh minio)"      → MINIO_ROOT_USER/PASSWORD
#   eval "$(scripts/setup/vault_env.sh agentgateway)" → AGW_AGENT_KEY
# Cần VAULT_ADDR + VAULT_TOKEN (token chỉ-đọc policy fsds-read).
set -euo pipefail
: "${VAULT_ADDR:?}" "${VAULT_TOKEN:?}" "${1:?postgres|minio|agentgateway|duckdns}"
curl -fsS -H "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/secret/data/fsds/$1" | python3 -c '
import json, shlex, sys
kind, data = sys.argv[1], json.load(sys.stdin)["data"]["data"]
names = {"postgres": {"user": "POSTGRES_USER", "password": "POSTGRES_PASSWORD", "db": "POSTGRES_DB"},
         "minio": {"user": "MINIO_ROOT_USER", "password": "MINIO_ROOT_PASSWORD"},
         "agentgateway": {"AGW_AGENT_KEY": "AGW_AGENT_KEY"},
         "duckdns": {"token": "DUCKDNS_TOKEN"}}[kind]
for key, env in names.items():
    print(f"export {env}={shlex.quote(data[key])}")' "$1"
