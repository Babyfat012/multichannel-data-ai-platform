#!/usr/bin/env bash
# Init (lần đầu) + unseal Vault, bật KV v2, nạp secret của FSDS từ .env, tạo policy chỉ-đọc và token
# cho CI. Khoá unseal + root token lưu ở out/vault/ (gitignored, quyền 600) — chỉ dùng local;
# trên GKE thay bằng auto-unseal Cloud KMS. Chạy lại nhiều lần an toàn.
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; . ./.env; set +a
export VAULT_ADDR="http://localhost:${VAULT_PORT:-8200}"
docker compose --profile security up -d vault >/dev/null
for _ in $(seq 1 30); do curl -s -o /dev/null "$VAULT_ADDR/v1/sys/health" && break; sleep 1; done
mkdir -p out/vault && chmod 700 out/vault
api() { curl -fsS -H "X-Vault-Token: ${ROOT:-}" "$@"; }
if [[ "$(curl -s "$VAULT_ADDR/v1/sys/init" | python3 -c 'import json,sys;print(json.load(sys.stdin)["initialized"])')" == False ]]; then
  api -X PUT "$VAULT_ADDR/v1/sys/init" -d '{"secret_shares":1,"secret_threshold":1}' > out/vault/init.json
  chmod 600 out/vault/init.json
fi
KEY=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["keys"][0])')
ROOT=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')
api -X PUT "$VAULT_ADDR/v1/sys/unseal" -d "{\"key\":\"$KEY\"}" >/dev/null
api "$VAULT_ADDR/v1/sys/mounts" | grep -q '"secret/"' || \
  api -X POST "$VAULT_ADDR/v1/sys/mounts/secret" -d '{"type":"kv","options":{"version":"2"}}'
put() { api -X POST "$VAULT_ADDR/v1/secret/data/fsds/$1" -d "{\"data\":$2}" >/dev/null; }
put minio "{\"user\":\"$MINIO_ROOT_USER\",\"password\":\"$MINIO_ROOT_PASSWORD\"}"
put postgres "{\"user\":\"$POSTGRES_USER\",\"password\":\"$POSTGRES_PASSWORD\",\"db\":\"$POSTGRES_DB\"}"
# Key agent của agentgateway: nguồn là Secret k8s do helm chart tạo; bỏ qua nếu cụm chưa có.
if agw=$(kubectl --context kind-fsds-ml -n agentgateway get secret agentgateway-keys \
    -o jsonpath='{.data.AGW_AGENT_KEY}' 2>/dev/null | base64 -d) && [[ -n "$agw" ]]; then
  put agentgateway "{\"AGW_AGENT_KEY\":\"$agw\"}"
fi
api -X PUT "$VAULT_ADDR/v1/sys/policies/acl/fsds-read" \
  -d '{"policy":"path \"secret/data/fsds/*\" { capabilities = [\"read\"] }"}'
if [[ ! -s out/vault/ci-token ]]; then
  api -X POST "$VAULT_ADDR/v1/auth/token/create" -d '{"policies":["fsds-read"],"period":"768h","display_name":"fsds-ci"}' \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["auth"]["client_token"])' > out/vault/ci-token
  chmod 600 out/vault/ci-token
fi
printf 'Vault sẵn sàng: %s, secret ở secret/fsds/{minio,postgres}; token chỉ-đọc: out/vault/ci-token\n' "$VAULT_ADDR"
