#!/usr/bin/env bash
# Submit job Flink stream feature (flink_jobs/ml/sku_return_feature.sql), chạy nền.
# Credential PostgreSQL: biến môi trường nếu có, không thì đọc Vault (VAULT_ADDR + VAULT_TOKEN,
# cách CI dùng), cuối cùng mới đọc .env khi chạy tay. Job cùng tên đang chạy bị huỷ trước ⇒ deploy lại không sinh hai job song song.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ -z "${POSTGRES_PASSWORD:-}" && -n "${VAULT_TOKEN:-}" ]]; then
  eval "$(scripts/setup/vault_env.sh postgres)"
elif [[ -z "${POSTGRES_PASSWORD:-}" && -f .env ]]; then
  set -a; . ./.env; set +a
fi
: "${POSTGRES_DB:?}" "${POSTGRES_USER:?}" "${POSTGRES_PASSWORD:?}"
rest="http://localhost:${FLINK_UI_PORT:-8082}"
name=ml-sku-return-24h
for id in $(curl -fsS "$rest/jobs/overview" | python3 -c "import json,sys
for j in json.load(sys.stdin)['jobs']:
    if j['name'] == '$name' and j['state'] == 'RUNNING': print(j['jid'])"); do
  curl -fsS -X PATCH "$rest/jobs/$id?mode=cancel" >/dev/null && echo "đã huỷ job cũ $id"
done
sql=$(sed -e "s|@@PG_DB@@|${POSTGRES_DB}|" -e "s|@@PG_USER@@|${POSTGRES_USER}|" \
  -e "s|@@PG_PASSWORD@@|${POSTGRES_PASSWORD}|" flink_jobs/ml/sku_return_feature.sql)
docker compose --profile flink exec -T flink-jobmanager bash -c \
  'f=$(mktemp); cat > "$f"; ./bin/sql-client.sh -f "$f"; rm -f "$f"' <<<"$sql"
