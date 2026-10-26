#!/usr/bin/env bash
# Dựng Langfuse v2 trong kind (ns langfuse), DB = database `langfuse` trên pgvector, rồi nạp 3 system prompt của Agent
# (chỉ khi Langfuse chưa có prompt đó) và đồng bộ sang ConfigMap. Khoá API sinh 1 lần, giữ trong Secret.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ -z "${POSTGRES_USER:-}" ]; then set -a; . ./.env; set +a; fi
KC="kubectl --context kind-platform"
$KC -n rag exec deploy/pgvector -- sh -c \
  "psql -U \"\$POSTGRES_USER\" -d rag -tc \"SELECT 1 FROM pg_database WHERE datname='langfuse'\" | grep -q 1 \
   || psql -U \"\$POSTGRES_USER\" -d rag -c 'CREATE DATABASE langfuse'"
$KC apply -f infra/k8s/agents/langfuse.yaml
get() { $KC -n langfuse get secret langfuse-env -o jsonpath="{.data.$1}" 2>/dev/null | base64 -d || true; }
PK=$(get LANGFUSE_INIT_PROJECT_PUBLIC_KEY); SK=$(get LANGFUSE_INIT_PROJECT_SECRET_KEY)
NA=$(get NEXTAUTH_SECRET); SALT=$(get SALT); PW=$(get LANGFUSE_INIT_USER_PASSWORD)
[ -n "$PK" ] || PK="pk-lf-$(openssl rand -hex 12)"; [ -n "$SK" ] || SK="sk-lf-$(openssl rand -hex 16)"
[ -n "$NA" ] || NA=$(openssl rand -hex 32); [ -n "$SALT" ] || SALT=$(openssl rand -hex 16); [ -n "$PW" ] || PW=$(openssl rand -hex 12)
$KC -n langfuse create secret generic langfuse-env --dry-run=client -o yaml \
  --from-literal=DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@pgvector.rag.svc.cluster.local:5432/langfuse" \
  --from-literal=NEXTAUTH_SECRET="$NA" --from-literal=SALT="$SALT" \
  --from-literal=LANGFUSE_INIT_PROJECT_PUBLIC_KEY="$PK" --from-literal=LANGFUSE_INIT_PROJECT_SECRET_KEY="$SK" \
  --from-literal=LANGFUSE_INIT_USER_EMAIL=admin@platform.local --from-literal=LANGFUSE_INIT_USER_PASSWORD="$PW" \
  | $KC apply -f -
# Khoá cho script đồng bộ / Jenkins (ns kagent), cùng giá trị.
$KC -n kagent create secret generic langfuse-api --dry-run=client -o yaml \
  --from-literal=LANGFUSE_PUBLIC_KEY="$PK" --from-literal=LANGFUSE_SECRET_KEY="$SK" | $KC apply -f -
$KC -n langfuse rollout restart deploy/langfuse >/dev/null
$KC -n langfuse rollout status deploy/langfuse --timeout=600s
python3 scripts/setup/sync_agent_prompts.py --seed
