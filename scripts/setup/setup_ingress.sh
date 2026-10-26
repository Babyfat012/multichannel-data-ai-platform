#!/usr/bin/env bash
# Dựng route qua NGINX Ingress trong kind: CA nội bộ, Ingress HTTPS cho Feature API và UI agent
# (basic auth + rate limit). Mật khẩu UI agent sinh một lần, cất ở Vault secret/platform/agent-ui.
# Cần: ingress controller (helm nginx-ingress), cert-manager, Vault đã setup.
set -euo pipefail
cd "$(dirname "$0")/../.."
K="kubectl --context kind-platform"
node_ip=$(docker inspect platform-control-plane --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
dash=${node_ip//./-}
$K apply -f infra/k8s/ingress/issuer.yaml >/dev/null
$K -n cert-manager wait --for=condition=Ready certificate/platform-local-ca --timeout=120s >/dev/null
export VAULT_ADDR="http://localhost:${VAULT_PORT:-8200}"
ROOT=$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')
api() { curl -fsS -H "X-Vault-Token: $ROOT" "$@"; }
if ! api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(15))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/agent-ui" -d "{\"data\":{\"user\":\"seller\",\"password\":\"$pw\"}}" >/dev/null
fi
pw=$(api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["password"])')
$K -n kagent create secret generic agent-ui-basic-auth --type=nginx.org/htpasswd \
  --from-literal=htpasswd="seller:$(openssl passwd -apr1 "$pw")" --dry-run=client -o yaml | $K apply -f - >/dev/null
for f in feature-api agent-ui; do
  sed "s/@@NODE_IP_DASH@@/$dash/g" "infra/k8s/ingress/$f.yaml" | $K apply -f - >/dev/null
done
# UI vận hành: mật khẩu chung "ops" ở Vault secret/platform/ops-ui; route chỉ tạo khi service đã có.
if ! api "$VAULT_ADDR/v1/secret/data/platform/ops-ui" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(15))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/ops-ui" -d "{\"data\":{\"user\":\"ops\",\"password\":\"$pw\"}}" >/dev/null
fi
ops_pw=$(api "$VAULT_ADDR/v1/secret/data/platform/ops-ui" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["password"])')
# tên-route  namespace  service  cổng  host
while read -r name ns svc port host; do
  $K -n "$ns" get svc "$svc" >/dev/null 2>&1 || { echo "bỏ qua $name: chưa có $ns/$svc"; continue; }
  $K -n "$ns" create secret generic ops-basic-auth --type=nginx.org/htpasswd \
    --from-literal=htpasswd="ops:$(openssl passwd -apr1 "$ops_pw")" --dry-run=client -o yaml | $K apply -f - >/dev/null
  sed -e "s/@@NAME@@/$name/g" -e "s/@@NS@@/$ns/g" -e "s/@@SVC@@/$svc/g" -e "s/@@PORT@@/$port/g" \
    -e "s/@@HOST@@/$host.$dash.sslip.io/g" infra/k8s/ingress/ops-ui.yaml | $K apply -f - >/dev/null
  echo "route https://$host.$dash.sslip.io:30443 → $ns/$svc:$port"
done <<'ROUTES'
grafana observability grafana 80 grafana
prometheus observability prometheus-server 80 metrics
jaeger observability jaeger 16686 traces
agent-registry agentregistry agentregistry 12121 registry
ROUTES
printf 'https://api.%s.sslip.io:30443  https://agent.%s.sslip.io:30443 (user seller, mật khẩu ở Vault secret/platform/agent-ui)\n' "$dash" "$dash"
