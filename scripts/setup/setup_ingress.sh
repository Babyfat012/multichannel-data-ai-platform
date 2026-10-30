#!/usr/bin/env bash
# Dựng route qua NGINX Ingress trong kind: CA nội bộ, Ingress HTTPS cho Feature API và UI agent
# (basic auth + rate limit). Mật khẩu UI agent sinh một lần, cất ở Vault secret/platform/agent-ui.
# Cần: ingress controller (helm nginx-ingress), cert-manager, Vault đã setup.
set -euo pipefail
cd "$(dirname "$0")/../.."
K="kubectl --context ${KCTX:-kind-platform}"
if [[ -n "${INGRESS_DOMAIN:-}" ]]; then
  [[ "$INGRESS_DOMAIN" =~ ^[a-zA-Z0-9.-]+$ ]] || { echo 'Invalid INGRESS_DOMAIN' >&2; exit 1; }
  host_suffix=$INGRESS_DOMAIN
  https_port=${INGRESS_HTTPS_PORT:-443}
elif [[ "${KCTX:-kind-platform}" == kind-* ]]; then
  node_ip=$(docker inspect platform-control-plane --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}')
  host_suffix="${node_ip//./-}.sslip.io"
  https_port=30443
else
  echo 'Set INGRESS_DOMAIN and point DNS at the cloud ingress before creating routes.' >&2
  exit 1
fi
$K apply -f infra/k8s/ingress/issuer.yaml >/dev/null
$K -n cert-manager wait --for=condition=Ready certificate/platform-local-ca --timeout=120s >/dev/null
# Cloud: INGRESS_ISSUER=platform-letsencrypt (cert Let's Encrypt thật, HTTP-01); mặc định CA nội bộ.
issuer=${INGRESS_ISSUER:-platform-local-ca}
if [[ "$issuer" == platform-letsencrypt ]]; then
  : "${LE_EMAIL:?Đặt LE_EMAIL cho tài khoản ACME}"
  [[ "$LE_EMAIL" =~ ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+$ ]] || { echo 'LE_EMAIL không hợp lệ' >&2; exit 1; }
  sed "s/@@LE_EMAIL@@/$LE_EMAIL/" infra/k8s/ingress/issuer-letsencrypt-http01.yaml | $K apply -f - >/dev/null
elif [[ "$issuer" != platform-local-ca ]]; then
  echo "INGRESS_ISSUER chỉ nhận platform-local-ca hoặc platform-letsencrypt" >&2; exit 1
fi
# Let's Encrypt lần đầu (Certificate chưa Ready; secret có thể đã tồn tại mà rỗng): redirect HTTPS (chưa có cert) và basic auth chặn HTTP-01
# ⇒ apply bản mở, chờ cert Ready, rồi apply bản đủ. ponytail: gia hạn ngày 60 vẫn bị basic auth chặn ⇒
# chạy lại script (gap #24); prod dùng mergeable ingress hoặc DNS-01.
apply_ing() {  # $1 namespace, $2 secret TLS; stdin = Ingress đã render
  local y; y=$(cat)
  if [[ "$issuer" == platform-letsencrypt ]] && [[ "$($K -n "$1" get certificate "$2" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" != True ]]; then
    sed -e '/nginx.org\/basic-auth/d' \
      -e 's|^\(    acme.cert-manager.io/http01-edit-in-place: .*\)$|\1\n    ingress.kubernetes.io/ssl-redirect: "false"|' \
      <<<"$y" | $K apply -f - >/dev/null
    for _ in $(seq 1 30); do $K -n "$1" get certificate "$2" >/dev/null 2>&1 && break; sleep 2; done
    $K -n "$1" wait --for=condition=Ready "certificate/$2" --timeout=300s >/dev/null || {
      echo "cert $1/$2 chưa Ready: kubectl --context ${KCTX:-kind-platform} -n $1 describe challenges" >&2; exit 1; }
  fi
  $K apply -f - <<<"$y" >/dev/null
}
export VAULT_ADDR="${VAULT_ADDR:-http://localhost:${VAULT_PORT:-8200}}"
# Vault ở EC2: truyền VAULT_TOKEN qua môi trường (tunnel 8200); local đọc init.json như cũ.
ROOT=${VAULT_TOKEN:-$(python3 -c 'import json;print(json.load(open("out/vault/init.json"))["root_token"])')}
api() { curl -fsS -H "X-Vault-Token: $ROOT" "$@"; }
if ! api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" >/dev/null 2>&1; then
  pw=$(python3 -c 'import secrets;print(secrets.token_urlsafe(15))')
  api -X POST "$VAULT_ADDR/v1/secret/data/platform/agent-ui" -d "{\"data\":{\"user\":\"seller\",\"password\":\"$pw\"}}" >/dev/null
fi
pw=$(api "$VAULT_ADDR/v1/secret/data/platform/agent-ui" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["data"]["password"])')
$K -n kagent create secret generic agent-ui-basic-auth --type=nginx.org/htpasswd \
  --from-literal=htpasswd="seller:$(openssl passwd -apr1 "$pw")" --dry-run=client -o yaml | $K apply -f - >/dev/null
for f in feature-store/feature-api kagent/agent-ui; do
  sed -e "s/@@NODE_IP_DASH@@.sslip.io/$host_suffix/g" -e "s/cluster-issuer: platform-local-ca/cluster-issuer: $issuer/" \
    "infra/k8s/ingress/${f#*/}.yaml" | apply_ing "${f%/*}" "${f#*/}-tls"
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
    -e "s/@@HOST@@/$host.$host_suffix/g" -e "s/cluster-issuer: platform-local-ca/cluster-issuer: $issuer/" \
    infra/k8s/ingress/ops-ui.yaml | apply_ing "$ns" "$name-tls"
  echo "route https://$host.$host_suffix:$https_port → $ns/$svc:$port"
done <<'ROUTES'
grafana observability grafana 80 grafana
prometheus observability prometheus-server 80 metrics
jaeger observability jaeger 16686 traces
agent-registry agentregistry agentregistry 12121 registry
ROUTES
printf 'https://api.%s:%s  https://agent.%s:%s (user seller, mật khẩu ở Vault secret/platform/agent-ui)\n' "$host_suffix" "$https_port" "$host_suffix" "$https_port"
