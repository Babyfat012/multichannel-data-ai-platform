<!-- Mục đích: NGINX Ingress cho các service MLE — HTTPS, basic auth, rate limit; bằng chứng rubric mle 37–43. -->
# Gateway — NGINX Ingress, HTTPS, auth

Rubric `mle` dòng 37–43: service đứng sau gateway (metric, log, trace, UI test agent, UI registry),
basic auth + rate limit cho UI test agent, domain + HTTPS cho Web API kéo dữ liệu.

## Chọn controller

Dự án cộng đồng **ingress-nginx** đã ngừng bảo trì (bản chart cuối 4.15.1, 03/2026, không còn bản
vá bảo mật). Dùng **F5 NGINX Ingress Controller** (mã nguồn mở, chart `nginx-ingress` 2.7.3 /
app 5.6.3) — vẫn là "NGINX Ingress Controller" như rubric yêu cầu, annotation dạng `nginx.org/*`.

```bash
helm --kube-context kind-fsds-ml install nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress \
  --version 2.7.3 -n ingress --create-namespace --set controller.service.type=NodePort \
  --set controller.service.httpPort.nodePort=30080 --set controller.service.httpsPort.nodePort=30443 \
  --set controller.ingressClass.name=nginx --set controller.ingressClass.setAsDefaultIngress=true
scripts/setup/setup_ingress.sh
```

| Route | Host (local) | Bảo vệ | File |
|---|---|---|---|
| Feature API (Web API kéo dữ liệu) | `api.<ip-node>.sslip.io` | HTTPS, redirect HTTP→HTTPS, 50 r/s | [`feature-api.yaml`](../infra/k8s/ingress/feature-api.yaml) |
| kagent UI (chat test agent) | `agent.<ip-node>.sslip.io` | HTTPS + **basic auth** + **5 r/s, burst 10, 429** | [`agent-ui.yaml`](../infra/k8s/ingress/agent-ui.yaml) |
| Prometheus (metric) | `metrics.<ip-node>.sslip.io` | HTTPS + basic auth `ops` | [`ops-ui.yaml`](../infra/k8s/ingress/ops-ui.yaml) |
| Grafana (metric + log qua Loki), Jaeger (trace) | `grafana.` / `traces.` | HTTPS + basic auth `ops` | cùng template |
| Agent registry UI (agentregistry 0.4.0, không có auth riêng) | `registry.` | HTTPS + basic auth `ops` | cùng template |

- **Domain**: sslip.io phân giải `api.172-25-0-2.sslip.io` → `172.25.0.2` (IP node kind) — có tên
  miền thật mà không phải mua. Trên GKE: domain riêng trỏ IP tĩnh của load balancer.
- **HTTPS**: cert-manager cấp cert từ CA nội bộ [`issuer.yaml`](../infra/k8s/ingress/issuer.yaml)
  (`fsds-local-ca`; xuất ra `out/ingress/fsds-local-ca.crt` để client tin). Trên GKE: ACME/Let's Encrypt.
- **Mật khẩu UI** sinh ngẫu nhiên, cất ở Vault `secret/fsds/agent-ui` (UI agent) và `secret/fsds/ops-ui` (UI vận hành); secret htpasswd trong k8s tạo
  từ đó (`setup_ingress.sh`), không nằm trong repo.

## Bằng chứng (2026-10-21, từ máy host)

```text
GET http://api.172-25-0-2.sslip.io:30080/            → 301 → https://…
GET https://api.172-25-0-2.sslip.io:30443/healthz    → 200, ssl_verify_result = 0 (cert hợp lệ với CA)
GET https://agent.172-25-0-2.sslip.io:30443/          → 401 không mật khẩu, 200 có mật khẩu
40 request đồng thời có mật khẩu                     → 11 × 200, 29 × 429 (rate limit)
GET https://metrics.172-25-0-2.sslip.io:30443/        → 401; có mật khẩu ops: /api/v1/query?query=up → 12 target
GET https://grafana.172-25-0-2.sslip.io:30443/        → 401 không mật khẩu, 200 có mật khẩu (log Loki xem qua Grafana Explore)
GET https://traces.172-25-0-2.sslip.io:30443/         → 401 không mật khẩu, 200 có mật khẩu (Jaeger UI)
GET https://registry.172-25-0-2.sslip.io:30443/       → 401 không mật khẩu, 200 có mật khẩu (agent registry)
```

Giới hạn local: redirect trả cổng 443 thay vì NodePort 30443; IP node đổi khi tạo lại cluster thì
chạy lại `setup_ingress.sh`.

<!-- Ảnh trình duyệt: hộp đăng nhập UI agent + ổ khoá HTTPS của API: người học chụp. -->
