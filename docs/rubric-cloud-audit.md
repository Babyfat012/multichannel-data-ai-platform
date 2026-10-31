# Rubric × cloud — kiểm kê 2026-10-29

Nguồn: `edai-k11-cw/rubic/EDAI K11 - MLE.xlsx`, sheet `de` (100đ) và `mle` (100đ). Nguyên tắc learner chốt:
mọi dòng phải có bằng chứng **triển khai trên AWS**; thiếu là phải làm. Bằng chứng cloud ghi ở
[data-platform-evidence.md](data-platform-evidence.md) mục "Trên AWS"; bằng chứng local ở
[ml-llm-platform-evidence.md](ml-llm-platform-evidence.md).

Ký hiệu: ✅ có bằng chứng AWS · ⚠️ có trên AWS nhưng thiếu phần rubric đòi · ❌ chỉ local / chưa có.

## Sheet `de`

| Dòng rubric | Điểm | Trạng thái | Việc còn lại |
|---|---|---|---|
| Docker & Compose + optimize Dockerfile | 2+3 | ✅ | Compose chạy trên EC2; số đo multistage 2,39 GB → 698 MB (đo local, cùng Dockerfile) |
| Generator offline: skew, schema evolution, duplicate, lưu nguồn | 2×4 | ✅ | 100 GB sinh trên EC2, raw S3 + Postgres EC2 |
| Generator online: late arrival, duplicate stream | 2×2 | ✅ | Kafka + producer trên EC2 |
| Spark baseline / skew / schema evolution / other + Spark UI | 2+3+3+3 | ✅ | 29/10: đo skew trên AWS + Spark UI History EC2 ([spark.md 5g](spark.md)) |
| Spark job tích hợp pipeline | 2 | ✅ | dp1–dp3 trên Airflow EC2 |
| Flink baseline / late arrival / other / window | 2+3+3+2 | ✅ | 5 job Flink trên EC2, ảnh Flink UI |
| Lakehouse optimize (compaction/partition) | 3 | ✅ | 29/10: shopee ngày→tháng, 180→6 file, lọc 30 ngày 8× ([storage.md](storage.md)) |
| Warehouse indexing | 3 | ✅ | 29/10: `dp4_warehouse` 37 tr dòng, EXPLAIN 42,6 s → 1,5 s ([storage.md](storage.md)) |
| DP1 / DP2 / DP3 ingest + validate | 2×6 | ✅ | Airflow AWS |
| DataHub lineage + validation DP1/DP2/DP3 | 2×6 | ✅ | DataHub EC2, 6 DAG: 22 dataset, 30 job, 16 assertion |
| Schema: zones, SCD2, feat_, dim–fact, naming | 2×5 | ✅ | Tài liệu bảng Gold A (cùng bảng trên AWS) |
| Novel idea 1: Trino federation | 10 | ✅ | 29/10: Trino EC2, Iceberg S3 = Postgres 4.517 dòng ([trino.md](trino.md)) |
| Novel idea 2: CDC Debezium | 10 | ✅ | Debezium EC2, probe c/u/d |

## Sheet `mle`

Cập nhật 30/10 tối sau buổi EKS. Bằng chứng: [LLM và agent](llm-platform-agents.md#trên-eks-2026-10-30),
[RAG](llm-platform-rag.md#trên-aws-2026-10-30), [Web API](ml-llm-platform-web-api.md#trên-eks-2026-10-30),
[Observability](ml-llm-platform-observability.md#trên-eks-2026-10-30), ảnh trong `evidence/aws/*-20261030.*`.

| Dòng rubric | Điểm | Trạng thái | Việc còn lại |
|---|---|---|---|
| LLM inference platform + benchmark | 2+2 | ✅ | llama.cpp Qwen 2.5 1,5B trên EKS; bench 6/3/4 luồng (`bench-llm-eks-t*.json`), chọn 3 |
| Custom LLM model | 2 | ✅ | GGUF tự host trên EKS |
| agentgateway + security/policy | 1 | ✅ | `gateway-smoke-20261030.json` 17/17 (API key, PII, rate limit) |
| Global model config | 2 | ✅ | `default-model-config` → agentgateway |
| Agent registry | 2 | ✅ | agentregistry 0.4.0, 1 MCP + 3 agent (`agent-registry-ui`) |
| RAG pipeline (Airflow) + governance (DataHub) | 2+2 | ⚠️ | `ml_rag_ingest` 5 task xanh trên Airflow EC2 + `/chunks` EKS ✅; `publish` DataHub skipped (DataHub tắt) |
| Materialize offline → online | 2 | ✅ | `ml_feature_materialize` 4/4 |
| Push streaming feature → offline + online | 2 | ✅ | 31/10: Flink EC2 76/76 checkpoint, đối chiếu 1.004.354 cửa sổ khớp, API `approved_returns_24h` ([stream](ml-platform-stream-feature.md)); dữ liệu phát lại (gap #25) |
| KFP training ≥4 bước | 2 | ✅ | `train-aws-3` 5/5 |
| MLflow version model + data + tag | 2 | ✅ | v1 `production` |
| KServe model-inside-image | 2 | ✅ | predict OK |
| FastAPI + pydantic + healthcheck / async / MCP helm rolling + fallback | 1+1+2 | ✅ | Feature API helm 2 replica, rolling; Swagger `/features` `/predict` `/chunks` |
| Agent 1 multi-replica + KEDA, Sandbox, publish | 2+1+1 | ⚠️ | data-agent 2 pod + KEDA 2→3→2 ✅, publish registry ✅; Sandbox (Substrate) ❌ |
| Agent 2: online feature + predict + RAG | 8 | ✅ | model-agent gọi `predict_return_risk` và `search_policy_chunks` (ảnh `rag-model-agent-*`) |
| MCP gateway / A2A gateway | 2+2 | ✅ | route `/mcp`, A2A theo header; Grafana đếm theo route |
| Coordinator multi-replica + KEDA, publish | 2+2 | ✅ | coordinator 2 pod, ScaledObject min 2 max 3, publish registry |
| Warm Up (Agent Substrate) | 2 | ❌ | Bước 8 |
| Unit coverage, EP/BVA, mutation, property-based | 1+2+2+2 | ✅ | Kiểm ở mức code (PR testing), không phụ thuộc nơi chạy |
| Load test locust Web API | 2 | ✅ | Locust → Feature API EKS: 3.004 request, 0 lỗi (`load-test-feature-api-20261030.html`) |
| CI/CD RAG / agent / coordinator / stream feature | 2×4 | ⚠️ | 31/10 Jenkins EC2 → ECR/EKS/Compose: `feature-api-agents` #8 (eval agent 12/12), `rag-pipeline` #2, `stream-features` #2 xanh ([ảnh](evidence/aws/jenkins-feature-api-agents-20261031.png), [rag](evidence/aws/jenkins-rag-pipeline-20261031.png), [stream](evidence/aws/jenkins-stream-features-20261031.png)); `coordinator` deploy xanh, eval timeout 180 s (LLM CPU) ❌ |
| NGINX: Grafana, log, trace, agent UI, registry UI | 2×5 | ✅ | Route HTTPS + cert LE: grafana., traces., agent., registry. |
| Basic auth + rate limit | 2 | ✅ | 401 không mật khẩu; rate limit trong gateway smoke |
| Domain + HTTPS Web API | 1 | ✅ | `api.15-134-182-157.sslip.io` cert LE, Swagger qua HTTPS |
| Terraform / Ansible | 1+1 | ✅ | Terraform AWS + Ansible chạy thật |
| Observability: Web API metrics, Prom+Grafana, logs, traces | 1×4 | ✅ | Panel Feature API/KServe, Loki `{namespace="feature-store"}`, Jaeger trace agentgateway |
| LLM telemetry (token, TTFT…) / Agent telemetry | 2+2 | ✅ | Grafana token, TTFT, thời gian/token, lượt gọi Agent + MCP tool |
| A/B agent / A/B LLM | 1+1 | ❌ | Bước 5 |
| Vault | 1 | ✅ | Vault EC2 (EBS), KV v2, mật khẩu ingress |
| README + docs theo rule | 2 | ⚠️ | Thiếu sơ đồ triển khai AWS tổng thể |
| Novel idea 1 / 2 | 2+2 | ⚠️ | Chốt ML-N1 (agent eval) + ML-N2 (drift Evidently); chưa có bằng chứng AWS (bước 5, 6) |

## Ước lượng

- `de`: **100đ có bằng chứng AWS** (29/10, đợt 1 xong).
- `mle` (30/10 tối, tự đếm): có bằng chứng AWS **~77đ** (72 dòng ✅ + 5 phần đã xong của dòng ⚠️); thiếu ~9đ ở dòng ⚠️ (DataHub RAG 2, Sandbox 1, README 2, novel 4); ❌ 14đ (stream 2, Warm Up 2, CI/CD 8, A/B 2).
- Ràng buộc: EKS 2 × c6i.xlarge (8 GB), quota 16 vCPU đã dùng hết.

## Cấu hình và kế hoạch chốt (learner duyệt 29/10)

Cấu hình: EC2 `r6i.xlarge` (4 vCPU/32 GB, IP riêng 10.42.0.12 giữ nguyên khi đổi type), 2 node EKS
`r6i.xlarge` (2 × 4 vCPU/32 GB). 12/16 vCPU quota. ~1,1 USD/giờ khi bật cả hai; ~0,18 USD/giờ khi tắt hết
(EKS control plane 0,10 + EBS). Ngân sách tính 100 USD ⇒ chạy theo đợt, tắt khi nghỉ (~10 ngày).
gVisor KHÔNG cài qua launch template (sai key containerd 1.x/2.x có thể làm node không join) ⇒ DaemonSet ở đợt 5.
30/10: đổi hướng — Agent Substrate 0.0.9 (khớp kagent 0.10.3) tự đặt runtime gVisor qua node agent của nó ⇒ không tự viết DaemonSet sửa containerd ([ví dụ kagent](https://www.kagent.dev/docs/kagent/examples/agent-substrate)); code `scripts/setup/deploy_substrate.sh` + `infra/k8s/agents/sandbox/`.

| Đợt | Bật | Việc |
|---|---|---|
| 0 | cả hai | Terraform EC2 + EKS đổi type; Ansible với `feast_redis` = IP node mới; materialize lại |
| 1 | EC2 | dp4_warehouse + EXPLAIN; compaction có trước/sau thật; đo skew AWS; Trino liên nguồn; DataHub 2 DAG mới |
| 2 | EKS | Mạng public cho Ingress (LB AWS, tag subnet, SG 80/443, domain DuckDNS) → ingress-nginx + basic auth + rate limit + HTTPS; Prometheus/Grafana/Loki/Jaeger (+ alert đĩa, slot lag, retry: gap #7 #8 #17 #20); Vault; KEDA |
| 3 | EKS | llama.cpp Qwen + benchmark; agentgateway; model config; registry; Feature API (MCP, helm); data-agent, model-agent, coordinator + KEDA; MCP/A2A gateway; Langfuse; telemetry LLM + agent |
| 4 | cả hai | RAG (Airflow → pgvector) + lineage DataHub; stream push (Flink → feast-pusher, gap #5 #12); materialize lại |
| 5 | EKS (+EC2) | Jenkins 4 pipeline (gap #9); A/B agent + LLM; gVisor DaemonSet + Substrate (Sandbox, Warm Up) |
| 6 | — | README + sơ đồ AWS; evidence; chốt 2 Novel MLE; commit |
| 7 | tuỳ | Gap loại 2 còn lại: nhỏ trước #10 #15 #19 #14 #6, lớn sau #3 #22 #18 (#11 nói thật) |

## Đối chiếu từng dòng rubric thật (30/10 tối)

Đọc lại `EDAI K11 - MLE.xlsx` sheet `mle` cột "Document": nhiều dòng đòi **ảnh**, không chỉ chạy được. Ngoài bước
4–9 trong handoff, còn thiếu để đủ 100/100:

| # | Dòng | Điểm | Thiếu | Cần cloud |
|---|---|---|---|---|
| 1 | RAG pipeline + governance | 2+2 | Ảnh lineage DataHub cho `ml_rag_ingest` (`publish` đang skipped) | Có: bật DataHub EC2 |
| 2 | Terraform / Ansible | 1+1 | ✅ 31/10 ảnh `terraform plan` No changes + Ansible `PLAY RECAP` trong `IaC.md` | — |
| 3 | Computing telemetry | 1 | ✅ 31/10 ảnh Grafana EC2 CPU/RAM/disk/network | — |
| 4 | README theo ô A2 | 2 | ✅ 31/10 `aws-architecture-layer12/layer3.png` trong README | — |
| 5 | Unit coverage > 90% | 1 | ✅ 31/10 ảnh `serving-coverage-20261031.png` | — |
