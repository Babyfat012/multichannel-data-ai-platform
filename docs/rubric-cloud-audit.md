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

| Dòng rubric | Điểm | Trạng thái | Việc còn lại |
|---|---|---|---|
| LLM inference platform + benchmark | 2+2 | ❌ | llama.cpp Qwen lên EKS + benchmark |
| Custom LLM model | 2 | ❌ | GGUF tự host trên EKS |
| agentgateway + security/policy | 1 | ❌ | agentgateway trên EKS |
| Global model config | 2 | ❌ | `default-model-config` → agentgateway |
| Agent registry | 2 | ❌ | agentregistry trên EKS + ảnh UI |
| RAG pipeline (Airflow) + governance (DataHub) | 2+2 | ❌ | pgvector EKS, DAG `ml_rag_ingest` trên Airflow EC2, lineage DataHub |
| Materialize offline → online | 2 | ✅ | `ml_feature_materialize` 4/4 |
| Push streaming feature → offline + online | 2 | ❌ | Flink `sku_return_24h` + feast-pusher trên AWS (hiện file rỗng) |
| KFP training ≥4 bước | 2 | ✅ | `train-aws-3` 5/5 |
| MLflow version model + data + tag | 2 | ✅ | v1 `production` |
| KServe model-inside-image | 2 | ✅ | predict OK |
| FastAPI + pydantic + healthcheck / async / MCP helm rolling + fallback | 1+1+2 | ❌ | Feature API trên EKS (`deploy_feature_api.sh` nhánh s3) |
| Agent 1 multi-replica + KEDA, Sandbox, publish | 2+1+1 | ❌ | data-agent + KEDA trên EKS; Sandbox (Substrate) local cũng chưa có |
| Agent 2: online feature + predict + RAG | 8 | ❌ | model-agent trên EKS |
| MCP gateway / A2A gateway | 2+2 | ❌ | agentgateway route `/mcp`, A2A |
| Coordinator multi-replica + KEDA, publish | 2+2 | ❌ | coordinator + KEDA trên EKS |
| Warm Up (Agent Substrate) | 2 | ❌ | Local cũng chưa có (cần gVisor) |
| Unit coverage, EP/BVA, mutation, property-based | 1+2+2+2 | ✅ | Kiểm ở mức code (PR testing), không phụ thuộc nơi chạy |
| Load test locust Web API | 2 | ⚠️ | Rubric ghi "Local Testing"; nên chạy lại trên Feature API EKS khi có |
| CI/CD RAG / agent / coordinator / stream feature | 2×4 | ❌ | Jenkins local; cần đích ECR + EKS |
| NGINX: Grafana, log, trace, agent UI, registry UI | 2×5 | 🟡 | 30/10: route HTTPS + cert LE cho Grafana (log Loki xem trong Grafana), metrics, traces; route `agent.` có cert, backend kagent ở đợt 3; registry đợt 3 |
| Basic auth + rate limit | 2 | 🟡 | Basic auth đo được (401 không mật khẩu); rate limit đã khai, đo khi Feature API có ở đợt 3 |
| Domain + HTTPS Web API | 1 | 🟡 | `api.15-134-182-157.sslip.io` có cert LE thật; Feature API deploy đợt 3 (hiện 502) |
| Terraform / Ansible | 1+1 | ✅ | Terraform AWS + Ansible chạy thật (cập nhật dòng cũ "chưa kiểm chứng") |
| Observability: Web API metrics, Prom+Grafana, logs, traces | 1×4 | 🟡 | Prom+Grafana ✅, Loki+Fluent Bit chạy (chưa có ảnh truy vấn log), Jaeger ✅ (trace của chính nó); Web API metrics chờ Feature API đợt 3. Thêm: cảnh báo EC2 (#17 #20) |
| LLM telemetry (token, TTFT…) / Agent telemetry | 2+2 | ❌ | |
| A/B agent / A/B LLM | 1+1 | ❌ | |
| Vault | 1 | ✅ | 30/10: Vault EC2 (EBS), KV v2, mật khẩu ingress sinh và đọc từ Vault |
| README + docs theo rule | 2 | ⚠️ | Thêm sơ đồ triển khai AWS tổng thể |
| Novel idea 1 / 2 | 2+2 | ❓ | Chưa xác định ý tưởng MLE; cần chốt |

## Ước lượng

- `de`: **100đ có bằng chứng AWS** (29/10, đợt 1 xong).
- `mle`: ~17đ ✅ (materialize 2, KFP 2, MLflow 2, KServe 2, testing 7, IaC 2); ⚠️ 4đ; ❓ novel 4đ; ❌ ~75đ.
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
