<!-- Mục đích: đối chiếu từng dòng rubric `mle` với artifact + bằng chứng, và chỗ còn thiếu (local và AWS). -->
# Ma trận rubric MLE

Nguồn: sheet `mle` của `EDAI K11 - MLE.xlsx` (dòng 3–57, 100 điểm). Trạng thái ngày 2026-10-21, chạy
trên **local** (`kind` + Docker Compose). Đích cloud cập nhật 2026-10-22 là **AWS** (`ap-southeast-1`). Mốc 1 chỉ DE batch EC2 + S3 theo [runbook](aws-deploy.md); MLE giữ bằng chứng local. EKS/GPU/domain là bước bật–học–xóa sau, ngoài mốc 1.

Ký hiệu: ✅ chạy thật có bằng chứng · ⚠️ có artifact, thiếu một phần · ⏳ chờ AWS/learner · ❔ chưa thấy tài liệu.

| Dòng | Nội dung | Đ | TT | Artifact / bằng chứng | Còn thiếu |
|---|---|---|---|---|---|
| 3 | LLM inference platform + benchmark | 2 | ⚠️ | llama.cpp (thay llm-d) — [mle-agents](mle-agents.md) | llm-d cần GPU trên AWS/EKS; ngoài mốc 1 |
| 4 | Custom LLM model | 2 | ⚠️ | Qwen2.5 GGUF tự host — [mle-agents](mle-agents.md) | xác nhận "custom" với GV |
| 5 | agentgateway + security/policy | 1 | ✅ | API key, rate limit, mcpAuthorization — [mle-agents](mle-agents.md) | |
| 6 | Benchmark + tối ưu model server | 2 | ✅ | config B: req/s 0,475→0,578, p95 9,56→7,0 s — [evidence](evidence/llm-benchmark-20261021.jsonl) | |
| 7 | Global ModelConfig qua gateway | 2 | ✅ | `default-model-config` → agentgateway `/v1` — [mle-agents](mle-agents.md) | |
| 8 | Agent registry | 2 | ✅ | agentregistry 0.4.0 + route `registry.` — [mle-gateway](mle-gateway.md) | ảnh UI |
| 9 | RAG pipeline Airflow → Feast | 2 | ✅ | DAG `ml_rag_ingest` chạy thật — [mle-rag](mle-rag.md) | ảnh Airflow |
| 10 | Data governance RAG (lineage) | 2 | ⏳ | bước publish DataHub có sẵn trong DAG | cần publish lineage RAG trong runtime MLE AWS/EKS; ngoài mốc 1 (DataHub DE không thay proof RAG) |
| 11 | Materialize offline → online (Airflow) | 2 | ✅ | DAG `ml_feature_materialize`, tăng dần 2 lần — [mle-feature-store](mle-feature-store.md) | ảnh Airflow |
| 12 | Stream feature → offline + online | 2 | ✅ | Flink → Kafka → feast-pusher, 72/72 khớp oracle — [mle-stream-feature](mle-stream-feature.md) | ảnh Flink UI |
| 13 | Training pipeline KFP ≥ 4 bước | 2 | ✅ | 5 bước SUCCEEDED — [mle-training](mle-training.md) | ảnh KFP |
| 14 | MLflow version model + data, tag | 2 | ✅ | v3 `production`, tag `latest`, sha256 data — [mle-training](mle-training.md) | ảnh MLflow |
| 15 | KServe model-inside-image | 2 | ✅ | `return-risk` READY, predict OK — [mle-serving](mle-serving.md) | ảnh pod + curl |
| 16 | FastAPI + pydantic + healthcheck | 1 | ✅ | [mle-web-api](mle-web-api.md) | |
| 17 | Async concurrent | 1 | ✅ | `asyncio.gather` theo view — [mle-web-api](mle-web-api.md) | |
| 18 | MCP tool helm + rolling + fallback | 2 | ✅ | helm `--atomic` — [mle-web-api](mle-web-api.md) | |
| 19 | Agent multi-replica + KEDA | 2 | ✅ | data-agent 2→3→2 — [evidence](evidence/keda-data-agent-20261021.txt) | |
| 20 | Agent trong Sandbox | 1 | ⏳ | kagent 0.10 chạy Agent trong sandbox qua Agent Substrate (gVisor, valkey ×6, rustfs) — quá nặng cho kind 14 GB; hiện có: Agent không gọi được LLM/tool/Agent khác nếu không qua agentgateway có API key + `mcpAuthorization` — [mle-agents](mle-agents.md) | AWS/EKS: substrate + `SandboxAgent`; ngoài mốc 1 |
| 21 | Publish agent lên registry | 1 | ✅ | `publish_agentregistry.sh` — [mle-agents](mle-agents.md) | |
| 22 | Agent 2: online feature + predict + RAG | 8 | ✅ | model-agent gọi KServe `return-risk` + RAG — [mle-agents](mle-agents.md), [mle-serving](mle-serving.md) | ảnh chat |
| 23 | MCP gateway + policy | 2 | ✅ | agentgateway `/mcp` — [mle-agents](mle-agents.md) | |
| 24 | A2A gateway + policy | 2 | ✅ | route theo `x-kagent-host` — [mle-agents](mle-agents.md) | |
| 25 | Coordinator multi-replica + scale | 2 | ✅ | KEDA min 2 / max 3 theo request A2A ở gateway: 2→3→2 pod — [evidence](evidence/keda-coordinator-20261021.txt), [`coordinator-keda.yaml`](../infra/k8s/agents/coordinator-keda.yaml) | ảnh kagent UI chat |
| 26 | Publish coordinator lên registry | 2 | ✅ | [mle-agents](mle-agents.md) | |
| 27 | Warm Up / Agent Fleet (substrate) | 2 | ⏳ | Agent Substrate (kagent `controller.substrate`, WorkerPool gVisor, snapshot/rehydrate agent rảnh) cần ate-system ~2–3 GB RAM — không chạy được cạnh KFP/LLM trên máy 14 GB; local thay bằng scale-to-0 có kiểm soát (KEDA pause, scale qwen 0) | AWS/EKS: cài substrate, ảnh nhiều agent / ít worker; ngoài mốc 1 |
| 28 | Unit test coverage > 90% | 1 | ✅ | 85 test, `serving/app.py` 98% (fixture + MagicMock/AsyncMock + httpx MockTransport) — [evidence](evidence/serving-coverage-20261021.txt), [`test_app.py`](../serving/tests/test_app.py) | ảnh terminal |
| 29 | Equivalence partitioning / BVA | 2 | ✅ | parametrize biên — `tests/unit/test_serve.py`, `test_agent_eval.py`, [mle-web-api](mle-web-api.md) | |
| 30 | Mutation testing > 80% | 2 | ✅ | mutmut — [mle-web-api](mle-web-api.md) | |
| 31 | Property-based (hypothesis) | 2 | ✅ | model: [`tests/unit/test_model_properties.py`](../tests/unit/test_model_properties.py) (dự đoán idempotent, train lại cùng seed ra cùng model, split không lọt tương lai; hypothesis tìm ra ca test một-lớp) + Web API: `test_idempotent` (Hypothesis) — gọi lại cùng request và gửi lặp SKU ra cùng kết quả, [`serving/tests/test_app.py`](../serving/tests/test_app.py) | |
| 32 | Load test locust (HTML) | 2 | ✅ | [mle-web-api](mle-web-api.md) | |
| 33 | CI/CD RAG pipeline | 2 | ⚠️ | `ci/jenkins/rag-pipeline.Jenkinsfile`: test → build → **push registry** → deploy → chạy DAG. Image đi qua **registry local** `localhost:5001` (Jenkins push → k8s pull; đã kiểm pod Feature API kéo `localhost:5001/fsds-feature-api:<sha>` ngày 2026-10-22) — [mle-cicd](mle-cicd.md). | build từ `main` đã commit + push, thu Stage View |
| 34 | CI/CD MCP + agent (+ Langfuse prompt) | 2 | ⚠️ | `feature-api-agents.Jenkinsfile`: test → build → **push registry** → helm `--atomic` → **sync prompt từ Langfuse** → deploy Agent → smoke 4 tool → **Eval agent** (dưới ngưỡng 0,95 ⇒ build đỏ). Langfuse v2 giữ 3 prompt (label production); Agent đọc qua `systemMessageFrom` ConfigMap; đổi prompt v2 trên Langfuse ⇒ Agent nhận sau sync (đã kiểm) — [mle-agents](mle-agents.md#system-prompt-từ-langfuse) | build từ `main` đã commit + push, thu Stage View |
| 35 | CI/CD coordinator | 2 | ⚠️ | `coordinator.Jenkinsfile`: validate → sync prompt Langfuse → deploy → smoke A2A qua gateway → **Eval agent** | build từ `main` đã commit + push, thu Stage View |
| 36 | CI/CD stream feature jobs | 2 | ⚠️ | `stream-features.Jenkinsfile`: test → build → **push registry** → deploy feast-pusher theo SHA — [mle-stream-feature](mle-stream-feature.md) | build từ `main` đã commit + push, thu Stage View |
| 37 | Metric UI sau gateway | 2 | ✅ | `metrics.` + `grafana.` — [mle-gateway](mle-gateway.md) | |
| 38 | Log UI sau gateway | 2 | ✅ | Loki qua Grafana Explore — [mle-observability](mle-observability.md) | |
| 39 | Trace UI sau gateway | 2 | ✅ | `traces.` (Jaeger) — [mle-gateway](mle-gateway.md) | |
| 40 | UI test agent | 2 | ✅ | `agent.` (kagent UI) | |
| 41 | UI agent registry | 2 | ✅ | `registry.` | |
| 42 | Basic auth + rate limit | 2 | ✅ | 401/200/429 — [mle-gateway](mle-gateway.md) | |
| 43 | Domain + HTTPS Web API | 1 | ⚠️ | sslip.io + CA nội bộ; Let's Encrypt DNS-01 qua DuckDNS đã chuẩn bị, mặc định tắt (`scripts/setup/setup_letsencrypt.sh`) — [mle-gateway](mle-gateway.md) | domain thật + ACME trên AWS/EKS; ngoài mốc 1 |
| 44 | Terraform cloud services | 1 | ⏳ | [Terraform AWS](../infra/aws/terraform/README.md): EC2/VPC/S3/IAM; [IaC](IaC.md) | learner apply theo [runbook](aws-deploy.md#terraform), thu proof cloud services; chưa có EKS |
| 45 | Ansible deploy VM | 1 | ⏳ | [Ansible AWS](../infra/aws/ansible/README.md), [IaC](IaC.md) (phần DE) | chạy playbook trên EC2 và chụp PLAY RECAP theo [runbook](aws-deploy.md#ansible) |
| 46 | Web API metrics | 1 | ✅ | feature-api `/metrics`; KServe `return_risk_*` — [mle-serving](mle-serving.md) | |
| 47 | CPU/RAM/disk/network | 1 | ✅ | Prometheus + Grafana — [mle-observability](mle-observability.md) | |
| 48 | Logs | 1 | ✅ | Loki + Fluent Bit — [mle-observability](mle-observability.md) | |
| 49 | Traces | 1 | ✅ | agentgateway/kagent OTLP → Jaeger | |
| 50 | LLM telemetry (token, TTFT, PII) | 2 | ✅ | token in/out/tổng mỗi request, thời gian 1 lượt, **TTFT** (`gen_ai_server_time_to_first_token`), **PII**: guardrail agentgateway chặn email/thẻ/SSN (403), `guardrail_checks_total{action}` — dashboard "FSDS — Agents, LLM, MCP", [mle-observability](mle-observability.md) | ảnh Grafana |
| 51 | Agent telemetry (call agent/tool, lỗi) | 2 | ✅ | lượt gọi mỗi Agent (`agentgateway_requests_total{route=a2a-*}`), mỗi tool (`agentgateway_mcp_requests_total`), **lỗi mỗi tool** (`mcp_tool_calls_total{tool,outcome}` trong Feature API) — dashboard "FSDS — Agents, LLM, MCP" | ảnh Grafana |
| 52 | A/B agent khác model config | 1 | ✅ | data-agent vs data-agent-b — [mle-ab-test](mle-ab-test.md) | |
| 53 | A/B hai bản LLM | 1 | ✅ | `fsds-chat` 1:1 — [mle-ab-test](mle-ab-test.md) | |
| 54 | Vault | 1 | ✅ | [mle-vault](mle-vault.md) | |
| 55 | Repository design / README | 2 | ⚠️ | README theo rubric ô A2: domain, mục lục, repo structure, 2 sơ đồ deployable unit ([dữ liệu + ML](diagrams/architecture-layer12.html), [LLM + agent](diagrams/architecture-layer3.html)), tài liệu gom theo rubric | ảnh cho từng doc |
| 56 | Novel 1: eval harness agent | 2 | ✅ | A 100% vs B 91,7%; cổng chạy trong Jenkins (stage `Eval agent`, `scripts/run/eval_agent_ci.sh`) — [mle-agent-eval](mle-agent-eval.md) | ảnh Stage View có stage Eval |
| 57 | Novel 2: drift monitoring | 2 | ✅ | Evidently, Sale event alert — [mle-drift](mle-drift.md) | |

## Việc của learner

1. Build 4 job Jenkins từ `main` đã commit + push, thu ảnh Stage View (dòng 33–36); chưa tự đổi trạng thái CI/CD khi chưa build.
2. Chụp ảnh theo các chỗ `<!-- … chụp -->` trong từng `docs/mle-*.md`, giữ rõ đây là runtime local.
3. Chạy [runbook AWS](aws-deploy.md) cho EC2 + S3, thu Terraform/Console/PLAY RECAP theo [IaC](IaC.md) cho dòng 44–45. Gói này triển khai DE, chưa triển khai MLE lên AWS.
4. Ngoài mốc 1: EKS bật–học–xóa, GPU cho llm-d, substrate cho dòng 20/27, lineage RAG dòng 10 và domain thật + HTTPS dòng 43; cần kế hoạch/cấu hình riêng trước khi chạy. Không dùng ảnh DE để đóng proof MLE.
