# Đối chiếu tài liệu theo rubric

Tài liệu tham khảo để đối chiếu các mục coursework với thành phần của platform.
Điểm bắt đầu cho người dùng mới: [README dự án](../README.md) và [mục lục tài liệu](README.md).

Mỗi dòng dẫn tới tài liệu chứa giải thích, code liên quan và ảnh chụp bằng chứng.

### Data Platform (sheet `de`)

| Mục rubric | Tài liệu |
|---|---|
| Docker & Docker Compose, tối ưu Dockerfile | [Docker](Docker.md) |
| Data Generator (offline + online data problems) | [Thiết kế generator](generator-design.md), chất lượng [batch](data-quality-report-batch.md) / [stream](data-quality-report-stream.md) |
| Spark job (baseline, skew, schema evolution, duplicate) | [Spark](spark.md), [Silver](silver.md), [năm Channel](silver-five-channels.md) |
| Flink job (late arrival, duplicate, window) | [Flink](flink.md) |
| Data Storage (lakehouse, warehouse indexing) | [Storage](storage.md), [Iceberg maintenance](iceberg-maintenance.md) |
| Orchestration DP1 / DP2 / DP3 | [Airflow](airflow.md) |
| Data Governance (lineage, validation) | [DataHub](datahub.md) |
| Schema design (zone, SCD2, `feat_`, dim–fact, naming) | [Schema](schema.md), [thiết kế Gold](gold-design.md), [Gold](gold.md) |
| Novel idea 1 — Trino liên nguồn | [Trino](trino.md) |
| Novel idea 2 — CDC Debezium | [CDC](cdc.md) |
| Bằng chứng | [Evidence Data Platform](data-platform-evidence.md#đọc-bằng-chứng-aws-theo-rubric) |

### ML Platform / LLM Platform (sheet `mle`)

| Mục rubric | Tài liệu |
|---|---|
| LLM inference, custom model, benchmark, global model config | [LLM và agent](llm-platform-agents.md) |
| Agent registry, agent, coordinator, Sandbox, Warm Up | [LLM và agent](llm-platform-agents.md) |
| MCP Gateway, A2A Gateway | [agentgateway: LLM, MCP và A2A](llm-platform-agents.md#agentgateway-cổng-cho-llm-và-mcp) |
| RAG pipeline + governance | [RAG](llm-platform-rag.md) |
| Feature store (materialize, push stream feature) | [Feature store](ml-platform-feature-store.md), [stream feature](ml-platform-stream-feature.md) |
| Training pipeline, MLflow versioning | [Training](ml-platform-training.md) |
| ML inference service (KServe) | [Serving](ml-platform-serving.md) |
| Web API + MCP tool (FastAPI, async, helm) | [Feature API](ml-llm-platform-web-api.md) |
| Validation & Verification (coverage, EP/BVA, mutation, property-based, locust) | [Feature API — test](ml-llm-platform-web-api.md#test) |
| CI/CD | [CI/CD](ml-llm-platform-cicd.md) |
| NGINX Ingress, basic auth, rate limit, HTTPS | [Gateway](llm-platform-gateway.md) |
| IaC (Terraform, Ansible) | [IaC](IaC.md) |
| Observability, LLM / agent telemetry | [Observability](ml-llm-platform-observability.md) |
| A/B testing | [A/B](llm-platform-ab-test.md) |
| Vault | [Vault](ml-llm-platform-vault.md) |
| Novel idea 1 — agent eval harness | [Agent eval](llm-platform-agent-eval.md) |
| Novel idea 2 — drift theo kênh | [Drift](ml-platform-drift.md) |
| Bằng chứng | [Evidence ML / LLM Platform](ml-llm-platform-evidence.md), [kiểm kê rubric × cloud](rubric-cloud-audit.md) |
