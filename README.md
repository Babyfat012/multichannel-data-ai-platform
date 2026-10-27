# Multichannel Data & AI Platform

Nền tảng dữ liệu và AI cho một Seller bán hàng trên Amazon, TikTok Shop, Shopify, Walmart và Shopee. Hệ thống chuẩn hóa dữ liệu đa kênh, dự đoán rủi ro Return hoặc Refund của từng dòng đơn hàng, và cung cấp agent trả lời câu hỏi bằng dữ liệu. Toàn bộ dữ liệu được sinh giả lập.

## Tổng quan

| Phần | Chức năng | Công nghệ chính | Hướng dẫn |
|---|---|---|---|
| **Data Platform** | Nguồn → Bronze / Silver / Gold → warehouse; batch, streaming, điều phối và quản trị | Spark, Kafka, Flink, Debezium, Iceberg, MinIO, Lakekeeper, PostgreSQL, Trino, Airflow, DataHub | [Data Platform](docs/data-platform.md) |
| **ML Platform** | Feature store, training, model registry, serving và giám sát drift | Feast, Redis, LightGBM, Kubeflow Pipelines, MLflow, KServe, Evidently | [ML Platform](docs/ml-platform.md) |
| **LLM Platform** | LLM inference, RAG, agent, gateway và đánh giá câu trả lời | llama.cpp, pgvector, agentgateway, kagent, Langfuse, KEDA | [LLM Platform](docs/llm-platform.md) |

Hệ thống phục vụ một Seller trên bảy Storefront: Amazon US/UK/DE, TikTok Shop US, Shopify, Walmart US và Shopee VN. Feature được tra cứu theo `sku_id` + `storefront_id`; nhãn dự đoán xét Return hoặc Refund trong 30 ngày sau giao hàng. Chi tiết: [business domain](docs/Domain.md), [thuật ngữ](docs/terminology.md), [contract nhãn](docs/return-label-contract.md).

Sơ đồ triển khai: [Data Platform / ML Platform](docs/diagrams/data-ml-platform.html) · [LLM Platform](docs/diagrams/llm-platform.html).

## Bắt đầu

Yêu cầu: Python 3.12+, `uv`, Docker Engine và Docker Compose v2 có lệnh `wait`. Chạy các lệnh từ thư mục gốc repo.

### Kiểm tra code

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

Pytest mặc định chạy unit test và test RAG offline; test Spark và các hệ thống chạy thật có hướng dẫn riêng trong tài liệu từng thành phần.

### Chạy nguồn dữ liệu local

```bash
cp .env.example .env
# Sửa mật khẩu và cổng trong .env trước khi chạy.
docker compose up -d --build
docker compose wait generator-load
docker compose ps -a
```

Lệnh trên dựng MinIO, PostgreSQL và sinh/nạp dữ liệu theo `configs/small.yaml`. Đây là môi trường nguồn; Spark, Airflow, streaming và các platform khác cần bật theo hướng dẫn riêng. Xem [Docker Compose](docs/Docker.md) để chạy stream, chọn cấu hình lớn hơn hoặc build lại image.

Dừng môi trường nguồn bằng `docker compose stop`. Các service bổ sung và cụm `kind` được quản lý theo runbook của từng thành phần.

## Trạng thái và triển khai

[Cloud setup cho cả ba platform](docs/cloud-setup.md): chuẩn bị tài khoản, registry, mạng và Kubernetes trên AWS; các lệnh triển khai do người vận hành trực tiếp chạy.

**Data Platform đã chạy trên AWS (EC2 + S3, 2026-10-27):** raw 141,8 GB trên S3, Bronze Amazon 108.248.563 dòng và Walmart 10.949.058 dòng khớp nguồn, DAG Airflow `dp1_bronze` validate đạt; xem [bằng chứng AWS](docs/data-platform-evidence.md#trên-aws-cloud--2026-10-27). Bằng chứng local (Docker Compose, `kind`) giữ làm đối chứng. ML Platform và LLM Platform trên EKS là bước sau.

- [Trạng thái Data Platform](docs/data-platform-order-lifecycle-status.md) và [ma trận bằng chứng](docs/data-platform-evidence.md).
- [Ma trận bằng chứng ML Platform / LLM Platform](docs/ml-llm-platform-evidence.md).
- [Triển khai AWS](docs/aws-deploy.md) và [Terraform / Ansible](docs/IaC.md).

## Cấu trúc repo

| Đường dẫn | Nội dung |
|---|---|
| `generator/`, `configs/`, `contracts/` | Nguồn giả lập, profile dữ liệu, hợp đồng và schema evolution |
| `spark_jobs/`, `flink_jobs/`, `sql/`, `trino/` | Batch, streaming, lakehouse và warehouse |
| `dags/` | Pipeline Airflow và các bước validate/materialize |
| `feature_repo/`, `ml/`, `serving/` | Feature store, training, serving, drift và API công cụ |
| `rag/` | Chunking, embedding và truy xuất tài liệu |
| `infra/`, `docker/`, `ci/`, `.github/` | Hạ tầng, image, Kubernetes/Helm và CI/CD |
| `scripts/`, `tests/` | Setup, chạy pipeline, thu bằng chứng và kiểm thử |
| `docs/` | Tài liệu kỹ thuật; `evidence/` và `images/` giữ bằng chứng chạy thực tế |

## Tài liệu

Bắt đầu tại [mục lục tài liệu](docs/README.md): tài liệu được nhóm theo ba platform, kèm phần triển khai, hợp đồng thiết kế và evidence. README này giữ phần giới thiệu và cách bắt đầu; hướng dẫn chi tiết nằm trong tài liệu thành phần.

Quy ước phát triển: [code style](docs/code-style.md). Từ vựng dự án: [GLOSSARY](GLOSSARY.md). Quyết định kiến trúc: [ADR](docs/adr/README.md).
