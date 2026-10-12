# FSDS: Nền tảng dữ liệu đa kênh cho Seller thương mại điện tử

Data platform gom dữ liệu bán hàng của một Seller trên 5 nền tảng thương mại điện tử (Amazon, TikTok Shop, Shopify, Walmart, Shopee) vào một lakehouse, phục vụ bài toán dự đoán khả năng trả hàng. Toàn bộ dữ liệu được sinh giả lập.

> **Trạng thái**: đang triển khai Data Generator (tiến độ xem [docs/generator-design.md](docs/generator-design.md#15-triển-khai)). Các phần khác, công nghệ và cấu hình trong tài liệu này vẫn là **dự kiến**.

## Mục lục

1. [Giới thiệu business domain](#1-giới-thiệu-business-domain)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Công nghệ](#3-công-nghệ)
4. [Dữ liệu và Data Generator](#4-dữ-liệu-và-data-generator)
5. [Repo Structure](#5-repo-structure)
6. [Tài liệu chi tiết](#6-tài-liệu-chi-tiết)
7. [Lộ trình](#7-lộ-trình)

## 1. Giới thiệu business domain

Dự án **không** xây một sàn thương mại điện tử. Dự án xây hệ thống dữ liệu của **một Seller** bán trên nhiều sàn cùng lúc. Mỗi sàn trả dữ liệu theo định dạng, trạng thái đơn và tiền tệ riêng, nên Seller cần một nơi thống nhất để phân tích và dự đoán.

Ví dụ: công ty ACME bán áo hoodie trên Amazon, TikTok Shop, Shopify, Walmart và Shopee. Hệ thống gom dữ liệu của cả 5 nơi về một mô hình chung.

| Thuật ngữ | Nghĩa |
|---|---|
| **Seller** | Người bán duy nhất mà hệ thống phục vụ |
| **Channel** | Nền tảng bán hàng bên ngoài (Amazon, TikTok Shop, Shopify, Walmart, Shopee) |
| **Marketplace** | Gian hàng theo quốc gia trong một Channel, ví dụ Amazon DE (không phải Channel nào cũng có) |
| **Storefront** | Chỗ cụ thể Seller bán hàng = Channel + Marketplace nếu có |
| **SKU** | Mã sản phẩm do Seller đặt, dùng chung xuyên Channel |
| **Listing** | Bản đăng bán của một SKU trên một Storefront |
| **Order / Order Line** | Đơn Channel giao cho Seller xử lý, và từng dòng sản phẩm trong đơn |

**Phạm vi**: 7 Storefront (Amazon US/UK/DE, TikTok Shop US, Shopify, Walmart US, Shopee VN). Amazon được mô hình hóa sâu nhất, bốn Channel còn lại mô hình hóa phần chung.

**Bài toán dự đoán**: một Order Line có bị Return hoặc Refund trong 30 ngày sau khi giao hay không. Các Channel giấu hoặc không cấp định danh người mua dùng chung được, nên feature được tra cứu theo `sku_id` + `storefront_id` thay vì theo khách hàng. Nhãn đến sau 30 ngày, nên việc giám sát drift phải làm khi chưa có đáp án thật.

Chi tiết xem [docs/Domain.md](docs/Domain.md).

## 2. Kiến trúc tổng thể

![Sơ đồ triển khai nền tảng dữ liệu FSDS](docs/diagrams/architecture.png)

Sơ đồ triển khai cấp cao của module data engineering. Mỗi hộp là một **deployable
unit** — thứ chạy được thành một container riêng. Thư viện không triển khai độc lập
được (Iceberg là format bảng, không phải service) không xuất hiện trên sơ đồ.

Cách đọc sơ đồ:

- **Nhãn trên mũi tên là dữ liệu truyền đi**, không phải tên thao tác.
- **Luồng Batch đánh số `B1→B6`, luồng Stream đánh số `S1→S3`** theo đúng thứ tự
  chạy. Đọc lần lượt theo số là đi hết một vòng dữ liệu.
- **Hai mũi tên nét đứt không phải luồng dữ liệu**: một là điều phối (Airflow submit
  job cho Spark), một là lineage (Airflow đẩy metadata sang DataHub).
- Khung `DOCKER COMPOSE · FSDS` bao quanh phần tự dựng bằng Compose. Hộp ngoài khung
  là hệ thống bên ngoài (Channel) hoặc công cụ chạy trên máy (DBeaver).

Một chỗ đã đơn giản hoá, cần biết khi đọc: **PostgreSQL vẽ một lần** bên phải với
subtitle `warehouse · dept · catalog` vì nó kiêm ba vai. Vai "hệ thống phòng ban
khác" chỉ được gọi tên chứ không vẽ mũi tên riêng sang Spark — trong sơ đồ, Spark
lấy dữ liệu nguồn qua MinIO (`B2`).

Bản vector để phóng to hoặc đưa vào slide: [`docs/diagrams/architecture.svg`](docs/diagrams/architecture.svg).
Bản dựng gốc: [`docs/diagrams/architecture.html`](docs/diagrams/architecture.html).

### Deployable units

| Unit | Vai trò |
|---|---|
| Data Generator | Sinh dữ liệu giả lập theo cấu hình, ghi vào MinIO, PostgreSQL và Kafka |
| MinIO | Object storage của lakehouse — bảng Iceberg ba tầng Bronze, Silver, Gold |
| REST Catalog (Lakekeeper) | Giữ con trỏ tới snapshot Iceberg hiện hành; Spark, Flink và Trino đều đi qua đây |
| Kafka | Bus cho luồng event trạng thái đơn và trả hàng |
| Spark | Xử lý batch: skew, high cardinality, schema evolution, dedup |
| Flink | Xử lý streaming: dedup, late arrival, burst, window |
| Trino | Query liên nguồn — một câu SQL đọc được cả lakehouse lẫn PostgreSQL |
| Airflow | Điều phối pipeline theo nhịp write → audit → publish |
| PostgreSQL | Kiêm ba vai: data warehouse (có index), DB phòng ban khác, backend của catalog |
| DataHub | Metadata, lineage và data contract |
| DBeaver | Công cụ chạy trên máy, nối Trino để xem schema và ERD của cả ba tầng |

### Các pipeline

| Pipeline | Nội dung |
|---|---|
| **DP1** | Ingest dữ liệu thô từ nguồn vào Bronze, kèm bước validate |
| **DP2** | Từ Bronze sang Silver và Gold, kèm bước validate |
| **DP3** | Tính bảng feature offline (ví dụ `feat_sku_return_rate_90d`), kèm bước validate |

## 3. Công nghệ

| Vai trò | Công nghệ | Ghi chú |
|---|---|---|
| Đóng gói | Docker, Docker Compose | Tối ưu image (multi-stage build) |
| Batch | Apache Spark | Xử lý skew, high cardinality, schema evolution |
| Streaming | Apache Kafka, Apache Flink | Xử lý burst, late arrival, duplicate, window |
| Điều phối | Apache Airflow | |
| Lakehouse | MinIO + Apache Iceberg | Compaction, z-order, partitioning. Chọn Iceberg thay Delta: [ADR-0003](docs/adr/0003-iceberg-va-rest-catalog-cho-lakehouse.md) |
| Catalog | Lakekeeper (Iceberg REST) | Catalog cho cả Spark, Flink và Trino |
| Query liên nguồn | Trino | Một cửa sổ SQL thấy cả lakehouse lẫn PostgreSQL |
| Data Warehouse | PostgreSQL | Indexing |
| Governance | DataHub | Lineage, validation, data contract |

Các lựa chọn này là dự kiến và có thể thay đổi trong quá trình triển khai.

## 4. Dữ liệu và Data Generator

Generator tạo dữ liệu theo cấu hình. Ví dụ cấu hình dự kiến:

```yaml
scale:
  n_skus: 2000
  days_history: 180
  orders_per_day: 4000        # trên toàn bộ 7 Storefront
storefront_weights:
  amazon_us: 0.45
  tiktok_shop_us: 0.15
  amazon_uk: 0.12
  amazon_de: 0.10
  shopify: 0.08
  walmart_us: 0.07
  shopee_vn: 0.03
```

Quy mô kết quả: khoảng 720 nghìn Order và hơn 1 triệu Order Line. Cấu hình đầy đủ xem [docs/generator-design.md](docs/generator-design.md#10-cấu-hình).

Các vấn đề dữ liệu được mô phỏng:

| Vấn đề | Cách mô phỏng |
|---|---|
| Skew | Tỷ trọng Storefront lệch, vài SKU chiếm phần lớn doanh số, 8 danh mục với tỷ lệ trả hàng khác nhau |
| High cardinality | Khoảng 720 nghìn mã Order, hơn 1 triệu mã Order Line, mã vận đơn, mã buyer theo Channel |
| Schema evolution | Dữ liệu đơn Amazon chuyển từ Orders v0 sang Orders 2026-01-01 giữa chừng (đổi tên, đổi cấu trúc, thêm field) |
| Duplicate offline | Khoảng 2% dòng trùng do export chồng lấn ngày, có cả bản sửa lại |
| Duplicate, burst, late arrival | Đặc tính riêng của từng Channel trong luồng streaming |
| Data drift | Đợt sale lớn và livestream làm tỷ lệ trả hàng đổi mà feature vẫn trông bình thường |

Chạy môi trường nguồn giả lập (MinIO, PostgreSQL) và sinh dữ liệu vào đó:

```bash
cp .env.example .env
docker compose up -d                                   # MinIO, PostgreSQL, sinh Batch, nạp PostgreSQL
uv run python -m generator report --source ./out/small # báo cáo chất lượng dữ liệu (skew, cardinality, ...)
```

Image của generator được tối ưu từ **2,35 GB xuống 677 MB** (giảm 71%) nhờ multi-stage build, base image `python:3.12-slim` và không giữ cache cài đặt. Chi tiết cách chạy và cách đo xem [docs/Docker.md](docs/Docker.md). Kết quả đo chất lượng của lần chạy đầy đủ xem [docs/generator-design.md](docs/generator-design.md#kết-quả-chạy).

## 5. Repo Structure

Cấu trúc repo. Mục đánh dấu **(CHƯA có)** sẽ được tạo khi triển khai phần tương ứng; các mục còn lại đã có trong repo.

```
.
├── README.md              # Tài liệu tổng quan này
├── docs/                  # Toàn bộ tài liệu chi tiết, kèm ảnh chụp minh họa
│   ├── diagrams/          # Hình kiến trúc (html/svg/png) dùng trong tài liệu
│   └── evidence/          # Ảnh chụp Spark UI làm bằng chứng tối ưu
├── generator/             # Data Generator: sinh dữ liệu batch và streaming theo cấu hình
├── configs/               # File cấu hình YAML của generator (small để thử, default đủ quy mô)
├── scripts/               # Script vận hành: setup_lakekeeper.sh, run_bronze_evidence.sh
├── spark_jobs/            # Các job Spark cho DP1, DP2, DP3
├── flink_jobs/            # Job Flink xử lý luồng event (CHƯA có)
├── notebooks/             # Notebook báo cáo chất lượng dữ liệu (CHƯA có; output xóa trước khi commit)
├── dags/                  # Airflow DAG cho DP1, DP2, DP3 (CHƯA có)
├── sql/                   # DDL cho bảng ở các zone và Data Warehouse (CHƯA có)
├── docker/                # Dockerfile phụ (bản chưa tối ưu để so dung lượng)
├── Dockerfile             # Image tối ưu của generator (multi-stage)
├── docker-compose.yml     # MinIO, PostgreSQL và các bước sinh, nạp dữ liệu
├── .env.example           # Mẫu biến môi trường cho Compose (mật khẩu, cổng)
├── tests/                 # Unit test
└── pyproject.toml         # Thư viện Python của dự án (quản lý bằng uv)
```

## 6. Tài liệu chi tiết

| Tài liệu | Nội dung |
|---|---|
| [docs/plan.md](docs/plan.md) | **Kế hoạch chốt**: kiến trúc lakehouse, sáu giai đoạn tới 100 điểm, ngân sách RAM đo được, đường lên cloud |
| [docs/Domain.md](docs/Domain.md) | Business domain, thuật ngữ, bài toán dự đoán, vấn đề dữ liệu cần mô phỏng |
| [docs/generator-design.md](docs/generator-design.md) | Thiết kế Data Generator: kiến trúc, đường thời gian Batch và Stream, vấn đề dữ liệu, nơi lưu, cấu hình, bằng chứng chất lượng |
| [docs/channel-sources.md](docs/channel-sources.md) | Nguồn tài liệu và mức tin cậy của dạng dữ liệu từng Channel |
| [docs/Docker.md](docs/Docker.md) | Docker Compose của môi trường nguồn, tối ưu dung lượng image generator |
| [docs/spark.md](docs/spark.md) | Tối ưu Spark giai đoạn Bronze: baseline, chuỗi tối ưu, bằng chứng Spark UI |
| [docs/silver.md](docs/silver.md) | Tầng Silver: hai bảng `stg_*` theo kênh, độ hạt Order Line, khoá dedup, kiểu dữ liệu, số đo lần chạy thật, và hai khoản còn mở |
| [docs/data-quality-report-batch.md](docs/data-quality-report-batch.md) | Báo cáo chất lượng dữ liệu của generator phần Batch, đối chiếu từng dòng rubric với số đo và ảnh chụp |
| [docs/data-quality-report-stream.md](docs/data-quality-report-stream.md) | Báo cáo phần Stream: trễ, lệch thứ tự, trùng, burst livestream, và số tin trong Kafka |

Các tài liệu sau sẽ được bổ sung cùng phần triển khai tương ứng: Flink và tối ưu lưu trữ, schema phần Gold (`dim_`/`fact_`/`feat_`/`obt_`) và SCD2, Orchestration, Governance.

## 7. Lộ trình

Module data engineering (tài liệu này) là nền cho phần mở rộng ở giai đoạn sau: Feature Store (Feast), huấn luyện và versioning mô hình, serving, drift detection, CI/CD, observability, và các thành phần RAG/Agent. Sơ đồ ở mục 2 sẽ được mở rộng khi các phần đó được thiết kế.
