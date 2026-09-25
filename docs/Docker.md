# Docker và Docker Compose

Tài liệu này ghi cách đóng gói generator thành image, cách dựng môi trường nguồn giả lập bằng Docker Compose, và số liệu tối ưu dung lượng image. Thiết kế tổng thể của generator xem [generator-design.md](generator-design.md).

## Mục lục

1. [Môi trường nguồn bằng Docker Compose](#1-môi-trường-nguồn-bằng-docker-compose)
2. [Tối ưu Dockerfile](#2-tối-ưu-dockerfile)

## 1. Môi trường nguồn bằng Docker Compose

`docker-compose.yml` dựng hai hệ thống giả lập nơi dữ liệu đang nằm trước khi pipeline DP1 kéo về Bronze:

| Service | Image | Vai trò | Kiểu chạy |
|---|---|---|---|
| `minio` | `quay.io/minio/minio` (bản cố định theo ngày) | Object storage chứa export dạng file của Channel | Chạy liên tục. API cổng 9000, console cổng 9001 |
| `minio-init` | như trên (có sẵn `mc`) | Tạo bucket `fsds` | Chạy một lần |
| `postgres` | `postgres:15` | Hệ thống của phòng ban khác: `walmart.orders`, `seller.skus` | Chạy liên tục, cổng 5432 |
| `generator-batch` | `data-generator` (build từ `Dockerfile`) | `batch`: sinh lịch sử, ghi thẳng vào MinIO | Chạy một lần, sau `minio-init` |
| `generator-load` | `data-generator` | `load-postgres`: đọc `pg-load/` trên MinIO, nạp vào PostgreSQL | Chạy một lần, sau `generator-batch` và khi `postgres` sẵn sàng |

Thứ tự được đảm bảo bằng `depends_on` với `service_healthy` (MinIO, PostgreSQL có healthcheck) và `service_completed_successfully` (các bước chạy một lần).

```bash
cp .env.example .env                  # đổi mật khẩu, cổng nếu cần
docker compose up -d                  # mặc định configs/small.yaml
docker compose wait generator-load    # chờ nạp PostgreSQL xong
GENERATOR_CONFIG=configs/default.yaml docker compose up -d --force-recreate generator-batch generator-load   # đủ quy mô
```

**Vì sao chỉ một bucket `fsds`.** Mỗi lần chạy ghi bốn thứ: `source-exports/`, `pg-load/`, `ground-truth/` và `run_manifest.json`. Manifest phải nằm trong một bucket, nên generator ghi vào gốc một bucket và bốn phần trên là bốn tiền tố. DP1 chỉ đọc `source-exports/`, còn `ground-truth/` dành cho phần huấn luyện mô hình.

**Mật khẩu và cổng** nằm trong `.env`, file này không được commit. Nếu máy đã có chương trình dùng cổng 9000, 9001 hoặc 5432, đổi `MINIO_API_PORT`, `MINIO_CONSOLE_PORT` hoặc `POSTGRES_PORT` trong `.env`.

**Đọc từ máy host.** Lệnh `report` cần ba biến môi trường để đọc MinIO:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://fsds \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

## 2. Tối ưu Dockerfile

Hai Dockerfile cài cùng một generator với cùng thư viện, chỉ khác cách build:

| | `docker/Dockerfile.naive` | `Dockerfile` |
|---|---|---|
| Base image | `python:3.12` (Debian đầy đủ, có trình biên dịch, header) | `python:3.12-slim` |
| Số stage | 1 | 2: stage build cài vào `.venv`, stage chạy chỉ copy `.venv` |
| Trình cài | `pip install .`, giữ cache pip trong image | `uv sync --frozen --no-dev --no-editable`, cache nằm ngoài image (`--mount=type=cache`) |
| Nội dung copy | Cả build context | Stage chạy chỉ có `.venv` và `configs/`, không có mã nguồn, `uv` hay cache |
| Phiên bản thư viện | Theo `pyproject.toml`, mỗi lần build có thể khác | Khóa theo `uv.lock` |
| User | root | `nobody` |

Cả hai dùng chung `.dockerignore`, loại `.venv`, `out/`, `.git`, `docs/` và `tests/` khỏi build context. Vì vậy phần chênh lệch dưới đây đến từ cách build, không phải từ việc copy nhầm dữ liệu.

### Kết quả

Đo bằng `docker images data-generator` (Docker 29):

| Image | Dung lượng |
|---|---|
| `data-generator:naive` | 2,35 GB |
| `data-generator:optimize` | 677 MB |

![Dung lượng hai image](images/docker/docker.png)

Trong ảnh, cột `DISK USAGE` là dung lượng sau khi giải nén trên máy. Cột `CONTENT SIZE` là dung lượng nén khi đẩy lên registry, giảm từ 658 MB xuống 167 MB.

Dung lượng giảm khoảng 71%. Phần lớn đến từ base image: `python:3.12` mang theo bộ công cụ build mà generator không cần lúc chạy. Phần còn lại đến từ việc không giữ cache cài đặt.

**Phần còn lại của 677 MB là thư viện**, đo bằng `du` trong image:

| Thành phần | Dung lượng |
|---|---|
| `pyarrow` (ghi Parquet) | 156 MB |
| `duckdb` (lệnh `report`) | 58 MB |
| `numpy` và thư viện đi kèm | 70 MB |
| `botocore` (qua `s3fs`, ghi MinIO) | 30 MB |
| `psycopg` bản binary (gồm libpq) | 19 MB |
| Python của base slim (`/usr/local`) | 37 MB |

Muốn giảm thêm có thể tách `duckdb` thành nhóm thư viện tùy chọn, vì lệnh `report` thường chạy trên máy host. Hiện vẫn giữ trong image để chạy được báo cáo ngay trong Compose.

### Cách đo lại

```bash
docker build -t data-generator:naive -f docker/Dockerfile.naive .
docker build -t data-generator:optimize .
docker images data-generator
```
