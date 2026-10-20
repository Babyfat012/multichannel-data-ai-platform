---
status: accepted
---

# Hợp đồng schema nằm trong Git, CI chặn thay đổi phá vỡ

Ngày 2026-10-19, người học chọn giữ registry của DE-N1 là **file trong Git** (`contracts/registry.json`,
`contracts/schemas/**`) và thêm **CI chạy `check-registry`**, thay vì dựng một service schema registry.
Lý do của người học: **team nhỏ** — thêm một service là thêm thứ phải vận hành và một điểm có thể sập.

Producer ngoài (Amazon, Walmart, Shopee…) không đăng ký schema vào registry của mình; lớp bảo vệ thật với họ
là **cổng cách ly lúc chạy** ở đầu Silver (`silver.lifecycle_quarantine_records`). CI chỉ bảo vệ phía mình: không ai
sửa registry/schema theo cách phá vỡ (version breaking dùng lại parser cũ) mà không bị đỏ.

## Thực hiện

- `.github/workflows/ci.yml`: mỗi push/PR chạy `python -m spark_jobs.lib.data_contracts check-registry
  --contracts-dir contracts` và `pytest -q` (không Docker, không Spark, không Cloud).
- `.pre-commit-config.yaml`: hook local chạy `check-registry` khi commit sửa `contracts/` hoặc
  `spark_jobs/lib/data_contracts/` — báo sớm trên máy, không thay CI.
- **Branch protection** cho `main` (bắt CI xanh trước khi merge, cấm push thẳng) do người dùng tự bật trên GitHub.

## Giá phải trả (phân tích phiên Sonnet, người học chưa tự nêu)

- Chỉ chặn khi CI chạy: thiếu branch protection thì push thẳng lên `main` vẫn lọt; `git commit --no-verify` bỏ
  qua được hook local.
- Phải tự viết và bảo trì bước CI; phụ thuộc GitHub Actions.
- Không chặn producer ngoài — việc đó vẫn là cổng cách ly lúc chạy.

## Phương án không chọn

Service registry (kiểu Confluent Schema Registry): chặn ngay lúc đăng ký, có API cho producer, nhưng nặng
thêm một service, cần lưu trữ/backup riêng, và producer ngoài của dự án vẫn không dùng nó.
