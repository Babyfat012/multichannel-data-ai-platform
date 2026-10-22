<!-- Mục đích: điểm vào Terraform K1, dẫn tới hướng dẫn triển khai và vận hành bằng tiếng Việt. -->
# Terraform GCP cho DE

Đọc [runbook triển khai](../../../docs/gcp-deploy.md) trước khi chạy lệnh.
Terraform ≥1.6, provider `hashicorp/google` ghim `6.49.0`; state mặc định local.
Một VM GCE, hai bucket GCS, VPC/IAP SSH, IAM tối thiểu và budget tùy chọn.
Mặc định 8 vCPU/32 GiB, boot 50 GiB + data 200 GiB, cùng `pd-balanced`.

Chỉ người học chạy Cloud/Terraform plan/apply/destroy/Ansible. Trợ lý chỉ
chuẩn bị file và kiểm offline. `init -backend=false` vẫn cần mạng để tải
provider; planner kiểm `init` + `validate`, không chạy plan/apply.

`raw_bucket`, `lakehouse_bucket`, `vm_name`, `zone`, `service_account_email`
là tên output bàn giao cho K2, không thay đổi. Có thêm URI, lệnh SSH và tunnel.

Data disk là resource độc lập: gắn lại sau khi VM bị thay thế, không tự xóa
cùng VM. `terraform destroy` **vẫn xóa data disk**. Không có thuộc tính
`auto_delete` trong block `attached_disk` theo hiểu biết schema provider;
planner cần xác nhận bằng `validate` và người học kiểm plan trước apply.
