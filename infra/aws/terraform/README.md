<!-- Mục đích: điểm vào Terraform AWS cho DE (mốc 1): dựng gì, chạy thế nào, tốn gì. -->
# Terraform AWS cho DE (mốc 1)

Terraform ≥1.6, provider `hashicorp/aws` `~> 6.0` (bản cụ thể ghim trong
`.terraform.lock.hcl`). State để local. Mốc 1 **chưa có EKS**.

Dựng: VPC `10.42.0.0/16` + 1 public subnet + Internet Gateway, security group
**không mở cổng vào**, một EC2 `t3.xlarge` Ubuntu 24.04, một EBS gp3 dữ liệu,
IAM role (instance profile, không access key), 2 bucket S3 (`raw`, `lakehouse`),
budget tùy chọn. Sơ đồ: [`docs/diagrams/aws-infra.html`](../../../docs/diagrams/aws-infra.html).

Trước khi chạy: xem [rà soát sẵn sàng AWS](../../../docs/aws-readiness-20261022.md).
Credit không tự mở quyền dùng mọi loại EC2: ảnh tài khoản ngày 2026-10-22 vẫn là
Free plan; `t3.xlarge` chưa được xác nhận có thể launch trên tài khoản này.

## Chạy

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars   # điền ssh_public_key
terraform init
terraform plan -out de.tfplan
terraform apply de.tfplan
```

Máy bạn cần `aws` CLI đã đăng nhập và `session-manager-plugin` (để SSH qua SSM).
Sau đó sang [Ansible](../ansible/README.md).

## Lưu ý tiền

- EC2 chỉ tính tiền khi chạy: `aws ec2 stop-instances --instance-ids <id>` khi nghỉ.
- **EBS vẫn tính tiền khi EC2 tắt.** Public IPv4 tự cấp của cấu hình này được
  AWS thu hồi khi stop; không có Elastic IP. Xem [quy tắc IP của EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html).
- `cpu_credits = "standard"`: hết CPU credit thì máy chậm lại, không bị tính thêm.
- Budget chỉ cảnh báo, không chặn chi. Bật bằng `enable_budget = true` + `budget_email`.
- `terraform destroy` xóa cả EBS dữ liệu. Bucket còn dữ liệu sẽ chặn destroy
  trừ khi `force_destroy = true`.

## Khác bản GCP

| GCP (`infra/gcp`) | AWS (ở đây) |
|---|---|
| IAP + OS Login, firewall mở 22 cho dải IAP | SSM, security group không ingress |
| Service account gắn VM | IAM role + instance profile |
| `/dev/disk/by-id/google-fsds-data` | `/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol…` |
| Budget tự gửi mail Billing Admin | Phải ghi `budget_email` |
| Bật API (`google_project_service`) | Không cần |
