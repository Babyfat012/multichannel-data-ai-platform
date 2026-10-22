# Rà soát sẵn sàng AWS — 2026-10-22

Trạng thái: gói DE mốc 1 đã được kiểm offline; **chưa đủ bằng chứng để triển khai
ngay trên tài khoản trong ảnh**. Không chạy Terraform plan/apply, không gọi API
tài khoản AWS, không chạy playbook lên VM, không tạo hay khởi động tài nguyên.

## Credit và điều kiện tài khoản

Ảnh người dùng cung cấp hiển thị $200 credit, Free plan, còn 183 ngày đến
2027-04-22; tháng hiện tại $0 và chưa có dữ liệu chi phí. Đây là bằng chứng từ
Console tại thời điểm chụp, chưa được đối chiếu bằng API.

Free plan giới hạn dịch vụ/tính năng; credit không đồng nghĩa với quyền dùng
mọi loại máy. `t3.xlarge` trong cấu hình hiện tại không nằm trong danh sách EC2
được AWS đánh dấu Free Tier eligible. Cần xác nhận khả năng sử dụng loại máy
này theo plan thực tế trước khi chạy; không tự nâng cấp tài khoản. Nếu cần
Paid plan, đó là quyết định của người dùng; credit còn lại vẫn được áp cho
chi phí đủ điều kiện và có thể phát sinh thanh toán sau khi credit hết.
Nguồn: [EC2 Free Tier](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-free-tier-usage.html),
[Free/Paid plan](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html).

Ảnh Console đang ở `ap-southeast-2` (Sydney), cấu hình mẫu chọn
`ap-southeast-1` / `ap-southeast-1a` (Singapore). Chưa đổi region dựa trên ảnh;
cần chọn một region rồi điền nhất quán Terraform, Ansible và AWS CLI.

Các truy vấn chỉ đọc dành cho lần kiểm tài khoản sau này, **chưa chạy**:

```bash
aws sts get-caller-identity
aws freetier get-account-plan-state
aws ec2 describe-instance-types --region ap-southeast-1 \
  --filters Name=free-tier-eligible,Values=true \
  --query 'InstanceTypes[*].[InstanceType,VCpuInfo.DefaultVCpus,MemoryInfo.SizeInMiB]'
aws service-quotas get-service-quota --service-code ec2 \
  --quota-code L-1216C47A --region ap-southeast-1
```

Quota trên là On-Demand Standard instances; còn phải đối chiếu số vCPU đã
sử dụng, khả năng cung cấp instance tại AZ và quyền IAM. Các truy vấn này
không chứng minh launch thành công.
Nguồn: [GetAccountPlanState](https://docs.aws.amazon.com/cli/latest/reference/freetier/get-account-plan-state.html).

## Phạm vi gói hiện tại

- Terraform: EC2 Ubuntu 24.04, EBS gp3, VPC/subnet/route, IAM instance profile,
  hai bucket S3 và budget tùy chọn; chưa có EKS.
- Runtime: PostgreSQL, Lakekeeper, Spark master/hai worker, Airflow; worker
  dùng cùng image Python 3.10 với driver. Overlay dành cho máy ít nhất 16 GiB.
- Nguồn: fixture hoặc scale, đồng bộ S3 và nạp Walmart vào PostgreSQL; smoke
  S3/Iceberg chỉ chạy khi người dùng yêu cầu triển khai.
- Kafka/Flink/CDC/Trino bị vô hiệu trong overlay. MLE, Feast, model, agent và
  EKS chưa có gói triển khai AWS; sơ đồ AWS MLE là thiết kế, chưa là bằng chứng.

## Lỗi chuẩn bị đã sửa

1. Spark master kế thừa `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` từ Compose
   local: thay toàn bộ mapping environment của master trong overlay AWS để
   không đưa placeholder MinIO vào chuỗi xác thực AWS.
2. Airflow Spark operator luôn lấy credential MinIO: chọn theo `FSDS_STORAGE`;
   S3/GCS dùng danh tính máy, không tra Connection MinIO, không truyền key
   MinIO. Loại credential cũ khỏi môi trường process Spark, kể cả session
   token, rồi phục hồi trạng thái operator sau thành công hoặc lỗi.
3. Allowlist Terraform cho phép `t3.large` 8 GiB dù overlay cần khoảng 14 GiB:
   bỏ lựa chọn này. Thêm điều kiện AZ thuộc region.
4. Lệnh tunnel thiếu private key mẫu: bổ sung `-i ~/.ssh/fsds_aws` cho khớp
   inventory. Sửa mô tả chi phí: public IPv4 tự cấp được thu hồi khi stop;
   EBS vẫn được giữ và tính phí, không có Elastic IP trong cấu hình.

## Kiểm tra đã thực hiện

- `terraform fmt -check` và `terraform validate`: đạt với provider AWS
  6.70.0 được ghim trong `.terraform.lock.hcl`; validate chỉ kiểm schema,
  không chứng minh quota hay quyền tài khoản.
- Ansible `--syntax-check`: đạt với ansible-core 2.21.5 và collection
  `ansible.posix` có sẵn. Có cảnh báo `apt_repository` sẽ bị bỏ ở core 2.25;
  đây chưa phải lỗi cú pháp của phiên bản đã kiểm.
- Render template env bằng giá trị giả, chạy Compose `config` sau khi merge:
  đạt; đúng sáu service runtime và ba init, port loopback, không còn key AWS
  tĩnh trong environment của các service runtime. Không khởi động container.
- Test runtime Airflow bao gồm danh tính Cloud, loại key cũ và phục hồi trạng
  thái khi Spark lỗi; test cấu hình AWS/GCP, DAG, loader Walmart và tài liệu:
  **426 passed**, không có lỗi. Một cảnh báo về `record_property` khi xuất JUnit.
- Ruff và `bash -n` cho các file liên quan: đạt.

Kết quả có cấu trúc: [evidence JSON](evidence/aws-readiness-20261022.json).

## Còn thiếu trước khi cho phép chạy

- Xác nhận account plan, loại EC2 được phép, region, quota còn trống và giá
  tại region đã chọn. Nếu bị giới hạn Free plan, chốt phương án với người dùng;
  không hạ xuống máy ít RAM rồi coi là tương đương cấu hình này.
- Máy điều khiển hiện chưa có `aws` CLI và `session-manager-plugin` trên PATH.
  Cần chuẩn bị đăng nhập, quyền Terraform/SSM, key SSH đúng và Ansible cho
  lần triển khai. Công cụ Ansible dùng trong audit nằm ở môi trường tạm.
- Các file thật `terraform.tfvars`, inventory và biến Ansible phải được điền,
  secret mã hóa bằng Vault. Giá trị `CHANGE_ME` trong file mẫu chưa dùng được.
- Chốt budget/email và lịch dừng máy. Budget chỉ cảnh báo; EBS/S3 vẫn có chi
  phí khi EC2 dừng. Chưa có tác vụ tự động dừng EC2 trong gói hiện tại.
- Review Terraform plan theo tài khoản thật, rồi mới quyết định apply. Sau đó
  vẫn cần kiểm SSM, IMDSv2 từ container, S3 raw, ghi/đọc Iceberg, DAG đầu cuối
  và lưu receipt; các mục này chưa được chứng minh bởi kiểm tra offline.

Điểm vào: [Terraform AWS](../infra/aws/terraform/README.md),
[Ansible AWS](../infra/aws/ansible/README.md).
