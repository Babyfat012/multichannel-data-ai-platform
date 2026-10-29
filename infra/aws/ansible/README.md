<!-- Mục đích: cách chạy Ansible cấu hình EC2 mốc 1 sau khi Terraform AWS đã apply. -->
# Ansible AWS Data Platform

Cấu hình EC2 do [Terraform](../terraform/README.md) tạo: cài Docker, mount EBS
vào `/mnt/platform-data`, đồng bộ repo, chạy Compose (`docker-compose.yml` +
`docker-compose.aws.yml`), tạo warehouse Lakekeeper trên S3.

SSH không đi qua Internet: `ProxyCommand` gọi `aws ssm start-session`, nên
security group không cần mở cổng 22.

Máy điều khiển cần AWS CLI đã đăng nhập, `session-manager-plugin`, Ansible và
collection bên dưới. Private key `~/.ssh/platform_aws` phải khớp `ssh_public_key`
của Terraform; đổi cả inventory và lệnh tunnel nếu dùng đường dẫn khác.

```bash
cd infra/aws/ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini                 # instance_id, region
cp group_vars/all.example.yml group_vars/all.yml       # output Terraform + secret
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
# Chỉ đổi dags/, spark_jobs/, scripts/, trino/ (bind mount): đồng bộ code, ~1 phút, không restart
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags code
# Tùy chọn: sinh nguồn và đẩy lên S3, rồi smoke ghi đọc Iceberg
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags source_data
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Khi triển khai đã được cho phép và hoàn tất: lấy lệnh bằng
`terraform output -raw port_forward_command` trong thư mục Terraform, chạy lệnh
SSH được in ra, rồi vào
`http://127.0.0.1:8081` (Airflow), `:8080` (Spark), `:8181` (Lakekeeper).

Overlay chỉ bật PostgreSQL, Lakekeeper, Spark (1 master + 2 worker) và Airflow,
tổng giới hạn ≈14 GiB để vừa `t3.xlarge` 16 GiB. Kafka, Flink, Trino, Debezium
tắt mặc định; bật từng phần khi cần và tắt phần khác để không hết RAM.

Xem kết quả rà soát và các điều kiện còn thiếu
trước khi chạy playbook trên EC2.
