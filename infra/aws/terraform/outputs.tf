# Giao diện bàn giao cho Ansible và các lệnh để người vận hành tự mở phiên/UI qua SSM.
output "raw_bucket" {
  description = "Tên bucket raw (không gồm s3://)."
  value       = aws_s3_bucket.data["raw"].id
}

output "lakehouse_bucket" {
  description = "Tên bucket warehouse Iceberg (không gồm s3://)."
  value       = aws_s3_bucket.data["lakehouse"].id
}

output "raw_bucket_uri" {
  value = "s3://${aws_s3_bucket.data["raw"].id}"
}

output "lakehouse_bucket_uri" {
  value = "s3://${aws_s3_bucket.data["lakehouse"].id}"
}

output "region" {
  value = var.region
}

output "vpc_id" {
  value = aws_vpc.data_platform.id
}

output "kubernetes_subnet_ids" {
  value = var.enable_kubernetes_network ? concat(
    [aws_subnet.public.id], aws_subnet.kubernetes_secondary[*].id
  ) : []
}

output "data_security_group_id" {
  value = aws_security_group.vm.id
}

output "data_private_ip" {
  value = aws_instance.data_platform.private_ip
}

output "ecr_repositories" {
  value = { for name, repo in aws_ecr_repository.application : name => repo.repository_url }
}

output "image_registry" {
  description = "Prefix dùng với scripts/run/push_images.sh; gồm account registry và namespace dự án."
  value = var.enable_ecr ? trimsuffix(
    aws_ecr_repository.application["platform-feature-api"].repository_url, "/platform-feature-api"
  ) : null
}

output "instance_id" {
  description = "Dùng làm ansible_host trong inventory (SSH đi qua SSM theo ID)."
  value       = aws_instance.data_platform.id
}

output "data_volume_id" {
  description = "Ansible tìm disk ở /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_<id bỏ dấu ->."
  value       = aws_ebs_volume.data.id
}

output "iam_role_name" {
  value = aws_iam_role.vm.name
}

output "ssm_command" {
  value = "aws ssm start-session --target ${aws_instance.data_platform.id} --region ${var.region}"
}

output "port_forward_command" {
  description = "Tunnel SSH qua SSM; dùng cùng key path với inventory mẫu (đổi -i nếu lưu key nơi khác)."
  value       = "ssh -N -i ~/.ssh/platform_aws -o ExitOnForwardFailure=yes -o ProxyCommand='aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p --region ${var.region}' -L 127.0.0.1:8080:127.0.0.1:8080 -L 127.0.0.1:8081:127.0.0.1:8081 -L 127.0.0.1:8181:127.0.0.1:8181 -L 127.0.0.1:9002:127.0.0.1:9002 -L 127.0.0.1:8089:127.0.0.1:8089 -L 127.0.0.1:8082:127.0.0.1:8082 ubuntu@${aws_instance.data_platform.id}"
}
