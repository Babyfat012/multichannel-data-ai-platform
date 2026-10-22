# Giao diện bàn giao cho Ansible và các lệnh để người học tự mở phiên/UI qua SSM.
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

output "instance_id" {
  description = "Dùng làm ansible_host trong inventory (SSH đi qua SSM theo ID)."
  value       = aws_instance.de.id
}

output "data_volume_id" {
  description = "Ansible tìm disk ở /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_<id bỏ dấu ->."
  value       = aws_ebs_volume.data.id
}

output "iam_role_name" {
  value = aws_iam_role.vm.name
}

output "ssm_command" {
  value = "aws ssm start-session --target ${aws_instance.de.id} --region ${var.region}"
}

output "port_forward_command" {
  description = "Tunnel SSH qua SSM; dùng cùng key path với inventory mẫu (đổi -i nếu lưu key nơi khác)."
  value       = "ssh -N -i ~/.ssh/fsds_aws -o ExitOnForwardFailure=yes -o ProxyCommand='aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p --region ${var.region}' -L 127.0.0.1:8080:127.0.0.1:8080 -L 127.0.0.1:8081:127.0.0.1:8081 -L 127.0.0.1:8181:127.0.0.1:8181 ubuntu@${aws_instance.de.id}"
}
