# Một EC2 Ubuntu 24.04 với EBS dữ liệu riêng; không cài runtime bằng user_data (Ansible lo).
# Hỏi tham số công khai SSM của Canonical thay vì tìm theo tên: tài khoản này không liệt kê
# được ảnh Canonical qua DescribeImages (0 kết quả), nhưng tham số SSM vẫn trả AMI mới nhất.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_key_pair" "operator" {
  key_name   = "${var.name_prefix}-data-platform"
  public_key = var.ssh_public_key
}

resource "aws_instance" "data_platform" {
  ami                         = data.aws_ssm_parameter.ubuntu.insecure_value # ID AMI công khai, không bí mật: hiện rõ trong plan
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.vm.id]
  iam_instance_profile        = aws_iam_instance_profile.vm.name
  key_name                    = aws_key_pair.operator.key_name
  associate_public_ip_address = true # IP public thay NAT Gateway (~$32/tháng)

  # Chỉ dòng t có CPU credit; m6i bỏ qua khối này.
  dynamic "credit_specification" {
    for_each = startswith(var.instance_type, "t") ? [1] : []
    content {
      cpu_credits = var.cpu_credits
    }
  }

  # IMDSv2 bắt buộc; hop 2 để container Docker (thêm một hop mạng) vẫn lấy được credential.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.boot_disk_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = { Name = "${var.name_prefix}-data-platform" }

  # AMI mới ra không được tự thay máy đang chạy.
  lifecycle {
    ignore_changes = [ami]
    precondition {
      condition     = startswith(var.availability_zone, "${var.region}")
      error_message = "availability_zone phải thuộc region đã chọn."
    }
  }
}

# Disk độc lập: thay EC2 vẫn giữ dữ liệu; `terraform destroy` vẫn xóa nó.
resource "aws_ebs_volume" "data" {
  availability_zone = var.availability_zone
  size              = var.data_disk_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data-platform-data" }
}

# Máy Nitro thấy /dev/sdf dưới tên nvme; Ansible tìm theo volume ID nên không phụ thuộc tên này.
resource "aws_volume_attachment" "data" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.data_platform.id
}
