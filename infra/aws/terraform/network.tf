# VPC riêng: có đường ra Internet để kéo image, không mở cổng vào nào (SSH đi qua SSM).
resource "aws_vpc" "data_platform" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name_prefix}-data-platform-vpc" }
}

# /24 đầu cho mốc 1; mốc 2 (EKS) thêm subnet ở AZ khác trong cùng /16.
resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.data_platform.id
  cidr_block        = "10.42.0.0/24"
  availability_zone = var.availability_zone
  tags              = { Name = "${var.name_prefix}-data-platform-public" }
}

# AWS mặc định kín: thiếu gateway + route thì VM không ra được Internet.
resource "aws_internet_gateway" "data_platform" {
  vpc_id = aws_vpc.data_platform.id
  tags   = { Name = "${var.name_prefix}-data-platform-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.data_platform.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.data_platform.id
  }
  tags = { Name = "${var.name_prefix}-data-platform-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Không có ingress: SSM agent tự gọi ra AWS nên không cần mở cổng 22.
resource "aws_security_group" "vm" {
  name        = "${var.name_prefix}-data-platform-vm"
  description = "Data Platform EC2 instance: no ingress, egress for SSM, image pull and S3"
  vpc_id      = aws_vpc.data_platform.id
  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
