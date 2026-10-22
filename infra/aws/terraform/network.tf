# VPC riêng: có đường ra Internet để kéo image, không mở cổng vào nào (SSH đi qua SSM).
resource "aws_vpc" "de" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name_prefix}-de-vpc" }
}

# /24 đầu cho mốc 1; mốc 2 (EKS) thêm subnet ở AZ khác trong cùng /16.
resource "aws_subnet" "public" {
  vpc_id            = aws_vpc.de.id
  cidr_block        = "10.42.0.0/24"
  availability_zone = var.availability_zone
  tags              = { Name = "${var.name_prefix}-de-public" }
}

# AWS mặc định kín: thiếu gateway + route thì VM không ra được Internet.
resource "aws_internet_gateway" "de" {
  vpc_id = aws_vpc.de.id
  tags   = { Name = "${var.name_prefix}-de-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.de.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.de.id
  }
  tags = { Name = "${var.name_prefix}-de-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Không có ingress: SSM agent tự gọi ra AWS nên không cần mở cổng 22.
resource "aws_security_group" "vm" {
  name        = "${var.name_prefix}-de-vm"
  description = "FSDS DE VM: no ingress, egress for SSM, image pull and S3"
  vpc_id      = aws_vpc.de.id
  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
