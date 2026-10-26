# VPC riêng: có đường ra Internet để kéo image, không mở cổng vào nào (SSH đi qua SSM).
resource "aws_vpc" "data_platform" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name_prefix}-data-platform-vpc" }
}

# /24 đầu cho mốc 1; mốc 2 (EKS) thêm subnet ở AZ khác trong cùng /16.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.data_platform.id
  cidr_block              = "10.42.0.0/24"
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = var.enable_kubernetes_network
  tags = merge(
    { Name = "${var.name_prefix}-data-platform-public" },
    var.enable_kubernetes_network ? { "kubernetes.io/role/elb" = "1" } : {}
  )
}

# EKS cần subnet ở hai AZ. Node có public IP cho egress, không có rule SSH public.
resource "aws_subnet" "kubernetes_secondary" {
  count                   = var.enable_kubernetes_network ? 1 : 0
  vpc_id                  = aws_vpc.data_platform.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = var.secondary_availability_zone
  map_public_ip_on_launch = true
  tags = {
    Name                     = "${var.name_prefix}-kubernetes-public-secondary"
    "kubernetes.io/role/elb" = "1"
  }

  lifecycle {
    precondition {
      condition = var.secondary_availability_zone == null ? false : (
        startswith(var.secondary_availability_zone, var.region) &&
        var.secondary_availability_zone != var.availability_zone
      )
      error_message = "Kubernetes yêu cầu secondary_availability_zone khác AZ đầu và cùng region."
    }
  }
}

resource "aws_route_table_association" "kubernetes_secondary" {
  count          = var.enable_kubernetes_network ? 1 : 0
  subnet_id      = aws_subnet.kubernetes_secondary[0].id
  route_table_id = aws_route_table.public.id
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
