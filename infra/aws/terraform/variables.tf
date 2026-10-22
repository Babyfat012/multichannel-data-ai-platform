# Đầu vào cho một EC2 DE; allowlist máy để không lỡ tay bật cỡ máy đốt hết credit.
variable "region" {
  type    = string
  default = "ap-southeast-1"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.region))
    error_message = "Region phải có dạng ap-southeast-1."
  }
}

variable "availability_zone" {
  description = "EBS và EC2 phải cùng AZ; EBS không gắn được sang AZ khác."
  type        = string
  default     = "ap-southeast-1a"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9][a-z]$", var.availability_zone))
    error_message = "AZ phải có dạng ap-southeast-1a."
  }
}

variable "name_prefix" {
  type    = string
  default = "fsds"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,16}[a-z0-9]$", var.name_prefix))
    error_message = "Prefix có 2–18 ký tự chữ thường/số/gạch nối để tên bucket hợp lệ."
  }
}

variable "instance_type" {
  description = "t3.xlarge = 4 vCPU / 16 GiB, khớp giới hạn trong docker-compose.aws.yml."
  type        = string
  default     = "t3.xlarge"
  validation {
    condition     = contains(["t3.xlarge", "t3.2xlarge", "m6i.xlarge", "m6i.2xlarge"], var.instance_type)
    error_message = "Overlay cần ít nhất 16 GiB RAM: chọn t3.xlarge/2xlarge hoặc m6i.xlarge/2xlarge."
  }
}

variable "cpu_credits" {
  description = "standard: hết CPU credit thì chạy chậm lại, không tính thêm tiền như unlimited."
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "unlimited"], var.cpu_credits)
    error_message = "cpu_credits chỉ nhận standard hoặc unlimited."
  }
}

variable "ssh_public_key" {
  description = "Public key cho SSH đi qua SSM; không mở cổng 22. Không có mặc định."
  type        = string
  nullable    = false
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256) ", var.ssh_public_key))
    error_message = "Dán nội dung file .pub (bắt đầu bằng ssh-ed25519, ssh-rsa hoặc ecdsa-sha2-nistp256)."
  }
}

variable "boot_disk_gb" {
  type    = number
  default = 30
  validation {
    condition     = var.boot_disk_gb >= 20 && floor(var.boot_disk_gb) == var.boot_disk_gb
    error_message = "Boot disk phải là số nguyên ≥20 GiB."
  }
}

variable "data_disk_gb" {
  description = "EBS vẫn tính tiền khi EC2 tắt; tăng dung lượng là tăng chi phí cố định."
  type        = number
  default     = 100
  validation {
    condition     = var.data_disk_gb >= 10 && floor(var.data_disk_gb) == var.data_disk_gb
    error_message = "Data disk phải là số nguyên ≥10 GiB."
  }
}

variable "enable_budget" {
  type    = bool
  default = false
}

variable "budget_email" {
  description = "AWS không tự gửi mail như GCP; phải ghi rõ người nhận khi bật budget."
  type        = string
  default     = null
}

variable "budget_amount_usd" {
  type    = number
  default = 30
  validation {
    condition     = var.budget_amount_usd > 0 && floor(var.budget_amount_usd) == var.budget_amount_usd
    error_message = "Budget USD phải là số nguyên dương; đây là cảnh báo, không khóa chi tiêu."
  }
}

variable "tags" {
  type = map(string)
  default = {
    project     = "fsds"
    environment = "learning"
  }
}

variable "noncurrent_retention_days" {
  description = "Xóa phiên bản object không còn hiện hành sau số ngày này; không xóa live Iceberg."
  type        = number
  default     = 30
  validation {
    condition     = var.noncurrent_retention_days >= 1 && floor(var.noncurrent_retention_days) == var.noncurrent_retention_days
    error_message = "Retention phải là số ngày nguyên dương."
  }
}

variable "temp_retention_days" {
  description = "Chỉ xóa object dưới prefix tmp/; không đặt dữ liệu chính thức vào prefix này."
  type        = number
  default     = 7
  validation {
    condition     = var.temp_retention_days >= 1 && floor(var.temp_retention_days) == var.temp_retention_days
    error_message = "Retention tạm phải là số ngày nguyên dương."
  }
}

variable "force_destroy" {
  description = "Mặc định từ chối xóa bucket còn dữ liệu; chỉ bật sau khi đã kiểm tra và sao lưu."
  type        = bool
  default     = false
}
