# Các đầu vào cho một VM DE, có kiểm thô để tránh vượt trần trial 8 vCPU.
variable "project_id" {
  description = "Project ID do người học nhập, không có mặc định hay ID thật trong repo."
  type        = string
  nullable    = false
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Project ID phải có 6–30 ký tự chữ thường/số/gạch nối, bắt đầu bằng chữ."
  }
}

variable "region" {
  type    = string
  default = "asia-southeast1"
  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+$", var.region))
    error_message = "Region phải có dạng asia-southeast1."
  }
}

variable "zone" {
  type    = string
  default = "asia-southeast1-b"
  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+-[a-z]$", var.zone))
    error_message = "Zone phải có dạng asia-southeast1-b."
  }
}

variable "name_prefix" {
  type    = string
  default = "fsds"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,16}[a-z0-9]$", var.name_prefix))
    error_message = "Prefix có 2–18 ký tự chữ thường/số/gạch nối để tên bucket và SA hợp lệ."
  }
}

variable "machine_type" {
  description = "Kiểm thô theo allowlist E2 (≤8 vCPU mỗi VM), không kiểm quota/VM khác đang chạy."
  type        = string
  default     = "e2-standard-8"
  validation {
    condition = contains([
      "e2-micro", "e2-small", "e2-medium",
      "e2-standard-2", "e2-standard-4", "e2-standard-8",
      "e2-highmem-2", "e2-highmem-4", "e2-highmem-8",
      "e2-highcpu-2", "e2-highcpu-4", "e2-highcpu-8"
    ], var.machine_type)
    error_message = "Chỉ cho phép E2 trong bảng ≤8 vCPU. Người học vẫn phải kiểm tổng quota trial."
  }
}

variable "boot_disk_gb" {
  type    = number
  default = 50
  validation {
    condition     = var.boot_disk_gb >= 20 && floor(var.boot_disk_gb) == var.boot_disk_gb
    error_message = "Boot disk phải là số nguyên ≥20 GiB."
  }
}

variable "data_disk_gb" {
  type    = number
  default = 200
  validation {
    condition     = var.data_disk_gb >= 10 && floor(var.data_disk_gb) == var.data_disk_gb
    error_message = "Data disk phải là số nguyên ≥10 GiB; tăng dung lượng làm tăng chi phí."
  }
}

variable "enable_budget" {
  type    = bool
  default = false
}

variable "billing_account_id" {
  description = "Chỉ nhập local khi bật budget; không phải credential."
  type        = string
  default     = null
  validation {
    condition     = var.billing_account_id == null ? true : can(regex("^[A-Fa-f0-9]{6}-[A-Fa-f0-9]{6}-[A-Fa-f0-9]{6}$", var.billing_account_id))
    error_message = "Billing account phải đúng dạng ba nhóm sáu ký tự hex hoặc null."
  }
}

variable "budget_amount_usd" {
  type    = number
  default = 50
  validation {
    condition     = var.budget_amount_usd > 0 && floor(var.budget_amount_usd) == var.budget_amount_usd
    error_message = "Budget USD phải là số nguyên dương; đây là cảnh báo, không khóa chi tiêu."
  }
}

variable "labels" {
  type = map(string)
  default = {
    project     = "fsds"
    environment = "learning"
  }
  validation {
    condition = length(var.labels) <= 64 && alltrue([
      for k, v in var.labels :
      can(regex("^[a-z][a-z0-9_-]{0,62}$", k)) && can(regex("^[a-z0-9_-]{0,63}$", v))
    ])
    error_message = "Tối đa 64 labels; key bắt đầu bằng chữ thường, key/value ≤63 ký tự hợp lệ."
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
