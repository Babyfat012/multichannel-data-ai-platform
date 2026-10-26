# Đầu vào phải được chốt theo tài khoản/quota/ngân sách thực trước khi lập plan.
variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "name_prefix" {
  type    = string
  default = "platform"
}

variable "data_platform_state" {
  description = "Đường dẫn state local sau apply root ../terraform với mạng hai AZ đã bật. Không commit state."
  type        = string
}

variable "kubernetes_version" {
  description = "Chọn bản EKS đang được AWS hỗ trợ tại region triển khai; không tự chọn theo phiên bản kind local."
  type        = string
}

variable "admin_role_arn" {
  description = "IAM role thường trực dùng để quản trị EKS, không dùng STS assumed-role session ARN."
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+", var.admin_role_arn))
    error_message = "admin_role_arn phải là IAM role ARN."
  }
}

variable "api_allowed_cidrs" {
  description = "IP egress của máy quản trị/CI, ví dụ IPv4/32; endpoint private luôn bật cho node."
  type        = list(string)
  validation {
    condition = length(var.api_allowed_cidrs) > 0 && alltrue([
      for c in var.api_allowed_cidrs : can(cidrnetmask(c)) && try(tonumber(split("/", c)[1]) >= 24, false)
    ])
    error_message = "Cần CIDR IPv4 cụ thể /24 hoặc hẹp hơn; không dùng 0.0.0.0/0."
  }
}

variable "node_instance_types" {
  description = "Các instance x86_64 cùng cỡ tài nguyên; đối chiếu quota và giá thực trước khi điền."
  type        = list(string)
  validation {
    condition     = length(var.node_instance_types) > 0
    error_message = "Chọn ít nhất một loại node."
  }
}

variable "node_desired_size" {
  type    = number
  default = 1
  validation {
    condition     = var.node_desired_size >= 0 && floor(var.node_desired_size) == var.node_desired_size
    error_message = "node_desired_size phải là số nguyên >= 0; 0 dành cho tạm nghỉ workload."
  }
}

variable "node_min_size" {
  type    = number
  default = 1
  validation {
    condition     = var.node_min_size >= 0 && floor(var.node_min_size) == var.node_min_size
    error_message = "node_min_size phải là số nguyên >= 0."
  }
}

variable "node_update_strategy" {
  description = "DEFAULT tạo node thay thế trước; MINIMAL giảm peak CPU quota nhưng có thể gián đoạn workload."
  type        = string
  default     = "DEFAULT"
  validation {
    condition     = contains(["DEFAULT", "MINIMAL"], var.node_update_strategy)
    error_message = "Chọn DEFAULT hoặc MINIMAL."
  }
}

variable "single_az_workers" {
  description = "Một worker tiết kiệm: giữ AZ cố định để PVC EBS vẫn dùng được sau scale-to-zero/resume. Control plane vẫn hai AZ."
  type        = bool
  default     = false
}

variable "node_max_size" {
  type    = number
  default = 2
}

variable "node_disk_gb" {
  type    = number
  default = 80
}

variable "data_service_ports" {
  description = "Chỉ các cổng EC2 được mở từ SG của cluster. Mặc định chỉ PostgreSQL và Lakekeeper."
  type        = set(number)
  default     = [5432, 8181]
  validation {
    condition     = alltrue([for p in var.data_service_ports : contains([5432, 8181, 5050, 8200], p)])
    error_message = "Chỉ cho phép PostgreSQL, Lakekeeper, MLflow hoặc Vault; không mở UI/SSH công khai."
  }
}
