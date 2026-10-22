# Đầu vào cụm GKE phần MLE; mặc định vừa trần trial 8 vCPU (2 × e2-standard-4 = 8 vCPU / 32 GiB).
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
}

variable "zone" {
  description = "Cụm ZONAL: phí quản lý một cụm zonal được miễn (free tier GKE)."
  type        = string
  default     = "asia-southeast1-b"
}

variable "name_prefix" {
  type    = string
  default = "fsds"
}

variable "machine_type" {
  description = "Máy node. Thiếu RAM chứ không thiếu CPU ⇒ e2-standard-4 (4 vCPU/16 GiB)."
  type        = string
  default     = "e2-standard-4"
  validation {
    condition     = contains(["e2-standard-2", "e2-standard-4", "e2-highmem-2", "e2-highmem-4"], var.machine_type)
    error_message = "Chỉ cho phép E2 ≤4 vCPU mỗi node để 2 node không vượt trần trial 8 vCPU."
  }
}

variable "max_nodes" {
  description = "Trần node pool. min luôn 0 để hạ cụm về 0 máy khi hết buổi (tắt tiền compute)."
  type        = number
  default     = 2
  validation {
    condition     = var.max_nodes >= 1 && var.max_nodes <= 2
    error_message = "Tối đa 2 node (trần trial 8 vCPU với e2-standard-4)."
  }
}

variable "spot" {
  description = "Node Spot rẻ hơn ~60–90% nhưng có thể bị thu hồi bất kỳ lúc nào."
  type        = bool
  default     = false
}

variable "labels" {
  type    = map(string)
  default = { project = "fsds", component = "mle" }
}
