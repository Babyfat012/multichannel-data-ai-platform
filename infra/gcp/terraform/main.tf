# Bật các API cần thiết; khi destroy không tắt API đang được project dùng chung.
locals {
  required_services = toset(concat([
    "compute.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iap.googleapis.com"
  ], var.enable_budget ? ["billingbudgets.googleapis.com"] : []))
}

resource "google_project_service" "required" {
  for_each           = local.required_services
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
