# Hai bucket riêng cho raw và warehouse mới; bảo vệ public và chỉ dọn dữ liệu tạm/cũ.
resource "google_storage_bucket" "data" {
  for_each = toset(["raw", "lakehouse"])

  project                     = var.project_id
  name                        = "${var.name_prefix}-${var.project_id}-${each.value}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy
  labels                      = var.labels

  # Tắt soft delete để không giữ thêm bản tính phí sau khi đã xóa; versioning vẫn bật.
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  versioning {
    enabled = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      with_state                 = "ARCHIVED"
      days_since_noncurrent_time = var.noncurrent_retention_days
    }
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = var.temp_retention_days
      matches_prefix = ["tmp/"]
      with_state     = "LIVE"
    }
  }

  depends_on = [google_project_service.required]
}
