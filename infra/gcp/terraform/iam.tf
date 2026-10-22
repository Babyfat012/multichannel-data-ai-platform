# VM dùng metadata credentials/ADC, chỉ có quyền dữ liệu trên hai bucket, không tạo key.
resource "google_service_account" "vm" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-de-vm"
  display_name = "FSDS DE VM"
  depends_on   = [google_project_service.required]
}

resource "google_storage_bucket_iam_member" "objects" {
  for_each = google_storage_bucket.data
  bucket   = each.value.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_storage_bucket_iam_member" "bucket_reader" {
  for_each = google_storage_bucket.data
  bucket   = each.value.name
  role     = "roles/storage.legacyBucketReader"
  member   = "serviceAccount:${google_service_account.vm.email}"
}

resource "google_project_iam_member" "telemetry" {
  for_each = toset(["roles/logging.logWriter", "roles/monitoring.metricWriter"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.vm.email}"
}
