# Service account riêng cho node: chỉ ghi log/metric và kéo image, không dùng SA mặc định (Editor).
resource "google_service_account" "nodes" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-mle-nodes"
  display_name = "FSDS MLE GKE nodes"
  depends_on   = [google_project_service.required]
}

resource "google_project_iam_member" "nodes" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/artifactregistry.reader",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.nodes.email}"
}
