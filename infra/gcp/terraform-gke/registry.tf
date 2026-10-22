# Kho image cho model-inside-image (KServe), Feature API, agent; dọn version cũ để không tốn GB.
resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = "${var.name_prefix}-mle"
  format        = "DOCKER"
  labels        = var.labels
  cleanup_policies {
    id     = "keep-latest-5"
    action = "KEEP"
    most_recent_versions {
      keep_count = 5
    }
  }
  depends_on = [google_project_service.required]
}
