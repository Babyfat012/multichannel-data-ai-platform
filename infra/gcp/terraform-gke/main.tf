# Bật API cho GKE + Artifact Registry; khi destroy không tắt API đang được project dùng chung.
resource "google_project_service" "required" {
  for_each           = toset(["container.googleapis.com", "artifactregistry.googleapis.com", "compute.googleapis.com"])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
