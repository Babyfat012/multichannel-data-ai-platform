# Thông tin bàn giao cho bước deploy (kubectl/helm, docker push).
output "cluster_name" {
  value = google_container_cluster.mle.name
}

output "get_credentials_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.mle.name} --zone ${var.zone} --project ${var.project_id}"
}

output "registry_url" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "scale_to_zero_command" {
  value = "gcloud container clusters resize ${google_container_cluster.mle.name} --node-pool main --num-nodes 0 --zone ${var.zone} --project ${var.project_id} --quiet"
}
