# Giao diện bàn giao cố định cho K2 và các lệnh để người học tự mở SSH/UI.
output "raw_bucket" {
  description = "Tên bucket raw (không gồm gs://)."
  value       = google_storage_bucket.data["raw"].name
}

output "lakehouse_bucket" {
  description = "Tên bucket warehouse Iceberg mới (không gồm gs://)."
  value       = google_storage_bucket.data["lakehouse"].name
}

output "raw_bucket_uri" {
  value = google_storage_bucket.data["raw"].url
}

output "lakehouse_bucket_uri" {
  value = google_storage_bucket.data["lakehouse"].url
}

output "service_account_email" {
  value = google_service_account.vm.email
}

output "vm_name" {
  value = google_compute_instance.de.name
}

output "zone" {
  value = var.zone
}

output "ssh_command" {
  value = "gcloud compute ssh ${google_compute_instance.de.name} --project=${var.project_id} --zone=${var.zone} --tunnel-through-iap"
}

output "port_forward_command" {
  description = "Tunnel SSH tới loopback VM: Spark master 8080, Airflow 8081, Lakekeeper 8181 (đúng port docker-compose.gcp.yml publish)."
  value       = "gcloud compute ssh ${google_compute_instance.de.name} --project=${var.project_id} --zone=${var.zone} --tunnel-through-iap -- -N -o ExitOnForwardFailure=yes -L 127.0.0.1:8080:127.0.0.1:8080 -L 127.0.0.1:8181:127.0.0.1:8181 -L 127.0.0.1:8081:127.0.0.1:8081"
}
