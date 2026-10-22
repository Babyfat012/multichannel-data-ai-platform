# Cụm GKE zonal + một node pool tự quản (xoá pool mặc định). Autoscale 0..max_nodes để hạ về 0 máy.
resource "google_container_cluster" "mle" {
  project                  = var.project_id
  name                     = "${var.name_prefix}-mle"
  location                 = var.zone
  network                  = google_compute_network.mle.id
  subnetwork               = google_compute_subnetwork.mle.id
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
  resource_labels          = var.labels
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }
  release_channel {
    channel = "REGULAR"
  }
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

resource "google_container_node_pool" "main" {
  project  = var.project_id
  name     = "main"
  location = var.zone
  cluster  = google_container_cluster.mle.name
  autoscaling {
    min_node_count = 0
    max_node_count = var.max_nodes
  }
  initial_node_count = var.max_nodes
  node_config {
    machine_type    = var.machine_type
    disk_size_gb    = 50
    disk_type       = "pd-balanced"
    spot            = var.spot
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = var.labels
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
  management {
    auto_repair  = true
    auto_upgrade = true
  }
}
