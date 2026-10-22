# VPC riêng của cụm MLE, dải phụ cho Pod/Service (VPC-native). Tách khỏi VPC của VM DE.
resource "google_compute_network" "mle" {
  project                 = var.project_id
  name                    = "${var.name_prefix}-mle-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "mle" {
  project       = var.project_id
  name          = "${var.name_prefix}-mle-subnet"
  region        = var.region
  network       = google_compute_network.mle.id
  ip_cidr_range = "10.52.0.0/22"
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.60.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.61.0.0/20"
  }
}
