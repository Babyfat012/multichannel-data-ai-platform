# VPC riêng: chỉ IAP được SSH vào VM; không mở cổng UI hoặc SSH toàn Internet.
resource "google_compute_network" "de" {
  project                 = var.project_id
  name                    = "${var.name_prefix}-de-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "de" {
  project       = var.project_id
  name          = "${var.name_prefix}-de-subnet"
  region        = var.region
  network       = google_compute_network.de.id
  ip_cidr_range = "10.42.0.0/24"
}

resource "google_compute_firewall" "iap_ssh" {
  project                 = var.project_id
  name                    = "${var.name_prefix}-iap-ssh"
  network                 = google_compute_network.de.name
  direction               = "INGRESS"
  source_ranges           = ["35.235.240.0/20"]
  target_service_accounts = [google_service_account.vm.email]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
