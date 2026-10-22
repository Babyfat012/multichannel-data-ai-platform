# Một VM Debian 12 với disk dữ liệu riêng, không cài runtime bằng startup script.
resource "google_compute_disk" "data" {
  project = var.project_id
  name    = "${var.name_prefix}-de-data"
  zone    = var.zone
  type    = "pd-balanced"
  size    = var.data_disk_gb
  labels  = var.labels

  lifecycle {
    precondition {
      condition     = startswith(var.zone, "${var.region}-")
      error_message = "Zone phải thuộc region đã chọn."
    }
  }
  depends_on = [google_project_service.required]
}

resource "google_compute_instance" "de" {
  project                   = var.project_id
  name                      = "${var.name_prefix}-de"
  zone                      = var.zone
  machine_type              = var.machine_type
  allow_stopping_for_update = true
  labels                    = var.labels

  boot_disk {
    auto_delete = true
    initialize_params {
      image = "debian-cloud/debian-12"
      type  = "pd-balanced"
      size  = var.boot_disk_gb
    }
  }
  # auto_delete không phải thuộc tính của attached_disk trong schema Google provider.
  # google_compute_disk là resource riêng, gắn qua attached_disk thì không tự xóa theo VM.
  attached_disk {
    source      = google_compute_disk.data.id
    device_name = "fsds-data"
    mode        = "READ_WRITE"
  }
  network_interface {
    subnetwork = google_compute_subnetwork.de.id
    # Khối rỗng cấp IPv4 public ephemeral; không tạo static address hoặc Cloud NAT.
    access_config {}
  }
  service_account {
    email  = google_service_account.vm.email
    scopes = ["cloud-platform"]
  }
  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }
  metadata = {
    enable-oslogin         = "TRUE"
    block-project-ssh-keys = "TRUE"
    serial-port-enable     = "FALSE"
  }

  depends_on = [
    google_project_service.required,
    google_storage_bucket_iam_member.objects,
    google_storage_bucket_iam_member.bucket_reader,
    google_project_iam_member.telemetry,
    google_compute_firewall.iap_ssh
  ]
}
