# Khai báo Terraform và ghim provider (cùng bản với module DE) cho cụm GKE phần MLE.
terraform {
  required_version = ">= 1.6.0, < 2.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.49.0"
    }
  }
  # Không khai báo backend: state nằm local cho tới khi người học chủ động chuyển.
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
