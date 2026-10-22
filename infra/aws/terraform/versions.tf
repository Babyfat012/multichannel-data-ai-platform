# Khai báo Terraform và ghim provider AWS; state local cho tới khi người học chủ động chuyển.
terraform {
  required_version = ">= 1.6.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  # Tag chung cho mọi resource, thay cho labels bên GCP.
  default_tags {
    tags = var.tags
  }
}
