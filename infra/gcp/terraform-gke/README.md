<!-- Mục đích: điểm vào Terraform cụm GKE phần MLE (tách state khỏi module VM DE). -->
# Terraform GKE cho MLE

Thư mục riêng, state riêng với [`../terraform`](../terraform/README.md) (VM DE): hai phần chạy
**lần lượt** dưới trần trial 8 vCPU, apply/destroy độc lập.

- Cụm **zonal** `fsds-mle` (miễn phí quản lý), node pool `main` 0..2 × `e2-standard-4` (8 vCPU/32 GiB).
- VPC riêng, VPC-native; SA node tối thiểu; Workload Identity; Artifact Registry giữ 5 version mới nhất.
- Hết buổi: chạy lệnh output `scale_to_zero_command` (node về 0, chỉ còn đĩa/registry tính tiền).

Chỉ người học chạy `plan/apply/destroy`. Trợ lý chỉ `init -backend=false` + `validate` + `fmt`.

## Sau `apply`

```bash
$(terraform -chdir=infra/gcp/terraform-gke output -raw get_credentials_command)
gcloud auth configure-docker asia-southeast1-docker.pkg.dev
REGISTRY_URL=$(terraform -chdir=infra/gcp/terraform-gke output -raw registry_url) \
  scripts/run/push_images_gke.sh fsds-feature-api:v5
```

Manifest trong `infra/k8s/` đang trỏ image local (`kind load`); trên GKE đổi tiền tố image sang
`registry_url`. Hết buổi: `$(terraform -chdir=infra/gcp/terraform-gke output -raw scale_to_zero_command)`.
