#!/usr/bin/env bash
# Đẩy các image MLE đã build local lên Artifact Registry (output `registry_url` của
# infra/gcp/terraform-gke). Ở kind dùng `kind load`; trên GKE pod kéo image từ registry này.
#   REGISTRY_URL=$(terraform -chdir=infra/gcp/terraform-gke output -raw registry_url) scripts/run/push_images_gke.sh
# Cần đã `gcloud auth configure-docker <region>-docker.pkg.dev` (người học tự chạy).
set -euo pipefail
: "${REGISTRY_URL:?đặt REGISTRY_URL từ output terraform}"
images=(fsds-feast:0.66.0 fsds-train:0.1.0 "fsds-return-risk:${RETURN_RISK_TAG:-v3}")
images+=("$@")  # image thêm (vd fsds-feature-api:v5) truyền qua tham số
for image in "${images[@]}"; do
  docker image inspect "$image" >/dev/null
  docker tag "$image" "$REGISTRY_URL/$image"
  docker push "$REGISTRY_URL/$image"
done
printf 'Đã đẩy %d image lên %s\n' "${#images[@]}" "$REGISTRY_URL"
