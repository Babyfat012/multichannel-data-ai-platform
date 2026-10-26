#!/usr/bin/env bash
# Dùng chung cho kind/ECR/Artifact Registry (đăng nhập registry ngoài trước).
# REGISTRY=localhost:5001 TAG=<sha> scripts/run/push_images.sh platform-feature-api:<source-tag>
# Có TAG: phát hành source image dưới TAG đó; không có TAG: giữ tag nguồn bất biến.
set -euo pipefail
REGISTRY=${REGISTRY:-localhost:5001}
[[ $# -gt 0 ]] || { echo 'Cần ít nhất một image:tag' >&2; exit 1; }
for image in "$@"; do
  source_tag=${image##*:}
  [[ "$source_tag" != "$image" && "$source_tag" != */* ]] || { echo "Thiếu tag: $image" >&2; exit 1; }
  tag=${TAG:-$source_tag}
  [[ "$tag" != latest ]] || { echo 'Không phát hành tag latest' >&2; exit 1; }
  name=${image%:*}
  name=${name##*/}
  target="${REGISTRY%/}/$name:$tag"
  docker image inspect "$image" >/dev/null
  docker tag "$image" "$target"
  docker push "$target"
done
