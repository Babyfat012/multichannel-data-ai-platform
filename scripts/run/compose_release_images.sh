#!/usr/bin/env bash
# Sinh override Compose: các service deploy kéo đúng TAG đã phát hành trong stage Push.
set -euo pipefail
: "${TAG:?đặt TAG bất biến}"
[[ "$TAG" != latest ]] || exit 1
REGISTRY=${REGISTRY:-localhost:5001}
printf 'services:\n'
for mapping in "$@"; do
  service=${mapping%%=*}
  image=${mapping#*=}
  [[ "$service" =~ ^[a-z0-9-]+$ && "$image" =~ ^[a-z0-9-]+$ ]] || exit 1
  printf '  %s:\n    image: "%s/%s:%s"\n' "$service" "${REGISTRY%/}" "$image" "$TAG"
done
