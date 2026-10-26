#!/usr/bin/env bash
# Điểm vào tương thích local; script chung chọn cấu hình theo PLATFORM_STORAGE/KCTX/REGISTRY.
set -euo pipefail
exec bash "$(dirname "$0")/deploy_feature_api.sh" "$@"
