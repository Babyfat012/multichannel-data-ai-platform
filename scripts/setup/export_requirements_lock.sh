#!/usr/bin/env bash
# Xuất requirements.lock từ uv.lock cho docker/Dockerfile.naive. Chạy lại mỗi khi uv.lock đổi,
# nếu không image naive sẽ cài bộ thư viện khác image tối ưu.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
uv export --frozen --no-dev --no-emit-project --format requirements-txt --output-file requirements.lock
