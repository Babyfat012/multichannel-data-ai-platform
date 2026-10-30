#!/usr/bin/env bash
# docker compose cho Jenkins: local = file gốc; AWS (DATA_STORAGE=s3, Jenkins trên EC2) thêm overlay AWS (+ stream khi
# CI_STREAM=1) để KHÔNG tạo lại service đang chạy bằng cấu hình local. Có out/ci-compose-images.yaml thì kéo image phát hành.
set -euo pipefail
files=(-f docker-compose.yml)
if [[ "${DATA_STORAGE:-minio}" == s3 ]]; then
  files+=(-f docker-compose.aws.yml)
  [[ "${CI_STREAM:-0}" != 1 ]] || files+=(-f docker-compose.aws-stream.yml)
fi
[[ ! -f out/ci-compose-images.yaml ]] || files+=(-f out/ci-compose-images.yaml)
exec docker compose "${files[@]}" "$@"
