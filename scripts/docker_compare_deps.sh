#!/usr/bin/env bash
# Build image generator naive và tối ưu từ cùng working tree, so bộ thư viện cài trong hai image,
# in dung lượng + digest. Exit khác 0 nếu bộ thư viện khác nhau (claim "chỉ khác cách build" sai).
# Dùng: bash scripts/docker_compare_deps.sh [thư mục ghi kết quả]
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
out=${1:-out/docker-compare}
mkdir -p "$out"
docker build -q -t fsds-generator:naive -f docker/Dockerfile.naive . >/dev/null
docker build -q -t fsds-generator:opt -f Dockerfile . >/dev/null
# Image tối ưu không có pip/uv ở stage cuối; đọc metadata dist-info bằng importlib.metadata của
# chính Python trong venv. Bỏ project và công cụ cài đặt (pip/setuptools/wheel) vì chúng thuộc base image.
freeze='import importlib.metadata as m
skip = {"fsds-generator", "pip", "setuptools", "wheel"}
names = sorted({(d.metadata["Name"].lower().replace("_", "-"), d.version) for d in m.distributions()})
print("\n".join(f"{n}=={v}" for n, v in names if n not in skip))'
docker run --rm --entrypoint python fsds-generator:naive -c "$freeze" >"$out/naive.txt"
docker run --rm --entrypoint python fsds-generator:opt -c "$freeze" >"$out/opt.txt"
for tag in naive opt; do
  docker image inspect "fsds-generator:$tag" --format "$tag {{.Id}} {{.Size}}"
done | tee "$out/images.txt"
if diff -u "$out/naive.txt" "$out/opt.txt" >"$out/deps.diff"; then
  echo "Cùng bộ thư viện: $(wc -l <"$out/opt.txt") package"
else
  echo "KHÁC bộ thư viện — xem $out/deps.diff" >&2
  exit 1
fi
