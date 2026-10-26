#!/usr/bin/env bash
# Cấu hình registry cho cụm HIỆN CÓ, không tạo/xoá node. Node thiếu config_path: bật một lần và restart
# containerd trong node (container đang chạy giữ nguyên nhờ shim, kubelet tự nối lại).
# Theo https://kind.sigs.k8s.io/docs/user/local-registry/ (containerd hosts.toml).
set -euo pipefail
cd "$(dirname "$0")/../.."
cluster=${KIND_CLUSTER:-platform}
nodes=$(kind get nodes --name "$cluster")
[[ -n "$nodes" ]] || { echo "Chưa có cụm $cluster; giữ nguyên hạ tầng." >&2; exit 1; }
for node in $nodes; do
  if ! docker exec "$node" containerd config dump | grep -E "config_path = [\"']/etc/containerd/certs.d" >/dev/null; then
    echo "$node: bật config_path=/etc/containerd/certs.d rồi restart containerd"
    docker exec -i "$node" sh -c 'cat >> /etc/containerd/config.toml' <<'TOML'

[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
TOML
    docker exec "$node" systemctl restart containerd
    until docker exec "$node" crictl info >/dev/null 2>&1; do sleep 2; done
  fi
done
docker compose --profile ml up -d --wait registry
for node in $nodes; do
  docker exec "$node" mkdir -p /etc/containerd/certs.d/localhost:5001
  docker exec -i "$node" cp /dev/stdin /etc/containerd/certs.d/localhost:5001/hosts.toml <<'HOSTS'
server = "http://registry:5000"
[host."http://registry:5000"]
  capabilities = ["pull", "resolve"]
HOSTS
done
kubectl --context "kind-$cluster" apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:5001"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
YAML
printf 'Registry localhost:5001 → registry:5000, đã cấu hình %s\n' "$cluster"
