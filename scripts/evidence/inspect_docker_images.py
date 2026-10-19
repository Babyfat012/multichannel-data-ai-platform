"""Liệt kê image/service/profile thật sự nộp (local và overlay GCP) từ `docker compose config`.

Mỗi service: image, Dockerfile build, profile, port (kèm host_ip), giới hạn RAM/CPU; image có sẵn
trên máy thì thêm digest, dung lượng, ngày build qua `docker image inspect`. Không pull, không mạng.

    .venv/bin/python scripts/docker_inventory.py --output docs/evidence/docker-inventory-local.json
    RAW_BUCKET=x LAKEHOUSE_BUCKET=y FSDS_SPARK_PACKAGES_GCP=z .venv/bin/python scripts/docker_inventory.py \\
        -f docker-compose.yml -f docker-compose.gcp.yml --profile catalog --profile spark --profile airflow \\
        --output docs/evidence/docker-inventory-gcp.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess


def services(config: dict) -> list[dict]:
    """Rút gọn JSON của `docker compose config` thành một hàng mỗi service, sắp theo tên."""
    rows = []
    for name, service in sorted(config.get("services", {}).items()):
        build = service.get("build") or {}
        rows.append({
            "service": name,
            "image": service.get("image"),
            "dockerfile": build.get("dockerfile") if build else None,
            "profiles": service.get("profiles", []),
            "ports": [{"host_ip": p.get("host_ip", "0.0.0.0"), "published": p.get("published"), "target": p.get("target")}
                      for p in service.get("ports", [])],
            "mem_limit": service.get("mem_limit"),
            "cpus": service.get("cpus"),
        })
    return rows


def inspect_image(image: str | None) -> dict:
    """Digest/dung lượng của image đã có trên máy; không có thì ghi present=false thay vì lỗi."""
    if not image:
        return {"present": False}
    result = subprocess.run(["docker", "image", "inspect", image, "--format", "{{json .}}"],
                            capture_output=True, text=True)
    if result.returncode != 0:
        return {"present": False}
    data = json.loads(result.stdout)
    return {"present": True, "id": data["Id"], "size_bytes": data["Size"], "created": data["Created"]}


def main() -> int:
    """Gọi compose config với file/profile cho trước, ghi inventory JSON."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-f", "--file", action="append", default=[], help="File compose (lặp lại được)")
    parser.add_argument("--profile", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    command = ["docker", "compose"]
    for path in args.file or ["docker-compose.yml"]:
        command += ["-f", path]
    for profile in args.profile:
        command += ["--profile", profile]
    config = json.loads(subprocess.run(command + ["config", "--format", "json"], check=True,
                                       capture_output=True, text=True).stdout)
    rows = services(config)
    for row in rows:
        row["image_inspect"] = inspect_image(row["image"])
    document = {"compose_files": args.file or ["docker-compose.yml"], "profiles": args.profile, "services": rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n")
    print(f"{len(rows)} service → {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
