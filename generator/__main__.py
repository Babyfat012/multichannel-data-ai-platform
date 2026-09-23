"""Điểm chạy dòng lệnh của generator.

Ví dụ:
    uv run python -m generator batch --config configs/small.yaml
    uv run python -m generator batch --config configs/default.yaml --output s3://source-exports-bucket
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import time

from generator.batch import BatchRun
from generator.config import load_config


def main() -> None:
    """Đọc tham số dòng lệnh và chạy chế độ được chọn."""
    parser = argparse.ArgumentParser(prog="generator", description="Sinh dữ liệu giả lập đa kênh")
    sub = parser.add_subparsers(dest="mode", required=True)
    batch = sub.add_parser("batch", help="Sinh dữ liệu Batch (lịch sử tới cutoff)")
    batch.add_argument("--config", required=True, help="Đường dẫn file cấu hình YAML")
    batch.add_argument("--output", help="Ghi đè storage.output_root trong cấu hình")
    args = parser.parse_args()

    config = load_config(args.config)
    if args.output:
        config = dataclasses.replace(config, storage=dataclasses.replace(config.storage, output_root=args.output))

    started = time.perf_counter()
    manifest = BatchRun(config).run()
    print(json.dumps({k: manifest[k] for k in ("datasets", "labels")}, indent=2, ensure_ascii=False))
    print(f"Xong sau {time.perf_counter() - started:.1f} giây, ghi tại {config.storage.output_root}")


if __name__ == "__main__":
    main()
