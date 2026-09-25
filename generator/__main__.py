"""Điểm chạy dòng lệnh của generator.

Ví dụ:
    uv run python -m generator batch --config configs/small.yaml
    uv run python -m generator batch --config configs/default.yaml --output s3://fsds
    uv run python -m generator load-postgres --source s3://fsds --dsn postgresql://fsds:fsds@localhost:5432/sources
    uv run python -m generator report --source s3://fsds --dsn postgresql://fsds:fsds@localhost:5432/sources
    uv run python -m generator stream --config configs/small.yaml --sink file --speed 0
    uv run python -m generator stream --config configs/default.yaml --sink kafka --bootstrap localhost:9092

Với `s3://`, thông tin đăng nhập MinIO lấy từ biến môi trường `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` và `FSSPEC_S3_ENDPOINT_URL` (ví dụ `http://localhost:9000`).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import time


def main() -> None:
    """Đọc tham số dòng lệnh và chạy chế độ được chọn."""
    parser = argparse.ArgumentParser(prog="generator", description="Sinh dữ liệu giả lập đa kênh")
    sub = parser.add_subparsers(dest="mode", required=True)

    batch = sub.add_parser("batch", help="Sinh dữ liệu Batch (lịch sử tới cutoff)")
    batch.add_argument("--config", required=True, help="Đường dẫn file cấu hình YAML")
    batch.add_argument("--output", help="Ghi đè storage.output_root trong cấu hình")

    load = sub.add_parser("load-postgres", help="Nạp pg-load/ của một lần chạy vào PostgreSQL")
    load.add_argument("--source", required=True, help="Gốc đầu ra của lần chạy (thư mục hoặc s3://...)")
    load.add_argument("--dsn", required=True, help="Chuỗi kết nối PostgreSQL")

    report = sub.add_parser("report", help="In báo cáo chất lượng dữ liệu của một lần chạy")
    report.add_argument("--source", required=True, help="Gốc đầu ra của lần chạy (thư mục hoặc s3://...)")
    report.add_argument("--dsn", help="Đọc Walmart và SKU từ PostgreSQL thay vì từ pg-load/")
    report.add_argument("--markdown", action="store_true", help="In dạng Markdown để dán vào docs/")

    stream = sub.add_parser("stream", help="Phát Stream sau cutoff vào Kafka hoặc ra file")
    stream.add_argument("--config", required=True, help="Đường dẫn file cấu hình YAML")
    stream.add_argument("--output", help="Ghi đè storage.output_root (nơi ghi stream_manifest.json)")
    stream.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    stream.add_argument("--bootstrap", default="localhost:9092", help="Địa chỉ Kafka khi --sink kafka")
    stream.add_argument("--speed", type=float, default=1.0,
                        help="1 là đúng nhịp cấu hình, 60 là nhanh gấp 60 lần, 0 là không chờ")

    args = parser.parse_args()
    started = time.perf_counter()
    if args.mode == "batch":
        _run_batch(args)
    elif args.mode == "stream":
        _run_stream(args)
    elif args.mode == "load-postgres":
        from generator.load_postgres import load_postgres

        print(json.dumps(load_postgres(args.source, args.dsn), indent=2))
    else:
        from generator.report import QualityReport, render_markdown, render_text

        render = render_markdown if args.markdown else render_text
        print("\n\n".join(render(t) for t in QualityReport(args.source, args.dsn).all_tables()))
    print(f"Xong sau {time.perf_counter() - started:.1f} giây")


def _run_batch(args: argparse.Namespace) -> None:
    """Sinh Batch theo cấu hình, in tóm tắt manifest."""
    from generator.batch import BatchRun
    from generator.config import load_config

    config = load_config(args.config)
    if args.output:
        config = dataclasses.replace(config, storage=dataclasses.replace(config.storage, output_root=args.output))
    manifest = BatchRun(config).run()
    print(json.dumps({k: manifest[k] for k in ("datasets", "labels")}, indent=2, ensure_ascii=False))
    print(f"Ghi tại {config.storage.output_root}")


def _run_stream(args: argparse.Namespace) -> None:
    """Phát Stream theo cấu hình, in số đo."""
    from generator.config import load_config
    from generator.storage import OutputStore
    from generator.stream import FileSink, KafkaSink, run_stream

    config = load_config(args.config)
    if args.output:
        config = dataclasses.replace(config, storage=dataclasses.replace(config.storage, output_root=args.output))
    sink = (FileSink(OutputStore(config.storage.output_root)) if args.sink == "file"
            else KafkaSink(args.bootstrap))
    print(json.dumps(run_stream(config, sink, args.speed), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
