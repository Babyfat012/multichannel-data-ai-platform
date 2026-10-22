"""Đọc nhiều file Walmart scale; tái dùng transaction của script nạp Walmart hiện có."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[6]
sys.path.insert(0, str(ROOT))
from scripts.setup.load_order_lifecycle_walmart import Row, load  # noqa: E402
from scripts.setup.setup_order_snapshots_debezium import load_settings  # noqa: E402


def main():
    """Nạp tất cả file Walmart trong manifest scale vào cùng transaction."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((args.root / "manifest.json").read_text())
    rows = []
    for item in manifest["files"]:
        if item["feed"] != "walmart.orders":
            continue
        count = 0
        with (args.root / item["path"]).open() as handle:
            for line in handle:
                value = json.loads(line)
                rows.append(
                    Row(
                        manifest["dataset_version"],
                        value["purchase_order_id"],
                        value["fetched_at"],
                        json.dumps(
                            value["raw_payload"],
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                            allow_nan=False,
                        ),
                    )
                )
                count += 1
        if count != item["rows"]:
            raise ValueError("Số dòng Walmart khác manifest")
    if not rows:
        raise ValueError("Không có Walmart để kiểm JDBC; tăng mẫu hoặc chọn fixture")
    count = load(rows, load_settings(Path(".env")))
    if count != len(rows):
        raise ValueError("Đối soát Walmart thất bại")
    print(f"Walmart scale: {count} orders")


if __name__ == "__main__":
    main()
