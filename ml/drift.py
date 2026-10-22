"""ML-N2: theo dõi data drift của đầu vào model return-risk bằng Evidently.

So một lô "hiện tại" với lô tham chiếu (giai đoạn train) trên các cột đầu vào, bằng khoảng cách
Jensen-Shannon. Có cột drift ⇒ `alert` ⇒ nên kiểm lại model trước khi tin score.
Chạy: `python -m ml.drift --reference <parquet> [--sale-truth <_truth/lines.jsonl>] --out out/drift`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from generator.generate_order_lifecycle import POSITIVE_SCENARIOS

COLUMNS = ["seller_sku", "label"]


def summarize(snapshot: dict) -> dict:
    """Rút gọn kết quả Evidently: điểm từng cột, cột drift, cờ cảnh báo.

    Chiều so phụ thuộc phương pháp ghi trong tên metric: p-value drift khi NHỎ hơn ngưỡng,
    khoảng cách (Jensen-Shannon...) drift khi LỚN hơn hoặc bằng ngưỡng.
    """
    columns = {}
    for metric in snapshot["metrics"]:
        name = metric.get("metric_name", "")
        if not name.startswith("ValueDrift("):
            continue
        fields = dict(part.split("=", 1) for part in name[len("ValueDrift(") : -1].split(","))
        score, threshold = float(metric["value"]), float(fields["threshold"])
        drift = score < threshold if "p_value" in fields["method"] else score >= threshold
        columns[fields["column"]] = {
            "method": fields["method"],
            "score": score,
            "threshold": threshold,
            "drift": drift,
        }
    drifted = sorted(c for c, v in columns.items() if v["drift"])
    return {
        "columns": columns,
        "drifted": drifted,
        "alert": bool(drifted),
    }


def check(reference: pd.DataFrame, current: pd.DataFrame, html: Path) -> dict:
    """Chạy DataDriftPreset trên COLUMNS (đều là categorical), lưu HTML, trả bản tóm tắt."""
    from evidently import DataDefinition, Dataset, Report
    from evidently.presets import DataDriftPreset

    definition = DataDefinition(categorical_columns=COLUMNS)
    data = [
        Dataset.from_pandas(f[COLUMNS].astype(str), data_definition=definition)
        for f in (current, reference)
    ]
    # Jensen-Shannon cho mọi cột phân loại. seller_sku có ~100 giá trị: nhiễu lấy mẫu của ~1.000 dòng
    # đã cho khoảng cách ~0,14 nên ngưỡng 0,2; label 2 giá trị giữ ngưỡng mặc định 0,1.
    preset = DataDriftPreset(
        cat_method="jensenshannon", per_column_threshold={"seller_sku": 0.2, "label": 0.1}
    )
    snapshot = Report([preset]).run(*data)
    html.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(html))
    return summarize(snapshot.dict())


def sale_batch(truth_path: str) -> pd.DataFrame:
    """Lô 'đợt Sale' từ file truth của generator: SKU + nhãn trả/hoàn trong 30 ngày."""
    rows = [json.loads(line) for line in Path(truth_path).read_text().splitlines()]
    return pd.DataFrame(
        {
            "seller_sku": [r["sku"] for r in rows],
            "label": [int(r["scenario"] in POSITIVE_SCENARIOS) for r in rows],
        }
    )


def main(argv=None):
    """Hai phép so: tham chiếu vs giai đoạn sau (ổn định) và vs đợt Sale (nếu có)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--reference", required=True, help="Parquet training_lines (local hoặc s3://)"
    )
    parser.add_argument("--reference-days", type=int, default=30)
    parser.add_argument("--sale-truth")
    parser.add_argument("--out", type=Path, default=Path("out/drift"))
    args = parser.parse_args(argv)
    frame = pd.read_parquet(args.reference)
    day = pd.to_datetime(frame["event_timestamp"], utc=True).dt.normalize()
    boundary = day.min() + pd.Timedelta(days=args.reference_days)
    reference, later = frame[day < boundary], frame[day >= boundary]
    results = {"stable": check(reference, later, args.out / "stable.html")}
    if args.sale_truth:
        results["sale_event"] = check(
            reference, sale_batch(args.sale_truth), args.out / "sale_event.html"
        )
    (args.out / "summary.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
