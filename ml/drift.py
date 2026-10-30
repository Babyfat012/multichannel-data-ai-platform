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
    # Import ở đây: Job trên EKS (pipeline A) không mang mã generator, chỉ cần khi có lô Sale của B.
    from generator.generate_order_lifecycle import POSITIVE_SCENARIOS

    rows = [json.loads(line) for line in Path(truth_path).read_text().splitlines()]
    return pd.DataFrame(
        {
            "seller_sku": [r["sku"] for r in rows],
            "label": [int(r["scenario"] in POSITIVE_SCENARIOS) for r in rows],
        }
    )


def comparisons(frame: pd.DataFrame, reference_days: int, shift_channel: str | None = None) -> dict:
    """Cặp (tham chiếu, hiện tại): `stable` = N ngày đầu vs phần sau; `channel_shift` = N ngày đầu vs
    phần sau chỉ của một kênh (cơ cấu đơn dồn về một sàn) — kịch bản drift dựng được trên pipeline A."""
    day = pd.to_datetime(frame["event_timestamp"], utc=True).dt.normalize()
    boundary = day.min() + pd.Timedelta(days=reference_days)
    reference, later = frame[day < boundary], frame[day >= boundary]
    pairs = {"stable": (reference, later)}
    if shift_channel:
        current = later[later["channel"] == shift_channel]
        if current.empty:
            raise ValueError(f"không có dòng nào của kênh {shift_channel!r} sau mốc tham chiếu")
        pairs["channel_shift"] = (reference, current)
    return pairs


def main(argv=None):
    """Các phép so: ổn định, lệch theo kênh (nếu có), đợt Sale của pipeline B (nếu có)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--reference", required=True, help="Parquet training_lines (local hoặc s3://)"
    )
    parser.add_argument("--reference-days", type=int, default=30)
    parser.add_argument("--shift-channel", help="vd shopee: lô hiện tại chỉ gồm một kênh")
    parser.add_argument("--sale-truth")
    parser.add_argument("--out", type=Path, default=Path("out/drift"))
    args = parser.parse_args(argv)
    frame = pd.read_parquet(args.reference)
    pairs = comparisons(frame, args.reference_days, args.shift_channel)
    results = {
        name: {
            "reference_rows": len(ref),
            "current_rows": len(cur),
            **check(ref, cur, args.out / f"{name}.html"),
        }
        for name, (ref, cur) in pairs.items()
    }
    if args.sale_truth:
        results["sale_event"] = check(
            pairs["stable"][0], sale_batch(args.sale_truth), args.out / "sale_event.html"
        )
    (args.out / "summary.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
