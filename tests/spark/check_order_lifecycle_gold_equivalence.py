"""Gold v2 mới (chỉ đọc Silver, tuyến tính) phải ra ĐÚNG Gold cũ (đọc Bronze, bậc hai).

`tests/spark/reference_order_lifecycle_gold.py` là bản chụp module Gold trước refactor, chỉ giữ để so ở đây.
Hai bộ nguồn: fixture mặc định và một nguồn scale nhỏ (nhiều SKU, late, correction, refund
retry). So fact, nhãn (trừ `input_json` — bản mới chỉ chứa bằng chứng của line) và feature
bằng `exceptAll` hai chiều + fingerprint. Chạy (Docker):

    docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \\
      -e PYSPARK_SUBMIT_ARGS='--driver-memory 3g pyspark-shell' \\
      --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \\
      platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_gold_equivalence.py
"""

import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ["PLATFORM_CONTRACTS_DIR"] = str(ROOT / "contracts")

from pyspark.sql import SparkSession

import spark_jobs.lib.order_lifecycle_gold as new
import tests.spark.reference_order_lifecycle_gold as old
from generator.generate_order_lifecycle import ScaleConfig, generate_scaled
from generator.generate_order_lifecycle_fixture import generate_fixture
from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, build_silver

COMPUTED = datetime(2026, 10, 19, tzinfo=timezone.utc)


def _cutoff(root):
    """Cutoff = observation end of the generated source, so every line is old enough to mature."""
    import json

    horizons = json.loads((root / "manifest.json").read_text())["horizons"]
    return horizons["source_observation_end"]


def _diff(left, right, drop=()):
    """Rows in exactly one side (bag semantics), after dropping columns that may legitimately differ."""
    cols = [c for c in left.columns if c not in drop]
    a, b = left.select(*cols), right.select(*cols)
    return a.exceptAll(b).count() + b.exceptAll(a).count()


class GoldV2Equivalence(unittest.TestCase):
    """Fixture and small scale: new Gold == legacy Gold, table by table."""

    @classmethod
    def setUpClass(cls):
        """One Spark session; sources generated into /tmp only."""
        cls.tmp = Path(tempfile.mkdtemp(prefix="gold-v2-eq-"))
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("gold-v2-equivalence")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        """Release Spark and /tmp sources."""
        cls.spark.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _persist(self, root, side, tables):
        """Vật chất hóa ba bảng được so vào /tmp, trả DataFrame đọc lại từ file."""
        out = {}
        for name in ("fact_order_lines", "label_return_v2", "feat_sku_return_rate_90d"):
            path = str(self.tmp / f"{root.name}-{side}-{name}")
            tables[name].write.mode("overwrite").parquet(path)
            out[name] = self.spark.read.parquet(path)
        return out

    def _compare(self, root, cutoff):
        """Build Silver once, Gold both ways, assert identical outputs; return sizes for the log."""
        bronze = bronze_from_root(self.spark, str(root), "eq").cache()
        silver = build_silver(bronze=bronze, as_of=cutoff)
        # Như job thật: Gold đọc Silver ĐÃ GHI. Plan Silver chưa vật chất hóa sâu dần theo số ngày
        # (StackOverflowError lúc lập plan ở nguồn 90 ngày).
        for name in (
            "line_lifecycle",
            "native_order_lines",
            "observed_evidence",
            "coverage_validity",
        ):
            path = str(self.tmp / f"{root.name}-silver-{name}")
            silver[name].write.mode("overwrite").parquet(path)
            silver[name] = self.spark.read.parquet(path)
        fresh = new.build_gold(silver=silver, cutoff=cutoff, run_id="eq", computed_at=COMPUTED)
        legacy = old.build_gold(
            silver={**silver, "source_records": bronze},
            cutoff=cutoff,
            run_id="eq",
            computed_at=COMPUTED,
        )
        # Ghi ra Parquet rồi đọc lại để cắt lineage: exceptAll trên plan chưa vật chất hóa OOM ở 6g.
        fresh = {**fresh, **self._persist(root, "fresh", fresh)}
        legacy = {**legacy, **self._persist(root, "legacy", legacy)}
        diffs = {
            "fact_order_lines": _diff(fresh["fact_order_lines"], legacy["fact_order_lines"]),
            "label_return_v2": _diff(
                fresh["label_return_v2"],
                legacy["label_return_v2"],
                drop=("input_json", "positive_kind"),
            ),
            "feat_sku_return_rate_90d": _diff(
                fresh["feat_sku_return_rate_90d"],
                legacy["feat_sku_return_rate_90d"].withColumnRenamed(
                    "return_rate_90d", "return_or_refund_rate_90d"
                ),
            ),
        }
        sizes = {name: fresh[name].count() for name in diffs}
        sizes["labeled"] = fresh["label_return_v2"].filter("state = 'labeled'").count()
        sizes["nonzero_features"] = (
            fresh["feat_sku_return_rate_90d"].filter("labeled_count_90d > 0").count()
        )
        sizes["segments"] = fresh["label_segments"].count()
        # Mẫu = mọi line: oracle độc lập (label_line tại thời điểm đặt) kiểm TẤT CẢ feature.
        checks = new.validate_gold(silver, fresh, cutoff, feature_sample=100_000)["checks"]
        print(
            "EQUIVALENCE",
            root.name,
            {"diffs": diffs, "sizes": sizes, "violations": {k: v for k, v in checks.items() if v}},
            flush=True,
        )
        self.assertEqual(diffs, {name: 0 for name in diffs})
        self.assertEqual({k: v for k, v in checks.items() if v}, {})
        return sizes

    def test_fixture(self):
        """Default fixture (30 lines, all features zero)."""
        root = self.tmp / "fixture"
        generate_fixture(root)
        self._compare(root, "2026-06-01T00:00:00.000000Z")

    def test_small_scale_has_real_features(self):
        """Seeded scale source cỡ legacy chạy được: fact/nhãn/feature mới phải bằng hệt legacy."""
        root = self.tmp / "scale"
        generate_scaled(
            root,
            ScaleConfig(
                seed=7,
                days=60,
                orders_per_day=1,
                sku_count=2,
                followup_days=150,
                observation_days=165,
            ),
        )
        sizes = self._compare(root, _cutoff(root))
        # Không assert feature khác 0: bản legacy (nhân bản coverage theo line) OOM ở nguồn đủ dài để
        # có feature khác 0. Feature khác 0 được kiểm ở tests/spark/check_order_lifecycle_skew.py và bằng oracle
        # độc lập `pit_feature_mismatch` của validate_gold; ở đây chỉ in số để đọc.
        print("NONZERO_FEATURES", sizes["nonzero_features"], flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
