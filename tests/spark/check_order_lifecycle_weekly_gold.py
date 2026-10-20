"""Kiểm Gold PIT dương từ Order scale thật; planner chạy Spark local offline, không catalog."""
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
os.environ.setdefault('SPARK_LOCAL_HOSTNAME', 'localhost')
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ['FSDS_CONTRACTS_DIR'] = str(ROOT / 'contracts')

from pyspark.sql import SparkSession  # noqa: E402
from generator.generate_order_lifecycle import ScaleConfig, generate_scaled  # noqa: E402
from spark_jobs.lib.order_lifecycle_lakehouse import bronze_from_root, build_silver  # noqa: E402
from spark_jobs.lib.order_lifecycle_gold import build_gold  # noqa: E402


class WeeklyGoldChecks(unittest.TestCase):
    """Một ca nhỏ 60 ngày, cùng cấu hình với phép đo Python thuần."""

    def test_generated_orders_have_positive_pit_history(self):
        """Coverage tuần phải tạo count lịch sử trên target do chính generator phát."""
        tmp = Path(tempfile.mkdtemp(prefix='weekly-gold-'))
        spark = (SparkSession.builder.master('local[1]').appName('weekly-gold-check')
                 .config('spark.sql.shuffle.partitions', '1')
                 .config('spark.sql.session.timeZone', 'UTC')
                 .config('spark.sql.warehouse.dir', str(tmp / 'warehouse'))
                 .config('spark.driver.bindAddress', '127.0.0.1').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        try:
            root = tmp / 'source'
            generate_scaled(root, ScaleConfig(days=60, orders_per_day=4, sku_count=3))
            bronze = bronze_from_root(spark, str(root), 'weekly').cache()
            silver = build_silver(bronze=bronze, contracts_dir=str(ROOT / 'contracts'))
            for name in ('line_lifecycle', 'native_order_lines', 'observed_evidence', 'coverage_validity'):
                silver[name] = silver[name].cache()
                silver[name].count()
            gold = build_gold(silver=silver, cutoff='2027-01-01T00:00:00Z', run_id='weekly')
            count = gold['feat_sku_return_rate_90d'].filter('labeled_count_90d > 0').count()
            print('weekly Gold generated targets labeled_count_90d > 0 =', count)
            self.assertEqual(count, 131)
        finally:
            spark.stop()
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
