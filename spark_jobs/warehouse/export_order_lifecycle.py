"""Job 25: export Gold v2 đủ năm Channel qua staging, receipt và publish nguyên tử."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from spark_jobs.lib.order_lifecycle_warehouse import TABLES, cutoff_utc, export_frames


def main(argv=None):
    """Pin từng snapshot Iceberg, chọn nhãn đúng run/cutoff và export qua JDBC."""
    parser = argparse.ArgumentParser(description='Gold v2 → PostgreSQL warehouse_lifecycle')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--cutoff', required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--tables', default=','.join(TABLES), help='Tên bảng phân cách bằng dấu phẩy')
    parser.add_argument('--sql-dir', type=Path, default=Path(os.getenv(
        'FSDS_WAREHOUSE_SQL_DIR', '/opt/sql/warehouse_lifecycle')))
    args = parser.parse_args(argv)
    names = args.tables.split(',')
    if not args.run_id.strip() or len(names) != len(set(names)) or not set(names).issubset(TABLES):
        parser.error('run-id hoặc tables không hợp lệ')
    cutoff = cutoff_utc(args.cutoff)
    for filename in ('001_tables.sql', '002_indexes.sql'):
        if not (args.sql_dir / filename).is_file():
            parser.error(f'Thiếu migration {args.sql_dir / filename}')
    from pyspark.sql import functions as F
    from spark_jobs.lib.iceberg_session import CATALOG, build_spark
    spark = build_spark('export-gold-order-lifecycle-warehouse', shuffle_partitions=4)
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    try:
        frames = {}
        for name in names:
            source = f'{CATALOG}.gold.lifecycle_{name}'
            snapshot = spark.sql(f'SELECT snapshot_id FROM {source}.history '
                                 'ORDER BY made_current_at DESC LIMIT 1').first()[0]
            frame = spark.read.option('snapshot-id', str(snapshot)).table(source)
            if name == 'label_return':
                frame = frame.filter((F.col('generation_id') == args.run_id) & (F.col('cutoff') == F.lit(cutoff)))
            frames[name] = frame
        export_frames(spark, frames, args.run_id, args.cutoff, args.receipt, args.sql_dir)
        return 0
    finally:
        spark.stop()


if __name__ == '__main__':
    raise SystemExit(main())
