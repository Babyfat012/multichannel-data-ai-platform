"""Các job Spark của lakehouse FSDS, chạy bằng `spark-submit` trong container Spark.

Hiện có:
- `01_bronze_baseline.py`: nạp export file của Amazon vào Iceberg `bronze.raw_amazon_orders`.
- `01b_bronze_walmart.py`: nạp bảng PostgreSQL `walmart.orders` vào `bronze.raw_walmart_orders`.
- `day_selection.py`: chọn N ngày rải đều để chạy mẫu, cố ý không import pyspark.

Thư mục này không phải thư viện để import. Tên file bắt đầu bằng số nên không import
được như module, và `spark-submit` đưa thư mục của file script vào `sys.path`.
"""
