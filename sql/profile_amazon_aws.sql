-- Đo đặc tính dữ liệu Amazon trên AWS (Trino EC2, catalog lakehouse): schema evolution, skew, cardinality, trùng.
-- Chạy: docker compose --profile query exec -T trino trino --output-format ALIGNED < sql/profile_amazon_aws.sql
-- 1. Schema evolution Amazon: khoá JSON payload trước (ngày 98) và sau (ngày 101) lần đổi API
SELECT export_date, arbitrary(array_join(array_sort(map_keys(CAST(json_parse(payload) AS map(varchar, json)))), ', ')) AS payload_keys
FROM lakehouse.bronze.raw_amazon_orders_dp1
WHERE export_date IN (DATE '2026-04-08', DATE '2026-04-12') GROUP BY export_date ORDER BY export_date;
-- 2. Hai dạng schema sau khi Silver hợp nhất
SELECT schema_form, min(export_date) AS from_date, max(export_date) AS to_date, count(*) AS order_lines
FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY schema_form ORDER BY from_date;
-- 3. Skew SKU (Zipf 0.8): phần Order Line của 1% SKU bán chạy nhất (20/2000) và 10 SKU đầu
WITH s AS (SELECT seller_sku, count(*) AS n FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY seller_sku),
r AS (SELECT seller_sku, n, row_number() OVER (ORDER BY n DESC) AS rk, sum(n) OVER () AS total FROM s)
SELECT count(*) AS skus, round(100.0 * sum(n) FILTER (WHERE rk <= 20) / max(total), 2) AS top1pct_share,
       round(100.0 * sum(n) FILTER (WHERE rk <= 200) / max(total), 2) AS top10pct_share,
       max(n) AS max_lines, min(n) AS min_lines FROM r;
-- 4. Skew theo storefront (marketplace) và cardinality (unique / tổng)
SELECT marketplace_id, count(*) AS order_lines, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY marketplace_id ORDER BY order_lines DESC;
SELECT count(*) AS order_lines,
       approx_distinct(order_id) AS order_id_unique, round(1.0 * approx_distinct(order_id) / count(*), 4) AS order_id_ratio,
       approx_distinct(postal_code) AS postal_unique, round(1.0 * approx_distinct(postal_code) / count(*), 6) AS postal_ratio,
       approx_distinct(city) AS city_unique, approx_distinct(seller_sku) AS sku_unique
FROM lakehouse.silver.stg_amazon_order_lines_dp2;
-- 5. Duplicate trước khi Silver bỏ trùng: Bronze vs Silver (Order) cho Amazon
SELECT (SELECT count(*) FROM lakehouse.bronze.raw_amazon_orders_dp1) AS bronze_rows,
       (SELECT count(DISTINCT order_id) FROM lakehouse.silver.stg_amazon_order_lines_dp2) AS silver_orders;
