-- Warehouse v2, migration 002: PK đã có từ 001 và đi cùng staging khi rename.
-- Q1: range created_at 30 ngày; INCLUDE cho headline theo ngày/channel/currency.
CREATE INDEX lifecycle_fact_sales ON warehouse_lifecycle.fact_order_lines (created_at) INCLUDE (channel, currency, headline_product_net_minor);
-- Q2: chọn SKU trước khi join nhãn bằng dataset/line/generation.
CREATE INDEX lifecycle_fact_sku ON warehouse_lifecycle.fact_order_lines (seller_sku, dataset_version, line_id) INCLUDE (generation_id);
-- Q2: chỉ nhãn labeled ở đúng cutoff; PK bảo vệ grain cả các state còn lại.
CREATE INDEX lifecycle_label_cutoff ON warehouse_lifecycle.label_return (cutoff, dataset_version, line_id, generation_id) INCLUDE (label) WHERE state = 'labeled';
-- Q3: lookup PIT <= event_timestamp; giữ created để chọn revision không nhìn tương lai.
CREATE INDEX lifecycle_feature_pit ON warehouse_lifecycle.feat_sku_return_rate_90d (dataset_version, seller_sku, event_timestamp DESC, created DESC);
