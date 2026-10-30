-- Trino (EC2, catalog lakehouse): Return đã duyệt (label = 1) của pipeline A trong @@DAYS@@ ngày trước cutoff,
-- kèm seller_sku từ Gold, theo thứ tự thời điểm biết Return. Dòng JSON đưa vào feature_repo/replay_return_events.py.
-- Phát LẠI từ sổ đáp án: stream của A (thông báo đơn) không có sự kiện Return.
SELECT f.seller_sku, to_iso8601(l.label_available_at) AS label_available_at, l.order_line_key
FROM lakehouse.gold.fact_return_labels_dp3 AS l
JOIN lakehouse.gold.fact_order_lines_dp2 AS f ON f.order_line_key = l.order_line_key
WHERE l.label = 1
  AND l.label_available_at >= TIMESTAMP '@@CUTOFF@@' - INTERVAL '@@DAYS@@' DAY
  AND l.label_available_at < TIMESTAMP '@@CUTOFF@@'
ORDER BY l.label_available_at;
