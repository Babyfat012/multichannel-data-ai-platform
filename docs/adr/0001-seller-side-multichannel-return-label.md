# Góc nhìn seller đa kênh, nhãn dự đoán là Return/Refund ở cấp Order Line

Hệ thống được thiết kế như nền tảng dữ liệu của một Seller bán trên nhiều Channel (Amazon, TikTok Shop, Shopify, Walmart, Shopee), không phải của một sàn. Nhãn dự đoán là Order Line có bị Return hoặc Refund trong 30 ngày kể từ ngày giao hay không, tra cứu feature theo `sku_id` + `storefront_id`, vì các Channel giấu hoặc không cấp định danh người mua dùng chung được, nên nhãn theo `customer_id` (churn, repeat purchase) không khả thi.

Cập nhật Q56/Q57 ngày 2026-10-18: giữ target binary Order Line, nhưng cửa
sổ theo shipment allocation và qualification/maturity được thay/làm rõ ở
[ADR-0010](0010-shipment-windows-return-label-v2.md). Phải dùng label version
khi đối chiếu proof/model trước và sau thay đổi; quan điểm Seller vẫn giữ.

## Considered Options

- **Churn hoặc repeat purchase theo `customer_id`**: bị loại vì Amazon chỉ cấp email alias và hạn chế PII, TikTok Shop che danh tính người mua, Shopify có thể null với khách không đăng nhập. Chỉ Walmart trả đủ thông tin.
- **Cancellation risk theo Order**: bị loại vì nhãn đến quá sớm, không tạo được tình huống giám sát drift khi chưa có groundtruth.
- **SKU demand (hồi quy)**: khả thi nhưng ít gắn với bài toán thực tế của Seller hơn tỷ lệ trả hàng.
