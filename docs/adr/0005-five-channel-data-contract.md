---
status: accepted
---

# Warehouse chung và nguồn năm Channel nhất quán

Ngày 2026-10-17, người dùng chốt một warehouse chung gồm fact Order Line
chi tiết của năm Channel, dimension SKU/Storefront và các bảng tổng hợp
ngày/SKU/Storefront. Nguồn mở rộng là một run mô phỏng chung 180 ngày,
catalog/config/cutoff nhất quán, cân scale đạt mốc 100 GB; không dùng độ phủ
khác nhau của bộ proof cũ để tuyên bố lịch sử đồng nhất năm Channel.

Cập nhật Q49: riêng độ dài lịch sử nguồn 180 ngày ở đoạn trên được thay
bằng mục tiêu ~365 ngày cho training, cửa sổ phục vụ vẫn 180 ngày; xem
[ADR-0008](0008-longer-history-for-training.md). Quyết định warehouse chung,
catalog/config/cutoff nhất quán và tiền tệ gốc vẫn giữ hiệu lực.

Tiền giữ theo currency gốc trong phiên bản đầu, không xuất tổng USD xuyên
tiền tệ khi chưa có nguồn FX. Trade-off là chưa có một chỉ số tiền duy nhất
cho toàn Seller, đổi lại không tạo quy đổi giả từ `price_factor`. Các số
đơn/units và tỷ lệ có thể đối chiếu giữa Storefront khi cohort và mẫu số
được định nghĩa thống nhất. Công thức metric, thời gian và hợp đồng Feast
chưa được chốt trong ADR này.
