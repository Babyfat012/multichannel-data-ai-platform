---
status: accepted
---

# Công bố phiên bản và ghim phiên bản khi serving

Iceberg (offline), PostgreSQL (warehouse) và Redis (online) không có transaction
chung, nên một refresh hỏng giữa chừng có thể để UI đọc lẫn số của hai phiên
bản. D21 đã chốt "chỉ publish bản đã validate, lỗi thì giữ bản thành công
trước"; Q45 chốt cơ chế: mỗi lần refresh tạo một **generation** có
**manifest**, mọi đầu ra bắt buộc phải xong trước khi manifest được mở, và
**request ghim version** để đọc đúng một generation. Chọn cách này thay vì
chỉ đặt cổng chặn ở serving, vì cổng chặn không tự bảo đảm các store đọc cùng
một phiên bản.

Hệ quả: materialize tăng dần phải bắt được cả **correction cũ** (Q36), không
chỉ mốc mới; khi refresh lỗi, UI phải hiển thị độ cũ theo D21; API serving
trả kèm `data_as_of`/version (Q46). Lựa chọn khó đảo ngược nên được ghi lại.

Chưa có proof cơ chế này đã triển khai. Physical namespace của từng store,
manifest store, request pin, concurrent publish và recovery phải được POC
theo [G2 và S10–S13](../implementation-readiness.md). Retention/expiry phải
bảo vệ generation đang được request hoặc training tham chiếu. Live view có
freshness/version riêng; correction daily metrics qua generation mới.
