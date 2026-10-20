---
status: accepted
---

# Mở rộng nguồn theo dòng/số lượng và ưu tiên kiểm tra cho Seller

Ngày 2026-10-18, người dùng xác nhận Q52–Q55: hỗ trợ partial fulfillment,
Return/Refund lifecycle độc lập theo line/số lượng; Listing/catalog mapping,
trạng thái và thuộc tính quan sát có lịch sử; lát inventory nhỏ theo
pool/location với available/reservation/inbound; và hàng đợi SKU read-only
kèm phân rã biến động/evidence drilldown. Chọn mở rộng nền nguồn trước sinh
lại dữ liệu lớn vì một trạng thái/ngày giao cấp Order hoặc catalog tĩnh
không biểu diễn được các câu hỏi này mà không sửa nhiều tầng về sau.

Q54 đưa lát inventory này từ roadmap Q31 vào phạm vi kế tiếp; tối ưu đặt
hàng, forecast model thứ hai, finance/ads/profit, bundle/kit và saved
investigation vẫn chưa được chọn. Nguồn core đủ năm Channel, độ phủ của các
feed bổ sung phải khai báo theo capability, không giả mọi Channel cung cấp
giống nhau. Bảo toàn namespace, native currency, advisory và quyền Cloud
thủ công/ngân sách đã chốt.

Q52 chưa quyết định lại nhãn Q41: mốc giao khi split delivery và sự kiện
Return/Refund nào kích hoạt nhãn phải được chốt tường minh. Grain dự đoán
vẫn là Order Line; việc thêm shipment/return/refund không được nhân bản fact
sales hoặc biến score xếp hạng thành xác suất. Xem
[impact assessment và quyết định phụ thuộc](../expanded-scope-design.md).

Cập nhật cùng ngày: người dùng đã xác nhận Q56–Q59; phần nhãn và các policy
tiếp theo ở [ADR-0010](0010-shipment-windows-return-label-v2.md). Đoạn trên
ghi trạng thái lúc chốt Q52, không còn là câu hỏi policy đang mở.
