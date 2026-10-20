---
status: accepted
---

# Lịch sử dài hơn cho training, cửa sổ phục vụ giữ 180 ngày

Q49 mở lại D11 (nguồn chung 180 ngày) một cách có chủ đích. Với 180 ngày, sau
khi trừ warmup 90 ngày, thời gian giao hàng và 30 ngày chờ nhãn chín, chỉ còn
khoảng 50–57 ngày Order Line có nhãn dùng được để train; tập này quá mỏng cho
một model có ý nghĩa. Chọn: **sinh lịch sử dài hơn cho training (mục tiêu
~365 ngày)** nhưng **giữ cửa sổ phục vụ/insight ở 180 ngày**, và gắn
**`dataset_version`** cho mỗi lần sinh để proof/model/version trỏ về đúng một
run tái lập được. Nếu ngân sách credit không cho phép, lùi về tập train nhỏ
với temporal split và nêu giới hạn tường minh, không giả vờ đủ dữ liệu (D25).

Ngoài code hiện tại, không có gì cho thấy hợp đồng "180 ngày" là bất biến; lý
do đổi là ràng buộc nghiệp vụ về số nhãn chín, không phải sở thích kỹ thuật.
