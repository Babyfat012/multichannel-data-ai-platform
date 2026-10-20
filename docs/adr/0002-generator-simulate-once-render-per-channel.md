# Generator mô phỏng một thế giới duy nhất, sau đó render theo từng Channel và chèn lỗi ở lớp giao hàng

Generator gồm ba lớp: lõi mô phỏng sinh sự thật chung (Order, Order Line, Return, Refund theo Order Status chuẩn, kèm SKU Defect Propensity ẩn), renderer theo Channel đổi sự thật thành dạng gốc của Channel, và lớp giao hàng chèn lỗi (trùng, trễ, theo lô, burst). Lõi chạy liền một mạch qua Cutoff: phần trước Cutoff thành Batch, phần sau thành Stream, nên một Order thấy ở Batch được cập nhật tiếp ở Stream và Return Label đến muộn thật sự. Lỗi luôn chèn sau cùng nên tỷ lệ thật được ghi vào `run_manifest.json` làm đáp án đối chiếu cho Spark và Flink.

## Considered Options

- **Mỗi Channel một generator riêng, sinh thẳng dạng gốc**: bị loại vì không có sự thật chung để kiểm tra Silver, quy luật nhãn phải viết lặp 5 lần, và không biết chính xác đã chèn bao nhiêu lỗi.
- **Stream là phát lại dữ liệu Batch**: bị loại vì không có Order mới, không có Return đến muộn, không có drift sau khi mô hình đã train, nên không dựng được tình huống giám sát khi chưa có groundtruth.

## Consequences

- Cùng `seed` và cùng config phải cho cùng dữ liệu, để manifest và test có giá trị.
- Renderer thêm một lớp code so với cách sinh thẳng, đổi lại có đáp án chung.
- Drift là thay đổi tham số của lõi theo lịch (covariate: phân phối giảm giá và SKU, concept: hệ số Return), không phải sửa dữ liệu sau khi sinh.
