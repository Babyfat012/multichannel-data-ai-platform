---
status: accepted
---

# Local để học và kiểm thử, Cloud là đích triển khai

Ngày 2026-10-17, người dùng làm rõ mọi phần chạy local phục vụ hiểu tường tận,
phát triển và kiểm thử; toàn bộ hệ thống hướng tới triển khai Cloud. Không
coi mở rộng DE/Feast trên Cloud là một demo rời khỏi stack local. Chốt GCE
cho giai đoạn DE/Feast, Kubernetes ở giai đoạn MLE/agent; giới hạn chi ngoài
credit là 0 USD. Kiểm tích hợp bằng dữ liệu nhỏ trước, sau đó nghiệm thu
Cloud với nguồn 100 GB và đủ năm Channel khi dự toán phù hợp ngân sách.

Hệ quả: thiết kế cần cấu hình môi trường local/cloud, cách chuyển dữ liệu và
metadata, lưu trữ bền vững, bật/tắt và tái tạo được. Chưa chốt GCS/MinIO,
region, retention hoặc cấu hình máy. Chưa cấp phát tài nguyên Cloud.
