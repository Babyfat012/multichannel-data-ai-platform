---
status: accepted
---

# Triển khai GCP và bật khi cần

Ngày 2026-10-17, người dùng chọn GCP với khoản trial credit 300 USD, và chỉ
chạy hệ thống khi cần. Hướng AWS trước đó chỉ là ứng viên được đề xuất và
không còn là hướng đích. Sau câu trả lời Q6/Q8/Q9, đã chốt chi ngoài credit
là 0 USD, GCE trước cho DE/Feast, Kubernetes khi làm MLE/agent; kiểm nhỏ rồi
nghiệm thu Cloud với nguồn 100 GB và đủ năm Channel trong dự toán phù hợp.
Mức credit thực còn lại/hạn dùng, region, quota và phương án storage chưa
kiểm chứng; không coi khoản credit là 300 USD mỗi tháng hoặc quyền bật paid
upgrade. Xem thêm [nguyên tắc local/cloud](0004-local-learning-cloud-delivery.md).

Cập nhật D22/Q26: đã chọn GCS và Feast Spark/Redis theo
[0006-gcs-feast-integration.md](0006-gcs-feast-integration.md); phần còn cần
kiểm chứng là POC, sizing/region/quota/credit thực và retention/backup.
