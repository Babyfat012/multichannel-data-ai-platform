# Chỉ mục quyết định kiến trúc

Repo có các ADR lịch sử trùng số 0001–0003. Dùng **tên file đầy đủ** khi
dẫn chiếu; giữ đường dẫn cũ để không làm hỏng liên kết. ADR mới dùng số lớn
hơn số lớn nhất hiện có (hiện là 0013).

| File | Nội dung / hiệu lực |
|---|---|
| [0001-seller-side-multichannel-return-label.md](0001-seller-side-multichannel-return-label.md) | Nền domain Seller và Return Label |
| [0001-seller-sku-insights.md](0001-seller-sku-insights.md) | Trải nghiệm SKU insight |
| [0002-generator-simulate-once-render-per-channel.md](0002-generator-simulate-once-render-per-channel.md) | Mô phỏng chung, renderer từng Channel |
| [0002-seller-advice-cloud-scope.md](0002-seller-advice-cloud-scope.md) | Quyền tư vấn và phạm vi Cloud |
| [0003-iceberg-va-rest-catalog-cho-lakehouse.md](0003-iceberg-va-rest-catalog-cho-lakehouse.md) | Nền lakehouse hiện tại |
| [0003-gcp-on-demand.md](0003-gcp-on-demand.md) | GCP on-demand |
| [0004-local-learning-cloud-delivery.md](0004-local-learning-cloud-delivery.md) | Local để học, Cloud là đích |
| [0005-five-channel-data-contract.md](0005-five-channel-data-contract.md) | Warehouse/năm Channel/tiền tệ; phần 180 ngày nguồn được 0008 thay thế |
| [0006-gcs-feast-integration.md](0006-gcs-feast-integration.md) | GCS + Feast Spark/Redis; cần POC |
| [0007-versioned-publication.md](0007-versioned-publication.md) | Generation/manifest/request pin; cần POC |
| [0008-longer-history-for-training.md](0008-longer-history-for-training.md) | Q49 thay độ dài nguồn D11; phục vụ vẫn 180 ngày |
| [0009-partial-lifecycle-inventory-seller-triage.md](0009-partial-lifecycle-inventory-seller-triage.md) | Q52–Q55 mở scope; phần nhãn tiếp nối tại 0010 |
| [0010-shipment-windows-return-label-v2.md](0010-shipment-windows-return-label-v2.md) | Q56–Q59: label v2, shared/dedicated pool, queue ba nhóm; cần implementation/POC |
| [0011-sales-basis-and-observable-seller-feeds.md](0011-sales-basis-and-observable-seller-feeds.md) | Q60/Q61: sales trừ hủy trước ship, Refund riêng; nguồn Seller operational giả lập có authority |
| [0012-contracts-in-git-with-ci.md](0012-contracts-in-git-with-ci.md) | Contract schema trong Git + CI GitHub Actions/pre-commit chặn thay đổi phá vỡ; không dựng service registry |
| [0013-weekly-contiguous-coverage.md](0013-weekly-contiguous-coverage.md) | Coverage scale theo tuần nối tiếp, union interval cho nhãn và giới hạn evidence từng line |

Quyết định Q mới đã được xác nhận thay phần cũ tương ứng; ghi rõ quan hệ
thay thế thay vì diễn giải đoạn lịch sử là policy hiện hành. Ledger quy định
nghiệm thu, ADR ghi trade-off, readiness
ghi gate còn mở; không tài liệu nào tự chứng minh runtime đạt.
