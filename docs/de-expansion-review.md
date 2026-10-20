# Rà soát mở rộng DE cho ML, agent và insight

Cập nhật 2026-10-17. Trạng thái: Q1–Q31 đã được xác nhận, Q26 thay thế lựa
chọn PostgreSQL online ở D23 bằng Redis. Đã rà nguồn và chốt các nhóm
analytics bổ sung, ưu tiên inventory cho roadmap nguồn tiếp theo.
Đây là đánh giá khả thi có điều kiện, chưa phải đặc tả triển khai hoàn chỉnh.
Chưa thay đổi code hoặc triển khai service trong lượt rà soát này.

Cập nhật 2026-10-18: người dùng đã duyệt Q52–Q55. Riêng inventory nhỏ
pool/location/reservation/inbound nay được nâng từ roadmap Q31 vào phạm vi
kế tiếp; các đoạn “chưa mở scope inventory” bên dưới ghi lịch sử Q1–Q31.
Scope và impact hiện hành theo [ADR-0009](adr/0009-partial-lifecycle-inventory-seller-triage.md)
và [thiết kế mở rộng](expanded-scope-design.md).

Sau Q31, người dùng yêu cầu discovery sâu hơn trước triển khai để tránh
làm lại. Vòng xác định tiếp tục tại
[rà phụ thuộc toàn hệ thống](de-discovery-risk-review.md); đã phát hiện
khoảng trống lịch sử nguồn, rubric toàn MLE/agent, manual/IaC, streaming
dual sink và RAG backend. Không coi Q1–Q31 là thiết kế đã đủ để implement.
Q32–Q51 nay đã xác nhận tại tài liệu đó. Các phần dưới giữ diễn tiến Q1–Q31;
câu “chưa chốt” trong phân tích lịch sử không ghi đè quyết định mới. Đích
hiện hành: GCS warehouse mới, Feast Spark/Redis, một UI + hai agent +
coordinator, generation/manifest/request pin và mục tiêu ~365 ngày nguồn /
180 ngày serve. Contract/POC còn mở theo
[điều kiện triển khai](implementation-readiness.md); dùng
[plan tổng](high-level-architecture-plan.md) làm điểm vào hiện hành.

## Yêu cầu của người dùng

1. Mở rộng Gold và feature để dùng được cho ML và agent.
2. Rà lại lý do warehouse chỉ có Walmart; xem xét đủ năm Channel.
3. Bổ sung Feast vào kiến trúc thực tế.
4. Bổ sung insight và recommendations.

Người dùng yêu cầu giải quyết thiết kế trước khi quay lại học code. Thuật ngữ
nghiệp vụ hiện có tiếp tục theo [terminology.md](terminology.md), các thuật ngữ
mở rộng theo [GLOSSARY.md](../GLOSSARY.md). Hợp đồng metric và quyền hành động
của recommendation được theo dõi bên dưới; các nguyên tắc metric và quyền
chỉ tư vấn đã chốt. Ngưỡng cụ thể từng cảnh báo cần đặc tả/kiểm chứng sau.

## Quyết định được xác nhận trong phỏng vấn

- D1 đã xác nhận: agent phục vụ Seller. Người dùng bổ sung trải nghiệm mong
  muốn tương tự việc chọn một SKU trong TikTok Shop Seller Center để xem
  insight và mở rộng sang gợi ý hành động cho Seller. Đây là tham chiếu trải
  nghiệm, chưa phải yêu cầu sao chép toàn bộ chỉ số/giao diện hoặc tích hợp
  API TikTok thật.
- D2 đã xác nhận: gợi ý để Seller quyết định; phiên bản đầu không tự đổi giá,
  hủy đơn, hoàn tiền hoặc gửi thông báo.
- D3 đã sửa theo yêu cầu người dùng: bắt buộc triển khai Cloud, bác bỏ đề
  xuất lấy demo trên laptop làm đích. D5 đã chọn GCP với trial credit 300 USD;
  D7 đã chọn chỉ chạy khi cần. D6 đã chốt chi ngoài credit là 0 USD; D8 chọn
  GCE trước cho DE/Feast, Kubernetes ở giai đoạn MLE/agent; D9 chốt thử nhỏ
  rồi nghiệm thu nguồn 100 GB và đủ năm Channel khi dự toán phù hợp. Region,
  quota, dung lượng thực, credit/hạn còn lại và cấu hình hạ tầng chưa kiểm tra.
- D4 đã xác nhận: phiên bản đầu tập trung bán hàng, xu hướng theo Storefront,
  rủi ro trả hàng và gợi ý kiểm tra. Traffic/ads/inventory là phần mở rộng sau;
  không suy conversion rate từ dữ liệu đơn hàng riêng lẻ.
- D10 đã xác nhận: warehouse chung có chi tiết năm Channel, dim SKU/
  Storefront và các bảng tổng hợp theo ngày/SKU/Storefront.
- D11 đã xác nhận: một bộ nguồn chung 180 ngày năm Channel, chung catalog/
  cấu hình/cutoff, cân scale đạt mốc 100 GB. Các proof cũ giữ làm tham chiếu.
- D12 đã xác nhận: tiền tệ gốc ở phiên bản đầu; chưa đưa tổng USD xuyên
  Storefront khi chưa có hợp đồng FX. Không coi price_factor là FX.

Quyết định phạm vi domain được ghi tại
[ADR-0001](adr/0001-seller-sku-insights.md).
Quyền tư vấn, phạm vi đầu tiên và yêu cầu Cloud được ghi tại
[ADR-0002](adr/0002-seller-advice-cloud-scope.md).
GCP và lịch bật khi cần được ghi tại [ADR-0003](adr/0003-gcp-on-demand.md).
Người dùng làm rõ toàn bộ local là để học/phát triển/kiểm thử; Cloud là đích
triển khai toàn hệ thống, ghi tại
[ADR-0004](adr/0004-local-learning-cloud-delivery.md).
Nguồn và warehouse năm Channel, tiền tệ phiên bản đầu được ghi tại
[ADR-0005](adr/0005-five-channel-data-contract.md).

## Tham chiếu trải nghiệm TikTok Shop

Đã đối chiếu tài liệu chính thức TikTok Shop US; đây là tham chiếu, không
khẳng định giao diện/quy tắc ở mọi thị trường giống nhau:

- [Product Analytics](https://seller-us.tiktok.com/university/essay?knowledge_id=4169993017509675):
  xem hiệu quả sản phẩm/SKU và xu hướng; funnel cần lượt xem, inventory cần
  tồn kho thực tế.
- [Shop Analytics](https://seller-us.tiktok.com/university/essay?knowledge_id=813364865828654):
  GMV gắn thời điểm thanh toán và quy tắc riêng; không tự gọi tổng tiền net
  Order Line của FSDS là GMV tương đương TikTok. Phân biệt số đơn, dòng hàng
  và units; refund có thời điểm khởi tạo/hoàn tất riêng.
- [Product Intelligent Analysis](https://seller-us.tiktok.com/university/essay?knowledge_id=5341265403184909):
  giải thích và gợi ý có thể dùng traffic, nội dung, creator, quảng cáo; FSDS
  chưa có các nguồn này.

Suy luận khả thi cho FSDS: có thể bắt đầu bằng SKU overview, xu hướng giá trị
đơn/số lượng theo currency và Storefront, risk từ nhãn đủ hạn, cùng gợi ý
kiểm tra có căn cứ. Conversion/CTR cần traffic; ROAS cần ads spend và
attribution; gợi ý nhập hàng cần inventory/lead time; kết luận nguyên nhân
trả hàng cần dữ liệu bổ sung. D4 đã xác nhận nhóm sales/trends/return-risk
cho phiên bản đầu; D16–D20 chốt nguyên tắc metric, cohort và ngày UTC.

## Hiện trạng đã đối chiếu code

| Phần | Dữ kiện | Hệ quả thiết kế |
|---|---|---|
| Gold | `spark_jobs/gold/build_order_snapshot_lines.py` gộp năm Channel; giữ tiền tệ nhưng chưa có Storefront/category/fulfillment trong fact chung. Silver Amazon có marketplace/fulfillment. | Có thể mở rộng từ nguồn hợp lệ; cần mapping Storefront, không suy Storefront chỉ từ nước nhận hàng. |
| Feature | `spark_jobs/gold/build_order_snapshot_return_features.py` chỉ chọn thuộc tính Order Line và biến đổi đơn giản; khóa là `order_line_key`; ghi thay toàn bảng. | Chưa có feature lịch sử dùng chung theo SKU + Storefront như định hướng trong Domain. |
| Thời gian | `event_timestamp` lấy ngày đặt hàng, `created` lấy lúc job chạy. Nguồn là Gold từ các snapshot Silver mới nhất. | Tên timestamp và blacklist cột chưa đủ chứng minh point-in-time. Cần kiểm tính bất biến hoặc lịch sử quan sát của giá trị, và phân biệt thời điểm nghiệp vụ, quan sát, tính toán, sẵn sàng phục vụ. |
| Nhãn | Generator chỉ xuất nhãn đủ hạn tại cutoff (`generator/generate_order_snapshots.py`, `_collect_labels`). Bridge hiện chỉ cho Walmart; không có tham số as-of cho từng run (`spark_jobs/gold/build_walmart_order_snapshot_labels.py`). | Tỷ lệ trả hàng lịch sử cần lọc nhãn theo `label_available_at`, mẫu số đủ hạn và bộ khóa chính xác cho từng Channel. Không coi nhãn chưa đủ hạn là 0. |
| Warehouse | `spark_jobs/warehouse/export_order_snapshot_lines.py` chủ động lọc Walmart, chọn sáu cột để vừa máy dev; JDBC overwrite. | Không bị giới hạn về công nghệ ở Walmart. Có thể xuất năm Channel, nhưng phải quyết định mart tổng hợp hay bản sao chi tiết; refresh cần giữ index/constraint và có cơ chế công bố nhất quán. |
| SKU | Catalog nguồn có SKU, tên, category, giá/cost USD; latent defect propensity không được xuất (`generator/generate_order_snapshots.py`, `_write_skus`). | Có thể tạo dim SKU từ catalog. Không dùng biến ẩn của simulator để làm feature hoặc bằng chứng giải thích. |
| Feast | Chưa có dependency/config/entity/registry/materialization/online lookup. README coi là phần mở rộng. | Cần tích hợp thật và proof historical retrieval + materialization + online retrieval; bảng `feat_*` hiện tại chưa phải Feast. |
| Agent | Chưa có data contract agent của FSDS. Ví dụ khóa học trong `edai-k11-cw/domain/ecommerce/03_llm_agent_platform.md` là chăm sóc khách, dùng customer/cart/session; domain hiện tại là Seller và không có clickstream xuyên Channel. | Phải chốt agent phục vụ Seller hay buyer, không tự áp ví dụ khóa học vào domain hiện tại. |

Nguồn ground truth hiện có nhãn nhị phân, thời điểm giao, thời điểm có nhãn;
chưa cung cấp đầy đủ refund amount/reason/event cho phân tích kinh tế hoàn trả.
Tiền bán hàng có nhiều currency, còn catalog cost là USD. Lợi nhuận hoặc tổng
doanh thu đổi tiền cần hợp đồng FX/cost/refund/fee rõ ràng; chưa đủ để cam kết.

Rà sâu nguồn cho vòng kế tiếp:

- `price_factor` tại `generator/core/storefronts.py` gộp cả điều chỉnh giá
  bán và yếu tố đổi tiền; không được coi nó là tỷ giá FX thuần. Chưa có FX
  source/table để quy đổi doanh số theo ngày.
- Một SKU có thể xuất hiện nhiều Order Line trong cùng Order. Cần order key
  riêng: bỏ line ID khỏi identity đang dùng, nhưng giữ secondary order ID
  của Walmart. Số đơn theo SKU là distinct order key; số dòng và units là
  hai phép đo khác. Không cộng số đơn qua các SKU thành tổng đơn toàn shop.
- `SUM(line_net_amount)` là giá trị hàng đặt sau giảm giá được nguồn thể
  hiện, chưa phải paid GMV, tiền thực thu hoặc lợi nhuận. Giá bán bình quân
  theo unit dùng tổng giá trị chia tổng quantity, không trung bình đơn giản
  các đơn giá khi quantity khác nhau.
- Manifest 100 GB cũ có export Amazon/Walmart/SKU, dù simulator có tạo đơn
  và nhãn của các Storefront khác. Bộ ba Channel mới hiện dùng nguồn nhỏ
  60 ngày/50 SKU; Gold hiện kết hợp Amazon 10 ngày và các nguồn có độ phủ
  khác nhau. Chưa có proof insight 90 ngày đồng nhất năm Channel.
- Generator hiện hỗ trợ cả năm batch renderer và 180 ngày. Chạy cấu hình
  scale cũ với renderer mới làm volume tăng; cần ước lượng/cân scale lại nếu
  xây run chung năm Channel đạt mốc 100 GB. Nhãn/key/cutoff/catalog phải
  nhất quán theo run, không tự ghép các nguồn giả lập khác config thành một
  lịch sử được tuyên bố đồng nhất.

## Feast: đường tích hợp có căn cứ, cần POC

- Tài liệu chính thức có [SparkSource](https://docs.feast.dev/reference/data-sources/spark)
  với [IcebergFormat](https://docs.feast.dev/reference/data-sources/table-formats).
  Đây là tích hợp contrib; cần pin phiên bản và kiểm với Spark 3.5.3,
  Python thực thi, Lakekeeper REST catalog và MinIO của repo.
- [Trino offline store](https://docs.feast.dev/reference/offline-stores/trino)
  cũng là contrib. Spark vẫn tính/ghi feature; adapter đọc để retrieval không
  thay nhiệm vụ tính feature. Chưa chọn adapter.
- [Point-in-time joins](https://docs.feast.dev/getting-started/concepts/point-in-time-joins)
  có ràng buộc event time, TTL và tùy chọn availability qua created timestamp.
  Không phải mọi backend đều hỗ trợ mọi tùy chọn. `created` là thời điểm backfill
  không tự trở thành bằng chứng giá trị đã sẵn có trong quá khứ.
- Đơn vừa tạo chưa có trong batch feature đã materialize. Feature của request
  có thể cần lấy từ request; Feast hữu ích cho feature lịch sử dùng lại theo
  SKU/Storefront. Entity và freshness còn phụ thuộc use case sẽ chốt.
- Offline retrieval và online serving là hai nhu cầu riêng; online cần backend,
  materialization/push, missing-value và freshness contract. Xem
  [online store matrix](https://docs.feast.dev/reference/online-stores/overview).

Rubric hiện tại, sheet `mle` trong `EDAI K11 - MLE.xlsx`, còn có RAG pipeline
qua Feast (dòng 9), incremental materialization (11), training từ offline
store (13), ML serving (15), API dữ liệu/chunk (16), agent gọi inference và
RAG (22). Điều này cần tính trong thiết kế tiếp nối; không có nghĩa triển
khai toàn bộ MLE trong lượt mở rộng DE này.

## Môi trường phát triển và yêu cầu Cloud

Quan sát 2026-10-17: RAM tổng 14 GiB, available khoảng 8.3 GiB;
đĩa `/` còn khoảng 9.6 GiB và `/data` còn 18 GiB. Đây là ảnh chụp tài nguyên,
không phải ngân sách đã cam kết và không phải cấu hình Cloud đích. Bản sao
chi tiết nhiều tầng/online store và chạy toàn bộ stack đồng thời cần đo riêng.
Không tự xóa dữ liệu để lấy chỗ.

Repo hiện có Docker Compose với mount đường dẫn host, endpoint nội bộ và
credential MinIO; chưa thấy IaC Cloud/Terraform/Kubernetes trong file đã
kiểm tra. Cloud cần xử lý đường dẫn dữ liệu/object storage, identity,
network, persistent storage, lịch job và công bố dịch vụ; không chỉ chuyển
nguyên file Compose lên máy khác. Chưa cấp phát tài nguyên Cloud.

GCP đã được chọn; phân tích AWS trước đó không còn là phương án đích. Theo
[GCP Free Trial](https://docs.cloud.google.com/free/docs/free-cloud-features),
trial credit 300 USD là tổng credit trong 90 ngày, không phải mỗi tháng;
trial chưa upgrade có hạn chế GPU/quota. Chưa truy cập account để xác nhận
credit thực còn lại, ngày hết hạn hoặc quota ở region dự kiến.

Hướng đã được đồng ý: GCE chạy nhóm service DE/Feast, rồi tiếp nối Kubernetes
khi mở rộng MLE/agent. Storage bền vững cần tách vòng đời compute; chưa chọn
backend. GCE 8 vCPU/32 GiB chỉ là giả thuyết sizing, chưa có proof quota
hoặc tải. Không kết luận phải dùng MinIO, GCS hay GKE khi chưa kiểm chứng.

Code hiện hardcode S3FileIO/MinIO ở Spark session, runtime và Trino;
chuyển GCS cần thay adapter, catalog storage profile và identity tương ứng,
không chỉ đổi một endpoint. Giữ MinIO trên GCE là hướng giảm thay đổi code
để thử, nhưng tạo thêm trách nhiệm quản lý storage/backup; các trade-off
còn đang mở.

[Lakekeeper GCS](https://docs.lakekeeper.io/docs/latest/storage-gcs/) đã có
trong bản v0.13.6 đang pin (xem
[release notes](https://docs.lakekeeper.io/about/release-notes/)). Spark cần
GCSFileIO/gcp-bundle phù hợp; đọc raw bằng Hadoop `gs://` còn cần connector
filesystem riêng. Iceberg metadata cũ chứa đường dẫn `s3://`, nên copy file
sang GCS không tự chuyển được bảng; cần warehouse mới/rebuild hoặc migration
có kiểm chứng. Chưa lựa chọn phương án migration.

[GKE pricing](https://cloud.google.com/kubernetes-engine/pricing) có monthly
cluster-management credit cho loại cluster đủ điều kiện; không được suy mọi
phí cluster được miễn hoặc compute/storage miễn phí. GKE start/stop khác
stop một VM. Chọn GCE/GKE phải xét triển khai Kubernetes ngay hay ở phase
MLE sau, và quota trial. Chưa chốt tổng chi phí.

Chưa báo giá tổng khi chưa biết region, giờ chạy, dữ liệu và retention. Dự
toán phải tính compute, dung lượng persistent disk được cấp, object storage
raw/các tầng/snapshot, requests, backup, IP/network/logs nếu có. Chỉ bật VM
khi cần không đồng nghĩa storage được giữ sẽ miễn phí. Chi ngoài credit đã
chốt là 0 USD; chưa có quyền paid upgrade.

## Cây quyết định

Sau câu trả lời Q1–Q4:

- D1 đã chốt: Seller xem insight/recommendation khi chọn một SKU.
  - Sau D1: câu hỏi cụ thể → metric/grain → Gold marts và nguồn bổ sung.
  - Sau D1: prediction contract → entity/time/freshness → Feast adapter/store.
  - Sau D1: insight vs recommendation → bằng chứng và tiêu chí đánh giá.
- D2 đã chốt: chỉ tư vấn ở phiên bản đầu.
  - Sau D1+D2: công cụ agent, quyền ghi, quy trình con người duyệt nếu cần.
- D3 đã chốt yêu cầu Cloud:
  - D5 đã chọn GCP trial credit 300 USD; region/account/quota chưa kiểm tra.
  - D6 đã chốt chi ngoài credit là 0 USD; credit/quota thực còn cần kiểm tra.
  - D7 đã chốt chỉ chạy khi cần; dữ liệu giữ lại vẫn có chi phí.
  - D8 đã chọn GCE trước cho DE/Feast; Kubernetes ở giai đoạn MLE/agent.
  - D9 đã chốt thử nhỏ trước, nghiệm thu Cloud với 100 GB và đủ năm Channel
    khi dự toán phù hợp. Cần hợp đồng run nguồn và độ dài lịch sử nhất quán.
  - Sau D1+D3: warehouse đầy đủ hay mart, refresh, dung lượng và proof.
  - Sau hợp đồng Feast+D3: POC tương thích và acceptance tests.
- D4 đã chốt nhóm chỉ số sales/trends/return-risk. Còn mở công thức, cửa sổ,
  độ phủ, currency, độ mới và cách thể hiện thiếu dữ liệu.
  - D10 đã chốt warehouse chung chi tiết năm Channel + dimensions + marts.
  - D11 đã chốt run nguồn chung 180 ngày, cân scale đạt mốc 100 GB.
  - D12 đã chốt v1 giữ tiền gốc, chưa bổ sung tổng USD/FX.
  - D13 đã chốt: entity Feast lịch sử SKU + Storefront; thuộc tính Order Line
    vừa tạo lấy từ request/transform thay vì chờ batch materialize.
  - D14 đã chốt: snapshot lịch sử theo ngày, refresh sau pipeline thành công;
    hiển thị data_as_of riêng với thời điểm tính toán, phân biệt đồng hồ mô
    phỏng với thời gian chạy; thiếu lịch sử không tự điền thành 0.
  - D15 đã chốt: return-risk v1 là thống kê/cờ cảnh báo có căn cứ, xác suất
    ML chỉ công bố sau khi có model được train và đánh giá ở phase MLE.
  - D16 đã chốt: metric sales theo ngày đặt, cửa sổ 7/30/90 ngày hoàn chỉnh,
    so sánh cửa sổ liền trước cùng độ dài; tách cancelled và tiền gốc.
  - D17 đã chốt: tỷ lệ trả hàng theo Order Line đã giao đủ 30 ngày tại cutoff,
    hiển thị tử/mẫu số; ngưỡng mẫu tối thiểu 30 là quy tắc demo cấu hình được,
    không phải bảo đảm thống kê.
  - D18 đã chốt: recommendation v1 theo quy tắc có bằng chứng, nêu hành động
    kiểm tra và giới hạn; không tự khẳng định nguyên nhân hoặc hiệu quả.
  - D19 đã chốt: màn hình SKU tối thiểu và API trong phạm vi mở rộng này;
    chọn Storefront/khoảng ngày, xem metric/trend/recommendation và căn cứ.
  - D20 đã chốt: ngày chuẩn UTC, cửa sổ chỉ gồm ngày hoàn chỉnh trước cutoff;
    tỷ lệ trả hàng dùng cohort theo ngày giao, chỉ đưa vào nhãn đã đủ hạn.
  - D21 đã chốt: chỉ công bố snapshot phục vụ khi các đầu ra bắt buộc đã qua
    kiểm tra; lỗi refresh giữ bản thành công trước và thể hiện độ cũ.
  - D22 đã chốt: GCS lưu raw/Iceberg trên Cloud; tạo warehouse Cloud mới và
    chạy lại pipeline từ nguồn, giữ proof local làm tham chiếu.
  - D23 đã chốt ban đầu: Spark offline + PostgreSQL online; Q26 thay thế
    online backend bằng Redis. Vẫn kiểm tra độ mới tường minh.
  - D24 đã chốt: nghiệm thu POC nhỏ gồm tính đúng, thời gian, rerun, lỗi publish
    và stop/start trước khi mở rộng full-scale đã chốt ở D9.
  - Ràng buộc vận hành: toàn bộ thao tác Cloud do người dùng làm tay. Trợ lý
    chuẩn bị file/cấu hình và runbook giải thích từng bước, hỗ trợ đọc kết quả
    do người dùng cung cấp; không tự đăng nhập, gọi API/lệnh Cloud, cấp phát,
    deploy, bật/tắt, xóa tài nguyên hoặc thay đổi billing/IAM trên Cloud.
    Việc đồng ý kiến trúc không phải quyền thực hiện thao tác Cloud.
  - D25 đã chốt: dữ liệu lịch sử được dựng lại phải dùng dữ liệu có thể biết
    tại cutoff; ghi riêng thời điểm tính/publish thực tế, không backdate
    availability để giả nhận hệ thống đã phục vụ online trong quá khứ.
    Giá trị không đủ bằng chứng lịch sử phải nêu giới hạn hoặc loại khỏi tập
    huấn luyện; không dùng snapshot mới nhất như bằng chứng quá khứ.

Đã đối chiếu tài liệu chính thức cho đề xuất D22–D24, chưa có bằng chứng
tích hợp chạy thành công. Lakekeeper có hỗ trợ GCS (nguồn ở phần Cloud).
[Spark offline store](https://docs.feast.dev/reference/offline-stores/spark)
là contrib, chưa có full test coverage; cần pin phiên bản và POC.
[PostgreSQL online store](https://docs.feast.dev/reference/online-stores/postgres)
giữ feature mới nhất, không hỗ trợ concurrent same-key writes hoặc TTL
filter lúc retrieval. Đề xuất một luồng materialize và kiểm tra data_as_of;
đánh giá lại backend khi có yêu cầu tải MLE. Spark/GCS cần kiểm chứng cả
raw filesystem connector và Iceberg FileIO, cùng Trino/catalog. PostgreSQL
và catalog cần persistent storage/backup riêng; GCS không tự bảo vệ các DB.

Phân tích PostgreSQL online ở trên là phương án cũ; Q26 đã chọn Redis.
Lựa chọn storage/Feast được ghi trong ADR 0006; vẫn cần POC để xác nhận.
Region, quota/credit thực, sizing và retention còn cần xác định trong kế
hoạch triển khai thủ công; chưa tuyên bố tổng chi phí hoặc full-scale pass.

## Điều kiện tối thiểu để gọi một hướng là khả thi

- Mỗi đầu ra có người dùng, câu hỏi nghiệp vụ, grain, khóa, thời gian và nguồn.
- Metric đa Channel có currency, cutoff và denominator rõ; thiếu dữ liệu thì
  báo thiếu, không tự tạo nguyên nhân/hiệu quả can thiệp.
- Historical feature loại thông tin chưa sẵn có ở mốc dự đoán; phân biệt
  backfill offline với SLA online thật.
- Warehouse refresh không mất index/constraint hoặc trộn hai phiên bản dữ liệu.
- Feast POC đọc đúng source, retrieval theo thời gian, materialize và online
  lookup có kiểm tra freshness/missing entity trên môi trường đã pin.
- Insight có phép tính và dữ liệu nguồn tái kiểm tra được; recommendation
  nêu căn cứ và không tuyên bố causal/uplift khi chưa có đánh giá phù hợp.

## Kết luận vòng thiết kế kiến trúc

Cập nhật sau Q28: chưa đóng phiên. Người dùng muốn mở rộng thêm nếu khả thi.
Đã chọn Redis online trên GCE cho POC, PostgreSQL phục vụ warehouse. Redis
được Feast hỗ trợ: https://docs.feast.dev/reference/online-stores/redis .
Thêm Redis đòi hỏi quản lý RAM và phục hồi/rematerialize khi bật lại.

Các quyết định rà toàn cảnh đã chốt:
- Q26: chuyển online store sang Redis; không mặc định dùng managed Redis.
- Q27: trải nghiệm đích tích hợp trang SKU + hỏi đáp agent dựa trên số liệu,
  tài liệu và model khi có, vẫn chỉ tư vấn; thực hiện theo giai đoạn.
- Q28: mở rộng Gold theo nguồn thật sự có: category/fulfillment, hủy đơn và
  thời gian giao khi đủ timestamp; inventory/ads/traffic cần nguồn bổ sung,
  không suy từ dữ liệu đơn hàng. Người dùng mong muốn hơn nữa nếu khả thi;
  chưa coi câu này là chấp thuận mọi nguồn mới hoặc mọi tính năng đề xuất.

Bốn hướng mở rộng có đường triển khai khả thi về kiến trúc: Gold/warehouse
chung năm Channel, feature lịch sử theo SKU + Storefront qua Feast, insight
và recommendation tư vấn trên trang SKU, đích GCP do người dùng thao tác.
Q1–Q25 không phải bằng chứng các thành phần đã được triển khai hoặc chạy đúng.

Trước triển khai cần hoàn thiện đặc tả metric (status, cohort boundaries,
ngưỡng cảnh báo), hợp đồng timestamp và cơ chế publish không trộn phiên bản.
POC phải xác nhận tương thích Spark/Iceberg/GCS/Feast và tính đúng thời gian.
Trước cấp phát Cloud, người dùng kiểm tra account/credit/quota; cùng chọn
region, sizing, retention/backup và dự toán dựa trên kết quả nhỏ. Các mục
này được hoãn có chủ đích tới đặc tả/POC/runbook, không coi là đã giải quyết.

Tiếp tục mục tiêu học sau khi người dùng xác nhận bản tổng hợp phản ánh đúng
ý định; thiết kế mở rộng là đích để học, code hiện tại vẫn được giải thích
theo đúng trạng thái đang có. Không tự triển khai Cloud.

## Rà mở rộng nghiệp vụ sau Q28

Đối chiếu read-only nguồn/code, chưa thêm tính năng:
- Gold có order identity/SKU/quantity để tính basket/co-purchase; phải khử
  lặp SKU trong đơn. Generator chọn SKU độc lập theo popularity
  (`generator/core/simulate.py:177`), nên không chứng minh bổ trợ sản phẩm
  thực tế hoặc hiệu quả bundle từ dữ liệu mô phỏng này.
- Có thể tính top-N/tỷ trọng SKU, SKU catalog không có doanh số quan sát,
  và mix địa lý từ Gold (`spark_jobs/gold/build_order_snapshot_lines.py:81,97`). Không
  có sales không đồng nghĩa hết cầu/tồn kho; địa chỉ thiếu phải có nhóm unknown.
- Gross/discount có ở bốn Channel, Walmart null (`gold/build_order_snapshot_lines.py:59`).
  Tỷ lệ giảm giá dùng tổng discount/tổng gross, thiếu không coi 0; phân tích
  tương quan với sales/returns không chứng minh uplift hay giá tối ưu.
- Customer ID có ở Walmart Silver, TikTok/Shopify/Shopee raw; Amazon không
  xuất buyer ID. Cần đưa lên curated, giới hạn theo Channel/Storefront,
  phân biệt guest và first-observed với first-ever. Không nối danh tính
  xuyên Channel. Nguồn: `silver/build_walmart_order_snapshots.py:141`, generator channels
  `tiktok_batch.py:99`, `shopify.py:90`, `shopee.py:99`.
- Inventory/replenishment/lead-time và traffic/ads là miền nguồn bổ sung.
  Cost USD có trong catalog nhưng không đủ tính lợi nhuận thực: thiếu FX,
  phí, settlement/refund economics và lịch sử cost.

Các quyết định Q29–Q31 đã xác nhận:
- Q29 thêm basket, SKU concentration, địa lý và discount associations vào
  v1 theo độ phủ trường dữ liệu; recommendation nêu giới hạn mô phỏng.
- Q30 thêm repeat/first-observed customer trong từng Storefront có ID,
  hiển thị coverage, không hứa đủ năm Channel hoặc customer identity chung.
- Q31 roadmap nguồn tiếp theo ưu tiên inventory/nhập hàng; traffic/ads và
  profit cần hợp đồng dữ liệu riêng. Đây là ưu tiên đã chốt, chưa mở scope
  triển khai các miền này ngay.

## Lộ trình tổng hợp sau Q31

1. Mở rộng DE và đầu ra Seller: nguồn năm Channel nhất quán; Gold/detail/dim/
   marts; warehouse PostgreSQL; API/trang SKU. Analytics gồm sales, return
   cohort đủ hạn, hủy/giao hàng/category/fulfillment, danh mục, giỏ hàng,
   địa lý, giảm giá và khách quay lại theo độ phủ nguồn đã kiểm chứng.
2. Tích hợp Feast Spark/Iceberg offline + Redis online, chứng minh đúng thời
   gian, khóa, độ mới và phục hồi bằng POC. Bước này thuộc mở rộng nền dữ
   liệu; không chờ model hoàn thiện mới kiểm chứng Feast.
3. Tiếp nối MLE và agent: train/evaluate/serve model, agent có ngữ cảnh SKU
   dùng số liệu/tài liệu/inference có căn cứ. Cần đặc tả riêng use case hành
   động, nguồn RAG và tiêu chí đánh giá trước khi triển khai giai đoạn này.
4. Roadmap nguồn tiếp theo ưu tiên tồn kho/nhập hàng; traffic/ads và lợi
   nhuận ở các bước sau với data contract riêng. Chưa hứa có dữ liệu hoặc
   tính năng này trong phạm vi DE đầu tiên.

Cloud là đích cho các giai đoạn, không phải bước thêm cuối cùng. Người dùng
tự thao tác GCP; local phục vụ học/phát triển/kiểm thử. Nhỏ trước rồi full
100 GB khi POC và dự toán đạt yêu cầu. Phân chia trên không thay các giới
hạn đã chốt: native currency, quyền tư vấn, không dùng tương lai khi train,
không suy causal từ correlation, không chi tiền túi ngoài credit.
