# Xử lý kết quả audit tích hợp — 2026-10-18

Người dùng yêu cầu “Hiểu, xử lý” theo đề xuất Q60/Q61. Hai policy đã được
ghi vào [ADR-0011](adr/0011-sales-basis-and-observable-seller-feeds.md) và
đồng bộ [plan tổng](high-level-architecture-plan.md), readiness, ledger,
glossary. Không đổi ngân sách, quyền read-only hoặc Cloud do người dùng tự
thao tác. Không có deployment Cloud hoặc sinh lại bộ nguồn lớn trong lượt này.

## Code đã xử lý và giới hạn

| Vấn đề | Thay đổi | Bằng chứng / chưa chứng minh |
|---|---|---|
| IR01: resume đổi config trộn world | [Dataset identity](../generator/dataset_identity.py) pin config, Storefront order, source-tree hash và runtime; [Batch](../generator/generate_order_snapshots.py) kiểm trước mutation, root/checkpoint/manifest cùng version | Sáu regression mới fail trước sửa, pass sau sửa; clean/dirty resume byte-exact, restart/shrink và metadata guard đạt. Case tái hiện đã xử lý; G0 tổng vẫn mở |
| IR03: hai latest và offset trùng sau reset | [CDC boundary](../spark_jobs/lib/cdc_offsets.py) atomic first-writer manifest theo run, exact partitions/offsets, cluster/topic UUID; DAG/ingest/validate dùng chung; merge có source epoch | 12 unit tests range/retry/retention/reset/checksum/concurrent manifest writers đạt. **Chưa chạy Kafka–Spark–Iceberg v2**; Py4J metadata calls, table MERGE và broker failure modes vẫn cần POC |
| IR21: metadata count và cùng count sai khóa | [Feature validator](../spark_jobs/gold/validate_order_snapshot_return_features.py) đọc logical source count, missing/extra keys, source uniqueness và event time; giữ required-value/run checks | 4 ca Spark local đạt: matching rows, substituted key cùng count, wrong timestamp, logical filtered relation. Không phải Iceberg delete-file integration, toàn bộ payload oracle hoặc full PIT |

IR01 từng có ba giả thuyết: checkpoint chỉ khóa ngày; SKU rewrite là nguyên
nhân độc lập; RNG không tái lập. Repro cho raw Orders giữ nguyên nhưng SKU/
manifest đổi khi seed đổi, và same-config replay byte-exact, xác định thiếu
identity guard là gốc. [Probe lỗi ban đầu](evidence/design-audit-small-probes-20261018.json)
được giữ nguyên cùng hash code cũ. [Evidence sau sửa](evidence/audit-remediation-20261018.json)
ghi code hashes/test results mới; không dùng hash cũ xác nhận code mới.

Generator guard giả định một writer, chưa scan hash tất cả file đã sinh và
chưa là world/substream v2 của IR05/IR08. Root legacy thiếu identity sẽ bị
từ chối khi resume: dùng root mới, hoặc chủ động explicit restart nếu muốn
tái sinh dữ liệu generator sở hữu. Không tự nâng manifest cũ hoặc xóa dữ liệu
cũ trong quá trình sửa. Đổi source code dù không đổi semantics vẫn làm guard
từ chối, lựa chọn bảo thủ để tránh chứng minh byte-equivalence không có căn cứ.

CDC control directory là shared POSIX driver volume, không phải GCS/S3 atomic
store. Setup dùng `raw_walmart_cdc_v2`, bảng cũ thiếu epoch bị từ chối và giữ
nguyên proof legacy. Source range là toàn phần còn retained tại lúc freeze,
không phải incremental low-watermark. Mỗi run mới có thể đọc lại range đó;
MERGE chống lặp theo epoch/key. Kafka tombstones vẫn bị bỏ theo policy hiện
có. Mất manifest hoặc retention làm source range mất thì fail, không tự lấy
latest. Tham khảo [CDC](cdc.md).

**Giới hạn quan trọng của UUID check:** trước/sau capture/read phát hiện
nhiều case reset nhưng không giữ một broker lock trong lúc Spark đọc. Admin
không được recreate/reset topic đồng thời; fencing/quiescence đó phải được
kiểm chứng trong POC. Unit tests của metadata state machine không chứng minh
transport chống mọi race hoặc việc cache Spark không bị recompute.

Feature validator vẫn đọc table names hiện tại. Việc pin input snapshot,
kiểm knowledge/availability và value oracle độc lập còn thuộc IR02/IR21/IR23;
không gọi các kiểm tra key/time vừa thêm là kiểm chứng point-in-time đầy đủ.

## Contracts đã cụ thể hóa

- [Source contract v2](source-contract-order-lifecycle.md): local capability matrix,
  producer/authority, event/revision/causal IDs, ordered/canceled/refund money,
  immutable bindings, coverage/uncertainty và ba horizon có tail; mapping
  fixtures tới IR04/05/07/08/09/11/12/13/14/15/16/20/24.
- [Run/publication contract](run-publication-contract.md): frozen input/stage
  vectors, label history, output profiles, model-ready/evaluation clocks,
  compatible serving bundle, PG CAS/fencing, turn/cursor/cache pins,
  dependency retention/restore epoch và shared work budgets;
  IR02/06/10/17/18/19/20/22/23/25.

Hai tài liệu này mô tả schema/interface/probe cần implement; chưa có DDL,
new source feeds, publication service, Feast, ML hoặc agent runtime mới từ
việc viết contract. G0–G4 vẫn mở cho kiến trúc mở rộng.

## Kiểm chứng và bước kế tiếp đã xác định

Bộ Python chạy toàn repo; Spark chạy local[1] trong image cached
`fsds-airflow:2.10.5-spark3.5.3`, container không mạng, workspace read-only,
không mount project data volumes hoặc nối catalog. Lần đầu Spark không khởi
động vì hostname trong container không mạng; cấu hình loopback cho test
runner rồi chạy đạt. Đây là lỗi môi trường kiểm tra, không phải regression
feature đã được tái hiện trước sửa. JUnit/code hashes và kết quả cuối nằm
trong evidence JSON. Kiểm JSON/link/whitespace chỉ là kiểm tài liệu.

Kết quả cuối: **166 test Python đạt** (`pytest`, 8,29 giây), **4 test Spark
đạt** (`unittest`, 14,485 giây). JUnit Python được giữ tại
[audit-remediation-python-20261018.xml](evidence/audit-remediation-python-20261018.xml).
19 tài liệu không có link local hỏng hoặc trailing whitespace; 11 JSON
examples parse hợp lệ. Hash của 12 code/test files khớp evidence sau sửa.

Thứ tự tiếp theo là implement fixture/source envelope v2 ở G0, kiểm Kafka
boundary v2 trên stack nhỏ, rồi pinned inputs/publication POC trước dataset
lớn. Dependency lock, sizing, ngưỡng benchmark/retention và Cloud quota/budget
vẫn cần kết quả đo. Không có câu hỏi policy Q60/Q61 pending; không có căn cứ
để khẳng định mọi vấn đề runtime đã được loại bỏ chỉ bằng rà soát thiết kế.
