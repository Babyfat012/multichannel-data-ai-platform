# Điều kiện triển khai và nghiệm thu FSDS

Ngày 2026-10-17. Bổ sung sau rà soát plan theo yêu cầu người dùng. Đây là
đặc tả kiểm chứng cho Q1–Q61/D1–D25, chưa phải kết quả POC hoặc runtime.
Các gate dưới đây hiện **chưa có evidence đạt** cho kiến trúc mở rộng.

Lượt audit tích hợp tiếp 2026-10-18: [sổ vấn đề](integration-risk-register.md)
gom IR01–IR25. Q60/Q61 đã chốt theo yêu cầu “Hiểu, xử lý”; xem
[ADR-0011](adr/0011-sales-basis-and-observable-seller-feeds.md).
[Source contract](source-contract-order-lifecycle.md) và
[publication contract](run-publication-contract.md) là thiết kế thực thi.
[Bản xử lý](audit-remediation-20261018.md) ghi sửa resume/CDC/feature và
test cục bộ; chưa gate G0–G4 nào hoàn thành chỉ nhờ các kiểm tra đó.

Cập nhật 2026-10-18: [discovery mở rộng](plan-expansion-discovery.md) đã
khảo sát các nhánh có thể đổi grain nguồn và phạm vi sản phẩm. Người dùng
đã chốt Q52–Q55. [Impact assessment](expanded-scope-design.md) bổ sung
S19–S30 vào phạm vi nghiệm thu; Q56–Q59 đã chốt label v2, pool topology,
movement authority và ba nhóm queue. [Contract nhãn](return-label-contract.md)
ghi expected behavior, chưa có code/POC mới. Các fixture B/M/PF
khác trong research vẫn là đề xuất nếu chưa được ánh xạ vào scope này.
G0/G1 cần quantity/Listing/stock contracts; G2 cần label/PIT tương ứng;
G3 cần queue/decomposition; G4 cập nhật footprint/restore của feed mới.

## Thứ tự và điều kiện chuyển bước

Phase chỉ nhóm sản phẩm bàn giao; POC của phụ thuộc phải làm trước phần dùng
nó. Giữ thứ tự bàn giao P1 → P2 → P3 → P4, nhưng chuẩn bị Cloud, bảo mật,
versioning và kiểm thử ngay từ đầu.

| Gate | Phải hoàn thành trước | Điều kiện đạt | Việc còn cần xác định |
|---|---|---|---|
| G0 — contract và bộ nhỏ | Sinh lại nguồn lớn | Schema mẫu đủ năm Channel; identity/time/snapshot/delta rõ; S01–S08 đạt, fixture mới S19–S30 có expected output/contract theo Q56–Q59 | Schema vật lý từng feed, capability, tie-break, reject/quarantine |
| G1 — dữ liệu đầu cuối | Full-scale P1 | Bộ nhỏ qua Bronze/Silver/Gold/PG; count/key/quantity/amount/coverage/SCD2 đối soát; GCS POC và S19/S21–S24/S29 đạt | Partition/index, scale estimate, ma trận phiên bản runtime |
| G2 — feature và publication | Train chính thức hoặc mở serving | POC Feast/publication/dual-sink; S09–S13/S19/S20/S27/S28 và L01–L17 đạt; historical feature chỉ dùng dữ liệu biết tại cutoff | Physical version namespace, manifest store, feature definitions, retention |
| G3 — ML/agent | Nghiệm thu P2/P3 | Temporal evaluation so baseline; model/features/label version tương thích; POC CPU/RAG/GKE; S14–S16/S25/S26/S30 đạt | Model/embedding pin, ngưỡng chất lượng và SLA từ benchmark nhỏ |
| G4 — vận hành | Mỗi lần chạy Cloud lớn và nghiệm thu P4 | S17–S18 đạt; dự toán, quota/credit, backup/restore, stop/start và run budget được ghi; người dùng chủ động chạy | Region, sizing, giờ chạy, TTL/retention, RPO/RTO, concurrency |

Trước full-scale P1 cần G0/G1, POC phụ thuộc nền tảng của G2 (Feast,
publication, dual-sink) và kiểm tra ngân sách G4 tương ứng; chưa đòi hoàn tất
model/UI. Trước chi phí lớn cho P2/P3 cần POC CPU/RAG/GKE. Giá trị SLA,
ngưỡng chất lượng, retention và ngân sách cụ thể phải ghi trước lần đo nghiệm
thu; chưa có giá trị thì gate còn mở, không chọn lại sau khi xem kết quả.

## Hợp đồng nguồn và identity

- Grain Bronze Amazon hiện tại: một dòng JSONL là một bản Order quan sát
  trong một export, chứa nhiều Order Line. Bản lặp vẫn được giữ. Bốn cột
  provenance hiện có không phải toàn bộ hợp đồng nguồn tương lai.
- Mỗi feed phải khai báo grain, schema version, kiểu full snapshot/delta/
  tombstone, khóa và thứ tự phiên bản. Khóa nghiệp vụ bao gồm namespace
  Channel + account/shop; Order Line giữ đầy đủ khóa nguồn (kể cả secondary
  order ID của Walmart). Storefront và Listing→SKU có mapping tường minh;
  không suy từ địa chỉ nhận hàng hoặc nối customer xuyên Channel.
- `dataset_version` định danh bộ sinh bất biến: seed, config/catalog hash,
  phiên bản generator, khoảng lịch sử, cutoff và manifest file/checksum/count.
  `run_id` định danh lần thực thi; retry/attempt được phân biệt trong log.
  `generation_id` định danh đầu ra công bố. Một dataset có nhiều run và
  nhiều generation; không dùng ba mã này thay thế nhau.
- Q49: mục tiêu khoảng 365 ngày lịch sử nguồn, cửa sổ phục vụ 180 ngày.
  365 ngày không đồng nghĩa 365 ngày mẫu train dùng được: còn warmup, giao
  hàng, maturity và thời điểm quan sát. Cùng bộ nguồn cho cả năm Channel,
  không ghép proof khác config/cutoff thành bộ nghiệm thu chung.
- Nguồn immutable được giữ để replay; manifest chỉ đánh dấu hoàn tất khi
  mọi file/phân đoạn bắt buộc đã có và kiểm tra được. Thiếu feed/ngày không
  được tự diễn giải thành ngày có doanh số 0. Cần quy tắc nhận biết duplicate
  file, file ghi dở, schema lạ và payload lỗi, cùng log/quarantine truy nguồn.
- Full snapshot bỏ line B nghĩa B không còn trong trạng thái hiện hành của
  Order đó. Delta không nhắc B không có nghĩa xóa B. Hợp đồng từng feed phải
  phân biệt hai trường hợp trước khi viết dedup/upsert.
- Late return phải có lịch sử quan sát phù hợp cửa sổ nhãn; lookback stream
  hiện tại 20 ngày không được mặc định đủ cho return trong 30 ngày cộng delay.

## Hợp đồng thời gian, metric và feature

| Mốc | Ý nghĩa và ràng buộc |
|---|---|
| `event_time` | Thời điểm nghiệp vụ xảy ra; correction có thể nói về quá khứ |
| `source_observed_at` | Thời điểm dữ liệu có thể quan sát qua nguồn; không backdate theo event |
| `export_date` | Ngày lô export suy từ partition/metadata nguồn; không thay event time hoặc availability |
| `ingested_at` | Thời điểm pipeline ingest thực tế; ở job Spark hiện tại là timestamp đầu query |
| `feature_as_of` | Cutoff dùng chọn thông tin được phép biết khi tính feature |
| `computed_at` / `published_at` | Thời gian thực tính/công bố; không backdate khi replay |
| `label_available_at` | Khi có thể biết nhãn; áp dụng cùng maturity và completeness của feed |

Contract feature phải ánh xạ rõ các mốc này sang `event_timestamp`/`created`
của bảng Feast trên adapter đã pin. Tên cột không chứng minh point-in-time.
Replay theo đồng hồ mô phỏng và freshness online theo wall-clock có báo cáo
riêng. Historical TTL, expiry Redis và ngưỡng stale API là ba chính sách riêng.

Metric sales dùng ngày đặt UTC, cửa sổ 7/30/90 ngày hoàn chỉnh trước cutoff,
so với cửa sổ liền trước; canceled tách riêng. `SUM(line_net_amount)` là giá
trị hàng đặt sau giảm giá theo currency gốc. Số Order distinct, số Order Line
và units là ba phép đo khác nhau; số Order theo SKU không cộng thành tổng shop.
Đơn giá bình quân dùng tổng giá trị/tổng units. Coverage đi kèm metric;
absent khác zero, không cộng tiền khác currency, không suy paid GMV/profit/FX.

Return Label v2 theo Q56/Q57: cửa sổ 30 ngày riêng từng phần giao; approved
Return request hoặc successful product Refund có linkage hợp lệ, OR lại
thành một label line. Cohort ngày UTC lần giao cuối phần thực giao khi
remainder đã giao/hủy; maturity và coverage áp dụng cả positive/negative.
Thiếu feed/linkage không thành nhãn âm. Boundary, event/known-time,
cancellation/correction và L01–L17 theo
[return-label-contract.md](return-label-contract.md). Ngưỡng mẫu 30 theo
D17 là cấu hình demo; không suy quantity/amount/reason từ binary label.

Feature lịch sử entity SKU + Storefront; dữ liệu đơn vừa tạo lấy từ request
đã validate. Temporal split phải tách feature cutoff, label cutoff và training
cutoff: nhãn dùng train phải biết trước training cutoff. Các phiên bản của
cùng Order không được rơi sang cả train/test gây leakage. Báo cáo số mẫu mất
do warmup/maturity/missing và so lift/PR-AUC với baseline; score là triage có
điều kiện được giao, cold-start phải abstain. Synthetic evaluation chỉ chứng
minh hành vi trên thế giới mô phỏng, không chứng minh hiệu quả kinh doanh thật.

Audit IR23 bổ sung điều kiện đánh giá: fold theo prediction/order time,
training information cutoff không sau prediction trong cùng đồng hồ.
Delivery cohort chỉ dùng phân tích outcome/maturity; không dùng để cho
model học D60 chấm Order đặt D10 như đã dự đoán tại D10. Replay mô phỏng
giữ actual artifact creation/deploy time riêng, không backdate theo
đồng hồ giả lập để tuyên bố đã serve trong quá khứ. Label revisions phải
được pin ở training cutoff, khác feature cutoff và eval outcome cutoff.

Các guard IR01–IR25 theo [sổ vấn đề](integration-risk-register.md) bổ sung
vào pass criteria G0–G4: input snapshot/offset vector, source incarnation,
linkage uncertainty, quantity authority, bundle/answer pin và clean restore.
Đây là yêu cầu thiết kế/POC, chưa phải evidence đạt.

## Đầu ra nghiệp vụ phải đi hết tới UI

| Quyết định | P1: Gold/mart và nguồn | P2/P3: serving và proof |
|---|---|---|
| D10/D16–D20, Q28 | Fact năm Channel; dim SKU/Storefront có SCD2 từ thay đổi quan sát được; sales/returns/canceled/delivery/category/fulfillment | Overview → SKU → Order Line; số liệu theo currency, tử/mẫu, cutoff và coverage |
| Q29 — basket | Cặp SKU distinct trong Order; tránh tự ghép SKU lặp | Co-purchase có denominator; không khẳng định bổ trợ/causal |
| Q29 — concentration/geography | Tỷ trọng/top SKU theo metric, currency và Storefront; địa lý có nhóm unknown | Tỷ trọng đối chiếu tổng; SKU không có sales không bị kết luận hết cầu |
| Q29 — discount | Tổng discount/gross trên phần nguồn có trường hợp lệ | Coverage và correlation; không gọi là uplift/giá tối ưu |
| Q30 — customer | First-observed/repeat trong từng Storefront có customer ID; guest/missing riêng | Nêu cửa sổ quan sát; không gọi first-ever hoặc hứa đủ năm Channel |
| Q52–Q54/Q58 — nguồn mở rộng | Partial lifecycle, Listing history, pool Seller chung/Channel riêng, movement/reservation/inbound nhỏ | Quantity/stock/money đối soát; capability theo feed; quarantine trước restock |
| Q55/Q59 — kiểm tra SKU | Reason/metric/evidence cùng generation, ba nhóm riêng | Read-only, decomposition/drilldown, incident grouping; rank riêng, không suy tác động kinh doanh |
| Q31 — roadmap còn lại | Tối ưu nhập hàng, forecast model thứ hai, traffic/ads/profit | Q54 chỉ nâng inventory nhỏ vào scope; các nhánh còn lại cần contract riêng |

## Publication, streaming và phục hồi

Trước viết pipeline phục vụ, POC phải chọn và ghi rõ manifest store, atomic
pointer update, physical namespace giữ các generation trong PG/Redis/Iceberg,
và cách request pin xuyên tool/model. Không chỉ thêm cột version vào response
trong khi query vẫn đọc latest. Manifest tối thiểu trỏ dataset, cutoff, code/
contract version, outputs/snapshot IDs, validation report và thời điểm publish.
Model, feature schema, prompt và corpus version đi theo trace/request; có
compatibility check trước đổi model, không ép các lifecycle này thành một mã.

Quy trình cần chứng minh: tạo candidate → ghi các sink → validate → publish
manifest → cho request mới đọc. Request đang chạy giữ generation cũ. Hai
writer đồng thời hoặc retry sau mất ACK không được publish nhầm/thụt version.
Không có distributed transaction ngầm giữa các store. Không còn generation
hợp lệ thì API trả unavailable; không gán missing thành 0 hoặc đọc lẫn version.

Stream dùng event ID/version để replay/dedup; định nghĩa xử lý out-of-order,
watermark, event quá hạn và sửa kết quả. Offline history và online update có
reconciliation tại cùng cutoff; không giả định adapter tự ghi được cả hai.
Live operational view có version/freshness riêng, không âm thầm ghi đè daily
official metrics. Correction vào số chính thức qua generation mới; các
generation cũ được giữ theo retention, không sửa tại chỗ.

Retention phải ràng buộc với generation đang được pin, training lineage và
khả năng replay. Snapshot expiry/orphan cleanup không được xóa dữ liệu còn
được tham chiếu. Redis rebuild từ offline rồi validate trước mở serving;
restore phải bao gồm catalog DB, warehouse, registry/artifact/config cần
thiết, không chỉ GCS. Việc restart không chỉ dựa vào cờ materialized cũ.

## Scenario catalog cho bộ nhỏ

Mỗi case dưới đây cần fixture và expected output được tính độc lập với hàm
đang test; seed cố định, version rõ. Đây là yêu cầu nghiệm thu, **chưa có
test được chạy/đạt** cho bộ mở rộng. ID dùng để nối tới evidence trong ledger.

| ID | Tình huống | Kết quả phải chứng minh |
|---|---|---|
| S01 | Hai shop có cùng order/line ID; cùng SKU xuất hiện hai line | Không gộp nhầm identity; số Order/Line/units đúng |
| S02 | Rerun cùng file; duplicate qua ngày và schema cũ/mới | Bronze truy được mọi bản; Silver current không tăng sai; provenance run tách dataset |
| S03 | Full snapshot A+B → A; delta chỉ cập nhật A; tombstone cả Order | B bị loại chỉ với snapshot đầy đủ/tombstone phù hợp; current Gold/PG đồng bộ; history vẫn truy được |
| S04 | Hai payload khác nhau cùng key và timestamp | Kết quả xác định theo source version/tie-break đã ghi, hoặc conflict được chặn; không chọn ngẫu nhiên |
| S05 | Event cũ đến sau cutoff; correction biết ngày sau | Feature tại cutoff cũ không dùng thông tin tương lai; restatement tạo generation mới |
| S06 | Return/refund trước/đúng/sau 30 ngày; chưa giao; feed thiếu | Nhãn/cohort/denominator theo biên đã định; unknown không thành negative; late correction truy được |
| S07 | JSON lỗi, schema lạ, thiếu partition/file/feed; nguồn rỗng | Không publish đầu ra giả đầy đủ; reject/quarantine có lý do và đối soát count |
| S08 | Cùng seed/config/version; đổi seed/cutoff/catalog | Run tái lập có checksum/manifest đối chiếu; dataset khác không bị trộn; volume đếm raw bytes không cộng bản sao các tầng |
| S09 | PIT retrieval tại hai cutoff; entity mới; TTL boundary | Kết quả khớp oracle độc lập, missing/stale rõ; không dùng snapshot latest thay history |
| S10 | PG xong, Redis mới ghi nửa thì lỗi | Manifest giữ bản tốt; request không đọc candidate; retry không tạo double-count |
| S11 | Publish trong lúc request chạy; hai writer; ACK mất | Request pin một generation; conflict/retry kiểm soát được; version không thụt |
| S12 | Redis mất dữ liệu nhưng registry còn; restart | Rebuild/validate từ offline rồi mở serving; missing không giả làm zero |
| S13 | Stream trùng/đảo thứ tự/quá watermark; offline sink lỗi | Replay/reconcile khớp tại cùng cutoff; event quá hạn được xử lý có dấu vết; daily/live không lẫn |
| S14 | Train/test theo thời gian; label đến muộn; model/schema lệch | Không rò nhãn/feature tương lai; baseline và sample coverage rõ; chặn inference không tương thích |
| S15 | Câu hỏi thiếu dữ liệu/khác currency; tool timeout; injection trong chunk | Agent abstain đúng, citation truy được version, quyền read-only giữ nguyên; lỗi không bịa số |
| S16 | Đổi/xóa chunk; đổi model/prompt; tải đồng thời | Không retrieve chunk đã xóa trong corpus mới; citation cũ truy được theo retention; benchmark ghi chất lượng/latency/RAM |
| S17 | Stop/start, restore DB/catalog, replay sau gián đoạn | Khôi phục version/freshness/key/count; đo thời gian phục hồi và lượng dữ liệu mất so RPO/RTO |
| S18 | Compaction/expiry khi request/training pin snapshot; run chạm budget | Dữ liệu đang tham chiếu còn đọc được; có quy trình dừng compute và ghi chi phí/state cần giữ |

## Evidence và các bất định phải đóng bằng POC

S19–S30 theo Q52–Q59 đã được bổ sung tại
[expanded-scope-design.md](expanded-scope-design.md). Chưa có evidence đạt
cho bộ này; expected label/topology/queue theo Q56–Q59 đã được xác nhận.

Mỗi evidence có: gate/scenario/rubric IDs, commit hoặc checksum code nếu
working tree chưa commit, runtime/dependency pins, config/seed/dataset version,
run/generation, phạm vi Channel/ngày, command, expected/actual, pass/fail,
log/report/screenshot path và giới hạn. Ảnh không có assertion hoặc không
truy được run chưa đủ để đóng gate. Bằng chứng local và Cloud ghi riêng.

| POC trong plan | Đo/kiểm tra tối thiểu | Khi không đạt |
|---|---|---|
| 1 — Feast | PIT S09, parity, TTL/missing, rerun/restart; pin Spark/Python/Feast/Iceberg/catalog | G2 mở; sửa adapter/contract trên bộ nhỏ trước train |
| 2 — Feast vector | Text→chunk→embedding→Feast→retrieval; citation, update/delete; tiếng Việt | Ghi fallback và tác động `mle` 9; không tự coi vector store riêng đạt rubric Feast |
| 3 — CPU LLM | Bộ câu hỏi S15/S16, tool schema, chất lượng, TTFT/latency/RAM/concurrency | Đo model/config nhỏ khác trong phạm vi CPU; chưa đạt thì giữ gate mở |
| 4 — GCS | Raw connector + Iceberg FileIO + catalog + reader thực dùng; restart đọc lại | G1 mở; giữ proof local, chưa tuyên bố Cloud pass |
| 5 — Publication | S10–S12 và tương thích model/schema | G2 mở; không phục vụ candidate |
| 6 — Dual-sink | S13, offline/online parity và recovery | G2 mở; chưa tuyên bố stream feature hoàn tất |
| 7 — GKE | Kubeflow/KServe, lưu artifact, quota và footprint cả nhóm service cần cùng chạy | G3/Cloud gate mở; impact assessment trước thay scope/budget |

Cloud runbook phải ghi region/quota/credit thực do người dùng kiểm tra, chi
phí cả lúc compute dừng, lịch bật nhóm service, retention/backup, cách dừng
và khôi phục, và ngân sách tối đa mỗi phiên. Auth, secret và giới hạn network
phải có trước lần mở endpoint đầu tiên; P4 hoàn thiện proof/ops, không hoãn
bảo vệ endpoint tới cuối. Ngưỡng và giá cần kiểm tra tại thời điểm lập runbook.

## Impact assessment và lượt rà thứ hai

Bổ sung này giữ phạm vi/ngân sách/quyền Cloud đã chốt và số dòng rubric.
Thay đổi là làm rõ dependency order, khôi phục Q29/Q30 trong plan, chuẩn hóa
evidence và ghi điều kiện thực thi. Chưa chọn library/physical schema hoặc
tuyên bố POC pass. Quyết định mới thay scope/backend/SLA cần ghi lý do, các
contract/job/proof chịu tác động và đường replay/migration trước thực thi.

Rà thứ hai đối chiếu bài 2, code nguồn/Bronze/Silver/feature/warehouse,
Q1–Q51, ADR, proposal và hai sheet visible `de`/`mle` của rubric. Bổ sung
các điểm dễ bị bỏ qua: lookback return stream, tie-break không xác định,
label availability khi temporal split, snapshot retention với request pin,
rubric README dòng 2, scope coverage Web API, và ranh giới novel idea DE-N2
với optimization bắt buộc. Xem ledger để nghiệm thu các điểm rubric này.
Các gate còn mở đã được nêu trên; chưa có cơ sở cam kết không cần thay đổi
khi POC cho kết quả mới.
