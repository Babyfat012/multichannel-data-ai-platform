# Sổ vấn đề tích hợp trước implementation — 2026-10-18

Rà bổ sung theo yêu cầu “ra hết vấn đề”, trên scope/policy đã chốt Q1–Q59.
Mục tiêu là counterexample và điều kiện đóng vấn đề, không phải thêm scope
hoặc tuyên bố hệ thống đã chạy đúng. Không thể chứng minh hết lỗi bằng đọc
tài liệu; các điều kiện runtime được giữ mở đến khi có evidence.

## Cách đọc trạng thái

- **Tái hiện**: đã có probe local nhỏ, code hash và kết quả của lỗi ban đầu.
- **Đã sửa cục bộ**: có code và kiểm tra ghi rõ phạm vi; chưa tự đóng cả gate.
- **Có căn cứ code**: đường code hiện tại cho phép failure; chưa tái hiện
  trên stack đầy đủ, không gọi là incident runtime đã xảy ra.
- **Khoảng trống contract**: counterexample chưa được contract xử lý đầy
  đủ; cách xử lý bên dưới là yêu cầu thiết kế trong policy đã chốt.
- **Cần quyết định**: lựa chọn làm đổi ý nghĩa số liệu; chưa tự duyệt.
- **Chờ POC**: phương án giải quyết phải được thử trên runtime đã pin.

P0 ở đây là mức ưu tiên **chặn khóa nguồn/contract lớn**, không phải phase
P0. P1 chặn nghiệm thu thành phần; P2 là cải thiện sau khi các bất biến đã
đúng. “Đã ghi cách xử lý” không phải trạng thái “đã sửa/đóng”.

## A. Nguồn, replay và đầu vào pipeline

| ID / ưu tiên | Vấn đề và counterexample | Căn cứ / trạng thái | Cách xử lý và điều kiện đóng |
|---|---|---|---|
| IR01 / P0 | Resume root của seed7 bằng seed8 giữ Orders cũ, đổi SKU và manifest sang seed8 | **Đã sửa cục bộ**: root/checkpoint/manifest identity, config/code/runtime guard trước mutation; regression fail trước sửa và pass sau sửa. [Repro cũ](evidence/design-audit-small-probes-20261018.json), [bản xử lý](audit-remediation-20261018.md) | Case đổi config khi resume đã chặn; legacy root từ chối tự adopt. Cùng identity resume byte-exact, explicit restart tái sinh. Chưa có distributed writer lock, file-integrity scan toàn bộ hoặc schema/feed v2; G0 tổng vẫn mở |
| IR02 / P0 | DP2 đọc Amazon snapshot A, sau đó Bronze bị thay, rồi đọc Walmart B; output có commit atomic nhưng input đã trộn | **Có căn cứ code**: DAG/job truyền table name, đọc current; input fencing chưa có | Mỗi run có input manifest: exact snapshot/file set/source DB snapshot và cutoff. Validate đọc cùng input vector; retry không tự resolve latest. G1/G2: đổi source giữa tasks không đổi candidate input hoặc fail rõ |
| IR03 / P0 | CDC ingest và validate tự chốt `latest` ở hai thời điểm; topic tái tạo có offset cũ nhưng dữ liệu khác | **Đã sửa cục bộ, chờ POC**: frozen range POSIX theo run, cluster/topic UUID epoch, DAG truyền chung boundary; helper unit tests đạt | Cần Kafka/Spark/Iceberg proof producer phát thêm, reset, retention, retry và table v2. UUID kiểm trước/sau không thay broker/admin fencing lúc đọc; không cho recreate topic đồng thời. Chưa đóng G1/G2 |
| IR04 / P0 | 365 ngày lịch sử + 30 ngày stream không làm line gần cuối đủ nhãn: còn delivery, approval/refund và observation delay | **Khoảng trống contract**, không chỉ lookback 20 ngày | Tách Order creation interval, lifecycle follow-up interval và observation closeout; chỉ công bố mature cohort thật đủ. G0/G2: Order ở ngày cuối vẫn có events theo policy hoặc được ghi censored/immature, không bù label0 |
| IR05 / P0 | Cùng seed không đảm bảo cùng world khi đổi buyer-pool size, history, config hoặc thêm lần rút RNG; comparison v1/v2 có thể đổi cả nguồn | **Có căn cứ code**: `Simulator._build_buyer_pool`, RNG theo ngày; chưa có modular world identity mới | Dataset world/config/code pin; stable entity/event IDs và substream cho module; cùng-world comparison có envelope rõ. G0: renderer/module/replay không tự đổi outcome của entity đã cam kết; v1/v2 comparison chỉ trên thế giới và eligible scope có thể đối chiếu |

## B. Nhãn, thời gian và population của model

| ID / ưu tiên | Vấn đề và counterexample | Căn cứ / trạng thái | Cách xử lý và điều kiện đóng |
|---|---|---|---|
| IR06 / P0 | Label0 biết ở C1, correction thành1 ở C2, sau sửa delivery thành immature ở C3; một `label_available_at` không lưu được cả ba | **Khoảng trống contract** v2: quy tắc correction đã có nhưng schema history chưa có | Label revisions có observed interval, reason, input references, maturity/state và version. Feature cutoff, training cutoff và evaluation outcome cutoff tách riêng. G2: query lại C1/C2/C3 đúng, model train ở C1 không dùng revision C2 |
| IR07 / P0 | Refund có Order ID, không map line: nếu bỏ unmatched khỏi label join, hai line có thể đều bị gán0 | **Khoảng trống contract**, mở rộng nguyên tắc unknown của Q57 | Truy uncertainty tới tập candidate line/allocations có thể bị ảnh hưởng; giữ scope/money unresolved, không trải amount hoặc positive tùy ý. G2: không line nào trong affected scope được chứng minh âm bằng việc inner join làm event biến mất |
| IR08 / P0 | Một shipment tách thành hai record cùng lượng/thời gian nhưng simulator rút Return độc lập mỗi record, OR label làm risk đổi | **Khoảng trống contract**; p0.1 bị đổi thành `1−0.9²=0.19` chỉ do cách biểu diễn | Outcome sinh theo cơ chế latent/exposure ổn định, renderer/split record không rút lại outcome. G0/G2: split/merge representation của cùng physical world không đổi label/quantity, future shipment count không thành feature lúc đặt |
| IR09 / P0 | Label v2 cần actual delivery/approval/product refund/allocation; native feed thiếu làm cohort huấn luyện lệch hoặc không có mẫu | **Q61 đã chốt, thiết kế chưa implement**; [source contract](source-contract-order-lifecycle.md) có matrix/authority/known-time/coverage | Nguồn Seller operational giả lập có producer/bridge riêng. G0/G2: nguồn/bridge truy được; chưa đủ thì abstain/unknown, không quảng bá performance toàn năm Channel từ subset |
| IR10 / P1 | “As-of ngày X” có thể là metric business window hoặc thông tin biết tại X; online latest không trả được historical request chỉ nhờ version param | **Khoảng trống API/adapter**, **chờ POC** | Request tách business window, knowledge cutoff và generation/view mode; manifest phải khớp. Historical route đọc source/history được pin, online route chỉ phục vụ phạm vi đã materialize; tổ hợp không hỗ trợ bị từ chối. G2/G3: restated và as-known khác đúng, không fallback latest |

## C. Số lượng, Listing, Inventory và metric

| ID / ưu tiên | Vấn đề và counterexample | Căn cứ / trạng thái | Cách xử lý và điều kiện đóng |
|---|---|---|---|
| IR11 / P0 | Native quantity hiện hành có thể đã trừ refund/removed; coi giảm quantity là cancel sẽ hủy cả hàng từng giao | **Khoảng trống source normalization** | Canonical ordered, canceled-before-ship, shipped/delivered, returned, refunded có meaning riêng; giữ native raw và reconcile. Snapshot disappearance là correction/state projection, không tự là physical cancellation. G0/G1: ship2→refund1 không thành cancel1 và vẫn giữ delivered2 |
| IR12 / P0 | Một line 3 units ×100, hủy1: sales vẫn300 hay200? “Tách cancelled” cấp Order chưa quyết định case này | **Q60 đã chốt, metric mới chưa implement** | Giữ ordered300/canceled100 riêng; default headline200, product Refund100 riêng không trừ tiếp. [ADR-0011](adr/0011-sales-basis-and-observable-seller-feeds.md). G1/G3: quantity/amount/discount và decomposition cùng currency/cutoff có expected thống nhất |
| IR13 / P0 | Listing đổi SKU/pool mapping khiến Order cũ bị re-key hoặc reservation đang mở chuyển sang pool khác khi join current | **Khoảng trống binding identity** | Business line/allocation/reservation identity bất biến; binding tại operation giữ version; effective mapping change khác correction. G0/G1: đổi mapping không tạo Order Line mới hoặc move stock; correction reattribute có audit/reconciliation |
| IR14 / P1 | Tử số Return group theo category hiện tại, mẫu số theo category lúc bán; tỷ lệ có thể >1 hoặc gán sai nhóm | **Khoảng trống dimension role** | Chốt role sale-time SKU/attributes cho các fact cần so cùng cohort; view current attributes phải gắn nhãn riêng. G1/G2: numerator là subset của denominator cùng dimension/eligible/cutoff; correction label/mapping recompute cùng generation |
| IR15 / P0 | Pool available10 được hai Storefront đọc thành mỗi nơi “có thể cam kết10”; API vô tình hứa ATP20 | **Khoảng trống output semantics** | Hiện stock là pool shared quantity, Storefront eligibility không phải quota. Reservation/pool allocation theo source, không tự chia tồn; UI group issue/pool. G1/G3: cùng pool không cộng lặp ở tổng Seller hoặc trình bày như availability độc lập |
| IR16 / P1 | Reservation/receipt/refund correction đến lệch thứ tự; replay đã dedup nhưng trạng thái cuối vẫn sai vì chọn timestamp mới nhất | **Khoảng trống ordering/reconciliation**, Q58 chưa triển khai | Stable operation + revision/type, source ordering có contract; pending causal reference trước khi apply/validate. G1/G2: release-before-reserve/cancel-after-ship/snapshot mismatch không bị clamp hoặc biến thành success im lặng |

## D. Công bố, phục hồi và evidence

| ID / ưu tiên | Vấn đề và counterexample | Căn cứ / trạng thái | Cách xử lý và điều kiện đóng |
|---|---|---|---|
| IR17 / P0 | Tool1 đọc g1, refresh, tool2 đọc g2; mỗi HTTP request đúng nhưng một câu trả lời agent trộn số | **Khoảng trống answer/turn boundary** | Coordinator pin generation cho toàn answer/turn và drilldown context; tool results echo identity/view, cache key đủ scope/version. Compare hai generation phải là tool/result tường minh. G3: publish giữa hai tool vẫn không lẫn số |
| IR18 / P0 | Model tag production mới nhưng image còn model cũ, hoặc label v2/schema mới gặp model v1; dữ liệu pin đúng mà inference sai | **Khoảng trống activation bundle** | Model image/artifact digest, feature schema/service, label version, corpus/prompt/tool compatibility ghi trong serving bundle; tag không là identity. G2/G3: mismatch bị chặn trước serving và rollback đọc đúng artifact |
| IR19 / P0 | Restore PG manifest g2 nhưng Redis/catalog chỉ còn g1; API ready nhờ process sống rồi trả nhầm/latest | **Khoảng trống recovery epoch**, **chờ POC** | Readiness kiểm dependency vector và restore epoch, không chỉ health process; writer fencing sau restore; rebuild trước mở traffic. G4: restore các store lệch thời điểm không thể publish/serve nhãn g2 bằng dữ liệu g1 |
| IR20 / P0 | Một feed không có capability làm mọi generation fail mãi, hoặc bỏ output khỏi manifest tùy run để lách validation | **Khoảng trống publication requirements** | Manifest profile version khai báo trước required/optional output + capability/coverage; required fail giữ bản tốt, optional thiếu được báo unavailable. G2/G3: một thiếu output không bị silently zero/drop hoặc thay requirement sau khi đo |
| IR21 / P1 | Validator đối chiếu cùng mutable latest/transform hoặc physical file count nên có thể cùng sai với job | **Đã sửa một phần**: feature validator dùng logical count, missing/extra keys và source-created-time; isolated Spark regression đạt | Còn independent frozen input/oracle, payload/full PIT và Iceberg delete-file integration. Metadata counts chỉ kiểm vật lý. G1/G2: source đổi giữa run và sai payload phải bị phát hiện; không đóng bằng các structural checks mới |
| IR22 / P1 | Cùng source key/run nhưng rule/config/prompt đổi giữa build/validate/serve làm evidence không tái lập | **Khoảng trống config activation** | Config/rule/schema/contract digests trong input/output/evidence/bundle; update tạo revision, không đổi dưới ID cũ. G0–G4: mở lại evidence dùng đúng phiên bản và biết artifact nào expired |
| IR23 / P0 | Model học thông tin đến D60 được chấm như đã dự đoán Order đặt D10 nhưng giao D65; chia theo delivery cohort vẫn có vẻ hợp lệ | **Khoảng trống evaluation protocol**; feature PIT đúng chưa đủ | Fold theo prediction/order-time; training information cutoff và model-ready trong cùng clock không sau prediction. Replay giữ actual artifact creation riêng, không backdate để giả đã deploy. G2: fixture D10/D60/D65 bị loại khỏi forward-at-order proof hoặc dùng model đủ điều kiện ở D10 |
| IR24 / P1 | Giữ nguyên return intents nhưng approval chậm qua deadline làm label rate giảm; agent kể là chất lượng SKU tốt lên | **Hệ quả target v2 đã chốt**, không cần đổi Q57 | Hiện target/policy version và coverage/approval-delay diagnostics; không gọi label rate là defect/physical-return rate. G2/G3: stress chỉ đổi approval-delay vẫn giữ diễn giải đúng, không khẳng định cải thiện sản phẩm |
| IR25 / P1 | Một owner gọi tool read-only nhưng query quá rộng/credential quá mạnh; nested retry tăng scan dù gateway rate limit đạt | **Khoảng trống enforcement nhiều hop**, chi tiết SR07 | Views/roles/tool allowlist và query scope bounds; deadline/work/cancel budget xuyên hop, credential riêng đúng audience. G3/G4: request bị hủy không giữ công việc vô hạn, tool không đọc control-plane/secret hoặc vượt scope |

## Quyết định bổ sung đã được chấp thuận

**Q60 — sales khi partial cancellation.** Q56–Q59 không tự quyết định giá
trị hàng đặt của phần đã hủy. Ví dụ Order Line gốc 3×100, hủy1 trước ship,
giao2, sau đó Refund1: đề xuất hiển thị original ordered300, canceled100,
current non-canceled200 và product refund100 riêng. Default sales/trend
dùng200, chưa giảm tiếp thành100 chỉ vì Refund. Người dùng yêu cầu “Hiểu,
xử lý” theo đề xuất; ADR-0011 bổ sung phần partial cancellation của D16.
Đây là policy hiện hành; công thức mới chưa được implement trong Gold/API.

**Q61 — nguồn bổ sung để nhãn v2 có thể quan sát được.** Đề xuất mô phỏng
thêm Seller OMS/WMS/carrier/post-sales feeds với producer/authority, native
identity bridge và observation delay tường minh; chúng đi qua Bronze/Silver,
không lấy biến ẩn gắn thẳng vào feature hoặc giả là trường của native Orders.
Phạm vi source mới nhỏ trước; không mặc định có mọi feed cho mọi pool do
Channel quản lý. Phương án khác là giữ nguồn hiện có và công bố model chỉ
phục vụ capability đủ, các Channel còn lại unsupported/unknown. Đây là
lựa chọn modeling nguồn/phạm vi sản phẩm, không phải kết nối API thật hay
quyền thao tác Cloud. Người dùng đã cho xử lý theo đề xuất Q61; chọn nguồn
Seller giả lập nhỏ, vẫn unsupported/unknown cho scope không đủ capability.

Các lựa chọn kỹ thuật như hash, physical namespace, CAS store, query layout,
idempotence key hoặc retry implementation không cần hỏi thành câu nghiệp vụ
mới. Chúng có thể được thiết kế/thử trong các gate đã chốt. Nếu một đề xuất
đòi đổi target/ngân sách/quyền hay scope thì phải quay lại impact assessment.

## Evidence và thứ tự đóng vấn đề

1. G0: IR01/IR04/IR05/IR08/IR09/IR11/IR13 cùng Q60/Q61; khóa source envelope,
   identity, quantity và contract mẫu trước dataset lớn.
2. G1/G2: IR02/IR03/IR06/IR07/IR10/IR14/IR16/IR20/IR21/IR23/IR24; fixture frozen
   input, PIT/label revisions, snapshot/offset range và publication.
3. G3/G4: IR15/IR17/IR18/IR19/IR22/IR25; answer pin, compatibility bundle và
   clean restore. IR20 profile/IR22 config được thiết kế từ G0, không đợi cuối.

Các note độc lập: [temporal/labels](audit-temporal-labels-20261018.md),
[cross-domain](audit-cross-domain-20261018.md),
[serving/recovery](audit-serving-recovery-20261018.md). Chúng bổ sung bằng
chứng/chi tiết và có thể nêu phương án khác; chỉ Q đã được xác nhận và
contract hiện hành là policy, không biến mọi đề xuất audit thành scope mới.

Case resume đổi config của IR01 đã sửa và regression đạt; IR03/IR21 mới có
code/kiểm tra cục bộ, vẫn chờ integration. [Bản xử lý](audit-remediation-20261018.md)
ghi rõ giới hạn. [Source contract](source-contract-order-lifecycle.md) cụ thể hóa nhóm
nguồn/quantity/uncertainty; [publication contract](run-publication-contract.md)
cụ thể hóa input pin/label revisions/activation/restore. Các IR còn lại
chưa đóng runtime; Q60/Q61 không còn là câu hỏi pending. Không dùng kết quả
kiểm link/Markdown làm evidence runtime hoặc coi G0–G4 đã qua.
