# Audit giao nhau giữa Order, Listing, inventory và metric

Ghi chú sau xử lý cùng ngày: tài liệu này giữ phân tích trước sửa và trước
Q60/Q61. Trạng thái hiện hành ở [bản xử lý](audit-remediation-20261018.md)
và [sổ IR](integration-risk-register.md); các đoạn “chưa chọn” phía dưới
không mở lại quyết định đã được người dùng chấp thuận.

Ngày 2026-10-18. Đối chiếu Q1–Q59 đã được người dùng chấp thuận. Đây là
adversarial design review trước triển khai: các ví dụ dưới đây là oracle đề
xuất để viết fixture, **chưa phải test đã chạy hoặc runtime proof**. Không
sửa pipeline, sinh nguồn hoặc thực thi Cloud. Chỉ một lựa chọn nghiệp vụ
mới được tách ở cuối; các sửa contract còn lại thực thi policy đã có.

Đã đọc [readiness](implementation-readiness.md),
[expanded design](expanded-scope-design.md),
[label v2](return-label-contract.md),
[research edge cases trước](research-scope-edge-cases-20261018.md),
[D16–D20](de-expansion-review.md),
[Q34–Q36](de-discovery-risk-review.md), cùng các file nguồn dẫn cụ thể bên
dưới. Những rủi ro như fan-out fact, hai clocks, shared pool và duplicate
reservation đã được ghi trước; phần mới của lượt này là contract tại chỗ
các projection chạm nhau, không đếm lại chúng như phát hiện mới độc lập.

Mức **P0**: chặn contract/generator G0 vì làm sai identity hoặc mất khả năng
replay. **P1**: chặn gate dữ liệu/serving liên quan vì có thể công bố kết
quả sai. Đây là mức ưu tiên thiết kế, không khẳng định có incident production.

## CD01 — Line identity không được phụ thuộc mapping có thể sửa

**P0 · G0/G1/G2.** Q34/Q53 đã yêu cầu SKU nội bộ ổn định và mapping lịch
sử, nhưng cần thêm invariant cho mọi bridge. Ngày D1 line L bán Listing X,
được map SKU A. D5 có Return R trỏ L. D8 correction xác nhận mapping lúc
bán phải là SKU B. Nếu line key được băm cả SKU hoặc Listing version,
restatement tạo line L2 và R vẫn trỏ L1; line count/label/sample thay đổi sai.
Native SKU text còn có thể được tái sử dụng cho sản phẩm khác; nó không là
business line key.

**Cách đóng trong scope:** business key của line chỉ gồm dataset/source
namespace + account/shop + các native order/line IDs bắt buộc. Mapping là
quan hệ có version, tách khỏi identity. Mọi shipment/Return/Refund phải
trỏ business key này. Sale attribution có effective/known time và nguồn
bằng chứng; native SKU text không tự tạo internal SKU equivalence. Native
line bị source thay bằng một ID khác cần explicit replacement/correction
link hoặc unknown/conflict, không suy từ SKU + quantity.

Gold hiện băm Channel/order/secondary-order/line, không chứa SKU; đây là
điểm nên giữ. Nó chưa có dataset/account namespace của kiến trúc mới, và
Amazon version key hiện chỉ dựa order + timestamp, cần migrate có kiểm
chứng; không có cơ sở coi các khóa hiện tại là đủ cho Q34.
[Gold key](../spark_jobs/gold/build_order_snapshot_lines.py),
[Amazon version key](../spark_jobs/lib/order_version_key.py).

**Oracle:** hai shop có cùng native IDs tạo hai line; mapping A→B do correction
không đổi line ID, không nhân sample; R vẫn liên kết; generation trước/sau
có attribution khác nhưng tổng line/quantity/money không đổi. Một SKU text
tái sử dụng không gộp lịch sử hai internal SKUs. Test null versus absent key
components và các ID chứa dấu phân cách để kiểm canonical serialization.

## CD02 — Snapshot xóa line không phải sự kiện hủy nghiệp vụ

**P0 · G0/G1.** S03 đã phân biệt full snapshot/delta. Còn thiếu ngữ nghĩa
liên feed: O có A+B; full snapshot mới chỉ còn A nhưng B đã có shipment và
Return ở feed khác. Xóa B khỏi current Order projection không chứng minh
B bị hủy trước giao, không tự release reservation, hoàn tiền hoặc xóa
delivery/label history. Có thể là source correction xác nhận B chưa từng
tồn tại; cũng có thể là edit/removal với sự kiện riêng. Hai cách có tác động
khác lên conservation và nhãn.

**Cách đóng trong scope:** giữ tombstone/removal ở projection nguồn, khác
canonical cancel event. Adapter phải ghi snapshot scope + completeness +
removed entity semantics. Chỉ feed/correction có ý nghĩa xác định mới được
phát sinh canonical cancel/invalidation. Khi các facts còn sống mâu thuẫn
với removal mà không có correction link, đánh conflict ở entity liên quan;
không cascade-delete evidence. Gate publication phải kiểm dangling links
và quan hệ bị vô hiệu trong cùng candidate generation.

**Oracle:** (1) B chưa ship, removal không có reason: current source B absent,
cancel/release unknown; (2) correction xác nhận B không có thật: generation
mới loại B khỏi metric cùng dependent invalidation, lịch sử cũ còn đọc;
(3) B đã ship: removal đơn độc không biến shipment thành cancel; (4) delta
không nhắc B: không thay B. Không tự chế stock adjustment để vượt gate.

## CD03 — Quantity hiện tại ở API không phải lượng đã đặt cho phương trình lifecycle

**P0 · G0/G1/G2.** Native `currentQuantity` có thể đã loại refunded/removed
units trong khi `quantity` vẫn chứa chúng. Shopify 2026-10 công bố đúng sự
khác biệt này; đây là bằng chứng một renderer/adapter không thể chỉ chọn
cột có tên nghe giống canonical quantity.
[Shopify LineItem](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/LineItem).

Ví dụ đặt 3, giao 3, hoàn tiền 1: canonical ordered=3, delivered=3; native
currentQuantity có thể là 2. Nếu dùng 2 làm ordered, invariant delivered≤ordered
báo giả conflict hoặc transform sửa sai delivered về 2. Approved Return có
thể vẫn đúng 1 line; refund không viết lại lượng từng giao thành 2.

**Cách đóng trong scope:** capability/schema mỗi quantity khai báo rõ
`ordered_original`, `ordered_corrected`, `canceled_unshipped`, cumulative
shipped/delivered và current native projection. Chọn chỉ một basis quantity
cho conservation, lưu native quantity riêng; thiếu basis thì unknown. Cần
phân biệt correction quantity thật với projection giảm do refund. Code
model hiện chỉ có một `quantity`; không đủ cho hợp đồng mới.
[OrderLine hiện tại](../generator/core/model.py),
[Shopify renderer](../generator/channels/shopify.py).

**Oracle:** refund1 không giảm ordered/delivered3; cancel trước giao1 làm
ordered3=canceled1+delivered2; source correction ordered3→2 khi delivered3
phải conflict hoặc có dependent correction, không clamp. Mỗi native field
có ít nhất một fixture chỉ ra thứ không được suy từ nó.

## CD04 — Order status đơn không đủ để tách canceled cho sales mới

**P1 · G0/G1/G3; có một lựa chọn nghiệp vụ chưa duyệt.** D16 quy định sales
ngày đặt và canceled tách riêng. Trước Q52 hầu hết cancel là toàn Order;
hiện một line có thể đặt3, giao1, hủy2, Order vẫn delivered/completed. Filter
`order_status != cancelled` sẽ coi đủ3 là non-canceled; filter loại line có
bất kỳ cancellation lại loại cả unit đã giao. Hai cách đều cần tránh.

**Phần làm được theo policy hiện tại:** giữ các thành phần original/corrected
ordered, canceled trước giao và phần còn lại, amount theo cùng basis, không
suy cancel từ refund/return hoặc currentQuantity. API không được để một
trường `sales` chứa hỗn hợp ordered-gross và cancellation-adjusted theo từng
Channel. D16 chỉ xác định ngày đặt/currency, chưa đủ chốt ý nghĩa headline
sales khi partial cancel. Đề xuất và fixture quyết định ở phần cuối.

## CD05 — Money allocation phải giữ basis qua cancellation và refund

**P1 · G0/G1/G3.** Đối soát allocated+unmatched đã có, nhưng vẫn có thể
đối soát tổng mà đổi sai lịch sử giữa các trạng thái. O có ba line 0,01 USD,
order discount 0,01 USD. Làm tròn từng tỷ lệ độc lập thành 0 sẽ mất cent;
gán cent theo thứ tự DataFrame làm rerun đổi line được giảm. Nếu một line
bị hủy rồi chia lại discount bằng current quantities, return của line cũ
có thể trỏ một giá trị khác dù không có correction amount từ nguồn.

**Cách đóng trong scope:** amounts dùng fixed precision/minor units theo
currency; phân biệt unit/line/order, tax/shipping/product, shop currency và
presentment currency. Ưu tiên line allocation gốc nếu nguồn có. Nếu simulator
cần phân bổ order discount, contract chọn quy tắc deterministic (chẳng hạn
largest remainder + stable line-key tie-break), basis và version; unknown
component không coi bằng 0. Cancel/refund có allocation riêng tham chiếu
sale allocation version; không tái chia sale amount do một phép filter.
Source correction explicit mới được restate khoản gốc theo Q36. Không tự
mở settlement/profit hoặc coi tiền trả về bằng qty × giá hiện tại.

Shopify phân biệt original total, discounted totals bao gồm/excluding
removed/refunded quantities, và giá trị có/không gồm discount cấp Order.
Refund line còn có subtotal và tax riêng, nên map amount phải có nghĩa field.
[LineItem fields](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/LineItem),
[RefundLineItem](https://shopify.dev/docs/api/admin-graphql/2026-10/objects/RefundLineItem).

**Oracle:** ba line 0,01 nhận đúng tổng discount 0,01 với cent thuộc cùng
stable key khi đảo thứ tự input; canceled1 không tự phân bổ lại discount
của line khác; source revised discount tạo generation mới; VND và USD
không chung decimal rule hoặc cộng tổng; missing tax/discount giữ coverage.
Phân rã sales phải dùng đúng quantity/amount basis đã chốt ở CD04, nếu không
volume/price effects vẫn cộng đúng delta nhưng kể sai ý nghĩa.

## CD06 — Cohort ngày giao không đổi dimension của sản phẩm đã bán

**P1 · G1/G2/G3.** Q56 chọn cohort lần giao cuối; Q53 chọn mapping/thuộc
tính tại lúc bán. D1 bán SKU A/category C1, D5 chuyển Listing sang SKU B,
D10 đổi category A sang C2, D20 giao cuối và D25 Return. Group label
denominator theo Listing/SKU/category current ở D20 hoặc D25 sẽ chuyển line
và positive sang B/C2. Ghép numerator theo lúc trả với denominator theo lúc
bán có thể làm một nhóm có positive mà không có eligible line.

**Cách đóng trong scope:** return-rate numerator và denominator bắt đầu từ
cùng set mature eligible business line IDs và sale attribution đã version;
cohort_date là trục thời gian của outcome, không là timestamp ngầm cho mọi
dim join. Order geography/fulfillment cũng cần định nghĩa observed-at-sale
hoặc actual-outcome field riêng theo metric contract. Current Listing/stock
chỉ dùng cho queue availability hiện tại; không viết lại feature as-of cũ.
Correction mapping sale-time tạo generation mới và phải cập nhật đồng bộ
sales/labels/queue attribution theo dependency graph.

**Oracle:** case trên vẫn A/C1 ở numerator/denominator cohort D20; B không
được nhận return của A. Correction D30 xác nhận sale-time là B thì current
restatement đổi cả hai trong generation mới, as-known D26 giữ A. Sum nhóm
bao gồm unknown bằng tổng toàn shop; thiếu mapping không loại khỏi tổng
shop rồi tuyên bố full coverage. Category reclassification hiện tại có thể
là một phép xem riêng nếu muốn, không âm thầm thay metric đã chốt.

## CD07 — Allocation đang mở không được chạy theo mapping pool mới

**P0 · G0/G1.** Q58 có mapping pool history và chưa mở transfer network.
Tại D1 reserve2 từ Seller pool P1, D2 Storefront đổi eligible pool sang P2,
D3 ship1, D4 cancel1. Nếu shipment/cancel join current pool mapping, P2 bị
trừ1 hoặc release1 dù chưa từng giữ; P1 vẫn giữ2. Tổng shop có thể gần đúng
trong khi từng pool sai. Điều tương tự xảy ra nếu Listing correction làm
allocation cũ được gán SKU khác.

**Cách đóng trong scope:** reservation/allocation giữ stock item identity,
pool/location và link nguồn tại operation, không resolve lại bằng current
Listing. Thay eligibility chỉ ảnh hưởng allocation mới. Nếu source thực sự
reassign reservation thì cần explicit linked release/reserve operation được
quan sát; ở lát chưa hỗ trợ chuyển kho, giữ conflict cho reassignment không
đủ bằng chứng. Return receipt có location thực nhận riêng, không mặc định
đảo đúng outbound movement nếu hàng nhận nơi khác. Không tự thêm service
hoặc mở transfer optimizer.

**Oracle:** D3/D4 tiêu thụ/release đúng P1, P2 không đổi; replay không lặp;
current eligibility vẫn P2 cho order mới. Native SKU rename/correction không
tự chuyển physical stock. Return receipt không có stock-location linkage
không tăng một pool được đoán theo Storefront.

## CD08 — Native reservation state và canonical order hold cần mapping độc quyền

**P1 · G0/G1.** Duplicate operation ID S21/S29 chưa đủ nếu hai feed dùng
hai ID khác nhau cho cùng lượng. Inventory on-hand10, native committed3,
native reserved2 (hold khác), available5; Order feed cũng có allocation3.
Nếu `native_reserved→canonical_reserved` và cộng thêm order3 rồi vẫn trừ
committed3 lần nữa, available thành2. Nếu bỏ reserved native vì “Order đã
giữ” thì available thành7. Cả hai đều sai.

Shopify mô tả committed/reserved là state riêng và on-hand là tổng các state
không giao nhau; committed được quản lý qua flow nghiệp vụ của nền tảng.
Không suy native reserved chính là lượng của Order feed.
[Inventory states](https://shopify.dev/docs/apps/build/orders-fulfillment/inventory-management-apps/manage-quantities-states).

**Cách đóng trong scope:** adapter có bảng mapping state→canonical partition,
authority và correlation evidence theo capability. Order allocation là
operational explanation của committed3, không phải physical hold cộng thêm.
Nếu không link được hai nguồn thì không tính một “available hợp nhất” bằng
cộng/trừ tùy ý; giữ quantity observed và reconciliation unresolved. Canonical
tên `reserved` trong trace hiện chỉ là order hold; phải ghi rõ semantics để
không biến cùng tên native thành equivalence.

**Oracle:** case10=available5+order-held3+other-unavailable2; hai feed với ID
khác nhưng link cùng order operation không nhân3. Không có correlation thì
coverage/linkage incomplete thay vì giữ thêm3. Unknown native state không
bị vứt đi để công thức vẫn chạy.

## CD09 — Publish một vector trạng thái operation, không từng số dư rời

**P1 · G1/G2.** Atomic manifest toàn platform đã có, nhưng validator cũng
phải nhìn operation completeness bên trong candidate. Trước ship có
on-hand10/hold3/available7; ship1 đúng là9/2/7. Nhận riêng movement −1 trước
reservation consume −1 tạo9/3/6; consume trước movement tạo10/2/8. Cả hai
có thể trông hợp lệ từng cột, nhưng là trạng thái trung gian không thể công
bố như stock reconciled. Cancel/ship cạnh tranh cùng reservation revision
còn có thể release lượng đã ship dù không duplicate event ID.

**Cách đóng trong scope:** mỗi operation canonical có danh sách effects cần
có, semantic version/causation, completeness và expected prior revision.
Candidate snapshot lấy tập effects hoàn chỉnh hoặc gắn unresolved; release
và consume chỉ trừ remaining hold hợp lệ của đúng allocation. Idempotency
key không tự đảm bảo kiểm soát hai operation khác ID dùng cùng remaining
quantity. API vẫn đọc manifest đã validate; live provisional phải nói rõ.

**Oracle:** đảo thứ tự hai effects không publish6/8 như số reconciled; khi
đủ effects ra7. Reserve3→ship1→cancel3 báo conflict cho cancel vượt remaining2,
không release3. Hai Storefront tranh unit cuối chỉ một reservation accepted
trong simulator authority; nguồn partial/missing trả unresolved thay vì âm
hoặc clamp0. Đây là kiểm snapshot/operation, không mở checkout transactional.

## CD10 — Shared pool availability không cộng được qua SKU cards/Storefront

**P1 · G1/G3.** Q58/Q59 đã cấm nhân stock/incident theo card. Cần đưa ranh
giới này vào API aggregation thay vì chỉ câu chữ UI. Pool P available5,
Storefront X/Y đều eligible. Query card X hiện5 và card Y hiện5; overview
`sum(cards.available)` vẫn ra10 dù incident có cùng issue ID. Một line
request hỏi “X có đủ bán5 cùng lúc Y bán5 không?” không được suy có từ
hai số5. Read-only Q55 chưa bao gồm ATP hoặc giữ stock.

**Cách đóng trong scope:** stock resource theo stock-item/pool/location,
Storefront mapping là eligibility edge. Response mang `quantity_scope`,
`pool_id`, shared indicator, cutoff/reconciled status, không giả quota
Storefront. Tổng stock dùng distinct physical resource keys và chỉ cộng
partition không chồng lấp. Sales vẫn tính theo Storefront của order;
pool issue có affected Storefront list và một incident ID, không phân bổ
mất sales/stock exposure theo tỷ lệ sales tự nghĩ ra. Agent tool schema phải
giữ scope này khi tóm tắt số và có case chống lời hứa khả dụng.

**Oracle:** overview physical5, hai cards mỗi card hiển thị “pool chung5”;
hai cards không tạo available10 hoặc cam kết đồng thời10. Một eligibility
edge hết hiệu lực không làm biến mất stock của pool ở Seller total, cũng
không khẳng định Listing buyable chỉ vì stock dương.

## Một lựa chọn nghiệp vụ mới thực sự cần chốt

**BC01 — headline sales sau partial cancellation.** Đây không phải yêu cầu
chọn SQL/library. D16 “tách canceled” chưa chốt phép đo khi canceled quantity
và delivered quantity cùng nằm trong một line. Đề xuất để người dùng duyệt:

- Giữ giá trị/units hàng đặt gốc (hoặc được sửa sai hợp lệ) theo ngày đặt làm
  baseline; trình bày canceled trước giao như thành phần riêng.
- Nếu cần headline sau hủy, đặt tên rõ **giá trị hàng đặt còn lại sau hủy**,
  lấy baseline trừ allocation hủy trước giao; không trừ product Refund hay
  physical Return lần nữa. Nó vẫn chưa phải paid GMV hoặc revenue kế toán.
- Partial line ordered3, unit100, line discount30 được phân10/unit: baseline
  ordered3/value270; cancel2/value180; remaining1/value90. Refund90 về sau
  không đổi remaining sale90; xuất ở metric refund riêng. Cancellation
  thuộc ngày đặt khi restate metric này theo Q36, lịch sử đã publish giữ lại.
- API/UI/queue decomposition phải công bố đang dùng baseline hay remaining;
  không tự chọn headline cho người dùng trong research note này.

Nếu người dùng chỉ chọn baseline headline, vẫn cần quantity/amount hủy để
đối soát và breakdown; không phát sinh scope finance/settlement. Nếu chưa
chốt, G0 phải để metric contract mở, không triển khai label `sales` mơ hồ.

## Evidence cần thêm vào gate và điểm dừng của lượt quét

| Gate | Bổ sung oracle liên quan | Kết quả mong đợi trước tiếp bước |
|---|---|---|
| G0 | CD01–CD05, CD07–CD08; BC01 nếu headline bị ảnh hưởng | Physical schema/capability map giữ identity, basis quantity/amount và semantics removal/hold; quyết định headline không bị tự duyệt |
| G1 | CD01–CD10 | Key/FK/quantity/money conservation trên snapshot hoàn chỉnh; conflicts không thành correction ngầm; tổng pool không qua sum cards |
| G2 | CD01/CD03/CD06/CD09 | Label/features/materialization cùng attribution/cutoff hợp lệ; operation chưa đủ không thành reconciled publication |
| G3 | CD04–CD06/CD10 | Tử/mẫu cùng grain/dim; metric definition rõ; agent không hứa ATP hoặc kể giả causal/finance |

CD01–CD10 là catalog audit cục bộ, **không tự cấp số S31 trở đi** hoặc tuyên
bố đã mở thêm gate; khi tổng hợp, ánh xạ vào S01/S03/S19–S30 hoặc mở scenario
chung có chủ sở hữu. Tài liệu có thể khóa invariant và expected output ngay;
các khả năng native feed/physical adapter cần chứng cứ bộ nhỏ trước full-scale.
Không có số lượng scenario hoặc một lượt audit nào chứng minh đã “hết mọi
vấn đề”; tiêu chí dừng discovery có ích là không còn business choice làm đổi
identity/grain/nhãn/topology, contract có counterexample và POC có fail gates.
