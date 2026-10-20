# Thiết kế và kiểm tra hệ quả Q52–Q55

Ngày 2026-10-18. Người dùng đã trả lời Q52 đồng ý, Q53 đồng ý, Q54 được,
Q55 đồng ý và yêu cầu tiếp tục kiểm tra kỹ. Đây là impact assessment và
thiết kế kiểm chứng, chưa phải code/runtime. Người dùng tiếp tục đồng ý
toàn bộ Q56–Q59; nhãn Q41 được làm rõ/thay phần ngữ nghĩa theo contract v2.
Scope chốt tại [ADR-0009](adr/0009-partial-lifecycle-inventory-seller-triage.md),
policy tiếp theo tại [ADR-0010](adr/0010-shipment-windows-return-label-v2.md).

## Scope được bổ sung và quan hệ với quyết định cũ

| Quyết định đã chốt | Phần thêm vào plan | Quan hệ với policy trước | Phần chưa tự mở |
|---|---|---|---|
| Q52 | Partial shipment/Return/Refund theo line/số lượng, lifecycle độc lập | Làm sâu Q35/Q40; Q56/Q57 chốt label v2, vẫn grain Order Line | Exchange/replacement, chargeback, đầy đủ kế toán |
| Q53 | Mapping Listing→SKU, trạng thái active và thuộc tính quan sát có lịch sử | Làm sâu Q34; không dùng catalog hiện tại để viết lại quá khứ | Bundle/kit, entity resolution hoặc tự sửa listing |
| Q54 | Inventory nhỏ: pool/location, available, reservation, inbound | Đưa lát nguồn này từ roadmap Q31 vào phạm vi kế tiếp | Tối ưu PO/replenishment, supplier platform và latent-demand model |
| Q55 | Hàng đợi SKU read-only, phân rã biến động và evidence drilldown | Mở rộng Q43; đánh giá cấp SKU bổ sung Q38 ở cấp line | Lưu ghi chú, action state, export hay gửi thông báo |

## Các contract cần có trước sinh nguồn lớn

Tên bảng/cột ở đây là vocabulary thiết kế, chưa chốt physical schema.
Tất cả identity có dataset + namespace nguồn phù hợp; mọi phiên bản giữ
event/effective time, observed time, source version và provenance.

| Đối tượng | Một dòng biểu diễn gì | Quan hệ cần giữ | Điều phải kiểm |
|---|---|---|---|
| Order Line | Dòng hàng trong một Order | Channel/account/Order/line key; Listing/SKU tại thời điểm bán | Không đổi grain vì quantity >1 hoặc nhiều shipment |
| Shipment allocation | Phần quantity của line thuộc một lần giao | Shipment + line + allocation ID; từng mốc ship/deliver và pool/location nếu có | Tách giao nhiều đợt; không lấy ngày giao Order thay mọi allocation |
| Return event/line | Một sự kiện xử lý quantity trả | Return + line + allocation nếu quan sát được; request/approve/receive/disposition | Requested, received, restockable là các sự kiện khác nhau |
| Refund transaction | Khoản tiền hoàn trong một transaction/status | Order/line/Return nếu có; amount/currency/status/reversal | Refund không có physical Return; không giả mọi khoản tiền map được một SKU |
| Listing version | Mapping/trạng thái/thuộc tính Listing có hiệu lực | Storefront + native listing/variant; SKU mapping | Không join chỉ vào phiên bản current khi phân tích quá khứ |
| Stock movement/reservation | Thay đổi tồn hoặc lượng giữ của SKU trong pool/location | Event/operation ID; liên kết shipment/return/reservation và source | Retry không trừ hai lần; reservation chưa phải xuất kho |
| Inbound line/event | Lượng dự kiến và lượng thực nhận | Inbound ID/line, pool/location đích, ETA và receipts | ETA không phải receipt; nhận một phần không thành nhận đủ |
| Investigation result | Một lý do cần kiểm tra cho SKU/Storefront/cutoff | Metric/evidence IDs, coverage, generation, rule version | Dedupe reason; đọc cùng version; không thực thi hành động |

Core Order đủ năm Channel vẫn là yêu cầu. Feed/field bổ sung phải có capability
matrix theo Channel/Storefront: cung cấp, không cung cấp, thiếu kỳ hoặc lỗi.
Không sinh payload giống nhau cho năm renderer rồi tuyên bố tương đương API.
Bronze mới cần phân biệt **Channel + feed**, giữ nguyên payload mỗi feed;
quy tắc lịch sử “một bảng mỗi Channel” của nguồn Orders không đủ mô tả toàn
bộ nguồn mở rộng. Tên bảng vật lý sẽ chốt sau capability/schema mẫu.

## Kiểm tra quantities và money độc lập

Một Order Line đặt 3 units: giao 1, hủy 2, sau đó buyer trả unit đã giao.
Lượng đã giao tích lũy vẫn là 1; lượng trả nhận thêm 1; không sửa lịch sử
giao thành 0 hoặc nhân sales thành 4 dòng. Reservation là một phần của
lượng chưa ship, không phải quantity bán thêm. Source correction làm quantity
nhỏ hơn số đã ship cần conflict/correction có dấu vết, không tự clamp về 0.

Đối với flow thông thường, bộ nhỏ cần đối soát lượng đặt hiện hành với lượng
hủy trước giao, chưa ship, đang vận chuyển và đã giao. Từng trạng thái phải
được định nghĩa không giao nhau; returns là flow sau delivery, không cộng
vào phương trình này như một đơn vị mới. Exchange/replacement hoặc chỉnh
stock âm phải có operation được hỗ trợ tường minh; dữ liệu ngoài contract
được đánh dấu để kiểm tra, không tự chế một flow hợp lệ.

Refund amount tách line quantity: refund shipping-only, goodwill, failed,
partial hoặc reversal không tự tạo physical Return. Discount/refund phân
bổ phải dùng precision của currency và đối soát tổng allocated + unmatched
bằng tổng nguồn, kể cả sai số làm tròn. Giữ currency gốc, không lấy `price_factor`
làm FX; chưa có contract thì không biến goodwill/fee thành loss theo SKU.

Trước join nhiều fact phải aggregate mỗi fact về cùng key/grain. Fixture
hai shipment × hai Return × hai Refund cho một line phải vẫn cho một dòng
sales đúng lượng/tiền, không phát sinh tám lần sales. Missing linkage phải
thành unknown/unmatched có coverage, không join theo SKU gần giống.

## Trace tồn kho tính tay để kiểm độc lập

Ví dụ đề xuất với một SKU, một pool/location; mỗi trạng thái dưới đây là
phân hoạch không giao nhau. Đây là expected output cho fixture, không phải
cam kết mọi Channel dùng cùng định nghĩa field available/on-hand.

| Bước | On-hand | Reserved | Không bán được | Available | Giải thích |
|---|---|---|---|---|---|
| Tồn đầu | 10 | 0 | 0 | 10 | Nguồn xác nhận opening balance |
| Giữ 3 units | 10 | 3 | 0 | 7 | Chưa xuất hàng |
| Ship 1 unit của reservation | 9 | 2 | 0 | 7 | Physical stock và phần giữ giảm cùng 1 |
| Hủy 2 units còn giữ | 9 | 0 | 0 | 9 | Release không làm physical stock tăng |
| Nhận lại 1 unit, chờ kiểm | 10 | 0 | 1 | 9 | Return received chưa tự restock |
| Xác nhận unit đó bán lại được | 10 | 0 | 0 | 10 | Chuyển trạng thái tồn, không receipt thêm lần nữa |
| Inbound hứa thêm 5 | 10 | 0 | 0 | 10 | Chưa nhận thì không tăng available |

Hai Storefront cùng nhìn pool này không biến tồn cuối thành 20. Một phần
inbound được nhận chỉ tăng đúng lượng đã nhận. Snapshot là phép quan sát
để reconciliation; không cộng snapshot như một movement. Replay operation
ID không đổi kết quả; correction có version/compensating record theo source
contract. Thiếu snapshot không tự làm balance về 0.

## Listing và phân tích biến động

Một listing ngừng bán từ ngày 5 nhưng feed báo ngày 7: replay tại ngày 6
chỉ dùng điều đã biết ngày 6; báo cáo restated có thể thể hiện hiệu lực ngày
5 trong generation mới. Hai góc nhìn không dùng chung timestamp mơ hồ.
Order cũ giữ mapping/thuộc tính tại sale time; correction mapping phải phân
biệt sửa dữ liệu sai với một thay đổi nghiệp vụ thực sự, có audit và version.

No-sales có thể đi kèm inactive, hết available, thiếu feed hoặc active nhưng
không có Order quan sát. Các điều kiện có thể đồng thời xảy ra; reason giữ
được nhiều bằng chứng hoặc unknown, không ép một nguyên nhân duy nhất.
Stock chung pool không được phân bổ thành tồn Storefront nếu chưa có allocation
policy. Tồn/available giảm không chứng minh mất bao nhiêu nhu cầu.

Fixture phân rã đơn giản: kỳ trước 10 units × 100 = 1.000, kỳ sau 8 × 120 =
960, delta = −40 cùng currency. Công thức dùng giá kỳ trước cho volume cho
đóng góp volume −200 và giá +160; công thức khác có thể phân phần tương tác
khác. Implement chọn một công thức có version, báo hạn chế và kiểm tổng bằng
delta; SKU mới/mất hoặc base bằng 0 có xử lý riêng, không suy causal.

## Q56–Q59 — đã chốt ngày 2026-10-18

### Q56 — cửa sổ 30 ngày khi một line giao nhiều đợt

Ví dụ giao unit A ngày 1, unit B ngày 10; một sự kiện nhãn xảy ra ngày 35.
Neo toàn line vào ngày 1 hay 10 cho kết quả khác nhau. Đã chọn mỗi allocation
có cửa sổ 30 ngày riêng, line dương nếu có allocation đủ điều kiện dương;
line âm chỉ khi các lượng liên quan đã chốt và hết maturity/completeness.
Muốn áp dụng phải có linkage quantity/event đủ tin cậy; thiếu thì unknown.
Nhãn chính thức vẫn binary Order Line, không biến quantity thành mẫu số mới.

Cohort theo lần giao cuối của phần thực giao khi remainder đã giao
hoặc hủy; line giao một phần rồi hủy remainder vẫn có thể eligible trên phần
thực giao, phải hiện lượng ordered/delivered/canceled. Line chưa giao gì
không có label; line còn quantity treo chưa vào tập maturity chuẩn. Positive
và negative đều chờ cùng tiêu chí maturity cho train/eval chính thức, dù
live view có thể thấy Return sớm. Version nhãn mới là
`return-or-refund-line-v2`, phải đối chiếu lại train/eval. Quy tắc boundary,
known-time, trạng thái unknown và L01–L17 ở
[return-label-contract.md](return-label-contract.md).

### Q57 — sự kiện Return/Refund nào kích hoạt nhãn

Request/approve/receive và refund pending/success là các mốc khác nhau.
Lifecycle giữ mọi mốc; nhãn dùng Return request đã được
chấp nhận hoặc product Refund transaction thành công, mỗi loại xét event
time tương ứng trong 30 ngày. Return nhận vật lý là metric riêng, không
thay mốc qualification đã chọn. Refund shipping-only không tự tạo product Return Label; thiếu linkage
line giữ ở unmatched, không đoán SKU/quantity. Pending/rejected/failed không
tự thành positive. Contract v2 đo historical qualifying event: lifecycle
cancel/reversal không tự xóa sự kiện từng hợp lệ; source correction xác
nhận event sai phải recompute. Hai trường hợp phải được phân biệt bằng
link/type nguồn; không đủ bằng chứng thì unknown/conflict.

Đây là định nghĩa nhãn ML, không áp dụng chính sách đổi trả của Channel thật.
Nếu xác nhận/reversal/correction biết sau cutoff thì giữ known-time và tạo
generation/label version mới theo Q36, không backdate để làm feature.

### Q58 — topology tồn kho cho simulator đầu tiên

Có một pool Seller dùng chung cho các Storefront Seller-fulfilled,
và pool Channel-fulfilled tách biệt theo địa điểm/owner tồn được công bố.
Mapping có lịch sử; chưa cần transfer network hoặc tối ưu phân bổ đa kho.
Lát đầu chứng minh reserve/release/ship/receive/return disposition/inbound
và reconciliation. Pool do Channel giữ không mặc nhiên cấp hàng cho các
Channel khác; eligibility phải có mapping.

Simulator có opening balance + movement làm authority quantity;
snapshot dùng đối soát và feed Channel có mapping semantics riêng. Reservation
từ Order và Inventory feed phải trỏ cùng operation, không giữ hai lần. Lệch
snapshot/ledger hoặc out-of-order release-before-reserve phải có trạng thái
reconciliation pending/conflict; không âm thầm thêm adjustment cho khớp.
Oversell/backorder chưa thuộc lát đã chọn và cần quyết định riêng nếu muốn
mô phỏng, không clamp số âm để che lỗi.

### Q59 — cách ưu tiên việc cần kiểm tra

Đã chọn ba nhóm nhìn riêng: return-risk, inventory/Listing availability và biến
động sales. Mỗi nhóm dùng rule/metric có evidence và support; không gộp
currency khác nhau hoặc cộng các score thành một điểm thiệt hại tùy ý.
Một SKU/Storefront có nhiều reason nhưng không lặp vô hạn cùng lý do/cutoff.
Một sự cố pool có thể ảnh hưởng nhiều Storefront nhưng giữ chung issue ID
để tổng incident không nhân theo số card; card vẫn theo Storefront + SKU.
Thiếu feed hiển thị vấn đề dữ liệu và giới hạn kết luận thay vì một cảnh
báo business chắc chắn. K và rule có cấu hình/version; benchmark action
grain bổ sung PR-AUC ở line, không thay bằng rate trên SKU bán nhiều.

Không có một top-K toàn cục giữa ba nhóm. K/tie-break/threshold là cấu hình
triển khai có rule version và benchmark; không cần người dùng chọn library
hay trọng số kỹ thuật. Các quyết định về saved state/notification và tối ưu
đặt hàng vẫn ngoài scope, không là điều kiện để hoàn tất ba nhóm này.

## Impact assessment theo phase và gate

| Thành phần | Tác động bắt buộc | Bằng chứng cần có |
|---|---|---|
| P1 generator/nguồn | Thêm capability/feed, identity liên kết, quantity flow và Listing history; nguồn inventory nhỏ | Schema mẫu, manifest byte/row theo feed, fixture independently calculated |
| P1 Bronze/Silver/Gold/PG | Feed riêng, projection history/current, fact độc lập, chống fan-out, reconciliation | Key/count/quantity/amount/coverage và comparison current/restated |
| P2 labels/features | Contract label v2 theo Q56/Q57; inventory/Listing chỉ dùng known-time; giữ late observations | PIT, maturity, cold start, partial delivery, model/schema compatibility |
| P3 UI/API/agent | Queue ba nhóm Q59, decomposition, drilldown và abstention | Numeric evidence, same-generation queries, top-K eligibility và corpus citations |
| P4/X | Dung lượng/nhiều feed tăng; retention/replay cần giữ links | Runtime/volume estimates, restore/replay toàn flow và trace generation |

G0 cần contract và fixture mới; G1 cần đối soát quantity/stock/join; G2 cần
label/PIT/coverage tương ứng; G3 cần queue/decomposition và action metrics;
G4 cần đo thêm feed/state. Không thêm service chỉ để minh họa một entity.
Sinh nguồn lớn chờ label/topology contracts và gate nhỏ liên quan đạt.

## Audit tích hợp sau khi chốt Q59

[IR01–IR25](integration-risk-register.md) kiểm thêm các giao thoa mà S01–S30
chưa mô tả đủ: currentQuantity/removal không đồng nghĩa cancel, mapping
không đổi business identity hoặc pool binding đang mở, input vector trước
build, uncertainty từ unmatched header và model knowledge cutoff.

Q60/Q61 đã được người dùng cho xử lý theo đề xuất: headline sales trừ
phần hủy trước ship, Refund riêng; nguồn Seller operational giả lập có
authority/coverage riêng. [ADR-0011](adr/0011-sales-basis-and-observable-seller-feeds.md)
ghi phần bổ sung D16; policy nhãn/inventory/queue đã chốt vẫn giữ.
Schema và các expected fixtures ở [source contract](source-contract-order-lifecycle.md),
publication/fencing ở [run contract](run-publication-contract.md).
Các chi tiết còn lại được xử lý trong contract/POC, không cần phỏng vấn
lại người dùng về library/hash/SQL hay retry algorithm.

## Bộ tình huống bổ sung vào nghiệm thu

Các ID S19–S26 bổ sung S01–S18 theo Q52–Q55 đã chốt. Chưa có test được
implement hoặc chạy đạt; expected label/topology/queue theo Q56–Q59 đã chốt.
S27–S30 bổ sung các hệ quả sau khi đóng policy này.

| ID | Tình huống | Assertion độc lập |
|---|---|---|
| S19 | 3 units, hai lần giao; partial cancel; return một phần | Quantity balance, shipment linkage, cohort/nhãn theo Q56/Q57; unknown khi thiếu linkage |
| S20 | Refund-only, pending→failed hoặc success; shipping-only; correction/reversal | Money/status không tạo stock; label chỉ theo contract; unmatched giữ rõ |
| S21 | Shared pool qua hai Storefront; reserve3→ship1→cancel2; replay | Stock trace tính tay đúng; available không nhân theo số Storefront |
| S22 | Inbound hứa5, nhận2; Return nhận1 chờ kiểm rồi restock | ETA không thành receipt; restock là chuyển trạng thái; movement không lặp |
| S23 | Listing inactive hiệu lực ngày5, quan sát ngày7; mapping đổi; current khác lịch sử | Event/known-time đúng; sale cũ không tự đọc mapping mới |
| S24 | Hai shipment × hai Return × hai Refund cho một Order Line | Sales fact/count/amount không fan-out; allocation totals + unmatched khớp nguồn |
| S25 | Sales10×100→8×120; SKU mới/mất; zero base; inactive/stockout/missing cùng lúc | Contribution cộng đúng delta; không chia0 hoặc bịa nguyên nhân duy nhất |
| S26 | Cùng SKU nhiều reason, khác currency; thiếu feed; case đã biết cutoff | Queue eligible/rank/dedupe theo Q59; numeric evidence và support/generation đúng |
| S27 | Sự kiện trước/đúng/sau deadline30; requestD29 approveD31; observe sau cutoff | L01–L17 đúng; event/known-time độc lập; không early-positive sampling |
| S28 | Approved request rồi cancel; refund success rồi reversal; correction invalid event | Historical qualifying event khác data correction; recompute chỉ ở generation mới, model/label version không lẫn |
| S29 | Hai reservation tranh unit cuối; release trước reserve; snapshot lệch ledger | Quantity authority/reconciliation có dấu vết; không double reserve hoặc clamp âm giấu lỗi |
| S30 | Ba nhóm queue; cùng pool nhiều card; một nhóm thiếu lịch sử | Không ép global score/top-K; incident không nhân đôi; abstain ML không che issue Listing có nguồn |

Nguồn và counterexample chi tiết xem
[nghiên cứu edge cases](research-scope-edge-cases-20261018.md). Phần contract
chưa hoàn thành như physical schema, source coverage và POC phải ghi status
open; không coi việc đã chốt Q59 là đã có threshold/benchmark đạt.
