# Kiểm tra tình huống biên sau khi nhận Q52–Q55

Ngày đối chiếu: **2026-10-18**. Người dùng đã đồng ý Q52 partial
fulfillment/return/refund theo line và quantity, Q53 lịch sử Listing/catalog,
Q54 lát inventory pool/location + available/reservation/inbound và Q55 hàng
đợi SKU chỉ đọc + phân rã biến động + drilldown. Đây là nghiên cứu độc lập
để hoàn thiện quyết định trước triển khai, không phải kết quả POC. Các
policy đề xuất dưới đây **chưa được người dùng phê duyệt**. Không thực thi
Cloud, sinh lại dữ liệu hoặc sửa code pipeline.

Đã đối chiếu [plan mở rộng](plan-expansion-discovery.md),
[nghiên cứu nghiệp vụ](research-business-expansion-20261017.md),
[nghiên cứu ML/agent](research-ml-agent-expansion-20261017.md),
[Q38–Q43](de-discovery-risk-review.md),
[readiness](implementation-readiness.md) và các source dẫn bên dưới.
API docs chính thức được dùng để kiểm chứng khả năng biểu diễn và cảnh
báo ngữ nghĩa; không chứng minh cả năm Channel có feed giống nhau. Các
công thức, fixture và lựa chọn dành cho FSDS là suy luận thiết kế.

## Kết quả quan trọng

Bốn phạm vi đã chọn tương tác với nhau: một line giao hai đợt có thể tạo
nhiều return/refund; hàng trả về chưa chắc bán lại được; một SKU có thể
bán qua hai Storefront dùng chung một pool; mapping Listing có thể được
sửa sau khi Order đã bán. Nếu chỉ mở rộng từng bảng riêng, các tổng vẫn
có thể đúng từng bảng nhưng sai khi nối và khi xếp hàng đợi.

Cần đóng bốn nhóm policy trước contract nguồn lớn: **nhãn cho split
delivery**, **sự kiện Return/Refund được tính**, **topology và authority tồn
kho**, **rubric chọn SKU cần kiểm tra**. Q41 vẫn là nhãn nhị phân Order Line
Return OR Refund trong 30 ngày, cohort ngày giao, chưa tự đổi thành nhãn
unit, giá trị tiền hoặc xác suất. API status không tự trả lời bốn lựa chọn này.

## 1. Nhiều đợt giao làm thay đổi cửa sổ nhãn

Hiện [Order](../generator/core/model.py) chỉ có một `delivered_at`, mỗi
Order Line có một `return_requested_at`, một `refunded_at` và một label.
[decide_return](../generator/core/returns.py) gán label từ request trong
cửa sổ, rồi tạo refund sau request 2–7 ngày. Cấu trúc này chưa biểu diễn
return-only, refund-only, failed refund hoặc partial. Shopify có
FulfillmentLineItem với quantity và liên kết về Order Line; ReturnLineItem
liên kết fulfillment line và các lượng xử lý riêng. Đây là căn cứ để giữ
shipment-line bridge thay vì nhân Order Line theo shipment.
[FulfillmentLineItem](https://shopify.dev/docs/api/admin-graphql/latest/objects/FulfillmentLineItem),
[ReturnLineItem](https://shopify.dev/docs/api/admin-graphql/latest/objects/ReturnLineItem).

**Fixture phân biệt policy:** Order Line L mua 3 units; shipment S1 giao 1
unit ngày D0, S2 giao 2 units ngày D20. Chỉ unit S2 có return ngày D45,
tức 25 ngày sau khi unit đó được giao. Giả sử feed đầy đủ và không có
correction. Các cách tính cho kết quả khác:

| Policy ứng viên | Label cuối | Khi đủ hạn kết luận âm nếu không có sự kiện | Đánh đổi |
|---|---|---|---|
| Dùng lần giao đầu D0 cho cả line | 0 | D30 | Bỏ mất return trong 30 ngày của đợt giao sau |
| Dùng lần giao cuối D20 cho cả line | 1 | D50 | Có thể tính cả return của unit S1 quá 30 ngày nếu nó xảy ra sau D20 |
| Cửa sổ riêng cho lượng mỗi shipment; OR lại thành một label line | 1 | Sau D50 và đủ coverage | Đúng exposure từng đợt, nhưng phải biết return thuộc shipment nào |
| Chỉ train/metric chuẩn trên line giao một đợt | Ngoài tập | Không áp dụng | Giữ baseline đơn giản nhưng mất coverage, có thể lệch nhóm |

**Khuyến nghị để quyết định:** ưu tiên cửa sổ từng shipment, label line là
OR của các sự kiện hợp lệ; vẫn giữ một line làm một mẫu. Chỉ kết luận âm
khi mọi phần giao được đưa vào phép đo đã qua 30 ngày và feed đủ. Cohort
line có thể chọn ngày giao cuối của phần thực giao sau khi fulfillment
terminal; phải chọn rõ trước tính metric. Đây là đề xuất làm rõ/mở rộng
Q41, chưa có hiệu lực. Không ghép một label tổng hợp mới với cohort cũ
mà không đổi metric/label version.

Các trường hợp phải có trong câu trả lời policy:

- Mua 3, giao 2, hủy 1: phần bị hủy không có ngày giao. Line có được đưa
  vào cohort sau khi phần còn lại terminal không? Khuyến nghị có, trên
  phần thực giao, kèm `ordered/delivered/canceled` để không giả là giao đủ 3.
- Mua 3, giao 1, còn 2 đang treo: return của unit đã giao có thể được thấy
  sớm; không kết luận line âm hoặc đủ maturity khi phần sau chưa đóng.
  Nếu cho positive vào train trước khi negative đủ hạn sẽ thay đổi cách
  lấy mẫu; nên dùng cùng tiêu chí maturity cho tập train/eval chuẩn.
- Request ngày D30 đúng giờ biên, source biết ngày D33: membership theo
  event time, availability theo observed time. Contract phải ghi rõ
  `0 <= event_time - delivered_at <= 30 ngày` hay biên khác; test trước,
  đúng và sau biên, không rút thành so sánh ngày lịch mất giờ.
- Không có liên kết shipment cho return: không tự gán về lần giao gần
  nhất. Giữ unresolved và coverage, hoặc chọn policy phân bổ tường minh.
  Fixture phải có hai shipment cùng quantity để không suy được bằng tổng.
- Hủy/sửa một delivery event sau công bố: tính lại maturity/cohort và
  label cho generation mới; bản cũ vẫn truy được theo Q36/Q45.

## 2. Return, refund và hàng quay lại kho là ba luồng khác nhau

Shopify xác nhận Refund object có thể liên quan Return hoặc không;
object tồn tại chưa bảo đảm tiền đã hoàn, cần xem transaction status.
Refund còn có thành phần shipping và phí. Vì vậy `refund_id != null`
không đủ để suy một sản phẩm đã được hoàn tiền.
[Refund](https://shopify.dev/docs/api/admin-graphql/latest/objects/Refund).
Disposition có quantity, location và loại xử lý riêng, hỗ trợ việc tách
return khỏi quyết định nhận hàng vào đâu.
[ReverseFulfillmentOrderDisposition](https://shopify.dev/docs/api/admin-graphql/latest/objects/ReverseFulfillmentOrderDisposition).

| Tình huống thử độc lập | Dữ kiện phải giữ | Điều phải quyết định hoặc kiểm |
|---|---|---|
| Request 1 unit ngày 10, bị từ chối ngày 12 | Hai sự kiện và reason/source status | Nhãn tính request hay accepted/received? Hiện generator dùng request; chuyển sang accepted là đổi target |
| Refund pending ngày 28, failed ngày 29, retry success ngày 32; không Return | Refund identity + các transaction attempt/status/time | Nếu chọn success làm sự kiện nhãn thì nằm ngoài 30 ngày; pending không là paid refund |
| Return request ngày 28, refund success ngày 35 | Hai clocks độc lập | Nếu request đủ điều kiện thì line dương từ nhánh Return, không đợi Refund |
| Shipping-only refund, không product allocation | Thành phần tiền và currency, Order ID | Không tự trải refund xuống mọi Order Line; chưa có line attribution thì không tự sinh line positive |
| Một refund trả tiền cho hai lines | Refund header, allocation theo line | Không nhân total amount theo hai dòng; reconcile allocations + phần chưa phân bổ |
| Một line có hai Return IDs, một Return có ba status updates | Stable IDs, quantity/status revision | Không cộng mọi status thành ba lần trả; không đếm replay thành return mới |
| Return received 2 units, 1 sellable, 1 damaged | Receipt và disposition riêng | On-hand có thể tăng 2 trong khi available chỉ tăng 1; refund không tự thay tồn |
| Refund thành công không nhận hàng vật lý | Refund transaction, không có receipt | Tiền có thể thay đổi mà stock không tăng; không tạo restock giả |

**Khuyến nghị để quyết định:** giữ Return request làm mốc nhánh Return để
gần ngữ nghĩa generator hiện tại, hiển thị accepted/rejected/received riêng.
Với refund-only, ưu tiên success của khoản product refund có attribution
tới line, không dùng pending/failed hoặc shipping-only. Tradeoff: target
gộp nhu cầu hậu mãi và hoàn tiền, không phải tỷ lệ hàng vật lý thực trả;
request bị từ chối vẫn có thể positive. Nếu người dùng muốn target physical
return, đó là quyết định khác và phải so sánh riêng với baseline Q41.

Đây chưa mở settlement/fees/profit, payment gateway hoặc workflow xử lý
hoàn tiền. Chỉ cần biểu diễn đủ để kiểm nhãn, số lượng và cảnh báo đọc.
Không mặc định tiền hoàn bằng `returned_units × current_price`: discount,
partial goodwill và rounding có thể làm khác. Unit ledger và money ledger
có invariant riêng. Success bị reversal hoặc request bị correction cần
giữ link/version; cách làm đổi label cuối phải theo policy đã chọn, không
ghi đè mất lịch sử.

Các invariant quantity đề xuất áp dụng cho mô hình không exchange/reship:
`delivered <= shipped <= ordered - canceled_unshipped`; phần return
thực nhận không vượt lượng đã giao sau khi khử duplicate/correction; lượng
disposition không vượt receipt chưa phân loại. Request bị hủy không tiêu
thụ vĩnh viễn quota của request tiếp theo. Nếu mở exchange/reship sau này,
không dùng invariant trên để âm thầm loại movement hợp lệ: cần contract
riêng. Q52 chưa mặc định mở exchange, bundle hoặc giao thay thế.

## 3. Shared pool cần một phép đếm vật lý và authority rõ ràng

Shopify phân biệt InventoryItem × Location và các trạng thái `on_hand`,
`available`, `committed`, `reserved`, `damaged`, `quality_control`,
`safety_stock`; on-hand là tổng các nhóm vật lý đó, incoming ở ngoài.
Đó là ví dụ để tránh trộn nghĩa, không phải yêu cầu FSDS sao chép mọi
trạng thái Shopify. Tài liệu cũng ghi scheduled changes đang bỏ dần;
không lấy API đó làm nền cho inbound mới.
[Manage inventory quantities and states](https://shopify.dev/docs/apps/build/orders-fulfillment/inventory-management-apps/manage-quantities-states).

**Fixture oracle đề xuất:** SKU A tại pool P có on-hand 10, order reservations
3 (Storefront X: 2, Y: 1), unavailable khác 1, available 6. X/Y cùng thấy
pool P thì tổng vật lý vẫn 10, không thành 20. Sau hủy reservation của Y:
on-hand 10, reserved 2, unavailable 1, available 7. X gửi 2: on-hand 8,
reserved 0, unavailable 1, available vẫn 7. Nhận return 1 vào quarantine:
on-hand 9, unavailable 2, available vẫn 7. Kiểm định đạt cho bán lại:
unavailable 1, available 8, on-hand vẫn 9. Replay mỗi event không đổi tổng.
Số này là contract mô phỏng đề xuất, không phải expected output code hiện tại.

| Lựa chọn topology/authority cần đóng | Khuyến nghị nhỏ đủ kiểm | Lý do và tradeoff |
|---|---|---|
| Pool vật lý và Storefront | Một pool Seller-shared phục vụ ít nhất hai Storefront, một pool Channel-managed độc lập; mapping có hiệu lực thời gian | Có case double-count mà không cần sinh mọi combination SKU × Channel × location |
| Reservation | Nguồn canonical reservation theo order-line/pool, phân biệt quantity reserve/release/consume; không đồng nhất với mọi trường native `reserved` | Tránh cộng reservation từ Order feed và inventory feed hai lần; native semantics phải map riêng |
| Snapshot hay ledger là truth | Simulator có opening balance + movement authority; snapshot dùng đối soát, không là delta bổ sung | Thiếu movement phải làm reconciliation fail/unknown, không tự điều chỉnh cho khớp rồi giấu lỗi |
| Returned units | Receipt vào unavailable trước; disposition sellable mới tăng available | Thêm một trạng thái nhỏ nhưng ngăn hoàn tiền tự tạo hàng bán được |
| Inbound | Expected quantity/time có revisions; receipt accepted/rejected/partial riêng | Quá ETA không tự trở thành on-hand; không cần supplier optimizer |

Tổng kiểm vật lý theo pool là opening + receipts thực nhận + returns thực
nhận + transfers-in − shipped − transfers-out − writeoffs + adjustments.
Available suy từ on-hand trừ các allocation/unavailable **không chồng lấp**;
định nghĩa mỗi state phải bảo đảm một unit không bị trừ hai lần. Chưa có
coverage thì không áp công thức để bù số 0 hoặc suy thành stockout.

Cần thử thêm: hai reservation cạnh tranh unit cuối; event release đến
trước reserve do out-of-order; cancel sau ship; chuyển pool khi đơn đang
mở; snapshot lệch ledger; inventory feed stale nhưng Order feed mới;
nhận inbound một phần rồi hủy remainder. Không clamp số âm thành 0 để
che lỗi. Nếu muốn mô phỏng oversell/backorder chủ đích, đó là policy mới:
quantity âm/shortfall cần có ý nghĩa, không coi mọi âm là dữ liệu hỏng.

Pool eligible cho một Storefront không bảo đảm toàn bộ available là quota
bán của Storefront ấy. V1 nên hiện lượng pool dùng chung và mức độ phủ;
chưa hứa available-to-promise/stock cam kết theo Channel. Do đó cũng chưa
gọi `available / trailing_sales_per_day` là nhu cầu hoặc quyết định đặt hàng.

## 4. Lịch sử mapping phải giữ đúng sản phẩm đã bán và trạng thái đã biết

Amazon thể hiện Listing có trạng thái BUYABLE/DISCOVERABLE riêng và dữ liệu
issues/availability. Điều này ủng hộ giữ lifecycle có coverage, không suy
no-sales là nhu cầu thấp.
[Manage Product Listings](https://developer-docs.amazon/sp-api/docs/manage-product-listings-guide).
Catalog nội bộ hiện là tập SKU tĩnh
([catalog.py](../generator/core/catalog.py)); Q53 là nguồn mới, chưa có
chứng cứ code đã quản lý lịch sử này.

**Fixture bắt buộc:** Listing L thuộc Storefront X map SKU A đến D10, map
SKU B từ D10; Order O đặt D9 được ingest D12. O vẫn thuộc A theo effective
time được biết ở cutoff truy vấn, không gán B theo current mapping. Ngày
D15 có correction nói D9 thực ra thuộc B: generation hiện hành có thể
đổi theo Q36, nhưng query as-known-at-D12 vẫn thấy A. Contract cần cả
valid time và observed time; SCD2 chỉ có một khoảng thời gian chưa chứng
minh truy lại cả hai cách xem.

Các bất biến thiết kế đề xuất: Listing key có Channel/shop/native variant,
SKU nội bộ ổn định; title/category đổi không tự tạo SKU mới; xóa Listing
không xóa order history; delist/relist giữ các khoảng hoạt động; native SKU
text tái sử dụng không tự chứng minh cùng physical item. Hai mapping
chồng hiệu lực ở cùng cutoff phải báo conflict, không lấy dòng mới nhất
im lặng. Mapping thiếu giữ unknown; không loại line khỏi tổng shop rồi
vẫn báo coverage đầy đủ. Bundle/kit và unit conversion ngoài lát Q53 hiện
tại, không thêm bridge many-to-many bằng suy đoán.

Nếu chỉ có snapshot ngày và trạng thái active tại cuối ngày, chưa biết
active đủ 24 giờ. V1 có thể công bố tỷ lệ ngày quan sát active; muốn số giờ
buyable hoặc doanh số trên exposure giờ phải có event/resolution phù hợp.
Inventory > 0 cũng không chứng minh Listing buyable ở thời điểm đó.

## 5. Hàng đợi SKU cần tách sự cố nguồn khỏi tín hiệu sản phẩm

Q55 đã mở queue chỉ đọc, chưa chọn một hàm tối ưu chung. Q38 giữ score
xếp hạng; không cộng score để gọi số return kỳ vọng. Đề xuất rubric mở đầu
gồm nhóm lý do với evidence + support + freshness, rồi sort ổn định trong
nhóm; người dùng cần chọn thứ tự giữa nhóm nếu muốn một top K toàn cục.

**Bộ phân biệt tối thiểu:** A bán nhiều, return rate 5% ổn định; B ít lịch
sử, return 1/1; C sales giảm 60% nhưng bị delist nửa kỳ; D pool hết available,
inbound đã trễ; E tăng return rate có đủ maturity/support và nguồn mới;
F số liệu cũ do feed thiếu; G SKU mới chưa có return history nhưng có
listing issue quan sát rõ. Không yêu cầu mọi bộ rank phải cho cùng thứ tự:
oracle phải chấm theo mục tiêu nhóm trước khi xem kết quả benchmark.

Các điều kiện pass đề xuất:

- B không được gắn model risk cao có căn cứ từ 1/1; abstention ML không
  được che cảnh báo inventory/listing có nguồn rõ của G.
- C được nêu exposure/lifecycle đã biết; không khẳng định giảm demand
  hoặc nhân quả delist. F được gắn vấn đề coverage, không biến thành
  "SKU bán yếu". A không tự đứng đầu chỉ vì volume/tổng score cao.
- E có numerator/denominator, cohort/cutoff, baseline/window và evidence;
  D chỉ nêu shortfall/inbound cần xem, không tự đặt hàng hay tính lost sales.
- Cùng pool ảnh hưởng X/Y có thể hiện hai hồ sơ Storefront + SKU, nhưng
  chung `issue_id` ở pool để tổng số incident và exposure không nhân đôi.
  Người dùng cần chọn queue theo SKU toàn Seller hay Storefront + SKU;
  khuyến nghị v1 dùng Storefront + SKU cho tương thích UI Q43, kèm group.
- Một request ghim một generation Q45; mọi card, contribution và drilldown
  đối chiếu cùng version. Số K, tie-break và reason/model version ghi trong
  evidence. Không lưu dismissed/resolved/notes vì Q55 chỉ đọc.

Phân rã doanh số cần công thức có identity kiểm tổng. Một lựa chọn đơn
giản theo SKU/currency là `R = q × p`, lấy biến động lượng và đơn giá theo
đối xứng: `ΔR = Δq × (p0+p1)/2 + Δp × (q0+q1)/2`. Với SKU mới/mất hoặc
q=0, p không suy được: hiện riêng contribution entrant/exit, không bù giá
0 rồi kể thành price effect. Đây là decomposition kế toán số học, không
chứng minh nguyên nhân. Mix/category dùng partition không chồng lấp để
đối soát; category history/correction cần version cùng metric contract.

Không xếp ưu tiên dựa trên tiền cộng khác currency. Nếu muốn tradeoff
doanh thu/return/stockout thành một score, phải chọn mục tiêu và trọng số
tường minh; chưa có cost/causal evidence thì không gọi "giá trị cứu được".
Precision@K của queue cần checklist đáng kiểm tra độc lập, cùng eligible
set/K/cutoff với baseline; không dùng "có ít nhất một return" làm oracle.

## Frontier để đóng trước schema nguồn lớn

Các mã dưới đây là **nhãn phân tích trong note**, không phải số quyết định
Q mới. Parent discovery có thể gom thành ít câu hỏi hơn. Không hỏi người
dùng chọn library, khóa hash hoặc layout storage.

| Nhãn | Quyết định nghiệp vụ còn mở | Đề xuất và hệ quả chính |
|---|---|---|
| E1 | Mốc 30 ngày và cohort của split/partial-cancel line | Cửa sổ theo shipment, OR thành label line; maturity sau phần giao cuối + completeness; cohort ngày giao cuối phần thực giao khi terminal. Phải duyệt rõ vì diễn giải Q41 |
| E2 | Sự kiện nhánh Return/Refund và attribution | Return request + product refund success có line allocation; status/reason khác chỉ giải thích; shipping-only/unallocated không tự làm line positive. Duyệt target kể cả request bị từ chối |
| E3 | Pool/reservation/inbound và hàng trả về | Shared Seller pool + pool Channel riêng; canonical quantity authority; quarantine→sellable; inbound không tự nhận theo ETA. Duyệt lát nhỏ, chưa replenishment/backorder/exchange |
| E4 | Unit và tiêu chí hữu ích của queue | Storefront + SKU, issue grouping cho pool; reason lanes, rule baseline, support/freshness; một top K toàn cục cần duyệt thứ tự ưu tiên nhóm |

Đã kiểm tra thêm quantity fan-out, idempotence, cancellation, refused
requests, refund retries, restock disposition, listing remap/correction,
shared stock, partial inbound, cold start, stale feeds, cross-currency và
zero-denominator decomposition. **Chưa có fixture nào được thực thi**;
ghi chú này cung cấp tình huống và expected behavior để đưa vào contract/
POC sau khi policy được chọn. Hoàn tất discovery có nghĩa các lựa chọn
được đóng hoặc hoãn có chủ đích, không bảo đảm mọi thay đổi tương lai
được loại bỏ.
