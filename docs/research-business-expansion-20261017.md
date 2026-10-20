# Khai thác và mở rộng nghiệp vụ Seller trước khi sinh lại nguồn

Ngày nghiên cứu và truy cập nguồn: **2026-10-17**. Đây là ghi chú discovery,
không phải quyết định mở scope hoặc bằng chứng chạy. Đã đối chiếu
[plan tổng](high-level-architecture-plan.md),
[điều kiện triển khai](implementation-readiness.md),
[quyết định Q28–Q31](de-expansion-review.md), generator và tài liệu chính thức
Shopify/Amazon. Các đề xuất dưới đây là suy luận thiết kế cho FSDS, được tách
khỏi các sự kiện do API docs xác nhận.

Giữ nguyên một Seller, năm Channel, UI owner-only tiếng Việt, chỉ tư vấn,
nguồn giả lập, không paid API, GCP do người dùng thao tác và không chi ngoài
credit 300 USD. Đọc API docs chỉ để thiết kế dữ liệu có căn cứ; không đồng
nghĩa dự án có tài khoản, quyền API, lịch sử hoặc mức phủ tương đương ở năm
Channel. Chưa nghiên cứu đủ API TikTok Shop/Walmart/Shopee để hứa parity.

## Kết luận để lựa chọn

Mở rộng có giá trị nhất là giúp Seller phân biệt **bán yếu do đâu cần kiểm
tra**, **đơn nào đang có vấn đề thực hiện**, và **tồn kho nào cần chú ý**.
Ba hướng này cần biết trạng thái listing, số lượng thực giao/trả, lịch sử
tồn và thời điểm dữ liệu được quan sát. Thêm biểu đồ doanh số không thay
được các nguồn đó.

Nên chốt ngay các seam nguồn và ngữ nghĩa bên dưới, dù chưa xây màn hình
tương ứng. Một trường `delivered_at` cấp Order, một dòng SKU tĩnh, hoặc một
con số `stock` cho mỗi Storefront sẽ giới hạn những câu hỏi có thể trả lời
về sau. Ngược lại, finance/ads có thể giữ là module tùy chọn, chỉ làm fixture
nhỏ nếu được chọn; chúng không cần chạy full-scale cùng nguồn Order.

| Phạm vi | Đã chốt | Discovery mới trong ghi chú này |
|---|---|---|
| Q28 | Category/fulfillment, hủy và giao hàng theo nguồn | Partial shipment/partial cancel, thời gian xử lý return/refund; hoàn thiện grain trước |
| Q29 | Basket/co-purchase, concentration, địa lý, discount association | Điều kiện đủ dữ liệu, phân rã biến động và chọn nhóm cần kiểm tra; chưa suy hiệu quả can thiệp |
| Q30 | Repeat/first-observed trong Storefront có ID | Cohort giữ chân quan sát được, phân biệt churn với chưa quan sát đủ; tùy chọn |
| Q31 | Inventory/nhập hàng ưu tiên roadmap; traffic/ads/profit cần contract | Tồn theo pool/location, bổ sung hàng có lead-time; reconciliation trước profit; traffic trước ads |

## 1. Thực hiện đơn và hậu mãi: từ cờ trạng thái sang luồng số lượng

Shopify có `FulfillmentLineItem` nối một Order Line với fulfillment và số
lượng thực hiện; `ReturnLineItem` nối ngược fulfillment line, có số lượng
return và các trạng thái xử lý. Đây là bằng chứng nguồn có thể có grain sâu
hơn một trạng thái chung của Order, không phải cam kết mọi Channel đều có
các trường đó. [FulfillmentLineItem](https://shopify.dev/docs/api/admin-graphql/latest/objects/FulfillmentLineItem),
[ReturnLineItem](https://shopify.dev/docs/api/admin-graphql/latest/objects/ReturnLineItem).

| Seller quyết định | Metric đề xuất | Grain và dữ liệu tối thiểu | Giới hạn / fixture bắt buộc |
|---|---|---|---|
| Kiểm tra đơn quá hạn còn hàng chưa giao | Units chưa fulfill; tuổi backlog; tỷ lệ giao đúng hẹn theo lượng hoặc shipment, công thức tường minh | `shipment_id × order_line_id`, quantity, event/status, promised interval, actual shipped/delivered, observed time | Đơn 3 units giao 1 trước hạn và 2 sau hạn; không gắn một timestamp cho cả 3 |
| Kiểm tra điểm nghẽn hậu mãi | Pending return units; thời gian request→received→processed; pending refund age | Return ID/line, liên kết shipment-line, quantity, reason có mapping, từng mốc quan sát | Partial return, nhiều return trên một line, return bị từ chối, refund không có physical return |
| Xem bất thường dịch vụ theo fulfillment/carrier | Cơ cấu quá hạn/hủy/return trên cohort có coverage | Nhóm phương thức/carrier chỉ khi feed có; trạng thái tuyến giao, số lượng và mẫu số | Đổi mix SKU hoặc địa lý có thể làm metric đổi; không kết luận carrier gây return |

Shopify mô tả Refund có thể gồm tiền hàng, phí vận chuyển và các thành phần
khác; liên kết tới Return là tùy chọn. Có Refund object **chưa chứng minh
tiền đã hoàn**: cần trạng thái transaction liên quan. Vì vậy phải quyết định
Q41 dùng refund requested, created hay successful cho nhãn, trước khi thêm
feed thật hơn; không đổi nhãn âm/dương chỉ theo tên object.
[Refund](https://shopify.dev/docs/api/admin-graphql/latest/objects/Refund).

Hiện [model](../generator/core/model.py) có `delivered_at` và timeline cấp
Order; Order Line chỉ có một `return_requested_at`/`refunded_at`, chưa có
quantity trả hay tập shipment. [decide_return](../generator/core/returns.py)
sinh refund nối tiếp request. Đây là đơn giản hóa hữu ích cho bài học nhưng
chưa kiểm thử được refund-only, payment failure, một phần hàng hoặc nhiều
lần return.

**Đề xuất nền:** giữ Order Line là grain phân tích hiện hành, thêm thực thể
shipment-line và return/refund event có quan hệ về Order Line. Khi một line
giao nhiều đợt, phải chốt mốc bắt đầu 30 ngày: theo từng lượng giao rồi gộp
nhãn line, theo lần giao đủ, hoặc tạm loại line này khỏi metric/model và nêu
coverage. Không âm thầm chọn earliest/latest vì các lựa chọn tạo cohort và
maturity khác nhau. Nhãn v1 Q41 chỉ đổi khi có quyết định tường minh.

**Chi phí/độ khó:** trung bình–cao về logic, vì cardinality và trạng thái tăng;
không nhất thiết thêm service. Lưu event delta và bridge thay vì nhân bản
toàn Order qua mọi bước. POC nhỏ có expected quantities độc lập có giá trị
hơn tăng GB. Đây là hướng cần quyết định sớm vì ảnh hưởng nguồn, nhãn và feature.

## 2. Inventory/nhập hàng: quyết định ưu tiên bổ sung nguồn

Shopify `InventoryLevel` nối một InventoryItem với một Location, có nhiều
trạng thái số lượng như available, on-hand, incoming, committed. InventoryShipment
có lượng accepted/rejected/canceled và quan hệ với transfer. Các schema này
ủng hộ tách số tồn sẵn bán, tồn vật lý và hàng đang về; chúng không chứng minh
đã có lịch sử purchase order, supplier lead-time hay từng biến động tồn cho
FSDS. [InventoryLevel](https://shopify.dev/docs/api/admin-graphql/latest/objects/InventoryLevel),
[InventoryShipment](https://shopify.dev/docs/api/admin-graphql/latest/objects/InventoryShipment).

| Seller quyết định | Metric đề xuất | Grain và dữ liệu tối thiểu | Giới hạn / failure case |
|---|---|---|---|
| SKU/pool nào cần kiểm tra trước khi hết hàng | Available units; ngày có hàng/không có hàng quan sát được; tốc độ bán trong ngày đủ coverage | SKU × physical pool/location × timestamp; opening balance, receipt, reserve/release, ship, adjustment, snapshot | Không cộng hai Channel cùng phản chiếu một pool vật lý; missing snapshot khác zero |
| Có nên chuẩn bị đơn nhập bổ sung | Days of cover theo giả định rõ; projected position; khoảng cần kiểm tra dưới các lead-time scenario | Tồn, reservations, inbound PO/line, expected receipt, MOQ/pack size, lead-time và khả năng nhận một phần | Incoming không chắc tới đúng hẹn; không dùng sales khi stockout như demand bằng zero |
| Nhà cung cấp/lô nhập nào đang chậm | Độ trễ promise→receipt; nhận thiếu/hư; backlog nhập | Supplier × PO line × receipt; quantity và thời gian đặt/hứa/nhận/accepted | Không coi transfer nội bộ là mua hàng supplier; không suy supplier từ Marketplace |

**Đề xuất topology:** một Seller có thể có pool do Seller giữ chia cho nhiều
Storefront, cộng pool do Channel giữ. Location/pool là dimension riêng;
Storefront→pool có hiệu lực theo thời gian. Quantity ledger và reservation
phải cùng hệ quy chiếu để phép cộng tồn không đếm hai lần.

**Đề xuất mức đầu tiên:** availability/stockout exposure + lịch nhập hàng,
khuyến nghị kiểm tra dựa trên kịch bản. Chưa gọi là tối ưu tồn kho hoặc dự
báo demand. Sale quan sát được bị giới hạn bởi còn hàng/listing bán được;
ngày stockout không cho biết chính xác bao nhiêu nhu cầu mất. Với synthetic
data có thể sinh latent demand để chấm simulator, nhưng agent/model chỉ thấy
feed được công bố, không được dùng latent demand như feature có thật.

**Chi phí/độ khó:** trung bình cho snapshot có coverage, cao khi thêm ledger,
reservations và replenishment. Số dòng snapshot xấp xỉ `SKU × pools × số
mốc`, không mặc định nhân thêm năm Channel khi chung pool. Theo dõi dung
lượng nhỏ trước quyết định độ phân giải giờ/ngày; không cần mua API hoặc
thêm hệ tối ưu hóa ở bước đầu.

## 3. Listing/catalog lifecycle: biết khi nào sản phẩm thực sự được bán

Amazon phân biệt listing buyable và discoverable, có thông báo thay đổi
trạng thái và issues. Vì thế thiếu doanh số có nhiều cách giải thích cần
kiểm tra ngoài cầu yếu: chưa được bán, bị suppression, thiếu thông tin hoặc
không còn tồn. API docs không chứng minh nguyên nhân nào đã xảy ra trong
FSDS. [Manage Product Listings](https://developer-docs.amazon/sp-api/docs/manage-product-listings-guide).

| Seller quyết định | Metric đề xuất | Grain và dữ liệu tối thiểu | Giới hạn / failure case |
|---|---|---|---|
| Sửa listing hay kiểm tra nhu cầu | Thời gian buyable/discoverable; no-sales trong thời gian đủ khả năng bán; số issue theo loại | Storefront × native listing/variant × interval; mapping SKU có hiệu lực; trạng thái, issue, giá, published/retired time | Unlisted khác active-but-zero-sales; issue mất trong feed không tự là đã giải quyết |
| Kiểm tra SKU/nhóm sản phẩm mới | Tuổi listing, thời gian quan sát, sales/return theo cohort launch | Product/variant/SKU mapping, category và thuộc tính quan sát được có lịch sử | Đổi title/category không tự tạo SKU mới; listing đổi mapping không được viết lại quá khứ |
| Phân tích hoàn hàng theo thuộc tính | Return cohort theo size/material/version khi thật sự được công bố | Observed product attributes với validity/availability, mapping tới Order Line tại thời điểm bán | Thuộc tính hiện tại không đại diện sản phẩm đã bán trước sửa; thiếu ghi unknown |

Catalog hiện tại sinh `unit_cost_usd` và `defect_propensity` cùng SKU;
`defect_propensity` được model mô tả là biến ẩn. Cần giữ riêng observed
attributes và simulation truth, tránh đẩy defect propensity vào dim sản
phẩm phục vụ rồi để model/agent đọc. Thêm lịch sử catalog cho cold start,
mapping correction, delisting/relisting là cơ hội tạo bài kiểm tra feature
và drift rõ hơn. [Catalog](../generator/core/catalog.py),
[model](../generator/core/model.py).

**Chi phí/độ khó:** thấp–trung bình nếu chỉ event + SCD2, ít tăng GB; lợi ích
lớn cho độ đúng mẫu số và giải thích no-sales. Không cần triển khai sửa
listing bằng API. Hướng này phù hợp làm contract/fixture sớm rồi mở mart khi
scope được chọn.

## 4. Tiền: đối soát trước, contribution margin sau

Amazon Finances v2024-06-19 có transaction ID, trạng thái deferred/released,
posted date, currency, identifiers liên quan và cây breakdown. Đây là
nguồn mô tả money movement ở grain khác Order Line; parent breakdown và
children không được cộng lặp. Trạng thái released của transaction không tự
là bằng chứng tiền đã vào ngân hàng. [Finances reference](https://developer-docs.amazon/sp-api/docs/finances-api-v2024-06-19-reference).

| Seller quyết định | Metric đề xuất | Grain và dữ liệu tối thiểu | Giới hạn / failure case |
|---|---|---|---|
| Khoản nào chưa đối soát được | Transaction unmatched; số tiền điều chỉnh; tuổi khoản deferred | Transaction/component × currency; related Order/refund/shipment/settlement; version và observed time | Có phí không gắn được một SKU; không phân bổ giả để làm tổng trông đầy đủ |
| Tiền hàng bị giảm do đâu | Waterfall tiền hàng/discount/refund/phí có nguồn | Signed amount, component taxonomy, currency, tax/shipping convention, reversal links | Không lấy `SUM(line_net_amount)` làm cash nhận về; correction/reversal phải tránh đếm lặp |
| SKU nào có contribution margin quan sát thấp | Doanh thu và chi phí biến đổi được đối soát, kèm phần cost thiếu | Lịch sử unit cost cùng currency, phí shipping/fulfillment/payment/return; allocation policy | `unit_cost_usd` tĩnh không dùng với revenue VND/EUR nếu chưa có FX; không gọi net profit khi thiếu overhead/tax |

**Đề xuất:** nếu chọn finance, bắt đầu bằng reconciliation có vùng unmatched
và coverage, nguyên gốc currency. Tách monetary fact khỏi sales fact và label
return; amount có thể thay đổi mà nhãn nhị phân giữ nguyên. Không mở FX hay
hạch toán thuế đầy đủ để ép toàn hệ có một tổng profit.

**Chi phí/độ khó:** cao về ngữ nghĩa, nhưng có thể làm fixture một Storefront
trước; không nên sinh fake fees cho mọi Channel rồi tuyên bố economics thực.
Nguồn và feed completeness riêng là điều kiện trước mở metric tiền mới.
Đây là thiết kế phân tích dữ liệu giả lập, không phải hướng dẫn kế toán.

## 5. Traffic/ads: đo phễu có nguồn, không suy từ Order

Amazon Sales and Traffic Business Report có dữ liệu sales và traffic theo
ngày/ASIN, với lựa chọn granularity; metadata report và quyền truy cập phải
được kiểm riêng. ASIN aggregate không tự là traffic cho từng seller SKU hay
visitor-level journey. [Analytics Reports](https://developer-docs.amazon/sp-api/docs/report-type-values-analytics).

| Seller quyết định | Metric đề xuất | Grain và dữ liệu tối thiểu | Giới hạn / failure case |
|---|---|---|---|
| Kiểm tra giảm traffic hay thay đổi chuyển đổi | Sessions/page views; units per session theo đúng định nghĩa nguồn; trend khi listing đủ coverage | Storefront/report scope × ASIN/variant × kỳ report, timezone, granularity, numerator/denominator | Không chia orders của mọi Channel cho sessions Amazon; sessions không tự là unique customers |
| Chiến dịch nào cần xem lại | Spend/click; attributed sales/spend theo currency và cửa sổ attribution đồng nhất | Account/campaign/ad/product × report date × attribution window/model, report revision | Tổng attributed sales không mặc định bằng order-date sales; denominator zero/missing xử lý riêng |
| Hiệu quả thật của tăng ngân sách | Chỉ có thể nêu giả thuyết hoặc thiết kế thí nghiệm riêng | Treatment/control assignment hoặc thiết kế đánh giá nhân quả được duyệt | ROAS/correlation không chứng minh incremental lift; không tự tăng ngân sách |

Amazon Ads xác nhận conversion có thể được ghi về ngày tương tác, report
chưa hoàn chỉnh cho tới hết lookback; định nghĩa tùy campaign. Không áp
dụng một cửa sổ 14 ngày chung cho mọi ad product. Điều này đòi hỏi freshness,
revision và maturity riêng, dù pipeline publication đã có.
[Ad campaign attribution](https://advertising.amazon.com/help/GX7KDKHMWQYMJ385?initialSessionID=140-2242586-3908950&ld=ASXXSCTUSDirect).

**Đề xuất:** traffic/listing exposure trước, ads aggregate sau; chưa sinh
clickstream mỗi lượt xem. Không trừ ad-attributed sales khỏi total sales để
gọi phần còn lại là organic khi chưa thống nhất kỳ, scope và attribution.

**Chi phí/độ khó:** traffic aggregate trung bình; ads cao vì nhiều grain,
revision và campaign semantics. Row volume tăng theo số campaign/keyword/
product/ngày/revision; user-level clickstream là nhánh khác có chi phí lớn,
không cần để trả lời các câu hỏi đầu tiên.

## 6. Khai thác sâu hơn phần đã duyệt mà chưa cần miền nguồn lớn mới

Các mục sau là **đề xuất metric/trải nghiệm**, không tự mở scope model mới.

- **Phân rã thay đổi doanh số:** hiện rõ units, net unit price, mix
  Storefront/category/SKU và currency thay đổi; kiểm tra total bằng sum các
  thành phần cùng grain. Cần chọn công thức phân rã và handling SKU mới/mất,
  không chỉ vẽ hai số rồi khẳng định nguyên nhân. Không phải causal analysis.
- **Basket có điều kiện:** đo support theo distinct Order, confidence/lift
  có mẫu số và ngưỡng; loại lặp SKU trong Order, tách quantity. So nhóm có
  đủ coverage và thời gian; kết quả từ generator chọn SKU độc lập có thể
  không có cấu trúc bổ trợ để khai thác. Nếu muốn kiểm thử basket, thêm cơ
  chế liên hệ có chủ đích vào scenario truth và ghi rõ đây là giả lập.
- **Retention theo cohort quan sát:** Q30 cho phép repeat trong Storefront
  có ID; có thể thêm thời gian đến lần mua lại và cohort đủ thời gian quan
  sát. Không gọi khách chưa mua lại trước cutoff là churn thật, không nối
  danh tính xuyên Channel, không suy lifetime value từ 365 ngày giả lập.
- **Danh sách kiểm tra có bằng chứng:** kết hợp listing/availability,
  lượng chưa giao, returns đủ hạn, độ mới và coverage để xếp nhóm Seller
  nên xem; dùng rule có reason/evidence. Chưa gộp thành một điểm “ưu tiên”
  thiếu định nghĩa chi phí/lợi ích hoặc lấy tiền khác currency để so sánh.

Các điều kiện này nối trực tiếp với D16–D20/Q29–Q30 và không cần thêm service.
Phần tính toán có thể dùng marts/API hiện định hướng. Khi còn thiếu nguồn,
câu trả lời agent phải chuyển thành điều cần kiểm tra thay vì kết luận.

## 7. Năm quyết định nền nên chốt trước sinh lại nguồn lớn

| Quyết định | Vì sao quyết định muộn sẽ tốn sửa | Đề xuất để người dùng chọn |
|---|---|---|
| **B1 — grain thực hiện và nhãn** | Một delivered_at cấp Order không biểu diễn partial line; đổi mốc maturity kéo theo train/eval | Contract shipment-line/quantity ngay; chốt policy 30 ngày cho split delivery, refund-success và line không đủ coverage; giữ Q41 khi chưa có quyết định thay |
| **B2 — topology tồn kho** | Storefront stock giả độc lập có thể double-count pool thật; nguồn Order có thể mâu thuẫn conservation | Pool/location riêng + mapping; chọn scope đầu tiên availability/inbound hay đầy đủ reservation ledger; tách latent demand khỏi observable sales |
| **B3 — lịch sử listing/SKU** | Catalog snapshot mới nhất làm viết lại thuộc tính quá khứ và mẫu số active SKU | Listing→SKU mapping có hiệu lực/quan sát; trạng thái active/buyable theo nguồn; observed attributes tách simulation truth; bundles/kits chỉ thêm khi chọn scope |
| **B4 — cấp độ tiền và attribution** | Gộp sales/cash/profit hoặc order-date/ad-date từ đầu làm mart/API nhập nhằng | Giữ sales chuẩn hiện tại; finance reconciliation và ads là facts tùy chọn; contract signed amount/currency/tax/fee/allocation/report window trước tính metric mới |
| **B5 — capability và simulator** | Cùng tên field ở năm renderer dễ giả tạo cùng availability; thêm domain sau làm dataset không tái lập | Matrix từng Channel/feed/field/time/coverage; scenario seed/config/version; optional module độc lập; truth chỉ cho oracle; fixture chéo domain trước full-scale |

Không cần chốt toàn bộ màn hình hoặc thuật toán trước khi làm. Cần chốt
ngữ nghĩa và seam chịu thay đổi: grain/identity, thời gian, source coverage,
money/quantity và đường nối các facts. Năm mục này là câu hỏi thiết kế,
không đánh số Q52 trở đi để tránh bị hiểu là người dùng đã phê duyệt.

## 8. Fixture mở rộng để quyết định có đáng đầu tư

Các fixture này chưa phải S19+ đã duyệt; dùng để thử lựa chọn, không thay
S01–S18 hiện tại. Expected output cần viết độc lập với transformation.

| Fixture | Kết quả cần chứng minh |
|---|---|
| 3 units, hai shipment, một partial return; refund pending rồi failed/success | Không nhân Order Line; có định nghĩa cohort; chỉ trạng thái đúng contract kích hoạt nhãn; quantity và money tách |
| Một pool cấp hai Storefront, hai reservation, một cancel release | Tồn vật lý và available không nhân hai theo số Channel; replay không trừ tồn lặp |
| 4 ngày stockout, 2 ngày listing suppressed, một ngày thiếu feed | Ba loại thiếu doanh số có trạng thái/coverage riêng; không tự ghi demand=0 |
| Listing đổi SKU mapping/category; SKU có defect latent | Drilldown trước/sau đúng version; historical feature không đọc thuộc tính tương lai hoặc truth |
| Fee không map Order Line; refund shipping-only; reversal; payout mismatch | Unmatched giữ lại; total theo currency kiểm được; không gán profit đầy đủ giả |
| Report ads ngày cũ được restate, khác order date | Daily official generation mới có lineage; attribution maturity hiển thị; không kết luận uplift |

Mỗi nhánh được chọn cần một demo quyết định Seller với evidence và chi phí
đo được: bytes/rows/feed, thời gian job, RAM peak, lịch chạy và retention.
Không coi thêm volume synthetic là proof thêm giá trị nghiệp vụ. Trong
ngân sách cố định, đưa nhiều miền vào full-scale cùng lúc làm tăng nhân
bản dữ liệu, số testcase và thời gian chạy; nên giữ Order core đủ năm Channel,
POC miền mới ở phạm vi nhỏ trước khi quyết định mở rộng.

## 9. Phạm vi chưa có bằng chứng

- Chưa có account/API access hoặc payload sản xuất để kiểm quyền, lịch sử,
  timezone, rate limit và field coverage của Seller thật.
- Chưa có schema mới, generator mới, POC hoặc benchmark cho các nhánh này.
- Shopify `latest` được trang đã đọc hiển thị là 2026-10. Khi triển khai phải
  pin phiên bản contract đã chọn; không để URL latest tự quyết định schema.
- Chưa thẩm định supplier procurement API, cost accounting/FX, ad attribution
  của cả năm Channel, customer consent hoặc cơ chế experiment thật. Chúng
  không là phụ thuộc v1 trừ khi chủ động chọn thêm scope.
- Ràng buộc synthetic khiến insight/model chỉ được đánh giá về tính đúng
  pipeline và khả năng phục hồi quy luật mô phỏng; không suy hiệu quả kinh
  doanh ngoài đời từ các fixture.
