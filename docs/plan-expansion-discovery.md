# Discovery mở rộng FSDS trước implementation

Mở ngày 2026-10-17, tổng hợp tiếp 2026-10-18. Người dùng yêu cầu quét thêm những gì có thể khai thác hoặc
mở rộng, vượt ra ngoài rà hiện trạng. Tài liệu này là **bản đồ cơ hội và
quyết định còn mở**. Cập nhật sau câu trả lời người dùng 2026-10-18:
**Q52–Q59 đã chốt**; scope/impact và contract tiếp theo tại
[expanded-scope-design.md](expanded-scope-design.md). Các nhánh khác ghi
“đề xuất” vẫn chưa chọn. Nghiên cứu/đặc tả chưa chứng minh runtime khả thi.

## Điểm xuất phát và phạm vi quét

Giữ các ràng buộc đã chốt: một Seller, năm Channel, advisory/read-only,
owner-only tiếng Việt, native currency, dữ liệu giả lập, toàn rubric DE/MLE,
GCP do người dùng thao tác, không chi ngoài credit. Q29/Q30 đã bao gồm
basket/concentration/geography/discount và customer theo độ phủ; đây không
phải tính năng mới cần xin chọn lại. Q54 nay nâng inventory nhỏ từ roadmap
Q31 vào phạm vi kế tiếp; tối ưu nhập hàng và model forecast vẫn chưa chọn.

Rà theo chuỗi: **Seller cần quyết định gì → cần biết gì → dữ liệu nào quan
sát được → grain/time/identity → metric/model → API/agent → kiểm chứng →
vận hành**. Một ý tưởng chưa có nguồn/expected output chỉ là giả thuyết.
“Quét hết” ở vòng này nghĩa mọi nhánh dưới đây có kết luận hoặc câu hỏi
tường minh, không có nghĩa đã liệt kê mọi tính năng thương mại điện tử.

| Nhánh | Kết quả rà | Đường đi tiếp |
|---|---|---|
| Seller workflow | Chọn SKU đã có; thiếu bước ưu tiên SKU nào và điều tra biến động | Đề xuất hàng đợi kiểm tra + giải thích số liệu |
| Identity/catalog/listing | SKU chung đã chốt; catalog hiện tĩnh, mapping ít biểu diễn lịch sử | Chốt khả năng biểu diễn thay đổi Listing/SKU trước nguồn lớn |
| Order/fulfillment | Lifecycle đã chốt; core hiện một delivered_at/fulfillment cho Order | Quyết định mức mô phỏng giao từng phần và grain quantity |
| Return/refund | Binary label đã chốt; thiếu lifecycle hoàn tiền độc lập | Phân biệt nhãn ML, tiến trình xử lý Return và tiền hoàn |
| Inventory/supply | Roadmap đã chọn; chưa có nguồn tồn, reservation, inbound | Quyết định đưa lát nhỏ vào lần nguồn mới hay hoãn rõ |
| Finance/settlement | Giá trị hàng đặt hiện có; chưa đủ net cash/profit | Ưu tiên đối soát nếu mở nguồn, chưa suy lợi nhuận |
| Traffic/ads | Chưa có exposure/spend/attribution | Roadmap riêng; không tính conversion/ROAS từ orders |
| Sales/customer analytics | Nhiều trường chưa đi hết tới Gold/API | Hoàn thiện Q28–Q30 trước thêm model mới |
| Synthetic validity | Sinh được volume nhưng giả định hành vi đơn giản | Tách benchmark hệ thống khỏi benchmark tính hữu ích |
| ML | Score Order Line, hành động theo SKU/Listing | Đánh giá tại cả grain dự đoán và grain hành động |
| Agent/RAG | Quyền/citation đã chốt; tool semantics cần thiết kế | Tool có metric contract, bounded queries và evidence |
| Serving/product UX | Version/freshness đã chốt | So sánh thế hệ báo cáo, giải thích missing/stale và drilldown |
| Data/platform | Version/PIT/stream/replay cần POC | Kiểm chứng khả năng adapter theo release chính xác |
| Cloud/operations | Budget và manual đã chốt, khả năng vận hành chưa đo | POC footprint, recovery và run budget theo nhóm service |
| Delivery/learning | Toàn rubric rất rộng | Lát đầu cuối nhỏ, mỗi lát có giải thích/code/proof |

## Những phát hiện vượt ra ngoài rà lỗi

### 1. Từ trang SKU sang danh sách việc nên kiểm tra

Q38 chấm Return-risk ở Order Line, nhưng Seller kiểm tra SKU/Listing.
Mean/max score không tự thành thứ tự hành động hợp lý: SKU ít đơn có score
cao có thể khác SKU nhiều đơn có mức rủi ro vừa. Cần đặc tả cách tổng hợp,
support/coverage, độ mới và lý do đưa vào danh sách. Đề xuất đánh giá top-K
SKU với cùng ngân sách kiểm tra K; không gọi tổng score là số return dự kiến
khi score chưa được hiệu chỉnh thành xác suất.

Một hành trình đáng làm thử: “SKU nào cần kiểm tra?” → “tại sao xuất hiện?”
→ so với kỳ trước/nhóm tương đồng → drilldown Order Line → đọc playbook có
trích dẫn. Gợi ý chỉ là kiểm tra, chưa tự thực hiện hành động bên ngoài.
Lưu ghi chú/bookmark hoặc trạng thái xử lý là một quyền ghi sản phẩm mới,
phải quyết định riêng; advisory/read-only hiện tại chưa bao hàm điều đó.

### 2. Phân tích biến động có thể hữu ích trước khi thêm ML

Từ metric đã chốt có thể đề xuất phân rã thay đổi giá trị hàng đặt theo units,
giá thực tế và mix SKU/Storefront trong cùng currency. Người dùng biết
“giảm vì bán ít hơn hay vì mix rẻ hơn”, thay vì chỉ thấy phần trăm giảm.
Phải chọn công thức attribution và cách phân bổ interaction, kiểm tổng đóng
góp khớp chênh lệch; phân rã số học không chứng minh nguyên nhân kinh doanh.

Thêm đối chiếu coverage trước cảnh báo: giảm sales vì thiếu export không
được báo là mất nhu cầu. Anomaly detection có thể bắt đầu từ rule/seasonal
baseline có bằng chứng; ML anomaly/forecast là lựa chọn sau, cần benchmark
và dữ liệu lịch sử phù hợp.

### 3. Lifecycle cần grain đủ dùng cho nhánh sau

Core hiện có `Order.delivered_at`, một `fulfillment`, và `OrderLine.quantity`
nhưng không có shipment-line quantity hoặc nhiều lần return/refund
([model.py](../generator/core/model.py)). Mọi line Shopify được suy trạng
thái fulfillment từ cùng trạng thái Order
([renderer](../generator/channels/shopify.py)). Nếu thêm inventory hoặc
partial fulfillment sau khi sinh nguồn lớn, có thể phải sửa simulator,
renderer, keys, labels, Silver/Gold và phần đối soát cùng lúc.

Đề xuất xem xét các thực thể quan sát độc lập: Order Line; lần giao + line/
quantity; yêu cầu/nhận Return + line/quantity; Refund transaction + amount/
status; inventory movement + location/pool. Đây là các grain đề xuất, không
phải yêu cầu dựng mọi bảng ngay. Khi giao một line nhiều lần, quy tắc ngày
giao dùng cho label 30 ngày phải được quyết định; không tự đổi nhãn Q41.

### 4. Nhu cầu và doanh số cần tách nếu mở inventory

Đơn hàng chỉ cho thấy lượng bán quan sát được. Muốn gợi ý nhập hàng cần
biết thời gian Listing active, có hàng để bán, tồn theo pool/location,
reservation, inbound và lead time. Các Channel có thể dùng chung pool:
cộng stock của mỗi Channel có thể đếm cùng hàng nhiều lần.

Lát mở rộng nhỏ có thể chỉ chứng minh inventory ledger và số ngày có hàng,
chưa cần tối ưu mua hàng hay dự báo demand. Nếu chưa chọn mở inventory,
forecast nên mang tên dự báo lượng bán quan sát, nêu giới hạn stockout và
chưa đưa thành khuyến nghị số lượng nhập. Q31 vẫn là roadmap cho tới khi
người dùng chọn nâng scope.

### 5. Catalog và Listing là một nguồn có giá trị riêng

Danh mục hiện có SKU/category/cost và popularity tĩnh
([catalog.py](../generator/core/catalog.py)). Chưa có lịch sử Listing active/
inactive, thay mapping, đổi giá, product variant hoặc thuộc tính size/color.
Nguồn Listing có thể giúp phân biệt SKU không bán được do chưa active với
SKU active nhưng không có sales; thuộc tính quan sát được cũng giúp nhóm
SKU tương đồng. Không xuất latent `defect_propensity` để thay nguồn này.

Q34 đã chốt SKU ổn định và mapping Listing. Phần cần quyết định thêm là mức
mô phỏng lịch sử của mapping/thuộc tính và xử lý SKU mới/ngừng bán; chưa cần
chuyển sang product recommender hoặc entity resolution tự động.

### 6. Sinh nhiều dữ liệu không tự làm benchmark thuyết phục

SKU được chọn gần độc lập theo popularity; buyer rút từ pool; return dùng
hàm logistic cố định của một số thuộc tính
([simulate.py](../generator/core/simulate.py),
[returns.py](../generator/core/returns.py)). Điều này hữu ích cho hệ thống
test có kiểm soát, nhưng repeat customer/basket/model có thể chỉ học lại
quy tắc sinh. Không dùng cùng hàm tính của simulator làm expected output duy
nhất cho mọi phép đánh giá.

Đề xuất ba nhóm kiểm chứng: fixture nhỏ tính tay cho logic; dataset scale
cho skew/throughput; bộ đánh giá giữ riêng seed/regime/scenario cho ML/agent.
Đổi seed vẫn giữ cùng cơ chế, vì vậy cần cả stress case đổi cơ chế hoặc
missingness khi muốn đo độ bền. Ghi rõ điều gì là truth riêng dùng chấm test,
điều gì Seller thực sự quan sát được và được phép dùng làm feature.

Khi tăng lịch sử từ 180 lên ~365 ngày, phải dời lịch sale/live/pacing theo
cutoff mới và kiểm đặc tính dữ liệu lại. Config hiện đặt live ngày 195 và
validator yêu cầu live thuộc stream; chỉ thay `days_history` sẽ không đủ
([config](../generator/config.py), [cấu hình](../configs/default.yaml)).

## Danh mục cơ hội và chi phí tương đối

Nghiên cứu chi tiết có nguồn gốc và các giới hạn tại:

- [Nguồn nghiệp vụ](research-business-expansion-20261017.md): năm miền nguồn,
  năm quyết định nền B1–B5 và sáu fixture chéo miền.
- [ML, insight và agent](research-ml-agent-expansion-20261017.md): mười thí
  nghiệm M01–M10, grain hành động, coverage và đánh giá độc lập.
- [Lựa chọn nền tảng](research-platform-options-20261017.md): mười phép thử
  PF01–PF10 cho adapter, publication, CPU, Kubernetes và phục hồi.

“Nhẹ/vừa/lớn” dưới đây là so với nền FSDS đã có, không phải ước lượng giờ
hoặc tiền. Mỗi nhánh vẫn cần đo nhỏ và chốt output trước triển khai.

| Cơ hội | Dữ liệu tối thiểu | Đầu ra/kiểm chứng | Chi phí và khuyến nghị |
|---|---|---|---|
| Hoàn thiện analytics Q28–Q30 | Các trường đã có trong nguồn, coverage rõ | Gold/API/UI đối soát cùng cutoff | Vừa; đã trong scope, ưu tiên |
| Giải thích biến động | Metric contract, units/value/mix kỳ hiện tại và trước | Các đóng góp cộng lại đúng chênh lệch | Nhẹ–vừa; đề xuất sớm |
| Hàng đợi SKU cần kiểm tra | Score/metric, support, coverage và lý do | Precision/lift@K hoặc fixture ưu tiên có giải thích | Vừa; đề xuất sớm |
| Cảnh báo bất thường có kiểm coverage | Series ngày, feed completeness, baseline | Inject thiếu file/sale/burst; phân biệt cảnh báo data và business | Vừa; đề xuất rule trước |
| Listing lifecycle | Mapping + effective/observed times + active status | Không-sale khi inactive khác active; mapping lịch sử đúng | Vừa; ưu tiên thiết kế nguồn |
| Partial shipment/return/refund | Sự kiện theo line/quantity và transaction | Đối soát số lượng/tiền; label policy cho giao nhiều lần | Lớn ở simulator; quyết định trước nguồn lớn |
| Inventory ledger nhỏ | Pool/location, movements, reservation, inbound | Stock khớp movement và snapshot; không đếm trùng giữa Channel | Lớn; ứng viên nâng Q31 vào lát kế tiếp |
| Forecast lượng bán | Series đủ lịch sử + backtest, exposure nếu có | So baseline; missing/stockout/cold-start tường minh | Vừa–lớn; sau metric và inventory decision |
| Đối soát settlement | Payout/fees/refund transaction cùng currency | Chênh lệch khớp transaction hoặc được giải thích | Lớn; trước khi hứa profit |
| Profit/FX/ads attribution | Cost history, phí, thuế, FX hoặc spend/exposure | Contract attribution và kiểm tiền tệ | Rất lớn; roadmap riêng |
| Agent giải thích bằng evidence | Semantic tools, metric definitions, lineage/corpus version | Claim đối chiếu số liệu, citation, abstention | Vừa; mở sâu tính hữu ích thay tăng số agent |
| Saved investigation/feedback | State sản phẩm + user intent rõ | Khôi phục phiên bản phân tích; phân biệt accept với hiệu quả | Vừa; optional vì thêm quyền ghi |

## Agent và nền tảng: các lựa chọn mới đáng kiểm chứng

**Metric contract chung:** UI, tool agent và feature phải dùng cùng định
nghĩa grain, cửa sổ, tiền tệ, additive rules, missing/coverage và version.
Đề xuất tool nghiệp vụ hẹp: lấy metric, so kỳ, phân rã, drilldown giới hạn,
lấy evidence risk và tra playbook. Khi một câu nói vừa có số liệu vừa có
khuyến nghị, phần số trỏ generation/metric result; phần khuyến nghị trỏ
rule/playbook. Citation tới một chunk không tự chứng minh con số đúng.
Tool-call, retry, token và thời gian mỗi request cần giới hạn tổng để hai
agent không gọi vòng; cache key phải bao gồm các version liên quan.

**Độ chắc chắn và coverage:** bổ sung funnel số line đặt → giao quan sát
được → đủ maturity → feed đủ → có feature → được score. Tỷ lệ và xếp hạng
đi kèm support/coverage. Interval hoặc category prior là ứng viên thử, chưa
thay chính sách abstain khi ít lịch sử. Kiểm agent bằng numeric oracle,
retrieval, citation và abstention riêng; không dùng chính LLM trả lời làm
người chấm duy nhất. Các phép thử M01–M10 nằm trong note ML/agent.

**Feast phải pin đường thực thi:** tài liệu contrib và mã `master` đọc được
có khác biệt. Mã Spark có cờ lọc theo created timestamp mặc định tắt và có
đường IcebergSource riêng; một helper đọc qua Arrow cần đo bộ nhớ driver.
Đây là tín hiệu để POC release cụ thể, không phải runtime FSDS đã có các
khả năng đó. So với SparkSource qua catalog trên cùng fixture known-at và
thử tăng dữ liệu nhỏ. [Mã nguồn adapter](https://raw.githubusercontent.com/feast-dev/feast/master/sdk/python/feast/infra/offline_stores/contrib/spark_offline_store/spark.py).

**RAG có phương án vẫn qua Feast:** POC tabular Spark/Iceberg→Redis và RAG
Feast→PostgreSQL/pgvector với cấu hình/registry tách rõ. Đây là ứng viên
Q44, không chuyển Redis tabular thành PostgreSQL và không bỏ yêu cầu Feast.
Cần đo contention, footprint, embedding/corpus version và update/delete.
[Feast PostgreSQL](https://docs.feast.dev/reference/online-stores/postgres).

**CPU và Kubernetes:** đo engine CPU, llm-d rồi toàn chuỗi coordinator/agent
riêng; số agent replica không đại diện throughput inference. KFP standalone
và KServe Standard là ứng viên giảm phần phụ trợ; vẫn phải có proof rubric
trên topology được chọn. Các ví dụ cấu hình trên tài liệu không là tài nguyên
tối thiểu hay bằng chứng vừa quota trial. PF06–PF08 xác định footprint nhóm
tích hợp, không chỉ đo từng công cụ rời.

**On-demand cần đủ state:** ngoài raw/PG/catalog còn có registry/artifact,
Kafka offset, Flink checkpoint/savepoint, orchestration metadata và secret
bootstrap. Phải chọn giữ hay replay cho mỗi loại, rồi restore từ môi trường
trống để tránh dựa vào cache laptop. Retention phải bảo vệ graph tham chiếu
của generation/model/corpus; DataHub lineage không tự thi hành quyền xóa.
Các ứng viên và nguồn chính thức nằm trong note nền tảng.

Những phát hiện kỹ thuật mới yêu cầu cập nhật giả định của POC, chưa thay
policy hoặc đóng gate. Khẳng định cũ như “Spark adapter không ghi offline”
phải hiểu theo phiên bản đã khảo sát; không dùng như hạn chế bất biến.

## Cây quyết định cần đi tiếp

1. **Độ sâu nguồn nghiệp vụ**: lifecycle ở grain Order hay hỗ trợ partial
   line/quantity? Nếu partial: chọn delivery anchor/label, quantity accounting,
   return/refund statuses và ví dụ độc lập trước schema vật lý.
2. **Phạm vi inventory**: giữ roadmap hay đưa inventory ledger nhỏ vào lần
   nguồn mới? Nếu đưa vào: chốt pool/location/reservation/inbound và exposure;
   forecast/replenishment phụ thuộc câu trả lời này.
3. **Trải nghiệm kiểm tra**: giữ chọn SKU để xem hay thêm hàng đợi ưu tiên và
   giải thích biến động? Nếu thêm: chốt grain hành động, K, evidence và metric
   hữu ích; lưu trạng thái/ghi chú là nhánh quyền ghi riêng.
4. **Nguồn bổ sung khác**: Listing lifecycle, settlement, traffic/ads. Không
   gom chúng thành một lựa chọn “thêm dữ liệu”; mỗi nguồn có cost/contract.
5. **Độ mạnh benchmark**: bộ nhỏ/scale/held-out khác nhau; chốt các scenario
   kinh doanh cần chứng minh sau lựa chọn nguồn, trước sinh full-scale.
6. **Tính khả thi kỹ thuật**: pin release và POC các phụ thuộc; kết quả fail
   quay về đúng nhánh contract/backend/SLA, không tự thay phạm vi đã duyệt.

Q52–Q55 đã giải quyết các nhánh scope tương ứng trong cây trên. Delivery
anchor/qualification của nhãn, topology stock và queue priority đã được
Q56–Q59 chốt tiếp; replenishment, forecast model và quyền ghi chưa tự mở.

## Cách mở rộng được đề xuất

Ưu tiên làm sâu **nguồn có thể giải thích được → metric có căn cứ → Seller
biết cần kiểm tra gì**, giữ return-risk làm bài toán ML chính. Không đề xuất
thêm đồng thời demand forecasting, pricing optimization và profit model vào
v1. Nếu mở inventory, bắt đầu ledger/exposure với vài kịch bản có thể đối soát;
chỉ bổ sung replenishment khi nguồn và benchmark đủ.

Trước khi khóa implementation ở quy mô lớn cần hoàn thành vòng lựa chọn
scope này, contract mẫu, dependency POC và acceptance. POC là bước xây nhỏ
để kiểm chứng lựa chọn, không thể thay bằng rà tài liệu. Không cần chốt sớm
mọi library hoặc triển khai
mọi nhánh đã khảo sát. Lát đầu cuối đầu tiên nên nhỏ về volume nhưng phủ các
trường hợp có thể làm đổi grain/keys/time; tăng volume sau khi qua gate.

## Q52–Q55 — đã được xác nhận ngày 2026-10-18

Người dùng đã đồng ý cả bốn theo đề xuất giới hạn dưới đây. Scope này đã
được cập nhật vào plan/ledger/readiness và ADR-0009; chưa có implementation.

| Câu | Scope đã chốt | Ranh giới | Câu phụ thuộc còn mở |
|---|---|---|---|
| Q52 | Partial line/quantity, Return/Refund lifecycle độc lập | Fixture nhỏ trước; label v2 theo Q56/Q57 | Đã chốt policy, còn source contracts/POC |
| Q53 | Mapping/active status/thuộc tính Listing có lịch sử | Observed history; chưa bundle/kit hoặc tự sửa Listing | Contract effective/known-time và source coverage |
| Q54 | Inventory nhỏ ở phạm vi kế tiếp | Pool/location + available/reservation/inbound; chưa tối ưu đặt hàng | Q58 đã chốt shared/dedicated pools và movement authority |
| Q55 | Hàng đợi SKU read-only + phân rã biến động + evidence drilldown | Chưa thêm state ghi/export/notification | Q59 đã chốt ba nhóm, rank riêng |

Q56/Q57 chốt label v2 theo allocation, approved Return/product Refund success
và maturity/coverage; Q58 chốt inventory topology/authority/quarantine;
Q59 chốt ba nhóm queue riêng. Contract nhãn và expected L01–L15 tại
[return-label-contract.md](return-label-contract.md). Còn schema vật lý,
capability matrix, rule/threshold benchmark và POC, không còn frontier
policy Q52–Q59 chờ trả lời. Finance/ads/profit, model forecast thứ hai và
saved investigation tiếp tục là các nhánh đã khảo sát nhưng chưa chọn.
