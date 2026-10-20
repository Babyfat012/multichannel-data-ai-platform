# Audit độc lập: thời gian, nhãn và đánh giá sau Q59

Ghi chú sau xử lý cùng ngày: đây là snapshot phân tích trước sửa code;
[bản xử lý](audit-remediation-20261018.md) và
[sổ IR](integration-risk-register.md) ghi trạng thái hiện hành. Contracts
mới cụ thể hóa giải pháp nhưng chưa đóng các POC temporal/runtime.

Ngày 2026-10-18. Đối chiếu tài liệu/code hiện có; **không chạy POC, không
đổi policy Q56/Q57, không chứng minh runtime đạt**. Các phản ví dụ dưới đây
là suy luận thiết kế từ nguồn nội bộ được dẫn cạnh từng phát hiện. Không cần
nguồn bên ngoài để xác lập chúng.

Đọc [contract nhãn v2](return-label-contract.md), [scope mở rộng](expanded-scope-design.md),
[readiness](implementation-readiness.md), [nghiên cứu ML](research-ml-agent-expansion-20261017.md),
[ADR-0010](adr/0010-shipment-windows-return-label-v2.md) và source generator/feature.
Các note nghiên cứu cũ có phần ghi đề xuất trước khi Q56–Q59 được duyệt;
policy được đối chiếu theo contract/ADR đã accepted.

## Kết luận

Policy đã rõ hơn nhưng còn tám seam phải đặc tả trước full-scale. Sáu seam
đầu là hoàn thiện cách thực thi các quyết định đã duyệt, không cần đổi
target hoặc hỏi lại người dùng. T07 cần đo/ghi rõ giới hạn diễn giải;
T08 cần chốt hướng nguồn hoặc phạm vi model khi capability không đủ.
Không nên biến việc đã có maturity/PIT thành khẳng định đã đóng toàn bộ
thời gian: còn thời điểm sinh Order, kết thúc observation, phiên bản nhãn,
thời điểm model được train và đối tượng bị ảnh hưởng bởi dữ liệu unmatched.

| ID | Phát hiện | Mức ưu tiên | Loại xử lý |
|---|---|---|---|
| T01 | Thiếu horizon theo dõi sự kiện sau ngày cuối nhận Order | Trước sinh nguồn | Contract/fixture theo policy cũ; sizing cần POC |
| T02 | `label_available_at` đơn lẻ không biểu diễn lịch sử correction của nhãn | Trước schema label | Contract bitemporal theo Q36/Q56/Q57 |
| T03 | Temporal split theo cohort giao có thể dùng model chưa tồn tại lúc đặt | Trước benchmark | Quy tắc evaluation theo Q38 |
| T04 | Refund không map line có thể làm các line bị gán negative sai | Trước label transform | Quy tắc uncertainty theo Q57 |
| T05 | Chia allocation làm thay đổi xác suất nhãn nếu sinh Bernoulli ngây thơ | Trước generator v2 | Simulator invariant; không đổi grain nhãn |
| T06 | Mapping SKU sửa sau bán làm lẫn identity lúc chấm điểm với restated analytics | Trước feature/training schema | Bitemporal lineage và compatibility |
| T07 | Approval-delay/policy regime có thể đổi metric mà không đổi lỗi hàng | Trước dashboard/eval | Metric wording, diagnostics; stress fixture |
| T08 | Core Orders đủ năm Channel không đồng nghĩa đủ nguồn nhãn v2 | Trước chốt source ownership | Quyết định nguồn bổ sung/phạm vi model, rồi POC capability |

## T01 — Ba horizon, không phải chỉ history + stream

**Nguồn.** [GeneratorConfig.stream_end](../generator/config.py) hiện lấy
`days_history + days_stream`; [stream_messages](../generator/delivery/stream_events.py)
chỉ lấy lifecycle event có event time trước `stream_end`. Lookback 20 ngày là
vấn đề đã được [readiness](implementation-readiness.md) ghi nhận. Phát hiện
mới ở đây là **đuôi sau ngày cuối**, không phải lặp lại lookback trước cutoff.
[Contract](return-label-contract.md) còn đòi fulfillment terminal, maturity
và feed complete, không chỉ hết 30 ngày từ lúc đặt.

**Phản ví dụ.** Ngày cuối stream là D395. Order đặt D394, giao đợt cuối
D402, approved Return D431 và quan sát D434. Target vẫn hợp lệ vì D431 nằm
trong cửa sổ của phần giao D402. Nếu dừng mọi nguồn ở D395 thì line không
bao giờ đủ nhãn; kéo tới D425 cũng chưa đủ. Nếu chèn nhãn truth ẩn trực tiếp
để bù đuôi sẽ phá coverage/known-time đã chốt.

**Ảnh hưởng.** Nhóm streaming mới nhất chỉ có điểm dự báo mà không có outcome
để kiểm; cấp coverage bằng đồng hồ wall-clock dễ gán negative sai. Khi thêm
delayed shipment/cancel/approval, kéo dài nguồn cố định 30 ngày không tự đủ.

**Đề nghị.** Manifest phân biệt `order_generation_end`, `business_event_end`,
`source_observation_end`; thời gian chạy/pacing là mốc thứ tư riêng. Có thể
ngừng tạo Order rồi tiếp tục lifecycle/feed cho các Order đã có, hoặc chấp
nhận right-censoring và báo số mẫu bị loại. Delay không có trần không thể
được chứng minh hết chỉ bằng một đuôi hữu hạn: cần cutoff quan sát và coverage
đúng phạm vi. Không tự kết thúc phần quantity treo để làm nhãn chín.

**Pass T01.** Fixture trên vẫn thấy Order/score ở D394; D395 immature; chỉ khi
terminal + hết deadline + complete mới có label. Một case ngoài observation
horizon phải còn censored/unknown, không 0. Manifest và dự toán bytes/runtime
phải tính cả tail; tổng mẫu mature/censored theo ngày đặt phải khớp tổng eligible.

## T02 — Nhãn có lịch sử phiên bản, không chỉ ngày đầu đủ hạn

**Nguồn.** [LABELS_SCHEMA](../generator/generate_order_snapshots.py) chỉ có label, delivery và
`label_available_at`; [_collect_labels](../generator/generate_order_snapshots.py) gán availability
bằng delivery +30 ngày. [Contract](return-label-contract.md) đã cho correction
đổi nhãn và thậm chí chuyển mature về immature/unknown. [Readiness](implementation-readiness.md)
đòi ba cutoff riêng nhưng chưa định dạng record nhãn có revision.

**Phản ví dụ.** Giao D0. Ở snapshot D35 nguồn complete và nhãn 0; correction
quan sát D40 bổ sung approval hiệu lực D10 → nhãn 1; correction D50 sửa lần
giao cuối thành D45 → line lại immature đến D75. Một cột availability D30
và một current label không trả lời được nhãn training run D36/D46/D56.

**Ảnh hưởng.** Rebuild model cũ đọc label current có thể đổi target âm thầm.
Ngược lại, lọc mọi label/correction theo **feature cutoff lúc đặt đơn** sẽ
loại cả outcome tương lai hợp lệ và không train được đúng target.

**Đề nghị.** Revision label giữ identity, policy version, state/value,
`label_as_of`/observation cutoff, observation validity interval hoặc source
snapshot IDs, terminal/coverage evidence, `maturity_end`, reason thay đổi.
`first_eligible_at` nếu có chỉ là một thuộc tính, không thay observation
version. Training manifest pin feature cutoff từng mẫu và **label observation
snapshot trước training cutoff**; feature D0 không nhận correction D40,
nhưng nhãn cho model train D46 được phép nhận correction D40. Giữ materialized
training dataset cũ bất biến. Báo eval as-known và eval restated bằng hai tên.

**Pass T02.** Cùng line trả đúng 0 / 1 / immature ở ba snapshot trên; model run
cũ tái lập checksum sau correction. Sửa label không làm feature quá khứ đổi;
đổi eligibility được đếm và giải thích, không chỉ đếm số 0→1.

## T03 — Model cũng phải tồn tại trước prediction timestamp

**Nguồn.** Q38 tại [discovery](de-discovery-risk-review.md) và
[readiness](implementation-readiness.md) xác định score lúc đặt Order,
train trên nhãn đã biết tại training cutoff. [Contract](return-label-contract.md)
lại xác định cohort bằng ngày giao cuối. Đây là hai trục hợp lệ nhưng không
thể dùng thay nhau trong chia tập.

**Phản ví dụ.** Model train D60 chỉ dùng nhãn hợp lệ biết trước D60. Test
cohort giao D65 chứa Order đặt D10 nhưng giao chậm tới D65. Tất cả feature
của Order đều PIT D10; nhãn chỉ dùng sau D95. Tuy vậy dùng model train D60
để chứng minh dự báo **tại D10** là sai: model đã học từ D11–D59, dù không
có cùng Order ở train. Điều kiện không trùng Order và feature PIT đều chưa
chặn được lỗi này.

**Đề nghị.** Fold evaluation chính theo prediction/feature timestamp lúc
đặt. Với model cố định, mọi test prediction phải sau training/model-ready
cutoff; với rolling retrain, mỗi row pin đúng model sẵn có trước prediction.
Delivery cohort dùng phân tích outcome/maturity, không quyết định model
được phép dùng. Transform, prior, calibration, threshold và chọn K cũng
fit/tune ở lịch sử tương ứng; report test outcomes có cutoff muộn riêng.
Nếu chạy model mới chấm đơn cũ để khảo sát thì đặt tên retrospective scoring,
không tính nó là proof deployed-at-order.

**Pass T03.** Fixture D10/D60/D65 bị loại khỏi fold deployed-at-order của model
D60, hoặc dùng model đã có ở D10. Kiểm model-ready <= prediction timestamp
trên mọi prediction, ngoài kiểm label-known < training cutoff. Không chỉ
assert max(train delivery_date) < min(test delivery_date).

Lưu ý clock domain: mọi bất đẳng thức model-ready/prediction phải trong
cùng đồng hồ. Offline replay có simulated knowledge/training cutoff và
actual artifact creation time riêng; không so artifact tạo hôm nay với
Order giả lập năm trước rồi backdate artifact để giả deployed-at-order.
Forward backtest chứng minh information protocol, không chứng minh hạ tầng
đã thực sự chạy ở thời điểm mô phỏng (D25).

## T04 — Unmatched event phải tạo vùng bất định tường minh

**Nguồn.** [Contract](return-label-contract.md) cần linkage tới allocation;
[scope](expanded-scope-design.md) giữ unmatched và không join theo SKU gần
giống. Nhưng chưa đặc tả **line nào bị ảnh hưởng** khi một event chỉ map tới
Order/header, hoặc header có một transaction success chưa map line.

**Phản ví dụ.** Order O có line A giao D0 và B giao D5. Product refund thành
công D10, chỉ có Order ID. Feed đầy đủ và tổng tiền khớp; chưa biết refund
thuộc A hay B. Nếu anti-join qualifying event đã map vào mỗi line rồi lấy
missing =0, cả A/B đều negative, dù ít nhất một line có thể positive. Ngược
lại, gán cả hai positive cũng sai. Coverage feed 100% không chứng minh
coverage linkage 100%.

**Đề nghị.** Mỗi unresolved event giữ candidate scope bằng namespace,
Order/line/allocation đã biết và ràng buộc time/quantity/amount đáng tin;
không đoán match. Các line còn khả năng bị event ảnh hưởng không đủ điều
kiện label chuẩn cho tới khi resolve hoặc chứng minh event không qualifying
cho chúng. Event thiếu Order ID có thể làm phạm vi bất định lớn hơn;
contract nguồn phải định nghĩa envelope thay vì bỏ event. Không dùng số tiền
trùng ngẫu nhiên làm proof allocation. Theo policy bảo thủ hiện có, line đã
có positive nhưng còn thiếu dữ liệu bắt buộc vẫn không vào tập chuẩn.

**Pass T04.** Case trên A/B unknown, không 0/1; khi source version sau map về
A thì recompute A1/B0 khi mọi điều kiện khác đủ. Báo riêng feed completeness,
line linkage coverage và số line bị một unmatched event ảnh hưởng; không
chia tổng event matched cho tổng event rồi gọi đó là coverage label.

## T05 — Shipment refinement không được tự sinh thêm rủi ro

**Nguồn.** [return_probability](../generator/core/returns.py) hiện là p theo
**line**, đã có quantity effect. [decide_return](../generator/core/returns.py)
rút một lần rồi quyết định request/nhãn. [Contract](return-label-contract.md)
OR sự kiện allocation thành một line; scope không đổi thành mẫu unit.

**Phản ví dụ.** Nếu generator v2 áp cùng p=0,1 độc lập cho mỗi allocation,
line một allocation có xác suất 0,1; tách thành hai allocation thành
`1 - (1 - 0,1)^2 = 0,19`, ba thành 0,271. Chỉ tách một shipment thành hai
record cùng thời gian/cùng units underlying đã làm target đổi. Nếu quantity
đã ảnh hưởng p rồi lại rút từng unit một cách ngây thơ, tác động quantity
cũng có thể bị tính hai lần.

**Đề nghị.** Chọn và version cơ chế latent lifecycle nhất quán: ví dụ sinh
intent/affected quantity ở grain đã định rồi map units sang shipments, sau
đó sinh request/approve/refund/time; hoặc mô hình unit/quantity explicit
với baseline được xác định lại. Không có một lựa chọn library đúng duy nhất.
Giữ policy OR v2. Seed/RNG key bảo đảm thay format/đóng gói allocation không
đổi underlying outcome khi semantics không đổi. Số shipment tương lai,
delivered quantity cuối và allocation linkage tương lai không được lén thêm
vào feature order-time để model học target dễ hơn.

**Pass T05.** Metamorphic fixture tách/gộp record nhưng giữ cùng units,
delivery timestamps và latent events phải giữ label, deadlines, money và
quantity. Đổi delivery time thật có thể đổi label và phải được giải thích.
Báo prevalence theo ordered/delivered units, allocations, fulfillment và
policy regime; p=0,1 ở trên là phản ví dụ tính tay, chưa là benchmark simulator.

## T06 — Identity lúc dự báo và mapping restated là hai lineage khác nhau

**Nguồn.** [Scope Listing](expanded-scope-design.md) giữ effective/known-time
và mapping tại sale time; correction được phép restate generation mới.
[Feature](implementation-readiness.md) dùng SKU + Storefront, trong khi
label identity theo native line. [Job hiện tại](../spark_jobs/gold/build_order_snapshot_return_features.py)
chọn `seller_sku` từ Gold current, minh họa rủi ro khi mở lịch sử mapping.

**Phản ví dụ.** Lúc đặt D10, Listing X được biết là SKU A và model đọc
history A. D40 nhận correction cho biết từ D1 X thực ra là B. Analytics
restated đúng là B; nếu training join current SKU B để dựng feature D10,
model đã được dùng mapping không có ở D10. Nếu join prediction cũ theo B,
queue A/feature A và outcome B cũng mất lineage dù Order Line ID còn nguyên.

**Đề nghị.** Prediction/training record pin native Order Line identity,
Listing identity, mapping version/SKU **as known tại feature cutoff**,
feature generation và model. Restated metric giữ mapping version riêng.
Label join qua immutable native line, không qua current SKU. Queue báo rõ
thực thể/evidence đang dùng generation nào; không tự gán lịch sử score A
thành score của B. Correction thay label cohort không đồng nghĩa sửa feature
identity cũ. Đây là triển khai bitemporal của Q53/Q36, không yêu cầu mở entity
resolution ngoài scope.

**Pass T06.** Replay D10 đọc A; analytics restated sau D40 có thể đọc B;
prediction cũ vẫn truy được feature A và outcome đúng native line. Rebuild
training cũ không đổi feature vì mapping current đổi. Số mẫu không tăng/giảm
do một prediction bị join cả hai phiên bản mapping.

## T07 — Target đã gồm tác động của quy trình approval

**Nguồn.** [L03](return-label-contract.md) chủ ý cho request D29, approved
D31 thành 0; [Q57](expanded-scope-design.md) dùng thời điểm approved/success,
không phải request. [Simulator cũ](../generator/core/returns.py) chỉ có
request/refund delay, chưa có approval mechanism. Đây không phải lỗi policy:
người dùng đã duyệt target; là hệ quả cần đi hết tới proof/UI.

**Phản ví dụ.** Hai kỳ có cùng 100 deliveries, cùng 10 request D29 và cùng
lỗi hàng. Kỳ A approve ngay D29 →10 positive. Kỳ B backlog approve D31 →0
positive, dù feed đầy đủ và request không giảm. Nếu UI gọi đó là “chất lượng
SKU cải thiện, return giảm” hoặc model/agent giải thích bằng defects là sai.
Tương tự, Channel có refund-only product success có thể khác Channel chỉ
quan sát approval; không thể coi capability thiếu là cùng target đo đầy đủ.

**Đề nghị.** Metric tên đầy đủ “line có approved Return hoặc product Refund
success trong 30 ngày theo label v2”; tooltip/agent tool contract nêu điều
kiện delivery, maturity, coverage/linkage và policy. Diagnostic tách request,
approval latency, approved, refund success và physical receipt khi nguồn có
khả năng. Simulator scenario/version ghi approval/refund processing regime;
stress fixture đổi delay/capability mà giữ underlying intents. Không tự đổi
target sang request, không gọi score là defect probability. Nếu sau này muốn
target request-time/physical return thì đó là quyết định nghiệp vụ mới.

**Pass T07.** Fixture A/B cho label 10/0 đúng contract nhưng lời giải thích
chỉ nêu thay đổi qualification timing quan sát được, không suy quality. Model
eval báo strata/regime/coverage; abstain khi không chứng minh được target
đầy đủ, không dùng “feed trống” làm mọi label 0.

## T08 — Nguồn quan sát đủ nhãn là quyết định kiến trúc, không chỉ thêm field

**Nguồn.** [Amazon renderer](../generator/channels/amazon.py) chủ ý không
biểu diễn actual delivery; `DELIVERED` và `COMPLETED` đều map thành `Shipped`
trong [status map](../generator/channels/amazon.py), chỉ emit promised dates
ở [render_v0](../generator/channels/amazon.py). Header của
[Shopify renderer](../generator/channels/shopify.py) và
[Shopee renderer](../generator/channels/shopee.py) cũng chủ ý bỏ actual
delivery. Đây là mô tả source mô phỏng hiện có, **không phải kết luận mọi API
endpoint của Channel thật đều không có khả năng lấy thông tin này**.
[Scope](expanded-scope-design.md) đã yêu cầu capability matrix nhưng chưa
chọn producer/authority thay thế khi field bắt buộc không có.

**Phản ví dụ.** P1 đủ Order của cả năm Channel và Gold/PG count đối soát tốt.
Riêng ba source trên không có actual allocation delivery. Theo v2 không thể
biết deadline chính xác, dù biết request/refund và chờ đủ 30 ngày theo promised
date. Gán maturity bằng promise là sai contract; lấy hidden `order.delivered_at`
để gắn nhãn rồi tuyên bố chứng minh từ nguồn Seller quan sát được là sai
provenance. Gating trung thực có thể làm một số Storefront không có mẫu train.

**Hai hướng hợp lệ để chốt.**

1. Thêm nguồn bổ sung mô phỏng Seller OMS/WMS/carrier/post-sales khi phù hợp,
   ghi rõ producer, authoritative facts, permission/availability assumptions,
   identity bridge và observation delay; giữ native Orders nguyên giới hạn.
   Không khẳng định nguồn bổ sung Seller biết được mọi allocation Channel-managed
   nếu chưa có contract. Truth ẩn vẫn chỉ là oracle. Nguồn bổ sung phải phát
   event qua Bronze/Silver như nguồn thật, không bypass trực tiếp vào features.
2. Giữ nguyên sources/capability: P1 có năm Channel nhưng v2/model chỉ phục vụ
   phạm vi có đủ actual delivery, qualification, linkage và coverage; phần
   khác hiển thị unsupported/unknown và analytics quan sát được, không score
   giả. Báo coverage mẫu/train/serve tường minh trong nghiệm thu.

**Phân loại quyết định.** Gating unknown đã được policy cho phép và phải làm
ngay trong thiết kế. Việc thêm **producer/authority nguồn bổ sung** hoặc chấp
nhận phạm vi model hẹp hơn mục tiêu là lựa chọn tác động sản phẩm/proof; cần
người dùng xác nhận hướng khi contract capability cho thấy không đủ. Q52 chấp
nhận partial lifecycle chưa tự chứng minh Seller có mọi feed cần thiết. Không
cần hỏi lại Q56/Q57 và không lấy một nguồn giống nhau cho năm renderer để
né vấn đề.

**Pass T08.** Matrix từng Storefront/feed liệt kê đủ actual delivery, allocation,
approval event/time, product refund success, completeness và source owner.
Ít nhất một fixture thiếu actual delivery phải không sinh label chính thức;
fixture dùng nguồn bổ sung phải truy mỗi field nhãn về payload/observed time
của nguồn bổ sung đó. Output/model report nêu rõ Channels có dữ liệu quan sát,
có đủ nhãn, được train và được serve; bốn phạm vi không mặc định bằng nhau.

## Phần không tự mở rộng

Không đề xuất tự thêm survival model, dự báo delivery, causal treatment,
calibrated probability, thay target thành physical-return hay weighting
missing-label để “sửa bias”. Q38 chủ ý conditional-delivered, Q56/Q57 đã
chốt historical event/OR. Các phương án đó nếu muốn theo đuổi cần quyết định
riêng. Những fixture trên chỉ làm policy hiện tại tái lập và không tự mâu thuẫn.

Trước full-scale: đưa T01/T04/T05/T06 vào contract G0; T02/T03 vào schema và
evaluation G2; T07 vào metric/tool contract và stress evaluation G3. Đây là
đề nghị ánh xạ, chưa có scenario/evidence mới được chạy đạt. T08 cần chốt hướng
nguồn trước G0 rồi mới tính volume và coverage của model.
