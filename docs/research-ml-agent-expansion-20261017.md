# Khai thác thêm ML, insight và agent trước triển khai FSDS

Ngày nghiên cứu/truy cập nguồn: **2026-10-17**. Đây là đề xuất nghiên cứu và
thiết kế thí nghiệm; chưa sửa policy Q1–Q51, chưa thêm tính năng vào scope v1,
chưa chạy POC, train model hoặc thao tác Cloud. Các ngưỡng ví dụ là cấu hình
thử nhỏ đề xuất, không phải SLA hay bảo đảm chất lượng đã chốt.

Đã đối chiếu [plan tổng](high-level-architecture-plan.md),
[readiness](implementation-readiness.md), [discovery](de-discovery-risk-review.md),
[expansion](de-expansion-review.md) và generator. Mục tiêu vòng này là tìm
thêm giá trị khai thác cùng các quyết định nền phải đóng trước khi xây.

## Kết quả chính

Hướng có giá trị nhất là biến insight/score thành **hồ sơ điều tra có bằng
chứng cho Seller**: điều gì thay đổi, thay đổi ở nhóm nào, dữ liệu đủ tin cậy
không, nên kiểm tra gì tiếp theo. Hướng này tận dụng Q29/Q30/Q38/Q39 và
generation đã có, trước khi tăng số mô hình hoặc thêm nguồn mới.

Năm seam cần thiết kế ngay: grain hành động SKU so với grain nhãn Order
Line; quy tắc support/abstention; đánh giá trên simulator thay đổi cơ chế;
metric contract chung cho API/UI/agent; và provenance của từng nhận định.
Forecast, anomaly và saved investigation có thể POC nhỏ sau các seam này.
Inventory/traffic/profit vẫn cần nguồn và quyết định scope riêng theo Q31.

## 1. Score hữu ích ở cấp hành động nào?

**Đã chốt:** Q38 score tại thời điểm đặt đơn, có điều kiện sẽ được giao,
train trên đơn đã giao; score xếp hạng, không trình bày xác suất; Seller
ưu tiên kiểm tra SKU/Listing. Baseline là tỷ lệ trả lịch sử, temporal
evaluation có lift/PR-AUC, ít lịch sử thì abstain. Không thay policy này.

**Khoảng trống mới:** nhãn/model ở Order Line nhưng người dùng kiểm tra
SKU/Listing. Model thắng PR-AUC trên line chưa chứng minh giúp chọn đúng
SKU cần xem. Tổng score cũng không phải số return kỳ vọng vì score chưa
được định nghĩa là xác suất. Nếu một SKU có nhiều line, cộng score có thể
chỉ chọn SKU bán nhiều. Đây là suy luận từ Q38 và grain trong
[model.py](../generator/core/model.py), không phải kết quả benchmark.

**Đề xuất:** đặc tả một hàng đợi kiểm tra theo `Storefront + SKU + cutoff`,
khử lặp SKU; so các cách rank bằng tín hiệu lịch sử, phân vị score và mức
độ phơi nhiễm đơn hàng, không gọi tổng điểm là tiền thiệt hại. Từng cách
phải nêu numerator/denominator, lịch sử đủ hạn, volume, độ phủ và lý do.
Chọn K theo số SKU Seller có thể xem trong một phiên. So với rule baseline
trên cùng tập eligible, cùng K, cùng cutoff.

Hai lớp đánh giá độc lập:

- **Line:** cố định định nghĩa PR-AUC, vì average precision và diện tích
  hình thang dưới PR curve không phải cùng một cách tính; báo prevalence,
  lift, từng Channel/category và coverage. API scikit-learn có chỉ số
  `average_precision_score`; chọn rõ hàm và version trong evidence. [S1]
- **SKU/Listing:** precision@K của hồ sơ đáng kiểm tra theo checklist
  độc lập hoặc kịch bản có oracle; tỷ lệ hồ sơ có đủ bằng chứng, lỗi lặp và
  thời gian đọc mỗi hồ sơ. Không đồng nhất "đáng kiểm tra" với "từng có
  ít nhất một return", vì SKU volume lớn dễ đạt điều kiện đó.

Đơn vị nhãn đánh giá action phải được chốt trước benchmark: ví dụ một
biến động return đủ support cần rà soát listing trong simulator. Chưa có
đánh giá can thiệp thì không được gọi precision@K là số return đã tránh.
Đây là định nghĩa sản phẩm đề xuất, chưa phải Q38 mới.

## 2. Exposure, maturity và selection cần được nhìn thấy

Readiness đã có maturity/label availability; có thể khai thác thêm bằng
**bảng độ phủ của tập đánh giá**: tổng line đặt → giao quan sát được → đủ
30 ngày → feed hoàn chỉnh → có feature → được score → vào top K. Hiển thị
theo Storefront/category/tuổi SKU để tránh bảng điểm tốt chỉ nhờ bỏ nhiều
trường hợp khó. Denominator phải gắn đúng mốc quan sát. Không bù missing
label bằng 0.

Theo nghiên cứu gốc, khi outcome chỉ quan sát được ở một phần quần thể,
phân phối labeled có thể khác quần thể áp dụng. [S2] **Áp dụng FSDS là
suy luận:** Q38 chủ động định nghĩa bài toán có điều kiện trên delivered;
không tự đổi thành dự đoán mọi đơn hoặc tuyên bố đã giải selection bias.
Trong simulator hiện tại, cancellation dùng một phép rút với `CANCEL_RATE`
ở [simulate.py](../generator/core/simulate.py); điều này chưa mô phỏng việc
cancellation, fulfillment hoặc missing feed phụ thuộc vào risk/Channel.

Thêm stress fixture có cancellation/missing feed lệch theo nhóm để đo
coverage và performance conditional. Giữ cùng định nghĩa Q38, nhưng đo
độ nhạy khi tập delivered thay đổi. Survival/time-to-return hoặc mô hình
hai bước delivery→return là nhánh nghiên cứu sau: chúng đổi estimand,
assumptions và bằng chứng cần có; không phải cách âm thầm sửa v1.

**Chi tiết kỹ thuật cần POC:** tài liệu Feast mô tả PIT join quay về quá
khứ theo timestamp entity và TTL. [S3] Issue chính chủ #6615 đề xuất lọc
`created_timestamp <= entity_timestamp` cho as-of-known-time. [S4] Issue
là tín hiệu rủi ro, không phải chứng minh mọi adapter/version hiện tại
cùng hành vi hoặc tính năng đã được phát hành. Trên đúng adapter pin,
test ba dòng: event ngày 1 biết ngày 2; correction cùng event biết ngày 5;
entity cutoff ngày 3 phải đọc bản biết ngày 2. Chỉ kiểm event time có thể
chưa đủ. Không đưa correction tương lai vào train bằng cách backdate.

Cập nhật đối chiếu mã nguồn 2026-10-18: `master` của Spark adapter có
`supports_filter_by_created_timestamp=True` và tham số
`filter_by_created_timestamp=False` mặc định. Vì thế issue trên không thể
được dùng để kết luận phiên bản hiện tại không hỗ trợ known-time filter.
Vẫn phải test release pin, mapping timestamp và lời gọi thực tế theo M04;
xem [note nền tảng](research-platform-options-20261017.md) và
[mã adapter](https://raw.githubusercontent.com/feast-dev/feast/master/sdk/python/feast/infra/offline_stores/contrib/spark_offline_store/spark.py).

## 3. Synthetic data: thêm phép thử đổi thế giới, không chỉ tăng volume

Hiện trạng đọc trực tiếp:

- [returns.py](../generator/core/returns.py) sinh return từ logistic với
  category, hidden SKU defect, Storefront, fulfillment, discount, price,
  quantity; các hệ số cố định. Return lag lấy Gamma và nhãn so cửa sổ.
- [simulate.py](../generator/core/simulate.py) chọn SKU theo popularity;
  Sale Event thay volume, SKU mix và discount; buyer lấy ngẫu nhiên từ pool.
- [model.py](../generator/core/model.py) chứa sự thật mô phỏng gồm
  `defect_propensity`, lifecycle tương lai và nhãn. Các giá trị này không
  được đi vào feature chỉ vì có sẵn trong bộ nhớ generator.

**Hệ quả suy luận:** temporal split cùng công thức chủ yếu đo khả năng
tái học quy luật simulator. Đổi seed kiểm tính ổn định nhưng chưa kiểm
được khi quy luật tạo dữ liệu đổi. Giữ riêng bốn loại evidence:

| Bộ thử đề xuất | Câu hỏi | Không chứng minh |
|---|---|---|
| Fixture oracle viết độc lập | Pipeline, time, identity, label đúng không? | Model có ích ngoài fixture |
| Cùng cơ chế, seed chưa dùng | Model/rule có ổn định với mẫu khác không? | Khả năng thích nghi cơ chế mới |
| Cơ chế thay đổi đã giữ kín | Discount effect đổi, SKU defect đổi, feed delay/selection khác thì fail ở đâu? | Hiệu quả thương mại thật |
| Dữ liệu thực độc lập sau này | Khả năng chuyển sang môi trường thực | Causal uplift của hành động nếu chưa có thiết kế can thiệp |

Generator cần interface scenario để cấu hình cơ chế và manifest ghi
scenario/version; không bắt model đọc scenario truth. Chọn một bộ phát
triển và một bộ holdout độc lập trước tuning; oracle không gọi lại chính
hàm transformation/model cần kiểm. Đừng điều chỉnh simulator sau khi xem
test chỉ để model đạt target. Thêm negative controls: feature nhiễu,
shuffle nhãn trên bộ kiểm riêng và kịch bản signal bị tắt. Đây là đề xuất
thiết kế kiểm thử dựa trên code, không phải kết quả khoa học đã xác nhận.

Basket và discount vẫn chỉ cho association theo Q29. Cơ chế simulator
gán discount làm tăng return không chứng minh giảm discount thực sẽ giảm
return; thay đổi policy có thể đổi quần thể khách/đơn và các yếu tố khác.
Không dùng hidden cost/defect làm lời giải thích Seller nếu nguồn không lộ.

## 4. Mở rộng insight trước khi thêm model phức tạp

### 4.1 Giải thích biến động bằng phân rã số học

Với một Storefront/currency và cùng metric contract, có thể phân rã tổng
sales thành đóng góp của SKU/category/fulfillment; kiểm tổng contribution
bằng chênh lệch tổng. Có thể xem volume, giá bán bình quân và product mix,
nhưng chọn công thức phân rã, trọng số/reference period rõ để không gán
phần tương tác hai lần. Nói "nhóm A đóng góp X vào chênh lệch quan sát",
không nói "A gây ra X". Đây là cơ hội tương đối ít công, dùng nguồn hiện
có; test bằng fixture đổi mix mà giá từng SKU giữ nguyên.

### 4.2 Sparse support và sự chắc chắn của xếp hạng

`1/2 = 50%` và `100/200 = 50%` không mang cùng lượng bằng chứng. Tỷ lệ
phải kèm tử/mẫu, maturity, coverage, và trạng thái không đủ support.
statsmodels hỗ trợ khoảng cho tỷ lệ nhị thức như Wilson/Jeffreys. [S5]
Đề xuất thử interval thay vì chỉ phần trăm, nhưng Order Line trong cùng
Order/SKU có thể phụ thuộc: interval nhị thức không tự giải quyết điều
này. Khi benchmark model cần xem bootstrap theo Order hoặc block thời
gian phù hợp, không mặc định mọi line độc lập.

Hierarchical shrinkage/prior category là tùy chọn POC cho bảng mô tả;
phải báo dữ liệu SKU và phần borrow từ nhóm, fit prior trên lịch sử training,
thử độ nhạy. Không dùng prior để tự động phá policy abstain ít lịch sử
của Q38 hoặc làm SKU mới trông chắc chắn. Khi quét hàng nghìn SKU, kiểm
false discoveries hoặc giới hạn candidate/hypothesis đã chọn; ngưỡng
mỗi SKU riêng lẻ chưa bảo đảm ít cảnh báo sai toàn hệ thống.

### 4.3 Anomaly và forecast là hai sản phẩm khác nhau

**Anomaly:** báo sai khác đáng xem sau khi loại ngày thiếu feed, trễ và
Sale Event đã biết. Đầu ra là cờ điều tra trong UI, có expected range,
observed value, coverage và drilldown; không gửi thông báo. Đo false
positives trên ngày bình thường, detection delay và kết quả khi mất feed.
Không dùng sự kiện biết sau cutoff để giải thích detector đã biết trước.

**Forecast:** nhánh tùy chọn dự báo units/order volume 7–14 ngày, chỉ trên
chuỗi đủ coverage. Thử baseline naive/seasonal naive, rolling-origin
evaluation và khoảng dự báo. FPP3 nêu baseline, temporal evaluation và
vấn đề MAPE khi actual bằng 0; đề xuất MAE/MASE khi mẫu số MASE hợp lệ,
pinball loss/coverage nếu xuất quantile. [S6] Chuỗi toàn 0/hằng có thể làm
MASE không xác định; metric fallback phải khai báo.

365 ngày chỉ có khoảng một chu kỳ năm, không đủ evidence chắc chắn về
annual seasonality; simulator hiện tại cũng không tự sinh weekly/annual
pattern chỉ vì đã tăng số ngày. Tách trend/seasonal/remainder là công cụ
mô tả chuỗi, không biến thành bằng chứng causal. [S7] Bắt đầu từ horizon
ngắn, weekly pattern chỉ nếu có cơ chế/quan sát phù hợp. Khi chưa có
availability/inventory, đầu ra gọi là **forecast sales quan sát được**;
không gọi latent demand, lượng nhập tối ưu hoặc lợi nhuận kỳ vọng.

## 5. Agent nên chia sẻ ngữ nghĩa, bằng chứng và giới hạn thực thi

Q39/Q46 đã yêu cầu API/citation/version. Cơ hội nền là một **metric contract
chạy được** chung: metric ID/version, grain, allowed dimensions, additive
rules, unit/currency, cửa sổ/cohort, null/coverage và giới hạn query. UI,
agent, model feature và evidence cùng tham chiếu định nghĩa; không để
LLM tự quyết cách cộng distinct order hoặc tính trung bình tỷ lệ.

Đề xuất tool read-only hẹp theo nghiệp vụ: lấy metric, so cửa sổ, phân rã
nhóm, drilldown có giới hạn, lấy risk evidence, retrieve playbook. API
trả structured result với evidence IDs; coordinator kiểm chứng evidence
trước khi dựng câu trả lời. Không cần thêm unrestricted text-to-SQL để
chứng minh agent biết đọc dữ liệu. Tool schema validation, query limits và
DB permission thực thi read-only; MCP `readOnlyHint` chỉ là hint, không
thay quyền hệ thống. Tài liệu MCP chính chủ nói rõ annotations không phải
enforcement. [S8]

**Một nhận định có hai loại nguồn:** "return rate tăng" phải trỏ metric
result/generation; "nên kiểm tra mô tả size" phải trỏ playbook version/chunk
và rule điều kiện. Chunk RAG có liên quan chưa chứng minh con số hiện tại.
Đề xuất mỗi recommendation mang claim, numeric evidence, rule/playbook,
missing prerequisites, suggested next check, scope/time và abstention.
Nếu nguồn mâu thuẫn hoặc thiếu điều kiện, trình bày mâu thuẫn/thiếu thay vì
chọn câu nghe hợp lý. Chất lượng recommendation tách khỏi chất lượng retrieval.

**Evaluation:** RAGAs phân biệt retrieval, faithfulness và generation. [S9]
FSDS nên có bộ câu hỏi tiếng Việt được người viết duyệt với oracle số học,
expected tool/currency/cutoff, source chunks và expected abstention. Có
câu nhiều bước, không dấu, SKU gần tên, nhầm kỳ so sánh, feed thiếu, corpus
cũ/mới mâu thuẫn, injection trong tool result/chunk và tool timeout. Chấm
numeric exactness trước prose; kiểm citation có hỗ trợ từng claim, không
chỉ kiểm tồn tại URL. Judge LLM có thể bổ trợ, nhưng CPU model dùng để
trả lời tự chấm chính nó không phải oracle độc lập. Không mặc định dùng
paid external judge, giữ Q42.

**Budget/trace:** OpenTelemetry có vocabulary cho model/tool usage, song
GenAI conventions đang phát triển và có chuyển repo; cần pin schema. [S10]
Đề xuất trace gồm request/generation/dataset, model/prompt/corpus/embedding/
tool versions, tool args đã lọc nhạy cảm, evidence IDs, retry, latency,
input/output token, trạng thái abstain/error. Không cần lưu chain-of-thought
để chứng minh câu trả lời. Một request có max tool calls/hops/tokens/time,
retry budget tổng, cancellation và response hữu ích khi hết budget;
không để hai agent gọi vòng nhau. Cache key phải gồm scope, generation,
metric/tool/corpus version; cache TTL không thay version pin.

## 6. Hồ sơ điều tra và feedback: mở rộng UI có ranh giới

Có thể tạo view "vì sao SKU này cần xem" gồm snapshot evidence, rule,
recommendation và link drilldown trong phiên read-only. Nếu muốn **lưu**
investigation, ghi chú hoặc đánh dấu đã xem thì đó là ghi trạng thái ứng
dụng mới, cần quyết định scope riêng dù không ghi hệ thống bán hàng.
Thiết kế đề xuất: owner bấm lưu qua API riêng, agent tiếp tục chỉ đọc;
saved query trỏ generation/version để mở lại và so với số hiện hành.
Không tự tạo hành động gửi thông báo, đổi giá/hủy đơn/hoàn tiền.

Feedback "hữu ích/không hữu ích" đánh giá recommendation; nó không phải
Return Label và không tự train model. Khi người dùng bắt đầu thực hiện
gợi ý ngoài ứng dụng, cần action/exposure log và evaluation phù hợp nếu
sau này muốn đo tác động; chưa có thì chỉ kết luận tính đúng và hữu ích
của hồ sơ điều tra. Export chưa thuộc Q43 v1.

## 7. Ma trận thí nghiệm giới hạn trước đầu tư lớn

Các mức nhỏ/vừa/lớn là **công tương đối**, không phải ước lượng ngày hay
chi phí. Chọn ngưỡng trên bộ phát triển rồi khóa trước holdout. Không
đóng gate bằng cách đổi ngưỡng sau xem kết quả; metric undefined/thiếu
support nghĩa chưa đủ evidence. Tất cả thí nghiệm có thể chuẩn bị local.

| ID / cơ hội | Bộ tối thiểu để POC | Tiêu chí quyết định đề xuất | Failure mode chính | Công / ảnh hưởng |
|---|---|---|---|---|
| M01 — action grain | Ít nhất 2 Storefront, nhiều SKU có volume cao/thấp, >K candidate; hai thời kỳ đủ maturity | Rule/model cùng eligible set và K; không SKU lặp; precision@K theo oracle hữu ích và coverage báo riêng PR-AUC line | Chỉ rank volume; cộng score như expected count | Vừa; cần trước model/UI |
| M02 — support | Fixture 0/0, 0/n, 1/2, 100/200, SKU mới, feed thiếu và line cùng Order | Không chia 0/false certainty; threshold/interval và abstention nhất quán; optional prior không phá Q38 | Category prior che thiếu dữ liệu | Nhỏ–vừa; contract nền |
| M03 — đổi cơ chế simulator | Bộ phát triển + seed holdout + ít nhất một regime holdout; event/observation time độc lập | Score/rule so theo cùng definition; báo worst-group/coverage; negative control không tạo lift có ý nghĩa lặp lại; fail thì giới hạn claim | Leakage hidden truth; tune generator theo test | Vừa; cần trước train chính thức |
| M04 — as-of-known-time | Hai version cùng event time, observed trước/sau cutoff; positive label đến muộn | Retrieval đúng oracle tất cả cutoff trên adapter pin; train chỉ dùng label biết ở training cutoff | Created timestamp chỉ dùng dedup | Vừa; mở rộng S09/S14 |
| M05 — contribution | Hai kỳ, 3 SKU, đổi mix/giá/units, missing/currency khác | Tổng contribution khớp delta theo precision tiền; không câu causal; invalid scope bị chặn | Simpson/mix effect bị gọi nguyên nhân | Nhỏ; giá trị sớm |
| M06 — anomaly | Chuỗi complete/missing, Sale Event, spike thật, ngày bình thường, sparse SKU | Khóa false-positive budget, đo detection delay; missing feed có trạng thái riêng; vượt budget thì giữ diagnostic | Cảnh báo outage thành sụt cầu | Vừa; tùy chọn |
| M07 — forecast | Ví dụ thử nhỏ 12 tuần daily complete, nhiều origin và horizon 7/14; sparse/all-zero cases riêng | So baseline trên rolling holdout; interval coverage/width; không cải thiện thì giữ baseline hoặc không xuất forecast | Backtest future info; MAPE 0; annual claim | Vừa–lớn; scope tùy chọn |
| M08 — semantic tools + RAG | Ví dụ 30–50 câu curated phủ toàn intent + negative/multi-step; corpus 2 version | Numeric invariants/currency/version đúng toàn bộ fixture; retrieval/citation/abstention tách; semantic thresholds chốt trước holdout | Citation đúng trang nhưng không hỗ trợ claim | Vừa; nền P3 |
| M09 — agent budget | Câu đơn giản/nhiều bước, timeout, loop/retry, cancel, concurrency nhỏ | Mọi request chấm dứt trong cap đã cấu hình; không gọi tiếp sau cancel; trace đủ evidence; đo p50/p95/RAM | Hết CPU do gọi vòng; cache lẫn version | Vừa; POC CPU phải bao gồm |
| M10 — investigation | Mock 3 hồ sơ cùng query nhưng generation khác, evidence hết retention | Owner mở lại đúng bản hoặc báo hết hạn; feedback tách label; agent không được tool ghi | Saved view âm thầm đọc latest | Nhỏ–vừa; cần quyết định mới nếu lưu |

M07 12 tuần và M08 30–50 câu chỉ là cỡ thử tính khả thi, không phải mức
đủ chứng minh chất lượng sản phẩm. Giữ giới hạn thời gian khảo sát: sau
mỗi POC chỉ quyết định tiếp/giữ baseline/hoãn và ghi lý do, không mở vô
hạn tổ hợp thuật toán.

## 8. Quyết định nên chốt trước khi lập ticket triển khai

1. Định nghĩa một hồ sơ SKU "đáng kiểm tra", K/capacity, baseline action
   và cách tổng hợp từ line; không sửa nhãn Q38 khi chưa quyết định riêng.
2. Rule support/abstain và bảng population funnel; category prior có được
   thử ở mô tả hay không, không mặc nhiên dùng cho cold-start scoring.
3. Giữ holdout simulator đổi cơ chế nào; tách scenario oracle khỏi nguồn
   feature; bằng chứng synthetic chỉ cho phép kết luận đến mức nào.
4. Khóa metric/evidence contract chung, rồi mới quyết định giao diện tool,
   citation và benchmark CPU hai agent.
5. Chọn contribution/sparse-support làm bổ sung ưu tiên; anomaly/forecast
   là POC tùy chọn; saved investigation cần quyết định trạng thái ghi mới.

## Nguồn sơ cấp đã kiểm tra

Tất cả URL truy cập **2026-10-17**. Nguồn mô tả phương pháp/capability;
việc chọn dùng trong FSDS là đề xuất ở trên. Không đọc issue/docs như bằng
chứng adapter local hoặc runtime FSDS đã pass.

| ID | Nguồn | Điểm dùng |
|---|---|---|
| S1 | [scikit-learn — model evaluation](https://scikit-learn.org/stable/modules/model_evaluation.html) | Phân biệt metric/scorer, AP, precision/recall |
| S2 | [Wei, ICML 2021 — Decision-Making Under Selective Labels](https://proceedings.mlr.press/v139/wei21a.html) | Outcome selection và giới hạn suy rộng |
| S3 | [Feast — Point-in-time joins](https://docs.feast.dev/getting-started/concepts/point-in-time-joins) | Entity timestamp, historical TTL |
| S4 | [Feast issue #6615](https://github.com/feast-dev/feast/issues/6615) | Rủi ro cần kiểm về known-time cutoff; không phải xác nhận implementation |
| S5 | [statsmodels — proportion_confint](https://www.statsmodels.org/stable/generated/statsmodels.stats.proportion.proportion_confint.html) | Tùy chọn interval cho tỷ lệ nhị thức và coverage caveat |
| S6 | [Hyndman & Athanasopoulos — FPP3, toolbox](https://otexts.com/fpp3/toolbox.html) | Baseline, rolling evaluation, errors, prediction intervals |
| S7 | [Hyndman & Athanasopoulos — decomposition](https://otexts.com/fpp3/decomposition.html) | Trend/seasonality/remainder |
| S8 | [MCP maintainers — Tool Annotations as Risk Vocabulary](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/) | Read-only hints không phải enforcement |
| S9 | [Es et al., EACL 2024 — RAGAs](https://aclanthology.org/2024.eacl-demo.16/) | Retrieval/faithfulness/generation evaluation |
| S10 | [OpenTelemetry — GenAI attributes](https://opentelemetry.io/docs/specs/semconv/registry/attributes/gen-ai/) và [thông báo chuyển conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-agent-spans/) | Usage/tool telemetry và yêu cầu pin schema |
