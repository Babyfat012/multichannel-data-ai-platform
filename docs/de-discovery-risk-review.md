# Rà phụ thuộc toàn hệ thống trước triển khai

Ngày 2026-10-17. Người dùng yêu cầu tiếp tục discovery sâu để giảm sửa lại
nền tảng; chấp nhận dành thời gian xác định. Q32–Q51 đã được người dùng
xác nhận ngày 2026-10-17. Các phát hiện kỹ thuật vẫn cần POC khi nêu rõ;
Q1–Q31 giữ hiệu lực trừ phần được người dùng chủ động mở lại. Cloud hoàn
toàn do người dùng thao tác, bao gồm tự chạy IaC và kích hoạt deploy.

Cập nhật 2026-10-18: Q52–Q55 đã xác nhận tại
[ADR-0009](adr/0009-partial-lifecycle-inventory-seller-triage.md). Scope bổ
sung partial lifecycle, Listing history, inventory nhỏ và hàng đợi kiểm tra;
impact và S19–S30 tại [expanded-scope-design.md](expanded-scope-design.md).
Q56–Q59 cũng đã được xác nhận; Q56/Q57 làm rõ/thay phần ngữ nghĩa Q41 bằng
[label v2](return-label-contract.md), Q58/Q59 chốt inventory/queue.

Rà bổ sung 2026-10-17: policy đã chốt không đồng nghĩa contract/POC hoàn tất.
[Điều kiện triển khai](implementation-readiness.md) ghi gate G0–G4,
scenario S01–S18, evidence và phụ thuộc trước full-scale. Các ghi chú bàn
giao bên dưới đã được đồng bộ với Q38–Q51; phát hiện code là hiện trạng,
không phải tuyên bố các thiếu sót đã được sửa bằng implementation.

Vòng 2026-10-18 mở rộng từ audit sang khai thác cơ hội, xem
[discovery mở rộng](plan-expansion-discovery.md). Lưu ý cập nhật nghiên cứu
Feast: tài liệu contrib cũ và mã `master` mới có khác biệt về offline write
và created-time filtering. Các hạn chế adapter được ghi trong đợt audit
dưới đây là giả định theo tài liệu/phiên bản lúc khảo sát; phải đối chiếu
release thực tế, không coi là bất biến. [Nguồn và phép thử PF01–PF04](research-platform-options-20261017.md).

## Các phát hiện có bằng chứng

1. **Nguồn 180 ngày không đủ lịch sử quan sát mỗi ngày.**
   `generator/delivery/batch_exports.py:3` và `:75` xuất bản chính tại ngày
   cập nhật cuối cùng trước cutoff, chỉ một phần nhỏ thêm bản duplicate/cũ.
   Không thể khôi phục mọi trạng thái từ các file này. `created_at` không
   chứng minh Seller đã quan sát đơn vào ngày tạo. Cần chốt nguồn trước
   khi tái sinh 100 GB, không chỉ thêm cột timestamp trong feature.
2. **Latest snapshot chưa xử lý đầy đủ sửa/xóa nghiệp vụ.** Silver dedup
   theo line có thể giữ dòng đã biến mất khỏi full-order snapshot; CDC
   (`spark_jobs/cdc/ingest_walmart_order_changes.py`) chưa chứng minh delete được áp dụng tới Gold.
   Cần chính sách restatement, history và snapshot/delta/tombstone.
3. **Single Seller và SKU chung là giả định nền.** `docs/terminology.md`
   định nghĩa một Seller, SKU chuẩn xuyên Channel. Gold key hiện không chứa
   seller/account/storefront (`spark_jobs/gold/build_order_snapshot_lines.py:138`).
   Đừng hứa nhiều shop/Seller nếu chưa chốt namespace và mapping Listing.
4. **Return Label không phải số lượng/tiền trả hàng.** Schema tại
   `generator/generate_order_snapshots.py:102` là binary label, delivered_at, available_at.
   Return và refund khác nhau; cần gọi chính xác positive outcome gồm
   Return hoặc Refund. Chưa có return quantity/refund economics để tính
   tỷ lệ theo units hay lợi nhuận. Maturity 30 ngày đã chốt, không hỏi lại.
5. **Streaming chưa tương đương ý tưởng trong tài liệu.**
   `generator/delivery/stream_events.py:45,84` chỉ render Amazon/TikTok
   order-status events, không đủ năm Channel và không có return-event feed.
   Giữ batch/stream cùng thế giới không tự chứng minh đủ lifecycle.
6. **Basket/customer simulation có giới hạn nghiệp vụ.**
   `generator/core/simulate.py:177` lấy SKU theo popularity độc lập;
   `:243` chọn buyer từ pool. Phân tích được nhưng chưa chứng minh bundle
   complementarity, retention thực hoặc causal effects. Đổi scale/history
   có thể đổi buyer pool và diễn giải ID; dataset version cần rõ.
7. **Rubric MLE rộng hơn một chatbot.** Sheet `mle` trong
   `edai-k11-cw/rubic/EDAI K11 - MLE.xlsx`: self-host LLM/custom model/
   benchmark (3–6), registry (8/21), RAG qua Feast (9–10), stream features
   offline+online (12), hai agent/coordinator (19/22/25–26), sandbox (20),
   MCP/A2A gateways (23–24), warm-up (27), CI/CD (33–36), IaC (44–45).
   Cần map rubric thành capability/acceptance cụ thể; sample buyer-support
   không bắt buộc áp nguyên use case. Proposal yêu cầu một hệ thống chung
   qua cả hai module (`edai-k11-cw/coursework_proposal.md`).
8. **Manual Cloud cần phân biệt người thực hiện và cơ chế triển khai.**
   Người dùng tự chạy Terraform/Ansible/kích hoạt CI khác với mọi resource
   đều tạo tay bằng Console. Chưa được tự suy ra cách nào. Cần làm rõ trước
   thiết kế IaC/CI; trợ lý không thao tác Cloud ở cả hai cách.
9. **Redis feature lookup không tự giải quyết RAG.** Feast Redis adapter
   lưu feature theo key; không đồng nhất khả năng Redis product vector
   search với adapter Feast. Cần thiết kế project/backend RAG riêng hoặc
   adapter được POC, không lặng lẽ thay Redis numeric feature store.
10. **Stream dual sink và publication chưa được giải quyết.** Spark offline
    adapter không hỗ trợ offline_write_batch; không mặc định Feast push
    ONLINE_AND_OFFLINE hoạt động. Cần offline history sink + online update
    có replay/parity. Materialize Redis dở dang không tự rollback cùng
    warehouse; D21 đòi hỏi version/gating hoặc cơ chế publish có kiểm chứng.
11. **Budget chưa chứng minh đủ toàn MLE.** GCP credit không tự chi trả
    provider API ngoài GCP; trial GPU/quota hạn chế. Rubric LLM platform có
    hướng CPU cần POC chất lượng tiếng Việt/tool-calling/latency. Không
    khẳng định bất khả thi hoặc chắc chắn đủ 300 USD trước đo.
12. **Tài liệu cũ có lời hứa khác code và quyết định mới.**
    `docs/domain-overview.md`/`docs/Domain.md` có USD FX, đa currency Shopify,
    all-channel return stream, Terraform destroy; một số chưa có code hoặc
    đã bị quyết định mới thay thế. Không dùng làm RAG truth chưa thẩm định.
    Tài liệu hiện trạng giữ vai trò mô tả proof cũ; policy mới theo Q/ADR.
    Corpus RAG phải có danh mục phiên bản đã duyệt, không ingest cả repo.

## Ma trận quyết định và phụ thuộc

| Miền | Cần chốt/kiểm chứng | Khi nào |
|---|---|---|
| Mục tiêu nghiệm thu | Toàn rubric DE+MLE+agent, chia phase và proof | Đã chốt Q32 |
| Manual Cloud | User tự chạy IaC/chủ động kích hoạt deploy; không auto vì commit | Đã chốt Q33 |
| Identity | Một Seller; namespace shop/account; Listing→SKU ổn định | Đã chốt Q34 |
| Lịch sử nguồn | Bổ sung creation/lifecycle/fulfillment/return feeds quan sát được | Đã chốt Q35 |
| Correction | Restate số liệu mới + bảo toàn phiên bản cũ | Đã chốt Q36 |
| Synthetic validity | Kịch bản có chủ đích, giới hạn kinh doanh, test độc lập | Đã chốt Q37 |
| Label semantics | Binary Return-or-Refund; quantity/reasons/amount riêng | Đã chốt Q41 |
| Metric semantics | Provisional vs complete, late correction, cohort boundaries | Đã chốt Q41 |
| Stream product | Kênh/event, live operational view vs daily official metrics | Đã chốt Q40 |
| ML action | Ai dùng score, quyết định gì, baseline/utility/cold start | Đã chốt Q38 |
| Agent | Vai trò hai agent/coordinator, tool read-only, abstention/citation | Đã chốt Q39 |
| RAG | Corpus có bản quyền/nguồn/version, embeddings/vector backend | Đã chốt Q44 (POC Feast vector còn mở) |
| LLM | CPU feasibility, provider/cost, quality/latency budget | Đã chốt Q42 (POC chất lượng còn mở) |
| Serving | API request identity, data version, stale/missing behavior | Đã chốt Q46 |
| UI | Shop overview/SKU/order drilldown, filter/export, access roles | Đã chốt Q43 |
| Security | Login, API scope, secrets, network exposure, prompt injection | Đã chốt Q47 (chi tiết triển khai còn mở) |
| Publication | Snapshot consistency across PG/Redis, failure injection | Đã chốt Q45 (ADR-0007) |
| Operations | Start/stop, restore/replay, retention, logging, run budgets | Đã chốt Q48 |
| Validation | Rubric matrix + source/feature/model/agent acceptance | Đã chốt Q50 (ledger khóa) |
| Learning | Concept→code→exercise→manual Cloud; tiến độ người học | Giữ mục tiêu người dùng |

## Quyết định Q32–Q37 (đã xác nhận)

- Q32: thiết kế tổng thể bao phủ toàn rubric DE/MLE/agent, chia phase triển
  khai và lập ma trận acceptance; chưa hứa đủ ngân sách trước POC.
- Q33: manual nghĩa người dùng tự thực thi cả lệnh IaC và chủ động kích hoạt
  deploy pipeline; không tự deploy vì commit hoặc do trợ lý thao tác.
- Q34: v1 vẫn một Seller, nhưng identity nguồn có shop/account để không
  khóa một Channel chỉ có một shop; SKU nội bộ ổn định và mapping Listing.
  Chưa triển khai SaaS đa Seller/RBAC tenant.
- Q35: bổ sung nguồn mô phỏng quan sát được cho order creation/lifecycle,
  fulfillment và returns khi cần; timestamp/provenance/delay rõ, không lấy
  oracle nội bộ làm feature; chốt trước khi tái sinh nguồn lớn.
- Q36: báo cáo hiện tại được cập nhật theo correction hợp lệ, giữ published
  versions để truy lại số liệu/model/recommendation trước; có case xóa line.
- Q37: thêm kịch bản synthetic có chủ đích để nghiệm thu (late/correction,
  partial/missing, basket/customer, drift), tái lập và có dữ liệu kiểm tra
  độc lập; không coi kết quả là bằng chứng hiệu quả kinh doanh thực tế.

Không hỏi người dùng chọn hashing, SQL/MERGE, schema registry library hoặc
thuật toán retry; đó là việc triển khai sau khi policy nghiệp vụ được chốt.
Không cam kết loại bỏ mọi thay đổi: discovery khóa quyết định nền, POC giải
quyết bất định kỹ thuật trước full-scale, thay đổi sau có impact assessment.

## Quyết định Q38–Q43 (đã xác nhận)

- Q38: score Return-or-Refund là **tín hiệu xếp hạng/triage** để Seller ưu
  tiên kiểm tra SKU/Listing, không trình bày là xác suất vô điều kiện; suy ra
  tại thời điểm đặt đơn với điều kiện "sẽ được giao"; train trên nhóm đã giao;
  baseline là rule tỷ lệ trả hàng lịch sử (D15); dưới ngưỡng lịch sử thì
  abstain; đánh giá temporal split + lift/PR-AUC so baseline, không đặt
  accuracy target tùy ý.
- Q39: một UI chat + 1 coordinator điều phối 2 agent chuyên trách (data/metrics
  đọc feature store/Web API; model+knowledge gọi KServe + RAG); mọi tool
  read-only; bắt buộc citation và abstain khi thiếu dữ liệu/ngoài phạm vi/khác
  currency; trace request liên kết data/model/prompt/tool version. Gateway,
  sandbox và registry là chi tiết triển khai.
- Q40: tách **live operational view** khỏi **daily official metrics**; stream
  phát order creation + status change + late return trên đủ 5 Channel (mở rộng
  Q35) cùng identity/time/version với batch; metric chính thức vẫn snapshot
  ngày kèm data_as_of. Cơ chế dual-sink/parity để POC.
- Q41: giữ nhãn nhị phân mỗi Order Line làm chuẩn; quantity/amount/reason chỉ
  thêm khi nguồn Q35 thực sự có, dùng để giải thích/analytics, không bắt buộc
  cho label; absent ≠ 0; loại nhãn chưa đủ 30 ngày khỏi tử/mẫu; correction cập
  nhật số hiện hành + giữ published version; giữ cohort theo ngày giao, ngày
  UTC hoàn chỉnh.
- Q42: self-host model nhỏ trên CPU (hướng llm-d/KServe), không dùng paid
  external API; POC tiếng Việt/tool-calling/latency/memory/concurrency trước
  khi chốt; không hứa chất lượng ngang provider lớn; không đặt ngưỡng chất
  lượng tùy ý.
- Q43: v1 owner-only private, tiếng Việt, không multi-role RBAC; UI shop
  overview → SKU → order-line drilldown, lọc theo Storefront/khoảng ngày;
  chưa export; hiển thị data_as_of/stale/offline (D21). Auth/HTTPS/rate-limit
  là chi tiết triển khai vòng sau.

## Quyết định Q44–Q47 (đã xác nhận)

- Q44: corpus RAG chỉ gồm tài liệu tự soạn/đã duyệt/versioned (playbook, định
  nghĩa metric, ghi chú giới hạn), không dùng tài liệu cũ chưa thẩm định làm
  sự thật; câu trả lời trích dẫn chunk nguồn; ưu tiên đúng hướng **Feast
  vector** cho rubric nhưng coi là **POC-gated**, có phương án vector store dự
  phòng, không âm thầm thay Redis numeric; chunk có cập nhật/xóa theo phiên
  bản; tiếng Việt là chính.
- Q45: công bố bằng **versioned generation + manifest + request pin**; chỉ mở
  manifest sau khi mọi đầu ra bắt buộc validate xong; lỗi giữ bản thành công
  gần nhất và hiển thị độ cũ (D21); materialize tăng dần phải bắt correction
  cũ (Q36). Ghi tại [ADR-0007](adr/0007-versioned-publication.md).
- Q46: API serving ghim version tường minh (từ manifest Q45); request gồm
  Storefront + SKU + khoảng ngày + as-of/version; response kèm data_as_of, độ
  tươi, độ phủ (tử/mẫu); thiếu dữ liệu trả "không có" chứ không phải 0; khác
  currency/ngoài phạm vi thì từ chối để agent abstain; ngày UTC.
- Q47: v1 một tài khoản owner qua gateway; API/UI sau gateway + rate limit;
  secret không nằm trong code, để trong secret manager (Vault theo rubric);
  tool agent read-only + sandbox; coi tài liệu RAG và câu hỏi người dùng là
  không đáng tin, test prompt injection và ngoài phạm vi; không mở service ra
  internet ngoài thứ bắt buộc.

## Quyết định Q48–Q49 (đã xác nhận)

- Q48: vận hành on-demand start/stop; giữ GCS + PostgreSQL, **coi Redis là
  rebuild được → rematerialize từ offline khi bật lại** (không tin mỗi cờ
  materialized); có runbook kèm **run budget tối đa mỗi phiên**; observability
  hiển thị freshness/version; retention định nghĩa riêng từng store. Thao tác
  Cloud do người dùng tự làm.
- Q49: **mở lại D11 có chủ đích** — sinh lịch sử dài hơn cho training (mục
  tiêu ~365 ngày) trong khi cửa sổ phục vụ/insight giữ 180 ngày; gắn
  **`dataset_version`** cho mỗi lần sinh để proof/model/version trỏ đúng một
  run; nếu ngân sách không cho thì chấp nhận tập train nhỏ + temporal split và
  nêu giới hạn tường minh (D25).

## Quyết định Q50–Q51 (đã xác nhận)

- Q50: khóa [acceptance ledger](de-acceptance-ledger.md) làm nguồn nghiệm thu
  chính thức; thực hiện theo phase P1 → P2 → P3 → P4, làm nhỏ/POC trước,
  full-scale sau (D9/D24); thay đổi sau cần impact assessment. Scenario
  catalog + hợp đồng source/metric/feature/API sẽ bổ sung khi implement.
- Q51: novel ideas đã chọn — DE-N1 data contract + schema registry, DE-N2
  Iceberg maintenance tự động (10đ mỗi cái); ML-N1 evaluation harness RAG/agent,
  ML-N2 drift monitoring (2đ mỗi cái). Dự phòng: Airbyte, Debezium, guardrails
  prompt-injection, A/B có kiểm định.

## Chi tiết bàn giao từ các đợt audit

- Rubric phải đọc sheet hiện hành `de` và `mle`, không nhập yêu cầu từ sheet
  hidden cũ `rubic final-coursework (final -`. Sample ecommerce chỉ là ví dụ;
  customer-purchase/voucher/RAG-every-turn không bắt buộc thay Seller domain.
- Ngoài các dòng đã liệt kê: current mle 13–15 yêu cầu Kubeflow >=4 stages,
  MLflow versioning và KServe model-inside-image; 16–21 API async/MCP, Helm
  rolling/fallback, agent scaling/sandbox/registry; 28–32 API coverage >90%,
  changed-code mutation >80%, property/idempotency và load report; 37–45
  gateway, auth/rate limit, domain/HTTPS, IaC; 46–54 telemetry, A/B, secrets;
  55–57 docs và hai novel ideas. Ledger từng dòng nay đã lập tại
  [de-acceptance-ledger.md](de-acceptance-ledger.md) (Q50).
- Q39 đã chốt một UI chat, hai agent chuyên trách và một coordinator;
  vai trò và quyền read-only theo quyết định phía trên, implementation còn mở.
- Q44 đã chốt corpus playbook Seller/metric definitions/limitations được
  duyệt và version hóa, tiếng Việt và citation; số liệu thay đổi lấy bằng
  tool. Còn POC retriever/backend và cơ chế chunk lineage/update/delete.
  Redis numeric feature store đã chốt, không tự thay nó vì RAG.
- Model nhãn hiện là Return OR Refund trong 30 ngày, không chỉ physical
  Return. Nếu train loại canceled/undelivered, score mang điều kiện về nhóm
  được giao; không tự trình bày là xác suất vô điều kiện cho mọi đơn mới.
  Q38 đã chốt hành động triage, baseline và abstention; còn đặc tả feature,
  split/cutoff và ngưỡng kiểm chứng. Q49 đã mở lịch sử nguồn ~365 ngày;
  vẫn phải trừ warmup, giao, maturity và kiểm label availability khi train.
- Phân biệt source_observed_at, feature_as_of, computed_at, available_at;
  replay mô phỏng và serving wall-clock cần contract riêng. Historical TTL
  của Feast khác Redis expiry và khác API freshness. Redis mất dữ liệu có
  thể cần full rematerialize dù registry đã đánh dấu materialize xong.
- Publication Q45/ADR-0007 đã chọn generation + manifest + request pin.
  Còn POC physical namespaces, manifest store, concurrency, retention và
  failure injection; Iceberg/PG/Redis không có transaction chung.
  Incremental materialization cần bắt cả correction cũ, không chỉ mốc mới.
- Stream cần lịch sử offline và Redis online có cùng identity/time/version,
  replay/parity; daily feature và live feature nên có semantic boundary rõ.
  Spark offline adapter không có offline_write_batch; không mặc định push
  ONLINE_AND_OFFLINE giải quyết dual sink. Nguồn:
  https://docs.feast.dev/reference/offline-stores/spark
- Feast vector support cần kiểm chứng riêng:
  https://docs.feast.dev/reference/alpha-vector-database
  và https://docs.feast.dev/tutorials/rag-with-docling .
- Rubric LLM platform link https://llm-d.ai/docs/getting-started/quickstart
  có hướng CPU; không kết luận trial không GPU đồng nghĩa bất khả thi.
  Cần đo tiếng Việt/tool-calling/latency/memory/concurrency trên model nhỏ.
  Credit GCP không tự trả external API hoặc tên miền; không hứa dùng paid API.
- Q43/Q47 đã chốt owner-only, tiếng Việt, overview→SKU→Order Line, chưa
  export, auth/rate limit và stale/offline states. Còn sizing/concurrency,
  SLA và cấu hình bảo mật cần kiểm chứng; advisory vẫn giữ hiệu lực.
- Acceptance agent cần tập câu hỏi đúng/sai/thiếu dữ liệu/khác currency,
  citation, tool failure, prompt injection và ngoài phạm vi; request traces
  liên kết data/model/prompt/tool versions. Phần test/telemetry là yêu cầu
  thiết kế, không phải bổ sung cuối khi demo đã xong.
