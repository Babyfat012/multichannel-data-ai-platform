# FSDS — Kế hoạch triển khai (high-level)

Trạng thái: policy đã chốt tới Q61 (2026-10-18). Tài liệu này là bản tổng
hợp để bám theo khi implement; chi tiết từng quyết định xem các file tham
chiếu. Đây chưa phải bằng chứng đã chạy. Rà bổ sung 2026-10-17: policy đã
chốt; contract thực thi và các gate POC còn mở được theo dõi tại
[điều kiện triển khai](implementation-readiness.md).

Vòng discovery mở rộng tiếp 2026-10-18 theo yêu cầu người dùng: đã quét cơ
hội nghiệp vụ/ML/agent/nền tảng tại
[bản đồ mở rộng](plan-expansion-discovery.md). Người dùng đã xác nhận
**Q52–Q55**: partial fulfillment/Return/Refund theo line/số lượng, Listing
lifecycle, inventory nhỏ và hàng đợi kiểm tra read-only. Q54 nâng lát
inventory từ roadmap Q31 vào phạm vi kế tiếp. Q56–Q59 cũng đã được xác nhận:
label v2 theo phần giao/approved Return/product Refund success, pool Seller
chung + pool Channel riêng, và ba nhóm queue. Impact assessment ở
[expanded-scope-design.md](expanded-scope-design.md); định nghĩa nhãn chính
thức ở [return-label-contract.md](return-label-contract.md). Policy đã chốt
không đồng nghĩa source contract/implementation/POC đã hoàn thành.

Lượt audit tích hợp mới phát hiện thêm [IR01–IR25](integration-risk-register.md),
trong đó lỗi resume generator đổi config đã sửa và có regression test.
Người dùng yêu cầu “Hiểu, xử lý” theo đề xuất Q60/Q61: sales mặc định trừ
phần hủy trước giao, Refund tách riêng; bổ sung nguồn Seller operational
giả lập có authority/coverage. [ADR-0011](adr/0011-sales-basis-and-observable-seller-feeds.md)
ghi quyết định. [Source contract v2](source-contract-order-lifecycle.md) và
[publication contract](run-publication-contract.md) cụ thể hóa thiết kế;
chưa thay thế các gate POC. CDC/feature validation đã có sửa cục bộ, phạm
vi kiểm chứng nằm trong [bản xử lý](audit-remediation-20261018.md).

## Hệ thống làm gì

Một Seller bán trên 5 Channel (Amazon, TikTok Shop, Shopify, Walmart, Shopee).
Hệ thống gom dữ liệu bán hàng → làm sạch → phân tích → dự đoán rủi ro trả
hàng → gợi ý hành động.

**Người dùng:** Seller (chính là người học). **Trải nghiệm:** xem danh sách
SKU cần kiểm tra, chọn SKU để xem sales/trend/return-risk và inventory theo
coverage; phân rã biến động, drilldown bằng chứng và hỏi đáp agent. Mọi gợi
ý chỉ **tư vấn**, không tự động đổi giá/hủy đơn.

## Kiến trúc 4 tầng

1. **Data platform** — sinh dữ liệu giả lập → lakehouse (bronze/silver/gold)
   → warehouse PostgreSQL. Xử lý batch (Spark) và stream (Flink), điều phối
   (Airflow), quản trị (DataHub).
2. **ML platform** — feature store (Feast: Spark/Iceberg offline + Redis
   online), train model return-risk (Kubeflow), version model/data (MLflow),
   serve model (KServe).
3. **Agent platform** — LLM inference tự host CPU, 1 UI chat + 1 coordinator
   điều phối 2 agent chuyên trách: data agent (đọc số liệu qua API/feature
   store) và model+knowledge agent (gọi model + RAG). MCP/A2A gateway, sandbox,
   registry.
4. **Cloud & Ops** — Triển khai GCP (GCE → GKE), storage GCS, IaC Terraform +
   Ansible, NGINX ingress, auth/HTTPS, Vault secrets, observability, CI/CD.
   Người dùng tự thao tác Cloud.

> Sơ đồ triển khai chi tiết sẽ vẽ trong README.md khi implement (rubric yêu
> cầu diagram với deployable units + mũi tên dữ liệu).

## Lộ trình 5 phase

| Phase | Mục tiêu | Chính sách đã chốt | Làm gì |
|---|---|---|---|
| **P0** Discovery | Chốt thiết kế nền | Q32–Q61, D1–D25 | **Đã chốt policy tới Q61.** Source/publication contracts đã đặc tả; implementation, dependency pins, sizing và POC còn phải hoàn thành. |
| **P1** Data platform | Nguồn 5 Channel + lakehouse + warehouse | Q28–Q30/Q34/Q35/Q37/Q49/Q52–Q54/Q58/Q60/Q61, D10–D12, D16–D20 | Contract + bộ nhỏ trước; ~365 ngày lịch sử / 180 ngày serve, `dataset_version`. Core Orders 5 Channel; lifecycle theo line/quantity, Listing history và inventory nhỏ theo capability. Facts độc lập và đối soát chống fan-out. Bronze → Silver → Gold/PG, Airflow/DataHub, Spark/Flink/Docker. GCE/GCS POC và sizing trước full-scale. |
| **P2** Feature + ML | Feast + train/eval/serve model return-risk | Q38/Q40/Q41/Q45/Q56/Q57, D13–D15, D21–D25 | Label v2, Feast Spark/Iceberg offline + Redis online; materialize + versioned publish. Kubeflow 4-step pipeline, MLflow, KServe. RAG pipeline qua Feast (POC trước). DP3 feature tables; model/label compatibility tường minh. |
| **P3** Agent platform | UI chat + 2 agent + RAG + LLM | Q39/Q42/Q43/Q44/Q46/Q55/Q59, D1/D2 | Self-host LLM CPU (llm-d), FastAPI, coordinator + 2 agent, MCP/A2A, sandbox/registry/warm-up. Corpus đã duyệt/versioned. UI owner-only tiếng Việt, hàng đợi SKU read-only, phân rã biến động và evidence drilldown. |
| **P4** Cloud & CI/CD | Hoàn thiện triển khai GCP + vận hành | Q33/Q47/Q48 | Hoàn thiện Terraform GKE + Ansible, ingress/auth/HTTPS/Vault, observability, A/B, Jenkins, backup/restore và run budget. Hạ tầng, bảo mật endpoint và POC Cloud bắt đầu từ phase sử dụng chúng. |
| **X** Xuyên suốt | Test + docs + novel ideas | Q50/Q51 | Web API unit coverage >90%; changed-code mutation >80%. Property-based/idempotency, load test, README + docs/, novel ideas. Evidence gắn dataset/run/generation và local/Cloud. |

Quy tắc xuyên suốt: **làm nhỏ/POC trước, full-scale sau (D9/D24)**. Mọi thao
tác Cloud do người dùng tự làm. Thay đổi sau ledger cần impact assessment.

P1→P4 là thứ tự bàn giao, không phải hoãn mọi việc Cloud tới P4. GCE/GCS
được thử nhỏ trong P1; Feast/publication/dual-sink được POC sớm trước khi khóa
đầu ra nguồn lớn; GKE/Kubeflow/KServe được thử trước mở rộng P2; CPU LLM/RAG
được thử trước đầu tư lớn P3. Các gate G0–G4, điều kiện đạt và dependency ở
[implementation-readiness.md](implementation-readiness.md).

## Đầu ra Seller phải giữ trong phạm vi

- D16–D20/Q28: sales/trend 7/30/90 ngày UTC hoàn chỉnh, return cohort đủ hạn,
  canceled/delivery/category/fulfillment theo nguồn; native currency và coverage.
- Q60: headline sales bằng giá trị hàng đặt sau discount trừ giá trị hủy
  trước ship; giữ ordered/canceled/product Refund riêng. Đặt300, hủy100,
  Refund100 thì headline200; không coi là tiền đã thu hoặc lợi nhuận.
- Q61: nguồn OMS/WMS/carrier/post-sales Seller giả lập đi qua Bronze/Silver,
  khai authority/bridge/known-time; thiếu capability thì unknown/abstain.
- Q29: basket/co-purchase, SKU concentration, địa lý (có unknown), discount
  associations; không suy causal hoặc hiệu quả bundle từ dữ liệu mô phỏng.
- Q30: repeat/first-observed customer trong từng Storefront có ID; guest và
  missing tách riêng, không nối customer xuyên Channel.
- Q52–Q54/Q58: lifecycle theo line/số lượng, Listing history, inventory
  nhỏ với pool Seller chung + pool Channel riêng; movement authority,
  snapshot đối soát, Return chờ kiểm trước sellable.
- Q56/Q57: label `return-or-refund-line-v2`; cửa sổ riêng từng phần giao,
  OR thành nhãn line; approved Return request hoặc successful product Refund.
  Cohort lần giao cuối phần thực giao, maturity/coverage áp dụng cả hai lớp.
- Q55: hàng đợi SKU read-only + phân rã biến động + evidence drilldown;
  Q59 tách ba nhóm return-risk, inventory/Listing availability, biến động
  sales; reason/support/freshness và rank riêng, không có global loss score.
- Q31 được Q54 thay một phần: inventory nhỏ đã chọn; tối ưu nhập hàng,
  forecast model thứ hai, traffic/ads/profit vẫn là roadmap riêng.

Mỗi đầu ra phải nối được từ nguồn → Gold/mart → API → UI/citation → evidence.
Scenario S01–S18 ở tài liệu điều kiện triển khai; S19–S30 và impact Q52–Q59
ở [expanded-scope-design.md](expanded-scope-design.md).

## Quyết định thiết kế then chốt

| Chủ đề | Quyết định (ngắn gọn) | Tham chiếu |
|---|---|---|
| **Identity** | Một Seller, shop/account namespace, SKU nội bộ ổn định | Q34 |
| **Nguồn dữ liệu** | 5 Channel, mục tiêu ~365 ngày lịch sử nguồn / 180 ngày serve; số ngày train hữu dụng còn trừ warmup/maturity; observable feeds | Q35, Q49, ADR-0008 |
| **Correction** | Restate số liệu mới, giữ published version cũ để truy lại | Q36 |
| **Kịch bản** | Sinh có chủ đích: late, correction, partial/missing, basket, drift; test độc lập | Q37 |
| **ML action** | Score là tín hiệu xếp hạng/triage (không xác suất), điều kiện đơn được giao, abstain khi thiếu lịch sử | Q38 |
| **Agent** | 1 coordinator + 2 agent read-only, citation + abstention bắt buộc | Q39 |
| **Stream** | Live operational view tách daily official metrics; đủ 5 Channel + return feed | Q40 |
| **Nhãn & metric** | Label v2 binary line theo allocation windows, approval/success, cohort/maturity/coverage; absent ≠ 0 | Q41 được Q56/Q57 làm rõ/thay phần ngữ nghĩa, ADR-0010 |
| **LLM** | Self-host CPU (llm-d), không paid API; POC tiếng Việt trước | Q42 |
| **UI & access** | Owner-only, tiếng Việt; shop overview → SKU → order drilldown | Q43 |
| **RAG** | Corpus đã duyệt + versioned; Feast vector (POC-gated, có fallback); citation nguồn | Q44 |
| **Publication** | Versioned generation + manifest + request pin; chỉ publish sau validate | Q45, ADR-0007 |
| **Serving API** | Ghim version tường minh, trả data_as_of/freshness/coverage; missing ≠ 0; UTC | Q46 |
| **Security** | Một owner qua gateway; rate limit; Vault secrets; sandbox; test prompt injection | Q47 |
| **Operations** | On-demand start/stop; Redis rematerialize khi bật lại; run budget | Q48 |
| **Dataset version** | `dataset_version` pin bộ nguồn; run/attempt/generation là identity riêng, model giữ training lineage | Q49, ADR-0008 |
| **Nghiệm thu** | Ledger từng dòng rubric `de`/`mle`; phase P1→P4; POC trước full-scale | Q50 |
| **Novel ideas** | DE: data contract/schema registry + Iceberg maintenance tự động; MLE: RAG/agent eval harness + drift monitoring | Q51 |
| **Partial lifecycle** | Shipment/Return/Refund độc lập theo line/quantity; label v2 có version riêng | Q52/Q56/Q57, ADR-0009/0010 |
| **Listing history** | Mapping, active status và thuộc tính quan sát có lịch sử | Q53 |
| **Inventory nhỏ** | Pool Seller chung/Channel riêng; movement authority, reservation/inbound, quarantine trước sellable | Q54/Q58 |
| **Kiểm tra SKU** | Ba nhóm read-only, rank riêng; phân rã biến động, evidence drilldown, incident grouping | Q55/Q59 |

## Bảng thuật ngữ cốt lõi

| Thuật ngữ | Nghĩa | Đừng nhầm với |
|---|---|---|
| **Seller** | Merchant duy nhất | Vendor, tenant |
| **Channel** | Nền tảng bán hàng (Amazon, TikTok Shop, ...) | Platform, sàn |
| **Storefront** | Sự hiện diện bán trên một Channel + Marketplace | Shop, store |
| **SKU** | Mã sản phẩm nội bộ, chuẩn xuyên Channel | ASIN, variant |
| **Order Line** | Một sản phẩm + số lượng trong một Order | Item |
| **Return Label v2** | Binary line có approved Return request/product Refund success trong cửa sổ 30 ngày phần giao; đủ maturity/coverage | Nhãn unit, physical-return-only, label v1 |
| **Return-risk score** | Tín hiệu xếp hạng (không phải xác suất) | Xác suất trả hàng |
| **Live operational view** | Trạng thái gần đây từ stream | Số liệu chính thức |
| **Daily official metrics** | Số chốt theo ngày trong một generation; correction qua lần công bố mới, giữ bản cũ | Realtime |
| **Published generation** | Một lần công bố đã validate, ghim bởi manifest | Snapshot |
| **Dataset version** | Định danh một lần sinh dữ liệu cố định | Snapshot ngày |

## POC bắt buộc trước full-scale

| # | POC | Rủi ro nếu bỏ qua |
|---|---|---|
| 1 | Feast Spark/Iceberg offline + Redis online parity | Tính đúng thời gian, TTL, rerun |
| 2 | Feast vector cho RAG (rubric dòng 9) | Nếu fail → fallback vector store riêng (Q44) |
| 3 | llm-d CPU: tiếng Việt, tool-calling, latency, memory | Chất lượng agent không đạt |
| 4 | Iceberg → GCS: FileIO, warehouse mới | Metadata s3:// không migrate được |
| 5 | Publication versioned + manifest xuyên PG/Redis | UI đọc số lẫn phiên bản |
| 6 | Dual-sink Spark offline + Redis online, replay/parity | Stream feature lệch batch |
| 7 | GKE + Kubeflow/KServe trên trial quota | Hết credit, không deploy được |

Các POC trên đều cần evidence; tiêu chí đo, failure cases và tác động khi
fail ở [điều kiện triển khai](implementation-readiness.md). Fallback vector
store riêng phải ghi tác động nghiệm thu `mle` 9, không tự coi đạt Feast.

## Novel ideas

| Idea | Rubric | Mô tả |
|---|---|---|
| **DE-N1** | `de` 39 (10đ) | Data contract + schema registry cho export mỗi Channel |
| **DE-N2** | `de` 40 (10đ) | Maintenance tự động có trigger, retention bảo vệ version đang dùng, recovery và số đo; cần proof phần mở rộng ngoài optimization bắt buộc `de` 20 |
| **ML-N1** | `mle` 56 (2đ) | Evaluation harness RAG/agent (RAGAS-style) + regression gate CI |
| **ML-N2** | `mle` 57 (2đ) | Drift monitoring (Evidently) gắn Sale Event |

## Ràng buộc

- **Ngân sách:** GCP trial credit 300 USD, không chi thêm tiền túi (D6).
- **Cloud:** người dùng tự chạy Terraform/Ansible/kích hoạt deploy; trợ lý
  chuẩn bị cấu hình + giải thích, không tự thao tác Cloud (Q33).
- **Quyền agent:** read-only, không tự đổi giá/hủy đơn/hoàn tiền/gửi thông báo
  (D2).
- **Local:** để học + phát triển + kiểm thử; Cloud là đích triển khai (ADR-0004).
- **Ngôn ngữ:** tiếng Việt (UI, tài liệu).
- **Tiền tệ:** giữ nguyên gốc, không quy USD (D12).

## Bắt đầu từ đâu

1. **Đọc lại code hiện tại** (nếu muốn học hiểu trước khi sửa) — dùng skill
   `teach`, theo `MISSION.md`.
2. **Triển khai P1:** hoàn thiện contract source/time/metric và fixture nhỏ
   theo G0; kiểm chứng G1 cùng các POC nền tảng G2 và ngân sách liên quan
   trước sinh nguồn lớn. Sau đó mới chạy đủ năm Channel ở quy mô nghiệm thu.
3. Tham chiếu: mở `docs/de-acceptance-ledger.md` để biết từng dòng rubric cần
   proof gì; mở `docs/de-discovery-risk-review.md` để xem các phát hiện kỹ
   thuật cần chú ý.

## Index tài liệu tham chiếu

| Cần gì | Mở file nào |
|---|---|
| Từng dòng rubric → phase → proof | `docs/de-acceptance-ledger.md` |
| Gate, hợp đồng, scenario và evidence bắt buộc | [implementation-readiness.md](implementation-readiness.md) |
| Quét cơ hội và scope Q52–Q55 đã chốt | [plan-expansion-discovery.md](plan-expansion-discovery.md) |
| Impact Q52–Q59, S19–S30 và các policy đã chốt | [expanded-scope-design.md](expanded-scope-design.md) |
| Nhãn v2: eligibility/time/correction và expected cases | [return-label-contract.md](return-label-contract.md) |
| Sổ vấn đề IR01–IR25 và điều kiện đóng | [integration-risk-register.md](integration-risk-register.md) |
| Schema nguồn v2, authority, quantity/money và ba horizon | [source-contract-order-lifecycle.md](source-contract-order-lifecycle.md) |
| Frozen inputs, CAS/fencing, model bundle và restore | [run-publication-contract.md](run-publication-contract.md) |
| Code sửa, kiểm chứng và giới hạn còn lại | [audit-remediation-20261018.md](audit-remediation-20261018.md) |
| Ma trận phụ thuộc + phát hiện audit + quyết định Q32–Q51 | `docs/de-discovery-risk-review.md` |
| Quyết định Q1–Q31 + hiện trạng code + Feast/GCP phân tích | `docs/de-expansion-review.md` |
| Thuật ngữ gốc (Seller, SKU, Order Line, ...) | `docs/terminology.md` |
| Thuật ngữ mở rộng (insight, score, live view, ...) | `GLOSSARY.md` |
| ADR (trade-off đã chốt, dùng tên file đầy đủ) | [ADR index](adr/README.md) |
| Mục tiêu học | `MISSION.md` |
| Tóm tắt lịch sử phiên | `NOTES.md` |
| Rubric gốc (read-only tham khảo) | `edai-k11-cw/rubic/EDAI K11 - MLE.xlsx` |
| Proposal khóa học | `edai-k11-cw/coursework_proposal.md` |
