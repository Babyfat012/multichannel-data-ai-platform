# Acceptance ledger — rubric DE/MLE → phase → proof

Trạng thái: **đã chốt (Q50/Q51), 2026-10-17**. Đây là nguồn nghiệm thu chính
thức; thay đổi sau cần impact assessment. Thứ tự thực hiện: P1 → P2 → P3 → P4,
làm nhỏ/POC trước, full-scale sau (D9/D24). Nguồn rubric:
`edai-k11-cw/rubic/EDAI K11 - MLE.xlsx`, sheet visible `de` (41 dòng) và `mle`
(58 dòng). Không dùng sheet hidden cũ. Ledger ánh xạ từng dòng rubric sang
cách đáp ứng theo quyết định đã chốt, phase triển khai, và proof cần nộp.
Chưa phải bằng chứng đã chạy. Bổ sung rà soát 2026-10-17 theo yêu cầu người
dùng: giữ mapping rubric và policy; thêm quy cách evidence/gate, khôi phục
chi tiết rubric bị tóm tắt thiếu. Đây không phải xác nhận đạt điểm.

Quyết định nền: Q32–Q49 và D1–D25 (xem `de-expansion-review.md`,
`de-discovery-risk-review.md`, `docs/adr/`).

## Phase triển khai

- **P0 Discovery/design** — policy đã chốt; contract chi tiết và POC còn mở.
- **P1 Data platform (DE)** — nguồn năm Channel + observable lifecycle feeds,
  bronze/silver/gold, warehouse, orchestration, governance, Docker, Spark/Flink.
- **P2 Feature + ML platform** — Feast (Spark/Iceberg offline + Redis online),
  DP3 feature, Kubeflow, MLflow, KServe, train/eval return-risk.
- **P3 LLM/Agent platform** — LLM inference self-host, gateway, RAG qua Feast,
  MCP/A2A, hai agent + coordinator, registry, sandbox, warm-up.
- **P4 Cloud + CI/CD + Ops** — Terraform/Ansible, GKE, ingress, auth/HTTPS,
  Vault, observability, A/B, CI/CD, runbook.
- **X Xuyên suốt** — test/validation, docs, novel ideas.

Cloud POC, bảo mật endpoint và chuẩn bị vận hành bắt đầu ở phase cần dùng;
P4 hoàn thiện triển khai/proof. Thứ tự dependency và điều kiện full-scale
theo G0–G4, không chờ P4 mới thử Cloud.

## Quy cách đóng một dòng nghiệm thu

Mỗi dòng chỉ được đánh dấu đạt khi evidence ghi rubric ID, expected/actual,
pass/fail, code/config/runtime version, dataset/run/generation, Channel/ngày,
local hay Cloud và đường dẫn log/report/ảnh giải thích kết quả. Ảnh đơn lẻ
không chứng minh tính đúng hoặc phạm vi full-scale. Gate/scenario S01–S18
và phần bất định còn mở tại readiness.

Các dòng pipeline/feature/API liên quan phải có đối soát key/count/amount/
coverage hoặc expected output tương ứng; lỗi phải chặn publication. Một run
thành công không thay proof retry, correction, failure injection và recovery.
Optimization so trước/sau trên cùng input, scope và resource/config được
ghi rõ; benchmark cải thiện phải kèm kiểm tra kết quả dữ liệu không sai.

Rubric **ô A2 của cả `de` và `mle`** còn yêu cầu README giới thiệu business,
repo structure, mục lục, sơ đồ tổng thể theo deployable units; mũi tên có
hướng dữ liệu, mô tả và thứ tự user flow. Docs/ảnh có giải thích và link từ
README; module/function/class có mô tả. Đây là yêu cầu xuyên suốt, liên hệ
`mle` 55, không chỉ kiểm số dòng có điểm từ 3 trở đi.

## Sheet `de`

| Dòng | Yêu cầu (tóm tắt) | Cách đáp ứng | Phase | Proof |
|---|---|---|---|---|
| 3 | Dùng Docker & Compose | Compose sẵn có | P1 | compose + doc optimize image |
| 4 | Optimize Dockerfile (multistage) | Multistage, so size trước/sau | P1/P4 | doc + số đo size |
| 5 | Simulate offline problems ≥100GB + characteristics | Generator năm Channel, Q37/Q49 | P1 | doc skew/cardinality/dup/volume |
| 6 | Simulate schema evolution | Renderer đa dạng schema | P1 | doc schema trước/sau |
| 7 | Offline problem khác (chọn duplicate; rubric ví dụ 2%) | Cấu hình duplicate | P1 | doc tỷ lệ thực + proof |
| 8 | Lưu data sau generate để ingest Bronze | MinIO/PG mô phỏng nguồn | P1 | doc + path nguồn |
| 9 | Simulate late arrivals | Late-arrival event, Q35 | P1/P3 | doc late bao lâu/tỷ lệ |
| 10 | Streaming problem khác (chọn duplicate; rubric ví dụ 1.5%) | Cấu hình duplicate stream | P1/P3 | doc tỷ lệ thực + proof |
| 11 | Spark baseline + step optimize + Spark UI + Airflow | Job hiện có + optimize | P1 | screenshots Spark UI + doc |
| 12 | Handle skew + giải thích | Salting/partitioning | P1 | trước/sau |
| 13 | Handle schema evolution + giải thích | Gộp schema chuẩn | P1 | trước/sau |
| 14 | Offline problem khác + giải thích | Dedup chẳng hạn | P1 | trước/sau |
| 15 | Spark tích hợp pipeline | Airflow DAG | P1 | ảnh DAG |
| 16 | Flink baseline | Job stream hiện có | P1/P3 | Flink UI + doc |
| 17 | Handle late arrival | Watermark/allowed lateness | P1/P3 | trước/sau |
| 18 | Streaming problem khác | Dedup/idempotent sink | P1/P3 | trước/sau |
| 19 | Window processing | Code window + giải thích | P1/P3 | doc code |
| 20 | Optimize lakehouse (compaction/z-order/partition) | Iceberg maintenance | P1 | code + trước/sau |
| 21 | Optimize warehouse (indexing) | PostgreSQL index | P1 | code + trước/sau |
| 22 | DP1 ingest stage; connection/variable dùng chung qua Airflow | Airflow | P1 | ảnh + doc + cấu hình dùng chung, secret qua backend phù hợp |
| 23 | DP1 validate stage | Validation | P1 | ảnh + doc |
| 24 | DP2 ingest stage | Airflow | P1 | ảnh + doc |
| 25 | DP2 validate stage | Validation | P1 | ảnh + doc |
| 26 | DP3 ingest (offline feature table) | Feast offline/feat_ | P2 | ảnh + doc |
| 27 | DP3 validate stage | Validation | P2 | ảnh + doc |
| 28 | DP1 lineage + DataHub | DataHub | P1 | ảnh DataHub |
| 29 | DP1 data validation | Assertions | P1 | ảnh |
| 30 | DP2 lineage | DataHub | P1 | ảnh |
| 31 | DP2 data validation | Assertions | P1 | ảnh |
| 32 | DP3 lineage | DataHub | P2 | ảnh |
| 33 | DP3 data validation | Assertions | P2 | ảnh |
| 34 | Visualize bảng mọi zone | ERD/schema doc | P1 | ảnh + doc |
| 35 | Dim SCD2 (valid_from/to, is_current) | dim SKU/Storefront SCD2 | P1 | code + ảnh |
| 36 | feat_ với event_timestamp + created | Feast/feat_ | P2 | code + ảnh |
| 37 | Quan hệ dim & fact | DBeaver export | P1 | ảnh |
| 38 | Naming convention raw_/stg_/dim_/fact_/obt_/feat_ | Đã theo glossary | P1 | doc |
| 39 | Novel idea 1 (10đ) | **DE-N1** data contract + schema registry | X | idea + proof |
| 40 | Novel idea 2 (10đ) | **DE-N2** Iceberg maintenance tự động | X | idea + proof |

## Sheet `mle`

| Dòng | Yêu cầu (tóm tắt) | Cách đáp ứng | Phase | Proof |
|---|---|---|---|---|
| 3 | Deploy LLM inference platform | Self-host (llm-d), Q42 | P3 | doc setup + benchmark |
| 4 | Setup custom LLM model | Model nhỏ, POC tiếng Việt | P3 | doc config + kết quả |
| 5 | Agent gateway + security/policy | agentgateway, Q47 | P3 | YAML + giải thích |
| 6 | Benchmark model server + optimize | POC latency/memory | P3 | số đo trước/sau |
| 7 | Global model config nối qua gateway | Config chung | P3 | code config |
| 8 | Agent registry | Deploy registry | P3 | ảnh registry UI + code |
| 9 | RAG pipeline: text→chunk→embedding→**Feast** | Q44, POC Feast vector | P2/P3 | ảnh Airflow + DataHub + code |
| 10 | Data governance cho RAG pipeline | Lineage + validation | P3 | ảnh |
| 11 | Incremental materialize offline→online | Q45/Q48 | P2 | ảnh pipeline + code |
| 12 | Streaming feature jobs → offline & online | Q40 dual-sink, POC parity | P2/P3 | ảnh jobs + code |
| 13 | Kubeflow ≥4 steps | load/split/train/eval | P2 | ảnh KF Pipelines |
| 14 | MLflow versioning + tag latest/production | Q38 | P2 | ảnh model/data version |
| 15 | KServe model-inside-image | Q42 | P2 | pod + YAML + ảnh gọi thử |
| 16 | Web API FastAPI + pydantic + healthcheck | Q46 | P3 | code + ảnh test |
| 17 | async concurrency | FastAPI async | P3 | code + load test |
| 18 | MCP tool k8s helm + rollingupdate + fallback | P3 | P3 | YAML + ảnh |
| 19 | Agent dùng MCP, multi-replica + autoscale | data agent, Q39 | P3 | ảnh scale + YAML |
| 20 | Agent trong sandbox | Q47 | P3 | ảnh + config |
| 21 | Publish agent lên registry | Q39 | P3 | ảnh registry |
| 22 | Agent 2: online FS + ML prediction + RAG | model+knowledge agent, Q39 | P3 | ảnh + YAML |
| 23 | MCP Gateway (agentgateway) | Q47 | P3 | YAML + policy |
| 24 | A2A Gateway | Q47 | P3 | YAML + policy |
| 25 | Coordinator multi-replica + autoscale | Q39 | P3 | ảnh + KAgent UI |
| 26 | Publish coordinator lên registry | Q39 | P3 | ảnh registry |
| 27 | Warm-up / agent fleet (Substrate) | P3 | P3 | ảnh worker ít |
| 28 | Unit test coverage >90%, proof test Web API (fixture/mock) | Báo cáo scope và exclusions tường minh | X | ảnh coverage + code |
| 29 | Equivalence partitioning / BVA | X | X | code test tham số hóa |
| 30 | Mutation testing >80% (changed code) | X | X | ảnh mutmut |
| 31 | Idempotency/property-based (ví dụ Hypothesis, CrossHair hỗ trợ) | X | X | code + kết quả |
| 32 | Load test Web API (locust, HTML SLA) | P3/P4 | P4 | HTML + số req/s |
| 33 | CI/CD RAG pipeline (Jenkins) | Q33 | P4 | ảnh Jenkins + file |
| 34 | CI/CD MCP servers & agents (Langfuse prompt) | Q33 | P4 | ảnh + file |
| 35 | CI/CD coordinator | Q33 | P4 | ảnh + file |
| 36 | CI/CD stream feature jobs | Q33 | P4 | ảnh + file |
| 37 | NGINX ingress — metric UI (Grafana) | Q47 | P4 | YAML ingress |
| 38 | NGINX — log UI (Kibana) | Q47 | P4 | YAML |
| 39 | NGINX — trace UI (Jaeger) | Q47 | P4 | YAML |
| 40 | UI test agent | Q27/Q43 | P3 | ảnh UI |
| 41 | UI agent registry | Q43 | P3 | ảnh UI |
| 42 | Basic auth + rate limit ở gateway | Q47 | P4 | YAML + test |
| 43 | Domain + HTTPS cho Web API | Q47 | P4 | ảnh + config |
| 44 | Terraform GKE/cloud | Q33 | P4 | ảnh terraform |
| 45 | Ansible roles trên VM | Q33 | P4 | ảnh ansible |
| 46 | Web API monitoring metrics | Q47 | P4 | ảnh telemetry |
| 47 | Prometheus+Grafana compute metrics | P4 | P4 | ảnh dashboard |
| 48 | Logs | P4 | P4 | ảnh |
| 49 | Traces | P4 | P4 | ảnh |
| 50 | LLM telemetry (input/output/total tokens, TTFT, round-trip, tần suất safety/PII) | P4 | P4 | ảnh metrics đủ từng loại |
| 51 | Agent telemetry (agent/tool calls, failures) | P4 | P4 | ảnh metrics |
| 52 | A/B test agents theo model config | P4 | P4 | doc + dashboard |
| 53 | A/B test LLM versions | P4 | P4 | doc + dashboard |
| 54 | HashiCorp Vault secrets | Q47 | P4 | ảnh Vault |
| 55 | README + docs theo A2 | X | X | README + docs/ |
| 56 | Novel idea 1 (2đ) | **ML-N1** evaluation harness RAG/agent | X | idea + proof |
| 57 | Novel idea 2 (2đ) | **ML-N2** drift monitoring | X | idea + proof |

## Novel ideas — đã chốt (Q51)

DE-N1/DE-N2 dưới đây là lựa chọn cho **scope mở rộng**, không tự thay Novel 1
(Trino federation) / Novel 2 (CDC) trong hồ sơ v1 đã nộp trừ khi learner quyết.
Ma trận giữ hai lớp này riêng tại [Novel v2](de-rubric-matrix.md#novel-v2).
Trạng thái local/GCP và run đang chạy xem [tiến độ v2](de-order-lifecycle-status.md);
ledger giữ yêu cầu nghiệm thu, không dùng test local làm xác nhận đã đạt GCP/full-scale.

DE (10đ mỗi cái):
- **DE-N1**: Data contract + schema registry cho export từng Channel — tự động
  phát hiện/so sánh tiến hóa schema thay vì xử lý thủ công.
- **DE-N2**: Iceberg maintenance tự động (compaction + Z-order + snapshot
  expiry) chạy theo lịch, đo số file/size và tốc độ query trước/sau. `de` 20
  đã yêu cầu storage optimization: proof novel phải thể hiện phần bổ sung
  như trigger tự động, retention bảo vệ generation, failure/retry và kiểm
  tra sau maintenance; không dùng lại riêng ảnh compaction để tự tính hai
  lần điểm. Mức công nhận novel vẫn thuộc người chấm.

MLE (2đ mỗi cái):
- **ML-N1**: Evaluation harness cho RAG/agent (kiểu RAGAS) + regression gate
  trong CI.
- **ML-N2**: Drift monitoring (Evidently) trên feature/prediction gắn với Sale
  Event (covariate drift, Q37).

Dự phòng (chưa dùng): Airbyte ingestion, Debezium CDC, guardrails chống
prompt injection, A/B có kiểm định ý nghĩa.

## POC bắt buộc trước full-scale (bất định kỹ thuật)

1. Feast Spark/Iceberg offline + Redis online: parity, TTL/freshness, rerun.
2. Feast vector/embedding cho RAG (rubric dòng 9) — nếu fail → vector store dự phòng (Q44).
3. llm-d CPU: chất lượng tiếng Việt, tool-calling, latency, memory, concurrency (Q42).
4. Iceberg → GCS: GCSFileIO/gcp-bundle, raw `gs://` connector, warehouse mới (D22).
5. Publication versioned generation + manifest + request pin xuyên PG/Redis (Q45).
6. Stream dual-sink: Spark offline history + Redis online update, replay/parity (Q40).
7. GKE/K8s + Kubeflow/KServe trên quota trial (D8/Q32 ngân sách).

Tiêu chí cụ thể/failure cases và tác động khi fail xem bảng POC trong
readiness. Vector fallback không tự đạt
`mle` 9; phải giữ mục này chưa đạt hoặc có xác nhận nghiệm thu thay thế.
CI có thể test/build tự động; deploy job thực thi tự động sau khi người dùng
chủ động kích hoạt (Q33), có proof cả deploy và rollback theo yêu cầu tương
ứng. Không biến manual trigger thành chỉ hướng dẫn deploy chưa chạy.

## Truy vết phạm vi nghiệp vụ ngoài rubric công nghệ

Cập nhật 2026-10-18 theo Q52–Q55 đã được người dùng xác nhận: thêm partial
lifecycle, Listing history, inventory nhỏ và queue/decomposition/drilldown
read-only. Q56–Q59 cũng đã được người dùng xác nhận; impact cùng S19–S30 tại
expanded-scope-design.md; gate readiness được
mở rộng tương ứng, chưa có evidence đạt và chưa đổi số dòng rubric.
Q54 thay một phần Q31 (inventory nhỏ từ roadmap vào phạm vi kế tiếp);
Q56/Q57 làm rõ/thay phần ngữ nghĩa Q41 bằng label v2; Q58 chốt pool/movement
authority, Q59 ba nhóm queue độc lập. [Contract nhãn](return-label-contract.md)
quy định eligibility/time/correction và L01–L17. Proof/model v1 giữ version
cũ; phải đối chiếu eligibility/cohort/label trước benchmark mới, không đổi
metadata artifact cũ thành v2. Phải đo lại volume,
runtime và replay của feed mới trước full-scale; không cộng volume nhiều
tầng thành raw100GB hoặc dùng proof dataset cũ cho scope mới.

Q28–Q30 cũng là acceptance sản phẩm: sales/returns/canceled/delivery,
basket/concentration/geography/discount và repeat/first-observed customer
phải có nguồn/coverage, Gold/mart, API/UI và phép đối chiếu. Mapping ở bảng
đầu ra Seller trong readiness. Chỉ đạt các
dòng công nghệ chưa đủ chứng minh trải nghiệm Seller đã hoàn thành.

## Audit tích hợp và điều kiện đóng issue

Sổ IR01–IR25 phân biệt reproduced, code
evidence, contract gap, cần quyết định và chờ POC. Một generator resume
đổi seed đã tái hiện và sửa với regression test. Input/offset
fencing, model knowledge cutoff, affected linkage scope, logical row
validation, serving bundle/turn pin và restore epoch là pass criteria
bổ sung trong scope hiện hành. Giữ số dòng rubric; không dùng count ảnh
hoặc số scenario để thay evidence pass. Q60/Q61 đã chốt: headline sales
trừ phần hủy trước ship, Refund tách riêng; nguồn Seller giả lập có authority.
[Source contract](source-contract-order-lifecycle.md), [publication contract](run-publication-contract.md)
và bản xử lý ghi chi tiết và giới hạn proof.

## Ghi chú khoá

- Ledger khóa khi người dùng duyệt. Thay đổi sau cần impact assessment.
- Điểm rubric là mục tiêu, không phải bằng chứng đã đạt; không dùng điểm tự
  đánh giá làm proof.
- Mọi thao tác Cloud do người dùng tự làm (Q33).
