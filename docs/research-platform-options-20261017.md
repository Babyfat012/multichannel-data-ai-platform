# Nghiên cứu lựa chọn nền tảng trước triển khai FSDS

Ngày tra cứu: 2026-10-17. Đọc cùng [plan](high-level-architecture-plan.md),
[readiness](implementation-readiness.md) và [ledger](de-acceptance-ledger.md).
Đây là nghiên cứu thiết kế và đề xuất phép thử; chưa chạy POC, chưa kiểm tra
tài khoản/quota, chưa chọn cấu hình triển khai mới. Những mục ghi “đề xuất”
là suy luận áp dụng cho FSDS. Tài liệu `latest`, `master`, `main` có thể thay
đổi; mỗi POC phải chốt release/commit và image digest trước chạy.

## Kết quả ảnh hưởng lựa chọn kiến trúc

1. Feast có đường Spark và vector, nhưng cần chốt **adapter + phiên bản + API**,
   không chỉ tên sản phẩm. Tài liệu và mã nguồn mới có khác biệt đáng kể.
2. Thời điểm nghiệp vụ, thời điểm biết thông tin và generation đang phục vụ
   là ba bài toán riêng. Một PIT join đạt chưa chứng minh hai bài toán còn lại.
3. Có thể thử Feast + pgvector riêng cho RAG trong khi Redis giữ feature dạng
   bảng. Đây là lựa chọn vẫn dùng Feast, khác với bỏ Feast khỏi retrieval.
4. Có ví dụ CPU chính thức cho llm-d, nhưng cấu hình ví dụ không vừa một
   trial nhỏ. Cần đo engine CPU rồi mới đo llm-d và chuỗi nhiều agent.
5. Kubeflow Pipelines standalone và KServe Standard là ứng viên giảm footprint
   mà giữ công cụ rubric. Không được suy từ đó rằng cả hệ thống vừa quota.
6. Cần thiết kế shutdown/restore, bootstrap secret và bảo vệ dữ liệu được
   tham chiếu ngay cùng publication; bổ sung ở cuối sẽ ảnh hưởng nhiều module.

## 1. Feast: chọn đường thực thi, không chỉ chọn store

Tài liệu Spark contrib cảnh báo coverage chưa đầy đủ; bảng chức năng ghi
historical retrieval có hỗ trợ nhưng `offline_write_batch` chưa có. Vì vậy
không thể lấy trang này làm bằng chứng dual-sink đã được giải quyết.
[Nguồn: Feast Spark](https://docs.feast.dev/reference/offline-stores/spark).

Mã `master` được đọc có `filter_by_created_timestamp=False` mặc định; SQL chỉ
lọc `created_timestamp <= entity_timestamp` khi bật cờ. Cùng file đã có đường
ghi `IcebergSource`, nhưng helper đọc source này gọi `scan().to_arrow()` rồi
tạo Spark DataFrame. Đây là dấu hiệu cần kiểm tra bộ nhớ driver, không phải
bằng chứng mọi đường Iceberg đều không scale. Phải đối chiếu bản cài thực tế.
[Nguồn: Spark adapter source](https://raw.githubusercontent.com/feast-dev/feast/master/sdk/python/feast/infra/offline_stores/contrib/spark_offline_store/spark.py).

**Đề xuất so hai đường trên cùng fixture:**

| Đường | Giá trị cần thử | Điểm phải bác bỏ được |
|---|---|---|
| `SparkSource` đọc bảng/query qua Spark catalog Iceberg đã cấu hình | Giữ đọc/lọc/join trong Spark | Session có đúng catalog/jar; scan có pruning; không collect toàn lịch sử về driver |
| `IcebergSource` của release được pin | Dùng tích hợp source trực tiếp | Đường đọc/ghi, auth/catalog, bộ nhớ và materialization thực tế; không suy khả năng từ `master` |

Test nhỏ PF01: một entity có v1 event ngày 1, observed ngày 1, giá trị 10;
v2 correction cùng event ngày 1, observed ngày 5, giá trị 20. Query cutoff
ngày 3 phải ra 10, cutoff ngày 6 ra 20; thêm entity chưa tồn tại, TTL sát
biên, timestamp trùng và timezone khác UTC. Test giá trị bằng oracle tự viết,
không dùng SQL Feast làm expected. Nếu mapping `created` là thời gian chạy
replay hiện tại thay vì thời gian thông tin sẵn có, bật cờ vẫn chưa đúng.

Đề xuất lưu feature history với các mốc có nghĩa tường minh rồi xác định
mapping vào API. Nếu release chưa thực thi known-at đúng, cân nhắc dựng
feature tại từng cutoff hợp lệ bằng Spark trước retrieval hoặc adapter riêng
có test. Đây là lựa chọn phải ghi tác động, không tuyên bố PIT tự có.

Test PF02: tăng dần dữ liệu feature từ bộ nhỏ theo nấc 10×, giữ giới hạn RAM
driver; ghi physical plan, bytes scanned, RSS, spill, materialize throughput.
Không cần sinh 100GB raw để phát hiện đường gom feature về một process.
Phân biệt quy mô raw, feature history, training matrix và online active set.

## 2. Redis và publication: version phải đi vào lookup vật lý

Feast Redis dùng key dẫn xuất từ project và entity; các Feature View cùng
entity chung một hash. TTL key xóa cả hash; cập nhật một view có thể kéo dài
đời sống view khác. Tài liệu phân biệt TTL offline của Feature View với expiry
online. Do đó cần kiểm chứng theo bản pin và trả freshness theo từng feature.
[Nguồn: Feast Redis](https://docs.feast.dev/reference/online-stores/redis).

**Đề xuất so ba namespace**, chọn bằng PF03 trước tạo schema chính thức:

| Phương án | Lợi ích | Chi phí/ràng buộc |
|---|---|---|
| Entity key gồm business key + generation | Lookup pin tường minh; cùng Feature View | Cardinality/RAM tăng; phải định nghĩa thời hạn generation |
| Project/registry namespace theo generation | Cách ly definition và key | Quản lý nhiều registry/config; lifecycle khó hơn |
| Hai serving slot độc lập, manifest trỏ slot | Có thể thử deploy/rollback đơn giản | Không được tái dùng slot khi request/training còn giữ; hai slot không đủ lịch sử dài |

PF03 dùng g1 với giá trị 10, g2 với 20. Giữ request g1 trong khi ghi g2;
kill writer giữa PG và Redis; writer khác cố publish; làm mất ACK; restart
API; request g1 vẫn phải ra đúng 10 hoặc lỗi version có nghĩa, tuyệt đối
không trả 20 dưới nhãn g1. Lưu generation trong response thôi chưa đủ.

Đề xuất manifest là điểm công bố duy nhất có compare-and-swap/transaction,
candidate immutable và trạng thái `prepared → validated → published`.
Manifest ghi exact PG namespace, Redis namespace, snapshot mỗi bảng,
feature definition, model digest và corpus version. Không giả định có
transaction chung PG/Redis/Iceberg. Reader không tìm thấy một phần generation
thì fail closed/abstain; không tự đọc latest ở backend khác.

Với stream, cần chọn chủ sở hữu retry: đề xuất log/history bền vững rồi
materialize idempotent, hoặc hai sink kèm reconciliation riêng. PF04 ngắt
offline sink sau online write, replay event trùng/đảo thứ tự và correction có
event time cũ; so parity tại cùng cutoff. Chỉ pipeline không throw chưa đủ.

## 3. Feast vector: thêm lựa chọn trước fallback ngoài Feast

Feast PostgreSQL hỗ trợ pgvector và cấu hình vector; trang alpha vector mô
tả API/backend chưa đồng nhất và API đang chuyển thế hệ. Redis không xuất
hiện trong bảng backend vector trên trang này; không suy Feast Redis tự có
retrieval vector chỉ vì Redis sản phẩm có tính năng vector.
[Feast PostgreSQL](https://docs.feast.dev/reference/online-stores/postgres),
[Feast vector](https://docs.feast.dev/master/reference/alpha-vector-database).

**Ứng viên mở rộng:** hai feature repository/service có cấu hình rõ: tabular
Spark/Iceberg→Redis cho model, RAG→Feast PostgreSQL/pgvector cho retrieval.
Có thể dùng cùng server PG với database/schema và role tách; vẫn đo contention,
extension, backup và footprint. Không mặc định một cấu hình Feast dùng được
nhiều online backend cùng lúc. Đề xuất này cần cập nhật impact assessment
Q44/Q45; chưa phải quyết định được duyệt.

PF05: 20 tài liệu tiếng Việt, hai corpus version, một chunk bị sửa, một
chunk bị xóa; query có SKU/mã chính xác, dấu/không dấu, câu paraphrase và
ngoài corpus. Kiểm tra top-k + citation đúng version, filter trước/sau search,
không trộn embedding model/dimension, xóa có hiệu lực và retry ingest.
So với baseline keyword để biết embedding mang giá trị gì; không cần thêm
search engine chỉ để có baseline. Feast vector fail thì fallback ngoài
Feast có thể giữ demo hoạt động, nhưng `mle` 9 vẫn chưa tự đạt.

## 4. CPU LLM: đo engine và chuỗi agent riêng

Repo llm-d có ví dụ CPU: không accelerator, vLLM CPU image, prefill tắt.
Ví dụ được đọc cấu hình hai decode replica, mỗi replica yêu cầu 8 CPU/32Gi.
Đó là cấu hình ví dụ, không phải tài nguyên tối thiểu. Không copy nguyên
manifest để suy khả năng chạy trial. [Nguồn: llm-d CPU scenario](https://github.com/llm-d/llm-d-benchmark/blob/main/config/scenarios/examples/cpu.yaml).

vLLM CPU có yêu cầu theo ISA, dtype và build; KV cache và thread binding ảnh
hưởng tài nguyên. Vì vậy số parameter của model không đủ để tính RAM/latency.
[Nguồn: vLLM CPU](https://docs.vllm.ai/en/latest/getting_started/installation/cpu/).

Đề xuất PF06 chia ba phép đo: engine CPU trực tiếp; cùng engine qua llm-d;
coordinator→hai agent→tool/RAG. Dùng cùng model/tokenizer revision, context
limit và bộ câu hỏi; ghi TTFT, tokens/s, end-to-end p50/p95, RSS, CPU throttling,
queue time, timeout, cold start và số lượt model cho mỗi tác vụ. Test tool
JSON/schema, tiếng Việt, missing data và prompt injection; chạy concurrency
1 trước rồi 2. Thêm timeout/cancel để request đã hủy không giữ CPU vô hạn.

Điểm mở rộng có ích: budget token/latency cho từng tác vụ, cache response
chỉ khi key gồm generation/model/prompt/corpus, deterministic renderer cho
số liệu và citation, context giới hạn theo intent. Không chuyển sang API trả
phí hoặc GPU để che POC CPU fail. Thay engine/framework đã chốt cần ghi
tác động rubric và policy; benchmark baseline không phải đổi kiến trúc.

## 5. Kubernetes: giữ công cụ, giảm phần triển khai chưa cần

Kubeflow hỗ trợ cài Pipelines standalone; tài liệu đánh dấu flavor dev là
phi production. Đây là ứng viên cho owner-only demo bốn bước, nhưng vẫn cần
auth và proof trên Kubernetes. Local runner không thay test retry, resource
policy và credential trên cluster.
[KFP installation](https://www.kubeflow.org/docs/components/pipelines/operator-guides/installation/),
[KFP local execution](https://www.kubeflow.org/docs/components/pipelines/user-guides/core-functions/execute-kfp-pipelines-locally/).

KServe Standard dùng Deployment/Service/network/HPA và không đòi Knative;
HTTP scale-from-zero không được hỗ trợ trong guide v0.17. Guide này yêu cầu
Kubernetes v1.32+; phải pin tương thích với GKE chọn, không gắn mọi version
KServe vào cùng một yêu cầu. [Nguồn: KServe v0.17 installation](https://kserve.github.io/website/docs/0.17/admin-guide/kubernetes-deployment).

PF07: render manifests trước; cộng requests/limits tất cả controller,
webhook, ingress, DB, metrics, sidecar, model và replica. Kiểm tra CPU/RAM
allocatable, PVC/RWO scheduling và upgrade surge. Sau đó người dùng chạy
cluster nhỏ: pipeline bốn bước → artifact → image model → InferenceService
→ gọi API → rollback. Test NGINX→service trong topology thực, không thêm
gateway/service mesh trùng chức năng mà chưa biết lý do.

Đề xuất lịch bật theo nhóm (DE build, ML train, online demo) nhưng phải có
một nhóm tích hợp chứa mọi service cần cho user flow và một phiên prove
multi-replica/autoscale theo ledger. Chạy từng công cụ rời không chứng minh
end-to-end. Phân biệt HPA agent/coordinator với năng lực inference model:
tăng replica agent có thể chỉ tăng hàng đợi trước cùng CPU LLM.

## 6. Trial và chi phí: kiểm tra feasibility trước khởi động đồng hồ

Trang Free Trial nêu 300 USD/90 ngày; nâng lên Paid mở billing cho phần vượt
credit. Giữ ràng buộc không nâng tài khoản để giải quyết thiếu quota khi chưa
có quyết định mới. Điều khoản trial nêu giới hạn tám core đồng thời; quota
thực tế vẫn cần người dùng kiểm tra. Trial mặc định không có GPU quota.
[Free Trial](https://docs.cloud.google.com/free/docs/free-cloud-features),
[Trial terms](https://cloud.google.com/terms/free-trial),
[GKE GPU quota](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/gpus).

GKE có thành phần phí quản lý và tài nguyên sử dụng; free-tier credit không
đồng nghĩa toàn bộ node/storage/network miễn phí. Lấy giá tại region và thời
điểm chạy, ghi nguồn và tổng giờ dự kiến; tài liệu này không đưa dự toán giả
làm báo giá. [Nguồn: GKE pricing](https://cloud.google.com/kubernetes-engine/pricing).

Budget cảnh báo không phải cam kết hard cap toàn tài khoản. Tài liệu mới có
spend caps nhưng chỉ cho một số dịch vụ, danh sách được đọc không gồm GCE/GKE;
enforcement còn có độ trễ. Không dùng tính năng đó làm bảo đảm không vượt
tiền túi sau khi nâng Paid.
[Nguồn: spend cap limitations](https://docs.cloud.google.com/billing/docs/how-to/budgets-spend-caps).

PF08 là worksheet do người dùng xác minh: trial còn bao lâu/bao nhiêu credit,
region/quota, footprint nhóm tích hợp, giờ warmup/train/benchmark/restore,
node và quản lý cluster, disk/snapshot/GCS, requests/egress, IP/LB/NAT,
registry image, telemetry retention và KMS nếu chọn. Dự trù một lần chạy lỗi
và một restore drill; không dùng hết credit cho lần demo thành công đầu tiên.
Chuẩn bị local trước, người dùng chủ động thao tác Cloud theo Q33.

## 7. On-demand: giữ gì, dựng lại gì và lấy secret đầu tiên ở đâu

Vault auto-unseal phụ thuộc seal/KMS còn hoạt động; recovery keys không thay
được KMS bị mất. GKE Workload Identity Federation cung cấp identity workload
để truy cập API Google Cloud. Đây là hai thành phần khác nhau, cần thiết kế
bootstrap tránh vòng phụ thuộc “phải có secret trong Vault mới mở được Vault”.
[Vault seal](https://developer.hashicorp.com/vault/docs/concepts/seal),
[GKE workload identity](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

Đề xuất phân lớp state:

| State | Chính sách đề xuất cần chốt |
|---|---|
| Raw/manifest, catalog, PG publication, model/artifact, feature registry | Backup nhất quán; đủ để dựng lại; kiểm checksum/version |
| Kafka offsets, Flink checkpoint/savepoint, Airflow/KFP/MLflow metadata | Giữ hoặc có protocol replay tường minh; không tự bỏ vì là “control plane” |
| Redis, vector index, DataHub search index | Chỉ coi rebuildable sau khi chứng minh tái dựng từ source/metadata |
| Secret/unseal material, KMS, certificate/DNS, Terraform state | Bootstrap/restore độc lập; dữ liệu nhạy cảm có nơi giữ và role rõ |
| Cache model, image, package | Tính cả download/cold-start/network; pin digest tránh đổi kết quả sau restart |

PF09: dựng môi trường trống từ backup, credential bootstrap tối thiểu;
không dùng config/cache còn sót trên laptop để vô tình vượt test. Restore
catalog/PG/registry, rematerialize Redis, verify generation/key/count rồi
mới ready API. Đo RPO/RTO và failure nếu backup thiếu một thành phần. Lập
ma trận hành động dừng: pod, VM, node pool, cluster, DB/storage không có cùng
ngữ nghĩa. Compute Engine còn có quy tắc riêng về Local SSD khi stop.
[Nguồn: stop/start instance](https://docs.cloud.google.com/compute/docs/instances/stop-start-instance).

## 8. Lineage, publication và retention phải nối thành một contract

Iceberg có branch/tag và retention; query tag/snapshot và branch có khác biệt
về schema. Tên branch không tự bảo đảm replay đúng schema lịch sử.
[Nguồn: Iceberg branching](https://iceberg.apache.org/docs/latest/branching/).

Iceberg cảnh báo orphan cleanup với retention ngắn hơn write đang chạy có
thể xóa nhầm dữ liệu. Vì vậy maintenance không chỉ là tối ưu số file.
[Nguồn: Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/).

DataHub cung cấp API tạo lineage giữa các asset. **Đề xuất FSDS:** lineage
hiển thị để truy vết, còn manifest/retention registry quyết định an toàn khi
xóa; không coi graph hiển thị tự thay enforcement của storage.
[Nguồn: DataHub lineage API](https://docs.datahub.com/docs/api/tutorials/lineage/).

PF10: tạo generation g1 và model m1 tham chiếu snapshot/corpus cũ; publish
g2, đổi schema, chạy maintenance trong lúc một request vẫn pin g1. g1/m1
phải đọc được dữ liệu/schema đã cam kết. Khi hết retention phải trả trạng
thái expired rõ, không tráo latest. Test restore catalog mất metadata nhưng
data file còn, và metadata còn nhưng file bị xóa; hai trường hợp không tương
đương. Đề xuất manifest lưu graph tham chiếu đủ snapshot mỗi bảng, model,
corpus, registry và evidence; mark/sweep có dry-run trước physical delete.

## Thứ tự thử và quyết định cần khóa

1. Khóa vocabulary event/known/computed/published, fixture oracle và ma trận
   runtime; chọn candidate artifact versions, chưa sinh thêm nguồn lớn.
2. PF01/PF03/PF04 trên bộ nhỏ: chứng minh known-at, lookup version và replay.
   Ba kết quả quyết định schema feature/publication.
3. PF02/PF05/PF06 local: memory path, Feast vector và CPU LLM. Kết quả quyết
   định online backend, model/context, pipeline grouping và capacity.
4. Render PF07 và worksheet PF08; người dùng xác minh trial/quota/giá. Chưa
   đạt budget thì sửa topology/schedule hoặc ghi phần nghiệm thu còn mở.
5. PF09/PF10 trước full-scale: restore và retention. Sau đó mới chạy nhóm
   tích hợp Cloud do người dùng kích hoạt và mở rộng dữ liệu theo gate.

Các mở rộng trên làm sâu khả năng đúng/replay/đo đạc của plan hiện tại.
KFP standalone, KServe Standard, Feast pgvector và namespace generation là
**ứng viên thiết kế**, không tự đổi quyết định đã chốt. Công cụ ledger vẫn
phải có proof riêng; feature optional như cache thông minh, search hybrid,
multi-tenant hoặc GPU serving không nên trở thành phụ thuộc mới của v1.
