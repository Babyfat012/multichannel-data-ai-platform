# Rà sâu tính nhất quán khi build, phục vụ và khôi phục

Ghi chú sau xử lý cùng ngày: các nhận xét code bên dưới mô tả snapshot
trước sửa CDC/feature validator. [Bản xử lý](audit-remediation-20261018.md)
và [sổ IR](integration-risk-register.md) ghi code/tests mới; POC publication,
restore và Kafka–Iceberg v2 vẫn mở.

Ngày 2026-10-18. Đối chiếu [plan](high-level-architecture-plan.md),
[readiness](implementation-readiness.md), [label v2](return-label-contract.md),
[ADR-0007](adr/0007-versioned-publication.md),
[thiết kế Q52–Q59](expanded-scope-design.md),
[nghiên cứu nền tảng](research-platform-options-20261017.md) và DAG/job hiện có.
Đây là phân tích failure bằng counterexample, **chưa phải kết quả POC**.
Tài liệu chính thức được đọc ngày này; mọi API/library phải đối chiếu bản pin
khi thực thi. Những giải pháp dưới đây là đề xuất kỹ thuật trong policy đã
duyệt, không tự thêm service, quyền Cloud hoặc tính năng sản phẩm.

Các yêu cầu “cùng generation”, “restore” và “read-only” đã có trong plan.
Phần mới ở đây là các chỗ những yêu cầu đó vẫn có thể thất bại, dù từng
store/task báo thành công. Các ID SR là fixture đề xuất để ánh xạ vào S/G;
chưa tự tuyên bố tăng số scenario nghiệm thu đã được implement.

## SR01 — Publication đúng không cứu được đầu vào build bị trộn

**Counterexample.** DP2 dựng Silver Amazon từ Bronze snapshot a1. DP1 tiếp
tục replace Bronze Walmart thành w2 trước lúc task Walmart của DP2 đọc.
Gold sau đó nối a1+w2. DP3 features chạy trước correction, label bridge
chạy sau correction. Tất cả output có cùng generation lúc publish và đúng
count nội bộ nhưng không thuộc một đầu vào/cutoff thống nhất. Mỗi DAG hiện
`max_active_runs=1` chỉ giới hạn DAG đó; không tạo khóa chung DP1/DP2/DP3.
Các DAG truyền tên bảng và `run_id`, chưa truyền input snapshot vector.
[DP1](../dags/dp1_bronze.py), [DP2](../dags/dp2_silver.py),
[DP3](../dags/dp3_features.py), [label bridge](../dags/dp3_walmart_labels.py).

**Hệ quả.** Nhãn/features lệch, reconciliation và baseline không tái lập;
candidate có thể được validate bằng nguồn latest khác nguồn thực build.

**Đề xuất trong Q45/Q49.** Một build manifest bất biến ghi file manifest,
dataset/cutoff, snapshot ID từng bảng, feed coverage, frozen config và code
hash. Task, retry và validator cùng nhận manifest này; validator đọc đúng
snapshot output đã commit. Tạo downstream manifest từ exact upstream output,
không resolve `latest` lại. Số snapshot ID khác nhau giữa bảng là bình thường;
điều cần chứng minh là cùng hợp đồng dataset/cutoff/coverage. Không lấy cùng
timestamp commit làm bằng chứng nghiệp vụ đồng nhất. Nếu source đang đọc
JDBC thì cũng phải có export/staging snapshot hoặc protocol consistency phù
hợp, không giả định parallel JDBC partition reads chung transaction.

Iceberg có read theo snapshot ID; tính năng này cung cấp cơ chế pin từng
bảng, không tự tạo snapshot đa bảng.
[Iceberg queries 1.7.2](https://iceberg.apache.org/docs/1.7.2/spark-queries/).
Trong PostgreSQL 15, hai SELECT ở Read Committed có thể thấy hai snapshot
khác nhau, nên chỉ nói “cùng transaction” chưa đủ.
[PostgreSQL isolation](https://www.postgresql.org/docs/15/transaction-iso.html).

**Pass/G1–G2, P1/P2.** Publish correction giữa hai task, sửa runtime variable
giữa retry, chạy DP1 cùng DP2. Kết quả hoặc giữ đúng input manifest/oracle,
hoặc chặn build với conflict; không tạo candidate trộn. Retry validator vẫn
đọc snapshot output cũ. Feature và label ghi chung input lineage có thể kiểm.

## SR02 — Cutoff nghiệp vụ không thay được offset boundary

**Counterexample có trong code.** Ingest CDC đọc `earliest → latest`; validator
lại tự đọc `earliest → latest` sau đó. Nếu có một event mới giữa hai task,
validator báo thiếu dù ingest đã nạp đủ phạm vi lúc bắt đầu. Sau retention,
Bronze giữ lịch sử nhiều hơn Kafka còn giữ nên so tổng count cũng sai.
[Ingest](../spark_jobs/cdc/ingest_walmart_order_changes.py),
[validator](../spark_jobs/cdc/validate_walmart_order_changes.py).

Một lỗi khác: snapshot DB tại t0, stream bắt đầu bằng event-time >t0. Một
event cũ nhưng commit/được quan sát sau t0 bị bỏ; snapshot/stream overlap
cũng có thể đếm hai lần. Offset Kafka không đồng nghĩa LSN PostgreSQL hoặc
thời điểm nghiệp vụ. Topic bị xóa/tạo lại có thể tái dùng partition+offset;
khóa Bronze hiện chỉ `(topic, partition, offset)` sẽ nhầm là event cũ.

**Đề xuất trong Q35/Q36.** Run manifest ghi source incarnation/epoch,
cluster/topic identity, partition set, start-inclusive/end-exclusive offsets,
LSN/snapshot handoff khi có CDC, checkpoint/savepoint và source availability
cutoff riêng. Bounded ingest và validator dùng chính range đó. Reconcile
theo range/identity; retained Bronze không phải bằng retained Kafka count.
Snapshot bootstrap và delta phải có handoff/source sequence + idempotency
được kiểm; không nối bằng một phép lọc event-time. Khi offset cần replay đã
hết retention, fail rõ và dùng rebuild protocol, không lặng lẽ bỏ đoạn thiếu.
Reset topic/source cần epoch mới; không tái dùng lineage cũ.

Spark Kafka cho phép chỉ định offsets theo từng partition. Debezium mô tả
snapshot ban đầu và tiếp tục tại vị trí WAL gắn với snapshot; ứng dụng FSDS
vẫn phải giữ/đối soát trạng thái và phạm vi mà nó sử dụng.
[Spark Kafka guide 3.5.6](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html),
[Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
Guide Spark 3.5.6 là nguồn semantics tham khảo, không đổi runtime 3.5.3 hiện
có; POC phải kiểm đúng bản pin và connector Debezium thực dùng.

**Pass/G1–G2/G4, P1/P2/P4.** Append event trong lúc validate; overlap một
update/delete giữa snapshot và stream; late event có event-time trước cutoff;
expire offset; recreate topic với offset 0. Chứng minh không mất/nhân event,
epoch không va; gap bị chặn và phục hồi bằng protocol đã ghi. Mở rộng S13/S17.

## SR03 — Restore cần kiểm membership và ngăn writer cũ sống lại

**Counterexample.** Manifest restore ở g9, PG backup chỉ có g8, Redis còn g10
và Airflow đánh dấu task g9 success. API thấy các service healthy nhưng g9
thiếu output. Hoặc writer cũ đang treo sống lại sau restore rồi CAS pointer
thành công vì token/generation ID bị tái dùng. Giữ nguyên tên ID không chứng
minh giữ nguyên nội dung và nguồn gốc.

**Đề xuất trong Q45/Q48.** Recovery manifest ghi backup set, exact artifact
identities/checksums, store namespaces và lineage. Trước ready, kiểm toàn bộ
dependency closure của generation được phục vụ, không chỉ kiểm con trỏ hay
count. Có recovery epoch/fencing token để writer/checkpoint từ incarnation
cũ không được publish; tạm khóa writer/scheduler trong recovery rồi mở sau
reconciliation. ID bất biến không được tái cấp cho nội dung khác. Lease/pin
retention cũng cần hết hạn và renew có kiểm: process chết không giữ snapshot
vĩnh viễn, process còn sống không bị cleanup do lease đã mất mà không biết.
Thời hạn cụ thể vẫn là cấu hình phải đo trước G4, không tự đặt trong note.

**Pass/G2/G4, P2/P4.** Restore các store từ ba thời điểm khác nhau; thử writer
cũ publish sau restore, pin đang chạy mất lease và request retry từ cache.
API chỉ ready khi exact dependency set kiểm được; phần lỗi trả unavailable
có lý do, không tự đọc latest. Writer cũ bị fence; rebuild g9 không nhận g10
dưới tên g9. Mở rộng S11/S12/S17/S18 và PF09/PF10.

## SR04 — Model digest phải nằm trong bundle thực chạy và lúc routing

**Counterexample.** MLflow trỏ model v2 nhưng image `risk:latest` vẫn chứa
weights v1; hoặc pod mới tải mutable artifact path khác pod cũ. Hai model
có cùng kiểu cột nhưng một model dùng label v1 và feature units khác. A/B
đẩy request tới model không tương thích; response vẫn ghi model v2 lấy từ
config gateway. Shape check không bắt được lỗi ngữ nghĩa này.

**Đề xuất trong Q38/Q45/Q46.** Deployment bundle pin image digest, weight
checksum, preprocessing/vocabulary, ordered feature schema/units/defaults,
feature definition, label-contract hash, output/calibration semantics và
training lineage. Startup xác minh artifact thực nạp; readiness/response
trả observed bundle identity. Gateway chỉ route tới endpoint khai báo tương
thích với generation/request contract. Nếu model đóng trong image thì kiểm
checksum bên trong image, không lấy image digest thay model checksum. A/B
so đúng target và cùng input eligibility; rollback cả bundle và routing.
Nhiều model/prompt/corpus lifecycle vẫn có thể độc lập; không ép mọi thay đổi
thành một global version hoặc buộc retrain mỗi lần data generation tăng.

Kubernetes nêu tag có thể đổi, còn image digest pin nội dung image; đây chưa
phải bảo đảm file model tải ngoài image bất biến.
[Kubernetes images](https://kubernetes.io/docs/concepts/containers/images/).

**Pass/G3, P2/P3/P4.** Sai weights dưới đúng tên model; reorder feature nhưng
giữ dtype; đổi cents→currency unit; route v2-label input vào v1 endpoint;
rolling deployment xen pod cũ/mới. Phải chặn trước inference hoặc route đúng
bundle, trace đúng artifact đã chạy. Mở rộng S14 và proof A/B/rollback.

## SR05 — Một câu trả lời gồm nhiều hop, page và cache cũng là một request

**Counterexample.** Coordinator pin g1, agent con tự resolve g2; tool retry đọc
g3. Trang queue đầu g1 và trang drilldown g2 có số liệu không khớp. Cache
key chỉ có SKU làm USD/VND, Storefront khác nhau hoặc current/restated va
nhau. Turn chat mới dùng số từ turn cũ như evidence hiện tại mà không gọi
tool lại. Trường `generation=g1` trong JSON không sửa được những trường hợp
này.

**Đề xuất trong Q39/Q43/Q45/Q46.** Request context do server phát/validate
đi xuyên coordinator→agent→tool→model→RAG: generation, cutoff/view mode,
metric-contract hash, entity namespace, currency/window/filter và artifact
versions liên quan. Pagination/drilldown cursor pin cùng scope/version;
cache key chứa toàn bộ inputs ảnh hưởng kết quả và authorization context.
Không tin generation/scope do LLM tự dựng. Một turn trả lời mới chọn bundle
rõ, revalidate evidence cũ; so hai generation là thao tác có khai báo từng
vế. Pin không còn hợp lệ thì expire/restart câu trả lời, không tự fallback
latest giữa chừng. Quy tắc là invariant kỹ thuật; không mở thêm saved state
hoặc thay UI read-only đã chốt.

**Pass/G3, P3.** Publish giữa hai tool call, retry sau timeout, paginate qua
publish, g1 hết retention, cùng SKU khác Storefront/currency, cache cũ và
follow-up turn. Mỗi claim phải truy đúng version/scope hoặc abstain/expire;
không có một câu trả lời vô tình cộng các scope. Mở rộng S11/S15/S16/S26.

## SR06 — “Mọi output bắt buộc” cần một danh sách kiểm được

**Counterexample.** Core sales đủ năm Channel nhưng inventory feed thiếu một
Storefront. Nếu toàn bộ candidate bị chặn vô hạn thì sales không cập nhật;
nếu coi inventory optional hoàn toàn thì queue có thể báo “không có vấn đề”
vì thiếu input. Return source được khai báo unsupported lại vô tình sinh
negative labels hoặc label denominator hợp lệ.

**Đề xuất trong Q35/Q45/Q52–Q59.** G0 phải có capability matrix versioned theo
feed/field/Storefront/time, cùng dependency matrix từng output: mandatory
input, expected partitions/range, completeness predicate, freshness và trạng
thái `complete/unsupported/missing/conflict/stale`. Chọn publication unit trên
bộ nhỏ rồi ghi vào manifest: output bắt buộc thất bại chặn generation; output
được contract cho phép unavailable phải có trạng thái đó và khiến các tính
năng phụ thuộc abstain. Không tự hạ requirement năm Channel hoặc bằng chứng
rubric khi một feed thiếu. Capability có thể đổi theo thời gian, không chỉ
một boolean gắn Channel vĩnh viễn. Đừng lấy expected output từ chính payload
đã ingest vì một feed mất hoàn toàn sẽ biến khỏi expectation.

**Pass/G0–G3, P1/P2/P3.** Mất toàn bộ file feed, feed đúng schema nhưng thiếu
một ngày/Storefront, capability đổi, late backfill và phân vùng empty hợp lệ.
Chứng minh khác biệt zero/unsupported/missing; label eligibility và ba queue
đúng predicate. Mọi đầu ra plan phải có owner/input/coverage/expected evidence
để không mất scope qua chữ “optional”. Mở rộng S07/S26/S30.

## SR07 — Read-only và một owner vẫn cần giới hạn quyền dữ liệu và công việc

**Counterexample.** Prompt không ghi DB nhưng tool SELECT dùng credential
chủ DB nên đọc secret/raw PII; tool nhận tên bảng, filesystem path hoặc URL
tùy ý và đọc control plane. Một request đi qua gateway hợp lệ nhưng hai
agent mỗi bên retry/scan toàn bộ lịch sử khiến CPU/RAM hết; HTTP disconnect
không hủy job. Rate limit ngoài gateway không giới hạn fan-out bên trong.

**Đề xuất trong Q39/Q42/Q47.** Không thêm multi-role RBAC. Một owner vẫn dùng
service identity với DB views/roles và network egress tối thiểu; agent chỉ
gọi tool allowlist với schema/filter/window/row bound, không SQL/path/URL tự
do. Server xác minh auth audience/issuer và effective dataset scope tại từng
hop, credential riêng thay passthrough token mù. Pin tool schema/permission
version; log/redact secrets và trường nhạy cảm. Deadline, concurrency, rows/
bytes/query time, token/context và tổng tool-call budget truyền xuống mọi
hop; retry dùng ngân sách còn lại, cancellation được propagate. Tổng chi phí
request cần đo bằng actual fan-out, không chỉ đếm số HTTP request gateway.

MCP hướng dẫn tránh token passthrough vì downstream có thể nhầm cơ sở đã
xác thực; việc dùng MCP không tự thực thi policy workload FSDS.
[MCP security best practices](https://modelcontextprotocol.io/docs/2026-07-28/tutorials/security/security_best_practices).

**Pass/G3/G4, P3/P4; trước endpoint đầu tiên.** Injection xin tool/URL ngoài
allowlist, direct backend bypass, token sai audience, query rộng, retry lồng,
cancel/disconnect và concurrent owner sessions. Không trả secret/đọc ngoài
scope; công việc thực dừng/được bound, trace đủ mà không lộ credential.
Mở rộng S15/S16 và benchmark CPU end-to-end PF06.

## SR08 — Metadata file count không phải oracle số dòng nghiệp vụ

**Counterexample có trong code.** [Validator DP3](../spark_jobs/gold/validate_order_snapshot_return_features.py)
lấy `SUM(record_count)` từ `source.files` rồi so với số feature rows. Với một
data file có 100 records và một position-delete file ghi xóa một record,
logical source có 99 rows nhưng tổng record metadata có thể là 101. Chỉ lọc
`content=0` cũng vẫn có 100 physical data records nếu delete chưa rewrite.
Với current job chỉ replace toàn bảng, điều kiện này có thể chưa xuất hiện;
nó trở thành lỗi khi thêm correction/delete bằng cơ chế giữ delete files.
Không khẳng định mọi MERGE đều tạo delete files: tùy format/write mode.

Iceberg mô tả `.files` gồm data, position-delete và equality-delete file;
`record_count` là số record trong file, không tự là số live rows sau áp dụng
delete. [Iceberg files metadata 1.7.2](https://iceberg.apache.org/docs/1.7.2/spark-queries/#files),
[Iceberg spec, manifests](https://iceberg.apache.org/spec/#manifests).

**Hệ quả.** Có thể fail validation đúng dữ liệu sau correction, hoặc làm
expected coverage sai. Ngoài ra kiểm danh sách cột cấm và
`event_timestamp <= created` chỉ kiểm hình thức: feature giá đã bị correction
ngày5 vẫn có thể được gắn `event_timestamp=ngày1`, `created=ngày6` và vượt qua
check cho cutoff ngày3. Validator chưa chứng minh PIT bằng giá trị.

**Đề xuất trong G1/G2.** Đếm logical rows ở exact pinned snapshot và key set
hợp lệ theo eligibility; anti-join missing/extra keys, không chỉ total count.
Metadata metrics giữ cho physical volume/diagnostics, không làm oracle live
rows khi chưa chứng minh điều kiện áp dụng. Fixture oracle từ contract và
trace tính độc lập: PIT ngày3 phải lấy giá cũ, ngày6 lấy giá corrected. Không
dùng lại phép biến đổi build làm expected; cùng một bug sẽ pass hai bên.

**Pass/G1/G2, P1/P2.** Data file + position/equality delete nếu backend đã
chọn hỗ trợ, copy-on-write case đối chứng, correction giữ cùng timestamp
event nhưng observed muộn, missing key bù bằng extra key cùng total count.
Validator chấp nhận đúng logical result và bắt sai key/value/PIT. Mở rộng
S03/S05/S09/S14; chưa cần đổi code runtime trước chốt input/schema contract.

## Thứ tự đóng các vấn đề

1. G0/G1: build input manifest + source incarnation/ranges + output dependency
   matrix trước physical schema và full-scale. Đây là phần dễ buộc replay
   hoặc đổi identity nếu thêm muộn.
2. G2: chạy SR01–SR03 cùng POC publication/dual-sink/restore trên bộ nhỏ;
   bổ sung retention leases và writer fencing vào protocol đã chọn.
3. G3: bundle compatibility, end-to-end request context/cache và budgets
   trước UI/agent/model routing; kiểm với counterexample thay vì chỉ smoke test.
4. G4: restore độc lập, endpoint protection và integrated load/cancel drill.
   Các giá trị RPO/RTO, thời hạn pin, size/time/token limits và backend cụ thể
   vẫn phải pin trước nghiệm thu. Note này chưa chọn chúng hoặc có proof đạt.
