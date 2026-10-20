# Lakehouse dùng Apache Iceberg với REST catalog, Gold được nhân bản sang PostgreSQL

Ba tầng Bronze, Silver, Gold là bảng Iceberg nằm trên MinIO trong bucket `fsds`, quản
bởi một REST catalog chạy cùng Compose. Gold được copy thêm sang PostgreSQL và đánh
index để phục vụ point lookup và ảnh DBeaver. Spark (batch) và Flink (stream) cùng
ghi vào lakehouse qua chung catalog đó.

Ba quyết định này gắn chặt nhau nên ghi trong một ADR: chọn Iceberg mở ra lựa chọn
catalog, và chọn lakehouse buộc phải trả lời câu hỏi point lookup.

## Considered Options

### Table format: Iceberg so với Delta Lake

- **Delta Lake**: bị loại. Delta dễ dựng hơn — không bắt khai báo catalog, `pip
  install delta-spark` tự lo jar — nhưng nhận cột **theo tên**, nên đổi tên cột (đúng
  bài toán Amazon `AmazonOrderId` → `orderId`, 3 điểm rubric) phải bật thêm
  `delta.columnMapping.mode`. Delta cũng **không có partition evolution**, và
  connector Flink của nó ít được chăm sóc hơn hẳn — trong khi Flink ghi vào lakehouse
  là quyết định đã chốt.
- **Parquet trần, không table format**: bị loại. Không có ACID nên job ghi dở rồi
  dừng sẽ để lại dữ liệu rác mà không ai biết; không có `MERGE INTO` nên dedup giữ
  bản mới nhất và SCD 2 phải ghi đè cả partition; và rubric cho 2 điểm "Lakehouse
  optimize" mà không có compaction hay z-order thì không lấy được.

Iceberg đánh số cột nên đổi tên an toàn sẵn, có partition evolution để sửa sai mà
không ghi lại dữ liệu, có `rewrite_data_files(strategy => 'sort', sort_order =>
'zorder(...)')` cho cả compaction lẫn z-order, và đang thành chuẩn chung của ngành
(AWS S3 Tables, GCP Lakehouse runtime catalog, Snowflake, BigQuery).

### Catalog: REST so với JDBC so với Hadoop

Catalog giữ **một cái kim chỉ**: tên bảng → file metadata hiện hành. Ghi vào bảng là
đổi kim, và việc đổi kim phải là **so-rồi-mới-đổi** (compare-and-swap).

- **Hadoop catalog**: bị loại. Nó dùng tên file làm kim, mà object storage không cho
  compare-and-swap. Spark và Flink cùng ghi thì cả hai cùng thấy `v3`, cùng ghi `v4`,
  một bên ghi đè bên kia — **mất dữ liệu trong im lặng, không báo lỗi**. Đây là loại
  lỗi tệ nhất vì chỉ phát hiện khi đối chiếu số với manifest.
- **JDBC catalog trên PostgreSQL đã có**: an toàn (PostgreSQL cho `UPDATE ... WHERE
  metadata_location = <bản cũ>` đúng nghĩa compare-and-swap) và không cần service
  mới. Bị loại vì lý do khác: catalog có quản lý của GCP nói **chuẩn Iceberg REST**,
  nên port lên cloud từ JDBC phải đăng ký lại từng bảng, còn từ REST chỉ đổi URL.
  Final-coursework có chấm IaC và deploy lên cloud.

REST catalog an toàn như JDBC và port được như GCP muốn. Giá phải trả là một container
nữa — chọn bản viết bằng Rust nên khoảng 80 MB, không phải JVM.

### Gold: nhân bản sang PostgreSQL so với chỉ nằm trên lakehouse

- **Chỉ lakehouse**: bị loại. Một point lookup trên Iceberg/MinIO cần bảy lượt đi-về
  mạng (catalog → metadata → manifest list → manifest → chọn file → footer Parquet →
  row group), mất 1–3 giây, so với ~1 ms của B-tree index. Thống kê min/max của
  Parquet **không cứu được**: với `order_line_id` rải ngẫu nhiên, khoảng min–max của
  mọi row group đều phủ gần hết miền giá trị nên không bỏ qua được block nào. Rubric
  cũng tách riêng 2 điểm "Datawarehouse (indexing)" mà không đánh index được lên
  Parquet.
- **Chỉ PostgreSQL**: bị loại. Mất 2 điểm "Lakehouse optimize" (compaction, z-order),
  và DP2 "bronze → silver and gold" sẽ chạy nửa trong lakehouse nửa ngoài.

Hai nơi trả lời hai loại câu hỏi khác nhau: lakehouse giỏi quét nhiều trả ít, database
giỏi tìm một trả một. Việc này có tên nghề là **serving layer**, và là bản làm tay của
thứ Feast sẽ làm ở final-coursework (offline store là lakehouse, online store là
database nhanh).

## Consequences

- Spark và Flink cùng ghi một lakehouse nên **tranh commit là chuyện bình thường**,
  không phải lỗi. REST catalog xử lý: kẻ thua đọc lại bản mới và thử lại. Job phải
  chịu được việc bị thử lại.
- Ghép chặt duy nhất là **Iceberg ↔ Spark** và **Iceberg ↔ Flink**: Iceberg phát hành
  runtime jar riêng cho từng phiên bản engine, và Spark 3.5 dùng Scala 2.12 còn
  Spark 4.x dùng Scala 2.13 nên tên jar khác nhau. Trino, DataHub và catalog **không**
  phụ thuộc phiên bản Spark. Chọn Spark 3.5 và Flink 1.20 vì nhiều người đã đi qua,
  không phải vì mới nhất.
- Gold tồn tại hai bản nên phải có **một chiều đồng bộ duy nhất**: lakehouse là nguồn
  chân lý, PostgreSQL là bản sao. Không bao giờ sửa trực tiếp PostgreSQL.
- Branch của Iceberg (`write.wap.enabled`, `spark.wap.branch`, `fast_forward`) cho
  Write-Audit-Publish gần như miễn phí vì branch chỉ là một cái kim có tên, file
  Parquet dùng chung. Đây là nền cho stage "Validate" của DP1, DP2, DP3.
- Thêm một container catalog vào ngân sách RAM vốn đã chật (7,1 GB rảnh). Chấp nhận
  được vì catalog viết bằng Rust, nhưng cộng dồn với Trino và DataHub thì **bắt buộc
  phải chia profile**, không bật tất cả cùng lúc.
