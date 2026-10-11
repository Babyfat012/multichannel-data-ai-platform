# Tối ưu Spark — giai đoạn Bronze

Tài liệu này ghi lại **cách tối ưu job Spark đầu tiên của lakehouse**: từ baseline cố tình
xấu, qua một giả định bị số đo bác bỏ, tới nguyên nhân thật và kết quả ở quy mô đầy đủ.
Cập nhật **2026-10-11**.

Mọi số ở đây lấy từ `HANDOFF.bronze.md` mục 4 và `docs/evidence/spark-ui/README.md` —
không đo lại, không suy đoán. Chỗ nào chưa có số thì ghi rõ "chưa đo".

---

## 1. Mục tiêu và phạm vi

**Bronze là gì** (theo `CONTEXT.md`): bản sao trung thực của dữ liệu Channel giao cho
Seller. Nó chỉ đổi file sang định dạng cột và thêm cột kỹ thuật (Export Date, tên file
nguồn, thời điểm nạp). Nó **không** sửa tên cột, **không** gộp schema, **không** bỏ bản
trùng. Mỗi dạng schema của một Channel là một bảng riêng; bảng tầng này mang tiền tố `raw_`.

**Job này làm gì:** `spark_jobs/01_bronze_baseline.py` đọc các file JSONL export của
Amazon nằm ở `/data/raw` (mount read-only vào container Spark), rồi ghi thành bảng Iceberg
`lakehouse.bronze.raw_amazon_orders` qua REST catalog Lakekeeper, dữ liệu bảng nằm trên MinIO.
Job **không parse JSON và không làm sạch gì** — đúng vai trò "chép nguyên bản" của Bronze.

**Phạm vi tài liệu:** baseline, chuỗi tối ưu hiệu năng, và bằng chứng Spark UI. Phần Silver,
Gold và tích hợp Airflow thuộc giai đoạn sau (mục 9 để trống, điền ở GĐ 4).

---

## 2. Hình dạng bảng và vì sao

Bốn quyết định thiết kế đã chốt (`HANDOFF.bronze.md` mục 1). Điểm quan trọng không phải
chọn gì, mà **vì sao**:

| # | Quyết định | Lý do |
|---|---|---|
| Q1 | Bronze **giữ nguyên chuỗi JSON** ở cột `payload`; Silver mới parse | Tên khoá Amazon đổi giữa kỳ (`AmazonOrderId` tháng 1 → `orderId` tháng 5). Nếu parse ở Bronze thì phải giải schema evolution ngay tại Bronze — trái vai trò "chép nguyên bản" |
| Q2 | **Hai bảng riêng** `raw_amazon_orders` + `raw_walmart_orders` | Commit Iceberg là atomic: tách bảng thì một nguồn hỏng vẫn chạy lại riêng được nguồn kia |
| Q3 | Provenance **phương án A**: mỗi bảng một tên cột nguồn riêng | Amazon dùng `source_file` (đọc từ đĩa nên có tên file); Walmart dùng `source_table` (đọc qua JDBC, `input_file_name()` trả chuỗi rỗng). Cả hai có `ingested_at`, `run_id`; Walmart giữ thêm `fetched_at` **của nguồn**, không gộp với `ingested_at` |
| Q4 | `raw_amazon_orders` PARTITIONED BY (`export_date`); `raw_walmart_orders` PARTITIONED BY (`days(fetched_at)`) | Partition khớp đúng cách file nằm trên đĩa. `days()` an toàn kể cả khi nguồn đổi nhịp kéo |

**Hệ quả đã chấp nhận: bảng Amazon KHÔNG có cột mã đơn hàng.** Cột của nó chỉ là
`payload, source_file, export_date, ingested_at, run_id`. Đây là cái giá của Q1: muốn tra
một đơn thì tra ở Silver. Bronze tồn tại để **chạy lại khi Silver lỗi**, không phải để tra cứu.

**Hệ quả nữa:** mục rubric *schema evolution* nằm ở `02_silver_*.py`, **KHÔNG** ở Bronze.
Tài liệu phải trỏ đúng chỗ đó.

---

## 3. Dữ liệu nguồn

Đường dẫn nguồn:
`/data/raw/source-exports/channel=amazon/dataset=orders/export_date=YYYY-MM-DD/part-*.jsonl`

| Số đo | Giá trị |
|---|---|
| Số ngày | 180 ngày (2026-01-01 → 2026-06-29) |
| Số file | 3.316 file |
| Số dòng | 108.248.563 dòng |
| Dung lượng | 80,77 GiB |

**Chỉ có HAI nguồn batch.** TikTok **không** có trong batch — nó chỉ đi Kafka/stream. Walmart
nằm ở `/data/raw/pg-load/walmart.orders/export_date=*/` (JSONL chờ nạp vào PostgreSQL), ba
trường `purchase_order_id`, `raw_payload`, `fetched_at`; `fetched_at` = `...T23:59:59Z` cho cả
ngày nên có 180 giá trị phân biệt.

Vì sao nguồn của Walmart là PostgreSQL (không phải file JSONL), và vì sao cột `raw_payload` khai `JSONB` nên thứ tự key bị xếp lại mà vẫn không phải lỗi: xem `docs/channel-sources.md`, mục **Ranh giới tin cậy: nguồn của Walmart là PostgreSQL, không phải file JSONL**.

**Skew là có thật, không phải giả định.** Dung lượng mỗi ngày Amazon:
- nhỏ nhất 2026-01-01 = **7,3 MB**
- giữa 2026-03-22 = **495 MB**
- lớn nhất 2026-06-29 = **1,64 GB**

Ngày lớn nhất lệch **224 lần** ngày nhỏ nhất. Đây chính là dòng rubric *skew*.

**Small-file là vấn đề phía ĐỌC.** 1.064 / 3.316 file nhỏ hơn 1 MB. Vì vậy đuôi file tí hon
là chi phí lúc **đọc**, không phải lúc ghi — điều này quyết định lựa chọn ở mục 5e.

---

## 4. Baseline và vì sao tắt AQE

Baseline **cố tình tắt tối ưu**:
- `spark.sql.adaptive.enabled=false` (AQE tắt)
- `spark.sql.shuffle.partitions=200`

**Vì sao tắt AQE:** rubric cho điểm khi **chứng minh tìm ra và sửa được vấn đề hiệu năng**.
Nếu bật AQE ngay từ đầu thì Spark tự sửa, không còn ảnh "trước" để so. AQE phải là **một
phương án được đo**, không phải thứ bị giấu.

**Nguyên tắc:** baseline phải **thật sự chậm, nhưng không được giả tạo chậm**. Tắt AQE là
một cấu hình hợp lệ (nhiều cluster thật vẫn chạy vậy), không phải cài thêm độ trễ nhân tạo.
Nhờ vậy khi bật lại AQE mà **không nhanh hơn**, ta mới dám tin con số.

---

## 5. Chuỗi tối ưu — đúng theo thứ tự thật đã xảy ra

Phần này giữ nguyên thứ tự thật, kể cả bước thất bại. Bước (b) là phần giá trị nhất: một
giả định bị số đo bác bỏ, và ta không giấu nó như một thất bại.

### a. Giả định ban đầu
"Skew do thiếu tối ưu → **bật AQE sẽ chữa skew**." Đây là kỳ vọng thông thường vì AQE được
quảng cáo là chữa skew.

### b. Đo — GIẢ ĐỊNH BỊ BÁC BỎ
Chạy **cùng 20 ngày rải đều** (có cả ngày bé lẫn ngày to), chỉ đổi cờ AQE:

| Lần | AQE | Thời gian | Stage ghi | Số task | Task lâu nhất |
|---|---|---|---|---|---|
| 1 | off | **72,3s** | stage 1 | 200 (chỉ ~20 có việc, 180 rỗng 0,01s) | **18,7s** |
| 2 | on | **81,7s** | stage 2 | **8** (AQE gộp lại) | **23,6s** |

Kết quả **ngược kỳ vọng**: bật AQE **chậm hơn** (72,3s → 81,7s) và task chậm nhất **tệ hơn**
(18,7s → 23,6s). Giả định ban đầu bị số đo bác bỏ.

### c. Vì sao AQE không chữa được skew của job này
AQE giỏi hai việc: (a) gộp nhiều task bé, và (b) **cắt skew trong JOIN**. Job này **không có
JOIN**. Chỗ nghẽn là một task phải ghi trọn một partition 1,64 GB — AQE **không chia việc ghi
một partition cho nhiều task**. Tệ hơn, AQE còn gộp 2-3 ngày vào một task nên task chậm nhất
nặng thêm (đúng như cột "số task" cho thấy: 200 → 8).

### d. Nguyên nhân thật — `write.distribution-mode`
Đòn thật không nằm ở AQE mà ở cách Iceberg phân phối việc ghi:

| Giá trị | Cách làm | Đánh đổi |
|---|---|---|
| `hash` (mặc định Iceberg 1.5 cho bảng có partition — **đang dùng**) | dồn mọi dòng cùng `export_date` về **một** task | 1 file/partition, gọn; nhưng ngày to = 1 task ôm hết → nghẽn |
| `none` | không shuffle, 114 task đọc gì ghi nấy | ghi song song, hết nghẽn; nhưng **nhiều file nhỏ** hơn — liên quan trực tiếp tới kết luận "small-file là vấn đề phía đọc" |

### e. Quyết định: GIỮ `hash` (chốt 2026-10-11)
Đây là câu hỏi treo ở `HANDOFF.bronze.md` mục 5, đã chốt: **giữ `hash`, không chạy lại**.

Lý do: ở quy mô 180 ngày, skew **vẫn còn** (ảnh `06`: median 33s, max 1,1 phút ≈ gấp 2 lần
median) **nhưng không chặn được việc** — job vẫn xong và ghi đủ dòng. Đổi sang `none` sẽ
đổi một cái nghẽn có kiểm soát lấy **nhiều file nhỏ hơn**, mà small-file vốn đã là vấn đề
phía đọc (1.064/3.316 file < 1 MB, mục 3). Giữ `hash` giữ đúng 1 file/partition, đổi lại
chấp nhận ngày to ghi trong một task. Đây là đánh đổi có lý do, không phải bỏ qua skew.

---

## 6. Kết quả ở quy mô đầy đủ — lần 3

`--aqe on --table raw_amazon_orders`, cả 180 ngày,
`run_id=e02f0e9f-3635-499e-a47d-aabc7ff55df9` (2026-10-11 08:02 → 08:21).

| Số đo | Giá trị |
|---|---|
| Thời gian | **1.150,7s** (19 phút 11 giây) |
| Số dòng ghi | **108.248.563** — khớp đúng tổng nguồn, không mất dòng |
| Task stage ghi | **1.002** (trước khi AQE gộp; AQE gộp còn **69 task**, xem ảnh 06) |
| Dung lượng bảng trên MinIO | `/data/fsds/lakehouse` = **10 GB** (ước ban đầu 17 GB) |
| Nén | **zstd ~10 lần** (từ ~80,77 GiB JSONL xuống bảng 10 GB) |
| Đĩa `/` sau khi chạy | còn **25 GB** (trước 33 GB → bảng ăn ~8 GB) |
| Event log | `out/spark-events/app-20261011080203-0001` (5,7 MB, đã hoàn tất) |

Số mới lấy từ ảnh lần 3: stage ghi = **stage 3**, output **7,9 GiB / 108.248.563 dòng**,
Duration min 11s / median 33s / **max 1,1 phút**. Stage đọc (stage 1) median **2s** — đọc
không phải chỗ nghẽn, đúng như ở quy mô 20 ngày.

---

## 7. Ảnh Spark UI

Chín ảnh dưới đây chụp từ **Spark History Server** (không phải UI cổng 4040 — UI đó chết
cùng job nên không canh được job chạy 19 phút). Nguồn: ba event log trong `out/spark-events/`.
Mỗi ảnh kèm lời giải thích **chỉ vào con số cần đọc**.

### 7.1. Danh sách ba lần chạy

![Trang chủ History Server, thấy cả 5 app](evidence/spark-ui/00-history-danh-sach-3-lan-chay.png)

Ảnh này để **định danh ba app bằng chứng** trong danh sách. Hai app còn lại trong danh sách
không phải bằng chứng (một lần chết giữa đường, một lần gãy vì thiếu MinIO) — xem mục 8.

### 7.2. Lần 1 — AQE off (20 ngày)

![Danh sách stage lần 1](evidence/spark-ui/01-lan1-aqe-off-stages.png)

Danh sách stage của lần 1: **stage 1 là stage ghi**. Đây là stage cần soi để thấy skew.

![Stage ghi lần 1 — ảnh skew rõ nhất](evidence/spark-ui/02-lan1-aqe-off-stage-ghi.png)

**Ảnh skew rõ nhất.** Stage 1 có **200 task**, Duration min **1,0ms** / median **3,0ms** /
max **19s**. 180/200 task rỗng còn vài task ôm hết việc — đúng hình dạng skew.

### 7.3. Lần 2 — AQE on (20 ngày)

![Danh sách stage lần 2](evidence/spark-ui/03-lan2-aqe-on-stages.png)

Danh sách stage lần 2: **stage 2 là stage ghi** (AQE chèn thêm stage so với lần 1).

![Stage ghi lần 2 — AQE gộp task](evidence/spark-ui/04-lan2-aqe-on-stage-ghi.png)

**Ảnh AQE gộp task.** Stage 2 còn **8 task**, Duration min **6s** / median **21s** / max
**23s**. Task đều hơn, nhưng task chậm nhất **tệ hơn** lần 1 (19s → 23s) — bằng chứng AQE
không chữa được skew của job này.

### 7.4. Lần 3 — 180 ngày (AQE on)

![Danh sách job lần 3](evidence/spark-ui/05-lan3-180ngay-jobs.png)

Danh sách job lần 3: dùng để đọc **tổng thời gian job** ở quy mô đầy đủ.

![Stage ghi lần 3 — 69 task](evidence/spark-ui/06-lan3-180ngay-stage-ghi.png)

Stage 3 = stage **ghi** của lần 3, **69 task** (AQE gộp từ 1.002). Output **7,9 GiB /
108.248.563 dòng**. Duration min **11s** / median **33s** / max **1,1 phút** — skew còn
nhưng không chặn được việc.

![Stage đọc lần 3](evidence/spark-ui/07-lan3-180ngay-stage-doc.png)

Stage 1 = stage **đọc** của lần 3, 1.002 task, Input **80,8 GiB / 108.248.563 dòng**.
Duration median **2s**, max **11s** → **đọc KHÔNG phải chỗ nghẽn**.

![Toàn bộ stage lần 3](evidence/spark-ui/08-lan3-180ngay-stages.png)

Toàn bộ stage lần 3: thấy **stage 2 bị SKIPPED** vì AQE thay bằng stage 3 — đúng cơ chế AQE
thay thế kế hoạch cũ.

---

## 8. Cách chạy lại

**BẮT BUỘC bật MinIO trước.** `spark-master` không `depends_on` MinIO, nên bật mỗi 3 service
Spark thì MinIO vẫn tắt, job đọc file và tính xong rồi **gãy đúng lúc ghi** với
`BadRequestException: Malformed request: IO error at s3://fsds/lakehouse/...`. Gặp lỗi này
thì `docker ps` xem MinIO có chạy không, **đừng** đi sửa Lakekeeper hay quyền S3.

```bash
cd /home/phattl/eonsr/FSDS
docker compose up -d minio minio-init                                        # BẮT BUỘC
docker compose --profile spark up -d spark-master spark-worker-1 spark-worker-2
bash scripts/setup_lakekeeper.sh                                             # chạy lại được, đã có thì bỏ qua
```

**Chạy job** (script chạy cả ba lần hoặc từng lần):
```bash
bash scripts/run_bronze_evidence.sh lan3   # 180 ngày, AQE on  → bảng thật
bash scripts/run_bronze_evidence.sh lan1   # 20 ngày, AQE off  → ảnh skew
bash scripts/run_bronze_evidence.sh lan2   # 20 ngày, AQE on   → ảnh so sánh
```

> **ĐỪNG chạy `docker compose --profile spark up -d` trần.** Nó khởi động luôn service
> default-profile `generator-batch` và bắt đầu sinh lại 100 GB vào MinIO trên `/`. Luôn nêu
> đúng tên 3 service Spark.

**Xem lại bằng History Server** (Spark UI cổng 4040 chết cùng job):
```bash
docker run -d --rm --name fsds-spark-history -p 18080:18080 --user root \
  -v "$PWD/out/spark-events:/spark-events:ro" \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///spark-events" \
  apache/spark:3.5.3-java17 /opt/spark/bin/spark-class \
  org.apache.spark.deploy.history.HistoryServer
# mở http://localhost:18080 ; xong thì: docker stop fsds-spark-history
```

Ba event log dùng được cho History Server là `app-20261010103931-0000` (lần 1),
`app-20261010104107-0001` (lần 2), `app-20261011080203-0001` (lần 3). Hai file
`app-20261010104633-0002.inprogress` (chết giữa đường) và `app-20261011080057-0000` (gãy vì
thiếu MinIO) **không phải bằng chứng** và đã được xoá.

---

## 9. Tích hợp vào Airflow pipeline

**Điền ở GĐ 4.**
