# Thiết kế tầng Silver — làm sạch và quy về ngôn ngữ chung

Tài liệu này ghi lại **thiết kế đã chốt** của tầng Silver: hai bảng `stg_*` theo kênh, độ
hạt Order Line, khoá dedup, kiểu dữ liệu, và cách xử lý khác biệt giữa hai dạng schema của
Amazon. Cập nhật **2026-10-12**.

Mọi số ở đây lấy từ một dòng code (ghi `file.py:dòng`) hoặc ghi rõ **"đã đo"** / **"chưa đo"**.
Chỗ nào chưa đo thì không suy đoán.

---

## 1. Phạm vi tài liệu

- Thiết kế đã chốt, và **code đã có**: `spark_jobs/02_silver_amazon.py`,
  `spark_jobs/02_silver_walmart.py`. Bảng ánh xạ tách ra `spark_jobs/silver_mapping.py`
  (không import pyspark nên test được trên máy host). Mục 10 ghi số đo lần chạy thật.
- Tài liệu tả *quyết định, lý do, và giá* — không phải hướng dẫn chạy.
- Mục 8 ghi **ba khoản cuối đã chốt** (2026-10-12). Mục 9 nêu **một** hệ quả còn mở, thuộc Gold.
- Mục 10 ghi **số đo lần chạy thật** và **hai khoản mới mở ra khi chạy**.
- Đọc kèm: `CONTEXT.md` (định nghĩa Bronze/Silver/Gold), `docs/spark.md` (Bronze đã xong).

---

## 2. Hai bảng riêng theo kênh, KHÔNG gộp

**Chốt:** hai bảng Silver riêng — `stg_amazon_order_lines`, `stg_walmart_order_lines`.
(Tên bảng là **đề xuất** của implementer 1, chưa duyệt.)

| | Amazon | Walmart |
|---|---|---|
| Hình khoá dedup | 2 cột | 3 cột |

**Lý do (người dùng nêu):** hai kênh có hình khoá dedup khác nhau. Gộp thì một job dedup phải
rẽ nhánh theo `channel` — khó ở code, và thêm kênh mới là phải sửa lại chính job đó.

**Cột `channel` giữ trong CẢ HAI bảng**, để bước gộp sau không phải đoán dòng từ đâu.

**Chỗ gộp hai kênh là Gold** (`fact_`/`obt_`), không phải Silver. Căn cứ: `CONTEXT.md:102`
định nghĩa Silver là "gộp các dạng schema của **cùng một** Channel" — gộp nhiều kênh không
thuộc Silver.

**Giá:** tách không làm mất việc gộp, chỉ đẩy nó xuống Gold, nơi hai kênh đã cùng snake_case
và đã sạch.

**Đồng bộ định nghĩa Bronze:** định nghĩa Bronze ở `docs/spark.md:17` và `CONTEXT.md:98` đã được
sửa cho khớp thiết kế thật — Bronze có **một bảng mỗi KÊNH**, hai dạng schema của Amazon nằm
chung vì Bronze không parse JSON.

---

## 3. Độ hạt và khoá

**Độ hạt = Order Line.** Một dòng Bronze là một Order chứa mảng Order Line, nên Silver phải
nổ mảng ra. Lý do người dùng nêu: "grain càng nhỏ thì càng sát với predict" — nhãn Return
Label chấm ở mức Order Line.

**Tên cột:** snake_case cho **mọi** cột, đồng bộ với cách đặt tên của Bronze (các cột Bronze
đều snake_case: `payload`, `source_file`, `export_date`, `ingested_at`, `run_id`,
`raw_payload`, `fetched_at`, `source_table`).

### 3.1 Khoá dedup

| Kênh | Khoá | Tie-break |
|---|---|---|
| Amazon | `(order_id, order_item_id)` | giữ `last_updated_time` lớn nhất |
| Walmart | `(purchase_order_id, customer_order_id, line_number)` | `status_changed_at` mới nhất |

**Chốt (khi tie-break hoà — provenance):** giữ bản có `export_date` **lớn nhất** (Walmart:
`fetched_at` lớn nhất).

**Lý do:** người dùng đã chọn "giữ bản mới nhất" ở tie-break chính (`last_updated_time` lớn
nhất). Dùng cùng một hướng thì toàn bộ quy tắc dedup chỉ cần **một** câu để giải thích: luôn lấy
cái mới nhất.

**Giá:** mất thông tin "lần đầu thấy đơn này là ngày nào" — Bronze giữ đủ, tra lại được.

**Chốt (tên khoá Walmart):** Silver **giữ tên nguồn** (`purchase_order_id`, `line_number`),
**không** thêm cột `order_id` cho bảng Walmart. Nơi **đổi tên là Gold**, không phải Silver.

**Lý do:** `purchase_order_id` **không cùng độ chắc** với `order_id` của Amazon. Mã Walmart là
`"1"` + 12 chữ số băm (không gian 10^12), và **đã đo được** một va chạm thật: giá trị
`1678172477013` xuất hiện ở `export_date` 2026-01-20 và 2026-02-17 là **hai đơn khác nhau**
(Houston vs Orlando, `customerOrderId` 9698229271973 vs 9655369243011). Đặt cả hai cột cùng tên
`order_id` thì khác biệt đó **biến mất** khỏi bảng — người đọc tưởng hai cột tin cậy như nhau.
Giữ tên nguồn là giữ lời cảnh báo.

**Giá:** Gold phải mang thêm một bước đổi tên; bù lại không có hai cột khác bản chất đội chung
một tên.

**Vì sao `order_id` một mình là SAI:** ở mức Order Line, một `order_id` nằm trên nhiều dòng
thật, nên dedup chỉ theo `order_id` sẽ **xoá Order Line thật**. Đây là lỗi **im lặng** — job
không báo gì, số dòng vẫn trông hợp lý.

### 3.2 Thứ tự bước: parse TRƯỚC, dedup SAU

**Đã đo:** 23.487 khoá có một dòng dạng v0 và một dòng dạng 2026 (cửa sổ `export_date`
2026-04-07..04-12). Chưa quy tên khoá về một dạng thì dedup không nhận ra đó là cùng một đơn.

**Dedup không làm được trong từng partition:** bản trùng luôn nằm ở ngày export khác
(`overlap_days = 3`), nên dedup trong một partition bắt được 0 bản trùng. Phải nhìn xuyên
≥ ±3 `export_date`.

---

## 4. Kiểu dữ liệu

### 4.1 Tiền = `decimal(14,2)`, cả hai kênh

**Biên số thật:**
- giá gốc max 300 USD (`generator/core/storefronts.py:99`, danh mục electronics `(20, 300)`)
- số lượng max 4 (`generator/core/simulate.py:302`)
- hệ số giá Amazon/Walmart 1,00 / 0,80 / 0,95

=> một dòng ≤ ~1.200. `decimal(14,2)` thừa sức.

**Vì sao KHÔNG `double`:** sai do làm tròn, và sai **dồn** khi cộng 108 triệu dòng. Gold sẽ
cộng doanh thu theo ngày và theo SKU => hai thứ tự cộng khác nhau có thể ra hai số khác nhau.

**Vì sao KHÔNG `string`:** chỉ **hoãn** quyết định sang chỗ ép kiểu, không giải.

**Vì sao hai kênh PHẢI cùng kiểu:** Gold union hai bảng; union `decimal` với `double` thì Spark
nâng cả hai về `double`, làm hỏng cả phần Amazon vốn đang đúng.

**Giá:** VND không có phần thập phân. Nếu kênh Shopee sau này dùng cùng kiểu thì mọi dòng VND
là ",00" vô nghĩa. Đó là giá của việc dùng **một** kiểu cho mọi kênh. (Biên Shopee:
300 × hệ số 17.500 × 4 = 21.000.000 — `storefronts.py:55`.)

**Bẫy Iceberg:** nới `p` được (14 → 18), đổi `s` **không** được. Nên `s = 2` là chọn một lần.

**Chốt (tên ba cột tiền mức dòng):** `line_gross_amount` (trước giảm giá),
`line_discount_amount`, `line_net_amount` (sau giảm giá). **Đã duyệt.** Amazon có cả ba; Walmart
nguồn chỉ cho **net** nên bảng Walmart **chỉ** có `line_net_amount`, không tạo hai cột rỗng.
`order_total_amount` chỉ Amazon có.

**Lý do:** giá dòng **khác nghĩa** giữa hai kênh — Amazon `ItemPrice` là **trước** giảm giá
(`generator/channels/amazon.py:157`, giảm giá tách riêng ở `:158`); Walmart `chargeAmount` là
**sau** giảm giá (`generator/channels/walmart.py:103`). Không được đặt cùng một tên cho hai cái
này, nếu không Gold sẽ cộng hai thứ khác nhau vào một chỗ.

**Giá:** ba tên khác nhau buộc người viết query phải biết mình đang cộng cái nào; nhưng đây đúng
là cái mà phương án **tách bảng** mua được — không phải tạo cột rỗng cho kênh không có.

### 4.2 Thời gian = `timestamp` UTC, và GHIM múi giờ

Amazon lưu chuỗi ISO có `Z`; Walmart lưu epoch **mili-giây**. Silver quy cả hai về `timestamp`
UTC. Lý do người dùng nêu: giữ nguyên hai dạng thì khó đọc — nhìn `1678172477013` không biết
là ngày nào.

**Ghim `spark.sql.session.timeZone = UTC`** trong cấu hình SparkSession của job Silver
(**không** rải trong từng phép đổi).

**Giới hạn phải khai:** quy về `timestamp` **không** xoá rủi ro múi giờ, chỉ dồn chỗ đổi về
**một** nơi. Hàm ngày của Spark (`to_date`) đổi kết quả theo `spark.sql.session.timeZone`, và
đích Return Label có **cửa sổ 30 ngày** — ranh giới ngày quyết định một dòng có được gán nhãn
hay không. Không ghim thì cùng dữ liệu, máy khác múi giờ, ra nhãn khác.

**Đồng bộ với Bronze:** Bronze đã có cột thời gian kiểu thật — `ingested_at`
(`spark_jobs/01_bronze_baseline.py:194`), `fetched_at`, và `raw_walmart_orders` partition theo
`days(fetched_at)`.

### 4.3 `is_prime` kiểu `boolean`

v0 có `IsPrime` (bool); dạng 2026 có `programs` (mảng). Silver dùng **một** cột `is_prime`
kiểu `boolean`; dạng 2026 parse bằng cách kiểm mảng có chứa `PRIME`.

**Dữ kiện dứt điểm:** `generator/channels/amazon.py:189`
`"programs": ["PRIME"] if order.is_prime else []` — mảng chỉ có **hai** khả năng, đúng bằng một
bool, không hơn một bit thông tin.

**Lý do thêm:** đích là bảng feature; mô hình nhận `boolean` là xong, nhận `array<string>` thì
ở Gold vẫn phải bung thành cờ 0/1 => chỉ hoãn việc.

**Phản biện đã xét:** "sau này nguồn thêm chương trình khác" => lúc đó **thêm cột** (Iceberg cho
thêm cột, rẻ). Chọn mảng ngay bây giờ là trả giá chắc chắn hôm nay để phòng một chuyện chưa
chắc xảy ra.

### 4.4 `country`: ép Walmart `"USA"` về `"US"`

**Chốt:** cột `country` dùng một bộ giá trị chung; Walmart `"USA"` ép về `"US"`.

**Lý do:** `US` là mã 2 chữ ISO 3166-1 alpha-2, và Amazon đã dùng. Điều này **không** ngược với
mục 6 (trạng thái gốc giữ y nguyên chữ): trạng thái có **hai** cột (gốc + chuẩn) nên cột gốc còn
chỗ để trung thực; `country` chỉ có **một** cột, nên cột đó phải là cột chuẩn.

**Giá:** mất cách viết gốc `"USA"` trong Silver — Bronze vẫn giữ `"USA"` nguyên văn, tra lại được.

---

## 5. Năm khoá bị bỏ ở dạng 2026 của Amazon: giữ 3, bỏ 2

**Bỏ hẳn khỏi Silver:** `is_business_order`, `is_replacement_order`. Ở dạng v0 chúng là hằng
số `False` (`generator/channels/amazon.py:142-143`) => mọi dòng giống nhau, không có gì để mô
hình học (zero variance). Bronze vẫn giữ nguyên payload nên không mất vĩnh viễn.

**Giữ:** `number_of_items_shipped`, `number_of_items_unshipped` (mức Order), `quantity_shipped`
(mức Order Line). Ba cột này nói "đã gửi mấy món / còn mấy món chưa gửi"; đơn chưa gửi thì
chưa giao, chưa giao thì chưa trả lại được => liên quan trực tiếp đích Return Label.

**Phần dạng 2026 (payload không có khoá): để NULL.** Không tính bù từ trạng thái + số lượng.
Lý do người dùng nêu: "để data trung thực nhất, để giống mô hình training thực tế".

**Hai cái giá của việc tính bù (nếu làm):**
1. số suy ra chỉ đúng **tại ngày export đó**, mà sau dedup giữ bản mới nhất thì cột mang nghĩa
   "tại thời điểm bản ghi mới nhất", không phải tổng cuối cùng;
2. cột nhìn **giống** dữ liệu nguồn nhưng là dữ liệu tự tính, và nó suy **từ** trạng thái =>
   mô hình học trên đó là học lại chính trạng thái, vòng tròn.

**Tỉ lệ null: CHƯA ĐO, cần cụm Spark.** Cận dưới chắc chắn ≥ **6,95%** số dòng
(7.524.691 / 108.248.563). Con số 48% theo byte file là **chỉ báo**, **không** phải số đo.

---

## 6. Trạng thái: HAI cột

`CONTEXT.md:44` đã chốt sẵn: "trạng thái gốc của Channel được giữ riêng và ánh xạ sang trạng
thái chuẩn". Nên Silver có hai cột — đây **không** phải lựa chọn của dự án.

- **Trạng thái chuẩn** `order_status`, đúng danh sách `CONTEXT.md:44`: `pending_payment` →
  `awaiting_shipment` → `shipped` → `delivered` → `completed`, cùng hai nhánh `cancelled` và
  `returned`.
- **Cột trạng thái gốc** `channel_status`: giữ y nguyên chữ của nguồn. `Canceled` (v0) và
  `CANCELLED` (2026) để nguyên, không chuẩn hoá.

**Lý do:** cột gốc tồn tại **chỉ** để trung thực. Sửa chữ trong đó là lấy đi đúng lý do nó có
mặt.

**Giá phải khai:** cột gốc có **5 giá trị cho 4 nghĩa**. Ai lọc trên cột gốc mà chỉ lấy một
cách viết thì **đếm thiếu**, không báo lỗi. => **Cột gốc không dùng để lọc; dùng `order_status`.**

**Một hệ quả của nguồn, ghi cho đủ:** Amazon **không bao giờ** ra `delivered`/`completed` (đã
giao vẫn là `Shipped`). Walmart có `Delivered` thật kèm `statusDate`, **nhưng** `COMPLETED` cũng
được ghi là `Delivered` (`generator/channels/walmart.py:38-39`) — nên với đơn completed,
`statusDate` là lúc **hoàn tất**, không phải lúc giao. Vì vậy cột ở Silver mang tên
`status_changed_at`, **không** phải `delivered_at` — xem mục 8.2.

---

## 7. Provenance mang xuống Silver

**Chốt:** Amazon mang `export_date` + `source_file`; Walmart mang `source_table` + `fetched_at`.
**Không mang** `ingested_at`, `run_id` của Bronze — Silver ghi cột của riêng nó.

**Lý do:**
- `export_date` buộc phải có trong Silver — nó chính là cột tie-break đã chốt ở mục 3.1 (khi
  `last_updated_time` hoà thì giữ `export_date` lớn nhất). Không mang xuống thì không thực hiện
  được tie-break đó.
- `source_file` cho phép lần một dòng Silver về đúng file Bronze đã sinh ra nó.
- `ingested_at` và `run_id` nói về "lúc **Bronze** ghi" và "lần chạy **Bronze** nào". Mang xuống
  Silver thì hai cột đó tả một tầng khác, đọc lên dễ tưởng là lúc Silver ghi. Silver ghi cột của
  riêng nó.

**Giá:** từ Silver một mình không biết được lần chạy Bronze nào sinh ra dòng đó. Nhưng
`source_file` + `export_date` đã khoanh đủ hẹp, và Bronze vẫn giữ `run_id`.

---

## 8. Ba khoản cuối: ĐÃ QUYẾT (2026-10-12)

Ba khoản này từng nằm ở mục "chưa quyết". Đã chốt, kèm lý do và giá.

### 8.1 Bốn cột chỉ Walmart có: giữ 2, bỏ 2

**Chốt (người dùng chọn):** giữ `buyer_id`, `is_guest`. Bỏ `customer_email_id`, `order_type`.

**Lý do bỏ:**
- `order_type` **luôn là `"REGULAR"`** — hardcode ở `generator/channels/walmart.py:119`, cả repo
  không có giá trị thứ hai. Một cột một giá trị không mang tin nào.
- `customer_email_id` là email đã băm (`walmart.py:114`), trùng vai *danh tính* với `buyer_id`
  mà thêm một cột dữ liệu cá nhân phải canh.

**Giá của việc bỏ: KHÔNG mất dữ liệu.** Bronze giữ `raw_payload` nguyên văn, hai khoá đó vẫn ở
đó. Muốn dùng lại thì sửa job Silver và chạy lại từ Bronze — mất **công**, không mất **dữ liệu**.

**Giá của việc GIỮ — phần dễ bỏ sót:**
- `is_guest` **không mang tín hiệu nào về Return Label trong dữ liệu này**: `generator/core/returns.py`
  không đọc `buyer` (grep: không một dòng). Đưa nó lên bảng feature thì model vẫn có thể bắt được
  tương quan ngẫu nhiên — một cột *trông như* có ích mà thật ra là nhiễu. Phải ghi rõ ở tầng
  feature, không để người sau tưởng nó là feature đã kiểm.
- `buyer_id` giữ làm **khoá nối**, **không** làm feature (đề xuất của planner, chưa có code để
  cưỡng chế). Nó là mã 12 chữ số băm (`native_code(..., 12)`, không gian 10^12) — độ hạt gần như
  một-dòng-một-giá-trị. Dùng trực tiếp làm feature là rò rỉ danh tính chứ không phải học quy luật.

### 8.2 Thời điểm giao của Walmart: cột tên `status_changed_at`, KHÔNG đặt `delivered_at`

**Chốt:** `stg_walmart_order_lines` có cột `status_changed_at` (timestamp UTC) lấy từ `statusDate`.
Silver **không** tạo cột `delivered_at`.

**Lý do:** `generator/channels/walmart.py:34-39` ánh xạ **cả `DELIVERED` lẫn `COMPLETED`** thành
cùng một chữ `"Delivered"`, và `statusDate` là thời điểm của **lần đổi trạng thái gần nhất**
(`walmart.py:77,107` — `epoch_millis(change.at)`). Nên với một đơn đã completed, `statusDate` là
lúc **hoàn tất**, không phải lúc **giao**, và từ payload **không phân biệt được hai trường hợp**.
`CONTEXT.md:65` định nghĩa **Delivered At** là "thời điểm Order Line thực sự được giao" và là
**mốc bắt đầu cửa sổ 30 ngày** của Return Label. Một cột mang tên đó mà chứa thời điểm hoàn tất
sẽ làm lệch nhãn một cách **im lặng** — không ai thấy lỗi, chỉ thấy nhãn sai. Nên cột ở Silver
mang tên đúng cái nó là.

**Giá:** `delivered_at` chưa có ở Silver; Gold phải tự dựng. Và chú ý một hệ quả đã biết của khoá
dedup đã chốt ở mục 3.1 (giữ bản `last_updated_time` mới nhất): nó sẽ giữ thời điểm **hoàn tất**
và bỏ thời điểm **giao**. Nên Gold không dựng được `delivered_at` từ Silver một mình — xem mục 9.

### 8.3 Tên và mức của Fulfillment Method

**Chốt:** tên cột `fulfillment_method`, đặt ở **mức Order**, lặp lại trên mọi Order Line của cùng
Order. Chỉ `stg_amazon_order_lines` có cột này; `stg_walmart_order_lines` **không có**.

**Lý do tên:** `CONTEXT.md:55-57` đã chọn **Fulfillment Method** làm từ vựng dự án, kèm
`_Avoid_: Shipping type, logistics mode`. Không dùng `fulfilled_by` vì đó là **tên khoá gốc** của
Amazon dạng 2026 (`generator/channels/amazon.py:178`) — lấy tên gốc đặt cho cột chuẩn sẽ lẫn hai
tầng, đúng cái lỗi mục 6 đã tránh bằng cách tách hai cột trạng thái.

**Lý do mức:** `CONTEXT.md:55` định nghĩa theo Order Line, nhưng dữ liệu **không có ở mức đó**:
`generator/core/model.py:102` đặt `fulfillment` trên class `Order` (`model.py:88`), còn class
`OrderLine` (`model.py:60`) không có field này. Amazon công bố ở mức Order ở cả hai dạng
(`amazon.py:135` `FulfillmentChannel`, `amazon.py:178` `fulfillment.fulfilledBy`). Walmart:
grep `fulfill` trong `generator/channels/walmart.py` ra **rỗng** — kênh này không công bố gì.

**Giá:**
- Cột nằm ở độ hạt Order Line nhưng giá trị chỉ đúng ở mức Order, nên nó **không tả được** một
  Order có hai Order Line khác bên giao. Generator hiện **không sinh** trường hợp đó, nên hôm nay
  không mất gì; nếu sau này sinh thì phải đổi cả cột này và chỗ đọc nó.
- `stg_walmart_order_lines` thiếu cột, nên bảng Gold gộp hai kênh phải chấp nhận **null cho
  Walmart** — và null ở đây nghĩa là "kênh không công bố", không phải "chưa biết".

---

## 9. Hệ quả còn mở, thuộc tầng Gold

Mục 8.2 đóng xong phần Silver nhưng mở ra đúng **một** câu, và nó thuộc Gold chứ không thuộc
Silver, nên tài liệu này không tự chốt:

**Dựng `delivered_at` của Walmart từ đâu?** Khoá dedup ở mục 3.1 giữ bản mới nhất, nên Silver chỉ
còn thời điểm hoàn tất. Hai đường đi được, cả hai **chưa đo**:
1. Lấy từ Bronze — `raw_walmart_orders` giữ mọi bản đã xuất, nên lần **đầu tiên** một Order Line
   xuất hiện với trạng thái gốc `"Delivered"` chính là lúc giao (vì `DELIVERED` xảy ra trước
   `COMPLETED` trong `generator/core/model.py` timeline).
2. Cho Silver giữ thêm một cột thời điểm giao sớm nhất, tính lúc dedup.

Đường 1 giữ Silver gọn nhưng buộc Gold đọc Bronze. Đường 2 giữ Gold gọn nhưng thêm việc cho Silver
và phá nguyên tắc "Silver chỉ làm sạch, không tổng hợp". Chưa chọn.

---

## 10. Số đo lần chạy thật (2026-10-12)

Chạy trên hai bảng Bronze NHỎ đã có sẵn, không đụng bảng thật. Hai bảng Silver thử nghiệm đã
`DROP ... PURGE` sau khi đo.

### 10.1 Walmart — `raw_walmart_orders_smoke` -> 1.875.607 Order Line

| | Số |
|---|---|
| dòng Bronze (Order) | 1.293.525 |
| Order Line ra | **1.875.607** |
| khoá dedup phân biệt | **1.875.607** — không khoá nào trùng |
| bản trùng bỏ được | **0** (lý do ở 10.3, KHÔNG phải lỗi) |
| "XONG sau" | 43,3s |

Ba chỗ dữ liệu **tự khớp nhau**, dùng làm kiểm tra chéo:
- `tracking_number` và `ship_date_time` null **129.438** dòng, đúng bằng tổng số đơn chưa gửi
  (`Created` 619 + `Acknowledged` 78.100 + `Cancelled` 50.719 = 129.438). Khớp không lệch một dòng.
- `country` = `US` cho **cả** 1.875.607 dòng => phép ép `"USA"` -> `"US"` (mục 4.4) chạy.
- `order_status` **0 null** => mọi chữ trạng thái nguồn đều có trong bảng ánh xạ.
- Tiền: min 3,54 / max **1.139,64** — nằm dưới mức ~1.200 mà mục 4.1 tính ra TRƯỚC khi có code.
  `decimal(14,2)` đúng cỡ, không phải đoán.

### 10.2 Amazon — gộp hai dạng schema, có số chứng minh

`raw_amazon_orders_sample` (20 ngày mẫu) -> **18.307.929** Order Line, "XONG sau" 383,3s.

| | dạng v0 | dạng 2026 |
|---|---|---|
| Order Line | 8.191.939 | 10.115.990 |
| chữ trạng thái gốc | `Shipped` · `Canceled` · `Unshipped` · `Pending` | `SHIPPED` · `CANCELLED` · `UNSHIPPED` · `PENDING` |
| sau khi quy chuẩn | `shipped` · `cancelled` · `awaiting_shipment` · `pending_payment` | **đúng bốn giá trị đó** |
| `fulfillment_method` | `AFN`/`MFN` -> `channel` 5.839.546 / `seller` 2.352.393 | `AMAZON`/`MERCHANT` -> `channel` 6.910.480 / `seller` 3.205.510 |
| `is_prime` | bool sẵn có | suy từ mảng `programs`, ra bool |
| `number_of_items_shipped` | null **0%** | null **100%** |
| `number_of_items_unshipped` | null **0%** | null **100%** |
| `quantity_shipped` | null **0%** | null **100%** |

Đây chính là bằng chứng cho dòng rubric *handle schema evolution*: hai bộ chữ khác nhau của
cùng bốn nghĩa đã về một bộ, và ba cột dạng 2026 không có thì null **tường minh** chứ không bị
tính bù (mục 5).

Kiểm chéo khác: `126,54 − 22,12 = 104,42` đúng bằng `line_net_amount` của dòng đó =>
`line_gross_amount − line_discount_amount` tính đúng. Tiền dòng max **1.199,64**,
`order_total_amount` max 2.125,07. 0 null ở `order_id`, `order_item_id`, `created_at`,
`order_status`, `is_prime`, `line_net_amount`, `country`.

**Tỉ lệ null của ba cột dạng 2026 — nay đã ĐO, nhưng chỉ trên mẫu:** 10.115.990 / 18.307.929
= **55,3%** số dòng của mẫu 20 ngày. Mục 5 trước đó ghi "chưa đo, cận dưới ≥ 6,95%". Hai số
KHÔNG chống nhau: 6,95% tính theo số dòng của **toàn bộ 180 ngày**, còn 55,3% là của **mẫu 20
ngày rải đều**, mà mẫu rải đều theo NGÀY nên nó lấy nhiều ngày cuối (ngày cuối nặng gấp ~254
lần ngày đầu). Tỉ lệ thật trên 180 ngày **vẫn chưa đo** — cần chạy job trên bảng thật.

### 10.3 Vì sao dedup bắt 0 bản trùng — và việc phải làm để chứng minh được 3 điểm đó

**Không phải lỗi code.** Khoá dedup chạy đúng: 1.875.607 dòng cho 1.875.607 khoá phân biệt.

Nguyên nhân nằm ở **dữ liệu nguồn của hai bảng Bronze nhỏ**: 20 `export_date` trong đó cách
nhau **9-10 ngày** (2026-01-01, 01-10, 01-20, 01-29, 02-08, ...). Bản trùng của generator luôn
nằm trong khoảng **±3 ngày** (`overlap_days = 3`, mục 3.2). Ngày cách nhau 9 ngày thì **không
thể** chứa cùng một đơn hai lần. Nên tỉ lệ trùng 0 là **đúng với dữ liệu này**.

=> **Khoản mở số 1:** để có bằng chứng cho dòng rubric *another offline problem (duplicate
2%)*, phải nạp Bronze từ các ngày **LIỀN NHAU** (ít nhất 7 ngày liền để phủ cửa sổ ±3), rồi
chạy lại job Silver. Chưa làm: cần sinh/nạp lại Bronze, và `/` chỉ còn ~23 GB.

### 10.4 Khoản mở số 2: cột partition của hai bảng Silver

Hai job hiện ghi `partitionedBy(days(created_at))` — chia theo **ngày đặt hàng**. Đây là
**đề xuất của planner**, tài liệu này chưa chốt, và đổi được rẻ cho tới khi chạy trên bảng thật.

Lý do đề xuất: sau dedup thì `export_date` (Amazon) và `fetched_at` (Walmart) chỉ còn nghĩa
"của bản thắng", không còn là một lát dữ liệu đầy đủ — chia theo chúng thì một đơn **nhảy
partition** mỗi lần có bản mới. `days(created_at)` là partition ẩn của Iceberg: cột vẫn là
`timestamp`, query lọc theo khoảng ngày đặt hàng chỉ đọc đúng lát cần. Hai bảng dùng **cùng**
cột partition để Gold union chúng không phải đọc hai cách chia khác nhau.

Giá: dedup ghi lại partition của **mọi** ngày có đơn được cập nhật, không chỉ ngày mới nhất.

### 10.5 Giới hạn đã biết
`DROP TABLE ... PURGE` in lỗi Lakekeeper `BadRequestException: Malformed request` **sau khi**
xoá, giống hệt điều đã gặp 7 lần ở tầng Bronze (`docs/spark.md` mục 5f). Bảng vẫn mất thật:
`SHOW TABLES IN lakehouse.silver` trả về rỗng. Purge chạy **nền**, nên đĩa trả chậm vài chục
giây. Chưa rõ nguyên nhân, chưa điều tra.
