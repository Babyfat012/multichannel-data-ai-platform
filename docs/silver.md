# Thiết kế tầng Silver — làm sạch và quy về ngôn ngữ chung

Tài liệu này ghi lại **thiết kế đã chốt** của tầng Silver: hai bảng `stg_*` theo kênh, độ
hạt Order Line, khoá dedup, kiểu dữ liệu, và cách xử lý khác biệt giữa hai dạng schema của
Amazon. Cập nhật **2026-10-11**.

Mọi số ở đây lấy từ một dòng code (ghi `file.py:dòng`) hoặc ghi rõ **"đã đo"** / **"chưa đo"**.
Chỗ nào chưa đo thì không suy đoán.

---

## 1. Phạm vi tài liệu

- Đây là **thiết kế đã chốt, CHƯA có code**. Chưa có `spark_jobs/02_silver_*.py` nào.
- Tài liệu tả *quyết định, lý do, và giá* — không phải hướng dẫn chạy.
- Mục 8 liệt kê đủ **3 chỗ chưa quyết**; tài liệu cố ý **không** tự chọn.
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
| Walmart | `(purchase_order_id, customer_order_id, line_number)` | `status_date` mới nhất |

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
`statusDate` là lúc **hoàn tất**, không phải lúc giao. Vì vậy **chưa đặt tên** cột `delivered_at`.

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

## 8. Chưa quyết

Ba khoản dưới đây **chưa quyết**. Tài liệu ghi ra để không ai tưởng đã chốt.

1. **Giữ hay bỏ các cột chỉ Walmart có:** `buyer_id`, `is_guest`, `customer_email_id`,
   `order_type`.
2. **Tên cột cho thời điểm giao của Walmart.** Chưa đặt tên (`delivered_at`/`delivered_time` hay
   tên khác) vì `COMPLETED` cũng được ghi là `Delivered`
   (`generator/channels/walmart.py:38-39`), nên với đơn completed thì `statusDate` là lúc **hoàn
   tất**, không phải lúc giao.
3. **`fulfilled_by` so với `fulfillment_method`.** `CONTEXT.md` định nghĩa Fulfillment Method
   theo **Order Line**, nhưng nguồn Amazon chỉ cho ở mức **Order**. Chưa quyết dùng tên nào và
   đặt ở mức nào.
