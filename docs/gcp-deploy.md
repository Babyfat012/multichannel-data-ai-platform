<!-- Mục đích: hướng dẫn người học tự triển khai, bật/tắt và dọn hạ tầng DE GCP trong credit trial. -->
# Triển khai DE lên GCP bằng Terraform

Terraform giống bản kê đồ dùng: khai báo cần một máy, hai kho và một mạng;
`plan` cho xem dự kiến, `apply` mới tạo thật. Ansible của K2 sẽ cài phần mềm
sau đó. **Mọi lệnh trong tài liệu này do người học tự chạy.** Trợ lý không
đăng nhập, gọi API Google, chạy plan/apply/destroy hoặc Ansible.

Đích đã chốt: GCE trước, Kubernetes ở phase MLE sau; GCS raw và warehouse
Iceberg **mới**, không copy metadata `s3://` cũ rồi coi là đã migrate.
Chạy POC nhỏ trước khi sinh/chạy lại nguồn 100 GB.

## 1. Kiểm trước khi bắt đầu

Mở Google Cloud Console → Billing → Credits/Overview, ghi lại **credit còn
lại và ngày hết hạn**. 300 USD là credit ban đầu, không phải số chắc chắn
còn lại hay ngân sách mỗi tháng. Không nâng cấp trả phí vì mục tiêu là
**0 USD tiền túi**. Nếu credit/hạn không đủ cho lần chạy và lưu dữ liệu sau
đó thì dừng, dọn tài nguyên khi còn credit.

Trên Console → IAM & Admin → Quotas, kiểm region `asia-southeast1`, E2 CPU,
quota trial tổng tối đa **8 vCPU đang chạy**, disk và external IPv4. VM mặc
định `e2-standard-8` dùng cả 8 vCPU/32 GiB; VM khác cũng tính vào trần.
Allowlist trong Terraform chỉ kiểm thô CPU **của một VM**, không xác nhận
quota tài khoản hoặc capacity zone. Không có GPU. Nếu không đủ quota,
tắt VM khác hoặc chọn E2 nhỏ hơn trong allowlist, rồi đánh giá lại RAM runtime.

Chuẩn bị Google Cloud CLI, Terraform ≥1.6 (<2), Git checkout repo trên máy
người học; chạy các lệnh sau tại **thư mục gốc repo**, giữ cùng terminal:

```bash
gcloud auth login
gcloud auth application-default login
read -r -p 'Nhập Project ID của bạn: ' PROJECT_ID
export PROJECT_ID
gcloud config set project "$PROJECT_ID"
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

Đăng nhập CLI và ADC là hai việc riêng: Terraform dùng ADC local; VM dùng
service account qua metadata server, **không tải key JSON**. Không copy ADC,
`.env`, state hoặc file plan lên Git.

Project phải tồn tại và đã liên kết billing trial. Người chạy cần quyền bật
API (Service Usage Admin), quản lý Compute/network, bucket, tạo service
account và gắn SA (Service Account Admin/User), cập nhật IAM project cho
hai role telemetry. Có thể nhờ quản trị viên cấp các quyền cần thiết; không
cấp Owner/Editor cho **SA của VM**. Service Usage API cần được quản trị viên
bật trước nếu project chưa cho phép bật API bằng Terraform. Budget tùy chọn
cần quyền Billing Costs Manager (hoặc quyền budget tương đương) trên billing
account, quyền đọc project; không yêu cầu tạo SA key.

Người mở SSH/IAP cần `roles/iap.tunnelResourceAccessor`,
`roles/compute.osAdminLogin` (Ansible cần sudo), quyền đọc VM/project (ví dụ
`roles/compute.viewer`) và `roles/iam.serviceAccountUser` trên SA gắn VM.
Admin cấp quyền đó cho **người học**, không phải cho SA runtime. Tài khoản
ngoài tổ chức có thể cần thêm OS Login External User từ quản trị tổ chức.
Terraform K1 không quản lý IAM người học. Nếu IAP báo 403, đối chiếu các quyền
này, không mở SSH công khai để xử lý.

## 2. Tạo hạ tầng, đọc plan và output

```bash
cp infra/gcp/terraform/terraform.tfvars.example infra/gcp/terraform/terraform.tfvars
"${EDITOR:-nano}" infra/gcp/terraform/terraform.tfvars
```

Thay `YOUR_PROJECT_ID` bằng `$PROJECT_ID` vừa nhập. Mặc định region/zone là
`asia-southeast1`/`asia-southeast1-b`, prefix `fsds`, boot 50 GiB và data
200 GiB đều `pd-balanced`. Nếu đổi region thì đổi zone tương ứng. Giữ
`force_destroy = false`. ID thật chỉ nằm trong file local đã được ignore.

```bash
terraform -chdir=infra/gcp/terraform init
terraform -chdir=infra/gcp/terraform validate
terraform -chdir=infra/gcp/terraform plan -out=de.tfplan
terraform -chdir=infra/gcp/terraform show de.tfplan
terraform -chdir=infra/gcp/terraform apply de.tfplan
terraform -chdir=infra/gcp/terraform output
rm infra/gcp/terraform/de.tfplan
```

Chỉ chạy `apply` sau khi đọc plan: một VM, hai bucket private, một data disk,
VPC/subnet, **chỉ ingress TCP 22 từ `35.235.240.0/20`**, không GPU/NAT/static
IP/SA key. Kiểm tên project và mọi dấu `destroy`/`replace`; lần đầu không
được xóa tài nguyên cũ. Saved plan chứa dữ liệu cấu hình, không đưa vào Git.
Nếu plan sai thì sửa tfvars, tạo lại plan trước khi apply.

Terraform bật Compute, Storage, IAM, IAP; `billingbudgets` chỉ khi bật budget.
APIs được giữ bật sau destroy. K1 không cài agent; nếu K2 dùng Cloud Logging/
Monitoring agent, người học cần bảo đảm API logging/monitoring đã bật theo K2.

| Output | Dùng để làm gì |
|---|---|
| `raw_bucket`, `lakehouse_bucket` | Tên hai bucket, bàn giao K2 |
| `raw_bucket_uri`, `lakehouse_bucket_uri` | URI `gs://...` cho cấu hình raw/warehouse mới |
| `service_account_email` | Danh tính VM dùng ADC, có quyền object trên đúng hai bucket |
| `vm_name`, `zone` | Chọn đúng VM cho Ansible, SSH, stop/start |
| `ssh_command`, `port_forward_command` | Lệnh truy cập qua IAP |

SA có `storage.objectAdmin` + `storage.legacyBucketReader` **trên mỗi bucket**
để đọc/ghi/list object và đọc thông tin bucket. Chỉ `logging.logWriter` và
`monitoring.metricWriter` ở project; scope `cloud-platform` không tự cấp thêm
IAM. Mọi container lấy được metadata credentials sẽ có quyền của SA đó;
không chạy workload không tin cậy trên VM.

Budget mặc định **tắt**. Muốn bật: đặt `enable_budget = true`, nhập
`billing_account_id` local và giữ/sửa `budget_amount_usd = 50`, rồi plan/apply
lại. Budget lọc đúng project theo project number, cảnh báo 50/90/100% chi
phí **trước credit**, mặc định chu kỳ tháng, gửi cho billing IAM recipients
mặc định. Kiểm email người nhận trên Console. **Budget chỉ cảnh báo, không
chặn chi**, không phải giới hạn tổng credit và thông báo có độ trễ.

## 3. Bước Ansible — xem `docs/gcp-runtime.md`

Xem [runbook runtime của K2](gcp-runtime.md). Nếu file chưa được bàn giao,
dừng tại đây; VM mới chưa có Docker/Compose hoặc service DE.

K2 dùng output để cài runtime và mount disk `fsds-data`, giữ PostgreSQL/
catalog cùng dữ liệu cần bền vững trên data disk, cấu hình ADC/GCS và tạo
warehouse mới. Boot disk có thể bị xóa khi recreate VM. Không format data
disk cũ khi chạy Ansible lại. Không chạy toàn bộ Spark/Flink/DataHub đồng
thời trước khi đo RAM/CPU/disk bằng POC nhỏ. GCS không thay backup PostgreSQL.

`attached_disk` của Google provider, theo hiểu biết chưa validate online,
không có thuộc tính `auto_delete`. Vì vậy data disk được khai báo riêng bằng
`google_compute_disk`, rồi gắn vào VM; kiểu gắn này giữ disk khi VM recreate
(tương đương `autoDelete=false` trên GCE). Không nhét thuộc tính không có vào
block. Planner phải `validate`; người học kiểm plan và auto-delete trên
Console trước khi đặt dữ liệu. **`terraform destroy` vẫn xóa data disk** vì
disk cũng là resource được Terraform quản lý.

## 4. Mở SSH và UI qua IAP

VM có public IPv4 **ephemeral** để kéo image/gói qua Internet, tránh phí
Cloud NAT. IP public có phí theo bảng giá; có thể đổi sau stop/start. Đổi
lại phải giữ firewall chặt: không mở bất kỳ cổng UI nào ra Internet. Không
có firewall allow-all nội bộ; egress theo mặc định của VPC. IAP chỉ được
vào TCP 22, rồi SSH chuyển tiếp tới loopback VM.

```bash
VM_NAME="$(terraform -chdir=infra/gcp/terraform output -raw vm_name)"
VM_ZONE="$(terraform -chdir=infra/gcp/terraform output -raw zone)"
gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap
```

Thoát phiên SSH bằng `exit`. Chạy tunnel dưới đây trong terminal riêng tại
repo gốc, đặt lại ba biến Project/VM/zone nếu là terminal mới. Giữ terminal
mở khi dùng trình duyệt, Ctrl-C để đóng tunnel:

```bash
terraform -chdir=infra/gcp/terraform output -raw port_forward_command
# Lệnh tương ứng, chỉ bind cổng local trên máy người học:
gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap -- \
  -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:8080:127.0.0.1:8080 \
  -L 127.0.0.1:8181:127.0.0.1:8181 \
  -L 127.0.0.1:8081:127.0.0.1:8081
```

Mở `http://127.0.0.1:8080` (Spark master), `http://127.0.0.1:8081` (Airflow),
`http://127.0.0.1:8181` (Lakekeeper). Cổng driver 4040 không được overlay publish.
Trong Compose local hiện tại Airflow host **8081**, container **8080**;
không suy Airflow nằm cùng host 8080 với Spark. Đối chiếu K2 rồi mở riêng:

```bash
gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap -- \
  -N -o ExitOnForwardFailure=yes -L 127.0.0.1:8081:127.0.0.1:8081
```

Airflow mở `http://127.0.0.1:8081`. DataHub dùng mapping K2 thực tế và thêm
`-L 127.0.0.1:CONG_LOCAL:127.0.0.1:CONG_VM` tương ứng; thay các chữ viết hoa
bằng số cổng trước khi chạy. Không dùng `gcloud compute start-iap-tunnel`
vào cổng UI trực tiếp: firewall chỉ cho TCP 22. Nếu báo address-in-use, đổi
cổng local (ví dụ 18080) và giữ nguyên cổng VM. Nếu connection refused, kiểm
runtime đã publish **đúng cổng trên loopback VM** chưa; K1 không tự mở UI.

## 5. Bật khi cần, tắt khi xong

Trước khi tắt, theo K2: chờ job hoàn tất hoặc dừng có kiểm soát, flush/backup
PostgreSQL/catalog ra nơi bền vững, ghi run/generation và kiểm dữ liệu. Sau đó:

```bash
gcloud compute instances stop "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE"
gcloud compute instances describe "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --format='value(status)'
```

Chỉ coi đã tắt khi thấy `TERMINATED`. Stop khác suspend, không dùng suspend.
Compute CPU/RAM dừng tính theo trạng thái stop, nhưng **boot/data disk được
cấp vẫn tính tiền**, cả khi trống; bucket/object versions, backup/snapshot,
request/egress phát sinh vẫn có phí. Ephemeral IPv4 thường được giải phóng
khi stop; kiểm Console, không đổi thành static IP để giữ địa chỉ.

Bật lại trước buổi học:

```bash
gcloud compute instances start "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE"
gcloud compute instances describe "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --format='value(status)'
```

Khi thấy `RUNNING`, theo K2 kiểm mount data disk, PostgreSQL/catalog, chạy lại
Compose có kiểm soát và rematerialize Redis nếu cần. Mở lại IAP tunnel;
đừng dùng lại IP cũ. K1 không có startup script cài phần mềm.

## 6. Dọn sạch khi không dùng nữa

Stop chỉ tiết kiệm compute. Muốn ngừng tiền disk/bucket phải sao lưu thứ
cần giữ ra nơi đã kiểm tra, rồi xóa tài nguyên. `destroy` xóa cả data disk,
không chỉ VM. Giữ local state tới khi dọn xong, không xóa state để “dọn”.

```bash
terraform -chdir=infra/gcp/terraform plan -destroy
terraform -chdir=infra/gcp/terraform destroy
```

Đọc kỹ project/tên bucket/disk trước khi xác nhận. Mặc định
`force_destroy=false` ngăn xóa bucket còn dữ liệu; **không** bảo vệ VM/disk
khỏi destroy. Nếu destroy dở dang vì bucket không rỗng, state ghi lại phần
còn lại để xử lý tiếp. Sau khi chắc dữ liệu có thể xóa, lấy tên từ output
(hoặc Console nếu output không còn), liệt kê trước:

```bash
RAW_BUCKET="$(terraform -chdir=infra/gcp/terraform output -raw raw_bucket)"
LAKEHOUSE_BUCKET="$(terraform -chdir=infra/gcp/terraform output -raw lakehouse_bucket)"
gcloud storage ls --all-versions "gs://${RAW_BUCKET}/**"
gcloud storage ls --all-versions "gs://${LAKEHOUSE_BUCKET}/**"
# Hai lệnh dưới xóa toàn bộ object, gồm các phiên bản cũ. Không thể dựa vào soft delete để cứu.
gcloud storage rm --recursive --all-versions "gs://${RAW_BUCKET}/**"
gcloud storage rm --recursive --all-versions "gs://${LAKEHOUSE_BUCKET}/**"
terraform -chdir=infra/gcp/terraform destroy
```

Bucket trống có thể làm lệnh liệt kê/xóa báo không có URL khớp; đối chiếu
Console thay vì đổi sang pattern rộng hơn. Không dùng lệnh xóa bucket bên
ngoài rồi bỏ state. Cách khác: sau sao lưu, sửa local tfvars
`force_destroy=true`, người học plan/apply để lưu cờ này vào state rồi
`destroy`; cờ này cho Terraform xóa cả nội dung. Tránh bật mặc định.

Lifecycle bật versioning cho cả hai bucket. Chỉ xóa noncurrent sau 30 ngày
(kể từ lúc thành noncurrent), live object trong **`tmp/`** sau 7 ngày;
các số ngày là biến. Xóa live `tmp/` có thể chuyển thành noncurrent trước
khi bị dọn tiếp, nên không giả định dung lượng biến mất ngay sau 7 ngày.
Không tự TTL live Iceberg/metadata: maintenance phải tôn trọng snapshot và
published generation. Soft delete được tắt rõ ràng (`0` giây) để tránh phí
giữ thêm; versioning không cứu được khi đã xóa mọi phiên bản hoặc destroy.

Sau destroy kiểm Console các tài nguyên còn lại, snapshots/backups tạo
ngoài K1 và billing có độ trễ. APIs cố ý còn bật; K1 không xóa project,
billing account, quyền người học hoặc dữ liệu cũ ngoài phạm vi này.

## 7. Dự toán trước mỗi lần chạy

**Các số dưới là ước tính minh họa theo hiểu biết giá niêm yết, chưa tra mạng
và chưa xác nhận giá Singapore/thuế/credit. Mỗi số cần người học kiểm trên
Pricing Calculator** tại `https://cloud.google.com/products/calculator`.
Không coi đây là báo giá hay bằng chứng credit đủ. Điền lại đơn giá trước
khi apply, tính cả ngày VM tắt và phiên bản object cũ.

| Hạng mục | Đơn giá ước tính USD — từng số cần người học kiểm trên Pricing Calculator | Công thức tự nhân |
|---|---|---|
| E2 standard 8, on-demand, Singapore | khoảng **0.27–0.40/giờ**, cần người học kiểm trên Pricing Calculator | `giờ chạy × đơn giá VM`; ví dụ `8 giờ × N buổi × đơn giá` |
| Boot + data pd-balanced | khoảng **0.10–0.15/GiB-tháng**, cần người học kiểm trên Pricing Calculator | `(50 + 200) × đơn giá disk × số giờ giữ disk / 730`; tính cả lúc VM tắt |
| GCS Standard regional | khoảng **0.020–0.030/GB-tháng**, cần người học kiểm trên Pricing Calculator | `GB trung bình gồm raw + lakehouse + versions + backup × đơn giá × tháng` |
| External IPv4 ephemeral đang dùng | khoảng **0.005–0.010/giờ**, cần người học kiểm trên Pricing Calculator | `giờ cấp IP × đơn giá`; kiểm việc giải phóng IP sau stop |
| Internet egress về máy cá nhân | khoảng **0.08–0.20/GB**, cần người học kiểm trên Pricing Calculator | `GB tải ra × đơn giá theo đích/tier`; IAP không biến egress thành miễn phí |
| GCS operations class A/B | khoảng **0.005–0.010/1.000 A**, **0.0004–0.001/1.000 B**, mỗi số cần người học kiểm trên Pricing Calculator | `(số A/1.000 × giá A) + (số B/1.000 × giá B)` |

730 giờ là quy ước ước lượng tháng, không phải thời gian bật VM bắt buộc.
Ví dụ VM chạy 8 giờ × 5 buổi thì tính 40 giờ compute/IP nhưng disk có thể
vẫn giữ cả tháng. 100 GB nguồn không đồng nghĩa tổng GCS chỉ 100 GB: còn
Bronze/Silver/Gold/Iceberg versions/exports. 200 GiB data disk chỉ là cấu
hình bắt đầu, phải đo dung lượng Docker images, DB, Kafka và Spark spill.
Tính thêm backup/snapshot, logs/metrics, cross-region/network và thuế nếu
có; không tự coi chúng miễn phí hoặc được credit bao phủ.

`dự toán tổng = compute + disk + GCS + IP + requests + egress + phần khác`.
Giới hạn số buổi và thời hạn giữ storage sao cho dự toán có phần dự phòng
nhỏ hơn **credit thực còn lại**, kiểm lại billing sau từng buổi. Budget 50
USD/tháng không thay việc theo dõi tổng credit và hạn trial.

## 8. Rủi ro và cách xử lý

| Rủi ro | Dấu hiệu/việc cần làm |
|---|---|
| Hết credit hoặc quá hạn | Kiểm Console trước mỗi buổi và lịch lưu disk/GCS; dừng sớm, sao lưu/destroy khi còn credit; không nâng cấp trả phí |
| Trial chỉ 8 vCPU hoặc thiếu quota/capacity | Tính tổng VM đang chạy; đọc quota region; tắt VM khác hoặc giảm allowlist machine type và đo lại runtime |
| Quên tắt VM | Đặt nhắc giờ kết thúc; stop, xác nhận `TERMINATED`; kiểm billing mỗi buổi; budget chỉ cảnh báo |
| Xóa nhầm bucket/disk | Đọc plan destroy, giữ `force_destroy=false`, đối chiếu project/tên, thử restore backup trước; destroy vẫn xóa data disk |
| Mất local state | Sao lưu state có kiểm soát, bảo vệ như secret; không apply từ thư mục/state trống khi hạ tầng cũ còn |
| Object TTL làm mất bảng | Không đặt dữ liệu chính vào `tmp/`; chỉ dọn noncurrent và prefix tạm, maintenance Iceberg theo K2/contracts |

## 9. State local và chuyển GCS backend sau

Mặc định state ở `infra/gcp/terraform/terraform.tfstate`, bị ignore. State
cần được giữ giữa các lần chạy và có thể chứa dữ liệu nhạy cảm. Sao lưu ở
nơi riêng có kiểm soát quyền; không bật hai tiến trình Terraform cùng lúc.
Sau lần init thành công, planner review `.terraform.lock.hcl` để ghim checksum;
file này có thể được quản lý phiên bản, khác với state/tfvars.

Khi cần remote state, người học tạo **bucket state riêng** trước (không dùng
raw/lakehouse vì các bucket này bị destroy cùng stack), bật versioning,
chặn public và chỉ cấp quyền cho người vận hành, không cấp SA runtime.
Thêm file `backend.tf` local theo mẫu, thay tên placeholder trước khi chạy:

```hcl
# Mục đích: remote state riêng, tách khỏi vòng đời dữ liệu và VM DE.
terraform {
  backend "gcs" {
    bucket = "YOUR_STATE_BUCKET_NAME"
    prefix = "fsds/de"
  }
}
```

Sao lưu local state, rồi người học chạy:

```bash
terraform -chdir=infra/gcp/terraform init -migrate-state
terraform -chdir=infra/gcp/terraform state list
```

Kiểm resource list và bản state đã chuyển trước khi xóa bản backup cũ.
Không chạy init migration khi chưa có bucket/quyền; K1 không tạo backend
GCS tự động, không dùng biến Terraform trong block backend.
