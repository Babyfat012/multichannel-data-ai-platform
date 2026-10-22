<!-- Hướng dẫn vận hành DE v2 trên GCE/GCS; mọi lệnh Cloud do người học tự chạy. -->
# Chạy pipeline DE v2 trên GCP

VM là một máy tính thuê: Docker chạy PostgreSQL, Lakekeeper, Spark và Airflow
trên máy đó. GCS là nơi giữ file nguồn và file bảng Iceberg. Data disk giữ DB,
receipt, log và chỗ Spark ghi tạm. Tắt VM vẫn giữ bucket và disk; chúng vẫn có
chi phí lưu trữ. Không có số đo Cloud nào được suy từ test offline.

Sau Terraform, lấy `raw_bucket`, `lakehouse_bucket`, `vm_name`, `zone`,
`service_account_email` từ output gói K1. Kiểm VM Debian 12, tối đa 8 vCPU / 32 GiB,
đã gắn đúng data disk và VM service account có quyền bucket cùng OAuth scope phù hợp.
Runtime dùng warehouse **mới** `fsds-gcp`; không chuyển dữ liệu warehouse MinIO cũ.
MinIO và generator v1 không chạy trong overlay này.
Đây là đợt batch; Kafka/Flink/Debezium/Trino đang tắt, DataHub chạy riêng.
Các phần đó chưa có bằng chứng deploy GCP chỉ nhờ runtime này.

| Thành phần | Chạy/lưu ở đâu | Port host, luôn loopback | CPU tối đa | RAM giới hạn |
|---|---|---|---:|---:|
| PostgreSQL (sources, lakekeeper, airflow) | VM, `/mnt/fsds-data/postgres` | 5432 | 0.75 | 3 GiB |
| Lakekeeper | VM; con trỏ metadata ở PostgreSQL | 8181 | 0.25 | 512 MiB |
| Spark master | VM | 7077, UI 8080 | 0.25 | 512 MiB |
| Spark worker 1 | VM; spill ở data disk | không publish | 2 | 7 GiB |
| Spark worker 2 | VM; spill ở data disk | không publish | 2 | 7 GiB |
| Airflow, scheduler và Spark driver | VM, `/mnt/fsds-data/airflow` | UI 8081 | 2 | 6 GiB |
| Ba init/migrate container, chỉ chạy lúc khởi tạo | VM | không publish | tổng 0.30 | tổng 512 MiB |
| Raw nguồn v2 | `gs://RAW_BUCKET/order_lifecycle/DATASET_NAME` | — | — | — |
| File bảng Iceberg | `gs://LAKEHOUSE_BUCKET/lakehouse` | — | — | — |
| **Tổng cả container init** | VM | | **7.55** | **24.5 GiB** |

Workers quảng cáo mỗi worker 2 core / 5 GiB cho executor, còn 2 GiB cho overhead.
Airflow task driver dùng heap `fsds_order_lifecycle_driver_memory` (mặc định 3 GiB; 1 GiB từng OOM local); container 6 GiB còn chạy scheduler/UI.
Các container Spark dùng cùng image Airflow (PySpark 3.5.3, Java 17, Python 3.11),
tránh executor Python khác phiên bản driver. Phần còn lại của 8 CPU / 32 GiB dành
cho OS/Docker. Sinh nguồn chạy tuần tự bằng tag riêng, tránh chạy cùng DAG.
Bảng này là quota cấu hình, chưa phải số đo sử dụng hay cam kết chạy được 100 GB.

## 1. Điền cấu hình ở máy cá nhân

Máy điều khiển cần gcloud, SSH, rsync, Ansible và collection `ansible.posix`.
Người học đăng nhập gcloud trên máy cá nhân và chuẩn bị SSH key/OS Login theo K1.
Không tải hoặc tạo SA key. Trong VM, gcloud và container tự lấy danh tính VM.

```bash
cd /DUONG_DAN/FSDS
cp infra/gcp/ansible/inventory.example.ini infra/gcp/ansible/inventory.ini
cp infra/gcp/ansible/group_vars/all.example.yml infra/gcp/ansible/group_vars/all.yml
ansible-galaxy collection install -r infra/gcp/ansible/requirements.yml
```

Điền inventory: `VM_NAME` = `vm_name`, `ZONE` = `zone`, project ID và tên người
OS Login. `ProxyCommand` dùng IAP để chuyển SSH tới VM, không cần public IP.
Điền `raw_bucket`, `lakehouse_bucket`, `service_account_email` đúng output.
`data_disk_name` là tên device data disk được Terraform gắn VM, **không phải boot disk**;
nếu K1 không xuất tên này, đối chiếu resource disk/attachment trong cấu hình K1.
`repo_local` là đường dẫn tuyệt đối repo trên máy điều khiển. Điền `app_dir=/opt/fsds`.

Tạo PostgreSQL password bằng chuỗi ngẫu nhiên ít nhất 16 ký tự và Lakekeeper
encryption key ít nhất 32 ký tự, chỉ chữ/số/`_`/`-` (ví dụ chuỗi hex). Giới hạn
này giúp password dùng an toàn trong DSN, Compose dotenv và shell cùng lúc.
Giữ nguyên encryption key qua các lần chạy vì Lakekeeper cần đọc state đã mã hóa.
Không đưa `all.yml`, inventory thật hoặc `.env` lên Git; khuyến nghị Vault:

```bash
ansible-vault encrypt infra/gcp/ansible/group_vars/all.yml
```

Các lệnh playbook dưới dùng `--ask-vault-pass`; bỏ cờ này nếu chưa mã hóa.
`service_account_email` dùng đối chiếu với VM K1, không được truyền làm key.

## 2. Cài và khởi động runtime

```bash
ansible-playbook -i infra/gcp/ansible/inventory.ini \
  infra/gcp/ansible/site.yml --ask-vault-pass
```

Role base cài Docker Engine/Compose từ kho Docker chính thức cho Debian 12,
thêm người SSH vào group docker, cài gcloud cho bước upload nguồn. Đây là thao
tác trên VM do người học gọi playbook. Role data_disk kiểm `blkid` trước mkfs;
chỉ tạo ext4 khi không thấy filesystem và không có partition/mount; disk có
filesystem giữ nguyên loại hiện có, mount bằng UUID trong fstab. Nếu gặp lỗi
phát hiện disk, dừng và đối chiếu tên disk; không ép format.

Role app rsync mã, loại `.venv/`, `out/`, `.git/`, `.env*`, `.briefs/`,
`.vibe-wise/`, credential/config riêng và state Terraform. `.env` mới có mode
0600 trên VM, lấy secret từ biến/Vault. Repo không chứa SA key. Build dùng image
Airflow hiện có, sau đó khởi động PostgreSQL/catalog/Spark/Airflow, bootstrap
warehouse GCS, upsert Connection/Variables Airflow. Khóa Fernet được tạo một lần
ở `/mnt/fsds-data/airflow/.fernet_key` trước khi Airflow khởi động và được giữ lại. Lần đầu image/packages có
thể tải chậm; chưa có SLA Cloud. Docker data disk giữ DB/spill/receipt; image
layers/build cache vẫn trên boot disk, cần chừa đủ dung lượng boot disk.

Overlay yêu cầu **Docker Compose ≥ 2.24.4** vì `!override` thay hẳn danh sách
port/volume/profile; nếu chỉ thêm port mới thì port wildcard cũ vẫn có thể tồn tại.
MinIO/minio-init cùng dịch vụ ngoài DE v2 nhận profile `disabled-gcp` và `scale: 0`.
App còn chỉ định rõ service khi `up`, không bật profile bị vô hiệu.
Các lệnh Compose chạy trong `/opt/fsds` tự đọc `COMPOSE_FILE` trong `.env`:

```bash
cd /opt/fsds
# Chỉ xem cấu hình; bản rút gọn tránh in credential từ .env.
docker compose --profile catalog --profile spark --profile airflow config --services
```

Không dùng overlay trên stack local đã có container cũ chạy: overlay dành cho
VM mới. Local tiếp tục dùng Compose gốc và mặc định `FSDS_STORAGE=minio`.

## 3. Sinh nguồn và nạp Walmart

Bắt đầu với `source_mode: fixture`, `source_dataset_name: fixture`. Cutoff mẫu
`2026-06-01T00:00:00.000000Z` phù hợp fixture mặc định. Khi chọn scale, điền seed,
số ngày, orders/day và horizon; cutoff phải nằm trong thời gian quan sát nguồn.
`followup_days >= days + 50`, `observation_days >= followup_days + 9` là giới
hạn generator đang có. Generator scale hiện nhận tối đa 180 ngày tạo đơn; không
tự hiểu gói runtime này đã mở rộng được nguồn 365 ngày.

```bash
ansible-playbook -i infra/gcp/ansible/inventory.ini \
  infra/gcp/ansible/site.yml --tags source_data --ask-vault-pass
```

Nguồn local VM nằm ở `/mnt/fsds-data/source/DATASET_NAME`. Có `manifest.json`
thì bỏ qua sinh lại; thay thông số phải dùng **tên dataset mới**, không ghi đè
thư mục cũ. Nếu lần sinh bị ngắt trước manifest, giữ lại thư mục lỗi để điều tra
và chọn tên mới. `gcloud storage rsync` upload theo checksum, không dùng cờ xóa
object thừa; `_truth/` không được upload. Manifest vẫn nhắc truth là dữ liệu
chỉ cho test, pipeline không đọc truth. Bucket raw phải dành prefix riêng.

Fixture dùng `scripts/setup/load_order_lifecycle_walmart.py`; scale dùng helper trong role
đọc mọi shard rồi tái dùng `Row/load` của script đó. Nạp lại một dataset thay
trong cùng transaction, không nhân đôi. Không xóa các dataset khác trong DB.
Nếu mẫu scale không có Walmart, helper dừng; tăng mẫu hoặc quay về fixture để
kiểm được cả JDBC. Helper hiện gom Walmart vào RAM, cần đánh giá trước nguồn lớn.

## 4. Chạy smoke

```bash
ansible-playbook -i infra/gcp/ansible/inventory.ini \
  infra/gcp/ansible/site.yml --tags smoke --ask-vault-pass
```

Smoke kiểm đủ service chạy, healthcheck hiện có xanh, init/migrate exit 0,
MinIO không chạy, warehouse có đúng bucket/prefix. Spark tạo bảng UUID riêng
trong `lakehouse.runtime_smoke`, ghi/đọc ba số và kiểm location `gs://`, sau đó
`DROP TABLE ... PURGE` đúng bảng đó trong `finally`. Namespace rỗng được giữ để
chạy lại. Smoke không xóa bucket/disk. Cuối cùng kiểm Airflow nhận DAG v2.
Smoke không thay thế việc chạy toàn pipeline và đối soát receipt.

## 5. Mở Airflow qua IAP và trigger pipeline

Trên máy cá nhân, dùng SSH qua IAP và forward tới loopback VM. Giữ terminal
này mở. IAP TCP forwarding trực tiếp tới port 8081 của IP VM không phù hợp với
service chỉ bind `127.0.0.1`; SSH chuyển tiếp tới loopback giải quyết điều đó.

```bash
gcloud compute ssh VM_NAME --zone=ZONE --project=PROJECT_ID \
  --tunnel-through-iap -- -N -L 8081:127.0.0.1:8081
```

Mở `http://127.0.0.1:8081`. Mật khẩu admin standalone nằm trong
`/mnt/fsds-data/airflow/standalone_admin_password.txt` trên VM; đọc bằng SSH,
không chụp hoặc đưa vào báo cáo. Bật DAG `dp_order_lifecycle` và Trigger với:

```json
{
  "source_root": "gs://RAW_BUCKET/order_lifecycle/DATASET_NAME",
  "cutoff": "2026-06-01T00:00:00.000000Z",
  "walmart_jdbc": "walmart_lifecycle.orders"
}
```

Thay bucket/dataset/cutoff bằng giá trị đã sinh. Với run mới, giữ run_id mới;
Gold append generation và có thể từ chối retry sau khi đã ghi generation.
DAG chạy contracts → Bronze → Silver → Gold → validate. Truyền Walmart JDBC
để có bằng chứng đọc DB nguồn, thay vì chỉ đọc file bootstrap.

## 6. Đọc bằng chứng rồi tắt VM

Receipt trên disk, không dùng `gs://` vì writer hiện dùng pathlib:
`/mnt/fsds-data/airflow/receipts/v2/TS_NODASH/`. Đọc đủ
`check_contracts.json`, `ingest_bronze.json`, `build_silver.json`, `build_gold.json`,
`validate_gold.json`. Bronze receipt chỉ ghi status, đối soát số dòng nằm
trong log task. Đối chiếu `run_id`, cutoff, checks và quarantine; DAG xanh
không tự chứng minh schema/quarantine bằng 0. Có thể lưu receipt lên raw bucket
bằng gcloud storage cp do người học tự chạy nếu cần giữ bản sao ngoài disk.

<!-- GCP: người học chạy, planner điền -->

Ghi thời gian chạy, rows/bytes/checks, cost và ảnh UI thật vào nghiệm thu.
Sau khi DAG xong, không còn upload/smoke, người học tắt VM từ máy cá nhân:

```bash
gcloud compute instances stop VM_NAME --zone=ZONE --project=PROJECT_ID
```

Start lại cùng VM/disk rồi chạy lại playbook nếu cần đưa container về trạng thái
đúng. Không xóa disk, bucket hoặc đổi encryption key khi muốn giữ state.

## 7. Publish DataHub từ receipt GCP

Chạy trên **máy cá nhân** sau khi đã copy và giải nén archive ở bước 6 của
[checklist](gcp-checklist.md). Chuẩn bị DataHub GMS/SDK theo
[runbook DataHub](datahub-order-lifecycle.md#lệnh-offline-và-gửi-thật).
Chọn một thư mục pipeline có đủ năm receipt, cùng `run_id`, trạng thái passed,
mọi check = 0; giữ `schemas.json` cạnh receipt. Không chọn thư mục warehouse
hoặc thư mục cha chứa nhiều run. Source root và lựa chọn JDBC phải khớp conf
của chính run đó.

```bash
# Thay bucket, dataset và timestamp bằng giá trị của run GCP đã nghiệm thu.
receipt_dir='out/gcp-evidence/unpacked/airflow/receipts/v2/<TS_PIPELINE>'
source_root='gs://<RAW_BUCKET>/order_lifecycle/<DATASET_NAME>'
datahub_python="$HOME/.local/share/uv/tools/acryl-datahub/bin/python"
"$datahub_python" scripts/run/publish_order_lifecycle_datahub.py --dry-run \
  --receipt-dir "$receipt_dir" --source-root "$source_root" \
  --walmart-jdbc walmart_lifecycle.orders \
  > out/gcp-evidence/datahub-gcp-mcps.json
# Kiểm MCP rồi dùng cùng arguments để gửi tới GMS local đã khởi động.
"$datahub_python" scripts/run/publish_order_lifecycle_datahub.py --publish \
  --server http://127.0.0.1:8089 \
  --receipt-dir "$receipt_dir" --source-root "$source_root" \
  --walmart-jdbc walmart_lifecycle.orders
```

Token nếu cần chỉ truyền qua `DATAHUB_GMS_TOKEN`. Chụp Lineage/Quality sau khi
kiểm assertion `runId` khớp receipt; chạy capture theo tài liệu DataHub hoặc
mở UI `http://127.0.0.1:9002`. Giữ MCP/ảnh cùng thông tin run/dataset để tránh
nhầm hai bộ fixture và scale.

Không dùng `scripts/run/run_order_lifecycle_live.sh --phase all` cho run GCP:
script hiện bật MinIO, trigger nguồn `/data/order_lifecycle` và publish định
danh local. Ngay cả pha `datahub` của script cũng dùng source root local;
với receipt GCP dùng lệnh publisher ở trên. Schema/receipt được copy từ GCP
không biến lần chạy DataHub trên máy cá nhân thành DataHub đã deploy GCP.

## Package, identity và các điểm planner phải kiểm

`fsds_spark_packages_gcp` gồm Iceberg Spark runtime 3.5/Scala 2.12 1.11.0,
`iceberg-gcp-bundle:1.11.0`, PostgreSQL JDBC 42.7.4 và
GCS connector Hadoop 3 **2.2.26 shaded** (artifact Maven
`com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26`, classifier `shaded`).
Spark `--packages` chỉ nhận ba phần, vì vậy connector không nằm trong chuỗi
packages: role app tải JAR/checksum từ Maven Central về data disk và mount vào
`$SPARK_HOME/jars` trên cả driver/master/workers.
Chọn nhánh Hadoop 3 vì Spark 3.5 dùng Hadoop 3; bản shaded giảm va chạm dependency
với Iceberg. Chưa tải JAR/resolve Maven trong sandbox, nên tương thích tổ hợp vẫn cần kiểm.

DAG cũ đọc `fsds_spark_packages`. Để giữ nguyên Variable local, overlay đặt
`AIRFLOW_VAR_FSDS_SPARK_PACKAGES` bằng chuỗi GCP, ưu tiên hơn metadata DB;
setup chỉ thêm Variable GCP riêng và cấu hình nguồn trên VM. Operator cũ vẫn
đọc Connection `fsds_minio`; `.env` dùng `unused-gcp` làm placeholder không có
quyền. Những giá trị này không dùng để xác thực GCS. Thay operator nằm ngoài
phạm vi file brief cho phép, nên không sửa trong gói này.

| Planner phải kiểm | Cơ sở / trạng thái |
|---|---|
| URL/checksum JAR `gcs-connector-hadoop3-2.2.26-shaded.jar` và tương thích Iceberg 1.11 | [POM connector v2.2.26](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/v2.2.26/gcs/pom.xml); chưa tải JAR |
| GCS connector `fs.gs.impl`, `fs.AbstractFileSystem.gs.impl` | Class `GoogleHadoopFileSystem`/`GoogleHadoopFS`; cần smoke trên driver và executor |
| Auth connector v2.2.26 | [Cấu hình đúng tag v2.2.26](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/v2.2.26/gcs/CONFIGURATION.md): `fs.gs.auth.service.account.enable=true`, không keyfile → identity GCE. Không dùng `fs.gs.auth.type` của phiên bản mới |
| Lakekeeper v0.13.6 system identity | [Tài liệu GCS hiện hành](https://docs.lakekeeper.io/docs/latest/storage-gcs/) xác nhận `LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS=true`, `type=gcs`, `credential-type=gcp-system-identity`; cần kiểm đúng image v0.13.6 |
| Response warehouse detail dùng `id` và `storage-profile` | Cần kiểm Management API v0.13.6 thật; script dừng lỗi nếu không đúng, không bỏ qua warehouse sai |
| Metadata server từ mạng bridge Docker + quyền bucket / OAuth scopes | Chỉ smoke thật mới xác nhận; không có SA key, không mount ADC người dùng |
| VM image Python/Java, tải jar, RAM/đĩa, quy mô 100 GB | Chưa chạy Cloud; worker cùng image với driver, quota không phải benchmark |

Kiểm offline trước khi chạy Cloud (planner chạy):

```bash
.venv/bin/pytest -q tests/unit/spark_jobs/lib/test_iceberg_session.py tests/spark/test_gcp_runtime.py
# Tên bucket giả chỉ để kiểm config, không kết nối GCS; không đọc .env thật.
env RAW_BUCKET=fsds-offline-raw LAKEHOUSE_BUCKET=fsds-offline-lakehouse \
  FSDS_SPARK_PACKAGES_GCP='org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-gcp-bundle:1.11.0,org.postgresql:postgresql:42.7.4' \
  docker compose --env-file .env.example -f docker-compose.yml -f docker-compose.gcp.yml \
  --profile catalog --profile spark --profile airflow config --quiet
uvx --from ansible-core ansible-galaxy collection install \
  -r infra/gcp/ansible/requirements.yml -p /tmp/fsds-ansible-collections
env ANSIBLE_COLLECTIONS_PATH=/tmp/fsds-ansible-collections \
  ANSIBLE_LOCAL_TEMP=/tmp/fsds-ansible-local \
  uvx --from ansible-core ansible-playbook \
  -i infra/gcp/ansible/inventory.example.ini infra/gcp/ansible/site.yml --syntax-check
# Môi trường planner phải có pyspark 3.5.3 và Java phù hợp; không cần Google.
FSDS_RUN_SPARK_TESTS=1 python -m pytest -q tests/spark/test_gcp_runtime.py
# Chạy lại Spark regression sẵn có theo runner local đang dùng của dự án:
# tests/spark/check_order_lifecycle_silver.py
```
