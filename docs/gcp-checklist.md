# GCP: từ số 0 tới có số liệu

Phạm vi đợt này là **batch lifecycle v2**: GCS/JDBC → Bronze → Silver → Gold
→ PostgreSQL. Overlay tắt Kafka, Flink, Debezium và Trino; DataHub chạy riêng
trên máy cá nhân bằng receipt của run GCP. Xem [checklist sẵn sàng DE](de-gcp-readiness-20261020.md)
cho các mục medium, skew, ≥100 GB, SCD2 và nhánh streaming còn mở.

**HẾT BUỔI PHẢI TẮT VM và kiểm `TERMINATED` (bước 7).** Mọi lệnh dưới đây do
**người học tự gõ**. Thay `<CHỮ_HOA>` trước khi chạy; máy cá nhân đứng ở gốc repo,
lệnh có nhãn **VM** chạy sau SSH. Chỉ chạy một job/DAG Spark tại một thời điểm.

- [ ] **0 — Credit, hạn và quota.** Mở Console → Billing → Credits/Overview, ghi
  credit còn lại/ngày hết hạn; IAM & Admin → Quotas kiểm E2 và tổng trial 8 vCPU.
  Trên máy cá nhân:

  ```bash
  export PROJECT_ID='<PROJECT_ID>'
  gcloud auth login
  gcloud auth application-default login
  gcloud config set project "$PROJECT_ID"
  gcloud auth application-default set-quota-project "$PROJECT_ID"
  gcloud compute regions describe asia-southeast1 --project="$PROJECT_ID" --format='yaml(quotas)'
  ```

  **Đạt:** credit/hạn đủ dự toán, quota CPU còn ít nhất 8 và không có VM khác chiếm trần trial.
  **Nếu sai:** [quyền, quota, ADC](gcp-deploy.md#1-kiểm-trước-khi-bắt-đầu).

- [ ] **1 — Tạo máy và hai kho.** Điền project/region/zone trong tfvars, giữ
  `force_destroy=false`; đọc plan trước apply, chỉ tiếp tục khi đúng tài nguyên.

  ```bash
  cp infra/gcp/terraform/terraform.tfvars.example infra/gcp/terraform/terraform.tfvars
  "${EDITOR:-nano}" infra/gcp/terraform/terraform.tfvars
  terraform -chdir=infra/gcp/terraform init
  terraform -chdir=infra/gcp/terraform validate
  terraform -chdir=infra/gcp/terraform plan -out=de.tfplan
  terraform -chdir=infra/gcp/terraform show de.tfplan
  terraform -chdir=infra/gcp/terraform apply de.tfplan
  mkdir -p out/gcp-evidence
  terraform -chdir=infra/gcp/terraform output -json > out/gcp-evidence/terraform-outputs.json
  export VM_NAME="$(terraform -chdir=infra/gcp/terraform output -raw vm_name)"
  export VM_ZONE="$(terraform -chdir=infra/gcp/terraform output -raw zone)"
  rm infra/gcp/terraform/de.tfplan
  ```

  **Đạt:** một VM E2, hai bucket private, data disk; lưu đủ outputs, không xóa/replace tài nguyên cũ.
  **Nếu sai:** [plan và outputs](gcp-deploy.md#2-tạo-hạ-tầng-đọc-plan-và-output).

- [ ] **2 — Điền phiếu cấu hình Ansible.** SSH lần đầu tạo key OS Login; lấy username
  từ `posixAccounts` (nếu chưa có, làm theo quyền OS Login của runbook).

  ```bash
  gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap --command='whoami'
  gcloud compute os-login describe-profile --project="$PROJECT_ID" --format='yaml(posixAccounts)'
  cp infra/gcp/ansible/inventory.example.ini infra/gcp/ansible/inventory.ini
  cp infra/gcp/ansible/group_vars/all.example.yml infra/gcp/ansible/group_vars/all.yml
  "${EDITOR:-nano}" infra/gcp/ansible/inventory.ini infra/gcp/ansible/group_vars/all.yml
  ansible-galaxy collection install -r infra/gcp/ansible/requirements.yml
  ansible-vault encrypt infra/gcp/ansible/group_vars/all.yml
  ```

  | Output / nguồn thật | Điền vào đâu |
  |---|---|
  | `vm_name`, `zone` | inventory: `ansible_host=VM_NAME`, `ZONE` trong ProxyCommand |
  | Project ID vừa chọn, username OS Login | inventory: `PROJECT_ID`, `ansible_user=OS_LOGIN_USER`; all.yml: `gcp_project` |
  | `raw_bucket`, `lakehouse_bucket`, `service_account_email` | all.yml: ba biến cùng tên, bucket không có `gs://` |
  | `raw_bucket_uri`, `lakehouse_bucket_uri` | ghi bằng chứng; source là URI raw + `/order_lifecycle/fixture`, warehouse là URI lakehouse + `/lakehouse` |
  | Không có output device; `compute.tf`: `device_name = "fsds-data"` | all.yml: `data_disk_name: fsds-data` |
  | Đường dẫn repo trên máy cá nhân | all.yml: `repo_local: <ĐƯỜNG_DẪN_TUYỆT_ĐỐI>`; giữ `app_dir: /opt/fsds` |
  | Tự sinh secret | all.yml: `postgres_password` ≥16, `lakekeeper_encryption_key` ≥32 ký tự chữ/số/`_`/`-`; giữ key qua các lần chạy |
  | `ssh_command`, `port_forward_command` | lệnh tham khảo; tunnel UI dùng bước 6 để có cổng Airflow 8081 |

  **Đạt:** không còn placeholder; `source_mode: fixture`, `source_dataset_name: fixture`, file bí mật đã Vault.
  **Nếu sai:** [inventory và Vault](gcp-runtime.md#1-điền-cấu-hình-ở-máy-cá-nhân).

- [ ] **3 — Cài rồi thử ba số.** Trên máy cá nhân:

  ```bash
  ansible-playbook -i infra/gcp/ansible/inventory.ini infra/gcp/ansible/site.yml --ask-vault-pass
  ansible-playbook -i infra/gcp/ansible/inventory.ini infra/gcp/ansible/site.yml --tags smoke --ask-vault-pass
  ```

  **Đạt:** recap `failed=0 unreachable=0`; smoke ghi/đọc ba số trên GCS, xóa đúng bảng thử, thấy DAG v2.
  **Nếu sai:** [cài runtime](gcp-runtime.md#2-cài-và-khởi-động-runtime), [smoke](gcp-runtime.md#4-chạy-smoke).

- [ ] **4 — Nghiệm thu nhỏ: sinh → chạy → đọc.** Máy cá nhân sinh/upload/nạp Walmart:

  ```bash
  ansible-playbook -i infra/gcp/ansible/inventory.ini infra/gcp/ansible/site.yml --tags source_data --ask-vault-pass
  gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap
  ```

  **VM**, copy SQL rồi trigger pipeline với generation mới; đợi Grid xanh đủ 5 task
  trước khi trigger warehouse. Dùng đúng `<RUN_PIPELINE_MỚI>` ở cả hai lệnh:

  ```bash
  cd /opt/fsds
# SQL đã mount read-only tại /opt/sql/warehouse_lifecycle.
  docker compose --profile airflow exec -T airflow airflow dags unpause dp_order_lifecycle
  docker compose --profile airflow exec -T airflow airflow dags trigger dp_order_lifecycle --run-id '<RUN_PIPELINE_MỚI>' --conf '{"source_root":"gs://<RAW_BUCKET>/order_lifecycle/fixture","cutoff":"2026-06-01T00:00:00.000000Z","walmart_jdbc":"walmart_lifecycle.orders"}'
  # Chỉ chạy hai lệnh tiếp sau khi pipeline thành công.
  docker compose --profile airflow exec -T airflow airflow dags unpause dp_order_lifecycle_warehouse
  docker compose --profile airflow exec -T airflow airflow dags trigger dp_order_lifecycle_warehouse --run-id '<RUN_WAREHOUSE_MỚI>' --conf '{"run_id":"<RUN_PIPELINE_MỚI>","cutoff":"2026-06-01T00:00:00.000000Z"}'
  sudo find /mnt/fsds-data/airflow/receipts/v2 -name '*.json' -print
  sudo cat /mnt/fsds-data/airflow/receipts/v2/<TS_PIPELINE>/*.json /mnt/fsds-data/airflow/receipts/v2/warehouse/<TS_WAREHOUSE>/*.json
  ```

  | Receipt / log fixture mặc định | Phải đạt |
  |---|---|
  | `check_contracts.json` | `status=passed`, `exit_code=0` |
  | `ingest_bronze.json` + log Bronze | `status=passed`, `checks_available=false`; log: manifest=Bronze=550, file=547, JDBC=3, invalid=0; Walmart missing/extra/duplicate=0 |
  | `build_silver.json` | mọi `checks`=0; `info.lines=30`, `native_lines=30`, `allocations=45`, `headline_product_net_minor=750000`; `quarantine.total=0` |
  | `build_gold.json`, cả hai `validate_gold.json` | `status=passed`, mọi `checks`=0, `info.fact_rows=30` |
  | `export_warehouse.json` | `status=passed`, mọi `checks` là **true**; mỗi bảng fact/label/feature: `tables.<tên>.gold.rows=postgres.rows=30`; `fact.money_by_channel_currency=true` |

  `run_id` trong receipt phải là generation pipeline; cutoff là cùng thời điểm UTC
  2026-06-01. Bronze/contracts không có cutoff; đừng tự thêm. Không clear/retry Gold đã
  append generation; dùng run mới. Số fixture lấy từ [Silver local](silver-order-lifecycle.md#kết-quả)
  và [receipt local](evidence/silver-v2-receipt-20261018.json); receipt cũ chưa có quarantine,
  receipt hiện tại phải kiểm trường này. **Đạt:** toàn bộ bảng trên đúng, cả hai DAG xanh.
  **Nếu sai:** [trigger](gcp-runtime.md#5-mở-airflow-qua-iap-và-trigger-pipeline),
  [receipt/log](gcp-runtime.md#6-đọc-bằng-chứng-rồi-tắt-vm); SQL/export xem [warehouse](warehouse-order-lifecycle.md#chạy-local-và-trên-vm).

- [ ] **5 — Nghiệm thu scale, EXPLAIN và DE-N2 trên bảng thật.** Thoát SSH bằng
  `exit`. Máy cá nhân chạy nguồn scale bằng role đã gọi `generator.generate_order_lifecycle`:

  ```bash
  ansible-playbook -i infra/gcp/ansible/inventory.ini infra/gcp/ansible/site.yml --tags source_data --ask-vault-pass -e '{"source_mode":"scale","source_dataset_name":"scale-2000x30-s7","source_seed":7,"source_days":30,"source_orders_per_day":2000,"source_followup_days":270,"source_observation_days":285}'
  ```

  Chạy lại **hai lệnh trigger ở bước 4** trên VM với run ID mới, đổi source_root thành
  `gs://<RAW_BUCKET>/order_lifecycle/scale-2000x30-s7` và **cả hai cutoff** thành
  `2026-10-13T00:00:00.000000Z` (hết observation 285 ngày từ 01/01; giữ cutoff fixture sẽ bỏ
  sự kiện follow-up). Đợi từng DAG xong. Local đã đo 60.000 đơn / 97.890 line /
  1.061.702 bản ghi / 1.546.066.142 byte; đối chiếu manifest thật, không coi đó là số đo Cloud.
  Scale vẫn phải có mọi check Silver/Gold=0, quarantine=0 và warehouse checks=true,
  count PostgreSQL bằng Gold; không áp số 30/550 của fixture.

  **VM**, lấy SKU/dataset/feature clock thật từ PostgreSQL rồi đo:

  ```bash
  cd /opt/fsds
  set -a
  source .env
  set +a
  docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT dataset_version,seller_sku,event_timestamp,created FROM warehouse_lifecycle.feat_sku_return_rate_90d ORDER BY event_timestamp DESC LIMIT 5;'
  POSTGRES_HOST=127.0.0.1 /mnt/fsds-data/venv/bin/python scripts/evidence/measure_order_lifecycle_warehouse.py --cutoff 2026-10-13T00:00:00Z --sku '<SKU_THẬT>' --dataset '<DATASET_THẬT>' --event-timestamp '<EVENT_TIMESTAMP_UTC_THẬT>' --as-of '<THỜI_ĐIỂM_UTC_KHÔNG_TRƯỚC_CREATED>' --receipt /mnt/fsds-data/airflow/receipts/v2/warehouse-scale-explain.json
  ```

  DE-N2 dùng bảng fact thật (có `seller_sku`, `created_at`), giữ orphan dry-run,
  không chạy song song writer; CLI dùng 3g thay DAG maintenance còn mặc định 600m:

  ```bash
  docker compose --profile airflow exec -T airflow python - <<'PY'
  import json
  from airflow.models import Variable
  config = json.loads(Variable.get('fsds_maintenance_config'))
  policy = config['tables']['lakehouse.demo.maintenance_demo']
  config['tables'] = {'lakehouse.gold.lifecycle_fact_order_lines': policy}
  policy['orphans_dry_run'] = True
  policy['probe_predicates'] = [{'name': 'sku-thuc', 'ranges': {'seller_sku': ['<SKU_THẬT>', '<SKU_THẬT>']}}]
  Variable.set('fsds_maintenance_config', json.dumps(config))
  PY
  docker compose --profile airflow exec -T airflow bash -s <<'SH'
  set -euo pipefail
  config="$(airflow variables get fsds_maintenance_config)"
  packages="$(airflow variables get fsds_spark_packages)"
  spark_args=(--master spark://spark-master:7077 --driver-memory 3g --executor-memory 4g --executor-cores 2 --conf spark.driver.host=airflow --conf spark.driver.bindAddress=0.0.0.0 --conf spark.jars.ivy=/opt/airflow/.ivy2 --packages "$packages")
  receipt_dir=/opt/airflow/receipts/order_lifecycle/de-n2-scale
  mkdir -p "$receipt_dir"
  for mode in plan-only execute; do
    spark-submit "${spark_args[@]}" /opt/spark_jobs/maintenance/maintain_iceberg_tables.py --"$mode" --tables '["lakehouse.gold.lifecycle_fact_order_lines"]' --config-json "$config" --receipt "$receipt_dir/$mode.json"
  done
  spark-submit "${spark_args[@]}" /opt/spark_jobs/maintenance/maintain_iceberg_tables.py --validate-receipt "$receipt_dir/execute.json" | tee "$receipt_dir/validate.log"
  SH
  ```

  **Đạt:** EXPLAIN có `repeats=5`, hai `phases.pk_only/indexed`, đủ ba query; DE-N2
  execute `status=success`, validate in `status=validated`, count/checksum/schema và tag trước=sau, validate exit 0;
  ghi số file/probe/snapshot thật, không ép phải giảm nếu policy không có ứng viên.
  **Nếu sai:** [nguồn](gcp-runtime.md#3-sinh-nguồn-và-nạp-walmart),
  [runtime/RAM](gcp-runtime.md#2-cài-và-khởi-động-runtime);
  chi tiết [scale](source-order-lifecycle-scale.md#kết-quả-đo), [EXPLAIN](warehouse-order-lifecycle.md#explain-trướcsau-trên-cùng-dữ-liệu),
  [DE-N2](novel-de-n2-iceberg-maintenance.md#hậu-kiểm-và-cách-đo).

- [ ] **6 — Mang bằng chứng về.** **VM:** chụp log Bronze của từng run qua UI,
  gom receipt/manifest thành file người SSH đọc được (không gom `.env`/password).

  ```bash
  sudo tar -czf /tmp/gcp-v2-evidence.tgz -C /mnt/fsds-data \
    airflow/receipts/v2 \
    airflow/receipts/order_lifecycle/de-n2-scale \
    source/fixture/manifest.json \
    source/scale-2000x30-s7/manifest.json
  sudo tar -tzf /tmp/gcp-v2-evidence.tgz
  sudo chown "$(id -u):$(id -g)" /tmp/gcp-v2-evidence.tgz
  exit
  ```

  **Máy cá nhân**, cùng terminal đã có biến project/VM/zone:

  ```bash
  gcloud compute scp --tunnel-through-iap --project="$PROJECT_ID" --zone="$VM_ZONE" "$VM_NAME:/tmp/gcp-v2-evidence.tgz" out/gcp-evidence/
  mkdir -p out/gcp-evidence/unpacked
  tar -xzf out/gcp-evidence/gcp-v2-evidence.tgz -C out/gcp-evidence/unpacked
  gcloud compute ssh "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --tunnel-through-iap -- -N -o ExitOnForwardFailure=yes -L 127.0.0.1:8081:127.0.0.1:8081 -L 127.0.0.1:8080:127.0.0.1:8080 -L 127.0.0.1:8181:127.0.0.1:8181
  ```

  Chụp Airflow `localhost:8081` Grid/Graph cả hai DAG (run ID/thời lượng/task),
  Spark master `localhost:8080` jobs/workers khi chạy, Lakekeeper `localhost:8181`
  warehouse GCS, GCS Console raw/lakehouse (prefix/bytes). Cổng driver 4040 chưa
  publish trong overlay; không coi tunnel 4040 là dùng được.
  Archive trên áp dụng sau khi đã chạy đủ fixture, scale và DE-N2; thiếu một
  đường dẫn thì tar lỗi, cần kiểm bước chưa hoàn tất trước khi bàn giao.
  Giữ cả `schemas.json` cạnh receipt để DataHub có schema của run thực.
  Log Bronze/JDBC, report skew và Spark event log cần thu riêng nếu đã chạy;
  không mặc định chúng có trong archive. Publish/capture DataHub theo
  [receipt GCP](gcp-runtime.md#7-publish-datahub-từ-receipt-gcp).
  **Đạt:** archive mở được, ảnh thấy run/bucket thật, không lộ secret.
  **Nếu sai:** [IAP/UI](gcp-deploy.md#4-mở-ssh-và-ui-qua-iap), [receipt](gcp-runtime.md#6-đọc-bằng-chứng-rồi-tắt-vm).

- [ ] **7 — TẮT VM NGAY KHI XONG.** Chờ job/upload xong và lưu/kiểm backup DB/catalog
  theo [chuẩn bị tắt](gcp-deploy.md#5-bật-khi-cần-tắt-khi-xong); Ctrl-C đóng tunnel.
  **Máy cá nhân:**

  ```bash
  gcloud compute instances stop "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE"
  gcloud compute instances describe "$VM_NAME" --project="$PROJECT_ID" --zone="$VM_ZONE" --format='value(status)'
  ```

  **Đạt:** `TERMINATED`; ghi giờ bật/tắt và số giờ thực chạy.
  Dự toán = `giờ chạy × giá VM/giờ + giờ IP × giá IP/giờ + 250 GiB × giá disk/GiB-tháng × giờ giữ/730 + GCS + requests + egress`.
  Lấy đơn giá từ Calculator/Billing của project trước buổi học; disk/GCS vẫn tính khi VM tắt.
  **Nếu sai:** [stop/start](gcp-deploy.md#5-bật-khi-cần-tắt-khi-xong), [dự toán](gcp-deploy.md#7-dự-toán-trước-mỗi-lần-chạy).

- [ ] **8 — Dọn sạch khi xong đồ án.** Chỉ sau khi kiểm bản sao bằng chứng/backup:

  ```bash
  terraform -chdir=infra/gcp/terraform plan -destroy
  terraform -chdir=infra/gcp/terraform destroy
  ```

  **Đạt:** tài nguyên trong state đã xóa và Console không còn VM/disk/bucket của đồ án.
  **Nếu sai:** bucket còn object/versions sẽ chặn destroy, làm đúng
  [dọn sạch](gcp-deploy.md#6-dọn-sạch-khi-không-dùng-nữa); destroy vẫn xóa data disk.

**Dán cho planner:** `out/gcp-evidence/terraform-outputs.json`,
`out/gcp-evidence/gcp-v2-evidence.tgz` (đủ 5 receipt pipeline + 2 warehouse cho
**mỗi** fixture/scale, `schemas.json`, hai manifest, EXPLAIN JSON,
DE-N2 plan/execute JSON và `validate.log`),
log Bronze/JDBC từng run, output validate DE-N2 và recap Ansible/smoke,
ảnh Grid/Graph/Spark/Lakekeeper/GCS, bảng run ID/cutoff/thời lượng/rows/bytes,
credit/hạn/đơn giá/giờ chạy/chi phí Billing thực và output `TERMINATED`.
Không gửi state, tfplan, tfvars, inventory, Vault, `.env`, password hay ADC.
**HẾT BUỔI: KIỂM LẠI VM ĐÃ `TERMINATED`.**
