<!-- Mục đích: pipeline RAG text → chunk → embedding → Feast (pgvector), -->
# RAG pipeline — LLM Platform

Pipeline kéo dữ liệu text → chia chunk → lưu embedding của chunk **vào feature store qua Feast**;
Governance pipeline RAG (lineage + validation trên DataHub).

```
rag/corpus/*.md (chính sách trả hàng 5 Channel + playbook Seller, giả lập)
   │ upload
   ▼
s3://platform/rag/raw/*.md ──chunk──► s3://platform/rag/chunks_text.parquet
                                     │ embed (fastembed BAAI/bge-small-en-v1.5, 384 chiều, chuẩn hoá)
                                     ▼
                         s3://platform/rag/chunks.parquet   ← offline source của Feast `platform_rag`
                                     │ load: feast apply + write_to_online_store
                                     ▼
              Postgres + pgvector (kind ns rag)  bảng platform_rag_policy_chunks   ← online store
                                     │ validate → receipt → publish (DataHub)
                                     ▼
       Feature API: POST /chunks, MCP tool search_policy_chunks → retrieve_online_documents_v2 (cosine)
```

| Phần | File |
|---|---|
| Tài liệu nguồn (giả lập, có ghi chú ở đầu mỗi file) | [`rag/corpus/`](../rag/corpus/seller-return-playbook.md) |
| Cắt chunk + embedding | [`rag/chunking.py`](../rag/chunking.py) |
| Các bước pipeline + validate + lineage DataHub | [`rag/pipeline.py`](../rag/pipeline.py) |
| Feast thứ hai (online store Postgres/pgvector) | [`rag/feature_repo/features.py`](../rag/feature_repo/features.py), [`feature_store.yaml`](../rag/feature_repo/feature_store.yaml) |
| DAG Airflow | [`dags/ml_rag_ingest.py`](../dags/ml_rag_ingest.py) |
| pgvector trong kind | [`infra/k8s/rag/pgvector.yaml`](../infra/k8s/rag/pgvector.yaml) |
| Test (chunk, lineage) | [`rag/tests/`](../rag/tests/test_chunking.py) |

## Vì sao Feast thứ hai

Feast của feature SKU dùng Redis làm online store; Redis không tìm theo vector. Feast hỗ trợ tìm vector với
online store Postgres + pgvector (`vector_enabled: true`, field `Array(Float32)` có `vector_index=True`,
`vector_search_metric="cosine"`). Tách project `platform_rag` (registry `s3://platform/feature-store/rag-registry.db`)
để không đụng Feast SKU đang chạy.

## Chunk

Mỗi mục `## ` của tài liệu là một chunk, đầu chunk ghi "Tiêu đề tài liệu — Tiêu đề mục:" để chunk tự đủ nghĩa
khi đứng một mình. Mục dài hơn 700 ký tự cắt theo câu, gối 100 ký tự. `chunk_id` = băm nội dung ⇒ chạy lại
ghi đè đúng dòng cũ, không nhân bản (kiểm: chạy `load` 2 lần, online vẫn 29 dòng).

## Validate (mỗi check = số vi phạm, 0 là đạt)

| Check | Ý nghĩa |
|---|---|
| `online_chunk_count_mismatch` | số entity trong bảng pgvector ≠ số chunk Parquet |
| `duplicate_chunk_ids` | trùng `chunk_id` |
| `empty_text` | chunk rỗng |
| `wrong_dimension` | vector khác 384 chiều |
| `not_unit_norm` | vector chưa chuẩn hoá (cosine sai) |
| `probe_wrong_top_doc` | 3 câu hỏi mẫu không ra đúng tài liệu ở vị trí đầu |

Receipt ghi vào `out/receipts/rag/<run_id>/validate.json`; bước `publish` biến mỗi check thành assertion
DataHub, kèm dataset (S3 raw, Parquet, bảng pgvector), job theo task DAG và cạnh lineage.

## Kết quả local 2026-10-21

Chạy từng bước trong container Python 3.10 (cùng phiên bản với Airflow) nối MinIO + pgvector thật:
29 chunk từ 6 tài liệu, **6/6 check = 0**, 3 câu hỏi mẫu ra đúng tài liệu
([receipt](evidence/rag-validate-local-20261021.json)).

Tìm thử "Shopee buyer says the parcel was damaged, what can I do?" → 3 chunk của `shopee-returns`
(Common reasons 0,242 · Dispute flow 0,298 · Return window 0,311).

Lỗi đã gặp và sửa:
- Feast 0.66 không nhận khoá `vector_len` trong `feature_store.yaml` (độ dài khai ở Field).
- Feast mặc định `sslmode=require`; pgvector local không bật TLS ⇒ `sslmode: disable` (Cloud đổi lại).
- `retrieve_online_documents_v2` của Postgres chọn đúng top_k nhưng trả về không theo thứ tự ⇒
  `search()` tự sắp theo `distance`.

## Chạy qua Airflow 2026-10-21

Image Airflow build lại: feast-venv thêm `feast[postgres]` + `fastembed==0.8.1` (cài bằng uv; pip mất >10 phút giải phụ
thuộc), Python giữ 3.10 cho Spark. `scripts/setup/setup_airflow.sh` nạp Connection `platform_rag_pg` và Variable `platform_rag_root`.
Run `rag_local_202610211739`:

| Task | Trạng thái | Thời gian |
|---|---|---|
| upload | success | 2 s |
| chunk | success | 2 s |
| embed | success | 13 s (model nằm trong volume Airflow sau lần đầu) |
| load | success | 26 s (`feast apply` + `write_to_online_store`) |
| validate | success | 11 s — 6/6 check = 0, 3 câu hỏi mẫu ra đúng tài liệu |
| publish | skipped | chưa đặt Variable `platform_datahub_gms_url` (DataHub để lên GCP) |

Receipt: [`evidence/rag-validate-airflow-20261021.json`](evidence/rag-validate-airflow-20261021.json). CI/CD:
[`ci/jenkins/rag-pipeline.Jenkinsfile`](../ci/jenkins/rag-pipeline.Jenkinsfile).

Còn lại cho GCP: chạy DataHub, đặt `platform_datahub_gms_url`, chạy lại DAG để `publish` đẩy dataset/job/lineage/assertion; ảnh Graph/Grid Airflow và Lineage DataHub.
