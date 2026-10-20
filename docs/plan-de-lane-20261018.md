# DE lane — 18/10/2026

Hạn **31/10**, giữ **Q52–Q61**. Lượt này làm **B**, không commit, không Cloud,
không sửa các file gói A hoặc `docs/evidence/*`.

- Đã thêm `generator/generate_order_lifecycle_fixture.py`: world/envelope/authority/capability, identity
  tuple/retry/revision/causal refs, quantity/money/cancel, Listing/stock/inbound,
  coverage/uncertainty và ba horizon có bounds. Có đủ5 Channel và feed Q52/Q53/Q54/Q61.
- Root bàn giao mới: `out/source-v2-b-final-20261018-051930425055/`.
  Ba biến thể nhỏ (default/split-schema2/approvalD31), mỗi bộ3 ngày tạo Order,
  2 SKU,15 Orders/30 lines; tổng JSONL2.511.009 byte. Không resume/restart root cũ.
- Test-first đỏ→xanh: `tests/unit/generator/test_generate_order_lifecycle_fixture.py` **12 passed**, gồm kiểm emitted
  sources/checksums, partial lifecycle, tiền/stock, mapping/bindings, revisions,
  late coverage/uncertainty, delay≤15d, thiếu partition/capability và same-world.
  Toàn suite cuối lượt: `.venv/bin/pytest -q` — **194 passed**.
- Còn: C/D/E/J ingest Bronze/Silver/JDBC, projection/corrections/reconciliation,
  official label/eligibility, stream recovery và publication. Walmart hiện chỉ
  bootstrap JSONL, chưa là proof JDBC. Không claim full-scale/Cloud/official labels.
  Cách chạy và giới hạn: [source-order-lifecycle-fixtures.md](source-order-lifecycle-fixtures.md).

## Gói C bước 1 — Claude, 18/10

Bronze chung `bronze.lifecycle_raw_records` và 12 bảng `silver.lifecycle_stg_*` cho
fixture `default`, kèm 12 check đối soát. Test Spark 17/17, chạy thật Bronze
550 = 550 và Silver 12/12 check bằng 0. Chi tiết, lệnh và giới hạn:
[silver-order-lifecycle.md](silver-order-lifecycle.md). Còn của C: parse đơn native 5 Channel và đối soát
chéo với OMS, áp coverage receipt trong job, Walmart JDBC, DAG/DataHub.

## Gói C bước 2–3 — Claude, 18/10

Parse đơn native 5 Channel (Amazon hai schema) và đối soát chéo với OMS; kiểm
coverage của producer ngay trong job (số thứ tự, số dòng, khoảng, hash cả tập);
Walmart nạp PostgreSQL `walmart_lifecycle.orders` rồi vào Bronze qua JDBC. Test Spark
27/27, pytest 197; chạy thật Bronze 550 (JDBC 3) và Silver 19/19 check bằng 0.
**Gói C xong ở mức fixture nhỏ.** Chưa có DAG/DataHub cho bảng v2 (gói I) và
Gold/nhãn v2 (gói E). Chi tiết: [silver-order-lifecycle.md](silver-order-lifecycle.md).
