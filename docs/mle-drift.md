<!-- Mục đích: Novel idea ML-N2 — theo dõi data drift đầu vào model bằng Evidently, có proof chạy được. -->
# ML-N2: theo dõi data drift (Evidently)

Rubric `mle` dòng 56–57 (Novel ideas: công cụ chưa dạy ở EDAI + proof). Evidently so phân phối đầu
vào của một lô mới với lô tham chiếu (giai đoạn train) và cảnh báo khi lệch — tín hiệu để kiểm lại
model **trước** khi tin score, ví dụ khi có đợt Sale làm đổi cơ cấu SKU.

| Thành phần | File |
|---|---|
| Kiểm drift + tóm tắt | [`ml/drift.py`](../ml/drift.py), test [`tests/unit/test_drift.py`](../tests/unit/test_drift.py) |
| Lô "đợt Sale" | `generator.generate_order_lifecycle --sku-zipf-s 0.3` (SKU khuyến mãi dàn đều) |
| Kết quả | [`evidence/mle-drift-summary-20261021.json`](evidence/mle-drift-summary-20261021.json), HTML ở `out/drift/` |

## Cách đo

- Cột: `seller_sku`, `label`, đều là cột phân loại. Phương pháp: khoảng cách **Jensen-Shannon**.
- Ngưỡng: `seller_sku` 0,2 (≈100 giá trị, nhiễu lấy mẫu của ~1.000 dòng đã đo được 0,14);
  `label` 0,1 (mặc định).
- `alert` = có ít nhất một cột drift.

**Bẫy đã gặp:** trên 1.000 dòng Evidently tự đổi từ chi-square (p-value, **nhỏ** = drift) sang
Jensen-Shannon (khoảng cách, **lớn** = drift). Bản đầu đọc mọi số như p-value ⇒ kết luận ngược hẳn
(báo `label` drift, bỏ sót `seller_sku`). Đã đối chiếu bằng scipy chi-square rồi sửa: chiều so lấy
theo tên phương pháp trong metric; test phủ cả hai chiều và biên ngưỡng.

## Proof (2026-10-21)

| Lô so với tham chiếu (30 ngày đầu, 2.307 line) | `seller_sku` | `label` | alert |
|---|---|---|---|
| 15 ngày sau, cùng phân phối (1.165 line) | 0,136 | 0,005 | **không** |
| Đợt Sale 5 ngày, zipf 0,3 (1.228 line) | **0,436** | 0,001 | **có** |

Đối chiếu tay: SKU đứng đầu chiếm 27,9% lô tham chiếu, chỉ 3,3% trong đợt Sale; tỷ lệ trả gần như
giữ nguyên (0,299 / 0,306 / 0,300) — khớp việc chỉ `seller_sku` drift.

## Chạy

```bash
uv run --no-project --with evidently==0.7.23 --with s3fs==2026.9.0 --with pyarrow \
  env PYTHONPATH=. AWS_ENDPOINT_URL_S3=http://localhost:9000 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  python -m ml.drift --reference s3://fsds/feature-store/training_lines \
    --sale-truth out/order_lifecycle_sale/_truth/lines.jsonl --out out/drift
```

<!-- Ảnh hai báo cáo HTML (stable.html / sale_event.html): người học chụp rồi gắn vào đây. -->
