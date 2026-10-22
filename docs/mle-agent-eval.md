<!-- Mục đích: Novel idea ML-N1 — eval harness cho agent + cổng hồi quy; proof chạy được trên hai nhánh A/B. -->
# ML-N1: eval harness cho agent + cổng hồi quy

Rubric `mle` dòng 56–57 (Novel ideas + proof). Câu hỏi "agent có trả lời **đúng số thật** không?"
không đo được bằng độ trễ hay số token. Harness hỏi agent một bộ câu cố định qua agentgateway (A2A),
lấy **đáp án chuẩn từ Feature API** (chính nguồn agent phải gọi tool), chấm từng câu, rồi dùng độ chính
xác làm cổng chặn trong CI: dưới ngưỡng ⇒ thoát mã 1 ⇒ không deploy bản agent/model mới.

| Thành phần | File |
|---|---|
| Harness + chấm điểm | [`ml/agent_eval.py`](../ml/agent_eval.py), test [`tests/unit/test_agent_eval.py`](../tests/unit/test_agent_eval.py) |
| Route chấm riêng nhánh B | `a2a-data-agent-b-path` trong [`agentgateway.yaml`](../infra/k8s/agents/agentgateway.yaml) |
| Kết quả | [`evidence/agent-eval-20261021.jsonl`](evidence/agent-eval-20261021.jsonl) |

## Luật chấm

Câu "What are the return features of sku-…?" **đạt** khi câu trả lời chứa đủ: `labeled_count_90d`,
`positive_count_90d` (số nguyên đúng) và `return_or_refund_rate_90d` (lệch ≤ 0,005; nhận cả `0.33`, `33%`).
Mã SKU (`sku-00010`) không được tính là số. SKU chưa có lịch sử không chấm (tránh agent nói "0" là đạt).
Lỗi HTTP/timeout của agent = câu trượt, harness không dừng.

**Chuẩn bị dữ liệu**: ảnh chụp online mới nhất (13/10) có mọi số = 0 ⇒ không phân biệt được ai lấy số thật.
Lúc eval, Redis được nạp lại tới mốc 15/03 (Feast không ghi đè giá trị mới bằng giá trị cũ ⇒ phải
`flushall` trước), xong khôi phục về 13/10.

## Proof (2026-10-21, 12 SKU, cùng câu hỏi, cùng tool)

| Agent | Model | Đạt | p50 |
|---|---|---|---|
| `data-agent` (A) | qwen2.5-1.5b | **12/12 = 100%** | 5,4 s |
| `data-agent-b` (B) | qwen2.5-0.5b | **11/12 = 91,7%** | 3,0 s |

Câu B trượt (sku-00005): nói `labeled_count_90d = 18` sai và tỷ lệ 0,085 không khớp 10/18 — **bịa số**,
loại lỗi mà dashboard độ trễ/token không bao giờ thấy.

Cổng: chạy B với `--min-accuracy 0.95` → `accuracy 0.917` → **exit 1** (bị chặn); A với 0,6 → exit 0.

## Dùng trong CI

Sau bước deploy agent (Jenkinsfile `feature-api-agents`), thêm:

```bash
eval "$(scripts/setup/vault_env.sh agentgateway)"
uv run python -m ml.agent_eval --agent data-agent --skus sku-00001,…,sku-00012 --min-accuracy 0.95
```
