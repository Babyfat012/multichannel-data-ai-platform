# Link và ghi chú hướng dẫn trong rubric DE/MLE

Ngày trích: 2026-10-18. Nguồn duy nhất: `EDAI K11 - MLE.xlsx` (khóa học cung cấp, không phân phối trong repo), SHA256 `ef5a69e8621b2959c998dd061616f6ce69a06adc2867519751fb4f2a5845683f`.

Đọc trực tiếp ZIP/OOXML bằng Python standard library (`zipfile`, `xml.etree.ElementTree`): workbook/sheet relationships, cell hyperlinks (`r:id`, `location`), shared strings/rich text, inline strings, công thức `HYPERLINK`, URL trong chữ, drawing/shape hyperlinks và comment/notes parts. Không truy cập hoặc xác minh các website; không chạy pipeline, không thay đổi workbook hoặc code.

| Sheet | Trạng thái | OOXML | Phạm vi |
| --- | --- | --- | --- |
| `de` | visible | `xl/worksheets/sheet1.xml` | Đã trích |
| `rubic final-coursework (final -` | hidden | `xl/worksheets/sheet2.xml` | Không thuộc hai sheet được yêu cầu; không tính link nội dung sheet này |
| `mle` | visible | `xl/worksheets/sheet3.xml` | Đã trích |

Có **22 cell hyperlink**: `de` 2, `mle` 20, với **17 `Target` khác nhau**. Hai link có thêm `location` (fragment). Ngoài ra chữ ở `mle!C30` chứa một URL khác target bị lỗi dấu câu: tổng **23 bản ghi URL/target tại ô, 18 chuỗi URL/target khác nhau** khi tính cả URL trong chữ. Không gộp những link giống nhau xuất hiện ở các ô khác nhau. Bảng ghi chú bên dưới có **19 câu/cụm hướng dẫn**, trong đó **6 trường hợp thiếu target riêng hoặc chưa xác định được target đúng**; không tính chúng thành URL.

Tên mục lấy từ nội dung hàng và ô gộp của rubric; vị trí trong bảng luôn là ô thực sự chứa hyperlink, không phải tất cả hàng nằm trong vùng gộp.

## 1. Toàn bộ hyperlink được lưu trong workbook

`Target` dưới đây giữ nguyên giá trị OOXML, kể cả dấu câu có vẻ sai. Cột `location` cũng giữ nguyên; URL truy cập tương ứng sẽ là `Target#location` khi có fragment. Không tự sửa target trong bảng này.

| # | Sheet | Ô / dòng | Tên mục rubric | Chữ hiển thị / trích đoạn | Target nguyên bản | location | Loại |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | de | A2 / 2 | Lưu ý README.md và documentation | `Lưu ý về README.md` | `http://readme.md/` | — | Cell hyperlink, external, rId1 |
| 2 | de | A34 / 34 | Documentation / Schema design | `ví dụ IaC.md thể hiện các output/document của step này` | `http://iac.md/` | — | Cell hyperlink, external, rId2 |
| 3 | mle | A2 / 2 | Lưu ý README.md và documentation | `Lưu ý về README.md` | `http://readme.md/` | — | Cell hyperlink, external, rId1 |
| 4 | mle | A3 / 3 | Deploy LLM inference platform + custom model & agentgateway | `Deploy a LLM inference platform theo hướng dẫn này và set up gateway cho agent theo hướng dẫn này` | `https://llm-d.ai/docs/getting-started/quickstart` | — | Cell hyperlink, external, rId2 |
| 5 | mle | C5 / 5 | Setup agent gateway, security & policy | `Setup agent gateway theo hướng dẫn và thiết lập security & policy phù hợp (trên này có rất nhiều doc để mn tham khảo)` | `https://agentgateway.dev/` | — | Cell hyperlink, external, rId3 |
| 6 | mle | A7 / 7 | Global model config cho Agent | `Deploy 1 global model config để các Agent có thể dùng follow tutorial này` | `https://kagent.dev/docs/kagent/supported-providers/byo-openai` | — | Cell hyperlink, external, rId4 |
| 7 | mle | A8 / 8 | Agent registry | `Deploy registry for agent theo tutorial này` | `https://aregistry.ai/` | — | Cell hyperlink, external, rId5 |
| 8 | mle | C16 / 16 | Web API: FastAPI, Pydantic, healthcheck k8s | `Có sử dụng FastAPI + data validation (với pydantic) + healthcheck for k8s` | `https://kubernetes.io/docs/concepts/workloads/pods/probes/` | — | Cell hyperlink, external, rId6 |
| 9 | mle | C18 / 18 | Deploy MCP tool lên k8s: Helm, rollingupdate, auto fallback | `Deploy in the form of MCP tool to k8s with helm + rollingupdate + auto fallback (see here)` | `https://octopus.com/devops/kubernetes-deployments/kubernetes-rolling-deployment/` | `defining-a-rolling-deployment-in-kubernetes` | Cell hyperlink, external + fragment, rId7 |
| 10 | mle | C19 / 19 | Agent dùng MCP: multi-replica, auto-scale | `Deploy 1 Agent sử dụng MCP tool ở trên với multi-replica và auto-scale sử dụng cái này` | `https://keda.sh/` | — | Cell hyperlink, external, rId8 |
| 11 | mle | C20 / 20 | Agent Sandbox / security | `Configure Agent chạy trong Sandbox để đảm bảo security` | `https://kagent.dev/docs/kagent/examples/agent-sandbox` | — | Cell hyperlink, external, rId9 |
| 12 | mle | B23 / 23 | MCP Gateway (agentgateway) | `Sử dụng agentgateway đã cài đặt ở trên để làm entrypoint cho các MCP Servers` | `https://agentgateway.dev/` | — | Cell hyperlink, external, rId10 |
| 13 | mle | B24 / 24 | A2A Gateway (agentgateway) | `Sử dụng agentgateway đã cài đặt ở trên để làm entrypoint cho các Agents` | `https://agentgateway.dev/` | — | Cell hyperlink, external, rId11 |
| 14 | mle | C25 / 25 | Coordinator Agent: multi-replica, auto-scale | `Deploy 1 Coordinator Agent với multi-replica và auto-scale sử dụng cái này` | `https://keda.sh/` | — | Cell hyperlink, external, rId12 |
| 15 | mle | A27 / 27 | Warm Up / Agent Fleet Architecture | `Cài đặt hệ thống ở chế độ Warm Up cho agent theo hướng dẫn sau` | `https://kagent.dev/docs/kagent/1.x/about/architecture/agent-substrate/` | — | Cell hyperlink, external, rId13 |
| 16 | mle | C29 / 29 | PR Testing: equivalence partitioning, boundary value analysis | `(Cái này chưa được dạy, nhưng đơn giản lắm, cả nhà coi ở đây)` | `https://share.google/aimode/tqb9NTql5vzv4BD9J` | — | Cell hyperlink, external, rId14 |
| 17 | mle | C30 / 30 | PR Testing: mutation testing, score > 80% | `là cái https://mutmut.readthedocs.io/).` | `https://mutmut.readthedocs.io/).` | — | Cell hyperlink, external, rId15; dấu câu nằm trong target |
| 18 | mle | C31 / 31 | PR Testing: idempotency, property-based testing | `You can use the library hypothesis for property-based testing, combine with crosshair backend to find bugs easier.` | `https://hypothesis.readthedocs.io/en/latest/reference/internals.html` | — | Cell hyperlink, external, rId16 |
| 19 | mle | C40 / 40 | External Routing & Gateway / UI test Agent | `UI để test agent (xem tại đây)` | `https://kagent.dev/docs/kagent/getting-started/first-mcp-tool` | `testing-the-agent-with-the-mcp-tool` | Cell hyperlink, external + fragment, rId17 |
| 20 | mle | C43 / 43 | Setup domain & HTTPS cho Web API | `Mọi người có thể tham khảo 2 đồ án sau: cái này và cái này` | `https://github.com/VuHuy-cse-9/prompt-guardrail-ml-system` | — | Cell hyperlink, external, rId18 |
| 21 | mle | B44 / 44 | IaC / Terraform GKE hoặc cloud services | `để ý cách chia folder theo từng service nếu có, ví dụ như sau` | `https://ajdevopssolutions.medium.com/best-practices-for-terraform-directory-layout-from-small-to-large-projects-4d4ff41cf93c` | — | Cell hyperlink, external, rId19 |
| 22 | mle | B55 / 55 | Repository Design | `README.md và documentation follow rule ở ô A2` | `http://readme.md/` | — | Cell hyperlink, external, rId20 |

### URL trong chữ không trùng target của ô

| # | Sheet | Ô / dòng | Tên mục rubric | URL xuất hiện trong chữ | Chữ bao quanh nguyên văn | Đối chiếu |
| --- | --- | --- | --- | --- | --- | --- |
| 23 | mle | C30 / 30 | PR Testing: mutation testing | `https://mutmut.readthedocs.io/` | `(cái này đã dạy ở lớp rồi nha cả nhà, là cái https://mutmut.readthedocs.io/).` | Chữ chứa URL kết thúc ở `/`; hyperlink OOXML lại chứa thêm `).`. Không truy cập để xác minh hay tự ghi đè target. |

## 2. Các câu/cụm dẫn tới hướng dẫn hoặc ví dụ

Đây là inventory câu chỉ dẫn, không phải danh sách URL mới. **Có link** nghĩa là ô có target được liệt kê ở bảng 1; OOXML chỉ gắn link ở mức ô, không xác định chính xác từng cụm chữ nào trong ô là vùng bấm. **Chưa rõ** nghĩa là workbook không lưu được target riêng phù hợp cho lời dẫn; không lấy link ở ô khác để tự điền.

| # | Sheet | Ô / dòng | Tên mục rubric | Ghi chú nguyên văn | Target / kết quả |
| --- | --- | --- | --- | --- | --- |
| N01 | de | A2 / 2 | README / deployment diagram | `Xem ví dụ về diagram của các khoá trước ở đây.` | **Chưa rõ**: ô chỉ có `http://readme.md/`, không có link riêng cho diagram các khoá trước. |
| N02 | mle | A2 / 2 | README / deployment diagram | `Xem ví dụ về diagram của các khoá trước ở đây.` | **Chưa rõ**: ô chỉ có `http://readme.md/`, không có link riêng cho diagram các khoá trước. |
| N03 | mle | A3 / 3 | LLM inference platform | `Deploy a LLM inference platform theo hướng dẫn này` | Có link #4: `https://llm-d.ai/docs/getting-started/quickstart`. Đây là target duy nhất của ô. |
| N04 | mle | A3 / 3 | Agent gateway | `và set up gateway cho agent theo hướng dẫn này` | **Chưa rõ**: không có target thứ hai trong A3; target duy nhất vẫn là quickstart llm-d (#4). C5 có link agentgateway riêng (#5), nhưng đó là occurrence khác. |
| N05 | mle | C5 / 5 | Agent gateway security & policy | `Setup agent gateway theo hướng dẫn và thiết lập security & policy phù hợp (trên này có rất nhiều doc để mn tham khảo)` | Có link #5: `https://agentgateway.dev/`. |
| N06 | mle | A7 / 7 | Global model config | `Deploy 1 global model config để các Agent có thể dùng follow tutorial này, config này sẽ link tới inference platform ở trên thông qua cái agent gateway đề cập ở trên nốt` | Có link #6: tutorial BYO OpenAI của kagent. |
| N07 | mle | A8 / 8 | Agent registry | `Deploy registry for agent theo tutorial này` | Có link #7: `https://aregistry.ai/`. |
| N08 | mle | C18 / 18 | MCP tool deployment / rolling deployment | `Deploy in the form of MCP tool to k8s with helm + rollingupdate + auto fallback (see here)` | Có link #9: Octopus rolling deployment, kèm fragment. |
| N09 | mle | C18 / 18 | Cách deploy MCP tool | `Remember, your MCP tool is not like normal API deployment, check it our how to deploy here.` | **Chưa rõ**: C18 chỉ có target Octopus rolling deployment (#9); không có target riêng cho lời dẫn thứ hai về MCP tool. |
| N10 | mle | C19 / 19 | Agent multi-replica / auto-scale | `Deploy 1 Agent sử dụng MCP tool ở trên với multi-replica và auto-scale sử dụng cái này` | Có link #10: `https://keda.sh/`. |
| N11 | mle | B23 / 23 | MCP Gateway policy | `Thiết lập security & policy phù hợp (trên này có rất nhiều doc để mn tham khảo)` | Có link #12: `https://agentgateway.dev/`. |
| N12 | mle | B24 / 24 | A2A Gateway policy | `Thiết lập security & policy phù hợp (trên này có rất nhiều doc để mn tham khảo)` | Có link #13: `https://agentgateway.dev/`. |
| N13 | mle | C25 / 25 | Coordinator Agent auto-scale | `Deploy 1 Coordinator Agent với multi-replica và auto-scale sử dụng cái này` | Có link #14: `https://keda.sh/`. |
| N14 | mle | A27 / 27 | Warm Up / Agent Fleet Architecture | `Cài đặt hệ thống ở chế độ Warm Up cho agent theo hướng dẫn sau để tối ưu chi phí, đồng thời giảm thiểu thời gian startup. Cái này cũng chính là Agent Fleet Architecture!` | Có link #15: agent-substrate của kagent. |
| N15 | mle | C29 / 29 | Equivalence partitioning / boundary value analysis | `(Cái này chưa được dạy, nhưng đơn giản lắm, cả nhà coi ở đây)` | Có link #16: `https://share.google/aimode/tqb9NTql5vzv4BD9J`. |
| N16 | mle | C40 / 40 | UI test Agent | `UI để test agent (xem tại đây)` | Có link #19: first-mcp-tool của kagent, kèm fragment. |
| N17 | mle | C43 / 43 | Domain / HTTPS, đồ án tham khảo thứ nhất | `Mọi người có thể tham khảo 2 đồ án sau: cái này và cái này` | Ô có một link #20: GitHub `VuHuy-cse-9/prompt-guardrail-ml-system`. **Chưa rõ** target này thuộc “cái này” thứ nhất hay thứ hai vì hyperlink được lưu ở mức ô. |
| N18 | mle | C43 / 43 | Domain / HTTPS, đồ án tham khảo còn lại | `Mọi người có thể tham khảo 2 đồ án sau: cái này và cái này` | **Chưa rõ**: chỉ một target cho hai đồ án; không có target riêng thứ hai trong workbook. |
| N19 | mle | B44 / 44 | Terraform directory layout | `Dùng Terraform để setup GKE hoặc các cloud services (để ý cách chia folder theo từng service nếu có, ví dụ như sau)` | Có link #21: bài Terraform directory layout trên Medium. |

Các ô có hyperlink nhưng không chứa câu dẫn kiểu “theo hướng dẫn / xem tại đây” vẫn được giữ đầy đủ trong bảng 1 (ví dụ Sandbox C20, Hypothesis C31, healthcheck C16). `mle!B55` còn có tham chiếu nội bộ bằng chữ **“follow rule ở ô A2”**; đây là chỉ dẫn tới `mle!A2`, không phải một hyperlink nội bộ được OOXML lưu. `de!A34` và hai ô A2 cũng nhắc các tên tài liệu `README.md`, `IaC.md`, `Security.md`, thư mục `docs/`; ngoài các target vật lý trong bảng 1, chúng không có link riêng trong workbook.

## 3. Điểm cần giữ nguyên khi dùng kết quả

- `http://readme.md/` (3 occurrence) và `http://iac.md/` (1 occurrence) là target thực sự trong workbook. Chúng trông giống tên file Markdown bị Excel nhận thành tên miền; đây là suy luận từ tên và ngữ cảnh, chưa xác minh. Không coi chúng là đường dẫn tới README/IaC trong repo, cũng không coi là link diagram các khoá trước.
- `mle!C30` có target lỗi dấu câu như đã ghi ở #17 và URL chữ #23. Giữ cả hai biểu diễn để không làm mất lỗi nguồn.
- `mle!C18` và `mle!C40` có fragment riêng. Chỉ sao chép phần Target sẽ mất đích neo của lời hướng dẫn.
- Những lời dẫn thiếu target riêng ở bảng 2 cần hỏi người cung cấp rubric nếu muốn biết đường dẫn gốc. Không thay bằng link suy đoán hoặc link tìm trên Internet.
- Không có công thức `HYPERLINK` ở hai sheet yêu cầu; công thức tổng điểm `SUM` không phải hyperlink. Chỉ có một URL HTTP(S) viết trực tiếp trong chữ (`mle!C30`).
- Ba drawing parts `drawing1.xml`, `drawing2.xml`, `drawing3.xml` đều rỗng: không có shape, text hoặc hyperlink; không có drawing relationship part. Drawing của `de` là drawing1, của `mle` là drawing3. Không có legacy comments, threaded comments, VML notes hoặc comment relationship trong archive. `xl/persons/person.xml` tồn tại nhưng không chứa comment/ghi chú để trích.

Đã đối chiếu số dòng hyperlink theo sheet relationships, kiểm tra fragment, đếm số target phân biệt, và kiểm tra các câu dẫn nhiều lần trong cùng ô. Đây là trích xuất dữ liệu của workbook tại checksum trên; không kết luận link còn sống, nội dung website còn đúng, hay hướng dẫn tương thích với code hiện tại.
