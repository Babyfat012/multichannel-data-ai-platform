"""Phần không cần mạng của gateway_smoke: đọc SSE MCP, chấm đạt/không, chuỗi gọi MCP có session."""

import asyncio
import json

import httpx
import pytest

from scripts.evidence.gateway_smoke import grade, mcp_calls, sse_json


def test_sse_json_takes_last_data_line_or_plain_json():
    """SSE nhiều dòng data ⇒ lấy dòng cuối; JSON thường vẫn đọc được."""
    assert sse_json('event: message\ndata: {"a": 1}\n\ndata: {"a": 2}\n') == {"a": 2}
    assert sse_json('{"a": 3}') == {"a": 3}


@pytest.mark.parametrize(
    ("got", "want", "ok"),
    [(401, 401, True), (200, 401, False), (True, True, True), (False, True, False), (429, [200, 429], True)],
)
def test_grade_marks_each_row_and_fails_on_any(got, want, ok):
    """Mỗi dòng có cờ ok; một dòng hỏng là cả lượt hỏng."""
    rows = [{"check": "x", "got": got, "want": want}]
    assert grade(rows) is ok and rows[0]["ok"] is ok
    assert grade([{"check": "a", "got": 1, "want": 1}, {"check": "b", "got": 2, "want": 1}]) is False


def _gateway(features, structured=True):
    """MCP giả: initialize trả session; mọi lời gọi sau phải mang đúng session."""
    def handle(request):
        body = json.loads(request.content)
        if body["method"] == "initialize":
            return httpx.Response(200, headers={"mcp-session-id": "s1"}, json={"result": {}})
        assert request.headers["mcp-session-id"] == "s1"
        if body["method"] == "tools/list":
            return httpx.Response(200, text='data: {"result": {"tools": [{"name": "get_sku_features"}]}}\n')
        if body["method"] == "tools/call":
            payload = {"features": [features]}
            result = {"structuredContent": payload} if structured else {
                "content": [{"type": "text", "text": json.dumps(payload)}]}
            return httpx.Response(200, text=f"data: {json.dumps({'result': result})}\n")
        return httpx.Response(202)
    return httpx.AsyncClient(base_url="http://gw", transport=httpx.MockTransport(handle))


@pytest.mark.parametrize("structured", [True, False])
def test_mcp_calls_reports_null_features_as_missing(structured):
    """Mang session sau initialize; feature toàn null (Redis rỗng) bị báo là thiếu."""
    async def go(features):
        async with _gateway(features, structured) as client:
            return await mcp_calls(client, "k", "APP-00042")

    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": 3})) == (200, ["get_sku_features"], True)
    # Redis rỗng: feature toàn null ⇒ phải báo KHÔNG có giá trị (lỗi im lặng).
    assert asyncio.run(go({"seller_sku": "APP-00042", "returns_30d": None}))[2] is False
