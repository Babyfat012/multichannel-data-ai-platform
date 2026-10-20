"""Chụp topic CDC trên giao diện kafka-ui bằng Chrome headless/CDP.

kafka-ui là ứng dụng một trang: phải chờ JavaScript dựng bảng rồi mới chụp, nên
script điều khiển Chrome qua DevTools Protocol giống các script chụp bằng chứng
khác, thay vì chỉ tải HTML.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import os
import signal
import subprocess
import tempfile
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "cdc" / "kafka-ui-cdc-topic.png"
BASE_URL = "http://127.0.0.1:8085"
CLUSTER = "fsds"
TOPIC = "fsds_cdc.walmart.orders"


class ChromePage:
    """Gửi lệnh CDP đến tab hiện tại và nhận đúng response theo ID."""

    def __init__(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        """Giữ kết nối và bộ đếm request của tab."""
        self.websocket = websocket
        self.next_id = 0

    async def call(self, method: str, params: dict | None = None) -> dict:
        """Thực thi một lệnh CDP, bỏ qua event chạy xen giữa."""
        self.next_id += 1
        request_id = self.next_id
        await self.websocket.send_json({"id": request_id, "method": method, "params": params or {}})
        while True:
            message = await self.websocket.receive_json(timeout=30)
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"CDP {method}: {message['error']}")
            return message.get("result", {})

    async def evaluate(self, expression: str):
        """Chạy JavaScript trong tab và trả về giá trị đơn giản."""
        result = await self.call(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True,
            },
        )
        if "exceptionDetails" in result:
            raise RuntimeError(result["exceptionDetails"])
        return result.get("result", {}).get("value")


async def wait_for_chrome(profile: Path) -> int:
    """Đọc port DevTools mà Chrome ghi sau khi khởi động."""
    port_file = profile / "DevToolsActivePort"
    for _ in range(100):
        if port_file.exists():
            return int(port_file.read_text().splitlines()[0])
        await asyncio.sleep(0.1)
    raise TimeoutError("Chrome chưa mở DevTools port")


async def capture(url: str, output: Path, expects: list[str]) -> None:
    """Mở trang kafka-ui, chờ nội dung cần thiết rồi lưu ảnh PNG."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fsds-kafka-ui-chrome-") as directory:
        profile = Path(directory)
        chrome = subprocess.Popen(
            [
                "google-chrome",
                "--headless=new",
                "--no-sandbox",
                "--disable-gpu",
                "--hide-scrollbars",
                "--window-size=1680,1050",
                "--remote-debugging-port=0",
                "--remote-allow-origins=*",
                f"--user-data-dir={profile}",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            port = await wait_for_chrome(profile)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/json") as response:
                    targets = await response.json()
                tab = next(item for item in targets if item["type"] == "page")
                async with session.ws_connect(
                    tab["webSocketDebuggerUrl"], max_msg_size=20_000_000
                ) as websocket:
                    page = ChromePage(websocket)
                    await page.call("Page.enable")
                    await page.call("Page.navigate", {"url": url})
                    for _ in range(60):
                        if await page.evaluate("document.readyState") == "complete":
                            break
                        await asyncio.sleep(0.5)
                    for _ in range(60):
                        body = await page.evaluate("document.body.innerText")
                        if all(token in body for token in expects):
                            break
                        await asyncio.sleep(0.5)
                    else:
                        raise RuntimeError(f"Nội dung chưa hiện ở {url}: {body[:500]!r}")
                    await asyncio.sleep(3)
                    image = await page.call(
                        "Page.captureScreenshot",
                        {
                            "format": "png",
                            "captureBeyondViewport": False,
                        },
                    )
                    output.write_bytes(base64.b64decode(image["data"]))
                    print(f"Đã chụp {output.relative_to(ROOT)}")
        finally:
            os.killpg(chrome.pid, signal.SIGTERM)
            try:
                chrome.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(chrome.pid, signal.SIGKILL)
                chrome.wait(timeout=5)


def main() -> None:
    """Đọc CLI và chạy phiên chụp."""
    parser = argparse.ArgumentParser(description="Chụp topic CDC trên kafka-ui")
    parser.add_argument("--url", default=f"{BASE_URL}/ui/clusters/{CLUSTER}/all-topics")
    parser.add_argument("--expect", action="append", default=[])
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    asyncio.run(capture(args.url, args.output.resolve(), args.expect))


if __name__ == "__main__":
    main()
