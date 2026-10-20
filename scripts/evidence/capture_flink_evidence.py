"""Chụp trang Flink UI của một job thành công bằng Chrome headless/CDP."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import signal
import subprocess
import tempfile
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "flink" / "flink-overview.png"
BASE_URL = "http://127.0.0.1:8082"


class ChromePage:
    """Gửi lệnh CDP đến tab hiện tại và nhận đúng response theo ID."""

    def __init__(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        """Bind a websocket for issuing Chrome DevTools Protocol commands."""
        self.websocket = websocket
        self.next_id = 0

    async def call(self, method: str, params: dict | None = None) -> dict:
        """Send one CDP command and wait for its matching response identifier."""
        self.next_id += 1
        request_id = self.next_id
        await self.websocket.send_json({"id": request_id, "method": method,
                                        "params": params or {}})
        while True:
            message = await self.websocket.receive_json(timeout=30)
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(f"CDP {method}: {message['error']}")
                return message.get("result", {})

    async def evaluate(self, expression: str):
        """Evaluate JavaScript in the page and return its by-value result."""
        result = await self.call("Runtime.evaluate", {
            "expression": expression, "returnByValue": True,
        })
        if "exceptionDetails" in result:
            raise RuntimeError(result["exceptionDetails"])
        return result.get("result", {}).get("value")


async def wait_for_chrome(profile: Path) -> int:
    """Wait for Chrome to publish its ephemeral DevTools debugging port."""
    port_file = profile / "DevToolsActivePort"
    for _ in range(100):
        if port_file.exists():
            return int(port_file.read_text().splitlines()[0])
        await asyncio.sleep(0.1)
    raise TimeoutError("Chrome chưa mở DevTools port")


async def capture(job_id: str | None, output: Path, expects: list[str]) -> None:
    """Open the Flink UI in headless Chrome, verify content, and save a screenshot."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fsds-flink-chrome-") as directory:
        profile = Path(directory)
        chrome = subprocess.Popen([
            "google-chrome", "--headless=new", "--no-sandbox", "--disable-gpu",
            "--hide-scrollbars", "--window-size=1680,1050",
            "--remote-debugging-port=0", "--remote-allow-origins=*",
            f"--user-data-dir={profile}", "about:blank",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        try:
            port = await wait_for_chrome(profile)
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/json") as response:
                    targets = await response.json()
                tab = next(item for item in targets if item["type"] == "page")
                async with session.ws_connect(tab["webSocketDebuggerUrl"],
                                              max_msg_size=20_000_000) as websocket:
                    page = ChromePage(websocket)
                    await page.call("Page.enable")
                    target_url = (f"{BASE_URL}/#/job/completed/{job_id}/overview"
                                  if job_id else BASE_URL)
                    await page.call("Page.navigate", {"url": target_url})
                    for _ in range(60):
                        if await page.evaluate("document.readyState") == "complete":
                            break
                        await asyncio.sleep(0.5)
                    await asyncio.sleep(2)
                    title = await page.evaluate("document.title")
                    if "Flink" not in title:
                        raise RuntimeError(f"Không thấy Flink UI: {title!r}")
                    if job_id and expects:
                        for _ in range(20):
                            body = await page.evaluate("document.body.innerText")
                            if all(token in body for token in expects):
                                break
                            await asyncio.sleep(0.5)
                        else:
                            raise RuntimeError(f"Job graph chưa hiện ở {target_url}: {body[:500]!r}")
                    image = await page.call("Page.captureScreenshot", {
                        "format": "png", "captureBeyondViewport": False,
                    })
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
    """Parse evidence capture options and run the asynchronous browser workflow."""
    parser = argparse.ArgumentParser(description="Chụp bằng chứng Flink UI")
    parser.add_argument("--job-id", help="Chụp graph của completed job")
    parser.add_argument("--expect", action="append", default=[],
                        help="Token phải xuất hiện trong graph (lặp lại được); bỏ trống thì bỏ qua kiểm tra")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    asyncio.run(capture(args.job_id, args.output.resolve(), args.expect))


if __name__ == "__main__":
    main()
