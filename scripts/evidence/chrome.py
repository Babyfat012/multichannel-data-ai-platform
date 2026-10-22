"""Shared Chrome/CDP session for UI evidence capture scripts."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp


class ChromePage:
    """Gửi lệnh CDP đến một tab Chrome và nhận đúng response theo id."""

    def __init__(
        self, websocket: aiohttp.ClientWebSocketResponse, *, await_promises: bool = True
    ) -> None:
        """Giữ kết nối và bộ đếm request của tab."""
        self.websocket = websocket
        self.next_id = 0
        self.await_promises = await_promises

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

    async def navigate(self, url: str) -> None:
        """Mở URL và chờ trang DOM load xong."""
        await self.call("Page.navigate", {"url": url})
        for _ in range(60):
            result = await self.evaluate("document.readyState")
            if result == "complete":
                return
            await asyncio.sleep(0.5)
        raise TimeoutError(f"Trang không load xong: {url}")

    async def evaluate(self, expression: str):
        """Chạy JavaScript trong tab và trả về giá trị đơn giản."""
        result = await self.call(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": self.await_promises,
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


@asynccontextmanager
async def chrome_page(
    *,
    window_size="1680,1050",
    disable_dev_shm_usage=False,
    max_msg_size=20_000_000,
    await_promises=True,
):
    """Yield an enabled CDP page; close the socket and Chrome even if capture fails."""
    with tempfile.TemporaryDirectory(prefix="fsds-evidence-chrome-") as directory:
        profile = Path(directory)
        chrome = subprocess.Popen(
            [
                "google-chrome",
                "--headless=new",
                "--no-sandbox",
                "--disable-gpu",
                *(["--disable-dev-shm-usage"] if disable_dev_shm_usage else []),
                "--hide-scrollbars",
                f"--window-size={window_size}",
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
                    tab["webSocketDebuggerUrl"], max_msg_size=max_msg_size
                ) as websocket:
                    page = ChromePage(websocket, await_promises=await_promises)
                    await page.call("Page.enable")
                    yield page
        finally:
            if chrome.poll() is None:
                os.killpg(chrome.pid, signal.SIGTERM)
                try:
                    chrome.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(chrome.pid, signal.SIGKILL)
                    chrome.wait(timeout=5)
