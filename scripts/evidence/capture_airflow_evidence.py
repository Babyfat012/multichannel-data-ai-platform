"""Chụp Grid của các DAG Airflow bằng Chrome headless cho hồ sơ DE.

Đọc mật khẩu standalone từ container qua stdout nội bộ của subprocess, không in
ra terminal hay ghi vào file. Dùng Chrome DevTools Protocol để đăng nhập và lưu
ảnh UI thật, không dựng lại ảnh từ trạng thái CLI.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import signal
import subprocess
import tempfile
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "airflow"
BASE_URL = "http://127.0.0.1:8081"


class ChromePage:
    """Gửi lệnh CDP đến một tab Chrome và nhận đúng response theo id."""

    def __init__(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        """Giữ kết nối và bộ đếm request của tab."""
        self.websocket = websocket
        self.next_id = 0

    async def call(self, method: str, params: dict | None = None) -> dict:
        """Thực thi một lệnh CDP, bỏ qua event chạy xen giữa."""
        self.next_id += 1
        request_id = self.next_id
        await self.websocket.send_json({"id": request_id, "method": method,
                                        "params": params or {}})
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
        result = await self.call("Runtime.evaluate", {
            "expression": expression, "returnByValue": True, "awaitPromise": True,
        })
        if "exceptionDetails" in result:
            raise RuntimeError(result["exceptionDetails"])
        return result.get("result", {}).get("value")


def standalone_password() -> str:
    """Đọc password Airflow từ container mà không lộ trong command line."""
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    command = ["docker", "compose", "--profile", "airflow", "exec", "-T",
               "airflow", "cat", "/opt/airflow/standalone_admin_password.txt"]
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


async def wait_for_chrome(profile: Path) -> int:
    """Đọc port DevTools mà Chrome ghi sau khi khởi động."""
    port_file = profile / "DevToolsActivePort"
    for _ in range(100):
        if port_file.exists():
            return int(port_file.read_text().splitlines()[0])
        await asyncio.sleep(0.1)
    raise TimeoutError("Chrome chưa mở DevTools port")


async def capture(password: str, dag_ids: list[str], suffix: str = "") -> None:
    """Đăng nhập Airflow và chụp Grid/Graph của DAG đã chọn thành PNG."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fsds-airflow-chrome-") as directory:
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
                    await page.navigate(f"{BASE_URL}/login/")
                    form_present = await page.evaluate(
                        "Boolean(document.querySelector('input[name=\"username\"]') "
                        "&& document.querySelector('input[name=\"password\"]'))"
                    )
                    if not form_present:
                        raise RuntimeError("Không thấy form đăng nhập Airflow")
                    await page.evaluate(
                        "(() => {"
                        f"document.querySelector('input[name=\"username\"]').value = {json.dumps('admin')};"
                        f"document.querySelector('input[name=\"password\"]').value = {json.dumps(password)};"
                        "document.querySelector('input[name=\"password\"]').form.submit();"
                        "})()"
                    )
                    for _ in range(60):
                        url = await page.evaluate("location.href")
                        if "/login/" not in url:
                            break
                        await asyncio.sleep(0.5)
                    else:
                        raise RuntimeError("Airflow không chấp nhận đăng nhập")

                    for dag_id in dag_ids:
                        for view in ("grid", "graph"):
                            await page.navigate(f"{BASE_URL}/dags/{dag_id}/{view}")
                            await asyncio.sleep(3)
                            url = await page.evaluate("location.href")
                            if "/login/" in url:
                                raise RuntimeError("Phiên đăng nhập hết hạn khi chụp DAG")
                            image = await page.call("Page.captureScreenshot", {
                                "format": "png", "captureBeyondViewport": False,
                            })
                            path = OUTPUT / f"{dag_id}-{view}{suffix}.png"
                            path.write_bytes(base64.b64decode(image["data"]))
                            print(f"Đã chụp {path.relative_to(ROOT)}")
        finally:
            os.killpg(chrome.pid, signal.SIGTERM)
            try:
                chrome.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(chrome.pid, signal.SIGKILL)
                chrome.wait(timeout=5)
            await asyncio.sleep(0.5)


def main() -> None:
    """Đọc CLI và chạy phiên chụp."""
    parser = argparse.ArgumentParser(description="Chụp bằng chứng Airflow UI")
    parser.add_argument("--dag-id", action="append", choices=(
        "dp1_bronze", "dp2_silver", "dp1_cdc", "dp3_features", "dp3_walmart_labels",
        "fsds_runtime_config_check",
    ),
                        help="DAG cần chụp; bỏ trống để chụp DP1/DP2 mặc định")
    parser.add_argument("--suffix", default="",
                        help="hậu tố để giữ ảnh của các lần chạy trước")
    args = parser.parse_args()
    if args.suffix and not re.fullmatch(r"-[a-z0-9-]+", args.suffix):
        parser.error("--suffix phải bắt đầu bằng '-' và chỉ có chữ thường, số, dấu '-'")
    dag_ids = args.dag_id or ["dp1_bronze", "dp2_silver"]
    asyncio.run(capture(standalone_password(), dag_ids, args.suffix))


if __name__ == "__main__":
    main()
