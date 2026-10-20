"""Capture the completed Silver SQL sub-execution from the local Spark History Server."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import signal
import subprocess
import tempfile
from pathlib import Path

import aiohttp

from scripts.evidence.capture_airflow_evidence import ChromePage, wait_for_chrome

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs/evidence/spark-ui"
APP_ID = "app-20261014035356-0002"
BASE_URL = "http://127.0.0.1:18080"


async def main() -> None:
    """Validate execution 3, preserve REST proof, and capture its real History UI."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{BASE_URL}/api/v1/applications/{APP_ID}/sql/3") as response:
            response.raise_for_status()
            execution = await response.json()
        plan = execution["planDescription"]
        if execution["status"] != "COMPLETED" or not all(
            token in plan
            for token in ("AmazonOrderId", "orderId", "from_json", "Union", "row_number")
        ):
            raise ValueError("Expected completed schema-normalization and dedup plan")
        (OUTPUT / "silver-amazon-execution-3.json").write_text(
            json.dumps(execution, ensure_ascii=False, indent=2) + "\n"
        )
        with tempfile.TemporaryDirectory(prefix="fsds-spark-proof-") as directory:
            profile = Path(directory)
            chrome = subprocess.Popen(
                [
                    "google-chrome",
                    "--headless=new",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--hide-scrollbars",
                    "--window-size=1920,1600",
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
                async with session.get(f"http://127.0.0.1:{port}/json") as response:
                    targets = await response.json()
                tab = next(target for target in targets if target["type"] == "page")
                async with session.ws_connect(
                    tab["webSocketDebuggerUrl"], max_msg_size=30_000_000
                ) as socket:
                    page = ChromePage(socket)
                    await page.call("Page.enable")
                    await page.navigate(f"{BASE_URL}/history/{APP_ID}/SQL/execution/?id=3")
                    for _ in range(60):
                        body = await page.evaluate("document.body.innerText")
                        if "Window" in body and "Union" in body:
                            break
                        await asyncio.sleep(0.5)
                    else:
                        raise RuntimeError(f"SQL execution 3 graph did not load: {body[:500]}")
                    await asyncio.sleep(1)
                    layout = await page.call("Page.getLayoutMetrics")
                    size = layout["cssContentSize"]
                    screenshot = await page.call(
                        "Page.captureScreenshot",
                        {
                            "format": "png",
                            "captureBeyondViewport": True,
                            "clip": {
                                "x": 0,
                                "y": 0,
                                "width": size["width"],
                                "height": size["height"],
                                "scale": 1,
                            },
                        },
                    )
                    (OUTPUT / "12-silver-amazon-dedup-plan.png").write_bytes(
                        base64.b64decode(screenshot["data"])
                    )
                    print(
                        json.dumps(
                            {
                                "application": APP_ID,
                                "execution": 3,
                                "status": execution["status"],
                                "page_size": size,
                            }
                        )
                    )
                    await page.evaluate("clickPhysicalPlanDetails()")
                    await page.evaluate("window.find('(4) Project', false, false, true)")
                    await asyncio.sleep(0.5)
                    details = await page.call(
                        "Page.captureScreenshot",
                        {
                            "format": "png",
                            "captureBeyondViewport": False,
                        },
                    )
                    (OUTPUT / "13-silver-amazon-schema-details.png").write_bytes(
                        base64.b64decode(details["data"])
                    )
            finally:
                os.killpg(chrome.pid, signal.SIGTERM)
                try:
                    chrome.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(chrome.pid, signal.SIGKILL)
                    chrome.wait(timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
