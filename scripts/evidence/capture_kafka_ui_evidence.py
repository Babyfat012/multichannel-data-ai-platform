"""Chụp topic CDC trên giao diện kafka-ui bằng Chrome headless/CDP.

kafka-ui là ứng dụng một trang: phải chờ JavaScript dựng bảng rồi mới chụp, nên
script điều khiển Chrome qua DevTools Protocol giống các script chụp bằng chứng
khác, thay vì chỉ tải HTML.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
from pathlib import Path

from scripts.evidence.chrome import chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "cdc" / "kafka-ui-cdc-topic.png"
BASE_URL = "http://127.0.0.1:8085"
CLUSTER = "fsds"
TOPIC = "fsds_cdc.walmart.orders"


async def capture(url: str, output: Path, expects: list[str]) -> None:
    """Mở trang kafka-ui, chờ nội dung cần thiết rồi lưu ảnh PNG."""
    output.parent.mkdir(parents=True, exist_ok=True)
    async with chrome_page() as page:
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
