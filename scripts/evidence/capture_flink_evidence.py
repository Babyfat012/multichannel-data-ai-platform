"""Chụp trang Flink UI của một job thành công bằng Chrome headless/CDP."""

from __future__ import annotations

import argparse
import asyncio
import base64
from pathlib import Path

from scripts.evidence.chrome import chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "flink" / "flink-overview.png"
BASE_URL = "http://127.0.0.1:8082"


async def capture(job_id: str | None, output: Path, expects: list[str]) -> None:
    """Open the Flink UI in headless Chrome, verify content, and save a screenshot."""
    output.parent.mkdir(parents=True, exist_ok=True)
    async with chrome_page(await_promises=False) as page:
        target_url = f"{BASE_URL}/#/job/completed/{job_id}/overview" if job_id else BASE_URL
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
    """Parse evidence capture options and run the asynchronous browser workflow."""
    parser = argparse.ArgumentParser(description="Chụp bằng chứng Flink UI")
    parser.add_argument("--job-id", help="Chụp graph của completed job")
    parser.add_argument(
        "--expect",
        action="append",
        default=[],
        help="Token phải xuất hiện trong graph (lặp lại được); bỏ trống thì bỏ qua kiểm tra",
    )
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    asyncio.run(capture(args.job_id, args.output.resolve(), args.expect))


if __name__ == "__main__":
    main()
