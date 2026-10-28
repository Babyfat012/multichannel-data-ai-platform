"""Chụp Grid của các DAG Airflow bằng Chrome headless cho pipeline Data Platform.

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
import subprocess
from pathlib import Path

from scripts.evidence.chrome import chrome_page

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "images" / "airflow"
BASE_URL = "http://127.0.0.1:8081"


def standalone_password() -> str:
    """Đọc password Airflow từ container mà không lộ trong command line."""
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    command = [
        "docker",
        "compose",
        "--profile",
        "airflow",
        "exec",
        "-T",
        "airflow",
        "cat",
        "/opt/airflow/standalone_admin_password.txt",
    ]
    return subprocess.check_output(command, cwd=ROOT, env=env, text=True).strip()


async def capture(password: str, dag_ids: list[str], suffix: str = "") -> None:
    """Đăng nhập Airflow và chụp Grid/Graph của DAG đã chọn thành PNG."""
    OUTPUT.mkdir(parents=True, exist_ok=True)
    async with chrome_page() as page:
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
                image = await page.call(
                    "Page.captureScreenshot",
                    {
                        "format": "png",
                        "captureBeyondViewport": False,
                    },
                )
                path = OUTPUT / f"{dag_id}-{view}{suffix}.png"
                path.write_bytes(base64.b64decode(image["data"]))
                print(f"Đã chụp {path.relative_to(ROOT)}")
    await asyncio.sleep(0.5)


def main() -> None:
    """Đọc CLI và chạy phiên chụp."""
    parser = argparse.ArgumentParser(description="Chụp bằng chứng Airflow UI")
    parser.add_argument(
        "--dag-id",
        action="append",
        choices=(
            "dp1_bronze",
            "dp2_silver",
            "dp1_cdc",
            "dp3_features",
            "dp3_return_labels",
            "platform_runtime_config_check",
        ),
        help="DAG cần chụp; bỏ trống để chụp DP1/DP2 mặc định",
    )
    parser.add_argument("--suffix", default="", help="hậu tố để giữ ảnh của các lần chạy trước")
    args = parser.parse_args()
    if args.suffix and not re.fullmatch(r"-[a-z0-9-]+", args.suffix):
        parser.error("--suffix phải bắt đầu bằng '-' và chỉ có chữ thường, số, dấu '-'")
    dag_ids = args.dag_id or ["dp1_bronze", "dp2_silver"]
    asyncio.run(capture(standalone_password(), dag_ids, args.suffix))


if __name__ == "__main__":
    main()
