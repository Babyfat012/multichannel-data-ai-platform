"""Một pipeline: Airflow bỏ qua DAG đường B, vẫn nạp dp1–dp4 và các DAG còn lại."""

from __future__ import annotations

import re
from pathlib import Path

DAGS = Path(__file__).resolve().parents[3] / "dags"


def test_only_order_lifecycle_dags_are_ignored():
    """Cú pháp regexp mặc định của Airflow 2.10: dòng # là comment, khớp theo đường dẫn tương đối."""
    lines = (DAGS / ".airflowignore").read_text().splitlines()
    patterns = [re.compile(p) for p in lines if p.strip() and not p.startswith("#")]
    ignored = {f.name for f in DAGS.glob("*.py") if any(p.search(f.name) for p in patterns)}
    assert ignored == {
        "dp_order_lifecycle.py",
        "dp_order_lifecycle_cdc.py",
        "dp_order_lifecycle_warehouse.py",
    }
