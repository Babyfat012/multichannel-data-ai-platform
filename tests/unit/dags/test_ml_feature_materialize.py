"""Materialize: full_reload nạp lại toàn bộ khi Redis bị xoá (incremental sẽ nạp 0 dòng mà vẫn xanh)."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DAG = (ROOT / "dags/ml_feature_materialize.py").read_text()


def _rewrite(args, params):
    """Chạy riêng nhánh đổi lệnh của run_feast (không cần Airflow)."""
    fn = next(
        n for n in ast.parse(DAG).body if isinstance(n, ast.FunctionDef) and n.name == "run_feast"
    )
    branch = next(n for n in fn.body if isinstance(n, ast.If))
    scope = {"args": args, "params": params, "FULL_RELOAD_START": "S"}
    exec(compile(ast.Module(body=[branch], type_ignores=[]), "run_feast", "exec"), scope)
    return scope["args"]


def test_full_reload_switches_to_ranged_materialize():
    """Bật full_reload ⇒ `materialize --views v START END`; tắt hoặc lệnh khác ⇒ giữ nguyên."""
    inc = ("materialize-incremental", "--views", "sku_return_daily", "E")
    assert _rewrite(inc, {"full_reload": True}) == (
        "materialize",
        "--views",
        "sku_return_daily",
        "S",
        "E",
    )
    assert _rewrite(inc, {"full_reload": False}) == inc
    assert _rewrite(inc, None) == inc
    assert _rewrite(("apply",), {"full_reload": True}) == ("apply",)
    assert '"full_reload": Param(\n            False, type="boolean"' in DAG
