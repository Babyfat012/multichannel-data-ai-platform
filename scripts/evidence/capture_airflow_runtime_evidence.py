"""Export credential-free Airflow metadata and selected runtime smoke log lines."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_ID = "de_runtime_20261015"


def main() -> None:
    """Require a successful two-task runtime run and preserve its safe proof."""
    probe = '''
import json
from pathlib import Path
from airflow.models import DagBag, DagRun
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session

dag_id = "fsds_runtime_config_check"
run_id = "de_runtime_20261015"
bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
if bag.import_errors:
    raise RuntimeError("Airflow DAG import errors must be resolved before exporting proof")
with create_session() as session:
    run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one()
    tasks = sorted(run.get_task_instances(session=session), key=lambda task: task.task_id)
    if run.state != "success" or len(tasks) != 2 or any(task.state != "success" for task in tasks):
        raise RuntimeError("Expected successful runtime run with both verification tasks")
    result = {
        "dag_id": dag_id, "run_id": run_id, "state": run.state,
        "logical_date": run.logical_date.isoformat(),
        "started_at": run.start_date.isoformat(), "completed_at": run.end_date.isoformat(),
        "dag_import_errors": 0,
        "tasks": [{"task_id": task.task_id, "state": task.state,
                   "started_at": task.start_date.isoformat(),
                   "completed_at": task.end_date.isoformat()} for task in tasks],
    }
connections = [BaseHook.get_connection(name) for name in ("fsds_minio", "fsds_postgres", "fsds_kafka")]
result["connections"] = [{"connection_id": conn.conn_id, "password_encrypted": conn.is_encrypted}
                         for conn in connections]
logs = Path("/opt/airflow/logs") / ("dag_id=" + dag_id) / ("run_id=" + run_id)
result["proof_lines"] = [line for path in sorted(logs.rglob("*.log"))
                         for line in path.read_text().splitlines()
                         if "FSDS runtime configuration resolved:" in line or "FSDS Spark runtime smoke:" in line]
if len(result["proof_lines"]) != 2:
    raise RuntimeError("Expected one configuration and one Spark success proof line")
result["scope"] = "Connection/Variable lookup and three-row Spark standalone query; no source or table writes"
print("FSDS_EVIDENCE_JSON=" + json.dumps(result))
'''
    env = os.environ.copy()
    env.pop("POSTGRES_PASSWORD", None)
    output = subprocess.check_output([
        "docker", "compose", "--profile", "airflow", "exec", "-T", "airflow",
        "python", "-c", probe,
    ], cwd=ROOT, env=env, text=True)
    marker = "FSDS_EVIDENCE_JSON="
    evidence = json.loads(next(line[len(marker):] for line in output.splitlines() if line.startswith(marker)))
    target = ROOT / "docs/evidence/airflow-runtime-20261015.json"
    target.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print(f"Runtime evidence saved: {target.relative_to(ROOT)}; run={RUN_ID}; tasks=2/2 success")


if __name__ == "__main__":
    main()
