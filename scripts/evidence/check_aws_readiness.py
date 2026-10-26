"""Collect read-only AWS deployment prerequisites when the operator runs this script."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def probes(instance_type: str) -> dict[str, list[str]]:
    """Declare the complete allowlist of AWS API calls used by this preflight."""
    return {
        "identity": ["sts", "get-caller-identity"],
        "account_plan": ["freetier", "get-account-plan-state"],
        "standard_vcpu_quota": [
            "service-quotas",
            "get-service-quota",
            "--service-code",
            "ec2",
            "--quota-code",
            "L-1216C47A",
        ],
        "instance_specification": [
            "ec2",
            "describe-instance-types",
            "--instance-types",
            instance_type,
        ],
        "instance_offerings": [
            "ec2",
            "describe-instance-type-offerings",
            "--location-type",
            "availability-zone",
            "--filters",
            f"Name=instance-type,Values={instance_type}",
        ],
        "eks_versions": ["eks", "describe-cluster-versions"],
    }


def collect(profile: str, region: str, instance_type: str) -> dict:
    """Collect responses without login, mutation, Terraform plan or credential export."""
    aws = shutil.which("aws")
    if not aws:
        raise RuntimeError("AWS CLI v2 is required on PATH.")
    result = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "region": region,
        "instance_type": instance_type,
        "read_only": True,
        "probes": {},
        "limitations": [
            "Quota is the configured limit, not unused capacity.",
            "An instance offering is not a capacity reservation or proof of launch.",
            "Read-only success does not prove permission to create infrastructure.",
            "No pricing, remaining credit, deployment, or workload readiness is verified.",
        ],
    }
    env = {**os.environ, "AWS_PAGER": "", "AWS_CLI_AUTO_PROMPT": "off"}
    for name, args in probes(instance_type).items():
        command = [
            aws,
            "--profile",
            profile,
            "--region",
            region,
            "--output",
            "json",
            "--cli-connect-timeout",
            "10",
            "--cli-read-timeout",
            "20",
            *args,
        ]
        try:
            run = subprocess.run(command, capture_output=True, text=True, env=env, timeout=60)
            entry = {"command": ["aws", *command[1:]], "exit_code": run.returncode}
            if run.returncode:
                entry["error"] = run.stderr.strip()[-2000:]
            else:
                try:
                    entry["response"] = json.loads(run.stdout)
                except json.JSONDecodeError:
                    entry.update(exit_code=1, error="AWS CLI returned invalid JSON.")
        except subprocess.TimeoutExpired:
            entry = {
                "command": ["aws", *command[1:]],
                "exit_code": 124,
                "error": "Probe timed out.",
            }
        result["probes"][name] = entry
        # Without an identity, stop rather than querying the other services.
        if name == "identity" and entry["exit_code"]:
            break
    result["status"] = (
        "collected" if all(p["exit_code"] == 0 for p in result["probes"].values()) else "incomplete"
    )
    return result


def main() -> int:
    """Write a private local receipt; the operator decides whether to share it."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "ap-southeast-1"))
    parser.add_argument("--instance-type", default="t3.xlarge")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not args.profile:
        parser.error("Pass --profile or set AWS_PROFILE; the default account is never guessed.")
    if not re.fullmatch(r"[a-z]{2}-[a-z-]+-\d+", args.region):
        parser.error("Invalid region.")
    if not re.fullmatch(r"[a-z0-9]+\.[a-z0-9]+", args.instance_type):
        parser.error("Invalid instance type.")
    try:
        receipt = collect(args.profile, args.region, args.instance_type)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = args.output or Path("out/cloud-preflight") / f"aws-{stamp}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(receipt, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    for name, entry in receipt["probes"].items():
        print(f"{name}: {'OK' if entry['exit_code'] == 0 else 'INCOMPLETE'}")
    print(f"Receipt: {output}")
    print("This receipt is not evidence that infrastructure has been deployed.")
    return 0 if receipt["status"] == "collected" else 1


if __name__ == "__main__":
    raise SystemExit(main())
