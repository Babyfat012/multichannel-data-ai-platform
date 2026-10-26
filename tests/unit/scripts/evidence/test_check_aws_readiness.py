"""Read-only preflight records permission failures without calling AWS in tests."""

import json
import subprocess

from scripts.evidence.check_aws_readiness import collect


def test_records_partial_permissions_without_claiming_available_capacity(monkeypatch):
    """Identity succeeds but denied quota remains incomplete and does not abort other probes."""
    calls = []
    monkeypatch.setattr("scripts.evidence.check_aws_readiness.shutil.which", lambda _: "/fake/aws")

    def run(command, **kwargs):
        """Return fake service responses and assert explicit targeting and no credential export."""
        calls.append(command)
        assert command[1:5] == ["--profile", "demo", "--region", "ap-southeast-1"]
        assert not any(
            x in command
            for x in ["configure", "create-cluster", "create-role", "get-session-token"]
        )
        if "get-service-quota" in command:
            return subprocess.CompletedProcess(command, 254, "", "AccessDenied")
        return subprocess.CompletedProcess(command, 0, json.dumps({"Account": "123456789012"}), "")

    monkeypatch.setattr("scripts.evidence.check_aws_readiness.subprocess.run", run)
    receipt = collect("demo", "ap-southeast-1", "t3.xlarge")
    assert len(calls) == 6
    assert receipt["status"] == "incomplete"
    assert receipt["probes"]["standard_vcpu_quota"]["error"] == "AccessDenied"
    assert receipt["probes"]["eks_versions"]["exit_code"] == 0
    assert any("not unused capacity" in text for text in receipt["limitations"])


def test_identity_failure_stops_before_other_services(monkeypatch):
    """Expired login never falls back to another profile/account or mutates authentication."""
    calls = []
    monkeypatch.setattr("scripts.evidence.check_aws_readiness.shutil.which", lambda _: "/fake/aws")

    def run(command, **kwargs):
        """Simulate an expired operator session."""
        calls.append(command)
        return subprocess.CompletedProcess(command, 255, "", "ExpiredToken")

    monkeypatch.setattr("scripts.evidence.check_aws_readiness.subprocess.run", run)
    receipt = collect("expired", "ap-southeast-1", "t3.xlarge")
    assert len(calls) == 1
    assert receipt["status"] == "incomplete"
    assert list(receipt["probes"]) == ["identity"]
