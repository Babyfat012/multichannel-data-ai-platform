"""Check diagram geometry, runtime boundaries and AWS-specific deployment contracts."""

import importlib
import struct
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "diagrams"))
from common import Diagram  # noqa: E402


@pytest.fixture(scope="module", params=["layer12", "layer3", "aws_layer12", "aws_layer3"])
def diagram(request):
    """Route each actual diagram once, sharing the expensive geometry fixture."""
    return importlib.import_module(request.param).make()


def test_geometry_and_accessible_output(diagram, tmp_path):
    """All four diagrams must route cleanly and export self-contained accessible SVG."""
    assert "geometry PASS" in diagram.check()
    diagram.render(tmp_path)
    html = (tmp_path / f"{diagram.slug}.html").read_text()
    svg = ET.fromstring(html[html.index("<svg") : html.index("</svg>") + 6])
    ns = {"s": "http://www.w3.org/2000/svg"}
    assert svg.find("s:title", ns).text == diagram.title
    assert svg.find("s:desc", ns).text
    assert "<script" not in html and "<link" not in html
    codes = [edge[2] for edge in diagram.edges]
    assert len(codes) == len(set(codes))
    assert {code[0] for code in codes if code[0] != "P"} == {f[0] for f in diagram.flows}


def test_aws_runtime_boundaries(diagram):
    """AWS units belong to real runtime frames and exclude disabled local services."""
    if not diagram.slug.startswith("aws-"):
        return
    assert diagram.deployment == "AWS · ap-southeast-2"
    names = " ".join(box.name for box in diagram.boxes.values())
    assert not any(name in names for name in ("MinIO", "generator-stream", "Docker registry"))
    aws = next(frame for frame in diagram.frames if frame[4].startswith("AWS ·"))
    vpc = next(frame for frame in diagram.frames if frame[4].startswith("VPC ·"))
    for box in diagram.boxes.values():
        if box.key in {"dev", "seller"}:
            assert not _contains(aws, box.rect)
        elif box.key in {"raw", "lake", "s3", "ecr"}:
            assert _contains(aws, box.rect)
            assert not _contains(vpc, box.rect)
        elif box.key == "nlb":
            assert _contains(vpc, box.rect)
            eks = next(frame for frame in diagram.frames if frame[4].startswith("EKS ·"))
            assert not _contains(eks, box.rect)
        else:
            runtime = "EKS ·" if box.tag.startswith("ns ") else "EC2 ·"
            frame = next(frame for frame in diagram.frames if frame[4].startswith(runtime))
            assert _contains(frame, box.rect), box.key


def _contains(frame, rect):
    """Check complete containment within a deployment boundary."""
    x, y, w, h = frame[:4]
    return x <= rect[0] < rect[2] <= x + w and y <= rect[1] < rect[3] <= y + h


def test_aws_flow_contracts(diagram):
    """Check replay semantics, private ports and the correct training/drift runtime."""
    if not diagram.slug.startswith("aws-"):
        return
    edges = {(s, t): label for s, t, _, label, *_ in diagram.edges}
    if diagram.slug.endswith("layer12"):
        assert "phát lại Return Gold A" in edges["trino", "kafka"]
        assert ":30379" in edges["air", "redis"]
        assert ":30379" in edges["push", "redis"]
        assert diagram.boxes["drift"].tag == "ns kubeflow"
        for pair in [
            ("lake", "kfp"),
            ("kfp", "mlflow"),
            ("mlflow", "dev"),
            ("dev", "ecr"),
            ("ecr", "serve"),
        ]:
            assert pair in edges
    else:
        assert ":30432" in edges["air", "pgv"]
        assert ":9100" in edges["node", "prom"]
        assert ":9187" in edges["pgexp", "prom"]
        assert "ci" in edges["jenkins", "api"]
        for pair in [
            ("seller", "nlb"),
            ("nlb", "ing"),
            ("ing", "ui"),
            ("ui", "coord"),
            ("coord", "agw"),
            ("agw", "api"),
            ("api", "serve"),
        ]:
            assert pair in edges


@pytest.mark.parametrize(
    "points,label",
    [
        ([(280, 200), (400, 220)], (300, 150, 380, 168)),
        ([(280, 200), (620, 200)], (300, 150, 380, 168)),
        ([(280, 200), (400, 200)], (300, 190, 380, 210)),
    ],
)
def test_checker_rejects_invalid_routes(points, label):
    """Reject diagonal arrows, arrows crossing units and labels covering arrows."""
    d = Diagram("invalid", "Negative fixture")
    d.box("a", 60, 160, "host", "A")
    d.box("b", 400, 160, "host", "B")
    d.labels = [label, (330, 230, 350, 250)]
    d.edges = [("a", "b", "B1", "payload", points, (340, 240, label), False)]
    with pytest.raises(AssertionError):
        d.check()


@pytest.mark.parametrize(
    "slug,size",
    [
        ("aws-architecture-layer12", (2840, 2000)),
        ("aws-architecture-layer3", (3100, 2290)),
    ],
)
def test_aws_png_artifacts(slug, size):
    """Raster artifacts must retain full canvas resolution, including browser padding."""
    data = (ROOT / "docs" / "diagrams" / f"{slug}.png").read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    width, height = struct.unpack(">II", data[16:24])
    assert width >= size[0] and height >= size[1]
