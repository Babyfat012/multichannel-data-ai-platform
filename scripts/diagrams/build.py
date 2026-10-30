"""Rebuild all four standalone HTML diagrams and run geometry checks (stdlib only).

Run from any directory: python3 scripts/diagrams/build.py.
"""

from aws_layer3 import make as aws_layer3
from aws_layer12 import make as aws_layer12
from common import ROOT
from layer3 import make as layer3
from layer12 import make as layer12


def main():
    """Build and validate the diagrams, failing on invalid geometry."""
    for make in (layer12, layer3, aws_layer12, aws_layer3):
        diagram = make()
        diagram.render(ROOT / "docs" / "diagrams")
        print(diagram.check())


if __name__ == "__main__":
    main()
