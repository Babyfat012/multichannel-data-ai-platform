"""Rebuild both standalone HTML diagrams and run geometry checks (stdlib only).

Run from any directory: python3 scripts/diagrams/build.py.
"""

from common import ROOT
from layer3 import make as layer3
from layer12 import make as layer12


def main():
    """Build and validate the diagrams, failing on invalid geometry."""
    for make in (layer12, layer3):
        diagram = make()
        diagram.render(ROOT / "docs" / "diagrams")
        print(diagram.check())


if __name__ == "__main__":
    main()
