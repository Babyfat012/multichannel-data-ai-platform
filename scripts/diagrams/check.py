"""Exercise the geometry checker with real diagrams and deliberately invalid routes.

Run: python3 scripts/diagrams/check.py. No browser or third-party module is needed.
"""

from common import Diagram
from layer3 import make as layer3
from layer12 import make as layer12


def main():
    """Build and validate the diagrams, failing on invalid geometry."""
    for make in (layer12, layer3):
        print(make().check())
    for points, label in (
        ([(280, 200), (400, 220)], (300, 150, 380, 168)),  # Diagonal.
        ([(280, 200), (620, 200)], (300, 150, 380, 168)),  # Crosses a unit.
        ([(280, 200), (400, 200)], (300, 190, 380, 210)),  # Crosses a label.
    ):
        d = Diagram("invalid", "Checker negative fixture")
        d.box("a", 60, 160, "host", "A")
        d.box("b", 400, 160, "host", "B")
        d.labels = [label, (330, 230, 350, 250)]
        d.edges = [("a", "b", "B1", "payload", points, (340, 240, label), False)]
        try:
            d.check()
        except AssertionError:
            continue
        raise RuntimeError("Geometry checker accepted an invalid fixture")
    print("Negative fixtures: diagonal / obstructed unit / obstructed label rejected")


if __name__ == "__main__":
    main()
