"""Offline SVG drawing and rectilinear routing; geometry is checked before export."""

from dataclasses import dataclass
from heapq import heappop, heappush
from html import escape
from pathlib import Path

COLORS = dict(
    B="#2e5aa8",
    S="#eb6c36",
    T="#2f7d55",
    M="#936329",
    A="#b94e25",
    K="#2f7d55",
    O="#936329",
    C="#7b4fa0",
    P="#7a8399",
)
BG = "#f5f5f5"


@dataclass
class Box:
    """A runnable unit, with coordinates and a source-backed runtime tag."""

    key: str
    x: int
    y: int
    tag: str
    name: str
    detail: tuple
    support: bool = False
    w: int = 220
    h: int = 80

    @property
    def rect(self):
        """Return the unit bounds used by routing and validation."""
        return (self.x, self.y, self.x + self.w, self.y + self.h)


def inside(p, r, pad=0):
    """Test whether a grid point lies inside a padded rectangle."""
    return r[0] - pad < p[0] < r[2] + pad and r[1] - pad < p[1] < r[3] + pad


def intersects(a, b, r):
    """Strict interior intersection; touching a unit's port is allowed."""
    if a[0] == b[0]:
        return r[0] < a[0] < r[2] and max(min(a[1], b[1]), r[1]) < min(max(a[1], b[1]), r[3])
    return r[1] < a[1] < r[3] and max(min(a[0], b[0]), r[0]) < min(max(a[0], b[0]), r[2])


def overlap(a, b):
    """Test strict overlap of two axis-aligned rectangles."""
    return a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]


class Diagram:
    """Collect units and flows, route on a ten-pixel grid and render accessible HTML."""

    def __init__(self, slug, title, width=2400, height=1500):
        """Initialize a blank canvas and deterministic routing state."""
        self.slug, self.title, self.width, self.height = slug, title, width, height
        self.boxes = {}
        self.frames, self.notes, self.flows, self.edges = [], [], [], []
        self.labels, self.used = [], set()
        self.port_counts = {}

    def frame(self, x, y, w, h, label, blue=False):
        """Add a labeled runtime boundary."""
        self.frames.append((x, y, w, h, label, blue))

    def box(self, key, x, y, tag, name, *detail, support=False):
        """Add one runnable unit at the given canvas position."""
        self.boxes[key] = Box(key, x, y, tag, name, detail, support)

    def note(self, x, y, text):
        """Add an explanatory caption outside the flow area."""
        self.notes.append((x, y, text))

    def port(self, box, other, attempt=0):
        """Choose a perimeter port, varying sides when a route is congested."""
        dx, dy = other.x - box.x, other.y - box.y
        if attempt >= 5:
            dx, dy = ((100, 0), (0, 100), (-100, 0), (0, -100))[(attempt // 5 - 1) % 4]
        side = (
            box.key,
            "h" if abs(dx) >= abs(dy) else "v",
            dx >= 0 if abs(dx) >= abs(dy) else dy >= 0,
        )
        count = self.port_counts.get(side, 0)
        self.port_counts[side] = count + 1
        off = ((0, -20, 20, -10, 10) if abs(dx) >= abs(dy) else (0, -40, 40, -80, 80))[count % 5]
        if abs(dx) >= abs(dy):
            return (
                ((box.x + box.w, box.y + 40 + off), (20, 0))
                if dx >= 0
                else ((box.x, box.y + 40 + off), (-20, 0))
            )
        return (
            ((box.x + 110 + off, box.y + 80), (0, 20))
            if dy >= 0
            else ((box.x + 110 + off, box.y), (0, -20))
        )

    def edge(self, source, target, code, label, secondary=False, attempt=0):
        """Route and label a data flow while avoiding units and earlier labels."""
        s, t = self.boxes[source], self.boxes[target]
        a, da = self.port(s, t, attempt)
        b, db = self.port(t, s, attempt + 5 if attempt >= 5 else 0)
        start, end = (a[0] + da[0], a[1] + da[1]), (b[0] + db[0], b[1] + db[1])
        obstacles = [v.rect for v in self.boxes.values()] + self.labels
        queue = [(0, 0, start, -1)]
        costs, prev = {(start, -1): 0}, {}
        goal = None
        while queue:
            _, cost, p, direction = heappop(queue)
            state = (p, direction)
            if cost != costs[state]:
                continue
            if p == end:
                goal = state
                break
            for d, (dx, dy) in enumerate(((10, 0), (-10, 0), (0, 10), (0, -10))):
                q = (p[0] + dx, p[1] + dy)
                if not (20 <= q[0] <= self.width - 20 and 80 <= q[1] <= self.height - 300):
                    continue
                if any(inside(q, r, 5) for r in obstacles):
                    continue
                value = cost + 10 + (35 if d != direction else 0) + (50 if q in self.used else 0)
                nxt = (q, d)
                if value < costs.get(nxt, float("inf")):
                    costs[nxt], prev[nxt] = value, state
                    heappush(queue, (value + abs(q[0] - end[0]) + abs(q[1] - end[1]), value, q, d))
        if goal is None:
            return self.retry(source, target, code, label, secondary, attempt)
        route = []
        while goal in prev:
            route.append(goal[0])
            goal = prev[goal]
        route = [a, start, *reversed(route), b]
        self.used.update(route)
        points = [route[0]]
        for i in range(1, len(route) - 1):
            u, v, w = points[-1], route[i], route[i + 1]
            if (u[0] == v[0] == w[0]) or (u[1] == v[1] == w[1]):
                continue
            if v != points[-1]:
                points.append(v)
        points.append(b)
        # Labels sit beside the line; circles intentionally sit on their own line.
        width = max(70, len(label) * 6 + 12)
        segments = list(zip(points, points[1:]))
        candidates = []
        for u, v in segments:
            length = abs(u[0] - v[0]) + abs(u[1] - v[1])
            for fraction in (0.5, 0.2, 0.8, 0.35, 0.65, 0.1, 0.9):
                x = round((u[0] + (v[0] - u[0]) * fraction) / 10) * 10
                y = round((u[1] + (v[1] - u[1]) * fraction) / 10) * 10
                if length < 60:
                    continue
                rects = [
                    (x - width / 2, y - 34, x + width / 2, y - 16),
                    (x + 18, y - 9, x + 18 + width, y + 9),
                    (x - 18 - width, y - 9, x - 18, y + 9),
                    (x - width / 2, y + 16, x + width / 2, y + 34),
                ]
                for r in rects:
                    badge = (x - 12, y - 12, x + 12, y + 12)
                    if any(overlap(r, ob) or overlap(badge, ob) for ob in obstacles):
                        continue
                    old_segments = [seg for e in self.edges for seg in zip(e[4], e[4][1:])]
                    if any(intersects(*seg, r) or intersects(*seg, badge) for seg in old_segments):
                        continue
                    if any(intersects(*seg, r) for seg in segments):
                        continue
                    candidates.append((length, x, y, r, badge))
        if not candidates:
            return self.retry(source, target, code, label, secondary, attempt)
        _, x, y, r, badge = max(candidates)
        self.labels.extend((r, badge))
        self.edges.append((source, target, code, label, points, (x, y, r), secondary))

    def retry(self, source, target, code, label, secondary, attempt):
        """Try another port after routing or label placement fails."""
        if attempt >= 24:
            raise ValueError(f"Cannot route and label {code}: {label}")
        return self.edge(source, target, code, label, secondary, attempt + 1)

    def check(self):
        """Reject diagonal routes, obstructed units and labels touching any route."""
        units = list(self.boxes.values())
        for i, box in enumerate(units):
            assert all(not overlap(box.rect, v.rect) for v in units[i + 1 :])
        for edge_index, (_, _, code, _, points, (_, _, label), _) in enumerate(self.edges):
            for a, b in zip(points, points[1:]):
                assert a != b and (a[0] == b[0] or a[1] == b[1]), code
                assert not any(intersects(a, b, v.rect) for v in units), code
                assert not any(intersects(a, b, r) for r in self.labels[::2]), code
                assert not any(
                    intersects(a, b, r) for i, r in enumerate(self.labels[1::2]) if i != edge_index
                ), code
            assert not any(overlap(label, v.rect) for v in units), code
        for i, r in enumerate(self.labels):
            assert not any(overlap(r, s) for s in self.labels[i + 1 :]), (i, r)
        return f"{self.slug}: {len(units)} units, {len(self.edges)} routes; geometry PASS"

    def render(self, root):
        """Validate and write a standalone accessible HTML document."""
        self.check()

        def txt(x, y, text, size=11, color="#4f5d75", anchor="start", cls="mono"):
            """Format SVG text."""
            return f'<text x="{x}" y="{y}" class="{cls}" font-size="{size}" fill="{color}" text-anchor="{anchor}">{escape(text)}</text>'

        svg = [
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.width} {self.height}" role="img" aria-labelledby="title desc"><title id="title">{escape(self.title)}</title><desc id="desc">Các unit triển khai local của nền tảng dữ liệu và AI. Đọc mã số và tên dữ liệu trên từng mũi tên; khung phân chia Compose, kind và bên ngoài. Chú thích các luồng ở cuối hình.</desc><defs>'
        ]
        for key, color in COLORS.items():
            svg.append(
                f'<marker id="arrow-{key}" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0 0 L8 4 L0 8" fill="{color}"/></marker>'
            )
        svg.append(f'</defs><rect width="100%" height="100%" fill="{BG}"/>')
        svg.append(txt(30, 25, "Data & AI Platform · KIẾN TRÚC TRIỂN KHAI LOCAL", 11))
        svg.append(txt(30, 59, self.title, 28, "#2d3142", cls="serif"))
        for x, y, w, h, label, blue in self.frames:
            svg.append(
                f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{"#e9edf5" if blue else "#eeeef1"}" stroke="#bbc1cc"/>'
            )
            label_x = x + w - 280 if w == 630 and label.startswith("DOCKER COMPOSE") else x + 16
            svg.append(txt(label_x, y + 23, label, 10))
        for _, _, code, label, pts, (x, y, r), secondary in self.edges:
            color = COLORS[code[0]]
            # Quadratic corner arcs preserve orthogonal straight segments.
            path = f"M{pts[0][0]} {pts[0][1]}"
            for i, p in enumerate(pts[1:-1], 1):
                u, v = pts[i - 1], pts[i + 1]
                radius = min(
                    6,
                    (abs(p[0] - u[0]) + abs(p[1] - u[1])) / 2,
                    (abs(v[0] - p[0]) + abs(v[1] - p[1])) / 2,
                )
                before = tuple(
                    p[j] + (radius if u[j] > p[j] else -radius if u[j] < p[j] else 0)
                    for j in (0, 1)
                )
                after = tuple(
                    p[j] + (radius if v[j] > p[j] else -radius if v[j] < p[j] else 0)
                    for j in (0, 1)
                )
                path += f" L{before[0]} {before[1]} Q{p[0]} {p[1]} {after[0]} {after[1]}"
            path += f" L{pts[-1][0]} {pts[-1][1]}"
            dash = ' stroke-dasharray="5 4"' if secondary else ""
            svg.append(
                f'<path d="{path}" fill="none" stroke="{color}" stroke-width="1.7"{dash} marker-end="url(#arrow-{code[0]})"/>'
            )
            svg.append(f'<circle cx="{x}" cy="{y}" r="11" fill="{BG}" stroke="{color}"/>')
            svg.append(txt(x, y + 3, code, 8, color, "middle"))
            svg.append(txt((r[0] + r[2]) / 2, r[3] - 5, label, 10, color, "middle"))
        for box in self.boxes.values():
            dash = ' stroke-dasharray="5 4"' if box.support else ""
            svg.append(
                f'<rect x="{box.x}" y="{box.y}" width="{box.w}" height="{box.h}" rx="7" fill="white" stroke="#9aa3b3"{dash}/>'
            )
            svg.append(
                f'<rect x="{box.x + 7}" y="{box.y + 5}" width="{len(box.tag) * 5.5 + 10}" height="15" rx="2" fill="none" stroke="#d0d4dc"/>'
            )
            svg.append(txt(box.x + 12, box.y + 16, box.tag, 8))
            svg.append(txt(box.x + 110, box.y + 38, box.name, 14, "#2d3142", "middle", "sans"))
            for i, line in enumerate(box.detail):
                svg.append(txt(box.x + 110, box.y + 55 + 13 * i, line, 9, anchor="middle"))
        for x, y, note in self.notes:
            svg.append(txt(x, y, note, 11))
        y = self.height - 230
        svg.append(f'<path d="M30 {y - 25} H{self.width - 30}" stroke="#c5cad3"/>')
        svg.append(txt(30, y, "FLOWS · LUỒNG DỮ LIỆU", 10))
        for i, (key, name, detail) in enumerate(self.flows):
            yy = y + 30 + i * 27
            color = COLORS[key]
            svg.append(f'<circle cx="42" cy="{yy - 4}" r="10" fill="white" stroke="{color}"/>')
            svg.extend(
                (
                    txt(42, yy - 1, key, 9, color, "middle"),
                    txt(65, yy, name, 12, color, cls="sans"),
                    txt(340, yy, detail, 11),
                )
            )
        svg.append("</svg>")
        html = f"""<!doctype html>
<html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(self.title)}</title><style>
*{{box-sizing:border-box}}body{{margin:0;padding:28px;background:{BG};color:#2d3142;font-family:Geist,system-ui,sans-serif}}
.eyebrow,.mono{{font-family:'Geist Mono',ui-monospace,monospace}}.eyebrow{{font-size:11px;letter-spacing:.16em}}h1{{font:normal 30px 'Instrument Serif',Georgia,serif;margin:10px 0 18px}}svg{{display:block;width:100%;min-width:1600px}}.serif{{font-family:Georgia,serif}}.sans{{font-family:Geist,system-ui,sans-serif}}main{{overflow:auto}}@media print{{body{{padding:0}}svg{{min-width:0}}}}
</style><main>{"".join(svg)}</main></html>"""
        (root / f"{self.slug}.html").write_text(html, encoding="utf-8")


ROOT = Path(__file__).resolve().parents[2]
