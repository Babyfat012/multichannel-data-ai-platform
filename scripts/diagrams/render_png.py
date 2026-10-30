"""Render AWS SVG-in-HTML offline with optional native librsvg and Cairo libraries.

Run after build.py: python3 scripts/diagrams/render_png.py. No network or pip needed.
"""

import ctypes as c
import re
from ctypes.util import find_library

from common import ROOT


class Rectangle(c.Structure):
    """Represent the librsvg viewport ABI."""

    _fields_ = [(key, c.c_double) for key in ("x", "y", "width", "height")]


def render(path):
    """Rasterize one embedded SVG and release all native resources."""
    rsvg = c.CDLL(find_library("rsvg-2"))
    cairo = c.CDLL(find_library("cairo"))
    gobject = c.CDLL(find_library("gobject-2.0"))
    rsvg.rsvg_handle_new_from_data.argtypes = [c.c_char_p, c.c_size_t, c.c_void_p]
    rsvg.rsvg_handle_new_from_data.restype = c.c_void_p
    rsvg.rsvg_handle_render_document.argtypes = [
        c.c_void_p,
        c.c_void_p,
        c.POINTER(Rectangle),
        c.c_void_p,
    ]
    rsvg.rsvg_handle_render_document.restype = c.c_int
    cairo.cairo_image_surface_create.argtypes = [c.c_int, c.c_int, c.c_int]
    cairo.cairo_image_surface_create.restype = c.c_void_p
    cairo.cairo_create.argtypes = [c.c_void_p]
    cairo.cairo_create.restype = c.c_void_p
    cairo.cairo_surface_write_to_png.argtypes = [c.c_void_p, c.c_char_p]
    cairo.cairo_surface_write_to_png.restype = c.c_int
    cairo.cairo_destroy.argtypes = [c.c_void_p]
    cairo.cairo_surface_destroy.argtypes = [c.c_void_p]
    gobject.g_object_unref.argtypes = [c.c_void_p]
    svg = re.search(r"<svg\b.*?</svg>", path.read_text(), re.S).group()
    width, height = map(int, re.search(r'viewBox="0 0 (\d+) (\d+)"', svg).groups())
    data = svg.encode()
    handle = rsvg.rsvg_handle_new_from_data(data, len(data), None)
    if not handle:
        raise RuntimeError(f"Invalid SVG: {path}")
    surface = cairo.cairo_image_surface_create(0, width, height)
    context = cairo.cairo_create(surface)
    try:
        if not rsvg.rsvg_handle_render_document(
            handle, context, c.byref(Rectangle(0, 0, width, height)), None
        ):
            raise RuntimeError(f"Rendering failed: {path}")
        output = path.with_suffix(".png")
        status = cairo.cairo_surface_write_to_png(surface, str(output).encode())
        if status:
            raise RuntimeError(f"PNG write failed ({status}): {output}")
        print(f"{output.name}: {width}×{height} PNG")
    finally:
        cairo.cairo_destroy(context)
        cairo.cairo_surface_destroy(surface)
        gobject.g_object_unref(handle)


def main():
    """Render the two AWS artifacts without changing existing local PNGs."""
    for name in ("aws-architecture-layer12", "aws-architecture-layer3"):
        render(ROOT / "docs" / "diagrams" / f"{name}.html")


if __name__ == "__main__":
    main()
