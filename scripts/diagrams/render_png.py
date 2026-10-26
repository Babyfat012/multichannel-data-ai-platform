"""Optional offline PNG export via system librsvg/Cairo (Python stdlib only).

Run build.py first, then python3 scripts/diagrams/render_png.py. This optional
Linux preview requires installed librsvg-2.so.2 and libcairo.so.2, and renders the
embedded SVG including its heading. HTML generation has no native dependency.
Planner can instead screenshot the HTML with headless Chrome.
"""

import ctypes as ct
import re

from common import ROOT


def main():
    """Render both embedded SVGs without network or a browser process."""
    rsvg = ct.CDLL("librsvg-2.so.2")
    cairo = ct.CDLL("libcairo.so.2")
    gobject = ct.CDLL("libgobject-2.0.so.0")
    rsvg.rsvg_handle_new_from_data.argtypes = [ct.c_char_p, ct.c_size_t, ct.c_void_p]
    rsvg.rsvg_handle_new_from_data.restype = ct.c_void_p
    rsvg.rsvg_handle_render_cairo.argtypes = [ct.c_void_p, ct.c_void_p]
    rsvg.rsvg_handle_render_cairo.restype = ct.c_int
    cairo.cairo_image_surface_create.argtypes = [ct.c_int, ct.c_int, ct.c_int]
    cairo.cairo_image_surface_create.restype = ct.c_void_p
    cairo.cairo_create.argtypes = [ct.c_void_p]
    cairo.cairo_create.restype = ct.c_void_p
    cairo.cairo_surface_write_to_png.argtypes = [ct.c_void_p, ct.c_char_p]
    cairo.cairo_destroy.argtypes = [ct.c_void_p]
    cairo.cairo_surface_destroy.argtypes = [ct.c_void_p]
    gobject.g_object_unref.argtypes = [ct.c_void_p]
    for slug in ("data-ml-platform", "llm-platform"):
        source = ROOT / "docs" / "diagrams" / f"{slug}.html"
        html = source.read_text(encoding="utf-8")
        svg = re.search(r"<svg.*?</svg>", html, re.S).group()
        style = re.search(r"<style>(.*?)</style>", html, re.S).group(1)
        svg = svg.replace("<defs>", "<style>" + style + "</style><defs>")
        width, height = map(int, re.search(r'viewBox="0 0 (\d+) (\d+)"', svg).groups())
        data = svg.encode("utf-8")
        handle = rsvg.rsvg_handle_new_from_data(data, len(data), None)
        if not handle:
            raise RuntimeError(f"Invalid SVG: {slug}")
        surface = cairo.cairo_image_surface_create(0, width, height)
        context = cairo.cairo_create(surface)
        try:
            if not rsvg.rsvg_handle_render_cairo(handle, context):
                raise RuntimeError(f"Render failed: {slug}")
            output = source.with_suffix(".png")
            if cairo.cairo_surface_write_to_png(surface, str(output).encode()) != 0:
                raise RuntimeError(f"PNG write failed: {slug}")
            print(output.relative_to(ROOT))
        finally:
            cairo.cairo_destroy(context)
            cairo.cairo_surface_destroy(surface)
            gobject.g_object_unref(handle)


if __name__ == "__main__":
    main()
