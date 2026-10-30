"""Kiểm mô tả mã nguồn, liên kết tương đối và độ phủ tài liệu công khai."""

import ast
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
SKIP_PARTS = {".git", ".venv", "out", "__pycache__", ".pytest_cache"}
COMMENT_PREFIXES = {
    ".sh": ("#",),
    ".sql": ("--", "/*"),
    ".yml": ("#",),
    ".yaml": ("#",),
    ".tf": ("#", "//", "/*"),
    ".properties": ("#", "!"),
}


def python_files():
    """Liệt kê Python được Git theo dõi, bỏ test và cây mutant sinh tự động."""
    result = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.py"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return sorted(
        ROOT / name
        for name in result.stdout.decode().split("\0")
        if name
        and not (SKIP_PARTS | {"tests", "mutants"}).intersection(Path(name).parts)
        and (ROOT / name).is_file()
    )


def documented_nodes(tree):
    """Duyệt module/class và hàm, dừng ở thân hàm để bỏ qua định nghĩa lồng."""
    for child in ast.iter_child_nodes(tree):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield child
        else:
            if isinstance(child, ast.ClassDef):
                yield child
            yield from documented_nodes(child)


@pytest.mark.parametrize("path", python_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_python_docstrings(path):
    """Mọi module, class, hàm và method ngoài hàm lồng phải có docstring."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    missing = [] if ast.get_docstring(tree) else ["module:1"]
    missing.extend(
        f"{node.name}:{node.lineno}"
        for node in documented_nodes(tree)
        if not ast.get_docstring(node)
    )
    assert not missing, f"{path.relative_to(ROOT)}: thiếu docstring {', '.join(missing)}"


def described_files():
    """Tìm script/cấu hình thuộc repo, dùng Git để bỏ qua tài liệu cá nhân và output."""
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return sorted(
        {
            ROOT / name
            for name in result.stdout.decode().split("\0")
            if name
            and Path(name).suffix in COMMENT_PREFIXES
            and not SKIP_PARTS.intersection(Path(name).parts)
            and (ROOT / name).is_file()
        }
    )


@pytest.mark.parametrize("path", described_files(), ids=lambda p: str(p.relative_to(ROOT)))
def test_file_purpose_comment(path):
    """Dòng nội dung đầu sau shebang phải là chú thích có mô tả, không chỉ dấu phân cách."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if lines and lines[0].startswith("#!"):
        lines = lines[1:]
    first = next((line.strip() for line in lines if line.strip()), "")
    prefixes = COMMENT_PREFIXES[path.suffix]
    assert any(
        first.startswith(prefix) and re.search(r"\w", first[len(prefix) :]) for prefix in prefixes
    ), f"{path.relative_to(ROOT)}: thiếu mô tả đầu file"


def markdown_targets(path):
    """Lấy đích link/image inline và reference, bỏ qua ví dụ trong code fence."""
    source = path.read_text(encoding="utf-8")
    source = re.sub(r"(?ms)^\s*(`{3,}|~{3,})[^\n]*\n.*?^\s*\1\s*$", "", source)
    # Nhãn có thể chứa backtick; chỉ đích link mới được giải mã URL.
    inline = re.findall(r"!?\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)", source)
    references = re.findall(r"(?m)^\s{0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)", source)
    for target in inline + references:
        url = urlsplit(target.strip("<>"))
        if not url.scheme and not url.netloc and url.path:
            yield (path.parent / unquote(url.path)).resolve()


def ignored_targets(targets):
    """Dùng chính quy tắc gitignore để phân loại cả đích chưa tồn tại."""
    relative = [str(path.relative_to(ROOT)) for path in targets if path.is_relative_to(ROOT)]
    if not relative:
        return set()
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "--stdin", "-z"],
        cwd=ROOT,
        input="\0".join(relative) + "\0",
        text=True,
        capture_output=True,
    )
    assert result.returncode in (0, 1), result.stderr
    return {ROOT / name for name in result.stdout.split("\0") if name}


def test_relative_markdown_links(record_property):
    """Link tương đối phải tồn tại; đếm riêng lượt link bị ignore theo yêu cầu brief."""
    links = [
        (source, target)
        for source in [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]
        for target in markdown_targets(source)
    ]
    ignored = ignored_targets({target for _, target in links})
    record_property("ignored_markdown_link_count", sum(target in ignored for _, target in links))
    missing = [
        f"{source.relative_to(ROOT)} -> {target.relative_to(ROOT) if target.is_relative_to(ROOT) else target}"
        for source, target in links
        if target not in ignored and not target.is_file()
    ]
    assert not missing, "Link tương đối hỏng:\n" + "\n".join(missing)


def test_readme_reaches_all_public_docs():
    """Mọi Markdown công khai trong docs/ phải truy cập được từ README qua các link tài liệu."""
    documents = set((ROOT / "docs").rglob("*.md"))
    public = documents - ignored_targets(documents)
    visited = set()
    pending = [ROOT / "README.md"]
    while pending:
        source = pending.pop()
        if source in visited:
            continue
        visited.add(source)
        pending.extend(
            target
            for target in markdown_targets(source)
            if target.is_relative_to(ROOT)
            and target.suffix == ".md"
            and target.is_file()
            and target not in visited
            and target not in ignored_targets({target})
        )
    assert not (public - visited), "Thiếu link tài liệu: " + ", ".join(
        str(path.relative_to(ROOT)) for path in sorted(public - visited)
    )
