"""Distribute importable Python packages to Spark workers without flattening modules."""

import tempfile
from hashlib import sha256
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def distribute_python_packages(context, *, include_tests=False):
    """Ship a content-addressed zip with package roots; local mounts remain usable too."""
    root = Path(__file__).resolve().parents[2]
    packages = ("spark_jobs", "tests") if include_tests else ("spark_jobs",)
    files = sorted(
        path
        for package in packages
        for path in (root / package).rglob("*.py")
        if "__pycache__" not in path.parts
    )
    digest = sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    archive = Path(tempfile.gettempdir()) / f"platform-python-{digest.hexdigest()}.zip"
    if not archive.exists():
        with ZipFile(archive, "w", compression=ZIP_DEFLATED) as bundle:
            for path in files:
                bundle.write(path, path.relative_to(root).as_posix())
    context.addPyFile(str(archive))
