"""Ghi dữ liệu ra thư mục local hoặc object storage (MinIO, S3) qua fsspec.

fsspec cho cùng một cách mở file với mọi nơi lưu: `./out/a.json` hay
`s3://bucket/a.json` đều dùng `fs.open(...)`. Nhờ vậy code sinh dữ liệu
không cần biết đang ghi vào đâu, chỉ cần đổi `storage.output_root` trong cấu hình.
"""

from __future__ import annotations

import io
import json
from typing import Any, Iterable, Iterator

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


class OutputStore:
    """Nơi ghi toàn bộ đầu ra của một lần chạy, gốc tại `root`."""

    def __init__(self, root: str) -> None:
        """Mở hệ thống file ứng với URL `root` (local, s3://, ...)."""
        self.fs, self.root = fsspec.core.url_to_fs(root)

    def path(self, relative: str) -> str:
        """Đường dẫn đầy đủ của `relative` bên dưới thư mục gốc."""
        return f"{self.root.rstrip('/')}/{relative}"

    def write_jsonl(self, relative: str, records: Iterable[dict[str, Any]]) -> int:
        """Ghi mỗi bản ghi thành một dòng JSON. Trả về số byte đã ghi."""
        buffer = io.StringIO()
        for record in records:
            buffer.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            buffer.write("\n")
        return self._write_bytes(relative, buffer.getvalue().encode("utf-8"))

    def write_json(self, relative: str, obj: Any) -> int:
        """Ghi một đối tượng thành file JSON dễ đọc (thụt lề). Trả về số byte."""
        return self._write_bytes(relative, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))

    def write_parquet(self, relative: str, columns: dict[str, list[Any]], schema: pa.Schema | None = None) -> int:
        """Ghi bảng dạng cột (tên cột → danh sách giá trị) thành file Parquet.

        Nên truyền `schema` để kiểu cột cố định. Không có schema thì pyarrow tự đoán
        kiểu, và một bảng rỗng sẽ bị đoán thành kiểu `null`.
        """
        table = pa.table(columns, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return self._write_bytes(relative, sink.getvalue().to_pybytes())

    def glob(self, pattern: str) -> list[str]:
        """Các đường dẫn tương đối (so với gốc) khớp `pattern`, đã sắp xếp."""
        prefix = self.path("")
        return sorted(p.removeprefix(prefix) for p in self.fs.glob(self.path(pattern)))

    def read_jsonl(self, relative: str) -> Iterator[dict[str, Any]]:
        """Đọc lần lượt từng bản ghi của một file JSON Lines."""
        with self.fs.open(self.path(relative), "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    def _write_bytes(self, relative: str, data: bytes) -> int:
        """Ghi `data` vào `relative`, tự tạo thư mục cha nếu cần."""
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(full, "wb") as f:
            f.write(data)
        return len(data)
