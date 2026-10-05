"""Ghi dữ liệu ra thư mục local hoặc object storage (MinIO, S3) qua fsspec.

fsspec cho cùng một cách mở file với mọi nơi lưu: `./out/a.json` hay
`s3://bucket/a.json` đều dùng `fs.open(...)`. Nhờ vậy code sinh dữ liệu
không cần biết đang ghi vào đâu, chỉ cần đổi `storage.output_root` trong cấu hình.
"""

from __future__ import annotations

import io
import json
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetAppender:
    """Bút ghi dần vào một file Parquet đang mở: mỗi lần `write` là một row group.

    Row group là khối dữ liệu độc lập mà Spark đọc hoặc bỏ qua theo thống kê
    min/max của nó. Nhiều khối bé thì metadata nở và Spark phải mở nhiều khối;
    một khối khổng lồ thì bên ghi phải giữ cả khối trong RAM. Mốc thường dùng là
    khoảng một triệu dòng mỗi khối.
    """

    def __init__(self, writer: pq.ParquetWriter, schema: pa.Schema) -> None:
        """Bọc một `ParquetWriter` đã mở. Vòng đời file do `OutputStore` giữ."""
        self._writer = writer
        self._schema = schema
        self.rows = 0
        self.row_groups = 0

    def write(self, columns: dict[str, list[Any]]) -> int:
        """Ghi một khối dạng cột thành row group mới. Trả về số dòng đã ghi.

        Khối rỗng bị bỏ qua: một row group không dòng nào chỉ làm nở metadata.
        """
        rows = len(next(iter(columns.values()), ()))
        if not rows:
            return 0
        self._writer.write_table(pa.table(columns, schema=self._schema))
        self.rows += rows
        self.row_groups += 1
        return rows


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

    @contextmanager
    def parquet_writer(self, relative: str, schema: pa.Schema) -> Iterator[ParquetAppender]:
        """Mở `relative` để ghi dần, trả về `ParquetAppender` dùng trong khối `with`.

        Dùng khi bảng quá lớn để giữ trọn trong RAM (xem `write_parquet` cho bảng
        nhỏ). `OutputStore` giữ trách nhiệm đóng file: footer Parquet chỉ được ghi
        lúc `close`, nên một writer không đóng sẽ để lại file Spark đọc không được.
        Khối `with` đảm bảo điều đó cả khi thân vòng lặp ném lỗi.
        """
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(full, "wb") as sink:
            with pq.ParquetWriter(sink, schema) as writer:
                yield ParquetAppender(writer, schema)

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
