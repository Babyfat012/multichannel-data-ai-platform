"""Ghi dữ liệu ra thư mục local hoặc object storage (MinIO, S3) qua fsspec.

Chỗ nào có thứ khác dựa vào "đã ghi xong" thì gọi `_fsync`; `write_json_atomic` ghi tạm rồi
đổi tên để không để lại file ghi dở. Trên object store dữ liệu bền khi `close` tải xong.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


def _fsync(handle: Any) -> bool:
    """Ép OS ghi `handle` xuống đĩa; trả False với file trên object store (không có `fileno()`)."""
    try:
        handle.flush()
        os.fsync(handle.fileno())
    except (AttributeError, OSError, ValueError, io.UnsupportedOperation):
        return False
    return True


class JsonlAppender:
    """Bút ghi dần vào một file JSON Lines đang mở. Đếm số dòng và số byte đã ghi."""

    def __init__(self, handle: Any) -> None:
        """Bọc một file đã mở ở chế độ nhị phân. Vòng đời file do pool giữ."""
        self._handle = handle
        self.rows = 0
        self.bytes = 0

    def write(self, record: dict[str, Any]) -> int:
        """Ghi một bản ghi thành một dòng JSON. Trả về số byte của dòng đó."""
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        data = line.encode("utf-8")
        self._handle.write(data)
        self.rows += 1
        self.bytes += len(data)
        return len(data)

    def close(self) -> tuple[int, int]:
        """`fsync` rồi đóng file, trả `(số dòng, số byte)`.

        Bên gọi ghi dấu "ngày này xong" ngay sau đó, nên file phải bền trước.
        """
        _fsync(self._handle)
        self._handle.close()
        return self.rows, self.bytes


class JsonlWriterPool:
    """Giữ nhiều file JSON Lines mở cùng lúc, mỗi file một khóa.

    Dùng khi các file nhận dòng xen kẽ: một Order hôm nay có thể export sau hai tuần, nên
    mở sẵn file giữ RAM không đổi thay vì gom theo ngày.
    """

    def __init__(self, store: OutputStore) -> None:
        """Pool rỗng, ghi vào `store`."""
        self._store = store
        self._open: dict[Any, JsonlAppender] = {}
        self._closed: set[Any] = set()

    def keys(self) -> list[Any]:
        """Các khóa đang mở. Trả về bản chụp để bên gọi vừa duyệt vừa đóng được."""
        return list(self._open)

    def write(self, key: Any, relative: str, record: dict[str, Any]) -> int:
        """Ghi `record` vào file của `key`, mở `relative` ở dòng đầu tiên.

        Ghi vào khóa đã đóng thì ném lỗi: mở lại sẽ xóa trắng file đã xong.
        """
        appender = self._open.get(key)
        if appender is None:
            if key in self._closed:
                raise ValueError(f"khóa {key!r} đã đóng, ghi thêm sẽ xóa trắng {relative}")
            appender = self._open[key] = JsonlAppender(self._store.open_write(relative))
        return appender.write(record)

    def close(self, key: Any) -> tuple[int, int]:
        """Đóng file của `key`, trả về `(số dòng, số byte)`. Khóa chưa mở trả `(0, 0)`."""
        appender = self._open.pop(key, None)
        self._closed.add(key)
        return appender.close() if appender is not None else (0, 0)

    def close_all(self) -> None:
        """Đóng mọi file còn mở. Gọi khi kết thúc, kể cả khi đang có lỗi."""
        while self._open:
            self.close(next(iter(self._open)))


class ParquetAppender:
    """Ghi dần vào một file Parquet: mỗi lần `write` là một row group."""

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
        return self._write_bytes(
            relative, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        )

    def open_write(self, relative: str) -> Any:
        """Mở `relative` để ghi nhị phân, tự tạo thư mục cha. Bên gọi phải đóng."""
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        return self.fs.open(full, "wb")

    @contextmanager
    def jsonl_writers(self) -> Iterator[JsonlWriterPool]:
        """Mở một `JsonlWriterPool`; mọi file còn lại được đóng khi ra khỏi khối."""
        pool = JsonlWriterPool(self)
        try:
            yield pool
        finally:
            pool.close_all()

    @contextmanager
    def parquet_writer(self, relative: str, schema: pa.Schema) -> Iterator[ParquetAppender]:
        """Mở `relative` để ghi dần, trả `ParquetAppender` dùng trong khối `with`.

        Footer Parquet chỉ được ghi lúc `close`, nên khối `with` đóng file cả khi có lỗi.
        """
        full = self.path(relative)
        self.fs.makedirs(full.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(full, "wb") as sink:
            with pq.ParquetWriter(sink, schema) as writer:
                yield ParquetAppender(writer, schema)
            # Footer đã nằm trong `sink`; fsync như `JsonlAppender.close`.
            _fsync(sink)

    def write_json_atomic(self, relative: str, obj: Any) -> int:
        """Ghi JSON kiểu ghi tạm rồi đổi tên, để người đọc không thấy file ghi dở.

        Thứ tự: ghi tạm -> `fsync` file tạm -> đổi tên -> `fsync` thư mục cha.
        """
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        full = self.path(relative)
        parent = full.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)
        tmp = f"{full}.tmp"
        with self.fs.open(tmp, "wb") as f:
            f.write(data)
            _fsync(f)
        if self._is_local:
            os.replace(tmp, full)
        else:
            self.fs.mv(tmp, full)
        self._fsync_dir(parent)
        return len(data)

    def read_json(self, relative: str) -> Any:
        """Đọc một file JSON đã ghi."""
        with self.fs.open(self.path(relative), "rt", encoding="utf-8") as f:
            return json.load(f)

    def exists(self, relative: str) -> bool:
        """Kiểm file đầu ra có tồn tại không, không tạo hay sửa gì."""
        return self.fs.exists(self.path(relative))

    def remove(self, relative: str) -> None:
        """Xóa một file. Không có file thì coi như xong, không ném lỗi."""
        with suppress(FileNotFoundError):
            self.fs.rm_file(self.path(relative))

    @property
    def _is_local(self) -> bool:
        """True khi đang ghi vào thư mục local, nơi `os.replace` dùng được."""
        protocol = self.fs.protocol
        return "file" in (protocol if isinstance(protocol, tuple) else (protocol,))

    def _fsync_dir(self, path: str) -> None:
        """Ép thư mục xuống đĩa, để cái tên vừa đổi cũng bền. Bỏ qua nếu không local."""
        if not self._is_local:
            return
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

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
