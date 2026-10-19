"""Ghi dữ liệu ra thư mục local hoặc object storage (MinIO, S3) qua fsspec.

fsspec cho cùng một cách mở file với mọi nơi lưu: `./out/a.json` hay
`s3://bucket/a.json` đều dùng `fs.open(...)`. Nhờ vậy code sinh dữ liệu
không cần biết đang ghi vào đâu, chỉ cần đổi `storage.output_root` trong cấu hình.

Về độ bền (durability): `write` của Python chỉ đưa dữ liệu vào bộ nhớ đệm của hệ
điều hành, chưa chắc xuống đĩa. Mất điện lúc đó là mất phần chưa xuống. Nên mọi
chỗ ghi mà sau đó có thứ khác dựa vào ("đã ghi xong") đều gọi `_fsync`, và
`write_json_atomic` dùng ghi-tạm-rồi-đổi-tên để không bao giờ để lại file ghi dở.
Trên object store thì không có khái niệm này: dữ liệu bền ngay khi `close` tải xong,
nên `_fsync` trả về False và đó là đúng, không phải lỗi.
"""

from __future__ import annotations

import io
import json
import os
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq


def _fsync(handle: Any) -> bool:
    """Ép hệ điều hành ghi nội dung của `handle` xuống đĩa thật, ngay.

    Trả về True nếu làm được. File trên object store không có `fileno()` nên trả
    về False — ở đó độ bền đến từ lúc `close` tải xong, không từ `fsync`.
    """
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
        """Ép dữ liệu xuống đĩa rồi đóng file, trả về `(số dòng, số byte)` đã ghi.

        `fsync` trước khi đóng vì bên gọi dùng lúc đóng làm mốc "ngày này xong rồi"
        và ghi một dấu hiệu bền ngay sau đó. Không `fsync` thì mất điện có thể để
        lại dấu hiệu "xong" mà file thì thiếu đuôi — mất dữ liệu mà không ai báo.
        """
        _fsync(self._handle)
        self._handle.close()
        return self.rows, self.bytes


class JsonlWriterPool:
    """Giữ nhiều file JSON Lines mở cùng lúc, mỗi file một khóa do bên gọi đặt.

    Dùng khi các file phải nhận dòng xen kẽ nhau nên không file nào gom trọn được
    trong RAM. Ví dụ ở Batch: một Order tạo hôm nay có thể chỉ export sau hai tuần
    (lúc giao xong), nên cả hai tuần ngày export đều đang chờ dòng mới. Gom theo
    ngày thì RAM tỉ lệ với số đơn mỗi ngày; mở sẵn file thì RAM không đổi.

    Bên gọi đóng từng khóa khi biết nó không còn nhận dòng nào nữa. Pool đóng mọi
    file còn lại khi ra khỏi khối `with` của `OutputStore.jsonl_writers`.
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
        """Ghi `record` vào file của `key`, mở `relative` nếu đây là dòng đầu tiên.

        Ghi vào một khóa đã đóng là lỗi lập trình, không phải việc cần làm lại: mở
        lại `relative` sẽ xóa trắng file đã ghi xong. Nên ném lỗi ngay thay vì âm
        thầm mất dữ liệu — bên gọi chỉ được đóng khóa khi chắc nó hết dòng.
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
            # Sau khối `with` của ParquetWriter là lúc footer đã nằm trong `sink`;
            # `fsync` ở đây để file đọc được sau khi mất điện, cùng lý do như
            # `JsonlAppender.close`.
            _fsync(sink)

    def write_json_atomic(self, relative: str, obj: Any) -> int:
        """Ghi JSON theo kiểu "ghi tạm rồi đổi tên": không bao giờ để lại file ghi dở.

        Ghi thẳng vào `relative` có một khoảng thời gian file đã tồn tại nhưng nội
        dung còn thiếu. Ai đọc đúng lúc đó sẽ thấy JSON vỡ — hoặc tệ hơn, thấy một
        file hợp lệ nhưng thiếu dữ liệu. Đổi tên thì khác: hệ thống file làm nó xong
        hẳn hoặc không làm, không có trạng thái giữa. Nên `relative` chỉ hiện ra khi
        nội dung đã đầy đủ và đã nằm trên đĩa.

        Thứ tự bắt buộc: ghi tạm -> `fsync` file tạm -> đổi tên -> `fsync` thư mục
        cha. Thiếu bước cuối thì cái tên mới có thể chưa bền dù nội dung đã bền.
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
        """Check for an output object without creating or modifying it."""
        return self.fs.exists(self.path(relative))

    def remove(self, relative: str) -> None:
        """Xóa một file. Không có file thì coi như xong, không ném lỗi."""
        try:
            self.fs.rm_file(self.path(relative))
        except FileNotFoundError:
            pass

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
