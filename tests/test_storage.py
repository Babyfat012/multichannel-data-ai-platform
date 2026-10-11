"""Test lớp ghi: các bảo đảm của bút ghi dần mà phần Batch dựa vào.

Các bảo đảm này là lý do `BatchRun` dám bỏ bộ đệm bản ghi và dám tin vào tem. Nếu một cái vỡ thì
đầu ra ở quy mô 100 GB hỏng sau nhiều giờ chạy, nên chúng được khóa ở đây chứ
không chỉ kiểm qua một lần chạy Batch nhỏ.
"""

from __future__ import annotations

import json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from generator.storage import OutputStore

SCHEMA = pa.schema([("n", pa.int64())])


def test_jsonl_pool_reports_bytes_matching_the_file_on_disk(tmp_path):
    """`close` trả về số byte thật, vì manifest lấy `datasets[*].bytes` từ đó."""
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        pool.write("a", "a.jsonl", {"x": 1})
        pool.write("a", "a.jsonl", {"x": "é"})
        rows, written = pool.close("a")

    on_disk = (tmp_path / "a.jsonl").stat().st_size
    assert (rows, written) == (2, on_disk)


def test_jsonl_pool_keeps_files_interleaved_without_mixing_them(tmp_path):
    """Nhiều file mở cùng lúc nhận dòng xen kẽ mà không lẫn sang nhau."""
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        for i in range(3):
            pool.write("a", "a.jsonl", {"n": i})
            pool.write("b", "b.jsonl", {"n": -i})

    def read(name: str) -> list[int]:
        """Giá trị cột `n` đọc lại từ một file JSONL, để so nội dung."""
        return [json.loads(line)["n"] for line in (tmp_path / name).read_text().splitlines()]

    assert read("a.jsonl") == [0, 1, 2]
    assert read("b.jsonl") == [0, -1, -2]


def test_jsonl_pool_closes_open_files_when_the_body_raises(tmp_path):
    """Lỗi giữa đường vẫn đóng file, nên dòng đã ghi đọc lại được.

    Đây là lý do chọn ranh giới "Storage giữ trách nhiệm đóng file": một lần chạy
    100 GB dài vài giờ, và dữ liệu đã sinh không nên mất vì một lỗi ở cuối.
    """
    store = OutputStore(str(tmp_path))
    with pytest.raises(RuntimeError):
        with store.jsonl_writers() as pool:
            pool.write("a", "a.jsonl", {"x": 1})
            raise RuntimeError("ngày mô phỏng lỗi")

    assert [json.loads(l) for l in (tmp_path / "a.jsonl").read_text().splitlines()] == [{"x": 1}]


def test_jsonl_pool_refuses_to_write_to_a_closed_key(tmp_path):
    """Ghi lại vào khóa đã đóng phải ném lỗi, vì mở lại sẽ xóa trắng file.

    `BatchRun._flush` chỉ đóng ngày export <= ngày vừa sinh, nên hiện không xảy ra.
    Test này là cái lưới: nếu sau này có kiểu export lùi ngày, test đổ ngay thay vì
    mất một ngày dữ liệu sau nhiều giờ chạy.
    """
    store = OutputStore(str(tmp_path))
    with store.jsonl_writers() as pool:
        pool.write("a", "a.jsonl", {"x": 1})
        pool.close("a")
        with pytest.raises(ValueError, match="đã đóng"):
            pool.write("a", "a.jsonl", {"x": 2})

    assert (tmp_path / "a.jsonl").read_text().count("\n") == 1


def test_parquet_writer_makes_one_row_group_per_write_and_skips_empty(tmp_path):
    """Mỗi `write` là một row group; khối rỗng bị bỏ qua để metadata không nở."""
    store = OutputStore(str(tmp_path))
    with store.parquet_writer("t.parquet", SCHEMA) as writer:
        assert writer.write({"n": [1, 2]}) == 2
        assert writer.write({"n": []}) == 0
        assert writer.write({"n": [3]}) == 1
        assert (writer.rows, writer.row_groups) == (3, 2)

    opened = pq.ParquetFile(tmp_path / "t.parquet")
    assert opened.num_row_groups == 2
    assert opened.read().column("n").to_pylist() == [1, 2, 3]


def test_parquet_writer_closes_the_file_when_the_body_raises(tmp_path):
    """Footer Parquet được ghi dù thân khối lỗi, nên file vẫn đọc được."""
    store = OutputStore(str(tmp_path))
    with pytest.raises(RuntimeError):
        with store.parquet_writer("t.parquet", SCHEMA) as writer:
            writer.write({"n": [1]})
            raise RuntimeError("ngày mô phỏng lỗi")

    assert pq.read_table(tmp_path / "t.parquet").column("n").to_pylist() == [1]


def test_write_json_atomic_leaves_no_temporary_file_behind(tmp_path):
    """Ghi tem xong thì không còn `.tmp` nào, vì tem được đổi tên vào chỗ chứ không chép."""
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("_checkpoint/day-00003.json", {"rows": 7})

    assert store.read_json("_checkpoint/day-00003.json") == {"rows": 7}
    assert not list(tmp_path.rglob("*.tmp"))


def test_write_json_atomic_replaces_an_existing_file_completely(tmp_path):
    """Tem mới đè hẳn tem cũ, không để lại đuôi của nội dung dài hơn trước đó.

    `BatchRun` ghi lại tem của cùng một ngày khi chạy lại, và tem cũ có thể dài hơn
    tem mới. Nếu phép đè chỉ ghi lên đầu file thì phần đuôi cũ còn lại và JSON vỡ.
    """
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("_checkpoint/day-00001.json", {"long": "x" * 500})
    store.write_json_atomic("_checkpoint/day-00001.json", {"n": 1})

    assert store.read_json("_checkpoint/day-00001.json") == {"n": 1}


def test_write_json_atomic_keeps_floats_exact_through_a_round_trip(tmp_path):
    """Số thực đi qua tem rồi về phải y nguyên, vì tổng giảm giá được cộng dồn qua tem.

    `amazon_discount_sum` là `float` cộng dồn suốt lần chạy. Lệch một bit ở đây thì
    manifest của lần chạy bị cắt khác lần chạy liền mạch, và phép so byte sẽ đổ —
    nên test này nói rõ vì sao nó không đổ.
    """
    store = OutputStore(str(tmp_path))
    value = 1234.5678901234567
    store.write_json_atomic("x.json", {"discount_sum": value})

    assert store.read_json("x.json")["discount_sum"] == value


def test_remove_is_quiet_when_the_file_is_already_gone(tmp_path):
    """Dọn file của ngày bị cắt phải chịu được việc file đã không còn ở đó."""
    store = OutputStore(str(tmp_path))
    store.write_json_atomic("a.json", {})
    store.remove("a.json")
    store.remove("a.json")

    assert store.glob("a.json") == []
