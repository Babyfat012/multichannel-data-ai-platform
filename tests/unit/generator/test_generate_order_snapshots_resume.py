"""Test chạy tiếp sau khi Batch bị cắt giữa đường.

Lần chạy 100 GB mất khoảng 3,2 giờ trên một core. Trong khoảng đó máy có thể phải
tắt. Các test ở đây khóa đúng một lời hứa: **bị cắt rồi chạy tiếp cho ra kết quả y
hệt chạy một lèo**, tới từng byte và từng con số trong manifest.

Cắt được mô phỏng bằng cách ném lỗi từ `write_json_atomic` — chỗ duy nhất ghi tem.
Hai kiểu cắt khác nhau về bản chất nên test cả hai:

- **Cắt sạch**: tem của ngày d đã ghi xong rồi mới chết. Trên đĩa mọi thứ nhất quán.
- **Cắt bẩn**: file của ngày d đã nằm trên đĩa nhưng tem CHƯA kịp ghi. Đây là trạng
  thái nguy hiểm: nếu lần chạy sau tin mấy file đó thì dữ liệu có thể thiếu một nửa.
  Đúng kiểu hỏng âm thầm — không lỗi, không test đỏ, chỉ sai số.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from generator.generate_order_snapshots import BatchRun


class _Cut(Exception):
    """Máy tắt. Không phải lỗi của code."""


def _run_with_cut(config, cut_at: int, *, stamp_first: bool) -> None:
    """Chạy Batch rồi cắt ở ngày `cut_at`.

    `stamp_first=True` là cắt sạch (tem ghi xong mới chết), `False` là cắt bẩn
    (chết đúng trước lúc dán tem, để lại file không có tem).
    """
    run = BatchRun(config)
    write_json_atomic = run.store.write_json_atomic

    def guarded(relative: str, obj):
        """Chặn đúng lúc ghi tem của ngày bị cắt: ném lỗi TRƯỚC khi ghi (cắt bẩn) hoặc SAU khi ghi (cắt sạch)."""
        is_stamp_of_cut_day = relative == f"_checkpoint/day-{cut_at:05d}.json"
        if is_stamp_of_cut_day and not stamp_first:
            raise _Cut
        written = write_json_atomic(relative, obj)
        if is_stamp_of_cut_day:
            raise _Cut
        return written

    run.store.write_json_atomic = guarded
    try:
        run.run()
    except _Cut:
        return
    raise AssertionError(f"khong cat duoc o ngay {cut_at}")


def _tree(root: Path) -> dict[str, bytes]:
    """Mọi file dưới `root`, đường dẫn tương đối -> nội dung byte.

    Bỏ `run_manifest.json` ra vì nó chứa `storage.output_root`, tức đường dẫn của
    chính lần chạy đó. So riêng bằng `_comparable`.
    """
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name != "run_manifest.json"
    }


def _comparable(manifest: dict) -> dict:
    """Manifest bỏ đi `output_root`, thứ duy nhất đúng là phải khác giữa hai thư mục."""
    stripped = json.loads(json.dumps(manifest))
    stripped["config"]["storage"].pop("output_root", None)
    return stripped


def _config_at(config, root: Path):
    """Cùng cấu hình nhưng ghi vào `root` khác."""
    return dataclasses.replace(
        config, storage=dataclasses.replace(config.storage, output_root=str(root))
    )


def _straight_run(config, root: Path) -> dict:
    """Chạy một lèo vào `root`, trả về manifest."""
    return BatchRun(_config_at(config, root)).run()


@pytest.mark.parametrize("cut", [False, True])
@pytest.mark.parametrize("changed", ["seed", "days_history", "orders_per_day"])
def test_resume_rejects_changed_dataset_before_mutation(tiny_config, tmp_path, cut, changed):
    """A resume must not relabel old Orders with a new catalog/config, even after a crash."""
    config = dataclasses.replace(
        _config_at(tiny_config, tmp_path / "guarded"),
        scale=dataclasses.replace(tiny_config.scale, days_history=2, orders_per_day=10),
    )
    if cut:
        _run_with_cut(config, cut_at=1, stamp_first=False)
    else:
        BatchRun(config).run()
    root = Path(config.storage.output_root)
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    modified = (
        dataclasses.replace(config, seed=config.seed + 1)
        if changed == "seed"
        else dataclasses.replace(
            config,
            scale=dataclasses.replace(
                config.scale, **{changed: getattr(config.scale, changed) + 1}
            ),
        )
    )
    with pytest.raises(ValueError, match="identity|dataset"):
        BatchRun(modified).run()
    after = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert after == before, "Reject mismatches before rewriting SKU/manifest or discarding data"


@pytest.mark.parametrize("damaged", ["legacy", "checkpoint", "manifest", "runtime_identity"])
def test_untrusted_resume_metadata_is_rejected_without_repairing_it(tiny_config, tmp_path, damaged):
    """Never silently adopt legacy data or relabel a foreign checkpoint as this world."""
    config = dataclasses.replace(
        _config_at(tiny_config, tmp_path / "guarded"),
        scale=dataclasses.replace(tiny_config.scale, days_history=2, orders_per_day=10),
    )
    BatchRun(config).run()
    root = Path(config.storage.output_root)
    if damaged == "legacy":
        (root / "_dataset_identity.json").unlink()
    else:
        name = {
            "checkpoint": "_checkpoint/day-00001.json",
            "manifest": "run_manifest.json",
            "runtime_identity": "_dataset_identity.json",
        }[damaged]
        path = root / name
        value = json.loads(path.read_text())
        if damaged == "runtime_identity":
            value["runtime"]["numpy"] = "foreign-runtime"
        else:
            value["dataset_version"] = "foreign-world"
        path.write_text(json.dumps(value))
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="identity"):
        BatchRun(config).run()
    assert {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    } == before


def test_resuming_after_a_clean_cut_matches_a_straight_run_byte_for_byte(tiny_config, tmp_path):
    """Tem ghi xong mới chết: chạy tiếp từ ngày sau và ra đúng kết quả cũ."""
    straight = tmp_path / "straight"
    cut = tmp_path / "cut"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, cut)
    _run_with_cut(config, cut_at=5, stamp_first=True)
    assert not (cut / "run_manifest.json").exists(), "lan chay bi cat khong duoc ghi manifest"

    resumed = BatchRun(config).run()
    assert _comparable(resumed) == _comparable(expected)
    assert _tree(cut) == _tree(straight)


def test_resuming_after_a_dirty_cut_matches_a_straight_run_byte_for_byte(tiny_config, tmp_path):
    """Chết TRƯỚC khi dán tem: file ngày đó bị bỏ đi và sinh lại, không trùng không thiếu.

    Đây là test quan trọng nhất của cơ chế. Nếu `_discard_days_after` không xóa file
    của ngày chưa có tem, lần chạy tiếp sẽ ghi thêm vào bên cạnh chúng và dữ liệu
    thành trùng — manifest vẫn cộng ra số đẹp, nên chỉ so từng byte mới thấy.
    """
    straight = tmp_path / "straight"
    cut = tmp_path / "cut"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, cut)
    _run_with_cut(config, cut_at=5, stamp_first=False)
    # Ngày 5 có để lại file trên đĩa, nhưng không có tem. Đúng trạng thái cần dọn.
    assert sorted(cut.glob("**/part-sim00005.*")), "cat ban phai de lai file cua ngay 5"
    assert not (cut / "_checkpoint/day-00005.json").exists()

    resumed = BatchRun(config).run()
    assert _comparable(resumed) == _comparable(expected)
    assert _tree(cut) == _tree(straight)


def test_the_run_picks_up_at_the_day_after_the_last_stamp(tiny_config, tmp_path):
    """Cắt bẩn ở ngày 5 thì ngày xong cuối cùng là 4, và ngày 5 được sinh lại."""
    config = _config_at(tiny_config, tmp_path / "cut")
    _run_with_cut(config, cut_at=5, stamp_first=False)

    run = BatchRun(config)
    assert run._last_complete_day() == 4
    removed = run._discard_days_after(4)
    assert removed, "phai xoa file cua ngay 5"
    assert all("sim00005" in path or "day-00005" in path for path in removed), removed


def test_a_stamp_standing_alone_after_a_gap_is_not_trusted(tiny_config, tmp_path):
    """Tem 1,2 rồi nhảy sang 9: ngày xong cuối cùng là 2, không phải 9.

    Tem lẻ không chứng minh được các ngày trước nó đã xong. Lấy số lớn nhất tìm được
    sẽ bỏ qua ngày 3..8 và mất dữ liệu trong im lặng.
    """
    root = tmp_path / "gap"
    config = _config_at(tiny_config, root)
    run = BatchRun(config)
    for day in (1, 2, 9):
        run.store.write_json_atomic(f"_checkpoint/day-{day:05d}.json", {})

    assert BatchRun(config)._last_complete_day() == 2


def test_the_stamp_is_written_only_after_every_file_of_the_day_is_closed(tiny_config):
    """Tem là việc cuối cùng của một ngày: lúc ghi nó, không file nào còn mở.

    Nếu tem ghi trước khi file đóng xong thì mất điện có thể để lại tem nói "ngày d
    xong" trong khi file ngày d thiếu đuôi. Lần chạy sau tin tem, bỏ qua ngày d, và
    dữ liệu thiếu mà không ai báo. Cả cơ chế sống chết ở thứ tự này.
    """
    run = BatchRun(tiny_config)
    write_json_atomic = run.store.write_json_atomic
    open_files_when_stamping = []

    def guarded(relative: str, obj):
        """Ghi lại số file còn mở đúng lúc tem được ghi, để kiểm tem là việc cuối cùng của ngày."""
        if relative.startswith("_checkpoint/"):
            open_files_when_stamping.append(len(run.jsonl.keys()))
        return write_json_atomic(relative, obj)

    run.store.write_json_atomic = guarded
    run.run()

    assert len(open_files_when_stamping) == tiny_config.scale.days_history
    assert set(open_files_when_stamping) == {0}


def test_restart_ignores_the_stamps_and_regenerates_everything(tiny_config, tmp_path):
    """`--restart` bỏ tem cũ, làm lại từ ngày 1, và không để lại file của lần cũ."""
    straight = tmp_path / "straight"
    again = tmp_path / "again"
    expected = _straight_run(tiny_config, straight)

    config = _config_at(tiny_config, again)
    _run_with_cut(config, cut_at=7, stamp_first=True)
    assert (again / "_checkpoint/day-00007.json").exists()

    restarted = BatchRun(config).run(restart=True)
    assert _comparable(restarted) == _comparable(expected)
    assert _tree(again) == _tree(straight)


def test_resuming_a_finished_run_changes_nothing(tiny_config, tmp_path):
    """Chạy lại trên thư mục đã xong thì không sinh lại ngày nào và kết quả y nguyên."""
    root = tmp_path / "done"
    config = _config_at(tiny_config, root)
    first = BatchRun(config).run()
    before = _tree(root)

    run = BatchRun(config)
    assert run._last_complete_day() == tiny_config.scale.days_history
    assert run.run() == first
    assert _tree(root) == before


def test_the_manifest_of_a_resumed_run_still_matches_the_files_on_disk(tiny_config, tmp_path):
    """Số byte và số file trong manifest sau khi chạy tiếp vẫn là số thật trên đĩa.

    `datasets[*].bytes` là bằng chứng đạt ngưỡng 100 GB của bộ dữ liệu. Nó được cộng dồn
    qua tem, nên nếu `_counters` bỏ sót một trường thì nó sai đúng ở lần chạy bị cắt
    — tức đúng ở lần chạy thật, chứ không phải ở test chạy một lèo.
    """
    root = tmp_path / "resumed"
    config = _config_at(tiny_config, root)
    _run_with_cut(config, cut_at=6, stamp_first=False)
    manifest = BatchRun(config).run()

    patterns = {
        "amazon.orders": "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        "walmart.orders": "pg-load/walmart.orders/export_date=*/part-sim*.jsonl",
        "seller.skus": "pg-load/seller.skus/part-00000.jsonl",
    }
    for dataset, pattern in patterns.items():
        parts = sorted(root.glob(pattern))
        assert parts, dataset
        assert manifest["datasets"][dataset]["bytes"] == sum(p.stat().st_size for p in parts), (
            dataset
        )
        assert manifest["datasets"][dataset]["files"] == len(parts), dataset

    rows = sum(len(p.read_text().splitlines()) for p in root.glob(patterns["amazon.orders"]))
    assert manifest["datasets"]["amazon.orders"]["rows"] == rows
    assert json.loads((root / "run_manifest.json").read_text()) == manifest


def test_a_shorter_rerun_removes_the_files_left_by_a_longer_previous_run(tiny_config, tmp_path):
    """Chạy lại với ít ngày hơn thì file của những ngày không còn tồn tại phải biến mất.

    Đây là lý do `_discard_days_after` tồn tại. Ghi lại một ngày thì tự đè file cũ
    (mở bằng `"wb"` là xóa trắng), nên ngày bị cắt không cần dọn. Nhưng file của
    ngày 13..20 do lần chạy trước để lại thì KHÔNG ngày nào đè lên — chúng cứ nằm đó.

    Hậu quả nếu không dọn: Spark, DuckDB và `generator/report.py` đọc cả thư mục nên
    vẫn thấy chúng và tính là dữ liệu thật, trong khi manifest không đếm. Lúc đó
    `datasets[*].bytes` — bằng chứng đạt ngưỡng 100 GB — không còn bằng byte thật
    trên đĩa, và không có gì báo lỗi.
    """
    root = tmp_path / "shrink"
    fresh = tmp_path / "fresh"
    longer = dataclasses.replace(tiny_config.scale, days_history=20)
    BatchRun(_config_at(dataclasses.replace(tiny_config, scale=longer), root)).run()
    assert sorted(root.glob("**/part-sim00018.*")), "lan chay 20 ngay phai co file ngay 18"

    expected = _straight_run(tiny_config, fresh)
    shrunk = BatchRun(_config_at(tiny_config, root)).run(restart=True)

    assert _comparable(shrunk) == _comparable(expected)
    assert _tree(root) == _tree(fresh)

    for dataset, pattern in (
        (
            "amazon.orders",
            "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        ),
        ("walmart.orders", "pg-load/walmart.orders/export_date=*/part-sim*.jsonl"),
    ):
        on_disk = sum(p.stat().st_size for p in root.glob(pattern))
        assert shrunk["datasets"][dataset]["bytes"] == on_disk, dataset
