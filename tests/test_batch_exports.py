"""Test lớp export của Batch: vị trí bản chính và cách chèn duplicate."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from generator.core.rngs import rng_for
from generator.core.simulate import Simulator
from generator.delivery.batch_exports import (
    IDENTICAL_COPY,
    OLDER_VERSION,
    PRIMARY,
    day_of,
    plan_exports,
)


def _with_duplicate_rate(config, rate):
    """Bản sao của cấu hình với `duplicate_rate` khác."""
    problems = dataclasses.replace(config.offline_problems, duplicate_rate=rate)
    return dataclasses.replace(config, offline_problems=problems)


def test_without_duplicates_each_order_appears_once_at_last_update(tiny_config):
    """Chứng minh `duplicate_rate=0` thì mỗi Order có đúng một bản chính, ở ngày export của lần đổi trạng thái cuối trước cutoff."""
    config = _with_duplicate_rate(tiny_config, 0.0)
    rng = rng_for(0, "test")
    for order in Simulator(config).simulate_day(10):
        placements = plan_exports(order, config, rng)
        assert [p.kind for p in placements] == [PRIMARY]
        last_change = order.status_at(config.cutoff - timedelta(seconds=1))
        assert placements[0].export_day == day_of(config, last_change.at)
        assert order.status_at(placements[0].as_of) == last_change


def test_duplicates_are_identical_later_or_older_earlier(tiny_config):
    """Chứng minh bản trùng hoặc giống hệt và đến muộn hơn, hoặc là bản cũ hơn đến sớm hơn, và luôn nằm trong phạm vi overlap."""
    config = _with_duplicate_rate(tiny_config, 0.4)
    rng = rng_for(0, "test")
    kinds = set()
    for day in range(1, 11):
        for order in Simulator(config).simulate_day(day):
            primary, *copies = plan_exports(order, config, rng)
            for copy in copies:
                kinds.add(copy.kind)
                gap = copy.export_day - primary.export_day
                assert 1 <= abs(gap) <= config.offline_problems.overlap_days
                assert copy.export_day <= config.scale.days_history
                if copy.kind == IDENTICAL_COPY:
                    assert gap > 0
                    assert order.status_at(copy.as_of) == order.status_at(primary.as_of)
                else:
                    assert copy.kind == OLDER_VERSION and gap < 0
                    assert order.status_at(copy.as_of).at < order.status_at(primary.as_of).at
    assert kinds == {IDENTICAL_COPY, OLDER_VERSION}


def test_orders_after_cutoff_are_not_in_batch(tiny_config):
    """Chứng minh Order tạo sau cutoff không được xuất trong Batch."""
    after_cutoff_day = tiny_config.scale.days_history + 1
    rng = rng_for(0, "test")
    for order in Simulator(tiny_config).simulate_day(after_cutoff_day):
        assert plan_exports(order, tiny_config, rng) == []


def test_order_created_in_the_last_second_before_cutoff_is_left_out(tiny_config):
    """Order tạo trong giây cuối của lịch sử bị loại, không làm `status_at` ném lỗi.

    Ảnh chụp của Batch lấy ở `cutoff - 1s`, nên Order tạo sau mốc đó chưa tồn tại
    lúc chụp. Xác suất mỗi Order là 1/86.400: ở `default.yaml` (4.000 Order/ngày)
    gần như không gặp, nhưng ở `scale-100gb.yaml` (852.099 Order/ngày) thì trung
    bình ~10 Order mỗi lần chạy. Không có test này thì lỗi chỉ lộ ra sau vài giờ.
    """
    order = next(iter(Simulator(tiny_config).simulate_day(1)))
    rng = rng_for(0, "test")
    as_of = tiny_config.cutoff - timedelta(seconds=1)

    for offset, expected in [(timedelta(0), True), (timedelta(microseconds=1), False),
                             (timedelta(milliseconds=500), False)]:
        late = dataclasses.replace(order, created_at=as_of + offset)
        assert bool(plan_exports(late, tiny_config, rng)) is expected, offset
