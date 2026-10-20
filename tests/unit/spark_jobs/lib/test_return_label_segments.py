"""`label_segments`/`feature_window` must equal brute-force `label_line` at every boundary.

Cases reuse the hand-built contract fixtures of test_label_v2 (L01–L17 shapes, corrections,
late observations, scope narrowing). The brute force evaluates `label_line` at each
observation time and maturity end, ±1µs, plus far before/after — every place the outcome
could change.
"""

from copy import deepcopy
from datetime import timedelta

import pytest

from spark_jobs.lib.return_label import (
    FAR_FUTURE,
    feature_window,
    label_line,
    label_segments,
    sku_return_feature,
    utc,
)
from tests.unit.spark_jobs.lib.test_return_label import approve, day, event, line, refund

MICRO = timedelta(microseconds=1)


def _corrected():
    """L13 shape: approval known D10, coverage D31, invalidation observed D40."""
    data = approve(line(), 10)
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["source_observed_at"] = day(31)
    data["events"].append(
        event(
            "return",
            "fix",
            10,
            observed=40,
            event_type="approved",
            quantity=1,
            allocation_id="a0",
            invalidated=True,
            corrects_source_event_id="approval",
            operation="correction",
        )
    )
    return data


def _late():
    """L11 shape: approval effective D29 but observed D35; coverage observed D39."""
    data = approve(line(), 29, observed=35)
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["source_observed_at"] = day(39)
    return data


def _scoped():
    """L16 shape for line B: order-scoped refund later narrowed to line A."""
    data = line()
    data.update(line_id="B", native_order_id="order", native_line_id="B")
    data["events"].append(
        event(
            "refund",
            "unmatched",
            10,
            allocation_id=None,
            component="product",
            status="success",
            amount_minor=100,
            scope={"native_order_id": "order"},
        )
    )
    data["events"].append(
        event(
            "refund",
            "resolve",
            10,
            observed=65,
            corrects_source_event_id="unmatched",
            invalidated=True,
        )
    )
    data["events"].append(
        event(
            "refund",
            "resolved",
            10,
            observed=65,
            allocation_id="a0",
            component="product",
            status="success",
            amount_minor=100,
            scope={"line_id": "A"},
        )
    )
    return data


CASES = {
    "two_allocations_positive": approve(line((0, 20)), 45, "a1"),
    "expired_first_allocation": approve(line((0, 20)), 45),
    "refund_at_deadline": refund(line(), 30),
    "immature_open_line": approve(line(ordered=3, terminal=False), 5),
    "missing_allocation": refund(line((0, 20)), 25, allocation=None),
    "source_correction": _corrected(),
    "late_observation": _late(),
    "scope_narrowed": _scoped(),
    "plain_negative": line(),
    "full_cancel": line((), ordered=3, canceled=3),
}


def _probe_times(data):
    """Every instant where the outcome could change, with its neighbours."""
    points = {utc(r["source_observed_at"]) for r in data["events"]}
    final = label_line(data, FAR_FUTURE)
    if final.maturity_end:
        points.add(final.maturity_end)
    for segment in label_segments(data):
        points.add(segment["maturity_end"])
    probes = {day(-400), day(400)}
    for point in points:
        probes |= {point - MICRO, point, point + MICRO}
    return sorted(probes)


@pytest.mark.parametrize("name", sorted(CASES))
def test_segments_equal_label_line_everywhere(name):
    """Inside a segment ⇔ labeled with that label, cohort and maturity; outside ⇔ not labeled."""
    data = CASES[name]
    segments = label_segments(data)
    for cutoff in _probe_times(data):
        brute = label_line(data, cutoff)
        inside = [
            s
            for s in segments
            if s["start_exclusive"] < cutoff
            and (s["end_inclusive"] is None or cutoff <= s["end_inclusive"])
        ]
        assert len(inside) <= 1
        if brute.state == "labeled":
            assert inside, (name, cutoff)
            segment = inside[0]
            assert (
                segment["label"],
                segment["cohort_date"],
                segment["maturity_end"],
                segment["decision_observed_at"],
            ) == (brute.label, brute.cohort_date, brute.maturity_end, brute.decision_observed_at), (
                name,
                cutoff,
            )
        else:
            assert not inside, (name, cutoff, brute.state)


def test_correction_case_has_two_segments_with_different_labels():
    """Sanity: the L13 shape is labeled 1 before the correction is observed and 0 after."""
    labels = [s["label"] for s in label_segments(CASES["source_correction"])]
    assert labels == [1, 0]


def _window_feature(target, history):
    """Feature via segments + half-open windows (what Gold computes with a sweep)."""
    t = utc(target["created_at"])
    values = []
    for candidate in history:
        if (
            candidate["dataset_version"] != target["dataset_version"]
            or candidate["seller_sku"] != target["seller_sku"]
            or candidate["line_id"] == target["line_id"]
        ):
            continue
        for segment in label_segments(candidate):
            window = feature_window(segment)
            if window and window[0] <= t < window[1]:
                values.append(segment["label"])
    return len(values), sum(values)


def test_feature_windows_equal_sku_return_feature_at_every_boundary():
    """Counts from windows equal the original definition at all segment/cohort boundaries."""
    history = []
    for index, name in enumerate(sorted(CASES)):
        data = deepcopy(CASES[name])
        data["line_id"] = f"h{index}"
        history.append(data)
    times = {day(-200), day(500)}
    for candidate in history:
        for segment in label_segments(candidate):
            window = feature_window(segment)
            edges = [segment["start_exclusive"]] + (
                [segment["end_inclusive"]] if segment["end_inclusive"] else []
            )
            for edge in edges + (list(window) if window else []):
                times |= {edge - MICRO, edge, edge + MICRO}
    for t in sorted(times):
        target = dict(line_id="target", dataset_version="dataset", seller_sku="sku", created_at=t)
        expected = sku_return_feature(target, history)
        assert _window_feature(target, history) == (
            expected["labeled_count_90d"],
            expected["positive_count_90d"],
        ), t
