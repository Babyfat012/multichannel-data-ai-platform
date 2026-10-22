"""Luật nhãn Return v2, không phụ thuộc Spark hay đáp án của simulator.

Đầu vào: định danh một line và `events` là các quan sát nguồn đã chuẩn hoá (`kind`, định
danh sự kiện, revision, `effective_at`, `source_observed_at`, payload). Mọi đồng hồ là UTC.

- Coverage: cần envelope nguồn đã kiểm, phủ trọn cửa sổ giao (cho phép nhiều envelope liền
  nhau). Thiếu authority, liên kết hay bằng chứng thì trả `unknown` (fail closed).
- Huỷ/đảo thông thường giữ nguyên lịch sử; correction tường minh chỉ thay quan sát kể từ
  lúc nó được quan sát.
- Đảo mơ hồ, giao từng phần ở các thời điểm khác nhau, revision xung đột: `unknown`.
- `decision_observed_at` tính mọi quan sát liên quan đã thấy, không đoán tập tối thiểu.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

LABEL_VERSION = "return-or-refund-line-v2"
WINDOW = timedelta(days=30)
REQUIRED_KINDS = ("commitment", "allocation", "delivery", "return", "refund")


@dataclass(frozen=True)
class LabelResult:
    """Kết quả nhãn (có thể null) và bằng chứng để kiểm lại lúc nào nó có được."""

    state: str
    label: int | None
    cohort_date: date | None
    maturity_end: datetime | None
    qualifying_event_id: str | None = None
    reasons: list[str] = field(default_factory=list)
    label_version: str = LABEL_VERSION
    decision_observed_at: datetime | None = None
    source_event_refs: list[str] = field(default_factory=list)
    # Ba ca hậu mãi khi label = 1 (Return ≠ Refund): return_and_refund / refund_only / return_only; else None.
    positive_kind: str | None = None


def utc(value: str | datetime) -> datetime:
    """Chuỗi ISO của nguồn hoặc datetime UTC không múi giờ của Spark -> datetime UTC có múi giờ."""
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def scope_matches(scope: Mapping[str, Any], line: Mapping[str, Any]) -> bool:
    """Giữ mọi line có thể bị ảnh hưởng; chỉ thu hẹp khi nguồn khai scope tường minh."""
    for key in (
        "dataset_version",
        "channel",
        "account_id",
        "native_order_id",
        "line_id",
        "native_line_id",
    ):
        if scope.get(key) is not None and scope[key] != line.get(key):
            return False
        plural = {
            "channel": "channels",
            "account_id": "account_ids",
            "native_line_id": "candidate_native_line_ids",
        }.get(key, key + "s")
        if scope.get(plural) and line.get(key) not in scope[plural]:
            return False
    return True


def visible_events(line: Mapping[str, Any], cutoff: str | datetime) -> list[dict]:
    """Lọc theo ảnh chụp quan sát (loại trừ cutoff) và scope, trước khi xử lý revision."""
    observed = [
        dict(r)
        for r in line.get("events", [])
        if r.get("source_observed_at") is not None and utc(r["source_observed_at"]) < utc(cutoff)
    ]
    result = []
    matched_ids = {
        r.get("source_event_id")
        for r in observed
        if scope_matches(r.get("scope") or r.get("affected_scope") or {}, line)
    }
    for record in observed:
        scope = record.get("scope") or record.get("affected_scope") or {}
        if scope_matches(scope, line):
            result.append(record)
        elif (
            record.get("corrects_source_event_id")
            and record["corrects_source_event_id"] in matched_ids
        ):
            # Scope cũ phải thấy sự kiện chưa khớp đã được giải quyết,
            # kể cả khi bản thay thế giờ thu hẹp về line khác.
            result.append({**record, "invalidated": True, "scope": {}, "affected_scope": {}})
    return result


def _resolved(records: list[dict]) -> tuple[list[dict], bool]:
    """Áp correction và revision trạng thái đã quan sát; giữ nguyên sự kiện quy trình trong lịch sử."""
    by_id = {}
    conflict = False
    for record in records:
        eid = record.get("source_event_id")
        if eid in by_id and record != by_id[eid]:
            conflict = True
        by_id[eid] = record
    corrected = {
        r["corrects_source_event_id"] for r in records if r.get("corrects_source_event_id")
    }
    kept = [r for r in by_id.values() if r.get("source_event_id") not in corrected]
    latest = {}
    history = []
    for record in kept:
        if record["kind"] in ("return", "refund"):
            history.append(record)
            continue
        key = (
            record["kind"],
            record.get("producer"),
            record.get("source_epoch"),
            record.get("entity_id", record.get("source_event_id")),
        )
        revision = record.get("entity_revision", record.get("revision", 1))
        prior = latest.get(key)
        if (
            prior
            and revision == prior.get("entity_revision", prior.get("revision", 1))
            and record != prior
        ):
            conflict = True
        if prior is None or revision > prior.get("entity_revision", prior.get("revision", 1)):
            latest[key] = record
    return list(latest.values()) + history, conflict


def _first_uncovered_kind(records, first_delivery: datetime, maturity: datetime) -> str | None:
    """Feed đầu tiên mà chuỗi coverage hợp lệ, nối liền, KHÔNG phủ hết `[first_delivery, maturity]`.

    Coverage nối tiếp (ADR-0013): bắt đầu từ lần giao đầu, mỗi envelope bắt đầu không muộn hơn
    chỗ đã phủ thì kéo dài chỗ phủ. Có khe hở là dừng. Coverage end là exclusive nên phải
    VƯỢT `maturity`.
    """
    coverage = [r for r in records if r["kind"] == "coverage"]
    for kind in REQUIRED_KINDS:
        valid = [
            r
            for r in coverage
            if r.get("covered_kind") == kind
            and r.get("status") == "complete"
            and r.get("receipt_status") == "complete"
            and r.get("evidence_ref")
            and r.get("interval_start")
            and r.get("interval_end")
            and not r.get("unresolved_scope_refs")
        ]
        covered_until = first_delivery
        for envelope in sorted(valid, key=lambda r: utc(r["interval_start"])):
            begin, end = utc(envelope["interval_start"]), utc(envelope["interval_end"])
            if begin > covered_until:
                break
            covered_until = max(covered_until, end)
        if covered_until <= maturity:
            return kind
    return None


def label_line(line: Mapping[str, Any], cutoff: str | datetime) -> LabelResult:
    """Tính nhãn chính thức của một line tại cutoff (loại trừ), chỉ dùng bằng chứng đã quan sát.

    Chưa giao (kể cả line còn mở) là `ineligible`. Thiếu commitment, định danh, authority,
    feed, hoặc allocation mơ hồ là `unknown`. Fulfillment chưa rõ được xét trước nhãn dương.
    Cuối coverage là loại trừ nên phải vượt mọi hạn chót. Nhãn dương và âm chờ cùng bằng chứng.
    """
    cutoff = utc(cutoff)
    visible = visible_events(line, cutoff)
    refs = sorted({r["source_event_id"] for r in visible if r.get("source_event_id")})
    known_at = max((utc(r["source_observed_at"]) for r in visible), default=None)
    cohort = maturity = None

    def result(state, value=None, reason=None, eid=None):
        """Dựng kết quả bất biến, kèm bằng chứng ảnh chụp dùng chung."""
        return LabelResult(
            state,
            value,
            cohort,
            maturity,
            eid,
            [reason] if reason else [],
            LABEL_VERSION,
            known_at,
            refs,
        )

    records, conflict = _resolved(visible)
    if conflict:
        return result("unknown", reason="conflicting_source_revision")
    groups = {kind: [r for r in records if r["kind"] == kind] for kind in REQUIRED_KINDS}
    if len(groups["commitment"]) != 1:
        return result("unknown", reason="missing_or_conflicting_commitment")
    commitment = groups["commitment"][0]
    ordered = commitment.get("ordered_basis_qty")
    canceled = commitment.get("canceled_unshipped_qty")
    if ordered is None or canceled is None or ordered <= 0 or canceled < 0 or canceled > ordered:
        return result("unknown", reason="invalid_commitment_quantity")
    deliveries = groups["delivery"]
    delivered = sum(r.get("delivered_qty", 0) for r in deliveries)
    if not delivered:
        if canceled == ordered and commitment.get("fulfillment_terminal"):
            return result("ineligible", reason="fully_canceled_before_delivery")
        return result("ineligible", reason="no_delivery")
    allocations = {r.get("allocation_id"): r for r in groups["allocation"]}
    windows = {}
    quantities = {}
    for delivery in deliveries:
        aid = delivery.get("allocation_id")
        if (
            aid is None
            or aid not in allocations
            or not delivery.get("actual_delivered_at")
            or delivery.get("delivered_qty", 0) <= 0
        ):
            return result("unknown", reason="missing_delivery_allocation")
        d = utc(delivery["actual_delivered_at"])
        if aid in windows and windows[aid] != d:
            return result("unknown", reason="allocation_has_multiple_delivery_times")
        windows[aid] = d
        quantities[aid] = quantities.get(aid, 0) + delivery["delivered_qty"]
    maturity = max(windows.values()) + WINDOW
    cohort = max(windows.values()).date()
    if delivered + canceled > ordered or any(
        quantities[a] > allocations[a].get("quantity", 0) for a in quantities
    ):
        return result("unknown", reason="conflicting_quantity")
    if (
        not commitment.get("fulfillment_terminal")
        or delivered + canceled < ordered
        or any(
            quantities.get(a, 0) < r.get("quantity", 0)
            for a, r in allocations.items()
            if r.get("allocation_state") != "canceled"
        )
    ):
        return result("immature", reason="fulfillment_not_terminal")
    if maturity >= cutoff:
        return result("immature", reason="window_not_closed")
    for r in records:
        if r.get("authority_valid") is False or r.get("ambiguous_reversal"):
            return result("unknown", reason="authority_or_reversal_conflict")
        if r["kind"] == "uncertainty" and r.get("resolution_state") != "resolved":
            return result("unknown", reason="unresolved_affected_scope")
    missing = _first_uncovered_kind(records, min(windows.values()), maturity)
    if missing:
        return result("unknown", reason=f"missing_complete_coverage:{missing}")
    qualifying = []
    kinds = set()
    for r in groups["return"] + groups["refund"]:
        positive = (
            r["kind"] == "return" and r.get("event_type") == "approved" and r.get("quantity", 0) > 0
        ) or (
            r["kind"] == "refund"
            and r.get("status") == "success"
            and r.get("component") == "product"
            and r.get("amount_minor", 0) > 0
        )
        if not positive or r.get("invalidated"):
            continue
        aid = r.get("allocation_id")
        if aid not in windows:
            return result("unknown", reason="qualifying_event_missing_allocation")
        if r["kind"] == "return" and r["quantity"] > quantities[aid]:
            return result("unknown", reason="return_quantity_exceeds_delivery")
        if windows[aid] <= utc(r["effective_at"]) <= windows[aid] + WINDOW:
            qualifying.append(r["source_event_id"])
            kinds.add(r["kind"])
    outcome = result("labeled", int(bool(qualifying)), eid=min(qualifying) if qualifying else None)
    # Ghép Return–Refund theo line + cửa sổ, chưa theo return_id (không phải Channel nào cũng gửi).
    positive_kind = (
        {"return", "refund"} == kinds
        and "return_and_refund"
        or kinds == {"refund"}
        and "refund_only"
        or kinds == {"return"}
        and "return_only"
        or None
    )
    return replace(outcome, positive_kind=positive_kind)


def sku_return_feature(target: Mapping[str, Any], history: list[Mapping[str, Any]]) -> dict:
    """Feature theo SKU trên 90 ngày cohort UTC [T.date-90, T.date), tính lại nhãn tại T.

    Đầu vào lịch sử là sự kiện nguồn, không phải nhãn cuối hay đáp án simulator. Tính lại tại
    T để không dính sự kiện đến muộn hay correction trong tương lai.
    """
    timestamp = utc(target["created_at"])
    values = []
    for candidate in history:
        if (
            candidate.get("dataset_version") != target.get("dataset_version")
            or candidate.get("seller_sku") != target.get("seller_sku")
            or candidate.get("line_id") == target.get("line_id")
        ):
            continue
        result = label_line(candidate, timestamp)
        if (
            result.state == "labeled"
            and result.maturity_end < timestamp
            and result.decision_observed_at < timestamp
            and timestamp.date() - timedelta(days=90) <= result.cohort_date < timestamp.date()
        ):
            values.append(result.label)
    return dict(
        labeled_count_90d=len(values),
        positive_count_90d=sum(values),
        return_or_refund_rate_90d=sum(values) / len(values) if values else None,
        label_version=LABEL_VERSION,
    )


FAR_FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


def label_segments(line: Mapping[str, Any]) -> list[dict]:
    """Mọi khoảng cutoff `(start, end]` mà line ở trạng thái `labeled`, kèm kết quả.

    `label_line` chỉ đọc cutoff ở hai chỗ: `observed < cutoff` và `maturity >= cutoff`. Giữa hai
    thời điểm quan sát liên tiếp x_i < x_{i+1}, tập sự kiện thấy được không đổi, nên kết quả
    không đổi trên (max(x_i, maturity_i), x_{i+1}]. Tính một lần ở đầu trên mỗi đoạn là đủ:
    tuyến tính thay vì tính lại cho mọi cặp (line, thời điểm đặt). `end=None` là đoạn cuối mở.
    """
    times = sorted(
        {
            utc(r["source_observed_at"])
            for r in line.get("events", [])
            if r.get("source_observed_at") is not None
        }
    )
    segments = []
    for index, start in enumerate(times):
        end = times[index + 1] if index + 1 < len(times) else None
        result = label_line(line, end or FAR_FUTURE)
        if result.state != "labeled":
            continue
        lower = max(start, result.maturity_end)
        if end is not None and lower >= end:
            continue
        segments.append(
            dict(
                start_exclusive=lower,
                end_inclusive=end,
                label=result.label,
                cohort_date=result.cohort_date,
                maturity_end=result.maturity_end,
                decision_observed_at=result.decision_observed_at,
                qualifying_event_id=result.qualifying_event_id,
            )
        )
    return segments


def feature_window(segment: Mapping[str, Any]) -> tuple[datetime, datetime] | None:
    """Khoảng thời điểm đặt hàng [a, b) mà một đoạn có nhãn được đếm vào feature PIT.

    Đếm tại t khi t thuộc (start, end] và ngày cohort thuộc [t.date-90, t.date), tức t thuộc
    [cohort+1 ngày, cohort+91 ngày) lúc 0h UTC. Timestamp chính xác tới micro giây nên
    (start, end] == [start+1µs, end+1µs).
    """
    micro = timedelta(microseconds=1)
    cohort = datetime.combine(segment["cohort_date"], datetime.min.time(), tzinfo=timezone.utc)
    begin = max(utc(segment["start_exclusive"]) + micro, cohort + timedelta(days=1))
    end = cohort + timedelta(days=91)
    if segment["end_inclusive"] is not None:
        end = min(end, utc(segment["end_inclusive"]) + micro)
    return (begin, end) if begin < end else None


FEED_KINDS = {
    "seller_oms.line_commitments": "commitment",
    "seller_oms.fulfillment_allocations": "allocation",
    "carrier.delivery_events": "delivery",
    "seller_postsales.return_events": "return",
    "seller_postsales.refund_events": "refund",
}


def _coverage_verified(raw: Mapping[str, Any], decoded: list[dict]) -> bool:
    """Tự kiểm một coverage envelope trên TOÀN BỘ feed đã quan sát (digest, số dòng, sequence, partition)."""
    import hashlib
    import json

    feed = raw.get("feed")
    observed = raw.get("source_observed_at")
    interval = raw.get("event_interval") or {}
    rows = [
        r
        for r in decoded
        if r.get("source_event_id")
        and r.get("feed") == feed
        and r.get("producer") == raw.get("producer")
        and r.get("source_epoch") == raw.get("source_epoch")
        and utc(r["source_observed_at"]) < utc(observed)
        and interval.get("start_inclusive")
        and interval.get("end_exclusive")
        and utc(interval["start_inclusive"])
        <= utc(r["effective_at"])
        < utc(interval["end_exclusive"])
    ]
    canonical = json.dumps(
        sorted(rows, key=lambda r: r["source_event_id"]),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    declared = {
        (item["partition"], n)
        for item in raw.get("source_sequence_ranges", [])
        for n in range(item["first"], item["last"] + 1)
    }
    actual = {(r.get("source_partition"), r.get("source_sequence")) for r in rows}
    valid = (
        raw.get("status") == "complete"
        and digest == raw.get("record_set_sha256")
        and len(rows) == raw.get("source_record_count")
        and declared == actual
        and sorted(raw.get("expected_partitions", []))
        == sorted(raw.get("completed_partitions", []))
        and bool(raw.get("capability"))
        and bool(raw.get("profile_id"))
        and interval.get("start_inclusive") is not None
        and interval.get("end_exclusive") is not None
    )
    if valid:
        valid = all(
            utc(interval["start_inclusive"])
            <= utc(r["effective_at"])
            < utc(interval["end_exclusive"])
            for r in rows
        )
    return valid


def _coverage_record(
    raw: Mapping[str, Any], decoded: list[dict], coverage_status: Mapping[str, str] | None
) -> dict:
    """Coverage envelope → bản ghi `coverage`; trạng thái lấy từ Silver nếu có, không thì tự kiểm."""
    event_id = raw["coverage_id"] + ":" + str(raw["revision"])
    if coverage_status is not None:
        status = coverage_status.get(event_id, "partial")
    else:
        status = "complete" if _coverage_verified(raw, decoded) else "partial"
    interval = raw.get("event_interval") or {}
    return {
        **raw,
        "kind": "coverage",
        "source_event_id": event_id,
        "entity_id": raw["coverage_id"],
        "covered_kind": FEED_KINDS[raw.get("feed")],
        "entity_revision": raw["revision"],
        "evidence_ref": raw["coverage_id"],
        "receipt_status": status,
        "interval_start": interval.get("start_inclusive"),
        "interval_end": interval.get("end_exclusive"),
    }


def _operational_record(
    raw: Mapping[str, Any], decoded: list[dict], identity: Mapping[str, Any]
) -> dict:
    """Sự kiện feed nghiệp vụ → bản ghi phẳng (envelope + payload), kèm scope và authority."""
    feed = raw.get("feed")
    payload = raw.get("payload") or {}
    scope = {
        key: payload[key]
        for key in ("line_id", "native_order_id", "native_line_id", "channel", "account_id")
        if payload.get(key) is not None
    }
    record = {**raw, **payload, "scope": scope, "kind": FEED_KINDS[feed]}
    # Coverage khai authority cho đúng feed/producer/epoch vận hành;
    # producer khác không được mượn bằng chứng của feed khác.
    record["authority_valid"] = any(
        c.get("coverage_id")
        and c.get("feed") == feed
        and c.get("producer") == raw.get("producer")
        and c.get("source_epoch") == raw.get("source_epoch")
        and c.get("capability")
        and c.get("profile_id")
        and scope_matches(c.get("scope") or {}, identity)
        for c in decoded
    )
    if record["kind"] == "refund":
        money = payload.get("allocation_money_rows", [])
        if len(money) == 1 and money[0].get("amount_minor") == payload.get("amount_minor"):
            record["allocation_id"] = money[0]["allocation_id"]
        elif money:
            # Refund hàng nhiều allocation cần bằng chứng hiệu lực cho từng allocation.
            record["allocation_id"] = None
    return record


def _relevant_coverage(events: list[dict], identity: Mapping[str, Any]) -> list[dict]:
    """Chỉ giữ coverage giao với `[delivery đầu, deadline cuối]` của line (khi đã có trạng thái Silver).

    Superset qua TẤT CẢ revision delivery: correction tương lai không mất coverage.
    Authority đã kiểm trên decoded đầy đủ, cùng feed/producer/epoch, trước khi lọc.
    Line chưa giao thì không cần coverage nào.
    """
    deliveries = [
        utc(r["actual_delivered_at"])
        for r in events
        if r["kind"] == "delivery"
        and r.get("actual_delivered_at")
        and scope_matches(r.get("scope") or {}, identity)
    ]
    if not deliveries:
        return [r for r in events if r["kind"] != "coverage"]
    begin, end = min(deliveries), max(deliveries) + WINDOW
    coverage = [r for r in events if r["kind"] == "coverage"]
    keep = {
        r["entity_id"]
        for r in coverage
        if not r.get("interval_start")
        or not r.get("interval_end")
        or (utc(r["interval_start"]) <= end and utc(r["interval_end"]) > begin)
    }
    # Giữ mọi revision của envelope liên quan: revision mới có thể thu hẹp/dời
    # interval ra ngoài. Nếu bỏ nó, revision cũ sẽ sống lại trong _resolved.
    changed = True
    while changed:
        refs = {r["source_event_id"] for r in coverage if r["entity_id"] in keep}
        additions = {r["entity_id"] for r in coverage if r.get("corrects_source_event_id") in refs}
        changed = bool(additions - keep)
        keep.update(additions)
    return [r for r in events if r["kind"] != "coverage" or r["entity_id"] in keep]


def source_line(
    identity: Mapping[str, Any],
    raw_records: list[str | dict],
    coverage_status: Mapping[str, str] | None = None,
) -> dict:
    """Đổi envelope nguồn đã quan sát thành đầu vào thuần cho luật nhãn, không dùng sự thật ẩn.

    Không có `coverage_status` (fixture nhỏ, unit test): kiểm digest/count/sequence/partition
    của coverage ngay tại đây trên cả feed, nên `raw_records` phải chứa đủ feed. Có
    `coverage_status` ({source_event_id của coverage: 'complete'|'partial'}, Silver tính một
    lần ở `coverage_validity`): chỉ cần bản ghi của line này, coverage cắt theo cửa sổ giao.

    Thời điểm tạo là `effective_at` của commitment revision 1; không có thì `created_at` null.
    Authority lấy từ capability/profile khai trên coverage envelope của feed vận hành.
    """
    import json

    decoded = [json.loads(r) if isinstance(r, str) else dict(r) for r in raw_records]
    decoded = [r for r in decoded if r.get("dataset_version") == identity["dataset_version"]]
    # Bản retry y hệt không phải quan sát mới.
    unique = {json.dumps(r, sort_keys=True, separators=(",", ":")): r for r in decoded}
    decoded = list(unique.values())
    events = []
    created = []
    for raw in decoded:
        feed = raw.get("feed")
        if raw.get("coverage_id"):
            if feed not in FEED_KINDS:
                continue
            record = _coverage_record(raw, decoded, coverage_status)
        elif raw.get("uncertainty_id"):
            record = {
                **raw,
                "kind": "uncertainty",
                "entity_id": raw["uncertainty_id"],
                "source_event_id": raw["uncertainty_id"] + ":" + str(raw["revision"]),
                "entity_revision": raw["revision"],
            }
        elif feed in FEED_KINDS:
            record = _operational_record(raw, decoded, identity)
            if (
                record["kind"] == "commitment"
                and raw.get("entity_revision") == 1
                and scope_matches(record["scope"], identity)
            ):
                created.append(utc(raw["effective_at"]))
        else:
            continue
        events.append(record)
    if coverage_status is not None:
        events = _relevant_coverage(events, identity)
    return {**identity, "events": events, "created_at": min(created) if created else None}
