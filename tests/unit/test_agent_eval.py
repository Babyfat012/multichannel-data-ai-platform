"""Kiểm phần chấm của ml/agent_eval: chuẩn hoá số, biên làm tròn tỷ lệ, SKU không chấm được."""

import pytest

from ml.agent_eval import numbers, score, texts

FEATURES = {"labeled_count_90d": 30, "positive_count_90d": 10, "return_or_refund_rate_90d": 1 / 3}


def test_numbers_normalise_percent_and_comma():
    """33,3% và 0.333 cùng là một số; số nguyên giữ nguyên."""
    assert numbers("rate 33,3% from 30 lines (0.333)") == {"0.333", "30"}


@pytest.mark.parametrize(
    ("answer", "ok"),
    [
        ("30 labeled, 10 returned, rate 0.33", True),
        ("30 labeled, 10 returned, rate 33.3%", True),
        ("30 labeled, 10 returned, rate 0.34", False),  # lệch 0,0067 > 0,005
        ("30 labeled, 11 returned, rate 0.33", False),
        ("about a third of orders come back", False),  # không có số thật ⇒ không đạt
    ],
)
def test_score_boundaries(answer, ok):
    """Đạt chỉ khi đủ cả ba số; tỷ lệ chấp nhận làm tròn tới 0,005."""
    assert score(answer, FEATURES)["pass"] is ok


@pytest.mark.parametrize(
    "features",
    [
        {"labeled_count_90d": 0, "positive_count_90d": 0, "return_or_refund_rate_90d": None},
        {"labeled_count_90d": None},
    ],
)
def test_sku_without_history_is_not_graded(features):
    """Chưa có lịch sử ⇒ không chấm bằng số (tránh agent nói '0' là đạt)."""
    assert score("0 0 0", features) == {"gradable": False, "pass": None}


def test_texts_collects_nested_a2a_parts():
    """Gom text ở mọi tầng của kết quả A2A."""
    result = {
        "status": {"message": {"parts": [{"kind": "text", "text": "a"}]}},
        "artifacts": [{"parts": [{"text": "b"}]}],
    }
    assert texts(result) == ["a", "b"]


def test_sku_id_digits_are_not_counted_as_answer_numbers():
    """'sku-00010' không được tính là số 10 ⇒ chỉ nhắc lại mã SKU thì không đạt."""
    assert numbers("sku-00010 sku-00030") == set()
    assert score("For sku-00010 and sku-00030: rate 0.33", FEATURES)["pass"] is False


@pytest.mark.parametrize(("answer", "exit_code"), [("30 10 33.3%", 0), ("30 11 33.3%", 1)])
def test_ci_http_truth_and_accuracy_gate(monkeypatch, tmp_path, capsys, answer, exit_code):
    """Port-forward HTTP không đòi CA; dưới ngưỡng trả 1 và vẫn lưu JSONL/summary."""
    import json

    from ml import agent_eval

    def fake_post(url, body, headers, context=None, timeout=180):
        assert context is None
        if url.endswith("/features"):
            return {"features": [{"seller_sku": "sku-00001", **FEATURES}]}
        assert headers["Authorization"] == "Bearer fake-key"
        return {"result": {"parts": [{"text": answer}]}}

    monkeypatch.setattr(agent_eval, "post", fake_post)
    monkeypatch.setenv("AGW_AGENT_KEY", "fake-key")
    output = tmp_path / "eval.jsonl"
    assert (
        agent_eval.main(
            [
                "--agent",
                "data-agent",
                "--skus",
                "sku-00001",
                "--feature-api",
                "http://localhost:18000",
                "--min-accuracy",
                "0.95",
                "--ca",
                "/does-not-exist",
                "--out",
                str(output),
            ]
        )
        == exit_code
    )
    assert json.loads(output.read_text())["pass"] is (exit_code == 0)
    assert json.loads(capsys.readouterr().out)["accuracy"] == (1 if exit_code == 0 else 0)
