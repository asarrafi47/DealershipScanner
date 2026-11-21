"""C2: complete() reports usage to an active guard, and is unchanged without one."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.llm import client as llm


class _SDK:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kw: Any) -> Any:
        return self.outcomes.pop(0)


def _resp(text="ok", stop="end_turn", usage=None):
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)], stop_reason=stop,
        usage=usage if usage is not None else {"input_tokens": 10, "output_tokens": 3},
    )


@pytest.fixture
def sdk(monkeypatch):
    holder = {}

    def install(outcomes):
        holder["sdk"] = _SDK(outcomes)
        monkeypatch.setattr(llm, "_sdk_client", lambda key: holder["sdk"])

    monkeypatch.setattr("backend.vision.claude_rate_limit.acquire_vision_slot", lambda: None)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    return install


def test_inactive_hook_changes_nothing(sdk):
    sdk([_resp("hello")])
    r = llm.complete("hi", model_role="chat", max_tokens=10)
    assert r.text == "hello" and r.ok and r.usage == {"input_tokens": 10, "output_tokens": 3}


def test_active_sink_sees_every_call(sdk):
    sdk([_resp("a"), _resp("b", usage={"input_tokens": 5, "output_tokens": 1})])
    seen = []
    token = llm.set_usage_sink(seen.append)
    try:
        llm.complete("one", model_role="chat", max_tokens=10)
        llm.complete("two", model_role="chat", max_tokens=10)
    finally:
        llm.reset_usage_sink(token)
    assert [r.text for r in seen] == ["a", "b"]


def test_sink_is_removed_after_reset(sdk):
    sdk([_resp("a"), _resp("b")])
    seen = []
    token = llm.set_usage_sink(seen.append)
    llm.complete("one", model_role="chat", max_tokens=10)
    llm.reset_usage_sink(token)
    llm.complete("two", model_role="chat", max_tokens=10)
    assert len(seen) == 1


def test_a_failing_sink_never_raises_into_the_model_call(sdk):
    sdk([_resp("fine")])

    def boom(_r):
        raise RuntimeError("sink bug")

    token = llm.set_usage_sink(boom)
    try:
        r = llm.complete("x", model_role="chat", max_tokens=10)
    finally:
        llm.reset_usage_sink(token)
    assert r.text == "fine"


def test_refusal_is_reported_and_flagged(sdk):
    sdk([_resp("", stop="refusal")])
    seen = []
    token = llm.set_usage_sink(seen.append)
    try:
        r = llm.complete("x", model_role="chat", max_tokens=10)
    finally:
        llm.reset_usage_sink(token)
    assert r.refused and seen and seen[0].refused
