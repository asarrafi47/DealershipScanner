"""backend.llm.client: one retry policy, explicit refusal/truncation, role-keyed models.

All tests use a fake SDK; the real API is never called.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.llm import client as llm


class _StatusError(Exception):
    def __init__(self, status: int, msg: str = "", headers: dict[str, str] | None = None) -> None:
        super().__init__(msg or f"Error code: {status}")
        self.status_code = status
        self.response = SimpleNamespace(status_code=status, headers=headers or {})


class APIConnectionError(Exception):
    pass


def _msg(text: str | None = "ok", stop: str = "end_turn") -> Any:
    content = [] if text is None else [SimpleNamespace(type="text", text=text)]
    return SimpleNamespace(content=content, stop_reason=stop, usage={"output_tokens": 3})


class FakeSDK:
    """Stands in for anthropic.Anthropic: scripted outcomes, records requests."""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []
        self.init_kwargs: dict[str, Any] = {}
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kw: Any) -> Any:
        self.calls.append(kw)
        out = self.outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return out


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch):
    state: dict[str, Any] = {"sleeps": [], "slots": 0}

    def install(outcomes: list[Any]) -> FakeSDK:
        sdk = FakeSDK(outcomes)

        def factory(api_key: str) -> FakeSDK:
            sdk.init_kwargs = {"api_key": api_key}
            return sdk

        monkeypatch.setattr(llm, "_sdk_client", factory)
        state["sdk"] = sdk
        return sdk

    def _slot() -> None:
        state["slots"] += 1

    monkeypatch.setattr("backend.vision.claude_rate_limit.acquire_vision_slot", _slot)
    monkeypatch.setattr(llm, "_sleep", lambda s: state["sleeps"].append(s))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    for k in ("ANTHROPIC_MAX_RETRIES", "ANTHROPIC_VISION_MAX_RETRIES", "LLM_CLAUDE_MODEL",
              "ANTHROPIC_MODEL_VISION", "ANTHROPIC_MODEL_CHAT", "ANTHROPIC_MODEL_EXTRACT"):
        monkeypatch.setenv(k, "")
    state["install"] = install
    return state


# --- retry -----------------------------------------------------------------


@pytest.mark.parametrize("err", [
    _StatusError(429), _StatusError(529, "overloaded_error"), _StatusError(500),
    _StatusError(503), APIConnectionError("Connection reset"),
])
def test_transient_errors_retried_then_succeed(fake, err) -> None:
    sdk = fake["install"]([err, _msg("done")])
    res = llm.complete("hi", model_role="extract", max_tokens=10)
    assert res.text == "done" and res.ok and res.attempts == 2
    assert len(sdk.calls) == 2
    assert fake["slots"] == 2  # the shared limiter is taken on every attempt
    assert len(fake["sleeps"]) == 1 and 0 < fake["sleeps"][0] <= 60.5


@pytest.mark.parametrize("status", [400, 401, 403, 404, 413])
def test_client_errors_not_retried_and_raised_unchanged(fake, status) -> None:
    err = _StatusError(status)
    sdk = fake["install"]([err, _msg()])
    with pytest.raises(_StatusError) as ei:
        llm.complete("hi", model_role="extract", max_tokens=10)
    assert ei.value is err  # original exception: auth checks on str(exc) keep working
    assert len(sdk.calls) == 1 and fake["sleeps"] == []


def test_retries_exhausted_reraises_last(fake, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_MAX_RETRIES", "3")
    errs = [_StatusError(429), _StatusError(429), _StatusError(429, "last")]
    sdk = fake["install"](errs)
    with pytest.raises(_StatusError, match="last"):
        llm.complete("hi", model_role="extract", max_tokens=10)
    assert len(sdk.calls) == 3 and len(fake["sleeps"]) == 2


def test_backoff_is_jittered_exponential_and_honours_retry_after(fake) -> None:
    fake["install"]([_StatusError(429), _StatusError(429), _StatusError(429, headers={"retry-after": "7"}), _msg()])
    llm.complete("hi", model_role="extract", max_tokens=10)
    s0, s1, s2 = fake["sleeps"]
    assert 2.0 <= s0 <= 4.3  # base 4 * [0.5, 1.0] + small jitter
    assert 4.0 <= s1 <= 8.3
    assert 7.0 < s2 <= 7.5  # retry-after wins


def test_sdk_builtin_retries_disabled(monkeypatch) -> None:
    import sys
    import types

    seen: dict[str, Any] = {}

    class Anthropic:
        def __init__(self, **kw: Any) -> None:
            seen.update(kw)

    mod = types.ModuleType("anthropic")
    mod.Anthropic = Anthropic
    monkeypatch.setitem(sys.modules, "anthropic", mod)
    llm._sdk_client("sk-x")
    assert seen == {"api_key": "sk-x", "max_retries": 0}


def test_legacy_wrapper_uses_same_policy(fake) -> None:
    from backend.vision.claude_rate_limit import anthropic_messages_create

    sdk = FakeSDK([_StatusError(529), _msg("x")])
    out = anthropic_messages_create(sdk, model="m", max_tokens=1, messages=[])
    assert out.content[0].text == "x" and len(sdk.calls) == 2 and fake["slots"] == 2


# --- refusal / truncation ---------------------------------------------------


def test_refusal_is_explicit_never_ok(fake) -> None:
    fake["install"]([_msg(None, "refusal")])
    res = llm.complete("hi", model_role="vision", max_tokens=10)
    assert res.refused and not res.ok and not res.truncated and res.text == ""
    with pytest.raises(llm.LLMRefusal) as ei:
        res.require_complete()
    assert ei.value.result is res


def test_truncation_is_explicit_never_ok(fake) -> None:
    fake["install"]([_msg('{"a": [1, 2', "max_tokens")])
    res = llm.complete("hi", model_role="extract", max_tokens=10)
    assert res.truncated and not res.ok and res.text == '{"a": [1, 2'
    with pytest.raises(llm.LLMTruncated):
        res.require_complete()


def test_complete_result_passes_require_complete(fake) -> None:
    fake["install"]([_msg("fine")])
    res = llm.complete("hi", model_role="chat", max_tokens=10)
    assert res.require_complete() is res and res.usage == {"output_tokens": 3}


def test_missing_key_raises_not_configured(fake, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    fake["install"]([_msg()])
    with pytest.raises(llm.LLMNotConfigured):
        llm.complete("hi", model_role="chat", max_tokens=10)


# --- model routing ------------------------------------------------------------


def test_role_table_defaults_are_todays_models(fake) -> None:
    sdk = fake["install"]([_msg(), _msg(), _msg()])
    for role in ("vision", "extract", "chat"):
        llm.complete("hi", model_role=role, max_tokens=5)
    assert [c["model"] for c in sdk.calls] == ["claude-haiku-4-5-20251001"] * 3


def test_role_env_override_is_per_role(fake, monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_MODEL_VISION", "claude-sonnet-x")
    sdk = fake["install"]([_msg(), _msg()])
    llm.complete("hi", model_role="vision", max_tokens=5)
    llm.complete("hi", model_role="extract", max_tokens=5)
    assert [c["model"] for c in sdk.calls] == ["claude-sonnet-x", "claude-haiku-4-5-20251001"]


def test_legacy_llm_claude_model_applies_to_chat_only(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_MODEL_CHAT", "")
    monkeypatch.setenv("ANTHROPIC_MODEL_EXTRACT", "")
    monkeypatch.setenv("LLM_CLAUDE_MODEL", "legacy-chat")
    assert llm.model_for("chat") == "legacy-chat"
    assert llm.model_for("extract") == "claude-haiku-4-5-20251001"
    monkeypatch.setenv("ANTHROPIC_MODEL_CHAT", "role-chat")
    assert llm.model_for("chat") == "role-chat"


def test_unknown_role_rejected() -> None:
    with pytest.raises(ValueError):
        llm.model_for("vison")


# --- request shape --------------------------------------------------------------


def test_prompt_and_images_build_single_user_turn(fake) -> None:
    sdk = fake["install"]([_msg()])
    llm.complete(
        "describe", model_role="vision", max_tokens=99, system="S", temperature=0,
        images=[("image/png", "AAA"), "BBB"], timeout=12.0,
    )
    call = sdk.calls[0]
    assert call == {
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 99,
        "system": "S",
        "temperature": 0,
        "timeout": 12.0,
        "messages": [{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAA"}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "BBB"}},
            {"type": "text", "text": "describe"},
        ]}],
    }


def test_optional_fields_omitted_when_unset(fake) -> None:
    sdk = fake["install"]([_msg()])
    llm.complete("hi", model_role="extract", max_tokens=5)
    assert set(sdk.calls[0]) == {"model", "max_tokens", "messages"}


def test_prompt_and_messages_are_exclusive(fake) -> None:
    fake["install"]([_msg()])
    with pytest.raises(ValueError):
        llm.complete("hi", model_role="chat", max_tokens=5, messages=[{"role": "user", "content": "x"}])
    with pytest.raises(ValueError):
        llm.complete(model_role="chat", max_tokens=5)


def test_tools_passed_through_and_text_joined(fake) -> None:
    resp = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="a"), SimpleNamespace(type="tool_use", id="t"),
                 SimpleNamespace(type="text", text="b")],
        stop_reason="tool_use", usage=None,
    )
    sdk = fake["install"]([resp])
    tools = [{"name": "t", "input_schema": {"type": "object"}}]
    res = llm.complete("hi", model_role="chat", max_tokens=5, tools=tools)
    assert sdk.calls[0]["tools"] == tools
    assert res.text == "ab" and len(res.content) == 3 and res.ok


# --- per-role retry budgets ---------------------------------------------------


def test_chat_policy_is_fast_and_batch_roles_keep_five_attempts() -> None:
    chat = llm.policy_for("chat")
    assert chat.max_attempts == 3  # 1 try + 2 retries, the old SDK default
    assert chat.max_backoff_s <= 2.0
    assert chat.max_slot_wait_s is not None and chat.max_slot_wait_s <= 20.0
    assert chat.deadline_s is not None and chat.deadline_s <= 20.0
    for role in ("vision", "extract"):
        p = llm.policy_for(role)
        assert p.max_attempts == 5 and p.base_s == 4.0
        assert p.deadline_s is None and p.max_slot_wait_s is None


def test_chat_role_gives_up_after_two_retries_with_short_sleeps(fake) -> None:
    sdk = fake["install"]([_StatusError(529, "overloaded_error")] * 5 + [_msg()])
    with pytest.raises(_StatusError):
        llm.complete("hi", model_role="chat", max_tokens=10)
    assert len(sdk.calls) == 3 and fake["slots"] == 3
    assert len(fake["sleeps"]) == 2 and all(0 < s <= 2.0 for s in fake["sleeps"])
    # every chat request carries the remaining deadline as its timeout
    assert all(0 < c["timeout"] <= 20.0 for c in sdk.calls)


def test_batch_role_still_retries_five_times_on_overload(fake) -> None:
    sdk = fake["install"]([_StatusError(529, "overloaded_error")] * 4 + [_msg("done")])
    res = llm.complete("hi", model_role="extract", max_tokens=10)
    assert res.text == "done" and res.attempts == 5 and len(sdk.calls) == 5
    assert "timeout" not in sdk.calls[0]


def test_chat_retry_after_beyond_budget_fails_immediately(fake) -> None:
    sdk = fake["install"]([_StatusError(429, headers={"retry-after": "30"}), _msg()])
    with pytest.raises(_StatusError):
        llm.complete("hi", model_role="chat", max_tokens=10)
    assert len(sdk.calls) == 1 and fake["sleeps"] == []


def test_chat_short_retry_after_is_honoured_within_cap(fake) -> None:
    sdk = fake["install"]([_StatusError(429, headers={"retry-after": "1"}), _msg("ok2")])
    res = llm.complete("hi", model_role="chat", max_tokens=10)
    assert res.text == "ok2" and len(sdk.calls) == 2
    assert 1.0 < fake["sleeps"][0] <= 2.0


def test_chat_deadline_stops_retries(fake, monkeypatch) -> None:
    clock = {"t": 1000.0}
    monkeypatch.setattr(llm, "_monotonic", lambda: clock["t"])

    def slow_fail(**_kw: Any) -> Any:
        clock["t"] += 18.0  # the request itself ate most of the 20 s budget
        raise _StatusError(503)

    sdk = fake["install"]([])
    sdk.messages = SimpleNamespace(create=slow_fail)
    with pytest.raises(_StatusError):
        llm.complete("hi", model_role="chat", max_tokens=10)
    assert fake["sleeps"] == []  # no retry crosses the deadline


def test_chat_retry_gets_the_remaining_deadline_as_timeout(fake, monkeypatch) -> None:
    clock = {"t": 1000.0}
    monkeypatch.setattr(llm, "_monotonic", lambda: clock["t"])
    seen: list[float] = []
    outcomes = [_StatusError(503), _msg("late ok")]

    def create(**kw: Any) -> Any:
        seen.append(kw["timeout"])
        clock["t"] += 10.0
        out = outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return out

    sdk = fake["install"]([])
    sdk.messages = SimpleNamespace(create=create)
    res = llm.complete("hi", model_role="chat", max_tokens=10)
    assert res.text == "late ok"
    assert seen[0] == 20.0 and seen[1] == pytest.approx(10.0)  # sleeps are faked


def test_chat_limiter_wait_is_bounded(fake, monkeypatch) -> None:
    monkeypatch.setattr(llm, "_slot_wait_estimate", lambda cost=1.0: 45.0)
    sdk = fake["install"]([_msg()])
    with pytest.raises(llm.LLMRateLimited):
        llm.complete("hi", model_role="chat", max_tokens=10)
    assert sdk.calls == [] and fake["slots"] == 0
    # batch roles still queue for the slot however long it takes
    res = llm.complete("hi", model_role="extract", max_tokens=10)
    assert res.ok and fake["slots"] == 1


def test_slot_wait_estimate_reads_the_shared_bucket(monkeypatch) -> None:
    import time as _time

    from backend.vision import claude_rate_limit as rl

    monkeypatch.setenv("ANTHROPIC_VISION_RPM", "60")  # 1 token / s
    monkeypatch.setattr(rl, "_last_refill", _time.monotonic())
    monkeypatch.setattr(rl, "_tokens", 0.0)
    assert 0.8 < llm._slot_wait_estimate() <= 1.0
    monkeypatch.setattr(rl, "_tokens", 5.0)
    assert llm._slot_wait_estimate() == 0.0
    monkeypatch.setattr(rl, "_last_refill", 0.0)
    assert llm._slot_wait_estimate() == 0.0


def test_anthropic_max_retries_can_lower_but_not_raise_chat(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_MAX_RETRIES", "1")
    assert llm.policy_for("chat").max_attempts == 1
    monkeypatch.setenv("ANTHROPIC_MAX_RETRIES", "8")
    assert llm.policy_for("chat").max_attempts == 3
    assert llm.policy_for("extract").max_attempts == 8


def test_max_attempts_one_is_a_single_request(fake) -> None:
    sdk = fake["install"]([_StatusError(529), _msg()])
    with pytest.raises(_StatusError):
        llm.complete("hi", model_role="extract", max_tokens=10, max_attempts=1)
    assert len(sdk.calls) == 1 and fake["sleeps"] == []


def test_ask_haiku_specs_is_a_single_attempt(fake, monkeypatch) -> None:
    """Restored pre-consolidation behaviour: one request, no retries on overload."""
    from backend.enrichment import service

    monkeypatch.setattr(service, "_load_haiku_cache", lambda *a, **k: None)
    monkeypatch.setattr(service, "_save_haiku_cache", lambda *a, **k: None)
    sdk = fake["install"]([_StatusError(529, "overloaded_error"), _msg('{"cylinders": 4}')])
    assert service._ask_haiku_specs("Toyota", "Camry", 2025, "LE") is None
    assert len(sdk.calls) == 1 and fake["sleeps"] == []
    assert sdk.calls[0]["timeout"] == 20.0


def test_web_chat_call_sites_use_the_fast_policy(fake, monkeypatch) -> None:
    """agent._claude_reply and llm_client._complete_claude fail after 3 tries, <= 2 s sleeps."""
    from backend.intelligence.ai import agent
    from backend.utils import llm_client

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-chat-key")
    for call in (
        lambda: agent._claude_reply("sys", "how many seats?", max_tokens=50),
        lambda: llm_client._complete_claude("hi", system=None, temperature=0.1, max_tokens=50),
    ):
        fake["sleeps"].clear()
        sdk = fake["install"]([_StatusError(529, "overloaded_error")] * 6)
        with pytest.raises(_StatusError):
            call()
        assert len(sdk.calls) == 3
        assert len(fake["sleeps"]) == 2 and sum(fake["sleeps"]) <= 4.0
        assert all(0 < c["timeout"] <= 20.0 for c in sdk.calls)
