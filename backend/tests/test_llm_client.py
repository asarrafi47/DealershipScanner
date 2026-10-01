"""Provider resolution for the LLM client (Claude prod / local dev)."""
from __future__ import annotations

from backend.utils import llm_client, local_llm


def test_auto_local_without_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "auto")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert llm_client.active_provider() == "local"


def test_auto_claude_with_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "auto")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    assert llm_client.active_provider() == "claude"


def test_explicit_overrides(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("LLM_PROVIDER", "local")
    assert llm_client.active_provider() == "local"
    monkeypatch.setenv("LLM_PROVIDER", "claude")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert llm_client.active_provider() == "claude"


def test_complete_routes_to_local(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "local")
    seen = {}

    def fake_local(prompt, **kw):
        seen["called"] = True
        return "local answer"

    monkeypatch.setattr("backend.utils.local_llm.generate", fake_local)
    out = llm_client.complete("hi")
    assert out == "local answer" and seen.get("called")


def test_completion_is_a_str_with_stop_metadata():
    c = local_llm.Completion("hello", stop_reason="max_tokens", truncated=True, provider="claude")
    assert c == "hello" and c.upper() == "HELLO"
    assert c.truncated and c.stop_reason == "max_tokens" and c.provider == "claude"


def test_claude_truncation_flagged(monkeypatch):
    """A max_tokens stop must surface as truncated=True, not pass as a full answer."""

    class _Block:
        type = "text"
        text = "cut off mid-"

    class _Resp:
        content = [_Block()]
        stop_reason = "max_tokens"

    class _Messages:
        def create(self, **kw):
            return _Resp()

    class _Anthropic:
        def __init__(self, api_key=None, **kw):
            self.messages = _Messages()

    import sys
    import types

    fake = types.ModuleType("anthropic")
    fake.Anthropic = _Anthropic
    monkeypatch.setitem(sys.modules, "anthropic", fake)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-truncation")

    out = llm_client.complete("hi", provider="claude")
    assert out == "cut off mid-"
    assert out.truncated and out.stop_reason == "max_tokens" and out.provider == "claude"


def test_local_truncation_flagged(monkeypatch):
    monkeypatch.setattr(local_llm, "resolve_model", lambda m: m)
    monkeypatch.setattr(
        local_llm, "_post",
        lambda path, payload: {"response": "partial answer", "done_reason": "length"},
    )
    out = local_llm.generate("hi", model="m")
    assert out == "partial answer"
    assert out.truncated and out.stop_reason == "length" and out.provider == "local"


def test_local_complete_finished_not_flagged(monkeypatch):
    monkeypatch.setattr(local_llm, "resolve_model", lambda m: m)
    monkeypatch.setattr(
        local_llm, "_post",
        lambda path, payload: {"response": "full answer", "done_reason": "stop"},
    )
    out = local_llm.generate("hi", model="m")
    assert out == "full answer" and not out.truncated


def test_generate_json_discards_truncated_but_parseable(monkeypatch):
    """Truncated JSON that still parses is silently incomplete — must return None."""
    monkeypatch.setattr(local_llm, "resolve_model", lambda m: m)
    monkeypatch.setattr(
        local_llm, "_post",
        lambda path, payload: {"response": '{"items": [1, 2]}', "done_reason": "length"},
    )
    assert local_llm.generate_json("hi", {"type": "object"}, model="m") is None
