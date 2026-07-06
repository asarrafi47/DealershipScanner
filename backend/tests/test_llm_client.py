"""Provider resolution for the LLM client (Claude prod / local dev)."""
from __future__ import annotations

from backend.utils import llm_client


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
