"""
Provider-routed LLM client: Claude (prod) or local Ollama (dev), one interface.

Rationale: the marketplace already runs its car chat on Claude Haiku
(``backend/intelligence/ai/agent.py``). For local development we don't want to
spend API tokens or need a key — a local model narrates/answers from the same
structured data. This module routes a single ``complete()`` call to whichever
provider is active, so callers (narrator, Q&A, chat) don't branch on environment.

Provider selection (``LLM_PROVIDER``):
    auto   (default) -> "claude" when ANTHROPIC_API_KEY is set, else "local"
    claude           -> always Claude Messages API
    local            -> always local Ollama (load-adaptive tier via local_llm)

So prod (key present) gets Haiku; a dev box (no key) gets the local hybrid, with
no code change. Force either explicitly with LLM_PROVIDER.

Config:
    LLM_PROVIDER            auto | claude | local
    ANTHROPIC_API_KEY       required for the claude path
    LLM_CLAUDE_MODEL        default claude-haiku-4-5-20251001
    LLM_CLAUDE_MAX_TOKENS   default 1024
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("llm_client")

DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5-20251001"


def active_provider() -> str:
    """Resolve the effective provider name: 'claude' or 'local'."""
    choice = (os.environ.get("LLM_PROVIDER") or "auto").strip().lower()
    if choice in ("claude", "anthropic"):
        return "claude"
    if choice in ("local", "ollama"):
        return "local"
    # auto
    return "claude" if (os.environ.get("ANTHROPIC_API_KEY") or "").strip() else "local"


def _claude_model() -> str:
    return (os.environ.get("LLM_CLAUDE_MODEL") or DEFAULT_CLAUDE_MODEL).strip()


def _claude_max_tokens() -> int:
    try:
        return int(os.environ.get("LLM_CLAUDE_MAX_TOKENS") or 1024)
    except ValueError:
        return 1024


def _complete_claude(
    prompt: str, *, system: str | None, temperature: float, max_tokens: int | None,
) -> str:
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    kwargs: dict[str, Any] = {
        "model": _claude_model(),
        "max_tokens": max_tokens or _claude_max_tokens(),
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system
    resp = client.messages.create(**kwargs)
    parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
    return "".join(parts).strip()


def _complete_local(
    prompt: str, *, system: str | None, temperature: float, max_tokens: int | None,
    json_schema: dict | None,
) -> str:
    from backend.utils.local_llm import generate

    return generate(
        prompt, system=system, temperature=temperature,
        max_tokens=max_tokens, json_schema=json_schema,
    )


def complete(
    prompt: str,
    *,
    system: str | None = None,
    temperature: float = 0.3,
    max_tokens: int | None = None,
    json_schema: dict | None = None,
    provider: str | None = None,
) -> str:
    """
    One-shot completion routed to the active provider.

    ``provider`` overrides the env-resolved choice (useful for tests/benchmarks).
    ``json_schema`` constrains output to JSON on the local provider; on Claude it
    is appended to the prompt as an instruction (the caller should still parse).
    """
    prov = (provider or active_provider()).lower()
    if prov == "claude":
        p = prompt
        if json_schema is not None:
            p = f"{prompt}\n\nRespond with JSON only, matching this schema:\n{json_schema}"
        return _complete_claude(p, system=system, temperature=temperature, max_tokens=max_tokens)
    return _complete_local(
        prompt, system=system, temperature=temperature,
        max_tokens=max_tokens, json_schema=json_schema,
    )
