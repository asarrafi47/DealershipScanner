"""
Provider-routed LLM client: Claude (prod) or local Ollama (dev), one interface.

Rationale: the marketplace already runs its car chat on Claude Haiku
(``backend/intelligence/ai/agent.py``). For local development we don't want to
spend API tokens or need a key — a local model narrates/answers from the same
structured data. This module routes a single ``complete()`` call to whichever
provider is active, so callers (narrator, Q&A, chat) don't branch on environment.

Provider selection (``LLM_PROVIDER``):
    auto   (default) -> local Ollama when it is *reachable*, else Claude when a
                        real ANTHROPIC_API_KEY is configured
    claude           -> Claude Messages API first (local as a safety net)
    local            -> always local Ollama (load-adaptive tier via local_llm)

Local-first is deliberate. A dev box that has a model server running should never
pay for tokens, and — the bug this ordering fixes — a *dead* remote key must
never be why a user sees "assistant unavailable": on Railway there is no local
server, so the chain collapses to Claude and prod behaviour is unchanged.

Config:
    LLM_PROVIDER            auto | claude | local
    ANTHROPIC_API_KEY       required for the claude path
    LLM_CLAUDE_MODEL        default claude-haiku-4-5-20251001
    LLM_CLAUDE_MAX_TOKENS   default 1024
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("llm_client")

DEFAULT_CLAUDE_MODEL = "claude-haiku-4-5-20251001"

# Values people leave lying around in a shell profile / .env.example. A literal
# "your-api-key-here" exported by a dotfile made active_provider() answer
# "claude" and every assistant call then died on a 401 — an unusable key is the
# same as no key.
_PLACEHOLDER_KEY_MARKERS = (
    "your-api-key", "your_api_key", "yourkey", "placeholder", "changeme",
    "xxxx", "...", "…", "<", "sk-ant-api03-xxx",
)


def anthropic_key() -> str:
    """The configured Anthropic key, or "" when it is absent or a placeholder."""
    raw = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not raw or len(raw) < 6:
        return ""
    low = raw.lower()
    if any(marker in low for marker in _PLACEHOLDER_KEY_MARKERS):
        logger.debug("ignoring placeholder ANTHROPIC_API_KEY")
        return ""
    return raw


_LOCAL_PROBE_TTL_S = 30.0
_local_probe: tuple[float, bool] = (0.0, False)


def local_available(force: bool = False) -> bool:
    """Is a local model server up? Cached briefly — this sits on a request path."""
    global _local_probe
    ts, ok = _local_probe
    if not force and (time.monotonic() - ts) < _LOCAL_PROBE_TTL_S:
        return ok
    from backend.utils.local_llm import server_reachable

    ok = server_reachable()
    _local_probe = (time.monotonic(), ok)
    return ok


def provider_chain() -> tuple[str, ...]:
    """Providers to try, in order. Routing decision — see active_provider()."""
    choice = (os.environ.get("LLM_PROVIDER") or "auto").strip().lower()
    if choice in ("local", "ollama"):
        return ("local",)
    if choice in ("claude", "anthropic"):
        # Explicitly pinned to Claude, but still don't fail closed if a local
        # model is sitting right there.
        return ("claude", "local") if local_available() else ("claude",)
    if local_available():
        return ("local", "claude") if anthropic_key() else ("local",)
    return ("claude",) if anthropic_key() else ("local",)


def active_provider() -> str:
    """The *configured* provider: 'claude' when a usable key is set, else 'local'.

    Reflects configuration only. Actual routing uses provider_chain(), which also
    accounts for whether the local server is answering right now.
    """
    choice = (os.environ.get("LLM_PROVIDER") or "auto").strip().lower()
    if choice in ("claude", "anthropic"):
        return "claude"
    if choice in ("local", "ollama"):
        return "local"
    return "claude" if anthropic_key() else "local"


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

    client = anthropic.Anthropic(api_key=anthropic_key())
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

    ``provider`` pins one provider (no fallback) — useful for tests/benchmarks.
    Otherwise every provider in provider_chain() is tried in order, so one dead
    provider (expired key, model server down) can't take the feature offline.
    ``json_schema`` constrains output to JSON on the local provider; on Claude it
    is appended to the prompt as an instruction (the caller should still parse).
    """
    chain = (provider.lower(),) if provider else provider_chain()
    first_error: Exception | None = None
    for prov in chain:
        try:
            if prov == "claude":
                p = prompt
                if json_schema is not None:
                    p = f"{prompt}\n\nRespond with JSON only, matching this schema:\n{json_schema}"
                return _complete_claude(
                    p, system=system, temperature=temperature, max_tokens=max_tokens
                )
            return _complete_local(
                prompt, system=system, temperature=temperature,
                max_tokens=max_tokens, json_schema=json_schema,
            )
        except Exception as exc:
            first_error = first_error or exc
            logger.warning("%s completion failed (%s)", prov, str(exc)[:200], exc_info=True)
    # Re-raise the *first* failure: it came from the preferred provider, so it is
    # the one whose fix message the user needs ("start ollama", not "bad key").
    raise first_error if first_error else RuntimeError("no_llm_provider")
