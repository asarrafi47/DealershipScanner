"""
Local LLM client (Ollama-backed) with load-adaptive model selection.

Design premise: the model only *narrates* structured data we already have — it
never retrieves facts — so a small model is accurate and cheap. To avoid starving
the scanner (Playwright + a swarm of Chromium procs) on the shared dev box, the
model tier adapts to system load:

    scanner running (or RAM tight)  -> LIGHT model  (fast, small footprint)
    machine idle                    -> HEAVY model  (better prose / reasoning)

Model selection reuses ``scanner_liveness.scanner_pids`` — the same signal the
DDL guard uses — so "is a scan running" is detected exactly, not guessed.

Config (env):
    LOCAL_LLM_BASE_URL     default http://localhost:11434  (point at a model
                           server in prod; the Railway web container can't host
                           a 7B itself)
    LOCAL_LLM_LIGHT_MODEL  default qwen2.5:3b-instruct
    LOCAL_LLM_HEAVY_MODEL  default qwen2.5:7b-instruct
    LOCAL_LLM_FORCE_MODEL  override — always use this exact model/tag
    LOCAL_LLM_MIN_FREE_GB  default 6 — below this free RAM, force LIGHT
    LOCAL_LLM_TIMEOUT_S    default 60
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("local_llm")

DEFAULT_BASE_URL = "http://localhost:11434"
DEFAULT_LIGHT = "qwen2.5:3b-instruct"
DEFAULT_HEAVY = "qwen2.5:7b-instruct"


def _base_url() -> str:
    return (os.environ.get("LOCAL_LLM_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")


def _light_model() -> str:
    return (os.environ.get("LOCAL_LLM_LIGHT_MODEL") or DEFAULT_LIGHT).strip()


def _heavy_model() -> str:
    return (os.environ.get("LOCAL_LLM_HEAVY_MODEL") or DEFAULT_HEAVY).strip()


def _min_free_gb() -> float:
    try:
        return float(os.environ.get("LOCAL_LLM_MIN_FREE_GB") or 6.0)
    except ValueError:
        return 6.0


def _timeout_s() -> float:
    try:
        return float(os.environ.get("LOCAL_LLM_TIMEOUT_S") or 60.0)
    except ValueError:
        return 60.0


def _scanner_running() -> bool:
    """True when an inventory scan is live (reuses the DDL-guard signal)."""
    try:
        from backend.scripts.scanner_liveness import scanner_pids

        return bool(scanner_pids())
    except Exception:
        return False


def _free_ram_gb() -> float:
    """Free + inactive (reclaimable) RAM in GB via vm_stat; 0.0 if unavailable."""
    try:
        out = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return 0.0
    page_size = 16384  # Apple silicon default; refined from the header if present
    free_pages = inactive_pages = 0
    for line in out.splitlines():
        low = line.lower()
        if "page size of" in low:
            digits = "".join(c for c in low if c.isdigit())
            if digits:
                page_size = int(digits)
        elif low.startswith("pages free:"):
            free_pages = int("".join(c for c in line if c.isdigit()) or 0)
        elif low.startswith("pages inactive:"):
            inactive_pages = int("".join(c for c in line if c.isdigit()) or 0)
    return (free_pages + inactive_pages) * page_size / (1024 ** 3)


@dataclass
class ModelChoice:
    model: str
    tier: str          # "light" | "heavy" | "forced"
    reason: str


def pick_model() -> ModelChoice:
    """Choose the model tier from current system load."""
    forced = (os.environ.get("LOCAL_LLM_FORCE_MODEL") or "").strip()
    if forced:
        return ModelChoice(forced, "forced", "LOCAL_LLM_FORCE_MODEL")
    if _scanner_running():
        return ModelChoice(_light_model(), "light", "scanner_running")
    free = _free_ram_gb()
    if free and free < _min_free_gb():
        return ModelChoice(_light_model(), "light", f"low_ram_{free:.1f}gb")
    return ModelChoice(_heavy_model(), "heavy", "idle")


# ── Ollama HTTP ──────────────────────────────────────────────────────────────


def _post(path: str, payload: dict) -> dict:
    import requests

    resp = requests.post(f"{_base_url()}{path}", json=payload, timeout=_timeout_s())
    resp.raise_for_status()
    return resp.json()


def available_models() -> list[str]:
    """Model tags currently pulled on the server ([] on any failure)."""
    import requests

    try:
        resp = requests.get(f"{_base_url()}/api/tags", timeout=10)
        resp.raise_for_status()
        return [m.get("name", "") for m in resp.json().get("models", [])]
    except Exception:
        return []


def generate(
    prompt: str,
    *,
    system: str | None = None,
    model: str | None = None,
    json_schema: dict | None = None,
    temperature: float = 0.3,
    max_tokens: int | None = None,
) -> str:
    """
    One-shot completion. ``model=None`` auto-picks the tier from system load.
    Pass ``json_schema`` to constrain output to valid JSON (Ollama structured
    output) — used for NL→filters query parsing where a stray token must not
    break search.
    """
    choice = ModelChoice(model, "forced", "explicit") if model else pick_model()
    options: dict[str, Any] = {"temperature": temperature}
    if max_tokens:
        options["num_predict"] = int(max_tokens)
    payload: dict[str, Any] = {
        "model": choice.model,
        "prompt": prompt,
        "stream": False,
        "options": options,
    }
    if system:
        payload["system"] = system
    if json_schema is not None:
        payload["format"] = json_schema
    logger.debug("local_llm generate: model=%s tier=%s reason=%s", choice.model, choice.tier, choice.reason)
    data = _post("/api/generate", payload)
    return (data.get("response") or "").strip()


def generate_json(prompt: str, schema: dict, *, system: str | None = None, model: str | None = None) -> Any:
    """generate() with a JSON schema, parsed. Returns None if the model emits invalid JSON."""
    raw = generate(prompt, system=system, model=model, json_schema=schema, temperature=0.0)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.warning("local_llm generate_json: non-JSON output: %s", raw[:200])
        return None
