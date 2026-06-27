"""
Diagnose failed dealer_jobs and recommend smart-retry actions.

Uses deterministic rules for known infra failures, then optional Claude Haiku when
``ANTHROPIC_API_KEY`` is set.
"""
from __future__ import annotations

import glob
import json
import logging
import os
import re
from typing import Any

from backend.scanner.retry_env import filter_retry_env, is_retry_env_key_allowed

_log = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"

_CATEGORIES = frozenset(
    {"infrastructure", "transient", "dealer_site", "code_bug", "configuration", "unknown"}
)
_STRATEGIES = frozenset(
    {
        "simple_retry",
        "retry_with_env",
        "retry_with_profile",
        "retry_with_python_scanner",
        "needs_code_fix",
        "needs_manual",
        "no_retry",
    }
)


def _playwright_chromium_path() -> str | None:
    home = os.path.expanduser("~")
    pattern = os.path.join(home, ".cache", "ms-playwright", "chromium-*", "chrome-linux", "chrome")
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


def _rule_diagnosis(*, error: str, log_tail: str, job_type: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    blob = f"{error}\n{log_tail}".lower()
    pw = _playwright_chromium_path()

    if any(
        tok in blob
        for tok in (
            "qemu-x86_64",
            "ld-linux-x86-64.so.2",
            "failed to launch the browser process",
            "chrome-linux64/chrome",
        )
    ):
        actions: list[dict[str, str]] = []
        if pw:
            actions.append({"type": "set_env", "key": "PUPPETEER_EXECUTABLE_PATH", "value": pw})
        return {
            "source": "rule",
            "summary": "Puppeteer tried to run an x86_64 Chromium binary on ARM Docker (browser launch failed).",
            "category": "infrastructure",
            "root_cause": "Architecture mismatch: bundled Puppeteer Chrome is AMD64 inside an aarch64 container.",
            "retry_recommended": bool(pw),
            "retry_strategy": "retry_with_env" if pw else "needs_code_fix",
            "retry_actions": actions,
            "code_fix_hint": "Point Puppeteer at Playwright's native ARM Chromium (docker entrypoint + scanner.js).",
            "confidence": 0.98,
        }

    if "module_not_found" in blob or "cannot find module" in blob:
        return {
            "source": "rule",
            "summary": "Node scanner failed because a required npm package was missing in the worker container.",
            "category": "infrastructure",
            "root_cause": "backend/scanner/node_modules not installed (bind mount hid image deps).",
            "retry_recommended": True,
            "retry_strategy": "simple_retry",
            "retry_actions": [],
            "code_fix_hint": "Worker entrypoint runs npm ci when node_modules is empty.",
            "confidence": 0.95,
        }

    if "job_timeout" in error.lower() or "timeout" in blob:
        return {
            "source": "rule",
            "summary": "Scanner exceeded the job timeout — the dealer site may be slow or the lot is very large.",
            "category": "transient",
            "root_cause": "SCANNER_JOB_TIMEOUT_SEC elapsed before scan finished.",
            "retry_recommended": True,
            "retry_strategy": "simple_retry",
            "retry_actions": [{"type": "set_profile", "value": "resilient"}],
            "code_fix_hint": None,
            "confidence": 0.75,
        }

    if any(tok in blob for tok in ("access denied", "captcha", "cloudflare", "403 forbidden", "blocked")):
        return {
            "source": "rule",
            "summary": "Dealer site may be blocking automated access (WAF / bot protection).",
            "category": "dealer_site",
            "root_cause": "HTTP or navigation blocked by dealer edge protection.",
            "retry_recommended": True,
            "retry_strategy": "retry_with_profile",
            "retry_actions": [
                {"type": "set_profile", "value": "resilient"},
            ],
            "code_fix_hint": "Persistent browser profile + resilient stealth may help; re-run Smart restart.",
            "confidence": 0.7,
        }

    if "scrape_confidence" in blob or '"level":"low"' in blob or '"level": "low"' in blob:
        return {
            "source": "rule",
            "summary": "Last scrape finished with low confidence — inventory may be incomplete or blocked.",
            "category": "dealer_site",
            "root_cause": "JSON intercept missed or WAF interfered; fallback paths did not recover enough data.",
            "retry_recommended": True,
            "retry_strategy": "retry_with_profile",
            "retry_actions": [{"type": "set_profile", "value": "resilient"}],
            "code_fix_hint": None,
            "confidence": 0.72,
        }

    if job_type == "onboard" and not (payload.get("url") or "").strip():
        return {
            "source": "rule",
            "summary": "Onboard job is missing the inventory URL in its payload.",
            "category": "configuration",
            "root_cause": "payload.url empty — cannot run smart import.",
            "retry_recommended": False,
            "retry_strategy": "needs_manual",
            "retry_actions": [],
            "code_fix_hint": "Re-queue with a valid https inventory URL.",
            "confidence": 0.99,
        }

    return None


def _llm_diagnosis(
    *,
    error: str,
    log_tail: str,
    job_type: str,
    dealer_id: str,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        return None
    try:
        import anthropic
        from backend.vision.claude_rate_limit import anthropic_messages_create
    except ImportError:
        return None

    prompt = {
        "error": error,
        "job_type": job_type,
        "dealer_id": dealer_id,
        "payload": payload,
        "log_tail": (log_tail or "")[-4000:],
    }
    system = (
        "You diagnose failed dealership inventory scanner jobs for operators. "
        "Return ONLY minified JSON with keys: summary, category, root_cause, retry_recommended (bool), "
        "retry_strategy, retry_actions (array of {type,value} or {type,key,value}), code_fix_hint (string|null), "
        "confidence (0-1). "
        f"category must be one of: {sorted(_CATEGORIES)}. "
        f"retry_strategy must be one of: {sorted(_STRATEGIES)}. "
        "retry_actions types: set_env, set_profile, use_python_scanner. "
        "If the log shows qemu-x86_64 or ld-linux-x86-64.so.2, category=infrastructure and suggest set_env "
        "PUPPETEER_EXECUTABLE_PATH to Playwright chromium. "
        "If a code bug in our repo is likely, use code_bug and needs_code_fix."
    )
    try:
        client = anthropic.Anthropic(api_key=key)
        msg = anthropic_messages_create(
            client,
            model=_MODEL,
            max_tokens=700,
            temperature=0,
            system=system,
            messages=[{"role": "user", "content": json.dumps(prompt, ensure_ascii=False)}],
        )
        text = ""
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                text += block.text
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        data = json.loads(text)
        if not isinstance(data, dict):
            return None
        data["source"] = "llm"
        return _normalize_diagnosis(data)
    except Exception as exc:
        _log.warning("job diagnosis LLM failed: %s", exc)
        return None


def _normalize_diagnosis(raw: dict[str, Any]) -> dict[str, Any]:
    cat = (raw.get("category") or "unknown").strip().lower()
    if cat not in _CATEGORIES:
        cat = "unknown"
    strat = (raw.get("retry_strategy") or "simple_retry").strip().lower()
    if strat not in _STRATEGIES:
        strat = "simple_retry"
    actions = raw.get("retry_actions")
    if not isinstance(actions, list):
        actions = []
    clean_actions: list[dict[str, str]] = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        t = (a.get("type") or "").strip().lower()
        if t == "set_env" and a.get("key"):
            key = str(a["key"])
            if is_retry_env_key_allowed(key):
                clean_actions.append(
                    {"type": "set_env", "key": key, "value": str(a.get("value") or "")}
                )
        elif t == "set_profile" and a.get("value"):
            clean_actions.append({"type": "set_profile", "value": str(a["value"])})
        elif t == "use_python_scanner":
            clean_actions.append({"type": "use_python_scanner"})
    try:
        conf = float(raw.get("confidence", 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    conf = max(0.0, min(1.0, conf))
    return {
        "source": raw.get("source") or "unknown",
        "summary": str(raw.get("summary") or "Job failed.")[:2000],
        "category": cat,
        "root_cause": str(raw.get("root_cause") or "")[:2000],
        "retry_recommended": bool(raw.get("retry_recommended", True)),
        "retry_strategy": strat,
        "retry_actions": clean_actions,
        "code_fix_hint": (str(raw["code_fix_hint"])[:2000] if raw.get("code_fix_hint") else None),
        "confidence": conf,
    }


def diagnose_failed_job(
    *,
    error: str,
    log_tail: str = "",
    job_type: str = "",
    dealer_id: str = "",
    payload: dict[str, Any] | None = None,
    use_llm: bool = True,
) -> dict[str, Any]:
    """Return operator-facing diagnosis dict for a failed job."""
    pl = payload or {}
    ruled = _rule_diagnosis(error=error, log_tail=log_tail, job_type=job_type, payload=pl)
    if ruled and ruled.get("confidence", 0) >= 0.9:
        return ruled
    if use_llm:
        llm = _llm_diagnosis(
            error=error,
            log_tail=log_tail,
            job_type=job_type,
            dealer_id=dealer_id,
            payload=pl,
        )
        if llm:
            if ruled and llm.get("confidence", 0) < ruled.get("confidence", 0):
                llm["rule_hint"] = ruled.get("summary")
            return llm
    if ruled:
        return ruled
    return {
        "source": "fallback",
        "summary": f"Job failed with {error or 'unknown error'}.",
        "category": "unknown",
        "root_cause": (log_tail or error or "")[:500],
        "retry_recommended": True,
        "retry_strategy": "simple_retry",
        "retry_actions": [],
        "code_fix_hint": None,
        "confidence": 0.4,
    }


def apply_diagnosis_to_payload(
    base_payload: dict[str, Any],
    diagnosis: dict[str, Any],
) -> dict[str, Any]:
    """Merge retry hints from diagnosis into a new job payload."""
    out = dict(base_payload or {})
    out.pop("ai_diagnosis", None)
    retry_env = filter_retry_env(out.get("retry_env"))
    for action in diagnosis.get("retry_actions") or []:
        if not isinstance(action, dict):
            continue
        t = action.get("type")
        if t == "set_env" and action.get("key"):
            key = str(action["key"])
            if is_retry_env_key_allowed(key):
                retry_env[key] = str(action.get("value") or "")
        elif t == "set_profile" and action.get("value"):
            out["profile"] = str(action["value"])
        elif t == "use_python_scanner":
            out["use_python_scanner"] = True
    if retry_env:
        out["retry_env"] = retry_env
    elif "retry_env" in out:
        out.pop("retry_env", None)
    out["ai_diagnosis"] = {
        "summary": diagnosis.get("summary"),
        "category": diagnosis.get("category"),
        "retry_strategy": diagnosis.get("retry_strategy"),
        "source_job_diagnosis": diagnosis.get("source"),
    }
    return out
