"""
Local LLM client (Ollama-backed) with load-adaptive model selection.

Design premise: the model only *narrates* structured data we already have — it
never retrieves facts. To avoid starving the scanner (Playwright + a swarm of
Chromium procs) on the shared dev box, the model tier adapts to system load:

    scanner running (or RAM tight)  -> LIGHT model  (fast, small footprint)
    machine idle                    -> HEAVY model  (better prose / reasoning)

Model selection reuses ``scanner_liveness.scanner_pids`` — the same signal the
DDL guard uses — so "is a scan running" is detected exactly, not guessed.

Why the tiers are 7B/14B and not 3B/7B
--------------------------------------
The premise above does *not* make any small model safe: narration still has to be
faithful. 3B was dropped after it invented a 1,500 lb tow rating for a Nissan
Sentra, invented seating capacity, and restated supplied warranty tables with the
wrong corrosion and battery terms (measured 2026-07-30 on three listings).

Re-measured 2026-07-31 on the two shipping tiers, after the prompt was split into
an instruction half and a data half. Sample: 10 active priced listings drawn at
random from Postgres (setseed 0.42) — 134318, 115699, 153798, 146547, 528420,
96855, 119765, 152517, 146442, 92206. Two batteries per tier, every stated fact
checked against the ``cars`` row and the verified-specs block the prompt is built
from, and every reply read individually:

    battery A — 14 single-fact questions x 10 cars = 140 answers
      (price, mileage, VIN, colour, drivetrain, transmission, fuel, cylinders,
       dealer, stock #, towing, horsepower, 0-60, MPG)
        qwen2.5:7b-instruct    137/140 correct. The 3 misses are all false
                               "that isn't in this listing's data" for a figure
                               that WAS in the prompt (hp 201 on #134318,
                               fuel Hybrid on #115699, tow 7,700 on #528420).
                               No fabricated and no contradicted fact.
        qwen2.5:14b-instruct   140/140 factually correct. Two answers dropped
                               trailing boilerplate from a value ("Anvil
                               Clear-Coat" for "Anvil Clear-Coat Exterior
                               Paint"); nothing stated was wrong.

    battery B — 7 questions per car whose answer is genuinely absent, chosen to
      bait fabrication (seating, payload, warranty, crash rating, cargo volume,
      ground clearance, battery kWh) = 70 answers per tier
        qwen2.5:7b-instruct    70/70. 64 refusals; the 6 warranty answers were
        qwen2.5:14b-instruct   70/70. correct restatements of a warranty table
                               that really is in the listing JSON (checked term
                               by term against block (2)/(3) on all 6 cars).

So: 7B = 207/210, 14B = 210/210 on this sample, with zero fabricated facts on
either tier.

What the earlier "1 wrong in 140 / 0 wrong on 14B" figure got wrong
------------------------------------------------------------------
It was an undercount, and the reason matters more than the number: a uniformly
random car sample hides a failure mode concentrated in a subpopulation. Asking
"what fuel does it take?" of 14 random *hybrid/EV* listings, 2 runs each, 22 of
28 7B answers returned the EPA row's "Regular Gasoline"/"Premium Gasoline"
instead of the dealer's "Hybrid" — deterministic, same wrong answer on both runs.
In a random 10-car sample that is 1-2 answers. The cause was a prompt-assembly
bug, not the model (``_VERIFIED_KEYS_SHADOWED_BY_DEALER`` in
``backend/intelligence/ai/agent.py`` did not list ``fuel_type``); after the fix
the same 28 answers are 24/28, and the 4 misses are refusals rather than wrong
fuels. Ratios quoted here are for these batteries on this sample — they are not a
general accuracy claim for free-form chat.

Warm latency cost: ~0.4s (3B) -> ~0.5s (7B) -> ~2.5s (14B) per turn.

Config (env):
    LOCAL_LLM_BASE_URL     default http://localhost:11434  (point at a model
                           server in prod; the Railway web container can't host
                           a 7B itself)
    LOCAL_LLM_LIGHT_MODEL  default qwen2.5:7b-instruct
    LOCAL_LLM_HEAVY_MODEL  default qwen2.5:14b-instruct
    LOCAL_LLM_FORCE_MODEL  override — always use this exact model/tag
    LOCAL_LLM_MIN_FREE_GB  default 12 — below this free RAM, force LIGHT (the
                           heavy tag is ~9GB on disk and ~12GB resident per
                           `ollama ps`, so 6GB of headroom was not enough)
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
DEFAULT_LIGHT = "qwen2.5:7b-instruct"
DEFAULT_HEAVY = "qwen2.5:14b-instruct"


class LocalLLMUnavailable(RuntimeError):
    """The local model server could not answer, with a *specific* reason.

    Callers surface this to users, so the reason has to name the missing piece
    ("ollama isn't running", "that model isn't pulled") — a generic "assistant
    unavailable" tells a user nothing they can act on.
    """

    def __init__(self, reason: str, detail: str = "", model: str = ""):
        self.reason = reason      # server_unreachable | model_missing | timeout | http_error
        self.detail = detail
        self.model = model
        super().__init__(detail or reason)


def human_reason(exc: BaseException) -> str:
    """One user-facing sentence naming what to start / pull / fix."""
    base = _base_url()
    if isinstance(exc, LocalLLMUnavailable):
        if exc.reason == "server_unreachable":
            return (f"The local AI model server isn't reachable at {base}. "
                    "Start it with `ollama serve` and try again.")
        if exc.reason == "model_missing":
            return (f"The local model `{exc.model}` isn't installed on the model server. "
                    f"Install it with `ollama pull {exc.model}`.")
        if exc.reason == "timeout":
            return (f"The local model `{exc.model}` didn't answer within the timeout. "
                    "It may still be loading — try again in a moment.")
    return f"The local AI model at {base} returned an error: {str(exc)[:160]}"


def _base_url() -> str:
    return (os.environ.get("LOCAL_LLM_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")


def _light_model() -> str:
    return (os.environ.get("LOCAL_LLM_LIGHT_MODEL") or DEFAULT_LIGHT).strip()


def _heavy_model() -> str:
    return (os.environ.get("LOCAL_LLM_HEAVY_MODEL") or DEFAULT_HEAVY).strip()


def _min_free_gb() -> float:
    try:
        return float(os.environ.get("LOCAL_LLM_MIN_FREE_GB") or 12.0)
    except ValueError:
        return 12.0


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
    """POST to Ollama, translating transport failures into LocalLLMUnavailable.

    Every failure mode here is one a human can fix (start the server, pull the
    model, wait for a cold load), so it must not reach the caller as an opaque
    requests exception.
    """
    import requests

    model = str(payload.get("model") or "")
    try:
        resp = requests.post(f"{_base_url()}{path}", json=payload, timeout=_timeout_s())
    except requests.Timeout as exc:
        raise LocalLLMUnavailable("timeout", str(exc), model) from exc
    except requests.RequestException as exc:
        raise LocalLLMUnavailable("server_unreachable", str(exc), model) from exc
    if resp.status_code == 404:
        # Ollama answers 404 for an unknown model tag — the server is up, the
        # weights just aren't pulled.
        raise LocalLLMUnavailable("model_missing", resp.text[:200], model)
    if resp.status_code >= 400:
        raise LocalLLMUnavailable("http_error", f"HTTP {resp.status_code}: {resp.text[:200]}", model)
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


_TAGS_TTL_S = 60.0
_tags_cache: tuple[float, list[str]] = (0.0, [])


def _installed(force: bool = False) -> list[str]:
    """available_models() with a short TTL — generate() consults it every call."""
    global _tags_cache
    import time

    ts, tags = _tags_cache
    if force or (time.monotonic() - ts) > _TAGS_TTL_S:
        tags = available_models()
        _tags_cache = (time.monotonic(), tags)
    return tags


def resolve_model(model: str) -> str:
    """Nearest installed model to *model*.

    A box that pulled a different tag than the configured tier (or renamed one)
    shouldn't take the assistant down: fall back to another installed chat model
    rather than 404. An empty tag list means the server is unreachable — return
    the request unchanged so _post() raises the accurate "not running" error.
    """
    tags = _installed()
    if not tags or model in tags:
        return model
    # Ollama accepts a bare name for a ":latest" tag; match that spelling too.
    for tag in tags:
        if tag == model or tag.split(":")[0] == model.split(":")[0]:
            return tag
    for candidate in (_light_model(), _heavy_model()):
        if candidate in tags:
            logger.warning("local_llm: model %s not installed; using %s", model, candidate)
            return candidate
    for tag in tags:
        if "embed" not in tag:  # embedding models can't do completion
            logger.warning("local_llm: model %s not installed; using %s", model, tag)
            return tag
    return model


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
    choice = ModelChoice(resolve_model(choice.model), choice.tier, choice.reason)
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


# ── Managed server lifecycle (dev) ───────────────────────────────────────────
# The webapp owns the Ollama server in local dev so assistant search always has
# its LLM fallback: started with the app, stopped with it. A server that was
# already running (started by hand or by another app) is left alone on exit.

_managed_proc: subprocess.Popen | None = None


def server_reachable(timeout_s: float = 1.5) -> bool:
    import requests

    try:
        requests.get(f"{_base_url()}/api/version", timeout=timeout_s).raise_for_status()
        return True
    except Exception:
        return False


def ensure_server_running(wait_s: float = 15.0) -> bool:
    """Start ``ollama serve`` if the server isn't reachable. True when usable.

    Set ``LOCAL_LLM_AUTOSTART=0`` to opt out. Only a locally-hosted default
    base URL is autostarted — a remote LOCAL_LLM_BASE_URL is just probed.
    """
    global _managed_proc
    if (os.environ.get("LOCAL_LLM_AUTOSTART") or "1").strip().lower() in ("0", "false", "no", "off"):
        return server_reachable()
    if server_reachable():
        return True
    if "localhost" not in _base_url() and "127.0.0.1" not in _base_url():
        logger.warning("local_llm: remote server %s unreachable; not autostarting", _base_url())
        return False

    import shutil
    import time

    binary = shutil.which("ollama")
    if not binary:
        logger.warning("local_llm: ollama binary not found; assistant search LLM fallback disabled")
        return False
    try:
        _managed_proc = subprocess.Popen(
            [binary, "serve"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as exc:
        logger.warning("local_llm: failed to start ollama serve: %s", exc)
        return False
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        if server_reachable(timeout_s=1.0):
            logger.info("local_llm: started managed ollama serve (pid %s)", _managed_proc.pid)
            import atexit

            atexit.register(shutdown_managed_server)
            return True
        if _managed_proc.poll() is not None:
            logger.warning("local_llm: ollama serve exited immediately (code %s)", _managed_proc.returncode)
            _managed_proc = None
            return False
        time.sleep(0.3)
    logger.warning("local_llm: ollama serve did not become reachable within %.0fs", wait_s)
    return False


def shutdown_managed_server() -> None:
    """Stop the ollama we spawned. A pre-existing external server is untouched."""
    global _managed_proc
    proc = _managed_proc
    _managed_proc = None
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info("local_llm: stopped managed ollama serve")
    except OSError:
        pass
