"""
The one way the backend calls the Anthropic Messages API.

Before 2026-10 the API was reached through six transports (raw ``requests``
POSTs with their own retry loops, direct SDK calls with the SDK's default
retries, and the ``claude_rate_limit`` wrapper), each with its own retry,
refusal and truncation handling and a hard-coded model id (audit
docs/monolith_audit_2026_10_01, enrich.md P1 #6). This module replaces them:

* :func:`complete` — one call on the official SDK.
* One retry classifier (:func:`call_with_retry`): 408/409/429/5xx/529-overloaded
  and connection/timeout errors are retried with jittered exponential backoff
  (``retry-after`` honoured), every attempt first takes a slot from the shared
  process-wide limiter in ``backend.vision.claude_rate_limit``. Anything else
  (400, 401, 403, 404, 413...) is raised on the first attempt — the original SDK
  exception, so callers that inspect it (auth-error checks) keep working.
* Two retry budgets (:class:`RetryPolicy`, picked by role in :func:`policy_for`):
  batch roles (vision, extract) get 5 attempts with 4 s-based backoff; the
  ``chat`` role answers a web request on the single gunicorn worker, so it fails
  fast like the SDK defaults it replaced — at most 2 retries, backoff <= 2 s, a
  bounded limiter wait and a 20 s overall deadline — and the caller falls back
  (local model / error bubble) instead of holding the worker 30-60 s during an
  Anthropic overload.
* Explicit outcomes: :class:`LLMResult` carries ``stop_reason`` and the
  ``truncated`` / ``refused`` flags. A refusal or a max_tokens cut is never
  reported as ``ok``; :meth:`LLMResult.require_complete` turns either into an
  exception for callers that only accept a finished answer.
* Model ids from one table keyed by role (:data:`ROLE_MODELS`), overridable per
  role with ``ANTHROPIC_MODEL_<ROLE>`` (e.g. ``ANTHROPIC_MODEL_VISION``). The
  defaults are the model each call site used before consolidation — this phase
  upgrades nothing.

Env knobs:
    ANTHROPIC_API_KEY             key (callers may also pass ``api_key=``)
    ANTHROPIC_MODEL_<ROLE>        per-role model override
    LLM_CLAUDE_MODEL              legacy override, honoured for role "chat" only
    ANTHROPIC_MAX_RETRIES         attempts per call, 1-8 (falls back to
                                  ANTHROPIC_VISION_MAX_RETRIES, default 5)
    ANTHROPIC_RETRY_BASE_S        backoff base seconds (default 4)
    (the chat role caps attempts at 3 and ignores the base; see CHAT_POLICY)
"""
from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

HAIKU_4_5 = "claude-haiku-4-5-20251001"

# role -> default model. Every entry is what that role's call sites sent before
# the consolidation (all Haiku 4.5); change a default only as a deliberate,
# measured model upgrade.
ROLE_MODELS: dict[str, str] = {
    # image in, JSON out: photo equipment, gallery classify, interior colour,
    # window-sticker images
    "vision": HAIKU_4_5,
    # text in, JSON out: spec Q&A, sticker text, listing description, job
    # diagnosis, brochure trim ladders
    "extract": HAIKU_4_5,
    # conversational answers: car-page chat, llm_client.complete
    "chat": HAIKU_4_5,
}

# Statuses worth another attempt. 529 = overloaded_error.
_RETRY_STATUSES = frozenset({408, 409, 429, 500, 502, 503, 504, 529})


class LLMError(RuntimeError):
    """Base class for outcomes this module reports as errors."""


class LLMNotConfigured(LLMError):
    """No API key available."""


class LLMIncomplete(LLMError):
    """The model answered but the answer is not a finished one."""

    def __init__(self, message: str, result: "LLMResult") -> None:
        super().__init__(message)
        self.result = result


class LLMRefusal(LLMIncomplete):
    """stop_reason == "refusal": the model declined; content is not an answer."""


class LLMTruncated(LLMIncomplete):
    """stop_reason == "max_tokens": the answer was cut by the token budget."""


@dataclass(frozen=True)
class LLMResult:
    text: str
    stop_reason: str | None
    model: str
    role: str
    content: list[Any] = field(default_factory=list)
    usage: Any = None
    attempts: int = 1
    raw: Any = None

    @property
    def truncated(self) -> bool:
        return self.stop_reason == "max_tokens"

    @property
    def refused(self) -> bool:
        return self.stop_reason == "refusal"

    @property
    def ok(self) -> bool:
        return not (self.truncated or self.refused)

    def require_complete(self) -> "LLMResult":
        if self.refused:
            raise LLMRefusal(f"{self.role} model refused ({self.model})", self)
        if self.truncated:
            raise LLMTruncated(f"{self.role} output truncated at max_tokens ({self.model})", self)
        return self


# ---------------------------------------------------------------------------
# Model routing
# ---------------------------------------------------------------------------


def model_for(role: str) -> str:
    """Model id for *role*: ``ANTHROPIC_MODEL_<ROLE>`` env, else the table."""
    key = (role or "").strip().lower()
    if key not in ROLE_MODELS:
        raise ValueError(f"unknown model_role {role!r}; known: {sorted(ROLE_MODELS)}")
    env = (os.environ.get(f"ANTHROPIC_MODEL_{key.upper()}") or "").strip()
    if env:
        return env
    if key == "chat":
        legacy = (os.environ.get("LLM_CLAUDE_MODEL") or "").strip()
        if legacy:
            return legacy
    return ROLE_MODELS[key]


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    """Retry budget for one call.

    ``max_attempts`` counts the first try. ``max_backoff_s`` caps every sleep; a
    ``retry-after`` hint above it ends the retries instead of retrying early.
    ``max_slot_wait_s`` bounds the wait for a shared-limiter slot (``None`` =
    wait as long as it takes). ``deadline_s`` bounds the whole call, including
    each request's own timeout (``None`` = no overall deadline).
    """

    max_attempts: int
    base_s: float
    max_backoff_s: float = 60.0
    max_slot_wait_s: float | None = None
    deadline_s: float | None = None


# A retry under a deadline is only worth starting with at least this much time left.
_MIN_ATTEMPT_S = 2.0


class LLMRateLimited(LLMError):
    """The shared limiter could not give a slot within the policy's wait bound."""


# Web chat: the old SDK defaults (2 retries, ~0.5-8 s backoff), tightened so one
# overloaded car-page chat cannot hold the single gunicorn worker.
CHAT_POLICY = RetryPolicy(
    max_attempts=3, base_s=0.5, max_backoff_s=2.0, max_slot_wait_s=5.0, deadline_s=20.0
)


def _max_attempts() -> int:
    raw = os.environ.get("ANTHROPIC_MAX_RETRIES") or os.environ.get("ANTHROPIC_VISION_MAX_RETRIES") or "5"
    try:
        return max(1, min(8, int(str(raw).strip())))
    except (TypeError, ValueError):
        return 5


def _backoff_base() -> float:
    try:
        return max(0.1, min(30.0, float((os.environ.get("ANTHROPIC_RETRY_BASE_S") or "4").strip())))
    except (TypeError, ValueError):
        return 4.0


def _status_of(exc: BaseException) -> int | None:
    status = getattr(exc, "status_code", None)
    if status is None:
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None) if resp is not None else None
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_connection_error(exc: BaseException) -> bool:
    try:
        import anthropic

        conn = getattr(anthropic, "APIConnectionError", None)
        if isinstance(conn, type) and isinstance(exc, conn):
            return True
    except ImportError:
        pass
    name = type(exc).__name__
    return isinstance(exc, (ConnectionError, TimeoutError)) or "Connection" in name or "Timeout" in name


def is_retryable(exc: BaseException) -> bool:
    status = _status_of(exc)
    if status is not None:
        return status in _RETRY_STATUSES or status >= 500
    if _is_connection_error(exc):
        return True
    msg = str(exc).lower()
    return "overloaded" in msg or "429" in msg


def _retry_after(exc: BaseException) -> float | None:
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None) if resp is not None else None
    if not headers:
        return None
    try:
        val = headers.get("retry-after")
        return float(val) if val is not None else None
    except (TypeError, ValueError, AttributeError):
        return None


def batch_policy() -> RetryPolicy:
    """Policy for batch roles (vision, extract), read from the env per call."""
    return RetryPolicy(max_attempts=_max_attempts(), base_s=_backoff_base())


def policy_for(role: str | None) -> RetryPolicy:
    """The retry budget for *role*: :data:`CHAT_POLICY` for chat, else batch."""
    if (role or "").strip().lower() == "chat":
        # ANTHROPIC_MAX_RETRIES may lower the chat budget, never raise it.
        return RetryPolicy(
            max_attempts=min(CHAT_POLICY.max_attempts, _max_attempts()),
            base_s=CHAT_POLICY.base_s,
            max_backoff_s=CHAT_POLICY.max_backoff_s,
            max_slot_wait_s=CHAT_POLICY.max_slot_wait_s,
            deadline_s=CHAT_POLICY.deadline_s,
        )
    return batch_policy()


def _backoff(attempt: int, exc: BaseException, policy: RetryPolicy | None = None) -> float:
    cap = policy.max_backoff_s if policy is not None else 60.0
    base_s = policy.base_s if policy is not None else _backoff_base()
    hinted = _retry_after(exc)
    if hinted is not None and hinted > 0:
        wait = min(cap, hinted) + random.uniform(0.05, 0.5)
    else:
        wait = min(cap, base_s * (2**attempt)) * random.uniform(0.5, 1.0) + random.uniform(0.05, 0.25)
    # A tight budget (chat) is a hard ceiling, jitter included; the batch 60 s cap
    # keeps its historical small jitter overshoot.
    return min(cap, wait) if cap < 60.0 else wait


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _monotonic() -> float:
    return time.monotonic()


def _slot_wait_estimate(cost: float = 1.0) -> float:
    """Seconds until the shared limiter would hand out a slot (read-only peek)."""
    try:
        from backend.vision import claude_rate_limit as rl

        rpm = rl._rpm_cap()
        rate = rpm / 60.0
        with rl._lock:
            if rl._last_refill <= 0:
                return 0.0  # bucket not started yet: starts full
            tokens = min(rpm, rl._tokens + max(0.0, time.monotonic() - rl._last_refill) * rate)
        if tokens >= cost or rate <= 0:
            return 0.0
        return (cost - tokens) / rate
    except Exception:  # noqa: BLE001 - a broken peek must not block the call
        return 0.0


def _acquire_slot(max_wait: float | None = None) -> None:
    """Take a shared-limiter slot; with *max_wait*, refuse instead of queueing longer."""
    # Looked up per call so tests (and the limiter's own tests) can patch it.
    from backend.vision import claude_rate_limit

    if max_wait is not None:
        need = _slot_wait_estimate()
        if need > max_wait:
            raise LLMRateLimited(
                f"anthropic limiter: next slot in {need:.1f}s exceeds the {max_wait:.1f}s bound"
            )
    claude_rate_limit.acquire_vision_slot()


def call_with_retry(
    fn: Callable[[], _T],
    *,
    label: str = "anthropic",
    policy: RetryPolicy | None = None,
    started: float | None = None,
) -> tuple[_T, int]:
    """Run *fn* under the shared limiter with *policy* (default: the batch policy).

    Returns ``(value, attempts)``. Re-raises the last exception unchanged; with a
    deadline, a retry whose sleep would cross it is not attempted.
    """
    pol = policy or batch_policy()
    t0 = _monotonic() if started is None else started
    attempts = max(1, int(pol.max_attempts))
    for attempt in range(attempts):
        slot_wait = pol.max_slot_wait_s
        if pol.deadline_s is not None:
            left = pol.deadline_s - (_monotonic() - t0)
            slot_wait = left if slot_wait is None else min(slot_wait, left)
        if slot_wait is None:
            _acquire_slot()
        else:
            _acquire_slot(slot_wait)
        try:
            return fn(), attempt + 1
        except Exception as exc:  # noqa: BLE001 - classified below
            if not is_retryable(exc) or attempt >= attempts - 1:
                raise
            hinted = _retry_after(exc)
            if pol.deadline_s is not None and hinted is not None and hinted > pol.max_backoff_s:
                raise  # fail-fast budget, told to wait longer than it allows: fail now
            wait = _backoff(attempt, exc, pol)
            if (
                pol.deadline_s is not None
                and (_monotonic() - t0) + wait + _MIN_ATTEMPT_S > pol.deadline_s
            ):
                raise  # no room left under the deadline for a useful retry
            logger.warning(
                "%s transient failure (status=%s, attempt %d/%d): %s; sleeping %.1fs",
                label, _status_of(exc), attempt + 1, attempts, str(exc)[:160], wait,
            )
            _sleep(wait)
    raise RuntimeError("call_with_retry: no attempts")  # pragma: no cover


# ---------------------------------------------------------------------------
# Request building
# ---------------------------------------------------------------------------


def image_block(data_b64: str, media_type: str = "image/jpeg") -> dict[str, Any]:
    return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data_b64}}


def _as_image_block(img: Any) -> dict[str, Any]:
    if isinstance(img, Mapping):
        if img.get("type") == "image":
            return dict(img)
        return image_block(str(img["data"]), str(img.get("media_type") or "image/jpeg"))
    if isinstance(img, (tuple, list)) and len(img) == 2:
        media_type, data = img
        return image_block(str(data), str(media_type))
    return image_block(str(img))


def build_messages(prompt: str | None, images: Iterable[Any] | None) -> list[dict[str, Any]]:
    """Single user turn: images first, then the text (the shape every caller used)."""
    imgs = [_as_image_block(i) for i in (images or [])]
    if not imgs:
        return [{"role": "user", "content": prompt or ""}]
    content: list[dict[str, Any]] = list(imgs)
    if prompt:
        content.append({"type": "text", "text": prompt})
    return [{"role": "user", "content": content}]


def _resolve_key(api_key: str | None) -> str:
    key = (api_key if api_key is not None else os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        raise LLMNotConfigured("ANTHROPIC_API_KEY not set")
    return key


def _sdk_client(api_key: str) -> Any:
    import anthropic

    # Retries are ours (call_with_retry), so the SDK's own are switched off —
    # otherwise one call could make 3 x attempts requests.
    return anthropic.Anthropic(api_key=api_key, max_retries=0)


def _text_of(content: Sequence[Any]) -> str:
    parts: list[str] = []
    for b in content or []:
        btype = b.get("type") if isinstance(b, Mapping) else getattr(b, "type", None)
        if btype == "text":
            parts.append((b.get("text") if isinstance(b, Mapping) else getattr(b, "text", "")) or "")
    return "".join(parts)


def complete(
    prompt: str | None = None,
    *,
    model_role: str,
    max_tokens: int,
    messages: list[dict[str, Any]] | None = None,
    system: str | list[dict[str, Any]] | None = None,
    images: Iterable[Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float | None = None,
    timeout: float | None = None,
    api_key: str | None = None,
    model: str | None = None,
    max_attempts: int | None = None,
) -> LLMResult:
    """One Messages API call.

    Pass either *prompt* (+ optional *images*: base64 strings, ``(media_type,
    b64)`` pairs or image blocks; sent as one user turn, images first) or a full
    *messages* list. *system* may be a string or a list of blocks (for
    ``cache_control``). *model* pins an exact id and bypasses the role table —
    only for scripts comparing models.

    The retry budget is the role's (:func:`policy_for`); *max_attempts* lowers
    or raises only the attempt count (``1`` = a single request, no retries).
    With the chat policy's deadline each request's timeout is the time left.

    Transport failures raise (after the retry policy). A refusal or truncation
    is returned, flagged, never as ``ok``; call ``.require_complete()`` to turn
    those into :class:`LLMRefusal` / :class:`LLMTruncated`.
    """
    images = list(images or [])
    has_prompt = prompt is not None or bool(images)
    if messages is not None and has_prompt:
        raise ValueError("complete() takes prompt/images or messages, not both")
    if messages is None and not has_prompt:
        raise ValueError("complete() needs prompt/images or messages")
    resolved_model = model or model_for(model_role)
    key = _resolve_key(api_key)
    kwargs: dict[str, Any] = {
        "model": resolved_model,
        "max_tokens": int(max_tokens),
        "messages": messages if messages is not None else build_messages(prompt, images),
    }
    if system:
        kwargs["system"] = system
    if temperature is not None:
        kwargs["temperature"] = temperature
    if tools:
        kwargs["tools"] = tools
    if timeout is not None:
        kwargs["timeout"] = timeout

    policy = policy_for(model_role)
    if max_attempts is not None:
        policy = RetryPolicy(
            max_attempts=max(1, int(max_attempts)),
            base_s=policy.base_s,
            max_backoff_s=policy.max_backoff_s,
            max_slot_wait_s=policy.max_slot_wait_s,
            deadline_s=policy.deadline_s,
        )
    client = _sdk_client(key)
    started = _monotonic()

    def _create() -> Any:
        if policy.deadline_s is None:
            return client.messages.create(**kwargs)
        left = max(0.5, policy.deadline_s - (_monotonic() - started))
        per_call = dict(kwargs)
        per_call["timeout"] = min(float(kwargs.get("timeout") or left), left)
        return client.messages.create(**per_call)

    resp, attempts = call_with_retry(
        _create, label=f"anthropic[{model_role}]", policy=policy, started=started
    )
    content = list(getattr(resp, "content", None) or [])
    result = LLMResult(
        text=_text_of(content),
        stop_reason=getattr(resp, "stop_reason", None),
        model=resolved_model,
        role=model_role,
        content=content,
        usage=getattr(resp, "usage", None),
        attempts=attempts,
        raw=resp,
    )
    if result.refused:
        logger.warning("anthropic[%s] refusal from %s", model_role, resolved_model)
    elif result.truncated:
        logger.warning(
            "anthropic[%s] output truncated at max_tokens=%s (%s)", model_role, max_tokens, resolved_model
        )
    return result
