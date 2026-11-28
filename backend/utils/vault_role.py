"""Boot-time secret load from the vault with a role login (vault API v1.1.2).

Active only when ``VAULT_ADDR``, ``VAULT_ROLE_ID`` and ``VAULT_SECRET_ID`` are all
non-empty and ``KMAC_VAULT_DISABLE`` is not on. ``KMAC_VAULT_AUTO`` plays no part (prod
runs it at ``0``). When inactive, :func:`backend.utils.kmac_vault.load_kmac_vault_secrets`
keeps its old static-token behaviour unchanged.

Flow, once per process at boot (every entry point goes through
``load_kmac_vault_secrets``):

* ``POST {VAULT_ADDR}/auth/token`` with ``{role_id, secret_id}`` gives a short-lived
  access token (``expires_in`` seconds). It lives only in :data:`_TOKEN` (process
  memory), is refreshed 60 s (or half its lifetime, if shorter) before it expires, and is never written to the
  environment, a file or a log.
* ``GET {VAULT_ADDR}/get/dealershipscanner:<NAME>`` for every name in
  :data:`VAULT_ROLE_SECRET_NAMES`; the resolved value goes into ``os.environ``.

Precedence: the vault value wins, except for :data:`PROTECTED_NAMES`, whose current
non-empty environment value is kept when the vault differs (a changed session key,
SQLCipher key or phone pepper logs everyone out, locks the user databases or orphans
every stored phone hash), with a CRITICAL line naming the variable. The environment
is the fallback whenever the vault lacks a key, is sealed or unreachable past the
boot budget, or refuses the login or the key.

Sealed (503) and unreachable (connection error, timeout, other 5xx) are retried with
backoff 1, 2, 4, 8, 15, 15 ... s inside ONE budget for the whole load,
``VAULT_BOOT_WAIT_SECONDS`` (default 40, clamped to 0..120). The whole load is also
cut off at that budget + 10 s of wall time (a worker thread joined with a timeout), and
values reach ``os.environ`` only after the join. The vault re-seals after
its own deploys until a person unseals it, which is why the fallback exists.

Logging carries names and counts only: never a value, the role id, the secret id, the
token, the vault address or exception text (which could hold a response body).
The loader never raises.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

_log = logging.getLogger(__name__)

ENV_ADDR = "VAULT_ADDR"
ENV_ROLE_ID = "VAULT_ROLE_ID"
ENV_SECRET_ID = "VAULT_SECRET_ID"
ENV_WAIT = "VAULT_BOOT_WAIT_SECONDS"
ENV_DISABLE = "KMAC_VAULT_DISABLE"

KEY_PREFIX = "dealershipscanner:"

DEFAULT_WAIT_SECONDS = 40.0
MAX_WAIT_SECONDS = 120.0
BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0, 15.0)  # then 15 s each
HTTP_TIMEOUT_SECONDS = 5.0
# The whole load (retries, slow answers, DNS, a server that drips bytes) ends at
# budget + this, enforced by joining the worker thread; max_wait_seconds caps the total.
HARD_DEADLINE_EXTRA_SECONDS = 10.0
TOKEN_REFRESH_MARGIN_SECONDS = 60.0  # or half the token's lifetime when that is shorter
DEFAULT_TOKEN_TTL_SECONDS = 900.0
_MAX_BODY_BYTES = 1_000_000

# Every secret the code reads (names only; no database URL, no vault variable).
VAULT_ROLE_SECRET_NAMES: tuple[str, ...] = (
    # Flask session signing (backend/config.py Config.secret_key_raw: SECRET_KEY, then FLASK_SECRET_KEY)
    "SECRET_KEY",
    "FLASK_SECRET_KEY",
    # SQLCipher keys of the user databases (backend/db/users_sqlite.py, dev_users_sqlite.py)
    "USERS_DB_ENCRYPTION_KEY",
    "DEV_USERS_DB_ENCRYPTION_KEY",
    # Site admin bootstrap and admin login (scripts/bootstrap_site_admin.py, init_admin_db)
    "ADMIN_PASSWORD",
    # Claude features on the web and in the scanner post steps
    "ANTHROPIC_API_KEY",
    "GOOGLE_MAPS_API_KEY",
    "GOOGLE_CSE_API_KEY",  # backend/enrichment/spec_search_client.py
    "EIA_API_KEY",  # backend/cron/sync_gas_prices.py
    "REVIEWS_IP_HASH_SALT",  # backend/reviews/guards.py
    # Phone sign-in (backend/auth/phone/config.py)
    "PHONE_HASH_PEPPER",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_API_KEY_SID",
    "TWILIO_API_KEY_SECRET",
    "TWILIO_AUTH_TOKEN",
    "TWILIO_VERIFY_SERVICE_SID",
    "TURNSTILE_SECRET_KEY",
    # Origin lock behind Cloudflare (backend/utils/client_ip.py)
    "CF_ORIGIN_SECRET",
    # Billing (backend/billing/stripe_billing.py)
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "STRIPE_PREMIUM_WEBHOOK_SECRET",
    # OAuth sign-in (backend/auth/google_oauth.py, apple_oauth.py)
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "APPLE_OAUTH_PRIVATE_KEY",
    # Email codes and links (backend/utils/mfa_delivery.py, backend/auth/email_verification.py, password_reset.py)
    "RESEND_API_KEY",
    "SMTP_PASSWORD",
    "EMAIL_VERIFY_PEPPER",
    "PASSWORD_RESET_PEPPER",
    # /dev console gate (backend/dev/console.py)
    "DEV_CONSOLE_SECRET",
    # /api/loop server-side tokens (backend/routes/loop_api.py)
    "LOOP_READ_TOKEN",
    "LOOP_SUBMIT_TOKEN",
)

# A present environment value is kept over a differing vault value. FLASK_SECRET_KEY is
# the legacy alias of SECRET_KEY (Config.secret_key_raw), so it is protected too, and
# SECRET_KEY is compared against the effective session key (SECRET_KEY or FLASK_SECRET_KEY).
PROTECTED_NAMES: frozenset[str] = frozenset(
    {
        "SECRET_KEY",
        "FLASK_SECRET_KEY",
        "USERS_DB_ENCRYPTION_KEY",
        "DEV_USERS_DB_ENCRYPTION_KEY",
        "PHONE_HASH_PEPPER",
    }
)
_EFFECTIVE_ALIASES: dict[str, tuple[str, ...]] = {"SECRET_KEY": ("SECRET_KEY", "FLASK_SECRET_KEY")}

_TRUE = ("1", "true", "yes", "on")

# Response kinds
_SUCCESS = "ok"
_MISSING = "missing"  # 404
_UNAUTHORIZED = "unauthorized"  # 401
_FORBIDDEN = "forbidden"  # 403
_SEALED = "sealed"  # 503 with the sealed error
_UNREACHABLE = "unreachable"  # connection error, timeout, other 5xx
_BAD = "bad"  # any other answer (3xx, 4xx, unreadable JSON)
_RETRYABLE = (_SEALED, _UNREACHABLE)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def role_path_disabled() -> bool:
    return _env(ENV_DISABLE).lower() in _TRUE


def role_path_configured() -> bool:
    """True when the role login should run (all three variables set, not disabled)."""
    if role_path_disabled():
        return False
    return bool(_env(ENV_ADDR) and _env(ENV_ROLE_ID) and _env(ENV_SECRET_ID))


def boot_wait_seconds() -> float:
    raw = _env(ENV_WAIT)
    if not raw:
        return DEFAULT_WAIT_SECONDS
    try:
        val = float(raw)
    except ValueError:
        return DEFAULT_WAIT_SECONDS
    if val != val or val in (float("inf"), float("-inf")):  # NaN / inf
        return DEFAULT_WAIT_SECONDS
    return max(0.0, min(MAX_WAIT_SECONDS, val))


# ── token cache (process memory only) ───────────────────────────────────────────


@dataclass
class _TokenCache:
    value: str = ""
    expires_at: float = 0.0
    ttl: float = 0.0

    def usable(self, now: float) -> bool:
        margin = min(TOKEN_REFRESH_MARGIN_SECONDS, self.ttl / 2.0)
        return bool(self.value) and now < self.expires_at - margin

    def clear(self) -> None:
        self.value = ""
        self.expires_at = 0.0
        self.ttl = 0.0

    def __repr__(self) -> str:  # never show the token
        return f"_TokenCache(set={bool(self.value)})"


_TOKEN = _TokenCache()


def reset_token_cache() -> None:
    _TOKEN.clear()


# ── HTTP ─────────────────────────────────────────────────────────────────────────


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect would carry the bearer token to another URL: refuse it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


def _default_opener() -> urllib.request.OpenerDirector:
    # ProxyHandler({}): never send the role credentials or the token through an
    # HTTP(S)_PROXY from the environment; the vault is reached directly.
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


@dataclass
class _Resp:
    kind: str
    status: int = 0
    data: object = None


def _classify_http_error(err: urllib.error.HTTPError) -> _Resp:
    code = int(getattr(err, "code", 0) or 0)
    try:
        if code == 401:
            return _Resp(_UNAUTHORIZED, code)
        if code == 403:
            return _Resp(_FORBIDDEN, code)
        if code == 404:
            return _Resp(_MISSING, code)
        if code == 503:
            return _Resp(_SEALED if _body_says_sealed(err) else _UNREACHABLE, code)
        if 500 <= code < 600:
            return _Resp(_UNREACHABLE, code)
        return _Resp(_BAD, code)
    finally:
        try:
            err.close()
        except Exception:
            pass


def _body_says_sealed(err: urllib.error.HTTPError) -> bool:
    try:
        body = err.read(_MAX_BODY_BYTES)
        data = json.loads(body.decode("utf-8", "replace"))
        return isinstance(data, dict) and "sealed" in str(data.get("error") or "").lower()
    except Exception:
        return False


def _send(opener, req: urllib.request.Request, timeout: float) -> _Resp:
    try:
        with opener.open(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            body = resp.read(_MAX_BODY_BYTES)
        if status != 200:
            return _Resp(_BAD, status)
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            return _Resp(_BAD, status)
        return _Resp(_SUCCESS, status, data)
    except urllib.error.HTTPError as err:
        return _classify_http_error(err)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
        return _Resp(_UNREACHABLE)
    except Exception:
        return _Resp(_BAD)


# ── the load ─────────────────────────────────────────────────────────────────────


@dataclass
class RoleLoadReport:
    """Names only. ``state`` is ok, sealed, unreachable, refused, error or off."""

    state: str = "off"
    from_vault: list[str] = field(default_factory=list)
    kept_env: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    protected_kept: list[str] = field(default_factory=list)
    forbidden: list[str] = field(default_factory=list)


def _emit(level: int, msg: str, *args: object) -> None:
    """Log; at INFO with no handler anywhere (gunicorn preload before logging is set up),
    write the line to stderr so the one summary line is never lost. WARNING and above
    already reach stderr through logging's last-resort handler."""
    _log.log(level, msg, *args)
    if level < logging.WARNING and not _log.hasHandlers():
        try:
            sys.stderr.write("vault_role: " + (msg % args if args else msg) + "\n")
        except Exception:
            pass


class _Loader:
    def __init__(
        self,
        *,
        addr: str,
        role_id: str,
        secret_id: str,
        budget: float,
        hard_limit: float,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        opener,
    ) -> None:
        self._addr = addr.rstrip("/")
        self._role_id = role_id
        self._secret_id = secret_id
        self._clock = clock
        self._sleep = sleep
        self._opener = opener
        self.budget = budget
        start = clock()
        # Retries stop at the budget; the hard deadline also bounds a vault that answers
        # but slowly (30-odd keys at the request timeout each would otherwise run long).
        self.deadline = start + budget
        self.hard_deadline = start + hard_limit
        self.stopped = False
        self.stop_reason = ""  # sealed | unreachable | refused | error
        self.relogged = False
        self.bad_answers = 0
        self._critical_once: set[str] = set()

    # transport with retries inside the one budget
    def _call(self, build: Callable[[], urllib.request.Request]) -> _Resp:
        attempt = 0
        while True:
            left = self.hard_deadline - self._clock()
            if left <= 0:
                self._stop(_UNREACHABLE)
                return _Resp(_UNREACHABLE)
            timeout = max(0.1, min(HTTP_TIMEOUT_SECONDS, left))
            r = _send(self._opener, build(), timeout)
            if r.kind not in _RETRYABLE:
                return r
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            attempt += 1
            remaining = self.deadline - self._clock()
            if remaining <= 0:
                self._stop(r.kind)
                return r
            _log.debug("vault %s; retry %d in %.0fs", r.kind, attempt, min(delay, remaining))
            self._sleep(min(delay, remaining))

    def _stop(self, reason: str) -> None:
        if not self.stopped:
            self.stopped = True
            self.stop_reason = reason

    def _critical(self, kind: str, msg: str, *args: object) -> None:
        if kind in self._critical_once:
            return
        self._critical_once.add(kind)
        _emit(logging.CRITICAL, msg, *args)

    def _login(self) -> str:
        if self.stopped:
            return ""
        payload = json.dumps({"role_id": self._role_id, "secret_id": self._secret_id}).encode("utf-8")
        url = self._addr + "/auth/token"

        def build() -> urllib.request.Request:
            return urllib.request.Request(
                url,
                data=payload,
                method="POST",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )

        r = self._call(build)
        if r.kind == _SUCCESS and isinstance(r.data, dict):
            tok = r.data.get("access_token")
            if isinstance(tok, str) and tok.strip():
                ttl = r.data.get("expires_in")
                try:
                    ttl_s = float(ttl) if ttl is not None and not isinstance(ttl, bool) else DEFAULT_TOKEN_TTL_SECONDS
                except (TypeError, ValueError):
                    ttl_s = DEFAULT_TOKEN_TTL_SECONDS
                if not ttl_s > 0:
                    ttl_s = DEFAULT_TOKEN_TTL_SECONDS
                _TOKEN.value = tok.strip()
                _TOKEN.ttl = ttl_s
                _TOKEN.expires_at = self._clock() + ttl_s
                return _TOKEN.value
            r = _Resp(_BAD, r.status)
        if r.kind in (_UNAUTHORIZED, _FORBIDDEN):
            self._critical(
                "login",
                "vault refused the role login (HTTP %d); using environment values (check %s / %s)",
                r.status,
                ENV_ROLE_ID,
                ENV_SECRET_ID,
            )
            self._stop("refused")
        elif r.kind in _RETRYABLE:
            self._stop(r.kind)
        else:
            self._critical("login-bad", "vault role login gave an unexpected answer (HTTP %d); using environment values", r.status)
            self._stop("error")
        return ""

    def _token(self) -> str:
        if _TOKEN.usable(self._clock()):
            return _TOKEN.value
        _TOKEN.clear()
        return self._login()

    def _get_once(self, key: str, tok: str) -> _Resp:
        url = self._addr + "/get/" + urllib.parse.quote(key, safe=":")
        headers = {"Authorization": "Bearer " + tok, "Accept": "application/json"}
        return self._call(lambda: urllib.request.Request(url, headers=headers, method="GET"))

    def fetch(self, name: str) -> tuple[str, str | None]:
        """(kind, value). kind is ok / missing / forbidden / stopped / bad."""
        if self.stopped:
            return ("stopped", None)
        if self._clock() >= self.hard_deadline:
            self._stop(_UNREACHABLE)
            return ("stopped", None)
        tok = self._token()
        if not tok:
            return ("stopped", None)
        key = KEY_PREFIX + name
        r = self._get_once(key, tok)
        if r.kind == _UNAUTHORIZED and not self.relogged:
            self.relogged = True
            _TOKEN.clear()
            tok = self._login()
            if not tok:
                return ("stopped", None)
            r = self._get_once(key, tok)
        if r.kind == _UNAUTHORIZED:
            _TOKEN.clear()
            self._critical(
                "get-401",
                "vault rejected the access token (HTTP 401) after a fresh login; using environment values",
            )
            self._stop("refused")
            return ("stopped", None)
        if r.kind == _FORBIDDEN:
            return (_FORBIDDEN, None)
        if r.kind == _MISSING:
            return (_MISSING, None)
        if r.kind in _RETRYABLE:
            return ("stopped", None)
        if r.kind == _SUCCESS and isinstance(r.data, dict):
            got_key = r.data.get("key")
            value = r.data.get("value")
            if (got_key is None or got_key == key) and isinstance(value, str):
                value = value.strip()
                return (_SUCCESS, value) if value else (_MISSING, None)
        self.bad_answers += 1
        return (_BAD, None)


def _apply(report: RoleLoadReport, fetched: dict[str, str], snapshot: dict[str, str]) -> None:
    for name in VAULT_ROLE_SECRET_NAMES:
        env_val = snapshot.get(name, "")
        vault_val = fetched.get(name)
        if vault_val:
            if name in PROTECTED_NAMES:
                effective = ""
                for alias in _EFFECTIVE_ALIASES.get(name, (name,)):
                    effective = snapshot.get(alias, "")
                    if effective:
                        break
                if effective and effective != vault_val:
                    report.protected_kept.append(name)
                    report.kept_env.append(name)
                    _emit(
                        logging.CRITICAL,
                        "vault value for %s differs from the environment; keeping the environment value "
                        "(protected name: change it in both places together)",
                        name,
                    )
                    continue
            if (os.environ.get(name) or "") != vault_val:
                os.environ[name] = vault_val
            report.from_vault.append(name)
        elif env_val:
            report.kept_env.append(name)
        else:
            report.missing.append(name)


@dataclass
class _Fetched:
    """What the worker thread found (names and values). Read by the caller only after join."""

    values: dict[str, str] = field(default_factory=dict)
    forbidden: list[str] = field(default_factory=list)
    stop_reason: str = ""
    bad_answers: int = 0
    error: str = ""  # exception type name
    done: bool = False


def _fetch_all(loader_args: dict, out: _Fetched, lock: threading.Lock) -> None:
    """Worker thread: network only. Never touches os.environ; publishes into ``out``
    under ``lock`` at the end, and only if the caller has not given up on it."""
    values: dict[str, str] = {}
    forbidden: list[str] = []
    stop_reason = ""
    bad = 0
    error = ""
    try:
        loader = _Loader(**loader_args)
        for name in VAULT_ROLE_SECRET_NAMES:
            if loader.stopped:
                break
            kind, value = loader.fetch(name)
            if kind == _SUCCESS and value:
                values[name] = value
            elif kind == _FORBIDDEN:
                forbidden.append(name)
        stop_reason = loader.stop_reason
        bad = loader.bad_answers
    except Exception as exc:
        error = type(exc).__name__
    with lock:
        if out.done:  # the caller timed out and moved on: publish nothing
            return
        out.values, out.forbidden, out.stop_reason, out.bad_answers, out.error = values, forbidden, stop_reason, bad, error
        out.done = True


def load_role_secrets(
    *,
    max_wait_seconds: float | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
    opener=None,
) -> RoleLoadReport:
    """Run the role-login load when configured; never raises. Returns names only.

    The network part runs in a daemon thread joined for at most the hard limit,
    ``VAULT_BOOT_WAIT_SECONDS`` + ``HARD_DEADLINE_EXTRA_SECONDS`` of real time, so no
    slow DNS answer or dripping server can hold the boot longer. ``max_wait_seconds``
    caps that whole limit, extra included (the web entrypoint's admin bootstrap passes
    10). Values are written to ``os.environ`` only here, in the calling thread, after
    the join; a timed-out load applies nothing and the environment is the fallback.
    """
    report = RoleLoadReport()
    try:
        if not role_path_configured():
            return report
        addr = _env(ENV_ADDR)
        if not (addr.startswith("http://") or addr.startswith("https://")):
            _emit(logging.CRITICAL, "%s is not an http(s) URL; vault role login skipped, using environment values", ENV_ADDR)
            report.state = "error"
            _finish(report, {}, _snapshot())
            return report
        budget = boot_wait_seconds()
        hard_limit = budget + HARD_DEADLINE_EXTRA_SECONDS
        if max_wait_seconds is not None:
            try:
                cap = max(0.0, float(max_wait_seconds))
                budget = min(budget, cap)
                hard_limit = min(hard_limit, cap)
            except (TypeError, ValueError):
                pass
        snapshot = _snapshot()
        out = _Fetched()
        lock = threading.Lock()
        worker = threading.Thread(
            target=_fetch_all,
            name="vault-role-load",
            daemon=True,
            args=(
                dict(
                    addr=addr,
                    role_id=_env(ENV_ROLE_ID),
                    secret_id=_env(ENV_SECRET_ID),
                    budget=budget,
                    hard_limit=hard_limit,
                    clock=clock or time.monotonic,
                    sleep=sleep or time.sleep,
                    opener=opener or _default_opener(),
                ),
                out,
                lock,
            ),
        )
        worker.start()
        worker.join(hard_limit)
        with lock:
            timed_out = not out.done
            out.done = True  # a late worker now publishes nothing
        if timed_out:
            report.state = "timeout"
            _emit(
                logging.WARNING,
                "vault role load did not finish within %.0fs; nothing applied, using environment values",
                hard_limit,
            )
            _finish(report, {}, snapshot)
            return report
        if out.error:
            report.state = "error"
            _emit(logging.CRITICAL, "vault role load failed (%s); using environment values", out.error)
            _finish(report, {}, snapshot)
            return report
        report.forbidden = list(out.forbidden)
        report.state = out.stop_reason or "ok"
        if out.stop_reason in _RETRYABLE:
            _emit(
                logging.WARNING,
                "vault %s past the %.0fs boot budget (%s); using environment values",
                out.stop_reason,
                budget,
                ENV_WAIT,
            )
        if report.forbidden:
            _emit(
                logging.CRITICAL,
                "vault denied %d key(s) outside the role's scope (HTTP 403); using environment values: %s",
                len(report.forbidden),
                ", ".join(report.forbidden),
            )
        if out.bad_answers:
            _emit(logging.WARNING, "vault gave %d unexpected answer(s); those names use environment values", out.bad_answers)
        _finish(report, out.values, snapshot)
    except Exception as exc:  # never raise out of boot
        report.state = "error"
        try:
            _emit(logging.CRITICAL, "vault role load failed (%s); using environment values", type(exc).__name__)
        except Exception:
            pass
    return report


def _snapshot() -> dict[str, str]:
    return {n: (os.environ.get(n) or "").strip() for n in VAULT_ROLE_SECRET_NAMES}


def _finish(report: RoleLoadReport, fetched: dict[str, str], snapshot: dict[str, str]) -> None:
    _apply(report, fetched, snapshot)
    if report.missing:
        _emit(
            logging.CRITICAL,
            "no value from the vault or the environment for %d name(s): %s",
            len(report.missing),
            ", ".join(report.missing),
        )
    _emit(
        logging.INFO,
        "vault role load (%s): %d from vault, %d kept env, %d missing; from vault: %s; kept env: %s; missing: %s",
        report.state,
        len(report.from_vault),
        len(report.kept_env),
        len(report.missing),
        ", ".join(report.from_vault) or "-",
        ", ".join(report.kept_env) or "-",
        ", ".join(report.missing) or "-",
    )
