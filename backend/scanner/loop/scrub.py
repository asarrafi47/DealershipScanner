"""Publisher scrubber (BUNDLE.md section 9). Pure, standard library only: the environment is passed in.

Nothing here logs, and no exception or assertion message carries a matched value. Callers of the
publisher catch everything and drop the field, never the run.
"""

from __future__ import annotations

import re
import unicodedata
from urllib.parse import unquote_plus

MIN_SECRET_LEN = 8
JSON_STRING_CAP = 500
ENV_MARK = "[redacted:env]"
PARAM_MARK = "[redacted:param]"
COUNT_KEYS = ("env_value", "url_param", "header_value", "control_chars")

_NAME_PARTS = ("URL", "TOKEN", "KEY", "SECRET", "PASSWORD")
_CRED_URL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://([^/\s:@]+):([^/\s@]+)@([^/\s?#]+)")
_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1", "[::1]", "0.0.0.0"})

_ANSI_RE = re.compile(
    "\x1b\\[[0-?]*[ -/]*[@-~]"  # CSI
    "|\x1b\\][^\x07\x1b]*(?:\x07|\x1b\\\\)?"  # OSC
    "|\x1b[@-Z\\\\-_]"  # two-character sequences
)
_NEEDS_CLEAN_RE = re.compile("[^\x20-\x7e\n]")
_URL_RE = re.compile(r"https?://[^\s\"'<>\\]+", re.IGNORECASE)
_USERINFO_RE = re.compile(r"(https?://)[^\s/?#]*@", re.IGNORECASE)
_PARAM_RE = re.compile(r"([?&;#])([^=&#;\s]+)=([^&#;\s]*)")
_KEY_NAME_RE = re.compile(
    r"(^|[_.-])(api[_.-]?)?(key|token|secret|password|passwd|pwd|auth|authorization|sig|signature|session|sessionid)$"
)
_HEADER_RE = re.compile(
    r"(?<![A-Za-z0-9_-])(?P<name>authorization|cookie|set-cookie|x-(?:[a-z0-9_-]{0,60}-)?key)"
    r"(?P<q>[\"']?)(?P<sep>[ \t]*[:=][ \t]*)"
    r"(?P<val>\"[^\"\n]*\"|'[^'\n]*'|[^\n]*)",
    re.IGNORECASE,
)
_HEADER_HINTS = ("authorization", "cookie", "x-")
_WS_RE = re.compile(r"\s+")


def _zero_counts() -> dict:
    return dict.fromkeys(COUNT_KEYS, 0)


def secret_values(env) -> list:
    """Exact secret values found in ``env`` (a mapping), longest first, each 8+ characters."""
    found = set()

    def add(value) -> None:
        if isinstance(value, str) and len(value) >= MIN_SECRET_LEN and value not in ENV_MARK + PARAM_MARK:
            found.add(value)

    for name, value in dict(env).items():
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        upper = name.upper()
        if any(part in upper for part in _NAME_PARTS):
            add(value)
        if upper.startswith("RAILWAY_") and (upper.endswith("_ID") or upper.endswith("_DOMAIN")):
            add(value)
        match = _CRED_URL_RE.match(value)
        if match:
            add(value)
            add(match.group(2))
            host = match.group(3).rsplit("@", 1)[-1]
            if host.startswith("["):
                host = host.split("]", 1)[0] + "]"
            else:
                host = host.split(":", 1)[0]
            if host.lower() not in _LOOPBACK and not host.startswith("127."):
                add(host)
    return sorted(found, key=lambda v: (-len(v), v))


def _drop_controls(text: str):
    """Remove ANSI sequences and Unicode C-category characters except LF and TAB; return (text, removed)."""
    if not _NEEDS_CLEAN_RE.search(text):
        return text, 0
    removed = 0
    text, n = _ANSI_RE.subn("", text)
    removed += n
    kept = []
    for ch in text:
        if ch == "\n" or ch == "\t" or not unicodedata.category(ch).startswith("C"):
            kept.append(ch)
        else:
            removed += 1
    return "".join(kept), removed


def _key_like(name: str) -> bool:
    return bool(_KEY_NAME_RE.search(unquote_plus(name).lower()))


def _scrub_url(match, counts: dict) -> str:
    url = match.group(0)

    def param(m):
        value = m.group(3)
        if value and value != PARAM_MARK and _key_like(m.group(2)):
            counts["url_param"] += 1
            return m.group(1) + m.group(2) + "=" + PARAM_MARK
        return m.group(0)

    return _PARAM_RE.sub(param, url)


def _scrub_header(match, counts: dict) -> str:
    val = match.group("val")
    sep = match.group("sep")
    quote = val[:1] if val[:1] in ("'", '"') and len(val) >= 2 and val[-1] == val[0] else ""
    if quote:
        inner, rest = val[1:-1], ""
    elif ":" in sep:
        inner, rest = val, ""
    else:
        cut = re.search(r"[\s&,;\"']", val)
        inner, rest = (val[: cut.start()], val[cut.start():]) if cut else (val, "")
    if inner == "" or inner.startswith("[redacted:"):
        return match.group(0)
    counts["header_value"] += 1
    return match.group("name") + match.group("q") + sep + quote + PARAM_MARK + quote + rest


def _scrub(text: str, secrets: list, counts: dict) -> str:
    text, removed = _drop_controls(text)
    counts["control_chars"] += removed
    for value in secrets:
        n = text.count(value)
        if n:
            counts["env_value"] += n
            text = text.replace(value, ENV_MARK)
    if "://" in text:
        text, n = _USERINFO_RE.subn(r"\1", text)
        counts["url_param"] += n
        text = _URL_RE.sub(lambda m: _scrub_url(m, counts), text)
    lowered = text.lower()
    if any(hint in lowered for hint in _HEADER_HINTS):
        text = _HEADER_RE.sub(lambda m: _scrub_header(m, counts), text)
    return text


def scrub_text(text, env):
    """Return ``(clean_text, counts)``; non-string input comes back unchanged with zero counts."""
    counts = _zero_counts()
    if not isinstance(text, str):
        return text, counts
    return _scrub(text, secret_values(env), counts), counts


def clean_line(text, max_len: int):
    """One line of at most ``max_len`` characters: controls dropped, whitespace collapsed, cut with ``...``."""
    if not isinstance(text, str):
        return text
    text, _ = _drop_controls(text)
    text = _WS_RE.sub(" ", text).strip()
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[: max(0, max_len)]
    return text[: max_len - 3].rstrip() + "..."


def _cut(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def scrub_json(obj, env):
    """Scrub every string of nested dicts, lists and tuples into a new structure; return ``(obj, counts)``."""
    counts = _zero_counts()
    secrets = secret_values(env)

    def walk(node):
        if isinstance(node, str):
            return _cut(_scrub(node, secrets, counts), JSON_STRING_CAP)
        if isinstance(node, dict):
            out = {}
            for key, value in node.items():
                out[_scrub(key, secrets, counts) if isinstance(key, str) else key] = walk(value)
            return out
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, tuple):
            return tuple(walk(item) for item in node)
        return node

    return walk(obj), counts


def assert_clean(blobs, env) -> None:
    """Raise ``ValueError`` if final bytes still hold an exact secret value or a key-like query parameter.

    ``blobs`` is a mapping of label to bytes/str, or an iterable of bytes/str. The message names only
    the blob and the rule, never the matched value.
    """
    items = list(blobs.items()) if hasattr(blobs, "items") else list(enumerate(blobs))
    encoded = [(v.encode("utf-8") if isinstance(v, str) else bytes(v)) for v in secret_values(env)]
    for label, blob in items:
        data = blob.encode("utf-8") if isinstance(blob, str) else bytes(blob)
        for i, secret in enumerate(encoded):
            if secret in data:
                raise ValueError("scrub check failed: blob " + str(label) + " still holds secret value #" + str(i))
        text = data.decode("utf-8", errors="replace")
        for m in _URL_RE.finditer(text):
            for p in _PARAM_RE.finditer(m.group(0)):
                if p.group(3) and p.group(3) != PARAM_MARK and _key_like(p.group(2)):
                    raise ValueError("scrub check failed: blob " + str(label) + " still holds a key-like query parameter")
