#!/usr/bin/env python3
"""Secret scan: refuse pushes that would publish a secret or a run artifact.

    python3 scripts/secret_scan.py --range OLD..NEW    # blobs the commits in OLD..NEW add
    python3 scripts/secret_scan.py --staged            # blobs the next commit would add
    python3 scripts/secret_scan.py --all [--rev REV]   # every file in the tree at REV (HEAD)

Options: ``--local-env`` also blocks any blob holding a value from this clone's
own untracked env files (``.env``, ``.env.*`` except templates, in the checkout
and in the main worktree); ``--env-file PATH`` (repeatable) does the same for
one KEY=VALUE file. Only keys that name a secret count. ``--allowlist PATH``
reads the allowlist from disk instead of from the commit being judged;
``--print-allowlist`` prints ready-made allowlist lines for the findings (local
use only: it refuses to run under CI).

What a mode scans
-----------------
``--range OLD..NEW`` takes the commits ``git rev-list OLD..NEW`` lists. An OLD
of all zeros (git's "no such ref", as a push of a new branch or tag reports it)
means: the commits reachable from NEW that are on no remote-tracking ref
(``refs/remotes/*``), i.e. the commits the remote does not have yet. Every
commit is diffed against its parents (``git diff-tree -r -c --root
--no-renames``), and each blob it adds or changes is scanned. A merge is judged
by what it adds itself (content that matches none of its parents), so a merged
branch counts through its own commits, which the range lists when they are new.
A file added and deleted inside the range is scanned all the same: its blob is
in the history being published.

``--staged`` scans what ``git diff --cached`` adds, ``--all`` the whole tree.

What blocks (exit 1)
--------------------
* Provider keys (Google API and OAuth, Anthropic, OpenAI, Stripe secret,
  restricted and webhook, AWS, GitHub, Slack, SendGrid, Twilio, Resend, Hugging
  Face, npm, PyPI, Postman, Railway, Algolia and Typesense header keys),
  private-key blocks, JWT-shaped tokens and password hashes (bcrypt, argon2,
  Werkzeug pbkdf2/scrypt). A provider key is let through only when it is an
  unmistakable placeholder (it spells example, placeholder, your-, dummy,
  fake, sample, redacted or xxxx, or runs one character six times); a
  private-key block only when it cannot be a key (a body under 40 base64
  characters, or one holding "...", "<" or ">").
* Database URLs with a password, unless the password is a placeholder: a
  common default (postgres, password, changeme, ...), the user name itself, 3
  characters or fewer, an interpolation (``${VAR}``, ``<pw>``), a value that
  names itself (``local-secret-pw``), or a documentation host (example.com,
  ``*.invalid``, ``*.test``).
* Generic secret assignments: ``api_key = "..."``, ``"password": "..."``,
  ``token == "..."`` (quoted, any text file) and ``SECRET_KEY=...`` /
  ``password: ...`` (unquoted: env, shell, Dockerfile, YAML/INI/TOML; not in
  code files, where the right-hand side is an expression) whose value is 12+
  characters and not placeholder-like. Placeholder-like: a template spelling
  (example, changeme, your-, dummy, placeholder, xxxx, pytest, insecure,
  do-not-use, ``${...}``, ``<...>``), an env var name, a path, a URL without
  credentials, or a value whose words name its own kind or call it a test value
  (``long-enough-password``, ``reset-token-abc``, ``test-api-key``).
* Any added path in PATH_RULES below: SQLite files, workspace/**, *.har, env
  files other than templates (.example, .sample, .template, .dist), private-key
  and keystore files, credential files, database dumps and git bundles. Path
  findings are judged on the name, so binary files are covered too.
* With ``--local-env``/``--env-file``: any verbatim value of a secret-named key.

Binary blobs (a NUL in the first 8000 bytes) are judged by path only; blobs over
100 MB (GitHub refuses them anyway) too. A blob git cannot read is an error
(exit 2), never a pass.

The allowlist
-------------
Known benign fixtures (test placeholders, documentation examples) are listed in
scripts/secret_scan_allowlist.txt, one per line::

    <rule>  sha256:<hex digest of the matched value>  <path>

The digest is of the value itself (for a path rule: of the file's bytes), never
the value. An entry lets through exactly that value of that rule at that path;
the same value anywhere else, or a changed value, is still blocked. Env-value
findings are never allowlisted. The allowlist is read from the commit being
judged (NEW for --range, the index for --staged, REV for --all), so the hook and
CI judge exactly what is pushed.

Output is masked: the first 6 and last 3 characters of a 20+ character value,
less for shorter ones, never a database password, an env value or a whole
email address. ``--print-allowlist`` prints digests, which for a weak password
would let anyone confirm a guess, so it stays out of CI logs.

Bypass
------
``git push --no-verify`` is the only way past the pre-push hook, and it skips
every pre-push check (VERSION rule, release guard, secret scan). CI's
``secret-scan`` job still scans the pushed range. A real secret is never
allowlisted: remove it, rotate it (it stays in the local history), and rewrite
the unpushed commits.

Exit codes: 0 clean, 1 blocked, 2 usage error or a git failure.

Callers: scripts/git-hooks/pre-push (every pushed ref, with --local-env) and the
``secret-scan`` job in .github/workflows/ci.yml (push and PR ranges). Tests:
backend/tests/test_secret_scan.py. Docs: docs/RELEASING.md "Secret scan".

Stdlib only and Python 3.9 compatible, like scripts/release_guard.py: git hooks
run whatever ``python3`` is on PATH, and macOS's /usr/bin/python3 is 3.9.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import os
import re
import subprocess
import sys
import threading
from typing import Dict, Iterable, Iterator, List, NamedTuple, Optional, Sequence, Set, Tuple

EXIT_OK = 0
EXIT_BLOCKED = 1
EXIT_USAGE = 2

ALLOWLIST_PATH = "scripts/secret_scan_allowlist.txt"
BINARY_SNIFF = 8000
MAX_BLOB_BYTES = 100 * 1024 * 1024  # GitHub refuses larger files anyway
MAX_PRINTED = 60


class ScanError(Exception):
    """A git command failed, an object is unreadable or an argument is unusable; exit 2."""


# --- rules ----------------------------------------------------------------------


class Rule(NamedTuple):
    name: str
    needles: Tuple[bytes, ...]  # cheap substring prefilter (lower-case when fold=True)
    regex: "re.Pattern[bytes]"
    group: int  # 0: the whole match is the value
    kind: str  # key | private_key | db_url | generic | env_assign
    fold: bool = False
    name_group: int = 0  # generic/env_assign: the group holding the assigned name


# Names that hold a secret when a quoted literal is assigned to them. Bounded
# prefixes ({0,40}) keep the scan linear on long identifier-like runs.
_GENERIC_WORDS = (
    rb"(?:api[_\-]?key|api[_\-]?secret|secret[_\-]?key|client[_\-]?secret|access[_\-]?key|access[_\-]?token"
    rb"|auth[_\-]?token|refresh[_\-]?token|bearer[_\-]?token|private[_\-]?key|signing[_\-]?key"
    rb"|encryption[_\-]?key|webhook[_\-]?secret|passphrase|password|passwd|pwd|secret|token)"
)
_ENV_WORDS = (
    rb"(?:secret|token|password|passwd|api[_\-]?key|apikey|private[_\-]?key|access[_\-]?key|signing[_\-]?key)"
)

RULES = (
    Rule("google_api_key", (b"AIza",), re.compile(rb"AIza[0-9A-Za-z_\-]{35}"), 0, "key"),
    Rule("google_oauth_client_secret", (b"GOCSPX-",), re.compile(rb"GOCSPX-[0-9A-Za-z_\-]{20,}"), 0, "key"),
    Rule("anthropic_api_key", (b"sk-ant-",), re.compile(rb"sk-ant-[A-Za-z0-9_\-]{20,}"), 0, "key"),
    Rule("openai_api_key", (b"sk-",),
         re.compile(rb"(?<![A-Za-z0-9_\-])sk-(?!ant-)(?:proj-|svcacct-|admin-)?[A-Za-z0-9_\-]{32,}"), 0, "key"),
    Rule("stripe_secret_key", (b"k_live_", b"k_test_"),
         re.compile(rb"(?<![A-Za-z0-9])[sr]k_(?:live|test)_[0-9A-Za-z]{16,}"), 0, "key"),
    Rule("stripe_webhook_secret", (b"whsec_",), re.compile(rb"(?<![A-Za-z0-9])whsec_[A-Za-z0-9]{24,}"), 0, "key"),
    Rule("aws_access_key_id", (b"AKIA", b"ASIA"),
         re.compile(rb"(?<![A-Z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}(?![A-Z0-9])"), 0, "key"),
    Rule("github_token", (b"ghp_", b"gho_", b"ghu_", b"ghs_", b"ghr_", b"github_pat_"),
         re.compile(rb"(?<![A-Za-z0-9])(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})"), 0, "key"),
    Rule("slack_token", (b"xox",), re.compile(rb"(?<![A-Za-z0-9])xox[abprse]-[0-9A-Za-z\-]{10,}"), 0, "key"),
    Rule("slack_webhook", (b"hooks.slack.com/services/",),
         re.compile(rb"hooks\.slack\.com/services/[A-Za-z0-9_/\-]{20,}"), 0, "key"),
    Rule("sendgrid_api_key", (b"SG.",), re.compile(rb"SG\.[A-Za-z0-9_\-]{22}\.[A-Za-z0-9_\-]{43}"), 0, "key"),
    Rule("twilio_api_key", (b"SK",), re.compile(rb"(?<![A-Za-z0-9])SK[0-9a-f]{32}(?![A-Za-z0-9])"), 0, "key"),
    Rule("resend_api_key", (b"re_",),
         re.compile(rb"(?<![A-Za-z0-9])re_[A-Za-z0-9]{8,}_[A-Za-z0-9]{16,}(?![A-Za-z0-9])"), 0, "key"),
    Rule("huggingface_token", (b"hf_",), re.compile(rb"(?<![A-Za-z0-9])hf_[A-Za-z0-9]{34,}(?![A-Za-z0-9])"), 0, "key"),
    Rule("npm_token", (b"npm_",), re.compile(rb"(?<![A-Za-z0-9])npm_[A-Za-z0-9]{36}(?![A-Za-z0-9])"), 0, "key"),
    Rule("pypi_token", (b"pypi-",), re.compile(rb"pypi-AgEIcHlwaS5vcmc[A-Za-z0-9_\-]{50,}"), 0, "key"),
    Rule("postman_api_key", (b"PMAK-",), re.compile(rb"PMAK-[0-9a-f]{24}-[0-9a-f]{34}"), 0, "key"),
    Rule("railway_token", (b"railway",),
         re.compile(rb"(?i)railway[_\-]?(?:api[_\-]?|project[_\-]?)?token[\"']?\s*[:=]\s*[\"']?"
                    rb"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"), 1, "key", True),
    Rule("algolia_api_key", (b"algolia",),
         re.compile(rb"(?i)x-algolia-api-key[\"']?\s*[=:,]\s*[\"']?([a-f0-9]{32})"), 1, "key", True),
    Rule("typesense_api_key", (b"typesense",),
         re.compile(rb"(?i)x-typesense-api-key[\"']?\s*[=:,]\s*[\"']?([A-Za-z0-9]{20,})"), 1, "key", True),
    Rule("private_key", (b"PRIVATE KEY",), re.compile(rb"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY(?: BLOCK)?-----"),
         0, "private_key"),
    Rule("jwt", (b"eyJ",),
         re.compile(rb"(?<![A-Za-z0-9_\-])eyJ[A-Za-z0-9_\-]{10,}\.eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"),
         0, "key"),
    Rule("bcrypt_hash", (b"$2",), re.compile(rb"\$2[abxy]?\$\d{2}\$[./A-Za-z0-9]{53}"), 0, "key"),
    Rule("password_hash", (b"$argon2", b"pbkdf2:", b"scrypt:"),
         re.compile(rb"\$argon2(?:id|i|d)\$v=\d+\$m=\d+,t=\d+,p=\d+\$[A-Za-z0-9+/]{11,}\$[A-Za-z0-9+/]{16,}"
                    rb"|pbkdf2:sha(?:1|256|512):\d+\$[A-Za-z0-9]{8,}\$[0-9a-f]{40,}"
                    rb"|scrypt:\d+:\d+:\d+\$[A-Za-z0-9]{8,}\$[0-9a-f]{64,}"), 0, "key"),
    Rule("db_url_password", (b"://",),
         re.compile(rb"(?i)(?<![A-Za-z0-9+])(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|mssql|sqlserver"
                    rb"|redis|rediss|amqps?|clickhouse)(?:\+[a-z0-9]+)?://([^:/\s@'\"<>]{1,256})"
                    rb":([^@/\s'\"<>]{1,256})@([^\s'\"<>/:?#,;]{1,256})"),
         0, "db_url"),
    # name = "value" / "name": "value" / name == 'value' (code, JSON, YAML, TOML)
    Rule("generic_secret_assignment", (b"key", b"secret", b"token", b"passw", b"pwd", b"passphrase"),
         re.compile(rb"(?i)(?<![A-Za-z0-9])([A-Za-z0-9_.\-]{0,40}" + _GENERIC_WORDS
                    + rb")[\"']?\s*(?:==|=|:|:=|=>)\s*[rbuf]?[\"']([^\"'\s]{12,})[\"']"), 2, "generic", True, 1),
    # NAME=value / name: value without quotes (.env, shell, Dockerfile ENV, compose,
    # YAML/INI/TOML). Never applied to code files (see filter_hits), where an
    # unquoted right-hand side is an expression; outside config files only
    # UPPER_CASE names count, so prose like "Token: see below" stays quiet.
    Rule("generic_secret_env", (b"secret", b"token", b"passw", b"api_key", b"api-key", b"apikey", b"private_key",
                                b"access_key", b"signing_key"),
         re.compile(rb"(?im)^[ \t]*(?:-[ \t]+)?[\"']?(?:(?:export|env|arg)[ \t]+)?"
                    rb"([A-Za-z0-9_.\-]{0,60}" + _ENV_WORDS + rb"[A-Za-z0-9_.\-]{0,60})[ \t]*[=:][ \t]*"
                    rb"([^\s\"'#$`{<\[|>(][^\s\"'#`]{11,})"),
         2, "env_assign", True, 1),
)
# A NAME ending like this points at a secret (a file, a path), it does not hold one.
_REFERENCE_NAME = re.compile(
    rb"(?i)[_.\-](?:FILE|PATH|DIR|NAME|ID|HEADER|URL|ENV|VAR|TTL|EXPIRY|EXPIRES|LENGTH|TYPE|SOURCE|MODE|REF|COUNT)$"
)
_CODE_FILE = re.compile(
    r"\.(?:py|pyi|pyw|js|mjs|cjs|jsx|ts|tsx|swift|kt|kts|java|go|rb|php|rs|c|h|cc|cpp|hpp|m|mm|cs|scala|dart"
    r"|lua|vue|svelte)$", re.I
)
_CONFIG_FILE = re.compile(
    r"\.(?:ya?ml|ini|cfg|conf|toml|properties|tfvars|env)$|(?:^|/)\.env[^/]*$|(?:^|/)(?:Dockerfile[^/]*|Procfile"
    r"|\.npmrc|\.pypirc|\.netrc|docker-compose[^/]*)$", re.I
)

# Path rules: (name, regex, why). Matched against every path a scan covers.
PATH_RULES = (
    ("path:sqlite", re.compile(r"\.(?:db|sqlite|sqlite3|db3|s3db|sl3)(?:-wal|-shm|-journal)?$", re.I),
     "SQLite database; the early user databases put emails and bcrypt hashes into history"),
    ("path:workspace", re.compile(r"^workspace/"),
     "run artifact under workspace/ (ignored); one carried a Google key into 20 public commits"),
    ("path:har", re.compile(r"\.har$", re.I), "HTTP archive; it records cookies and Authorization headers"),
    ("path:dotenv", re.compile(r"(?:^|/)(?:\.env(?:rc)?(?:\.[^/]*)?|[^/]+\.env)$"),
     "env file; only templates (.env.example) belong in git"),
    ("path:key_file", re.compile(r"\.(?:pem|key|p8|p12|pfx|jks|keystore|ppk)$|(?:^|/)id_(?:rsa|dsa|ecdsa|ed25519)$",
                                 re.I),
     "private key or keystore file; content scanning cannot see inside binary keystores"),
    ("path:credentials_file", re.compile(
        r"(?:^|/)(?:\.netrc|_netrc|\.npmrc|\.pypirc|\.git-credentials|\.htpasswd|\.pgpass|credentials\.json"
        r"|service[_\-]?account[^/]*\.json|kubeconfig|\.dockercfg|\.docker/config\.json)$", re.I),
     "credential file; it holds tokens or passwords by design"),
    ("path:db_dump", re.compile(r"(?:^|/)dump-[^/]*\.sql(?:\.gz)?$|\.(?:dump|pgdump)$", re.I),
     "database dump; a copy of the tables, users included"),
    ("path:git_bundle", re.compile(r"\.bundle$", re.I), "git bundle; a copy of the history this scan guards"),
)
_DOTENV_TEMPLATE = re.compile(r"\.(?:example|sample|template|dist)$")
# workspace/ files kept in git on purpose. They skip the path rule only; their
# content is scanned like any other file. The same set N35 (P18A.3/P18A.4) names
# "tracked on purpose" in .gitignore:
#   D-HY4 (a), 2026-10-18: dealers_backup.json, until its 43,105 extra dealers
#     exist elsewhere (docs/remediation/OWNER_DECISIONS_LOG.md).
#   D-HY3 (a), 2026-10-18: the two curated dealer-log lessons (P18A.4).
#   Phase workflow checkpoints: workspace/phase_runs/<phase>/RESUME.md
#     (docs/NEXT_PHASES_PLAN_2026_10.md, "Checkpoints").
WORKSPACE_KEPT = re.compile(
    r"^workspace/(?:dealers_backup\.json|phase_runs/[^/]+/RESUME\.md"
    r"|dealer_logs/_learning/(?:platform_playbook|location_patterns)\.md)$"
)

# --- placeholders -----------------------------------------------------------------

# A provider-key-shaped value is let through only with an unmistakable marker: a
# real key never spells "example" or runs one character six times. The short
# markers are case-sensitive, so a long random key cannot spell one by chance.
_KEY_PLACEHOLDER = re.compile(
    rb"(?i:example|placeholder|redacted|dummy|sample|not[_\-]?a[_\-]?real|your[_\-])"
    rb"|xxxx|XXXX|fake|Fake|FAKE|\.\.\.|<[^>]*>|(.)\1{5}"
)
# Generic values and database passwords: template spellings (substrings of 4+
# characters or structural markers, which a generated secret does not contain).
_VALUE_PLACEHOLDER = re.compile(
    rb"(?i)your|xxxx|example|change[_\-]?me|replace[_\-]?(?:me|this)|dummy|placeholder|sample|redacted|lorem"
    rb"|foobar|not[_\-]?a[_\-]?real|secret[_\-]?here|key[_\-]?here|pytest|insecure|do[_\-]?not[_\-]?use"
    rb"|not[_\-]?for[_\-]?prod|test[_\-]?(?:key|token|secret|password)"
    rb"|<[^>]*>|\$\{|\{\{|%\(|%s|\*\*\*|0{8}|1234567890|abcdef|(.)\1{5}"
)
# Whole words (runs of letters) that make a value a label: it names its own kind
# ("long-enough-password", "reset-token-abc") or calls itself a test value. A
# generated secret is not made of such words.
_LABEL_WORDS = frozenset((
    b"password", b"passwd", b"passphrase", b"secret", b"secrets", b"token", b"tokens", b"key", b"keys",
    b"apikey", b"credential", b"credentials", b"test", b"testing", b"fake", b"wrong", b"invalid", b"expired",
    b"todo", b"tbd", b"dummy", b"sample", b"example", b"placeholder", b"changeme",
))
_ENV_REFERENCE = re.compile(rb"^[A-Z][A-Z0-9_]{3,}$")  # names an env var, holds no value
_INTERPOLATION = re.compile(rb"^(?:\$|%|\{|\[|<|\(|@)")
_DB_PASSWORD_DEFAULTS = frozenset((
    b"postgres", b"password", b"passwd", b"pass", b"pw", b"pwd", b"secret", b"test", b"testing", b"dev", b"local",
    b"admin", b"root", b"user", b"guest", b"changeme", b"example", b"sample", b"dummy", b"fake", b"docker",
    b"ci", b"mysecretpassword", b"mypassword", b"hunter2",
))
_DOC_HOST = re.compile(rb"(?i)(?:^|\.)(?:example\.(?:com|net|org)|example|invalid|test)$")


def key_placeholder(value: bytes) -> bool:
    return bool(_KEY_PLACEHOLDER.search(value))


def label_like(value: bytes) -> bool:
    """True when one of the value's words names its own kind or calls it a test value."""
    return any(word.lower() in _LABEL_WORDS for word in re.findall(rb"[A-Za-z]+", value))


def template_value(value: bytes) -> bool:
    """A template spelling, an interpolation, an env var name, a path or a credential-free URL."""
    if _VALUE_PLACEHOLDER.search(value) or _ENV_REFERENCE.match(value) or _INTERPOLATION.match(value):
        return True
    low = value.lower()
    if low.startswith((b"http://", b"https://", b"file://")) and b"@" not in low:
        return True  # a URL without credentials is a reference, not a secret
    return value.startswith((b"/", b"./", b"../", b"~/"))  # a path


def value_placeholder(value: bytes) -> bool:
    return template_value(value) or label_like(value)


def db_url_placeholder(user: bytes, password: bytes, host: bytes) -> bool:
    low = password.lower()
    if low in _DB_PASSWORD_DEFAULTS or low == user.lower() or len(password) <= 3:
        return True
    if _DOC_HOST.search(host):
        return True
    return value_placeholder(password)


_PEM_BODY = re.compile(rb"[A-Za-z0-9+/=]")


def private_key_stub(block: bytes) -> bool:
    """A PEM block that cannot be a key: a body under 40 base64 characters
    (fixtures write "MIGT") or one holding "...", "<" or ">", which base64 never
    does. The shortest real private key, an Ed25519 PKCS#8 one, has a
    64-character body. Nothing else counts: real bodies run "AAAAAA" and may spell
    any short word, so the provider-key markers do not apply here.
    """
    lines = block.split(b"\n")
    body = b"".join(line for line in lines[1:] if not line.strip().startswith(b"-----"))
    if b"..." in body or b"<" in body or b">" in body:
        return True
    return len(_PEM_BODY.findall(body)) < 40


# --- masking ------------------------------------------------------------------------

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _short(text: str) -> str:
    if len(text) >= 20:
        return f"{text[:6]}...{text[-3:]}"
    if len(text) >= 12:
        return f"{text[:3]}..."
    return f"{text[:1]}..."


def mask_emails(text: str) -> str:
    """Every email address in ``text`` cut to ``ab...@d...``."""
    def cut(m: "re.Match[str]") -> str:
        local, _, domain = m.group(0).partition("@")
        return f"{local[:2]}...@{domain[:1]}..."
    return _EMAIL.sub(cut, text)


def mask(rule: str, value: bytes) -> str:
    """A form of ``value`` safe to print: never the whole value."""
    text = value.decode("utf-8", "replace")
    if rule == "private_key":
        first = text.split("\n", 1)[0]
        return f"{first} ({len(value)} bytes)"
    if rule == "db_url_password":
        m = re.match(r"([A-Za-z0-9+]+)://([^:]*):[^@]*@(.*)", text, re.S)
        if m:
            return f"{m.group(1)}://{m.group(2)[:2]}...:***@{m.group(3)[:3]}..."
        return "***"
    if rule.startswith("env:"):
        return f"<{len(value)} characters>"
    if "@" in text:  # never a whole email address
        local, _, domain = text.partition("@")
        return f"{local[:2]}...@{domain[:1]}..."
    return _short(text)


def fingerprint(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


# --- git ------------------------------------------------------------------------------


def git(*args: str, input_bytes: Optional[bytes] = None) -> bytes:
    proc = subprocess.run(["git", *args], input=input_bytes, capture_output=True)
    if proc.returncode != 0:
        msg = proc.stderr.decode("utf-8", "replace").strip()
        raise ScanError(f"git {' '.join(args[:3])} failed: {msg}")
    return proc.stdout


def is_zero(sha: str) -> bool:
    """git's "no such ref" (40 zeros, or 64 in a SHA-256 repository)."""
    return bool(sha) and set(sha) == {"0"}


def resolve_commit(ref: str) -> str:
    proc = subprocess.run(["git", "rev-parse", "--verify", "--quiet", ref + "^{commit}"], capture_output=True)
    sha = proc.stdout.decode().strip()
    if proc.returncode != 0 or not sha:
        raise ScanError(f"cannot resolve {ref!r} to a commit in this repository; fetch it first")
    return sha


def read_blob_at(spec: str) -> Optional[bytes]:
    """The blob at ``spec`` (``REV:path`` or ``:path``), or None when there is none."""
    proc = subprocess.run(["git", "cat-file", "blob", spec], capture_output=True)
    return proc.stdout if proc.returncode == 0 else None


class Entry(NamedTuple):
    path: str
    blob: str
    commit: str  # "" outside --range


def _decode_path(raw: bytes) -> str:
    return raw.decode("utf-8", "surrogateescape")


def parse_diff_raw(out: bytes, commit_headers: bool) -> List[Entry]:
    """Entries a ``git diff-tree``/``diff-index`` ``-z --raw`` output adds or changes.

    With ``commit_headers`` (``diff-tree --stdin``) a bare sha token starts each
    commit. A record is ``:<modes> <shas> <status>`` followed by the path; a
    combined (merge) record starts with one colon per parent and its last mode
    and sha are the merge result. Deletions and gitlinks are skipped.
    """
    tokens = out.split(b"\0")
    entries = []  # type: List[Entry]
    commit = ""
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        i += 1
        if not tok:
            continue
        if not tok.startswith(b":"):
            if commit_headers:
                commit = tok.decode().strip()
            continue
        if i >= len(tokens):
            raise ScanError(f"truncated diff record: {tok[:80]!r}")
        path = tokens[i]
        i += 1
        parents = len(tok) - len(tok.lstrip(b":"))
        fields = tok[parents:].split()
        modes = fields[: parents + 1]
        shas = fields[parents + 1: 2 * parents + 2]
        if len(modes) != parents + 1 or len(shas) != parents + 1:
            raise ScanError(f"unexpected diff record: {tok[:80]!r}")
        mode, sha = modes[-1].decode(), shas[-1].decode()
        if is_zero(sha) or mode in ("000000", "160000"):
            continue
        entries.append(Entry(_decode_path(path), sha, commit))
    return entries


def range_commits(old: str, new: str) -> Tuple[List[str], str]:
    new_sha = resolve_commit(new)
    if not old or is_zero(old):
        out = git("rev-list", new_sha, "--not", "--remotes")
        label = f"{new_sha[:9]} (commits on no remote-tracking ref)"
    else:
        old_sha = resolve_commit(old)
        out = git("rev-list", f"{old_sha}..{new_sha}")
        label = f"{old_sha[:9]}..{new_sha[:9]}"
    return [c for c in out.decode().split() if c], label


def range_entries(commits: Sequence[str]) -> List[Entry]:
    if not commits:
        return []
    out = git("diff-tree", "--stdin", "-r", "-c", "--root", "--raw", "-z", "--no-renames", "--no-ext-diff",
              input_bytes=("\n".join(commits) + "\n").encode())
    return parse_diff_raw(out, commit_headers=True)


def staged_entries() -> List[Entry]:
    has_head = subprocess.run(["git", "rev-parse", "--verify", "--quiet", "HEAD^{commit}"],
                              capture_output=True).returncode == 0
    if has_head:
        base = "HEAD"
    else:  # an unborn branch: compare with the empty tree (sha-1 or sha-256 alike)
        base = git("hash-object", "-t", "tree", "--stdin", input_bytes=b"").decode().strip()
    out = git("diff-index", "--cached", "-r", "--raw", "-z", "--no-renames", base)
    return parse_diff_raw(out, commit_headers=False)


def tree_entries(rev: str) -> List[Entry]:
    sha = resolve_commit(rev)
    out = git("ls-tree", "-r", "-z", "--full-tree", sha)
    entries = []  # type: List[Entry]
    for rec in out.split(b"\0"):
        if not rec:
            continue
        meta, _, path = rec.partition(b"\t")
        parts = meta.split()
        if len(parts) == 3 and parts[1] == b"blob":
            entries.append(Entry(_decode_path(path), parts[2].decode(), ""))
    return entries


def read_blobs(shas: Iterable[str]) -> Iterator[Tuple[str, Optional[bytes]]]:
    """(sha, content) for each blob via one ``git cat-file --batch``.

    Content is None for a blob over MAX_BLOB_BYTES (read and dropped in chunks).
    A blob git reports missing raises ScanError: unread is never clean.
    """
    wanted = list(dict.fromkeys(shas))
    if not wanted:
        return
    proc = subprocess.Popen(["git", "cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    assert proc.stdin is not None and proc.stdout is not None
    stdin = proc.stdin

    def feed() -> None:
        try:
            for sha in wanted:
                stdin.write(sha.encode() + b"\n")
        except BrokenPipeError:
            pass
        finally:
            try:
                stdin.close()
            except BrokenPipeError:
                pass

    writer = threading.Thread(target=feed, daemon=True)
    writer.start()
    out = proc.stdout
    try:
        for _ in wanted:
            header = out.readline()
            parts = header.split()
            if len(parts) == 2 and parts[1] == b"missing":
                raise ScanError(f"blob {parts[0].decode()} is missing from this clone; fetch it first")
            if len(parts) != 3:
                raise ScanError(f"git cat-file --batch: unexpected header {header[:80]!r}")
            size = int(parts[2])
            if size > MAX_BLOB_BYTES:
                left = size
                while left > 0:  # drop it in chunks instead of holding it
                    chunk = out.read(min(left, 1 << 20))
                    if not chunk:
                        raise ScanError("git cat-file --batch: output ended inside a blob")
                    left -= len(chunk)
                out.read(1)
                yield parts[0].decode(), None
                continue
            data = out.read(size)
            if len(data) != size:
                raise ScanError("git cat-file --batch: output ended inside a blob")
            out.read(1)
            yield parts[0].decode(), data
    finally:
        out.close()
        proc.kill()
        proc.wait()
        writer.join()


# --- env values -------------------------------------------------------------------------

_ENV_SECRET_KEY = re.compile(
    r"(?i)secret|token|passw|pwd|api_?key|private_?key|access_?key|signing_?key|encryption_?key|credential|dsn"
    r"|salt|pepper"
)


def load_env_values(paths: Sequence[str]) -> List[Tuple[str, bytes]]:
    """(key, value) pairs whose key names a secret (or whose value is a URL with a password).

    Values under 12 characters, template spellings (the .env.example defaults),
    plain numbers, paths, URLs without credentials and database URLs whose
    password is a default are skipped: they show up in tracked files on their
    own and would block pushes for nothing. The label-word rule is not applied:
    a value in the clone's own env file is real by definition.
    """
    values = []  # type: List[Tuple[str, bytes]]
    for path in paths:
        if not os.path.isfile(path):
            continue
        with open(path, "rb") as fh:
            lines = fh.read().splitlines()
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith(b"#") or b"=" not in line:
                continue
            key, _, val = line.partition(b"=")
            key = key.strip()
            if key.startswith(b"export "):
                key = key[7:].strip()
            val = val.strip()
            if len(val) >= 2 and val[:1] == val[-1:] and val[:1] in (b"'", b'"'):
                val = val[1:-1]
            elif b" #" in val:  # an inline comment after an unquoted value
                val = val.split(b" #", 1)[0].rstrip()
            name = key.decode("utf-8", "replace")
            url_with_password = b"://" in val and b"@" in val.split(b"://", 1)[1].split(b"/", 1)[0]
            if not (_ENV_SECRET_KEY.search(name) or url_with_password):
                continue
            if len(val) < 12 or val.isdigit() or template_value(val):
                continue
            db = _ENV_DB_URL.match(val)
            if db and db_url_placeholder(db.group(1), db.group(2), db.group(3)):
                continue  # postgres:postgres@localhost and friends are in tracked files too
            values.append((name, val))
    return values


_ENV_DB_URL = re.compile(rb"[A-Za-z0-9+]+://([^:/\s@]+):([^@/\s]+)@([^\s/:?#]+)")


def local_env_files() -> List[str]:
    """This clone's own env files: ``.env`` and ``.env.*`` (templates excepted) at the
    top of the checkout and of the main worktree (a linked worktree has no .env)."""
    roots = [git("rev-parse", "--show-toplevel").decode().strip()]
    try:
        common = git("rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()
        if os.path.basename(common) == ".git":
            roots.append(os.path.dirname(common))
    except ScanError:
        pass
    found = []  # type: List[str]
    for root in dict.fromkeys(roots):
        try:
            names = sorted(os.listdir(root))
        except OSError:
            continue
        for name in names:
            if (name == ".env" or name.startswith(".env.")) and not _DOTENV_TEMPLATE.search(name):
                full = os.path.join(root, name)
                if os.path.isfile(full):
                    found.append(full)
    return found


# --- scanning -----------------------------------------------------------------------------


class Hit(NamedTuple):
    rule: str
    value: bytes
    line: int
    name: bytes = b""  # the assigned name, for generic/env_assign


class _Lines:
    """Line numbers for byte offsets, computed once per blob and only when needed."""

    def __init__(self, data: bytes):
        self.data = data
        self.starts = None  # type: Optional[List[int]]

    def of(self, offset: int) -> int:
        if self.starts is None:
            self.starts = [m.end() for m in re.finditer(rb"\n", self.data)]
        return bisect.bisect_right(self.starts, offset) + 1


def _private_key_block(data: bytes, start: int, end: int) -> bytes:
    """The whole PEM block (BEGIN through END) so the digest names this key, not the header."""
    stop = data.find(b"-----END", end, end + 20000)
    if stop == -1:
        return data[start:end + 400]
    close = data.find(b"-----", stop + 8)
    return data[start:(close + 5) if close != -1 else stop + 8]


def scan_content(data: bytes, env_values: Sequence[Tuple[str, bytes]] = ()) -> List[Hit]:
    """Content hits of one blob, whatever its path (filter_hits applies the path)."""
    hits = []  # type: List[Hit]
    lines = _Lines(data)
    lowered = None  # type: Optional[bytes]
    for rule in RULES:
        if rule.fold:
            if lowered is None:
                lowered = data.lower()
            if not any(n in lowered for n in rule.needles):
                continue
        elif not any(n in data for n in rule.needles):
            continue
        for m in rule.regex.finditer(data):
            value = m.group(rule.group)
            name = m.group(rule.name_group) if rule.name_group else b""
            if rule.kind == "key" and key_placeholder(value):
                continue
            if rule.kind in ("generic", "env_assign") and value_placeholder(value):
                continue
            if rule.kind == "env_assign" and _REFERENCE_NAME.search(name):
                continue
            if rule.kind == "db_url" and db_url_placeholder(m.group(1), m.group(2), m.group(3)):
                continue
            if rule.kind == "private_key":
                value = _private_key_block(data, m.start(), m.end())
                if private_key_stub(value):
                    continue
            hits.append(Hit(rule.name, value, lines.of(m.start(rule.group)), name))
    for key, val in env_values:
        pos = data.find(val)
        if pos != -1:
            hits.append(Hit("env:" + key, val, lines.of(pos)))
    return hits


def filter_hits(path: str, hits: Sequence[Hit]) -> List[Hit]:
    """Drop the unquoted-assignment hits a file of this kind cannot hold."""
    code = bool(_CODE_FILE.search(path))
    config = bool(_CONFIG_FILE.search(path))
    kept = []  # type: List[Hit]
    for hit in hits:
        if hit.rule == "generic_secret_env":
            if code:
                continue
            if not config and hit.name != hit.name.upper():
                continue
        kept.append(hit)
    return kept


def path_hits(path: str) -> List[Tuple[str, str]]:
    found = []  # type: List[Tuple[str, str]]
    for name, regex, why in PATH_RULES:
        if regex.search(path):
            if name == "path:dotenv" and _DOTENV_TEMPLATE.search(path):
                continue
            if name == "path:workspace" and WORKSPACE_KEPT.match(path):
                continue
            found.append((name, why))
    return found


class Finding(NamedTuple):
    path: str
    rule: str
    shown: str  # masked value or the path rule's reason
    digest: str
    lines: Tuple[int, ...]
    commit: str


# --- allowlist ------------------------------------------------------------------------------

_ALLOW_LINE = re.compile(r"^(\S+)\s+(sha256:[0-9a-f]{64})\s+(\S.*?)\s*$")


def parse_allowlist(text: str, source: str) -> Set[Tuple[str, str, str]]:
    """{(rule, digest, path)}; a malformed line is a usage error, never silently ignored."""
    entries = set()  # type: Set[Tuple[str, str, str]]
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ALLOW_LINE.match(line)
        if not m:
            raise ScanError(f"{source}:{n}: expected '<rule>  sha256:<64 hex>  <path>', got {line[:60]!r}")
        entries.add((m.group(1), m.group(2), m.group(3)))
    return entries


def load_allowlist(explicit: Optional[str], spec: Optional[str]) -> Set[Tuple[str, str, str]]:
    if explicit:
        try:
            with open(explicit, encoding="utf-8") as fh:
                return parse_allowlist(fh.read(), explicit)
        except OSError as exc:
            raise ScanError(f"cannot read the allowlist {explicit}: {exc}")
    if spec is None:
        return set()
    data = read_blob_at(spec)
    if data is None:
        return set()
    return parse_allowlist(data.decode("utf-8", "replace"), spec)


# --- driver -----------------------------------------------------------------------------------


def scan(entries: Sequence[Entry], allow: Set[Tuple[str, str, str]],
         env_values: Sequence[Tuple[str, bytes]] = ()) -> Tuple[List[Finding], Dict[str, int]]:
    by_blob = {}  # type: Dict[str, List[Hit]]
    path_digest = {}  # type: Dict[str, str]  blob -> digest of its bytes, for path findings
    stats = {"blobs": 0, "binary": 0, "large": 0, "allowlisted": 0}
    flagged_paths = {e.blob for e in entries if path_hits(e.path)}
    for sha, data in read_blobs(e.blob for e in entries):
        stats["blobs"] += 1
        if data is None:
            stats["large"] += 1
            by_blob[sha] = []
            continue
        if sha in flagged_paths:
            path_digest[sha] = fingerprint(data)
        if b"\0" in data[:BINARY_SNIFF]:
            stats["binary"] += 1
            by_blob[sha] = []
            continue
        by_blob[sha] = scan_content(data, env_values)

    findings = {}  # type: Dict[Tuple[str, str, str], Finding]
    for e in entries:
        for name, why in path_hits(e.path):
            digest = path_digest.get(e.blob, "sha256:unread")
            if (name, digest, e.path) in allow:
                stats["allowlisted"] += 1
                continue
            key = (e.path, name, digest)
            if key not in findings:
                findings[key] = Finding(e.path, name, why, digest, (), e.commit)
        for hit in filter_hits(e.path, by_blob.get(e.blob, ())):
            digest = fingerprint(hit.value)
            if not hit.rule.startswith("env:") and (hit.rule, digest, e.path) in allow:
                stats["allowlisted"] += 1
                continue
            key = (e.path, hit.rule, digest)
            prior = findings.get(key)
            lines = (prior.lines if prior else ()) + ((hit.line,) if not prior or hit.line not in prior.lines else ())
            findings[key] = Finding(e.path, hit.rule, mask(hit.rule, hit.value), digest, lines,
                                    prior.commit if prior else e.commit)
    return sorted(findings.values(), key=lambda f: (f.path, f.lines[:1], f.rule)), stats


def _format(f: Finding) -> str:
    where = mask_emails(f.path) + (":" + ",".join(str(n) for n in f.lines[:5]) if f.lines else "")
    if len(f.lines) > 5:
        where += ",..."
    commit = f"  [commit {f.commit[:9]}]" if f.commit else ""
    return f"  {where}  {f.rule}  {f.shown}{commit}"


def in_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("CI", "").lower() in ("1", "true")


def main(argv: Optional[List[str]] = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass
    parser = argparse.ArgumentParser(
        prog="secret_scan.py",
        description="Refuse secrets, SQLite files, run artifacts and env files before they are published.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--range", dest="rng", metavar="OLD..NEW",
                      help="blobs added by the commits in OLD..NEW (OLD all zeros: commits on no remote ref)")
    mode.add_argument("--staged", action="store_true", help="blobs the next commit would add")
    mode.add_argument("--all", dest="whole", action="store_true", help="every file in the tree at --rev")
    parser.add_argument("--rev", default="HEAD", help="tree for --all (default HEAD)")
    parser.add_argument("--allowlist", help=f"allowlist file on disk (default: {ALLOWLIST_PATH} in the judged commit)")
    parser.add_argument("--env-file", action="append", default=[], help="KEY=VALUE file whose secret values block")
    parser.add_argument("--local-env", action="store_true",
                        help="also block the secret values of this clone's own .env files")
    parser.add_argument("--print-allowlist", action="store_true",
                        help="print allowlist lines for the findings (local only; refused under CI)")
    args = parser.parse_args(argv)

    if args.print_allowlist and in_ci():
        print("secret_scan: --print-allowlist prints digests and is refused under CI.", file=sys.stderr)
        return EXIT_USAGE
    try:
        env_files = list(args.env_file)
        for path in env_files:
            if not os.path.isfile(path):
                raise ScanError(f"--env-file {path}: no such file")
        if args.local_env:
            env_files += local_env_files()
        env_values = load_env_values(env_files)
        if args.rng is not None:
            if ".." not in args.rng or "..." in args.rng:
                raise ScanError(f"--range wants OLD..NEW, got {args.rng!r}")
            old, new = args.rng.split("..", 1)
            new = new.strip() or "HEAD"
            commits, label = range_commits(old.strip(), new)
            entries = range_entries(commits)
            allow = load_allowlist(args.allowlist, f"{resolve_commit(new)}:{ALLOWLIST_PATH}")
            scope = f"range {label}: {len(commits)} commit{'s' if len(commits) != 1 else ''}"
            again = f"--range {args.rng}"
        elif args.staged:
            entries = staged_entries()
            allow = load_allowlist(args.allowlist, f":{ALLOWLIST_PATH}")
            scope = "staged changes"
            again = "--staged"
        else:
            entries = tree_entries(args.rev)
            allow = load_allowlist(args.allowlist, f"{resolve_commit(args.rev)}:{ALLOWLIST_PATH}")
            scope = f"tree {args.rev}"
            again = f"--all --rev {args.rev}"
        findings, stats = scan(entries, allow, env_values)
    except ScanError as exc:
        print(f"secret_scan: {exc}", file=sys.stderr)
        return EXIT_USAGE

    summary = (f"{scope}, {stats['blobs']} blob{'s' if stats['blobs'] != 1 else ''} "
               f"({stats['binary']} binary and {stats['large']} over 100 MB, judged by path), "
               f"{stats['allowlisted']} allowlisted")
    if env_files:
        summary += f", {len(env_values)} env value{'s' if len(env_values) != 1 else ''} checked"
    if not findings:
        print(f"secret_scan: OK: {summary}; nothing to block.")
        return EXIT_OK

    if args.print_allowlist:
        print(f"# {len(findings)} finding(s) in {scope}. Add only benign fixtures, never a real secret.")
        for f in findings:
            print(f"# {mask_emails(f.path)}: {f.rule} {f.shown}")
            if f.rule.startswith("env:"):
                print("#   a value from your own env file: never allowlisted; take it out of the commit.")
                continue
            print(f"{f.rule}  {f.digest}  {f.path}")
        return EXIT_BLOCKED

    err = sys.stderr
    print(f"secret_scan: BLOCKED: {len(findings)} finding{'s' if len(findings) != 1 else ''} in {summary}.", file=err)
    for f in findings[:MAX_PRINTED]:
        print(_format(f), file=err)
    if len(findings) > MAX_PRINTED:
        print(f"  ... and {len(findings) - MAX_PRINTED} more", file=err)
    print(
        "secret_scan: a real secret: take it out of every listed commit and rotate it (it is in your history).\n"
        "  A run artifact or local database: git rm --cached <path>, then rewrite the unpushed commits.\n"
        "  A benign fixture only: python3 scripts/secret_scan.py " + again + " --print-allowlist\n"
        f"  prints lines for {ALLOWLIST_PATH}; review each one before adding it.\n"
        "  git push --no-verify is the only bypass of the pre-push hook (it skips every pre-push check);\n"
        "  CI's secret-scan job still scans the pushed range.",
        file=err,
    )
    return EXIT_BLOCKED


if __name__ == "__main__":
    sys.exit(main())
