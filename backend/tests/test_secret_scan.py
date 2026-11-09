"""Secret scan: scripts/secret_scan.py, its pre-push hook wiring and the CI job.

Everything that touches git runs against throwaway repositories under
``tmp_path`` (a work clone with copies of the real scripts, plus a bare
"origin" over ``file://``), with an empty global/system git config, the same
way backend/tests/test_release_tooling.py drives the hook. No network.

Fake secrets are built at runtime from sha256 output, never written in this
file: the file itself is scanned by every push. Names ending in the SQLite
suffixes are built from parts because backend/tests/test_no_sqlite.py scans
string literals.

Covered:

- the rules: provider keys, private keys (and stubs), JWTs, password hashes,
  DB URLs, generic and env-style assignments, and the placeholder heuristics;
- masking: no printed line ever holds a whole value;
- path rules: SQLite files (binary too), workspace/ (and the kept files), env
  files and templates;
- the modes: --range (explicit, all-zero OLD, add-then-delete, merges),
  --staged, --all with the allowlist read from the judged commit, a missing
  blob and an unknown ref (exit 2), --print-allowlist (refused under CI),
  --local-env;
- the hook: a clean push passes, a dirty one is refused (branch, tag, main),
  deletions pass, an unfetched remote sha falls back, a missing scanner
  refuses, --no-verify is the only bypass;
- the CI ``secret-scan`` job: pins, and its run step replayed on simulated
  push, new-branch, force-push, PR and dispatch checkouts;
- this repo: the allowlist is well formed and current, tracked workspace/
  files are only the kept ones, the 92694 fixture moved with its consumers,
  and the scanner is stdlib-only Python 3.9 source.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import os
import shutil
import string
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCAN = REPO_ROOT / "scripts" / "secret_scan.py"
HOOK = REPO_ROOT / "scripts" / "git-hooks" / "pre-push"
GUARD = REPO_ROOT / "scripts" / "release_guard.py"
ALLOWLIST = REPO_ROOT / "scripts" / "secret_scan_allowlist.txt"
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"

ZERO = "0" * 40
ALNUM = string.ascii_letters + string.digits
SQLITE_SUFFIX = "." + "db"
SQLITE3_SUFFIX = "." + "sqlite" + "3"


def _load_module():
    spec = importlib.util.spec_from_file_location("secret_scan_under_test", SCAN)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


ss = _load_module()


# --- fake values ------------------------------------------------------------------


def rand(seed: str, n: int, alphabet: str = ALNUM) -> str:
    """Deterministic random-looking text: the same seed gives the same value."""
    out = []  # type: list
    counter = 0
    while len(out) < n:
        digest = hashlib.sha256(f"{seed}:{counter}".encode()).digest()
        out.extend(alphabet[b % len(alphabet)] for b in digest)
        counter += 1
    return "".join(out[:n])


def google_key(seed: str = "google") -> str:
    key = "AIza" + rand(seed, 35, ALNUM + "_-")
    assert not ss.key_placeholder(key.encode())
    return key


def secret_value(seed: str = "generic", n: int = 32) -> str:
    value = rand(seed, n)
    assert not ss.value_placeholder(value.encode())
    return value


def pem(body_chars: int, seed: str = "pem") -> str:
    body = rand(seed, body_chars, ALNUM + "+/")
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    begin = "-----BEGIN " + "PRIVATE KEY-----"
    end = "-----END " + "PRIVATE KEY-----"
    return "\n".join([begin, *lines, end]) + "\n"


def content_rules(text: str, path: str = "notes.txt") -> list:
    return [h.rule for h in ss.filter_hits(path, ss.scan_content(text.encode()))]


# --- git scratch repos ------------------------------------------------------------


class Repo:
    def __init__(self, path: Path, env: dict):
        self.path = path
        self.env = env

    def run(self, *argv: str, check: bool = True, env: dict | None = None) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            list(argv), cwd=self.path, env=env or self.env, capture_output=True, text=True, timeout=120
        )
        if check and proc.returncode != 0:
            raise AssertionError(f"{argv} exited {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
        return proc

    def git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return self.run("git", *args, check=check)

    def sha(self, rev: str = "HEAD") -> str:
        return self.git("rev-parse", rev).stdout.strip()

    def write(self, rel: str, data) -> None:
        target = self.path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, bytes):
            target.write_bytes(data)
        else:
            target.write_text(data, encoding="utf-8")

    def commit(self, message: str, files: dict, remove: tuple = ()) -> str:
        for rel, data in files.items():
            self.write(rel, data)
        for rel in remove:
            self.git("rm", "-q", "--", rel)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)
        return self.sha()

    def scan(self, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
        return self.run(sys.executable, str(SCAN), *args, check=False, env=env)

    def push(self, *args: str) -> subprocess.CompletedProcess:
        return self.git("push", "origin", *args, check=False)


def out_of(proc: subprocess.CompletedProcess) -> str:
    return proc.stdout + proc.stderr


@pytest.fixture
def git_env(tmp_path: Path) -> dict:
    home = tmp_path / "home"
    home.mkdir()
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim = shim_dir / "python3"
    shim.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n', encoding="utf-8")
    shim.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_") and k not in ("CI", "GITHUB_ACTIONS")}
    env.update(
        HOME=str(home),
        PATH=f"{shim_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_TERMINAL_PROMPT="0",
        LC_ALL="C",
        LANG="C",
    )
    return env


def _identity(repo: Repo) -> None:
    repo.git("config", "user.name", "Secret Scan Test")
    repo.git("config", "user.email", "secret-scan@example.invalid")
    repo.git("config", "commit.gpgsign", "false")
    repo.git("config", "tag.gpgsign", "false")


CHANGELOG = "# Changelog\n\n## [Unreleased]\n\n## [1.0.0] - 2026-11-01\n\n- First release.\n"


def init_repo(path: Path, env: dict) -> Repo:
    """A repo shaped like the real one: VERSION, CHANGELOG.md, the hook and both scripts."""
    path.mkdir(parents=True)
    repo = Repo(path, env)
    repo.git("init", "-q", "-b", "main")
    _identity(repo)
    (path / "scripts" / "git-hooks").mkdir(parents=True)
    shutil.copy2(HOOK, path / "scripts" / "git-hooks" / "pre-push")
    shutil.copy2(GUARD, path / "scripts" / "release_guard.py")
    shutil.copy2(SCAN, path / "scripts" / "secret_scan.py")
    repo.commit("initial", {"VERSION": "1.0.0\n", "CHANGELOG.md": CHANGELOG, "app.py": "x = 0\n",
                            ".gitignore": ".env\n"})
    return repo


def init_bare(path: Path, env: dict) -> Path:
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(path)], env=env, check=True,
                   capture_output=True)
    return path


@pytest.fixture
def repo(tmp_path: Path, git_env: dict) -> Repo:
    return init_repo(tmp_path / "work", git_env)


@pytest.fixture
def pushed(tmp_path: Path, git_env: dict) -> Repo:
    """Work clone at 1.0.0, main pushed to a bare origin, then the hook installed."""
    remote = init_bare(tmp_path / "origin.git", git_env)
    work = init_repo(tmp_path / "work", git_env)
    work.git("remote", "add", "origin", remote.as_uri())
    work.git("push", "-q", "origin", "main")
    work.git("config", "core.hooksPath", "scripts/git-hooks")
    return work


def remote_sha(repo: Repo, ref: str) -> str:
    out = repo.git("ls-remote", "origin", ref).stdout.split()
    return out[0] if out else ""


def allow_line(rule: str, value, path: str) -> str:
    raw = value if isinstance(value, bytes) else value.encode()
    return f"{rule}  sha256:{hashlib.sha256(raw).hexdigest()}  {path}\n"


# --- rules (no git) -------------------------------------------------------------------


def test_provider_keys_are_found_and_placeholders_let_through():
    key = google_key()
    assert content_rules(f'MAPS_URL = "https://maps.googleapis.com/maps/api/js?key={key}"\n', "app.js") == [
        "google_api_key"
    ]
    for placeholder in ("AIza" + "X" * 35, "AIzaSyEXAMPLE" + rand("p", 22), "AIzaYOUR_" + rand("q", 26)):
        assert content_rules(f"key: {placeholder}\n", "README.md") == [], placeholder
    stripe = "sk_" + "live_" + rand("stripe", 24)
    anthropic = "sk-" + "ant-api03-" + rand("anthropic", 60, ALNUM + "_-")
    github = "ghp_" + rand("github", 36)
    aws = "AKIA" + rand("aws", 16, string.ascii_uppercase + string.digits)
    for value, rule in ((stripe, "stripe_secret_key"), (anthropic, "anthropic_api_key"),
                        (github, "github_token"), (aws, "aws_access_key_id")):
        assert rule in content_rules(f"see {value} here\n"), rule


def test_private_keys_are_found_and_stubs_let_through():
    assert content_rules(pem(1600)) == ["private_key"]
    # An Ed25519 PKCS#8 body is 64 characters, the shortest real one.
    assert content_rules(pem(64)) == ["private_key"]
    # Real bodies run zero bytes ("AAAAAA"): not a placeholder marker for keys.
    openssh = pem(400).replace(pem(400).split("\n")[1][:12], "AAAAAAAAAAAB", 1)
    assert content_rules(openssh) == ["private_key"]
    begin, end = "-----BEGIN " + "PRIVATE KEY-----", "-----END " + "PRIVATE KEY-----"
    assert content_rules(f"{begin}\nMIGT\n{end}\n") == []
    assert content_rules(f"{begin}\nMIGTAgEAMBMG...\n{end}\n") == []
    assert content_rules(f"{begin}\n<your key here>\n{end}\n") == []
    assert content_rules(f'if text.startswith("{begin}"):\n') == []  # a mention, no body


def test_a_private_key_inside_json_is_found_and_its_mask_is_the_header_only():
    # Service-account files carry the PEM with literal \n escapes, all on one line.
    escaped = pem(1600, "sa").replace("\n", "\\n")
    doc = '{"type": "service_account", "private_key": "' + escaped + '", "client_id": "1"}\n'
    hits = ss.scan_content(doc.encode())
    assert [h.rule for h in hits] == ["private_key"]
    shown = ss.mask("private_key", hits[0].value)
    assert shown.startswith("-----BEGIN " + "PRIVATE KEY----- (") and shown.endswith(" bytes)")
    body = pem(1600, "sa").split("\n")[1]
    assert body[:20] not in shown


def test_an_all_caps_base32_seed_is_not_taken_for_an_env_var_name():
    seed = rand("totp", 32, string.ascii_uppercase + "234567")
    assert content_rules(f'TOTP_SECRET = "{seed}"\n', "backend/mfa.py") == ["generic_secret_assignment"]
    assert content_rules('secret = "TOTP_SEED_FROM_ENV"\n', "backend/mfa.py") == []


def test_jwts_and_password_hashes_are_found():
    jwt = "eyJ" + rand("h", 30, ALNUM + "_-") + ".eyJ" + rand("p", 40, ALNUM + "_-") + "." + rand("s", 43, ALNUM + "_-")
    assert content_rules(f"Authorization: Bearer {jwt}\n") == ["jwt"]
    bcrypt = "$2b$12$" + rand("bcrypt", 53, ALNUM + "./")
    assert content_rules(f'{{"password_hash": "{bcrypt}"}}\n', "users.json") == ["bcrypt_hash"]
    werkzeug = "scrypt:32768:8:1$" + rand("salt", 16) + "$" + rand("hash", 128, "0123456789abcdef")
    assert content_rules(f"{werkzeug}\n") == ["password_hash"]


def test_db_urls_need_a_real_password():
    real = rand("dbpw", 18)
    assert content_rules(f"DATABASE_URL=postgresql://app:{real}@db.internal:5432/cars\n", "notes.txt") == [
        "db_url_password"
    ]
    for dsn in (
        "postgresql://postgres:postgres@localhost:5432/postgres",  # a default
        "postgresql://cars:cars@localhost/cars",  # the user name
        "postgresql://u:p@localhost/db",  # 3 characters or fewer
        "postgresql://app:${PGPASSWORD}@db.internal/cars",  # interpolation
        "postgresql://app:" + real + "@db.example.com/cars",  # a documentation host
        "postgresql://app:local-secret-pw@localhost/cars",  # it names itself
        "postgresql://localhost:15432/cars",  # no password at all
    ):
        assert content_rules(f"url = {dsn}\n", "notes.txt") == [], dsn


def test_generic_assignments_block_real_values_and_pass_labels():
    value = secret_value()
    assert content_rules(f'api_key = "{value}"\n', "client.py") == ["generic_secret_assignment"]
    assert content_rules(f'{{"client_secret": "{value}"}}\n', "config.json") == ["generic_secret_assignment"]
    assert content_rules(f'if token == "{value}":\n', "auth.py") == ["generic_secret_assignment"]
    for label in ("long-enough-password", "reset-token-abc", "test-api-key-123", "DealershipScanner/stripe-secret-key",
                  "change-me-in-production", "${API_KEY}", "SECRET_KEY_FROM_ENV", "<your-token>",
                  "pytest-only-not-a-real-value", "https://api.example.invalid/v1/token"):
        assert content_rules(f'password = "{label}"\n', "client.py") == [], label


def test_env_style_assignments_follow_the_file_kind():
    value = secret_value("env")
    # Names that start with the secret word count too (the draft missed these).
    for line in (f"SECRET_KEY={value}", f"PASSWORD={value}", f"export API_TOKEN={value}",
                 f"ENV STRIPE_SECRET_KEY={value}", f"  - GITHUB_TOKEN={value}"):
        assert content_rules(line + "\n", "deploy/run.sh") == ["generic_secret_env"], line
    # Lowercase names count in config files only.
    assert content_rules(f"password: {value}\n", "k8s/secret.yaml") == ["generic_secret_env"]
    assert content_rules(f"Token: {value}\n", "docs/guide.md") == []
    # Code files: an unquoted right-hand side is an expression.
    assert content_rules(f"STOCK_TOKEN_RE = {value}\n", "backend/x.py") == []
    # References and placeholders.
    assert content_rules("VAULT_TOKEN_FILE: /run/secrets/vault_token\n", "deploy/docker-compose.yml") == []
    assert content_rules("FLASK_SECRET_KEY=local-dev-secret-change-in-production\n", ".env.example") == []
    assert content_rules("API_TOKEN=${API_TOKEN}\n", ".env.example") == []


def test_mask_never_prints_a_whole_value():
    for n in (1, 5, 11, 12, 19, 20, 32, 80):
        value = rand(f"mask{n}", n).encode()
        for rule in ("google_api_key", "generic_secret_assignment", "env:API_TOKEN"):
            shown = ss.mask(rule, value)
            assert value.decode() not in shown, (rule, n, shown)
    url = ("postgresql://app:" + rand("pw", 16) + "@db.internal:5432/cars").encode()
    shown = ss.mask("db_url_password", url)
    assert rand("pw", 16) not in shown and "***" in shown
    assert ss.mask("generic_secret_assignment", b"someone.important@example.org") == "so...@e..."
    assert ss.mask_emails("workspace/jane.doe@example.org/notes.md") == "workspace/ja...@e.../notes.md"


def test_path_rules():
    blocked = {
        "users" + SQLITE_SUFFIX: "path:sqlite",
        "backend/data/cache" + SQLITE3_SUFFIX: "path:sqlite",
        "inventory" + SQLITE_SUFFIX + "-wal": "path:sqlite",
        "workspace/manifest_x.json": "path:workspace",
        "workspace/phase_runs/N9/unit.json": "path:workspace",
        "debug/session.har": "path:har",
        ".env": "path:dotenv",
        "backend/.env.local": "path:dotenv",
        "deploy/prod.env": "path:dotenv",
        "certs/server.pem": "path:key_file",
        "home/.ssh/id_ed25519": "path:key_file",
        ".npmrc": "path:credentials_file",
        "dump-cars-202611010000.sql": "path:db_dump",
        "backups/repo.bundle": "path:git_bundle",
    }
    for path, rule in blocked.items():
        assert [name for name, _ in ss.path_hits(path)] == [rule], path
    for path in (".env.example", "deploy/.env.sample", "scripts/cleanroom/run.env.template",
                 "workspace/dealers_backup.json", "workspace/phase_runs/N6/RESUME.md",
                 "workspace/dealer_logs/_learning/platform_playbook.md", "backend/app.py", "docs/workspace/x.md"):
        assert ss.path_hits(path) == [], path


def test_a_malformed_allowlist_line_is_a_usage_error():
    with pytest.raises(ss.ScanError):
        ss.parse_allowlist("generic_secret_assignment  backend/x.py\n", "allowlist")
    entries = ss.parse_allowlist("# comment\n\n" + allow_line("jwt", b"v", "a b/c.txt"), "allowlist")
    assert entries == {("jwt", "sha256:" + hashlib.sha256(b"v").hexdigest(), "a b/c.txt")}


# --- modes (git) ------------------------------------------------------------------------


def test_range_blocks_a_fake_google_key_and_masks_it(repo):
    base = repo.sha()
    key = google_key()
    repo.commit("maps", {"frontend/maps.js": f'const KEY = "{key}";\n'})
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 1, out_of(proc)
    assert "secret_scan: BLOCKED: 1 finding" in proc.stderr
    assert "frontend/maps.js:1  google_api_key  " + key[:6] + "..." + key[-3:] in proc.stderr
    assert key not in out_of(proc)
    assert "git push --no-verify is the only bypass" in proc.stderr


def test_a_placeholder_is_allowed(repo):
    base = repo.sha()
    repo.commit("docs", {"docs/setup.md": "GOOGLE_MAPS_API_KEY=AIza" + "X" * 35 + "\n",
                         ".env.example": "SECRET_KEY=change-me\nAPI_TOKEN=<your-token>\n"})
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 0, out_of(proc)
    assert "secret_scan: OK" in proc.stdout


def test_an_allowlisted_fixture_is_allowed_only_there_and_only_as_is(repo):
    base = repo.sha()
    fixture = secret_value("fixture")
    repo.commit("fixture", {
        "tests/test_login.py": f'PASSWORD = "{fixture}"\n',
        "scripts/secret_scan_allowlist.txt": allow_line("generic_secret_assignment", fixture, "tests/test_login.py"),
    })
    assert repo.scan("--range", f"{base}..HEAD").returncode == 0

    # The same value at another path is still blocked.
    repo.commit("copy", {"tests/test_other.py": f'PASSWORD = "{fixture}"\n'})
    other = repo.scan("--range", f"{base}..HEAD")
    assert other.returncode == 1 and "tests/test_other.py" in other.stderr
    assert "tests/test_login.py" not in other.stderr

    # A changed value at the allowlisted path is blocked too.
    repo.git("rm", "-q", "tests/test_other.py")
    repo.commit("drop copy", {})
    changed = secret_value("changed")
    mid = repo.sha()
    repo.commit("change", {"tests/test_login.py": f'PASSWORD = "{changed}"\n'})
    proc = repo.scan("--range", f"{mid}..HEAD")
    assert proc.returncode == 1 and changed not in out_of(proc)


def test_a_sqlite_file_and_a_workspace_file_are_blocked(repo):
    base = repo.sha()
    repo.commit("artifacts", {
        "users" + SQLITE_SUFFIX: b"SQLite format 3\x00" + bytes(range(256)) * 8,  # binary: judged by path
        "workspace/scan_results.json": '{"ok": true}\n',
    })
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 1
    assert "users" + SQLITE_SUFFIX + "  path:sqlite" in proc.stderr
    assert "workspace/scan_results.json  path:workspace" in proc.stderr
    assert "1 binary" in proc.stderr


def test_kept_workspace_files_pass_the_path_rule_but_not_the_content_rules(repo):
    base = repo.sha()
    repo.commit("kept", {"workspace/dealers_backup.json": '[{"dealer_id": "a"}]\n',
                         "workspace/phase_runs/N9/RESUME.md": "# N9 RESUME\n"})
    assert repo.scan("--range", f"{base}..HEAD").returncode == 0
    key = google_key("kept")
    repo.commit("leak", {"workspace/phase_runs/N9/RESUME.md": f"# N9 RESUME\nkey {key}\n"})
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 1 and "google_api_key" in proc.stderr and key not in out_of(proc)


def test_env_files_are_blocked_and_templates_are_not(repo):
    base = repo.sha()
    # An emptied .gitignore lets `git add -A` take the .env.
    repo.commit("env", {".gitignore": "", ".env": "DEBUG=1\n", ".env.example": "DEBUG=0\n"})
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 1
    assert "BLOCKED: 1 finding in" in proc.stderr
    assert "\n  .env  path:dotenv" in proc.stderr and ".env.example  path:dotenv" not in proc.stderr


def test_all_zero_old_scans_only_commits_on_no_remote_ref(tmp_path, git_env, repo):
    remote = init_bare(tmp_path / "origin.git", git_env)
    repo.git("remote", "add", "origin", remote.as_uri())
    key = google_key("published")
    repo.commit("already public", {"old.js": f'k = "{key}"\n'})
    repo.git("push", "-q", "origin", "main")  # no hook installed: it is on the remote now
    repo.git("checkout", "-q", "-b", "feat")
    repo.commit("new work", {"new.py": "y = 1\n"})
    proc = repo.scan("--range", f"{ZERO}..HEAD")
    assert proc.returncode == 0, out_of(proc)
    assert "1 commit" in proc.stdout and "commits on no remote-tracking ref" in proc.stdout
    # Without any remote-tracking ref the whole history counts.
    repo.git("update-ref", "-d", "refs/remotes/origin/main")
    assert repo.scan("--range", f"{ZERO}..HEAD").returncode == 1


def test_a_value_added_and_removed_inside_the_range_still_blocks(repo):
    base = repo.sha()
    value = secret_value("transient")
    repo.commit("add", {"settings.py": f'SECRET_KEY = "{value}"\n'})
    repo.commit("remove", {"settings.py": 'SECRET_KEY = os.environ["SECRET_KEY"]\n'})
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 1
    assert "settings.py:1  generic_secret_assignment" in proc.stderr and "[commit " in proc.stderr
    assert value not in out_of(proc)


def test_merges_are_judged_by_what_they_add_themselves(repo):
    repo.git("checkout", "-q", "-b", "side")
    key = google_key("side")
    side = repo.commit("side", {"side.js": f'k = "{key}"\n'})
    repo.git("checkout", "-q", "main")
    main = repo.commit("main", {"main.py": "m = 1\n"})
    repo.git("merge", "-q", "--no-ff", "-m", "merge side", "side")

    # The side commit outside the range (judged already): a clean merge adds nothing.
    clean = repo.scan("--range", f"{side}..HEAD")  # the main commit and the merge
    assert clean.returncode == 0 and "2 commits" in clean.stdout, out_of(clean)
    # Inside the range, the side commit is judged through its own commit.
    both = repo.scan("--range", f"{main}..HEAD")  # the side commit and the merge
    assert both.returncode == 1 and "side.js:1  google_api_key" in both.stderr

    # Content in neither parent (an evil merge) is judged on the merge itself.
    repo.git("reset", "-q", "--hard", main)
    repo.git("merge", "-q", "--no-ff", "--no-commit", "side")
    evil = google_key("evil")
    repo.write("evil.py", f'K = "{evil}"\n')
    repo.git("add", "evil.py")
    repo.git("commit", "-q", "-m", "merge with evil content")
    proc = repo.scan("--range", f"{side}..HEAD")
    assert proc.returncode == 1 and "evil.py:1  google_api_key" in proc.stderr
    assert "side.js" not in proc.stderr
    assert key not in out_of(both) and evil not in out_of(proc)


def test_staged_and_all_modes(repo):
    value = secret_value("staged")
    repo.write("conf.json", f'{{"api_secret": "{value}"}}\n')
    repo.git("add", "conf.json")
    staged = repo.scan("--staged")
    assert staged.returncode == 1 and "conf.json:1  generic_secret_assignment" in staged.stderr
    repo.git("commit", "-q", "-m", "conf")
    whole = repo.scan("--all")
    assert whole.returncode == 1 and "conf.json" in whole.stderr and value not in out_of(whole)


def test_the_allowlist_is_read_from_the_judged_commit_not_the_working_tree(repo):
    base = repo.sha()
    value = secret_value("tree")
    repo.commit("conf", {"conf.json": f'{{"token": "{value}"}}\n'})
    repo.write("scripts/secret_scan_allowlist.txt", allow_line("generic_secret_assignment", value, "conf.json"))
    assert repo.scan("--range", f"{base}..HEAD").returncode == 1  # only in the working tree
    assert repo.scan("--all").returncode == 1
    explicit = repo.scan("--all", "--allowlist", "scripts/secret_scan_allowlist.txt")
    assert explicit.returncode == 0, out_of(explicit)
    # --staged reads the allowlist from the index.
    repo.write("conf.json", f'{{"token": "{value}", "n": 1}}\n')
    repo.git("add", "conf.json")
    assert repo.scan("--staged").returncode == 1
    repo.git("add", "scripts/secret_scan_allowlist.txt")
    assert repo.scan("--staged").returncode == 0
    repo.git("commit", "-q", "-m", "allowlist")
    assert repo.scan("--range", f"{base}..HEAD").returncode == 0
    assert repo.scan("--all").returncode == 0


def test_print_allowlist_lines_make_the_scan_pass_and_are_refused_under_ci(repo):
    base = repo.sha()
    value = secret_value("print")
    repo.commit("fixture", {"tests/test_x.py": f'token = "{value}"\n'})
    printed = repo.scan("--range", f"{base}..HEAD", "--print-allowlist")
    assert printed.returncode == 1 and value not in out_of(printed)
    lines = [ln for ln in printed.stdout.splitlines() if ln and not ln.startswith("#")]
    assert lines == [allow_line("generic_secret_assignment", value, "tests/test_x.py").rstrip("\n")]
    repo.commit("allowlist", {"scripts/secret_scan_allowlist.txt": lines[0] + "\n"})
    assert repo.scan("--range", f"{base}..HEAD").returncode == 0

    ci_env = {**repo.env, "GITHUB_ACTIONS": "true"}
    refused = repo.scan("--all", "--print-allowlist", env=ci_env)
    assert refused.returncode == 2 and "refused under CI" in refused.stderr


def test_a_missing_blob_and_an_unknown_ref_exit_2(repo):
    base = repo.sha()
    repo.commit("blob", {"data.txt": "a blob that will go missing\n"})
    blob = repo.git("rev-parse", "HEAD:data.txt").stdout.strip()
    loose = repo.path / ".git" / "objects" / blob[:2] / blob[2:]
    loose.chmod(0o644)
    loose.unlink()
    proc = repo.scan("--range", f"{base}..HEAD")
    assert proc.returncode == 2 and "missing" in proc.stderr
    assert repo.scan("--range", "1" * 40 + "..HEAD").returncode == 2
    assert repo.scan("--range", "HEAD").returncode == 2  # not OLD..NEW
    assert repo.scan("--all", "--rev", "no-such-branch").returncode == 2


def test_local_env_values_block_and_are_never_printed(repo):
    token = rand("envtoken", 28)
    repo.write(".env", f"API_TOKEN={token}\nDEBUG=true\nDATABASE_URL=postgresql://postgres:postgres@localhost/x\n")
    base = repo.sha()
    repo.commit("notes", {"docs/notes.md": f"the token we use is {token} for now\n"})
    plain = repo.scan("--range", f"{base}..HEAD")
    assert plain.returncode == 0  # no rule sees an unlabelled value in prose
    proc = repo.scan("--range", f"{base}..HEAD", "--local-env")
    assert proc.returncode == 1, out_of(proc)
    assert "docs/notes.md:1  env:API_TOKEN  <28 characters>" in proc.stderr
    assert token not in out_of(proc)
    printed = repo.scan("--range", f"{base}..HEAD", "--local-env", "--print-allowlist")
    assert "never allowlisted" in printed.stdout and "sha256:" not in printed.stdout


# --- the pre-push hook ------------------------------------------------------------------


def test_hook_passes_a_clean_push_and_blocks_a_dirty_one(pushed):
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    clean = repo.commit("clean work", {"app.py": "x = 1\n", "VERSION": "1.0.1\n"})
    ok = repo.push("feat")
    assert ok.returncode == 0, out_of(ok)
    assert "secret_scan: OK" in out_of(ok)
    assert remote_sha(repo, "refs/heads/feat") == clean

    key = google_key("hook")
    repo.commit("leak", {"config.py": f'MAPS_KEY = "{key}"\n', "VERSION": "1.0.2\n"})
    refused = repo.push("feat")
    assert refused.returncode != 0
    out = out_of(refused)
    assert "secret_scan: BLOCKED" in out and "config.py:1  google_api_key" in out
    assert "pre-push: refused the push of refs/heads/feat to origin (secret scan above)." in out
    assert key not in out
    assert remote_sha(repo, "refs/heads/feat") == clean


def test_hook_blocks_dirty_tags_and_main_and_lets_deletions_through(pushed):
    repo = pushed
    main_before = remote_sha(repo, "refs/heads/main")
    value = secret_value("hooktag")
    notes = CHANGELOG.replace("## [1.0.0]", "## [1.0.1] - 2026-11-02\n\n- Fixed.\n\n## [1.0.0]")
    repo.commit("release with a leak", {"settings.py": f'SECRET_KEY = "{value}"\n', "VERSION": "1.0.1\n",
                                        "CHANGELOG.md": notes})
    repo.git("tag", "v1.0.1")
    tag = repo.push("refs/tags/v1.0.1")
    assert tag.returncode != 0 and "secret scan above" in out_of(tag)
    main = repo.push("main")
    assert main.returncode != 0 and "secret scan above" in out_of(main)
    assert "release_guard: OK" not in out_of(main)  # refused before the guard ran
    assert remote_sha(repo, "refs/heads/main") == main_before
    assert value not in out_of(tag) + out_of(main)

    assert repo.push("main:refs/heads/old").returncode != 0
    repo.git("push", "-q", "--no-verify", "origin", f"{main_before}:refs/heads/old")
    deletion = repo.push("--delete", "old")
    assert deletion.returncode == 0, out_of(deletion)


def test_hook_falls_back_when_the_remote_sha_is_not_fetched(pushed, tmp_path, git_env):
    repo = pushed
    other = Repo(tmp_path / "other", git_env)
    subprocess.run(["git", "clone", "-q", repo.git("remote", "get-url", "origin").stdout.strip(), str(other.path)],
                   env=git_env, check=True, capture_output=True)
    _identity(other)
    other.git("checkout", "-q", "-b", "feat")
    other.commit("theirs", {"theirs.py": "t = 1\n", "VERSION": "1.0.1\n"})
    other.git("push", "-q", "origin", "feat")  # no hook in this clone

    repo.git("checkout", "-q", "-b", "feat")
    repo.commit("ours", {"ours.py": "o = 1\n", "VERSION": "1.0.2\n"})
    proc = repo.push("--force", "feat")
    assert proc.returncode == 0, out_of(proc)
    assert "commits on no remote-tracking ref" in out_of(proc)


def test_hook_scans_values_of_the_clones_own_env_file(pushed):
    repo = pushed
    token = rand("hookenv", 30)
    repo.write(".env", f"STRIPE_WEBHOOK_TOKEN={token}\n")  # ignored by the repo's .gitignore
    repo.git("checkout", "-q", "-b", "feat")
    repo.commit("notes", {"docs/notes.md": f"use {token}\n", "VERSION": "1.0.1\n"})
    proc = repo.push("feat")
    assert proc.returncode != 0
    assert "env:STRIPE_WEBHOOK_TOKEN  <30 characters>" in out_of(proc)
    assert token not in out_of(proc)


def test_hook_refuses_without_the_scanner_and_no_verify_is_the_only_bypass(pushed):
    repo = pushed
    repo.git("checkout", "-q", "-b", "feat")
    key = google_key("bypass")
    dirty = repo.commit("leak", {"k.js": f'k = "{key}"\n', "VERSION": "1.0.1\n"})
    (repo.path / "scripts" / "secret_scan.py").unlink()
    missing = repo.push("feat")
    assert missing.returncode != 0 and "secret_scan.py is missing; refusing the push." in out_of(missing)
    repo.git("checkout", "-q", "--", "scripts/secret_scan.py")
    assert repo.push("feat").returncode != 0
    bypass = repo.git("push", "--no-verify", "origin", "feat", check=False)
    assert bypass.returncode == 0
    assert remote_sha(repo, "refs/heads/feat") == dirty


def test_hook_and_scanner_are_executable():
    assert os.access(HOOK, os.X_OK)
    assert os.access(SCAN, os.X_OK)


# --- the CI secret-scan job -----------------------------------------------------------


@pytest.fixture(scope="module")
def scan_job() -> dict:
    return yaml.safe_load(CI_YML.read_text(encoding="utf-8"))["jobs"]["secret-scan"]


def _run_steps(job: dict) -> list:
    return [s for s in job.get("steps") or [] if "run" in s]


def test_secret_scan_job_runs_on_every_event_with_full_history(scan_job):
    assert scan_job.get("name", "secret-scan") == "secret-scan"
    assert "if" not in scan_job  # a skipped job reports success
    assert "needs" not in scan_job
    assert "continue-on-error" not in scan_job and "permissions" not in scan_job
    assert scan_job["timeout-minutes"] == 10
    checkout = scan_job["steps"][0]
    assert (checkout.get("uses") or "").split("@")[0] == "actions/checkout"
    assert checkout["with"] == {"fetch-depth": 0, "filter": "blob:none"}
    runs = _run_steps(scan_job)
    assert len(runs) == 1
    assert runs[0]["env"] == {
        "EVENT_NAME": "${{ github.event_name }}",
        "BEFORE_SHA": "${{ github.event.before }}",
        "PR_BASE_SHA": "${{ github.event.pull_request.base.sha }}",
        "PR_HEAD_SHA": "${{ github.event.pull_request.head.sha }}",
    }
    script = runs[0]["run"]
    assert script.count("python3 scripts/secret_scan.py") == 4
    assert "--print-allowlist" not in script and "--local-env" not in script


def _replay(job: dict, workdir: Path, env: dict, **event: str) -> subprocess.CompletedProcess:
    """The job's run step the way the runner runs it (bash -eo pipefail)."""
    bash = shutil.which("bash")
    shell = [bash, "--noprofile", "--norc", "-eo", "pipefail", "-c"] if bash else ["sh", "-ec"]
    step_env = {**env, "EVENT_NAME": "", "BEFORE_SHA": "", "PR_BASE_SHA": "", "PR_HEAD_SHA": "", **event}
    (step,) = _run_steps(job)
    return subprocess.run([*shell, step["run"]], cwd=workdir, env=step_env, capture_output=True, text=True,
                          timeout=120)


def _ci_clone(tmp_path: Path, env: dict, remote: Path, sha: str, name: str) -> Path:
    """What a fetch-depth 0 checkout leaves: every branch under origin/*, the event's sha detached."""
    path = tmp_path / name
    subprocess.run(["git", "clone", "-q", "--no-checkout", remote.as_uri(), str(path)], env=env, check=True,
                   capture_output=True)
    subprocess.run(["git", "checkout", "-q", "--detach", sha], cwd=path, env=env, check=True, capture_output=True)
    return path


@pytest.fixture
def ci_origin(tmp_path, git_env):
    remote = init_bare(tmp_path / "origin.git", git_env)
    work = init_repo(tmp_path / "work", git_env)
    work.git("remote", "add", "origin", remote.as_uri())
    work.git("push", "-q", "origin", "main")
    return remote, work


@pytest.mark.parametrize("dirty", [False, True])
def test_job_on_a_push_scans_before_to_sha(tmp_path, git_env, scan_job, ci_origin, dirty):
    remote, work = ci_origin
    before = work.sha()
    key = google_key("cipush")
    work.commit("one", {"a.py": "a = 1\n"})
    work.commit("two", {"b.py": (f'K = "{key}"\n' if dirty else "b = 2\n")})
    work.commit("three", {"c.py": "c = 3\n"})
    work.git("push", "-q", "origin", "main")
    ci = _ci_clone(tmp_path, git_env, remote, work.sha(), "ci")
    proc = _replay(scan_job, ci, git_env, EVENT_NAME="push", BEFORE_SHA=before, GITHUB_SHA=work.sha())
    assert (proc.returncode == 0) is not dirty, out_of(proc)
    assert f"range {before[:9]}..{work.sha()[:9]}: 3 commits" in out_of(proc)
    assert key not in out_of(proc)


def test_job_on_a_new_branch_and_on_a_lost_force_push_scans_from_main(tmp_path, git_env, scan_job, ci_origin):
    remote, work = ci_origin
    work.git("checkout", "-q", "-b", "feat")
    value = secret_value("cibranch")
    work.commit("leak", {"conf.json": f'{{"api_key": "{value}"}}\n'})
    work.commit("more", {"d.py": "d = 4\n"})
    work.git("push", "-q", "origin", "feat")
    ci = _ci_clone(tmp_path, git_env, remote, work.sha(), "ci")
    for before in (ZERO, "1" * 40):  # a new branch; an old tip the remote no longer has
        proc = _replay(scan_job, ci, git_env, EVENT_NAME="push", BEFORE_SHA=before, GITHUB_SHA=work.sha())
        assert proc.returncode == 1, out_of(proc)
        assert "2 commits" in out_of(proc) and "conf.json:1  generic_secret_assignment" in out_of(proc)
        assert value not in out_of(proc)


@pytest.mark.parametrize("dirty", [False, True])
def test_job_on_a_pull_request_scans_base_to_head(tmp_path, git_env, scan_job, ci_origin, dirty):
    remote, work = ci_origin
    base = work.sha()
    work.git("checkout", "-q", "-b", "pr")
    files = {"workspace/run.json": "{}\n"} if dirty else {"e.py": "e = 5\n"}
    work.commit("pr work", files)
    work.git("push", "-q", "origin", "pr")
    head = work.sha()
    work.git("checkout", "-q", "main")
    work.commit("main moved", {"f.py": "f = 6\n"})
    work.git("push", "-q", "origin", "main")
    ci = _ci_clone(tmp_path, git_env, remote, head, "ci")
    proc = _replay(scan_job, ci, git_env, EVENT_NAME="pull_request", PR_BASE_SHA=base, PR_HEAD_SHA=head,
                   GITHUB_SHA=head)
    assert (proc.returncode == 0) is not dirty, out_of(proc)
    assert "1 commit," in out_of(proc)


def test_job_on_dispatch_scans_the_whole_tree(tmp_path, git_env, scan_job, ci_origin):
    remote, work = ci_origin
    ci = _ci_clone(tmp_path, git_env, remote, work.sha(), "ci")
    proc = _replay(scan_job, ci, git_env, EVENT_NAME="workflow_dispatch", GITHUB_SHA=work.sha())
    assert proc.returncode == 0, out_of(proc)
    assert "tree HEAD" in proc.stdout


# --- this repository -----------------------------------------------------------------------


def _tracked(*pathspec: str) -> list:
    out = subprocess.run(["git", "ls-files", "-z", "--", *pathspec], cwd=REPO_ROOT, capture_output=True,
                         check=True).stdout
    return [p for p in out.decode("utf-8", "surrogateescape").split("\0") if p]


def test_the_repo_allowlist_is_well_formed_and_every_entry_is_current():
    entries = ss.parse_allowlist(ALLOWLIST.read_text(encoding="utf-8"), str(ALLOWLIST))
    assert entries, "the allowlist is empty"
    by_path = {}  # type: dict
    for rule, digest, path in entries:
        by_path.setdefault(path, set()).add((rule, digest))
    for path, wanted in sorted(by_path.items()):
        data = (REPO_ROOT / path).read_bytes()
        found = {(name, ss.fingerprint(data)) for name, _ in ss.path_hits(path)}
        if b"\0" not in data[: ss.BINARY_SNIFF]:
            found |= {(h.rule, ss.fingerprint(h.value)) for h in ss.filter_hits(path, ss.scan_content(data))}
        assert wanted <= found, f"{path}: stale allowlist entries {sorted(wanted - found)}"
        assert found <= wanted, f"{path}: findings not allowlisted {sorted(r for r, _ in found - wanted)}"


def test_only_kept_or_allowlisted_workspace_files_are_tracked():
    # The same rule the hook applies to every push: a workspace/ file is a run
    # artifact unless WORKSPACE_KEPT names it or the allowlist lists its bytes.
    tracked = _tracked("workspace/")
    assert tracked, "D-HY4 keeps workspace/dealers_backup.json tracked"
    entries = ss.parse_allowlist(ALLOWLIST.read_text(encoding="utf-8"), str(ALLOWLIST))
    listed = {path for rule, _, path in entries if rule == "path:workspace"}
    assert [p for p in tracked if not ss.WORKSPACE_KEPT.match(p) and p not in listed] == []


def test_the_92694_fixture_moved_with_its_consumers():
    fixture = "backend/dev/fixtures/manifest_92694_25mi.json"
    assert (REPO_ROOT / fixture).is_file()
    for consumer in ("backend/dev/scan_lab.py", "backend/scanner/cli.py", "scripts/scan_irvinebmw_test.sh"):
        text = (REPO_ROOT / consumer).read_text(encoding="utf-8")
        assert fixture in text and "workspace/manifest_92694_25mi.json" not in text, consumer


def test_the_scanner_is_stdlib_only_and_python39_compatible():
    source = SCAN.read_text(encoding="utf-8")
    tree = ast.parse(source, feature_version=(3, 9))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            imported.add((node.module or "").split(".")[0])
    assert imported and imported <= set(sys.stdlib_module_names), imported - set(sys.stdlib_module_names)
    assert "from __future__ import annotations" in source


@pytest.mark.slow
def test_the_whole_tree_at_head_is_clean():
    # About 40 s over ~40k blobs, so it is in the slow tier; CI's secret-scan job
    # covers every pushed range.
    proc = subprocess.run([sys.executable, str(SCAN), "--all"], cwd=REPO_ROOT, capture_output=True, text=True,
                          timeout=600)
    assert proc.returncode == 0, proc.stdout + proc.stderr
