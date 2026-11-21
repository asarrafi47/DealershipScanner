#!/usr/bin/env python3
"""Client for the scan loop API (backend/routes/loop_api.py). Standard library only.

  python scripts/cloud/loop_client.py pull --out DIR      download the loop's files (logs, mirror, requests, proposals,
                                                          fix_cycles, config, findings and the newest run bundle)
  python scripts/cloud/loop_client.py push [--root DIR]   send a session's new requests, proposals, findings and its
                                                          summary.md / location.md notes found under DIR (default cwd)

Environment: LOOP_API_URL (the web service's https base URL), LOOP_READ_TOKEN (pull), LOOP_SUBMIT_TOKEN (push).
A token is sent only in the Authorization header to LOOP_API_URL and is never printed. Redirects are refused, so a
token can not follow a redirect to another host. ``push`` treats "already exists" as done (requests and proposals are
write-once). Exit: 0 done, 1 a transfer failed or a file was refused, 2 usage or setup.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PULL_PREFIXES = ("workspace/dealer_logs/", "mirror/", "requests/", "proposals/", "fix_cycles/", "config/", "findings/")
PUSH_GLOBS = ("requests/rq-*.json", "proposals/*/pr-*.json", "findings/*.md",
              "workspace/dealer_logs/*/summary.md", "workspace/dealer_logs/*/location.md")
_PATH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}(/[A-Za-z0-9][A-Za-z0-9._-]{0,99}){0,5}$")
MAX_EXTRACT_BYTES = 128 * 1024 * 1024


class ClientError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):  # noqa: D401
        return None


def _base() -> str:
    url = (os.environ.get("LOOP_API_URL") or "").strip().rstrip("/")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" and parsed.hostname not in ("localhost", "127.0.0.1"):
        raise ClientError("LOOP_API_URL must be an https URL")
    if not parsed.hostname:
        raise ClientError("LOOP_API_URL is not set")
    return url


def _call(method: str, route: str, token_env: str, body=None, timeout=60):
    token = (os.environ.get(token_env) or "").strip()
    if not token:
        raise ClientError(token_env + " is not set")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(_base() + route, data=data, method=method)
    request.add_header("Authorization", "Bearer " + token)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except (urllib.error.URLError, OSError) as exc:
        raise ClientError("cannot reach the API: " + type(exc).__name__) from None


def _extract(blob: bytes, out: Path) -> int:
    count = total = 0
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for member in tar:
            if not member.isfile() or not _PATH_RE.fullmatch(member.name):
                continue
            total += member.size
            if total > MAX_EXTRACT_BYTES:
                raise ClientError("export is too large")
            target = (out / member.name).resolve()
            if out.resolve() not in target.parents:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(tar.extractfile(member).read())
            count += 1
    return count


def pull(out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
    query = "&".join("prefix=" + urllib.parse.quote(p, safe="") for p in PULL_PREFIXES)
    status, blob = _call("GET", "/api/loop/export?" + query, "LOOP_READ_TOKEN", timeout=180)
    if status != 200:
        raise ClientError("export failed: HTTP " + str(status))
    files = _extract(blob, out)
    status, latest = _call("GET", "/api/loop/file?path=runs/LATEST.json", "LOOP_READ_TOKEN")
    if status == 200:
        (out / "runs").mkdir(exist_ok=True)
        (out / "runs" / "LATEST.json").write_bytes(latest)
        try:
            run_id = json.loads(latest)["run_id"]
        except (ValueError, KeyError, TypeError):
            run_id = ""
        if re.fullmatch(r"fleet_[0-9]{8}T[0-9]{6}Z", str(run_id)):
            status, blob = _call("GET", "/api/loop/export?prefix=" + urllib.parse.quote("runs/" + run_id + "/", safe=""),
                                 "LOOP_READ_TOKEN", timeout=180)
            if status == 200:
                files += _extract(blob, out)
    print("loop_client: pulled " + str(files) + " files into " + str(out))
    return 0


def push(root: Path) -> int:
    failed = sent = 0
    for pattern in PUSH_GLOBS:
        for file in sorted(root.glob(pattern)):
            if not file.is_file() or file.is_symlink() or file.name.endswith(".status.json"):
                continue
            rel = file.relative_to(root).as_posix()
            try:
                text = file.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                print("loop_client: unreadable " + rel, file=sys.stderr)
                failed += 1
                continue
            status, blob = _call("POST", "/api/loop/submit", "LOOP_SUBMIT_TOKEN", {"path": rel, "text": text})
            if status == 200 or status == 409:
                sent += 1
            else:
                reason = ""
                try:
                    reason = str(json.loads(blob).get("error", ""))[:120]
                except (ValueError, AttributeError):
                    pass
                print("loop_client: " + rel + " refused: HTTP " + str(status) + " " + reason, file=sys.stderr)
                failed += 1
    print("loop_client: " + str(sent) + " files sent or already there, " + str(failed) + " failed")
    return 1 if failed else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    pull_p = sub.add_parser("pull")
    pull_p.add_argument("--out", required=True)
    push_p = sub.add_parser("push")
    push_p.add_argument("--root", default=".")
    args = parser.parse_args(argv)
    try:
        return pull(Path(args.out)) if args.cmd == "pull" else push(Path(args.root))
    except ClientError as exc:
        print("loop_client: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
