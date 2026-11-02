#!/usr/bin/env python3
"""Railway service settings: a read-only check against service_settings.json (N4.1, D-N24).

Railway stops reading config-as-code files (railway.toml / railway.json) on
2026-12-01 (https://docs.railway.com/config-as-code). Its Infrastructure as Code
DSL (https://docs.railway.com/infrastructure-as-code/reference) has no field for
the builder, the Dockerfile path, the restart policy, the cron schedule or
resource limits, so those settings live on the service instance. They are read
with the documented ``serviceInstance`` / ``serviceInstanceLimits`` queries and
written with ``serviceInstanceUpdate`` (https://docs.railway.com/integrations/api/manage-services).
The read goes through ``railway api`` (https://docs.railway.com/cli/api), so the
CLI's own login is used and no token is handled here.

Commands:
  check <service> [--settings-file PATH|-]
      Run ONE read-only GraphQL query through ``railway api`` and compare the
      live settings with the expected ones. Exit 0 = all equal, 1 = at least one
      differs, 2 = could not read or compare (fails closed). The deploy scripts
      call this before every deploy and dry run (deploy/railway/_guarded_deploy.sh).
  print-query <service> [--settings-file PATH|-]
      Print that read-only ``railway api`` command (for owner packs). Runs nothing.
  print-update <service> [--settings-file PATH|-] [--with-undocumented]
      Print the ``serviceInstanceUpdate`` command that would write the expected
      settings (for owner packs). Runs nothing: this module never sends a mutation.
      Fields the Manage Services guide does not list as update inputs
      (railwayConfigFile, builder) are left out unless --with-undocumented;
      limits are always left out (serviceInstanceLimitsUpdate is a separate,
      undocumented mutation). Left-out fields are named on stderr.

stdlib only; run it with ``python3 -I``.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

SETTINGS_FILE = Path(__file__).resolve().with_name("service_settings.json")

# Fields this check may read: the ServiceInstance fields the Manage Services guide
# documents (query or update input), plus railwayConfigFile and builder, which the
# P0A.1 / P0A.3 audits read live (docs/SCANNING_OPS_LOG.md). service_settings.json
# does not check builder: it reads RAILPACK on Dockerfile-built services (P0A.3).
READABLE_FIELDS = frozenset(
    {
        "builder",
        "buildCommand",
        "cronSchedule",
        "dockerfilePath",
        "healthcheckPath",
        "healthcheckTimeout",
        "numReplicas",
        "railwayConfigFile",
        "region",
        "restartPolicyMaxRetries",
        "restartPolicyType",
        "rootDirectory",
        "sleepApplication",
        "startCommand",
    }
)
# ServiceInstanceUpdateInput fields documented in the Manage Services guide.
DOCUMENTED_UPDATE_FIELDS = frozenset(
    {
        "buildCommand",
        "cronSchedule",
        "dockerfilePath",
        "healthcheckPath",
        "healthcheckTimeout",
        "numReplicas",
        "region",
        "restartPolicyMaxRetries",
        "restartPolicyType",
        "rootDirectory",
        "sleepApplication",
        "watchPatterns",
    }
)
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_DEPRECATION = re.compile(r"deprecat|config as code|config-as-code", re.IGNORECASE)
_MISSING = object()
RAILWAY_TIMEOUT_S = 90


class SettingsError(Exception):
    """The expected settings file is unusable, or the live settings cannot be read."""


# --- expected settings --------------------------------------------------------


def load_settings(path: str | None) -> dict:
    """The expected settings from *path* ("-" = stdin; default: next to this file)."""
    origin = path or str(SETTINGS_FILE)
    try:
        text = sys.stdin.read() if path == "-" else Path(origin).read_text(encoding="utf-8")
    except OSError as exc:
        raise SettingsError(f"cannot read the expected settings ({origin}): {exc}") from exc
    return parse_settings(text, origin)


def parse_settings(text: str, origin: str = "service_settings.json") -> dict:
    try:
        data = json.loads(text)
    except ValueError as exc:
        raise SettingsError(f"cannot read the expected settings ({origin}): {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("services"), dict):
        raise SettingsError("expected settings: no 'services' object")
    if not _UUID.match(str(data.get("environmentId", ""))):
        raise SettingsError("expected settings: environmentId is not a UUID")
    return data


def service_entry(settings: dict, service: str) -> dict:
    entry = settings["services"].get(service)
    if not isinstance(entry, dict):
        known = ", ".join(sorted(settings["services"])) or "none"
        raise SettingsError(
            f"no expected settings for service '{service}' in deploy/railway/service_settings.json "
            f"(known: {known}); add an entry with its serviceId before deploying to it"
        )
    if not _UUID.match(str(entry.get("serviceId", ""))):
        raise SettingsError(f"expected settings: services.{service}.serviceId is not a UUID")
    fields = entry.get("serviceInstance")
    if not isinstance(fields, dict) or not fields:
        raise SettingsError(f"expected settings: services.{service}.serviceInstance is empty")
    unknown = sorted(set(fields) - READABLE_FIELDS)
    if unknown:
        raise SettingsError(f"expected settings: services.{service} names unknown fields {unknown}")
    limits = entry.get("limits")
    if limits is not None and (
        not isinstance(limits, dict)
        or set(limits) != {"cpu", "memoryBytes"}
        or not all(isinstance(v, int) and v > 0 for v in limits.values())
    ):
        raise SettingsError(f"expected settings: services.{service}.limits must be {{cpu, memoryBytes}} integers")
    return entry


# --- the read-only query ------------------------------------------------------


def build_query(entry: dict) -> str:
    """One line, so owner packs can paste it as a one-line command."""
    fields = ["serviceName", *sorted(entry["serviceInstance"])]
    body = [f"serviceInstance(environmentId: $environmentId, serviceId: $serviceId) {{ {' '.join(fields)} }}"]
    if entry.get("limits") is not None:
        body.append("serviceInstanceLimits(environmentId: $environmentId, serviceId: $serviceId)")
    doc = "query N41ServiceSettings($environmentId: String!, $serviceId: String!) { " + " ".join(body) + " }"
    assert_read_only(doc)
    return doc


def assert_read_only(doc: str) -> None:
    """``railway api`` runs mutations without a prompt, so only a query may leave here."""
    if not doc.lstrip().startswith("query") or re.search(r"\bmutation\b|\bsubscription\b", doc, re.IGNORECASE):
        raise SettingsError("refusing to send a GraphQL document that is not a plain query")


def query_variables(settings: dict, entry: dict) -> dict:
    return {"environmentId": settings["environmentId"], "serviceId": entry["serviceId"]}


def railway_api_argv(doc: str, variables: dict) -> list[str]:
    return ["railway", "api", doc, "--variables", json.dumps(variables, sort_keys=True)]


def _parse_body(stdout: str) -> dict:
    text = stdout.strip()
    try:
        body = json.loads(text)
    except ValueError:
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            raise SettingsError("railway api printed no JSON") from None
        try:
            body = json.loads(text[start : end + 1])
        except ValueError as exc:
            raise SettingsError(f"railway api printed unparseable JSON: {exc}") from None
    if not isinstance(body, dict):
        raise SettingsError("railway api printed JSON that is not an object")
    return body


def run_query(argv: list[str]) -> tuple[dict, list[str]]:
    """Run the read-only query. Returns (data, notices): notices are deprecation lines."""
    assert_read_only(argv[2])
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=RAILWAY_TIMEOUT_S, check=False)
    except FileNotFoundError:
        raise SettingsError("the railway CLI is not on PATH") from None
    except subprocess.TimeoutExpired:
        raise SettingsError(f"railway api did not answer within {RAILWAY_TIMEOUT_S} s") from None
    notices = [
        line.strip()
        for line in (proc.stderr + "\n" + proc.stdout).splitlines()
        if _DEPRECATION.search(line) and not line.lstrip().startswith(("{", "}", '"'))
    ]
    if proc.returncode != 0:
        detail = (proc.stderr.strip() or proc.stdout.strip() or "no output").splitlines()[-1][:300]
        raise SettingsError(
            f"railway api exited {proc.returncode}: {detail} "
            "(not logged in? CLI without `railway api`? run `railway whoami` / `railway upgrade`)"
        )
    body = _parse_body(proc.stdout)
    if body.get("errors"):
        raise SettingsError(f"railway api returned GraphQL errors: {json.dumps(body['errors'])[:400]}")
    data = body.get("data", body)
    if not isinstance(data, dict) or not isinstance(data.get("serviceInstance"), dict):
        raise SettingsError("railway api returned no serviceInstance (wrong service or environment id?)")
    return data, notices


# --- comparison ---------------------------------------------------------------


def normalize_limits(raw) -> dict | None:
    """serviceInstanceLimits is a JSON scalar whose shape the docs do not give.

    Accepted shapes (anything else is unrecognised and fails the check):
      {"containers": {"cpu": N, "memoryBytes": M}}   (the old limitOverride shape)
      {"cpu": N, "memoryBytes": M}
      {"vCPUs": N, "memoryGB": G} (also under "containers"), memory = G * 2**30
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None
    if not isinstance(raw, dict):
        return None
    if set(raw) == {"containers"} and isinstance(raw["containers"], dict):
        raw = raw["containers"]

    def number(value):
        return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    if set(raw) == {"cpu", "memoryBytes"}:
        cpu, mem = number(raw["cpu"]), number(raw["memoryBytes"])
    elif set(raw) == {"vCPUs", "memoryGB"}:
        cpu, gb = number(raw["vCPUs"]), number(raw["memoryGB"])
        mem = None if gb is None else gb * 2**30
    else:
        return None
    if cpu is None or mem is None:
        return None
    return {"cpu": cpu, "memoryBytes": mem}


def compare(service: str, entry: dict, data: dict) -> list[tuple[str, object, object, bool]]:
    """Rows of (field, expected, actual, equal). Missing fields never compare equal."""
    live = data["serviceInstance"]
    rows = [("serviceName", service, live.get("serviceName", _MISSING), live.get("serviceName", _MISSING) == service)]
    for field in sorted(entry["serviceInstance"]):
        want = entry["serviceInstance"][field]
        got = live.get(field, _MISSING)
        equal = got is not _MISSING and type(got) is type(want) and got == want
        rows.append((field, want, got, equal))
    if entry.get("limits") is not None:
        raw = data.get("serviceInstanceLimits", _MISSING)
        got = _MISSING if raw is _MISSING else normalize_limits(raw)
        if raw is not _MISSING and got is None:
            got = f"unrecognised serviceInstanceLimits shape: {json.dumps(raw)[:200]}"
        rows.append(("limits", entry["limits"], got, isinstance(got, dict) and got == entry["limits"]))
    return rows


def _show(value) -> str:
    return "(not returned)" if value is _MISSING else json.dumps(value)


def report(service: str, settings: dict, entry: dict, rows, notices: list[str]) -> str:
    width = max(len(r[0]) for r in rows)
    lines = [f"settings  {service}: serviceId {entry['serviceId']}, environment {settings['environmentId']}"]
    for field, want, got, equal in rows:
        status = "ok" if equal else "MISMATCH"
        lines.append(f"  {field.ljust(width)}  expected {_show(want)}  live {_show(got)}  {status}")
    for notice in notices:
        lines.append(f"  warning   the railway CLI printed a deprecation notice: {notice}")
    return "\n".join(lines)


# --- commands -----------------------------------------------------------------


def cmd_check(args) -> int:
    try:
        settings = load_settings(args.settings_file)
        entry = service_entry(settings, args.service)
        argv = railway_api_argv(build_query(entry), query_variables(settings, entry))
        data, notices = run_query(argv)
    except SettingsError as exc:
        print(f"settings  {args.service}: CANNOT CHECK: {exc}")
        return 2
    rows = compare(args.service, entry, data)
    print(report(args.service, settings, entry, rows, notices))
    bad = [r[0] for r in rows if not r[3]]
    if bad:
        print(
            f"settings  {args.service}: {len(bad)} setting(s) differ from deploy/railway/service_settings.json: {', '.join(bad)}"
        )
        return 1
    print(f"settings  {args.service}: all {len(rows)} checked settings match deploy/railway/service_settings.json")
    return 0


def cmd_print_query(args) -> int:
    try:
        settings = load_settings(args.settings_file)
        entry = service_entry(settings, args.service)
        argv = railway_api_argv(build_query(entry), query_variables(settings, entry))
    except SettingsError as exc:
        print(f"print-query: {exc}", file=sys.stderr)
        return 2
    print(shlex.join(argv))
    return 0


def update_input(entry: dict, with_undocumented: bool) -> tuple[dict, list[str]]:
    """(input, left_out): the serviceInstanceUpdate input for the expected settings."""
    fields = entry["serviceInstance"]
    allowed = READABLE_FIELDS if with_undocumented else DOCUMENTED_UPDATE_FIELDS
    payload = {k: fields[k] for k in sorted(fields) if k in allowed}
    left_out = sorted(k for k in fields if k not in allowed)
    if entry.get("limits") is not None:
        left_out.append("limits")
    return payload, left_out


def cmd_print_update(args) -> int:
    try:
        settings = load_settings(args.settings_file)
        entry = service_entry(settings, args.service)
    except SettingsError as exc:
        print(f"print-update: {exc}", file=sys.stderr)
        return 2
    payload, left_out = update_input(entry, args.with_undocumented)
    doc = (
        "mutation N41ServiceSettingsUpdate($environmentId: String!, $serviceId: String!, "
        "$input: ServiceInstanceUpdateInput!) { serviceInstanceUpdate(environmentId: $environmentId, "
        "serviceId: $serviceId, input: $input) }"
    )
    variables = dict(query_variables(settings, entry), input=payload)
    print(shlex.join(["railway", "api", doc, "--variables", json.dumps(variables, sort_keys=True)]))
    if left_out:
        print(
            f"print-update: not in this mutation (set separately, see the pack): {', '.join(left_out)}", file=sys.stderr
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func in (("check", cmd_check), ("print-query", cmd_print_query), ("print-update", cmd_print_update)):
        p = sub.add_parser(name)
        p.add_argument("service")
        p.add_argument("--settings-file", default=None, help="expected settings JSON, or - for stdin")
        if name == "print-update":
            p.add_argument("--with-undocumented", action="store_true")
        p.set_defaults(func=func)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
