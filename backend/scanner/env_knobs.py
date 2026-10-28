"""Registry of every environment variable the scanner package reads or sets (D-OD11).

One entry per name: type, default (as the reading code states it), category, the
backend/scanner files that read it, the files that set it, and a one-line doc. The
walker below (``scan_sources``) is the single source of truth for "who reads what":
backend/tests/test_scanner_env_knob_registry.py fails on a name the code touches that
is not registered, on a registered name nothing reads, and when the generated table
(docs/monolith_audit_2026_10_01/scanner_env_knobs.md) is stale.

Categories
  live         tunes behaviour of the HTTP-only pipeline; read on the current path
  operator     a kill switch or run-shape control the owner sets in deploys or CLIs
  browser_era  only the browser path (or nothing any more) reads it; P17C.7 retires it
  test_only    read only so tests can steer the code
  internal     set by the platform (Kubernetes, libpq) or a secret, not an owner knob

Not under scanner/config/: that directory holds the intercept policy JSON.

Regenerate the table after editing KNOBS:  python -m backend.scanner.env_knobs --write-doc
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

CATEGORIES = ("live", "operator", "browser_era", "test_only", "internal")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCANNER_DIR = Path(__file__).resolve().parent
DOC_PATH = _REPO_ROOT / "docs" / "monolith_audit_2026_10_01" / "scanner_env_knobs.md"


@dataclass(frozen=True)
class Knob:
    type: str  # bool | int | float | str | path | csv
    default: str | None  # as the reading code states it; None = unset means off/empty
    category: str
    readers: tuple[str, ...]  # files under backend/scanner that read it
    set_in: tuple[str, ...]  # repo files that set it (deploys, scripts, scanner code)
    doc: str


# Reads whose name is not a string constant at the call site. Keyed by file under
# backend/scanner; "reads" / "sets" list the names that site handles. The test checks
# each name still appears as a literal in its file.
DYNAMIC_SITES: dict[str, dict[str, tuple[str, ...]]] = {
    "http_fetch.py": {"reads": ("SCANNER_HTTP_PROXY", "HTTPS_PROXY", "HTTP_PROXY"), "sets": ()},
    "scan_efficiency.py": {
        "reads": (),
        "sets": (
            "SCANNER_VDP_EP_MAX",
            "SCANNER_VDP_PRICE_MAX",
            "SCANNER_POST_INTERIOR_VISION",
            "SCANNER_GALLERY_VISION_INLINE",
            "SCANNER_SCAN_ONLY",
            "SCANNER_POST_REPAIR",
            "SCANNER_POST_LISTING_DESCRIPTION",
            "SCANNER_POST_WINDOW_STICKER",
            "SCANNER_POST_GAS_PRICES",
            "SCANNER_POST_DEALER_RATINGS",
            "SCANNER_POST_LISTING_GAP_FILL",
            "SCANNER_POST_DICT_ENRICH",
            "SCANNER_POST_ENRICH",
            "SCANNER_POST_ENRICH_VISION",
            "SCANNER_GALLERY_VISION_POST",
            "SCANNER_VDP_DOWNLOAD_IMAGES",
            "SCANNER_POST_VPIC",
        ),
    },
}

_ENV_METHODS_READ = {"get", "getenv"}
_ENV_METHODS_SET = {"setdefault", "pop"}


def _is_environ(node: ast.AST) -> bool:
    return ast.unparse(node).endswith("environ")


def _env_call_kind(call: ast.Call) -> str | None:
    """'read' / 'set' when the call touches os.environ / os.getenv, else None."""
    f = call.func
    if not isinstance(f, ast.Attribute):
        return None
    if f.attr == "getenv":
        return "read"
    if _is_environ(f.value):
        if f.attr in _ENV_METHODS_READ:
            return "read"
        if f.attr in _ENV_METHODS_SET:
            return "set"
    return None


def _call_name(call: ast.Call) -> str | None:
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)


def scan_sources(root: Path | None = None) -> dict:
    """Walk backend/scanner and report env access.

    Returns {"reads": {name: {relpath}}, "sets": {name: {relpath}}, "dynamic": {relpath: [lineno]}}.
    Wrapper helpers (a function that passes one of its own parameters to os.environ,
    such as ``_env_flag``) are found first; their call sites count as reads and their
    bodies are skipped. DYNAMIC_SITES is merged in only for the real tree. Module-level ``NAME = "UPPER"`` constants are resolved.
    """
    own_tree = root is None
    root = root or _SCANNER_DIR
    trees = {p: ast.parse(p.read_text(encoding="utf-8")) for p in sorted(root.rglob("*.py"))}
    wrappers: set[str] = set()
    for tree in trees.values():
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            params = {a.arg for a in fn.args.args + fn.args.kwonlyargs}
            for n in ast.walk(fn):
                if isinstance(n, ast.Call) and _env_call_kind(n) and n.args:
                    a0 = n.args[0]
                    if isinstance(a0, ast.Name) and a0.id in params:
                        wrappers.add(fn.name)
    reads: dict[str, set[str]] = {}
    sets: dict[str, set[str]] = {}
    dynamic: dict[str, list[int]] = {}
    for path, tree in trees.items():
        rel = path.relative_to(root).as_posix()
        consts = {
            t.id: n.value.value
            for n in tree.body
            if isinstance(n, ast.Assign) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str)
            for t in n.targets
            if isinstance(t, ast.Name)
        }
        wrapper_spans = [
            (fn.lineno, fn.end_lineno or fn.lineno)
            for fn in ast.walk(tree)
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and fn.name in wrappers
        ]

        def in_wrapper(lineno: int) -> bool:
            return any(a <= lineno <= b for a, b in wrapper_spans)

        def note(arg: ast.AST | None, kind: str, lineno: int) -> None:
            if isinstance(arg, ast.Name) and arg.id in consts:
                arg = ast.Constant(consts[arg.id])
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                (reads if kind == "read" else sets).setdefault(arg.value, set()).add(rel)
            elif not in_wrapper(lineno):
                dynamic.setdefault(rel, []).append(lineno)

        for n in ast.walk(tree):
            if isinstance(n, ast.Call):
                kind = _env_call_kind(n)
                if kind and n.args:
                    note(n.args[0], kind, n.lineno)
                elif _call_name(n) in wrappers and n.args and not kind:
                    note(n.args[0], "read", n.lineno)
            elif isinstance(n, ast.Subscript) and _is_environ(n.value):
                note(n.slice, "set" if isinstance(n.ctx, (ast.Store, ast.Del)) else "read", n.lineno)
    for rel, spec in DYNAMIC_SITES.items() if own_tree else ():
        for nm in spec["reads"]:
            reads.setdefault(nm, set()).add(rel)
        for nm in spec["sets"]:
            sets.setdefault(nm, set()).add(rel)
    return {"reads": reads, "sets": sets, "dynamic": dynamic}


KNOBS: dict[str, Knob] = {
    'ANTHROPIC_API_KEY': Knob('str', None, 'internal', ('job_diagnosis.py', 'post_scan/pipeline.py'), ('deploy/docker-compose.yml', 'deploy/k8s/secret.yaml', 'deploy/railway/sync-vault-to-railway.sh'), 'Anthropic key; post-scan vision/enrichment stages and job diagnosis skip when empty.'),
    'DEALERS_FROM_ACTIVE_INVENTORY': Knob('bool', None, 'live', ('cli.py', 'manifest.py'), (), 'Roster = dealers with active inventory rows instead of the manifest.'),
    'DEALERS_FROM_DB': Knob('bool', None, 'live', ('cli.py', 'manifest.py'), (), 'Roster read from the dealers table instead of dealers.json.'),
    'DEALERS_FROM_SCANNABLE': Knob('bool', None, 'operator', ('manifest.py',), ('deploy/nightly_http_refresh.sh',), 'Roster = the scannable-roster view (see SCANNABLE_ROSTER_RULE).'),
    'DEALERS_MANIFEST_PATH': Knob('path', 'ROOT/dealers.json', 'operator', ('constants.py', 'manifest.py'), ('backend/scanner/cli.py', 'deploy/k8s/configmap.yaml', 'deploy/k8s/cronjob-discovery.yaml', 'scripts/scan_irvinebmw_test.sh'), 'Manifest file; the CLI --manifest flag sets it.'),
    'DEALER_LOGS_ROOT': Knob('path', 'workspace/dealer_logs', 'live', ('pipeline/constants.py', 'recipe_validation.py'), (), 'Where per-dealer logs are written.'),
    'GROUP_POOLS_PATH': Knob('path', 'upsert/group_pools default', 'live', ('upsert/ownership.py',), (), 'Group-feed pool definitions used by ownership attribution.'),
    'INTERIOR_VISION_FALLBACK_HERO': Knob('bool', None, 'live', ('post_scan/pipeline.py',), (), 'Interior vision may fall back to the hero image.'),
    'INTERIOR_VISION_FALLBACK_THROUGH_WINDOWS': Knob('bool', '1', 'live', ('post_scan/pipeline.py',), (), 'Interior vision may use through-window exterior shots.'),
    'INTERIOR_VISION_MAX_URL_TRIES': Knob('int', '3', 'live', ('post_scan/pipeline.py',), (), 'Image URLs tried per car in interior vision.'),
    'INTERIOR_VISION_NO_EXTERIOR_FALLBACK': Knob('bool', None, 'live', ('post_scan/pipeline.py',), (), 'Disable the exterior fallback in interior vision.'),
    'INVENTORY_PAGE_SIZE': Knob('int', '500', 'live', ('scrapers/dealer_com_bulk_fetch.py',), (), 'Page size for the dealer.com bulk fetch.'),
    'JOB_COMPLETION_INDEX': Knob('int', None, 'internal', ('manifest.py',), (), 'Kubernetes Indexed Job index; fallback for SCANNER_SHARD_INDEX.'),
    'LISTING_GAP_FILL_ALLOW_DDG': Knob('bool', '1', 'live', ('post_scan/gap_fill.py',), (), 'Gap fill may search DuckDuckGo for missing listing pages.'),
    'LISTING_GAP_FILL_DDG_MAX': Knob('int', '60', 'live', ('post_scan/gap_fill.py',), (), 'Max DuckDuckGo lookups per gap-fill run (0-2000).'),
    'LISTING_GAP_FILL_DDG_TIMEOUT': Knob('float', '15', 'live', ('post_scan/gap_fill.py',), (), 'Seconds per DuckDuckGo lookup.'),
    'PGDATABASE': Knob('str', None, 'internal', ('recipes.py',), ('scripts/pg_test_up.sh',), 'Fallback database name when a recipe DB URL omits it.'),
    'PGHOST': Knob('str', None, 'internal', ('recipes.py',), (), 'Fallback host when a recipe DB URL omits it.'),
    'PGPORT': Knob('int', '5432', 'internal', ('recipes.py',), ('scripts/pg_test_up.sh',), 'Fallback port when a recipe DB URL omits it.'),
    'PIPELINE_RETIRE_ALARM_GAP_AWARE': Knob('bool', None, 'live', ('pipeline/reconcile.py',), (), 'Pipeline retire alarm discounts rows covered by a gap.'),
    'RECIPES_DB_DISABLED': Knob('bool', None, 'operator', ('recipe_store.py',), ('backend/scripts/platform_candidates.py',), 'Stop reading and writing recipes in the database.'),
    'ROOFTOP_LEDGER': Knob('bool', '1', 'live', ('rooftop_ledger.py',), (), 'Write the rooftop attribution ledger.'),
    'SCANNABLE_ROSTER_RULE': Knob('bool', '1', 'operator', ('manifest.py',), ('backend/scripts/fleet_scan.py',), 'Apply the scannable-roster rule to rosters.'),
    'SCANNER_ALGOLIA_AUTO_SCOPE': Knob('bool', '1', 'live', ('scrapers/algolia_scope.py',), (), 'Auto-scope Algolia queries to the dealer.'),
    'SCANNER_ALGOLIA_RECOVERY_FLOOR': Knob('int', '80', 'live', ('inventory_recovery.py',), (), 'Minimum Algolia rows before a recovery result is accepted.'),
    'SCANNER_ALLOW_BROWSER': Knob('bool', None, 'operator', ('browser_gate.py',), ('Dockerfile.discovery', 'backend/scanner/discovery_capture.py', 'deploy/home_lane/run_home_lane.sh'), 'Allow browser (Playwright) paths; discovery only.'),
    'SCANNER_ALLOW_PRIVATE_HOSTS': Knob('bool', None, 'live', ('net/ssrf.py',), (), 'Switch the SSRF guard off (labs against local hosts).'),
    'SCANNER_AUTO_HEAL': Knob('bool', '1', 'live', ('post_scan/auto_heal.py',), (), 'Run the post-scan auto heal.'),
    'SCANNER_DEALER_COM_BULK_FETCH': Knob('bool', '1', 'operator', ('scrapers/dealer_com_bulk_fetch.py',), ('scripts/scan_irvinebmw_test.sh',), 'Use the dealer.com bulk inventory fetch.'),
    'SCANNER_DEALER_TIMEOUT_SEC': Knob('int', '10800', 'operator', ('orchestrator.py',), ('backend/scanner/cli.py',), 'Per-dealer wall-clock limit; CLI --dealer-timeout sets it.'),
    'SCANNER_DEFAULT_INTERVAL_HOURS': Knob('int', '24', 'live', ('job_queue.py',), (), 'Default rescan interval for queued jobs.'),
    'SCANNER_DELTA_CONCURRENCY': Knob('int', '4', 'operator', ('delta_scan.py',), ('deploy/nightly_http_refresh.sh',), 'Delta-scan dealer concurrency (1-12).'),
    'SCANNER_DELTA_DEALER_TIMEOUT': Knob('int', '300', 'operator', ('delta_scan.py',), ('deploy/nightly_http_refresh.sh',), 'Delta-scan per-dealer timeout in seconds.'),
    'SCANNER_EGRESS_TAG': Knob('str', None, 'operator', ('manifest.py', 'recipes.py'), ('deploy/home_lane/run_home_lane.sh', 'scripts/railway_scan_fleet.sh'), 'Names the egress lane (for example railway); filters roster and recipe cache.'),
    'SCANNER_FAST_MODE': Knob('bool', None, 'operator', ('scan_efficiency.py',), ('deploy/k8s/cronjob-scanner-sharded.yaml', 'deploy/k8s/cronjob-scanner.yaml'), 'Apply the fast-mode env defaults.'),
    'SCANNER_GALLERY_RECOVERY_MIN': Knob('int', '14', 'live', ('vdp/html_recovery.py',), (), 'Gallery size below which HTML recovery runs.'),
    'SCANNER_GALLERY_VISION_FILTER': Knob('bool', None, 'live', ('post_scan/pipeline.py',), (), 'Vision-filter gallery images after the scan.'),
    'SCANNER_GALLERY_VISION_INLINE': Knob('bool', None, 'operator', ('scan_efficiency.py',), ('backend/scanner/scan_efficiency.py',), 'Run gallery vision inline during the scan.'),
    'SCANNER_GALLERY_VISION_POST': Knob('bool', None, 'operator', ('scan_efficiency.py',), ('backend/scanner/cli.py', 'backend/scanner/scan_efficiency.py'), 'Run gallery vision after the scan; the CLI defaults it to 1.'),
    'SCANNER_GOTO_403_EXTRA_ATTEMPTS': Knob('int', '2', 'browser_era', ('phases/nav.py',), (), 'Extra browser goto attempts after a 403.'),
    'SCANNER_GOTO_MAX_ATTEMPTS': Knob('int', '3', 'browser_era', ('phases/nav.py',), (), 'Browser goto attempts.'),
    'SCANNER_HEAL_WORKERS': Knob('int', '4', 'live', ('post_scan/gap_fill.py',), (), 'Gap-fill worker threads (1-12).'),
    'SCANNER_HTTP_ONLY': Knob('bool', None, 'operator', ('browser_gate.py',), ('backend/scripts/dealer_pipeline.py',), 'Refuse every browser path.'),
    'SCANNER_HTTP_PROXY': Knob('str', None, 'operator', ('http_fetch.py', 'net/ssrf.py'), ('deploy/home_lane/run_home_lane.sh',), 'HTTP proxy for scanner fetches; also relaxes the SSRF guard.'),
    'SCANNER_HYDRATION_TIMEOUT_MS': Knob('int', '30000', 'browser_era', ('phases/nav.py',), (), 'Browser page hydration wait.'),
    'SCANNER_IDLE_IN_TXN_TIMEOUT_MS': Knob('int', '60000', 'live', ('database.py',), (), 'Postgres idle_in_transaction_session_timeout for scanner connections.'),
    'SCANNER_INFINITE_SCROLL_MAX_ROUNDS': Knob('int', '18', 'browser_era', ('phases/nav.py',), (), 'Browser infinite-scroll rounds.'),
    'SCANNER_INTERCEPT_COVERAGE_RATIO': Knob('float', '0.92', 'browser_era', ('scan_efficiency.py',), (), 'Browser intercept coverage ratio that ends the wait (0.5-1.0).'),
    'SCANNER_INTERCEPT_MIN_ROWS': Knob('int', '8', 'browser_era', ('scan_efficiency.py',), (), 'Browser intercept rows needed to end the wait.'),
    'SCANNER_INTERCEPT_URL_ALLOW': Knob('csv', None, 'browser_era', ('scrapers/scanner_intercept_filter.py',), (), 'Extra URL substrings the browser intercept allows.'),
    'SCANNER_INTERCEPT_URL_DENY': Knob('csv', None, 'browser_era', ('scrapers/scanner_intercept_filter.py',), (), 'Extra URL substrings the browser intercept denies.'),
    'SCANNER_INVENTORY_IDLE_SEC_FOUND': Knob('int', '2', 'browser_era', ('scan_efficiency.py',), (), 'Browser idle seconds once inventory was found.'),
    'SCANNER_INVENTORY_IDLE_SEC_MISSING': Knob('int', '6', 'browser_era', ('scan_efficiency.py',), (), 'Browser idle seconds while inventory is missing.'),
    'SCANNER_INVENTORY_JSON_TAIL_MS': Knob('int', '2500', 'browser_era', ('scan_efficiency.py',), (), 'Browser wait for trailing inventory JSON.'),
    'SCANNER_INVENTORY_PATHS': Knob('str', 'core', 'browser_era', ('scan_efficiency.py',), (), 'Inventory paths opened per dealer; extended = legacy nine-path sweep.'),
    'SCANNER_INVENTORY_RECOVERY': Knob('bool', '1', 'live', ('inventory_recovery.py',), (), 'Run inventory recovery strategies when a scan comes back thin.'),
    'SCANNER_INVENTORY_WAIT_MS': Knob('int', '18000', 'browser_era', ('phases/nav.py',), (), 'Browser wait for inventory responses.'),
    'SCANNER_JOB_TIMEOUT_SEC': Knob('int', '3600', 'live', ('job_queue.py',), (), 'Job-queue job timeout.'),
    'SCANNER_LISTING_FETCH_CHAIN': Knob('bool', '1', 'operator', ('post_scan/gap_fill.py',), ('deploy/nightly_http_refresh.sh',), 'Gap fill uses the listing fetch chain.'),
    'SCANNER_LOCK_PATH': Knob('path', 'workspace/scanner.lock', 'live', ('scan_lock.py',), (), 'Scanner lock file; one process per lock file.'),
    'SCANNER_MAX_DEALER_CONCURRENCY': Knob('int', None, 'operator', ('inventory_write.py',), ('backend/scanner/cli.py', 'deploy/k8s/cronjob-scanner-sharded.yaml'), 'Dealers scanned concurrently; CLI --dealer-concurrency sets it.'),
    'SCANNER_MAX_VDP_CONCURRENCY': Knob('int', None, 'operator', ('scan_efficiency.py',), ('backend/scanner/cli.py', 'scripts/scan_irvinebmw_test.sh'), 'VDP fetch concurrency; CLI --vdp-concurrency sets it.'),
    'SCANNER_MONRONEY_VISION': Knob('bool', '1', 'live', ('post_scan/pipeline.py',), (), 'Read Monroney stickers with vision after the scan.'),
    'SCANNER_NETWORK_LEDGER': Knob('bool', '1', 'browser_era', ('network_observer.py',), (), 'Record the network ledger in the observer.'),
    'SCANNER_NETWORK_LEDGER_AUTH': Knob('bool', '0', 'browser_era', ('network_observer.py',), (), 'Keep auth headers in the network ledger.'),
    'SCANNER_NO_RETIRE': Knob('bool', None, 'live', ('inventory_reconcile.py',), (), 'Stop button: no retirement by scanner, delta scan or rooftop disown.'),
    'SCANNER_PAGINATION_DEBUG': Knob('bool', None, 'browser_era', ('phases/nav.py',), (), 'Log browser pagination decisions.'),
    'SCANNER_PAGINATION_RESPONSE_WAIT_MS': Knob('int', '8000', 'browser_era', ('phases/nav.py',), (), 'Browser wait for a pagination response.'),
    'SCANNER_PARALLEL_UPSERT': Knob('bool', 'on for Postgres', 'operator', ('inventory_write.py',), ('deploy/k8s/cronjob-scanner-sharded.yaml',), 'Parallel inventory upserts; 0 or 1 overrides the backend default.'),
    'SCANNER_POST_DEALER_RATINGS': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan dealer ratings stage.'),
    'SCANNER_POST_DICT_ENRICH': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan dictionary enrichment stage.'),
    'SCANNER_POST_ENRICH': Knob('bool', '0', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan LLM enrichment stage.'),
    'SCANNER_POST_ENRICH_VISION': Knob('bool', '0', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Vision in the post-scan enrichment stage.'),
    'SCANNER_POST_GALLERY_RECOVERY': Knob('bool', '1', 'live', ('post_scan/pipeline.py',), (), 'Post-scan gallery recovery stage.'),
    'SCANNER_POST_GAS_PRICES': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan gas price refresh stage.'),
    'SCANNER_POST_INTERIOR_VISION': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan interior vision stage.'),
    'SCANNER_POST_LISTING_DESCRIPTION': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan listing description stage.'),
    'SCANNER_POST_LISTING_GAP_FILL': Knob('bool', '1', 'operator', ('delta_scan.py', 'post_scan/pipeline.py'), ('backend/scanner/scan_efficiency.py',), 'Post-scan listing gap fill (also in the delta scan).'),
    'SCANNER_POST_LISTING_GAP_FILL_MAX': Knob('int', '400', 'live', ('post_scan/gap_fill.py',), (), 'Max cars gap-filled per run.'),
    'SCANNER_POST_REPAIR': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan repair stage.'),
    'SCANNER_POST_SCAN_LOOKBACK_HOURS': Knob('int', '6', 'live', ('post_scan/job.py',), (), 'Post-scan job lookback window.'),
    'SCANNER_POST_VPIC': Knob('bool', '1', 'operator', ('scan_efficiency.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan NHTSA vPIC decode of new VINs.'),
    'SCANNER_POST_WINDOW_STICKER': Knob('bool', '1', 'operator', ('post_scan/pipeline.py',), ('backend/scanner/scan_efficiency.py',), 'Post-scan window sticker stage.'),
    'SCANNER_POST_WINDOW_STICKER_MAX': Knob('int', '0', 'live', ('post_scan/pipeline.py',), (), 'Max stickers fetched per run (0 = no cap).'),
    'SCANNER_PROFILER_TIMEOUT_S': Knob('int', '22', 'browser_era', ('phases/site_profile.py',), (), 'Site profiler timeout.'),
    'SCANNER_RECIPE_FETCH': Knob('bool', '1', 'live', ('recipes.py',), (), 'Recipe fetch path; 0 turns it off.'),
    'SCANNER_RECIPE_MIN_FIELD_COVERAGE': Knob('float', '0.5', 'live', ('recipes.py',), (), 'Minimum field coverage for a recipe to pass.'),
    'SCANNER_RECIPE_MIN_PRICE_COVERAGE': Knob('float', '0.5', 'live', ('recipes.py',), (), 'Minimum price coverage for a recipe to pass.'),
    'SCANNER_RECIPE_MIN_VEHICLES': Knob('int', '10', 'live', ('recipes.py',), (), 'Minimum vehicles for a recipe to pass.'),
    'SCANNER_RECONCILE': Knob('str', '1', 'operator', ('inventory_reconcile.py', 'pipeline/run.py'), ('backend/scanner/pipeline/run.py',), '1 retires, 0 disables, dry records the plan and writes nothing.'),
    'SCANNER_RECONCILE_BUCKET_SHARE': Knob('float', '0.6', 'live', ('inventory_reconcile.py',), (), 'Largest share of prior valid-VIN rows one run may retire.'),
    'SCANNER_RECONCILE_MIN_COVERAGE': Knob('float', None, 'live', ('inventory_reconcile.py',), (), 'Deprecated and ignored, except 0 (bucket guard off).'),
    'SCANNER_RECONCILE_MIN_ROWS': Knob('int', '20', 'live', ('inventory_reconcile.py',), (), 'Rows a scan needs before it may retire anything.'),
    'SCANNER_RECOVERY_HINT_FILTER': Knob('bool', '1', 'live', ('inventory_recovery.py',), (), 'Recovery filters strategies by platform hints.'),
    'SCANNER_RECOVERY_KNOWN_LOT_MIN': Knob('int', '20', 'live', ('inventory_recovery.py',), (), 'Known-lot size where the known-lot ratio applies.'),
    'SCANNER_RECOVERY_KNOWN_LOT_RATIO': Knob('float', '0.5', 'live', ('inventory_recovery.py',), (), 'Share of a known lot recovery must reach.'),
    'SCANNER_RECOVERY_MIN_ROWS': Knob('int', '8', 'live', ('inventory_recovery.py',), (), 'Rows a recovery result needs to be accepted.'),
    'SCANNER_RECOVERY_STRATEGY_TIMEOUT_SEC': Knob('int', '90', 'live', ('inventory_recovery.py',), (), 'Seconds per recovery strategy.'),
    'SCANNER_REPLAY_MAX_PAGES': Knob('int', 'hard cap', 'live', ('recipes.py',), (), 'Recipe replay page cap.'),
    'SCANNER_SCAN_ONLY': Knob('bool', None, 'operator', ('scan_efficiency.py',), ('backend/scanner/cli.py', 'backend/scanner/scan_efficiency.py', 'deploy/k8s/cronjob-scanner-sharded.yaml', 'deploy/k8s/cronjob-scanner.yaml', 'scripts/scan_irvinebmw_test.sh'), 'Disable post-scan stages (the CLI --scan-only sets it).'),
    'SCANNER_SCORE_INTERCEPT': Knob('bool', '1', 'browser_era', ('network_observer.py',), (), 'Score intercepted responses in the observer.'),
    'SCANNER_SHARD_COUNT': Knob('int', None, 'operator', ('manifest.py',), ('deploy/car-scanner/cronjob.yaml', 'deploy/k8s/cronjob-scanner-sharded.yaml'), 'Number of disjoint scan shards.'),
    'SCANNER_SHARD_INDEX': Knob('int', None, 'operator', ('manifest.py',), ('deploy/car-scanner/cronjob.yaml',), "This process's shard index (JOB_COMPLETION_INDEX fallback)."),
    'SCANNER_SISTER_STORE_FILTER': Knob('bool', '1', 'live', ('dealer/location.py',), (), 'Drop rows that belong to a sister store.'),
    'SCANNER_SISTER_STORE_MIN_KEEP': Knob('int', '8', 'live', ('dealer/location.py', 'phases/dealer_run_steps/enrich.py'), (), 'Rows the sister-store filter always keeps.'),
    'SCANNER_SISTER_STORE_SAFE': Knob('bool', '1', 'live', ('dealer/location.py', 'phases/dealer_run_steps/enrich.py'), (), 'Sister-store filter safe mode.'),
    'SCANNER_SKIP_DEALER_SUBSTRINGS': Knob('csv', None, 'live', ('manifest.py',), (), 'Dealers whose id or name contains one of these are skipped.'),
    'SCANNER_SNIFF_MAX_BYTES': Knob('int', '3000000', 'browser_era', ('network_observer.py',), (), 'Largest response body the observer sniffs.'),
    'SCANNER_SNIFF_NONJSON': Knob('bool', '1', 'browser_era', ('network_observer.py',), (), 'Observer also sniffs non-JSON bodies.'),
    'SCANNER_STALE_JOB_SEC': Knob('int', 'JOB_TIMEOUT+900', 'live', ('job_queue.py',), (), 'Age at which a running job is reaped.'),
    'SCANNER_SYNTH_FETCH_DELAY': Knob('float', '0', 'live', ('synth/http.py',), (), 'Seconds between recipe-synthesis fetches.'),
    'SCANNER_TRACE_VIN': Knob('str', None, 'live', ('upsert/sql.py',), (), 'Log every upsert decision for this VIN.'),
    'SCANNER_UNREACHABLE_BACKOFF_DAYS': Knob('int', '7', 'live', ('job_queue.py',), (), 'Back-off for unreachable dealers in the job queue.'),
    'SCANNER_VDP_COMPLETENESS_PASS': Knob('bool', None, 'operator', ('scan_efficiency.py',), ('scripts/scan_irvinebmw_test.sh',), 'VDP completeness pass mode.'),
    'SCANNER_VDP_DB_MERGE': Knob('bool', '1', 'live', ('vdp/prefetch.py',), (), 'VDP prefetch carries forward DB values.'),
    'SCANNER_VDP_DOWNLOAD_IMAGES': Knob('bool', '0', 'browser_era', (), ('backend/scanner/scan_efficiency.py', 'deploy/docker-compose.yml', 'deploy/k8s/configmap.yaml', 'deploy/railway/sync-vault-to-railway.sh'), 'Set by scan-only defaults; no reader left in the scanner.'),
    'SCANNER_VDP_EP_MAX': Knob('int', '0', 'browser_era', (), ('backend/scanner/scan_efficiency.py', 'scripts/scan_irvinebmw_test.sh'), 'Set by fast-mode defaults; no reader left in the scanner.'),
    'SCANNER_VDP_GALLERY_CAROUSEL_ONLY': Knob('bool', '1', 'browser_era', ('scan_efficiency.py',), (), 'Browser VDP gallery from the carousel only.'),
    'SCANNER_VDP_GALLERY_MAX_URLS': Knob('int', None, 'browser_era', ('scan_efficiency.py',), (), 'Cap on browser VDP gallery URLs.'),
    'SCANNER_VDP_HTML_RECOVERY': Knob('bool', '1', 'live', ('vdp/html_recovery.py',), (), 'VDP HTML gallery recovery.'),
    'SCANNER_VDP_HTTP_FIRST': Knob('bool', '1', 'live', ('vdp/prefetch.py',), (), 'VDP pages fetched over HTTP before any browser.'),
    'SCANNER_VDP_HTTP_FIRST_GALLERY_MIN': Knob('int', '12', 'live', ('vdp/prefetch.py',), (), 'Gallery size that satisfies the HTTP-first pass.'),
    'SCANNER_VDP_HTTP_FIRST_MAX': Knob('int', '800', 'live', ('vdp/prefetch.py',), (), 'Max VDP pages per dealer in the HTTP-first pass.'),
    'SCANNER_VDP_HTTP_FIRST_MAX_SEC': Knob('float', '300', 'live', ('vdp/prefetch.py',), (), 'Wall-clock cap of the HTTP-first pass.'),
    'SCANNER_VDP_HTTP_FIRST_SLOW_HOST_MAX': Knob('int', '60', 'live', ('vdp/prefetch.py',), (), 'Pages per slow host in the HTTP-first pass.'),
    'SCANNER_VDP_PRICE_MAX': Knob('int', '80', 'browser_era', (), ('backend/scanner/scan_efficiency.py', 'scripts/scan_irvinebmw_test.sh'), 'Set by fast-mode defaults; no reader left in the scanner.'),
    'SCANNER_VDP_RECIPE_CAPTURE': Knob('bool', '1', 'live', ('vdp/vdp_recipes.py',), (), 'Capture VDP recipes.'),
    'SCANNER_VDP_RECIPE_MAX_SEC': Knob('float', '300', 'live', ('vdp/vdp_recipes.py',), (), 'Wall-clock cap of VDP recipe replay.'),
    'SCANNER_VDP_RECIPE_REPLAY': Knob('bool', '1', 'live', ('vdp/vdp_recipes.py',), (), 'Replay VDP recipes.'),
    'SCANNER_VIN_OWNER_GUARD_HOURS': Knob('int', '48', 'live', ('database.py',), (), 'Window of the VIN owner-conflict guard.'),
    'SCANNER_WORKER_ID': Knob('str', 'worker-<hex>', 'internal', ('job_queue.py',), ('scripts/docker-entrypoint-jobs-worker.sh',), 'Job worker id.'),
    'SCANNER_WORKER_JOB_TYPES': Knob('csv', None, 'operator', ('job_queue.py',), ('Dockerfile.discovery',), 'Job types this worker takes.'),
    'SCAN_LANE': Knob('str', None, 'operator', ('manifest.py',), ('backend/scripts/fleet_scan.py', 'deploy/home_lane/run_home_lane.sh'), 'Roster lane filter.'),
    'WINDOW_STICKER_GM_EXPERIMENTAL': Knob('bool', None, 'live', ('post_scan/window_sticker.py',), (), 'Experimental GM window sticker path.'),
    'HTTPS_PROXY': Knob('str', None, 'internal', ('http_fetch.py',), (), 'Standard proxy variable; read after SCANNER_HTTP_PROXY.'),
    'HTTP_PROXY': Knob('str', None, 'internal', ('http_fetch.py',), (), 'Standard proxy variable; read after HTTPS_PROXY.'),
}

def render_table() -> str:
    """The generated knob table (markdown), one row per registered name."""
    lines = [
        "# Scanner environment knobs",
        "",
        "Generated from `backend/scanner/env_knobs.py` (`python -m backend.scanner.env_knobs --write-doc`).",
        "Do not edit by hand: `test_scanner_env_knob_registry.py` fails when this file is stale.",
        "",
        "Categories: live (tunes the current HTTP-only path), operator (kill switch or run-shape control),",
        "browser_era (only browser code reads it, or nothing does; P17C.7 retires these), test_only, internal",
        "(platform-set or secret). Readers are files under `backend/scanner/`; Set in is best effort (deploy",
        "files, scripts and scanner code that assign the name).",
        "",
    ]
    for cat in CATEGORIES:
        names = sorted(k for k, v in KNOBS.items() if v.category == cat)
        if not names:
            continue
        lines += [
            f"## {cat} ({len(names)})",
            "",
            "| Name | Type | Default | Readers | Set in | What it does |",
            "|---|---|---|---|---|---|",
        ]
        for nm in names:
            k = KNOBS[nm]
            readers = ", ".join(f"`{r}`" for r in k.readers) or "none"
            set_in = ", ".join(f"`{s}`" for s in k.set_in) or "not set in tracked files"
            default = f"`{k.default}`" if k.default else "unset"
            lines.append(f"| `{nm}` | {k.type} | {default} | {readers} | {set_in} | {k.doc} |")
        lines.append("")
    return "\n".join(lines)


def _main(argv: list[str]) -> int:
    if "--write-doc" in argv:
        DOC_PATH.write_text(render_table(), encoding="utf-8")
        print(f"wrote {DOC_PATH}")
        return 0
    print(render_table())
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
