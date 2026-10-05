#!/usr/bin/env python3
"""Mass HTTP-only dealer pipeline: recipe -> scan -> NHTSA heal -> assess -> triage.

One deterministic pass per dealer, no browser, no LLM. Its output (triage JSON)
is what the agentic discovery workflow consumes for the dealers that came back
empty, thin or broken.

Per dealer:
  1. recipe   ensure a non-stale recipe exists; synthesize from the homepage when
              missing (recipe_synth platform templates), record platform + totals
  2. scan     `scanner.py --dealer-id … --scan-only` with SCANNER_HTTP_ONLY=1
              (batched: N dealers per scanner process, dealer_concurrency=N)
  3. vpic     decode new VINs with NHTSA vPIC, store drivetrain / electrification
  4. assess   scan_runs.summary_json (capture_coverage, recipe_coverage,
              vdp_prefetch, error) + row counts -> verdict:
                ok        rows >= 50% of last listed count (or >= 20 when unknown)
                          and price/trim/colour >= 90%
                thin      rows fine, listed field(s) below 90%
                no_rows   recipe replay produced nothing (needs discovery)
                error     scanner error for the dealer
                no_recipe nothing to replay and synthesis failed (needs discovery)
  5. lifecycle after ALL batches (--no-lifecycle to skip): no_recipe / no_rows /
              validated_zero / auth errors / recipe_status stale: (and rejected:
              beside a failing verdict; a rejected re-synth beside a recipe that
              replays stays out) -> force re-synth + validation -> discovery
              capture (own process; never for an ok / thin / inaccurate scan) +
              validation -> one retry batch (HTTP-only) -> assess again. Once per
              dealer per UTC day (scan_hints.lifecycle_last_attempt). Triage rows
              gain `lifecycle` (none | resynth_ok | capture_ok | failed:<reason> |
              resynth_failed:<reason>, the last for a dealer whose scan passed)
              and `retried: <old> → <new>`; <out>/needs_discovery.txt lists the
              dealers still failing.
  6. platform clustering over the run's needs_discovery dealers
              (backend/scripts/platform_candidates.py): every cluster of 2+ prints
              `discovery: N dealers share unknown platform <signature> (a, b, c) →
              workspace/dealer_logs/_learning/platform_candidates.md`.

Usage:
  python -m backend.scripts.dealer_pipeline --dealers a,b,c --out workspace/pipeline/run1
  python -m backend.scripts.dealer_pipeline --manifest dealers.json --limit 40 --shard-index 0 --shard-count 4 --out …
  python -m backend.scripts.dealer_pipeline --dealers a --out … --skip-scan   # assess an existing scan only
"""
from __future__ import annotations

import argparse
import sys

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.db.inventory_db import get_conn  # noqa: E402,F401

# The pipeline lives in backend/scanner/pipeline/ (audit F11). Every name below is
# re-exported for the importers of this module (tests, discovery_probe,
# unstale_host_blocked_recipes). A test that stubs one of them must patch the
# module that looks it up (backend/tests/pipeline_patch.py), not this facade.
from backend.scanner.pipeline.assess import (  # noqa: E402,F401
    VIN_OWNER_CONFLICT_REASON_MIN,
    _assess,
    _one_condition_ok,
    assess,
    record_timing,
    verify_accuracy,
)
from backend.scanner.pipeline.constants import (  # noqa: E402,F401
    BASELINE_DAYS,
    DISCREPANCY_FLOOR,
    FIELD_FLOOR,
    HTTP_ONLY_ENV,
    INCOMPLETE_FLOOR,
    KEY_FIELDS,
    LOG_ROOT,
    MIN_ROWS_UNKNOWN,
    PROCESS_DOC,
    RECONCILE_MIN_SHARE,
    ROOT,
    ROW_FLOOR,
    SECONDARY_FIELDS,
)
from backend.scanner.pipeline.db import (  # noqa: E402,F401
    _assess_conn,
    _conn_alive,
    _rows,
    chromium_process_count,
    wait_for_db,
)
from backend.scanner.pipeline.dealer_logs import (  # noqa: E402,F401
    _log_append,
    _log_write_once,
    _pct,
    _timing_text,
    log_discovery,
    log_scan_run,
    write_instructions_if_first_success,
)
from backend.scanner.pipeline.lifecycle import (  # noqa: E402,F401
    _AUTH_ERROR_MARKERS,
    _NO_URL_SYNTHS,
    FAILING_VERDICTS,
    LIFECYCLE_OK,
    LIFECYCLE_VERDICTS,
    SCANNED_VERDICTS,
    _learning_append,
    _lifecycle_block,
    _scan_hints,
    _set_scan_hints,
    lifecycle_attempted_today,
    route_verdict,
    run_lifecycle,
    run_lifecycle_pass,
    validate_live_recipes,
)
from backend.scanner.pipeline.reconcile import reconcile_dealer  # noqa: E402,F401
from backend.scanner.pipeline.recipes import ensure_recipe  # noqa: E402,F401
from backend.scanner.pipeline.roster import dealer_from_manifest, dealers_from_db, load_manifest_dealers  # noqa: E402,F401
from backend.scanner.pipeline.run import run  # noqa: E402
from backend.scanner.pipeline.runner import (  # noqa: E402,F401
    LOCK_FILE,
    _default_lock_path,
    _lock_holder_alive,
    run_discovery_capture,
    run_http_only_scan,
    scan_retry_batch,
    wait_for_scanner_lock,
)
from backend.scanner.pipeline.triage import (  # noqa: E402,F401
    platform_cluster_lines,
    triage_table,
    write_needs_discovery,
    write_slow_dealers,
)
from backend.scanner.pipeline.vpic import vpic_for_dealers  # noqa: E402,F401


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dealers", default="", help="comma-separated dealer ids")
    ap.add_argument("--manifest", default=str(ROOT / "dealers.json"))
    ap.add_argument("--limit", type=int, default=0, help="first N manifest dealers (after sharding)")
    ap.add_argument("--shard-index", type=int, default=0)
    ap.add_argument("--shard-count", type=int, default=1)
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--batch", type=int, default=4, help="dealers per scanner process (= dealer concurrency)")
    ap.add_argument("--scan-timeout", type=int, default=3600, help="seconds per scanner batch")
    ap.add_argument("--lock-wait", type=int, default=5400, help="seconds to wait for another scanner run to finish")
    ap.add_argument("--skip-scan", action="store_true", help="assess the latest run since --since instead of scanning")
    ap.add_argument("--since", default="", help="with --skip-scan: ISO timestamp of the run to assess")
    ap.add_argument("--force-synth", action="store_true", help="re-synthesize recipes even when one exists")
    ap.add_argument("--no-vpic", action="store_true")
    ap.add_argument("--no-reconcile", action="store_true", help="report but do not retire rows the run did not return")
    ap.add_argument("--no-discover", action="store_true",
                    help="do not run the discovery browser capture for dealers whose recipe synthesis failed (default: run it once per dealer per day, in its own process)")
    ap.add_argument("--no-lifecycle", action="store_true",
                    help="skip the recipe lifecycle after the main batches (force re-synth -> discovery capture -> retry batch for no_recipe / no_rows / stale / rejected dealers; once per dealer per day)")
    args = ap.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
