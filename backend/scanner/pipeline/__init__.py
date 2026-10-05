"""The HTTP-only dealer pipeline (recipe -> scan -> NHTSA heal -> assess -> triage).

Split out of backend/scripts/dealer_pipeline.py (docs/monolith_audit_2026_10_01/datascripts.md F11),
which keeps the CLI (argparse + ``main``) and re-exports every name below for its importers:

  constants    paths (ROOT, LOG_ROOT), verdict floors, HTTP_ONLY_ENV
  db           process / DB plumbing: chromium count, wait_for_db, _assess_conn, _rows, get_conn
  roster       dealers from the manifest and the cars table
  recipes      1. ensure_recipe (synthesis + validation gate)
  runner       2. scanner lock wait, discovery capture subprocess, HTTP-only scan subprocess, retry batches
  vpic         3. NHTSA vPIC decode + heal
  dealer_logs  the per-dealer markdown logs (CLAUDE.md / docs/NETWORK_SCAN_PROCESS.md)
  reconcile    reconcile_dealer: retires listings a full accepted run did not return
  assess       4. assess + verify_accuracy + timing
  lifecycle    5. scan hints, route_verdict, re-synth / re-discovery, lifecycle pass
  triage       triage table, needs_discovery.txt, slow_dealers.txt, platform clustering
  run          ``run(args)``: main's numbered steps

Kept import-light on purpose: importing a submodule must not pull in the rest.
"""
