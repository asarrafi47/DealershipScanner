# DealershipScanner — session rules

- Before scanning, discovering, or fixing anything about a dealership, read
  `docs/NETWORK_SCAN_PROCESS.md`. It states the goal (network-traffic scanning, no headless
  browsers) and the per-dealership loop: discovery → verify → scan → verify with NHTSA →
  summary log + scan instructions + recipe.
- Every dealership touched gets its logs under `workspace/dealer_logs/<dealer_id>/`
  (discovery.md, location.md, scan_runs.md, summary.md, scan_instructions.md). Errors and
  how they were fixed go in those files, in detail; general lessons roll up into
  `workspace/dealer_logs/_learning/`. A scan without its logs is not finished.
- NHTSA vPIC values outrank the dealer feed for drivetrain and electrification. The
  catalog (EPA) never outranks the dealer's own engine text or the VIN decode.
- Every push carries a new `VERSION`: run `scripts/bump_version.sh patch|minor|major` before
  pushing (the tracked pre-push hook refuses otherwise; install it once with
  `git config core.hooksPath scripts/git-hooks`). A push to `main` must also raise VERSION
  above the remote main and have a filled `## [<VERSION>]` CHANGELOG section
  (`scripts/release_guard.py`; CI's `release-guard` job checks pushes and PRs to main).
- Releases follow `docs/RELEASING.md`: `main` moves only by fast-forward to a tagged
  release, Railway gets code only through `deploy/railway/deploy_web.sh` /
  `deploy_scanner_nightly.sh` from that tag, and no Railway service ever builds from GitHub.
- Never commit `.env` or secrets. Do not run heavy local workloads on battery without
  asking. One scanner process per lock file: `workspace/scanner.lock` by default, or the
  file named by `SCANNER_LOCK_PATH` when the fleet runs as several disjoint shards on one
  host (2026-09-28: the scanner is CPU-bound in one interpreter, so parallel shards are
  the throughput lever, not larger batches). Shards must never share dealers.
