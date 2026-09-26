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
- Never commit `.env` or secrets. Do not run heavy local workloads on battery without
  asking. One scanner process per machine (`workspace/scanner.lock`).
