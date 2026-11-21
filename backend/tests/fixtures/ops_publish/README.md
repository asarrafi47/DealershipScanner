# ops_publish fixtures: three real shards of the 2026-11-03 fleet scan

Input for the golden tests of the scan results bundle, the accuracy gates and the improvement queue (docs/CLOUD.md,
the cloud-first operating model). Source: `workspace/pipeline/scan_2026_11_03_s0`, `s3` and `s6` of the Mac fleet run
(458 dealers in 8 shards, a scratch database, 2026-11-03). Together they hold 172 of the 458 dealers: 101 ok, 13 thin,
55 inaccurate, 3 no_rows.

Per shard directory:
- `triage.json.gz`: the pipeline's `triage.json`, gzip, compact, sorted keys, the infrastructure-only `chromium_leaks`
  list removed. Read it with `json.load(gzip.open(path))`.
- `needs_discovery.txt`, `slow_dealers.txt`: copied unchanged.

Sanitizing: the files were scanned for API-key, token, password and URL-with-credential patterns and for local paths
(`/Users/`): nothing found. Dealer ids are public business domains; the verdict fields are our own quality labels. Do NOT add
dealer logs or recipes here: some of them carry third-party API keys and belong in the private data repository only.
The 11-03 numbers come from a Mac run against a scratch database: `known_before` is 0 for every dealer (empty 30-day window),
which is itself a finding of the gates design.
