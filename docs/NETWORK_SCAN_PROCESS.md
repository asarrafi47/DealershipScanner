# Network-traffic scanning: the goal and the process

**Read this before scanning any dealership.** It states the goal of this effort and the
loop every dealership goes through. Written from the owner's instructions, 2026-09-24.

## Goal

We are shifting the scanning process from using headless browsers to collect data from
VDPs to using network traffic data. **We will not touch headless browsers in this
process.** Every dealership's inventory is collected by replaying the site's own network
endpoints (feeds, search APIs, detail-page JSON) over plain HTTP.

## The loop, per dealership

### 1. Discovery

Run discovery on the dealership you want to scan and verify it presents results
correctly.

- If it does: let the HTTPS scan run (step 2).
- If it does not — 0 results, incorrect results, or anything else wrong — do both:
  1. create a log tied to that dealership's discovery to collect the errors
     (`workspace/dealer_logs/<dealer_id>/discovery.md`, see the layout below), and
  2. sit on top of the discovery process for that dealership to learn how they present
     data on their website (probe the site over HTTP: platform fingerprint, feed and
     search endpoints, pagination parameters, the inline JSON on detail pages, the
     headers the WAF wants), then rerun the discovery phase.
- Repeat this loop until you get VDP data. Only then move to the scanning phase.

Discovery here means the HTTP probing playbook (`backend/scanner/recipe_synth.py`
platform templates, `backend/scripts/synthesize_recipes.py`, the probes recorded in
`workspace/scan_lab/lab_20260923_http/NOTES.md`). Not a browser.

### 2. Scanning

In the scanning phase, verify that both **new and used** cars are being collected from
every dealership. Also verify the **location** of the car: dealerships often put other
dealerships' inventory on their own website. Keep a learning log of how each site
presents where the car is — in the image carousel, as text on the VDP, in a feed field —
in `workspace/dealer_logs/<dealer_id>/location.md` and roll the general patterns up into
`workspace/dealer_logs/_learning/location_patterns.md`.

For the general data that comes from the scan (drivetrain, fuel, engine, cylinders,
body, year/make/model), verify the data's accuracy with **NHTSA VIN decoding**
(`backend/enrichment/vpic_facts.py`, cache `nhtsa_vpic_cache`). If NHTSA does not have
it, use the internet to look it up (EPA fueleconomy.gov for the catalog, the
manufacturer's page for the model).

### 3. Summary and instructions

At the end of the scans for the dealership, create a summarizing log for that dealership
(`workspace/dealer_logs/<dealer_id>/summary.md`) that breaks down the issues faced and,
in detail, how each issue was fixed — written so Claude can build a proper learning
system to fix future discovering/scanning issues. Then move to the next dealership.

Once a successful run has been run, create:

- `workspace/dealer_logs/<dealer_id>/scan_instructions.md` — how to scan that
  dealership: platform, endpoints, pagination, pacing, headers, what the feed carries
  and what the detail page adds, known quirks;
- the recipe file for that dealership (`workspace/recipes/<dealer_id>.json`, and
  `workspace/recipes/vdp/<dealer_id>.json` when a per-car endpoint was learned) so the
  scanner scans it properly without discovery.

## Why the logs matter

It is **crucial** that we collect the details of errors through log files. That is how
we learn from our mistakes and build a trial-and-error system that solves issues
quickly. Write them so a future Claude session can pick them up without this
conversation: what was tried, what the site answered, what worked, what to try first
next time on the same platform.

## Log layout

```
workspace/dealer_logs/
  _learning/
    location_patterns.md      how sites present a car's location, by platform
    platform_playbook.md      what works per platform (endpoints, pagination, pacing, headers)
    errors_index.md           one line per distinct error class -> which dealer log shows the fix
  <dealer_id>/
    discovery.md              every discovery attempt: date, what was probed, what came back, verdict
    location.md               where this site says the car is, and how we read it
    scan_runs.md              one block per scan: rows new/used, coverage, NHTSA verification, discrepancies
    summary.md                issues faced and how each was fixed, in detail
    scan_instructions.md      how to scan this dealership (written after the first successful run)
```

## Verification checklist (the scan is not done until each line is answered)

1. Rows: total, new, used. Both present when the site sells both.
2. Location: which rooftop each row belongs to, and how that was determined.
3. Coverage before the upsert: price, trim, colours, engine, transmission, drivetrain,
   fuel, body, description, stock, MSRP, gallery.
4. NHTSA decode present for every VIN; drivetrain / electrification / cylinders /
   displacement stored from the decode where the feed disagreed; year/make/model agree.
5. Catalog link (EPA) present and consistent with the decode; missing model years pulled
   from the internet.
6. Discrepancies left, listed with examples, each classified: dealer error, our parser,
   dictionary gap, or genuine (e.g. "Call for price").
7. Logs written: discovery.md updated, scan_runs.md appended, summary.md and
   scan_instructions.md when the run succeeded, recipe files saved.

## Tools that implement the loop

- `backend/scripts/dealer_pipeline.py` — recipe → HTTP-only scan → NHTSA heal → assess
  (verification: new/used, location facts, incomplete fields, discrepancies) → per-dealer
  logs. Runs the probe below on every synthesis failure.
- `backend/scripts/discovery_probe.py` — the verbose discovery record for one dealership:
  redirect chain, status, size, title, challenge markers, every template's detect result,
  script hosts and API hints in the page, inventory-path probes, each synthesized
  candidate's page-1 replay (status, keys, rows, VINs, error). Appends to
  `discovery.md`, writes `discovery_<stamp>.json`, indexes the failure class in
  `_learning/errors_index.md`.
- `backend/scripts/scan_lab_report.py` — per-row incomplete fields and dictionary
  discrepancies with examples (the verification numbers).
- `.claude/workflows/dealer-discovery.js` — investigator / builder / verifier agents for
  the dealerships the pipeline could not scan or verify.
- `backend/scripts/heal_from_vpic.py`, `backend/scripts/backfill_vpic_cache.py` — NHTSA.
- `backend/scripts/import_epa_master.py --append-years` — catalog years from the internet.

## Browser policy (2026-09-26)

Scans are HTTP-only, always. A headless browser may run only inside discovery (`discovery_probe --browser-capture`) to learn a site's endpoint so a recipe can be written. The staged plan, the audit of every browser code path in the scanner, and the deletion list are in `docs/HTTP_ONLY_SCANS_PLAN.md`.
