# Server-side causes of the slow site (sarraficars.com, 2026-11-09 and 2026-11-10)

Topic: why pages are slow to open and to switch, seen from the server. It covers the gunicorn
setup, per-statement Postgres connections, writes and DDL on GET, cold caches after a deploy
and caches that expire while shoppers browse, the cost of building payloads, and anything on a
hot path that blocks. It also says which of these the staged releases 1.5.11-1.5.16 (branch tip
`phase/N37a`, `faea2b985`) already fix, and where Redis helps and where it does not.

Every number below comes from a measurement I ran, with its command, or is quoted from another
report and marked **[eval]** (`docs/structural_eval_2026_11_04/`), **[redis.md]**, **[live-speed.md]**,
**[R4]** (the R4 challenge logs, `docs/web_speed_search_2026_11/r4_challenge_raw/` and its
scratchpad twin `$SP/r4/out/`) or **[docs]** (a repo document). Numbers derived from measured parts
are marked **estimate** and show the arithmetic.

**Version note (round 3, 2026-11-10, evening Eastern).** The first version of this file came from two
live passes (2026-11-09 and 2026-11-10 UTC) and a local harness set. Round 3 re-ran the key harnesses on a
shared, busy laptop (load average 7-11, other jobs using 3-4 cores, so every local comparison below is
interleaved and repeated) and changed the report as follows.

* **Correction (4.2, 12).** The first version listed `price_plausibility` (1,521 ms) and
  `landing_featured_cars` (864 ms) as two separate reloads and summed them. They were one load counted
  twice: all the reloads on a fully cold process add up to about 1.7 s locally, not the 3.5 s the old rows summed to. The live spikes
  in 4.1 were measured on the live site and do not depend on this table.
* **More live evidence for the main finding (4.1).** The R4 logs add three more cold car pages: 3,014 ms
  (first after a quiet spell), 3,016 ms (328 s after the previous car page, same 10-minute bucket) and
  2,939 ms (first in the next bucket), each followed by a 242-800 ms page a few seconds later.
* **A fix prototype with numbers (4.6).** Serving the last value and reloading once in the background
  turns the first request after an expiry from 1.8 s into 0.06 s (one shopper) and from 3.4 s into 0.14 s
  (four shoppers).
* **What actually starves a request thread (4.7).** A long Python loop in a background thread stops the
  request completely for its whole duration; database load does not; the lower GIL switch interval does
  not help on this machine.
* **How often a visitor pays (4.8)** as a function of traffic, simulated from the code's TTL rules.
* **Checked and cleared (10):** no outbound HTTP on an anonymous GET; the static-file version stamp is
  computed once per process; garbage collection at 6.3 million tracked objects.
* **Fix status re-checked against the code diff (11).** One citation corrected (5.3).
* **Live requests this round:** one (`GET /health`, 2026-11-11 01:35:52 UTC: still release 1.5.8, commit
  `0fd91872b`, origin 75 ms). Budget used by this agent: 253 of 300 (252 from the first two passes, counter
  `$SP/live_budget.txt`, plus one, `$SP/sc3/live/live_budget.txt`).

## The short answer

A page view on production costs the server three things, in this order of size.

1. **A fixed ~79 ms before the app runs.** `/health` does no work (0.8 ms in the app locally) and
   takes 79 ms at the origin (median of 29 solo requests). The repo's own ops log puts the web
   service in Railway region `us-west2` (California) **[docs]**, while every request I made entered
   through an East Coast edge (Miami, Ashburn, Atlanta, New York). That is a cross-country hop on
   every uncached request, and nothing in the code can remove it.
2. **Per-statement database connections.** There is no pool. A car page opens 8-10 connections, a
   dealership page 4-6, the landing page 1. A connection plus `SELECT 1` costs 20-38 ms on
   production (medians of two measurement sets), against 1.4 ms locally. I estimate this is about
   155 ms of the 197 ms a car page costs above the fixed floor.
3. **Caches that expire and reload on a shopper's request.** On the live site the first car page
   after a 10-minute clock boundary cost **2.9-3.3 s** (against 0.27-0.31 s thirty seconds
   earlier), the first one after an 8-minute gap cost 1.9 s, the first one after a quiet spell
   cost 5.6 s (and 3.0 s three more times in [R4]'s probes), and the first search in a new area
   cost 4.4-13.4 s. Four concurrent shoppers at a boundary each run the same load (measured
   locally: the rarity loader ran 4 times and the four first requests took 2.1-4.6 s). A prototype
   that serves the last value and reloads once, in the background and with the database doing the
   counting, brought the first request after an expiry from 1.8 s to 0.06 s (one shopper) and
   from 3.4 s to 0.14 s (four shoppers) locally (4.6).

None of these three is fixed by releases 1.5.11-1.5.16. Releases from 1.5.11 on also change the
grid-card revision (`CARD_REV`), so deploying one makes every stored card stale until the card
builder is run (section 4.5). The staged releases fix the DDL-on-GET and the boot-time downloads;
they do not touch the pool, the TTL caches, the card aging, the worker count or the edge cache.

Redis helps with some of this (shared caches, shared rate limits, surviving deploys). It does not
fix the connection cost, the distance, or the CPU-bound card building (section 12).

---

## 1. How this was measured

**Three live passes, anonymous GET only, no cookies sent, no login, no forms, at least 1.1 s apart.**

| pass | when (UTC) | requests | context |
|---|---|---|---|
| first | 2026-11-09 13:40:20-14:05:41 | 151 | four other agents were probing the same site at the same time |
| second ("solo") | 2026-11-10 01:43:57-02:07:28 | 101 | no other agent probing; evening in the US (8:44-9:07 pm Eastern) |
| third (round 3) | 2026-11-11 01:35:52 | 1 | `GET /health` only, to confirm the live release before editing this file |

The counter in `$SP/live_budget.txt` stands at **252 of 300** for this agent across the first two passes, plus
the round-3 request counted in `$SP/sc3/live/live_budget.txt` (1), so 253 of 300. Six
of the second pass's requests were `GET /health` to the service's own Railway domain
(`web-production-26b11.up.railway.app`) instead of `sarraficars.com`, to separate Cloudflare from
Railway (section 3.1); all others went to `sarraficars.com`. The site served release 1.5.8,
commit `0fd91872b`, in all three passes (`/health` body; the round-3 check shows the same
`{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}`). Cloudflare adds
`server-timing: cfOrigin;dur=N` to every response: the time Cloudflare waited for the Railway
origin. That is the server-side number used throughout. `/health` (no SQL) is the baseline.

**Correction to the first pass.** The first pass ran at the same time as four other agents
(live-speed about 142 requests, phone-spacing 63, search 18, redis 15 **[their reports]**). Merging
every agent's request times (`server-causes-raw/r2/live/timeline.py`, `contam.py`) shows that 41 of
my 151 requests were being processed within 1 s of another agent's dynamic request. The steady-state medians do not
change when those are dropped (`analysis_out.txt`: `/health` 90 vs 90, car 317 vs 311, `/listings`
239 vs 240, paired connection cost 20 vs 22 ms). But two of the first pass's cold observations were
not clean and are replaced here by solo, controlled experiments: the "1,962 ms car page after
7 minutes idle" began in the same second as another agent's `GET /api/listings/geo-coords`, which
triggered the registry backfill **[redis.md, row 12]**, and the "2,739 ms first landing" was seen at
the same moment by two agents. Section 4.1 redoes both.

**Local.** Apple Silicon Mac, Python 3.14.7, gunicorn 26.0.0, Homebrew Postgres 17.10 on the `/tmp`
socket, database `cars` at migration V025 (357,149 cars, 214,675 active, 214,679
`listings_grid_cards` rows). The machine was on battery with load average 4-7 during the first two rounds
and 7-11 during round 3 (other agents' jobs: a Python script and a Postgres restore each at 100% of a
core, a Chromium), so absolute local numbers are pessimistic and noisy (the same car page read 40 ms at rest and
114 ms in one warm-up); I rely on interleaved A/B comparisons and repeats. Every Postgres session ran with
`PGOPTIONS='-c default_transaction_read_only=on'`: the app's own GET-time writes were refused and
counted, and nothing was written to the database. The users and admin SQLite files were scratch
copies. The code measured is the repo checkout, which is `main` = the live release 1.5.8. Claims
about the staged releases come from `git show phase/N37a:<path>` and `git diff`, not from running
that code: the local database is at V025 and the staged code needs V026-V029.

**Scripts and raw outputs** are in `server-causes-raw/` next to this file. `server-causes-raw/live`
and `server-causes-raw/local` are the first pass, `server-causes-raw/r2/` is the second, and
`server-causes-raw/r3/` is round 3 (scripts, outputs in `out/`, one live record in `live/`). The
scripts hard-code the session scratchpad
`scratchpad/`,
shortened to `$SP` below.

**Screenshots:** none. This topic is server-side; the evidence is response headers, timings, SQL
counts and profiles.

## 2. Ranked findings

Rank is by how much waiting it adds for a visitor and how often. "Fixed" means fixed by 1.5.11-1.5.16.

| rank | id | cause (server side) | key measurement | fixed? | fix | effort |
|---|---|---|---|---|---|---|
| 1 | S2 | Fleet-wide caches reload inline on a shopper's request when a short TTL ends, and several threads repeat the load | Live car page: **2,896 ms** right after the 01:50:00 boundary, 272 ms 30 s before; **3,335 ms** after 02:00:00; **1,923 ms** after an 8-min gap; **5,625 ms** first after a quiet spell; [R4] 3,014 / 3,016 / 2,939 ms for three more. Local: first request after an expiry **1.8 s**, four shoppers **3.4 s** (rarity loader ran 4 times); with serve-stale and one background reload **0.06 s** and **0.14 s** | No | Compute off the request (timer or after scans), serve stale while refreshing, one load at a time, count in SQL, warm at start. Prototype measured in 4.6 | S-M |
| 2 | S1 | First search in an area re-serializes every stored card in the web process, and the browser refetches the whole body until it finishes | Live: Irvine **13,447 -> 4,439 -> 2,887 -> 1,287 ms**; Dallas **4,383 -> 81 -> 92 -> 1,295 ms**. Local: 20,888 cards queued at 0.99 ms = **20.8 s** of CPU. 1.5.11+ also changes `CARD_REV` | No (makes it worse at deploy) | Build cards off the web process; stop day-aging; run the builder after every deploy that changes `CARD_REV` | M |
| 3 | S4 | A new Postgres connection for every statement; no pool anywhere | Live: **20-38 ms** per connection + `SELECT 1`. Car page 8-10 connections, 197 ms above floor (estimate: ~155 ms is connections). Local, interleaved: car page **52.6 -> 17.9 ms** pooled | No (N37a adds one more per car page) | `psycopg_pool` behind `pg_connect`; drop repeated `dealerships` reads | S-M |
| 4 | S10 | Every uncached request crosses the country: web service in `us-west2`, visitors on the East Coast | `/health`: **79 ms** at origin vs 0.8 ms in the app; same 77-86 ms through three East Coast edges; direct Railway domain 135 ms to first byte vs 145 ms through Cloudflare | No (not a code issue) | Move web and Postgres together to an East US region, after the pool | M |
| 5 | S9 | `/listings` is 1.4-1.55 MB of HTML built per view, and its cost on production is far above local | Live **202-422 ms** (13 solo samples, two clusters) vs 39 ms local with 0 SQL; scope JSON 14-21 MB raw per area | No | Cache the rendered shell per token; slimmer cards, first page first; add `Server-Timing` to find the gap | M |
| 6 | S3 | One gunicorn worker with 4 threads: a CPU-bound job stalls every other request | Local 1 worker: car page p50 **40 -> 420 ms** at 16 users, caps at 33-36 req/s; 4 workers 141 req/s, p50 111 ms. Car page **40 -> 126-277 ms** (one run; up to 171 ms in another) while the card refresh thread runs. A 700 ms pure-Python loop in a background thread: the one overlapping car request took **710-902 ms** in 8 of 8 runs (about 45 ms idle); a database-heavy background job: no effect (4.7) | No (1.5.11 throttles facet rebuilds; 1.5.16 makes limits per worker) | Remove the CPU-bound jobs from the web process first (or make them yield / count in SQL); then 2-3 workers with shared caches and limits | M-L |
| 7 | S5 | The edge cache is effectively off | Every HTML page sets `set-cookie: session` with `Vary: Cookie`; every response is `cf-cache-status: DYNAMIC`, scope JSON included | No | Cache rule for the listings JSON; no session for anonymous GET | S |
| 8 | S8 | Deploys go live cold; `/health` passes before any cache is warm | Local: `/health` 200 after 1.4-2.0 s, then first hits `/` **895 ms**, `/listings` **2,967 ms**, car **1,335 ms**, Irvine scope **2,552 ms** | Partly (downloads baked; backfill moved) | Warm in `post_fork`; readiness route; builder step in the release checklist | S-M |
| 9 | S6 | Signed-in visitors pay for SQLCipher key derivation and a sticker POST per car page | **50 ms** per users-store open (kdf 256,000); car page 2 opens = 97-99 ms. `POST packages/ensure` holds a thread up to 6 s | No | Reuse one connection per thread, or finish users -> Postgres (N37.10) | S |
| 10 | S7 | Writes and DDL triggered by GET | 74 refused card-store write batches in 61,369 local GETs; 3 DDL per dealership view on 1.5.8 | DDL yes (1.5.10); backfill moved (1.5.11) | Card writes off-request (S1) | S |
| 11 | S11 | The server reports no timing of its own, so proxy, app and database time cannot be separated | `/listings` gap (3-9x local) and the 20-38 ms connection cost could not be decomposed from outside | No | `Server-Timing: app;dur=,db;dur=,dbn=` on every response | S |
| 12 | S12 | A long Python loop with no sleeps, in any background thread of the web process (the rarity reload, the facet rebuild, any new refresher), stops every request thread for its whole length. This is the mechanism behind part of S1, S2 and S3 | A 700 ms pure-Python loop: the one overlapping car request took **710-902 ms** in 8 of 8 runs (45-50 ms idle), whichever switch interval; a database-heavy job: p50 65 ms; the shipped card refresher (2 ms sleep per 100 cards): 2.5x p50 but never stopped; facet rebuild: one request of 508-921 ms per rebuild (4.7) | Partly (1.5.11 allows one facet rebuild per 300 s) | Count in SQL, yield often, or run refreshers in another process | S |

Ruled out by measurement (section 10, 4.7): garbage collection (also at a 6 M-object heap on 3.12),
database load as the cause of request stalls, a C call holding the interpreter (`fetchall` 27 ms), outbound
HTTP on an anonymous GET, the Python version (prod 3.12 vs local
3.14), Cloudflare as the source of the fixed cost, and boot-time bcrypt as a visitor-facing cost.

---

## 3. Where one request's time goes (live, solo, steady state)

Source: `server-causes-raw/r2/live/analysis_out.txt` (`python3 $R2/live/analysis.py`), the A2 and
A3 phases of `live2_run.jsonl`, `live3_run.jsonl` and `live4_run.jsonl`. "Origin" is `cfOrigin`.
Medians; n in brackets. Local figures are the third request after a cold start through gunicorn
(`cold_seq_w1_r2.jsonl`), or p50 at 1 user (`sweep_w1t4_r2.jsonl`).

| request | origin ms | above `/health` | local ms | connections per view (local) | reading |
|---|---|---|---|---|---|
| `/health` | **79** (29; IQR 77-82) | 0 | 0.8 | 0 | the fixed cost |
| `/api/ready` | 119 (18) | +40 | 5.7 | 1 | one connect + `SELECT 1` |
| `/` landing | 120 (3) | +41 | 4.5 | 1 | one connection and ~4 ms of work |
| `/api/listings/filter-options` | 91 (3) | +12 | 6.9 | 0 | pure CPU: 12 vs 6.9 ms = 1.7x local |
| `/dealership/<key>` | 199 (3) | +120 | 26.8 | 4-6 | ~90 ms connections (estimate) |
| `/car/2002740` | 276 (3) | +197 | 40-46 | 8-10 | ~155 ms connections (estimate) |
| `/listings` | 342 (13; range 202-422) | +123 to +343 | 39.3 | 0 | 0 SQL: the gap is not connections |

The estimates use the pure-CPU calibration (filter-options, 1.7x) and the local connection cost
(15.6 ms for 8 statements on 8 fresh connections, `conn_cost2_r2.json`): car page CPU part =
(40 - 15.6) x 1.7 = 42 ms, so connections = 197 - 42 = 155 ms over 8-10 connections = 15-19 ms
each. Landing: connection = 41 - 5 = 36 ms for the single one. One connection on its own costs more
than one inside a burst, which fits the isolated pairs below.

The first-pass shared window gave the same shape: `/health` 90 (through Miami edges), car 311-317,
`/listings` 239-240, landing 120, dealership 205-216, filter-options 114-119 (clean-only and all).

### 3.1 The fixed cost and the region

* The web service's region is recorded in the repo's ops log: `replicas | 1 (manifest; instance
  numReplicas null; us-west2 x1)` for web **[docs]**
  (`git show phase/N37a:docs/SCANNING_OPS_LOG.md`, line 58, snapshot of 2026-10-05; line 949
  repeats it as `multiRegionConfig us-west2 x1`). I could not read the live setting. `us-west2`
  is Railway's California region.
* `/health` at origin by the Railway edge that served it (`x-railway-edge`, solo run):
  `iad1` 80.5 ms (n=14), `atl1` 77.0 (n=13), `jfk1` 85.5 (n=2). Three East Coast edges are within
  9 ms of each other, so the ~79 ms is the leg from any East Coast edge to the app, not the edge.
  In the first pass every edge was `mia1` and the median was 90 ms.
* Direct to the Railway-assigned domain (no Cloudflare), 6 requests to `/health`
  (`web-production-26b11.up.railway.app`, listed as enabled in the ops log **[docs]**): TCP connect
  to the edge 35 ms median, then **135 ms** from request sent to first byte (118-264). Through
  Cloudflare, 8 requests: connect 32 ms, **145 ms** to first byte (123-204). Cloudflare adds about
  10 ms; the rest is the Railway edge to the app. `server: railway-hikari`, `x-railway-edge: iad1`.
* The per-connection numbers imply the database is in or near the app's region (inference). A
  connection costs 20-38 ms, not the >400 ms a cross-country TCP + TLS + SCRAM handshake would
  take (7+ round trips at ~65 ms). So moving only the web service east would make every
  connection cross the country: web and Postgres must move together, and the pool (S4) should
  come first. The Postgres region itself was not read; the ops log table gives no region for it.
* **Estimate:** moving both to an East US region would cut the 79 ms floor to roughly 20-30 ms
  (the geographic round trip, ~50-60 ms, goes away; handshake and proxy overhead stays). That is
  a guess from geography, not a measurement.

Commands (solo run):

```
python3 $SP/server2/live2.py $SP/server2/live2_run.jsonl 2026-11-10T01:50:00      # 61 requests, phases A1 A2 C B L A3 D
python3 $SP/server2/live3.py $SP/server2/live3_run.jsonl                          # 20 requests, /health and /listings alternating
python3 $SP/server2/live4.py $SP/server2/live4_run.jsonl 10                       # 20 requests, 10 /health + /api/ready pairs
python3 $SP/server2/analysis.py                                                   # tables above
```

### 3.2 Cost of one connection on production

`/api/ready` opens exactly one inventory connection and runs `SELECT 1`
(`routes/health.py` `_check_inventory_db`; the live body is
`{"checks":{"db":"ok","pgvector":"skipped","redis":"skipped"},"ready":true}`). Paired against
`/health`, origin ms:

| set | pairs | median difference | quartiles | range |
|---|---|---|---|---|
| first pass, day, shared window (ABBA order) | 25 | **20** | | |
| first pass, clean-only | 16 | **22** | | |
| solo, evening, set 1 | 8 | **51.5** | | 9-62 |
| solo, evening, set 2 | 10 | **30** | | 9-48 |
| solo, evening, both | 18 | **38** | 28-49.5 | 9-62 |
| [R4], evening, adjacent pairs (`/health` then `/api/ready` one second later, 02:30 and 02:40 UTC) | 2 | +30 and +51 | | |

So one isolated connection costs about 20-38 ms on production (the medians of the larger sets; the first
evening set alone read 51.5 ms and R4's two pairs +30 and +51 ms). Inside the car page, eight to ten
of them fit in ~155 ms (15-19 ms each, estimate above). The local figures are 1.3-1.5 ms
(`conn_cost2_r2.json`).

---

## 4. Caches that reload on a shopper's request (S2) and the first search (S1)

### 4.1 The clock-aligned experiment (live, solo)

`rarity_score.get_inventory_counts()` is `lru_cache`d on `int(time.time() // 600)`: a wall-clock
bucket that flips at every `xx:x0:00` UTC. Reloading it is a pass over all 214,547 active cars
(one SELECT, then a Python loop with a paint-family lookup per row). `price_plausibility`, the
deal-score bands, the landing's featured cars and the dealership-URL fallback each use a 300 s TTL
(`listings_repo._FEATURED_CARS_TTL_S`, `grid_cards_repo._FALLBACK_TTL_S`, ...). The prediction from the code:
the first car page after a boundary pays the rarity reload, and the first after 300 s pays the
others. I tested it on one car page (`/car/2002740`), one request every 15-60 s across two
boundaries (`live2.py` phases B and L; log `live2_run.log`):

| time (UTC) | request | origin ms | what it is |
|---|---|---|---|
| 01:48:20 | car, first after a quiet spell (nobody had loaded a car page since at least 01:40) | **5,625** | everything cold |
| 01:49:00 / 01:49:25 / 01:49:45 | car | 296 / 375 / 272 | warm |
| **01:50:15** | car, 15 s after the 01:50:00 boundary | **2,896** | bucket flipped (rarity only; the 300 s caches were 115 s old) |
| 01:50:35 / 01:51:00 | car | 264 / 280 | warm |
| 01:59:00 | car, 8 min since the last car, same bucket | **1,923** | 300 s caches expired (rarity still warm) |
| 01:59:25 / 01:59:45 | car | 269 / 313 | warm |
| **02:00:15** | car, 15 s after the 02:00:00 boundary | **3,335** | bucket flipped |
| 02:00:35 / 02:01:00 | car | 299 / 314 | warm |
| 01:51:40 / 01:51:45 | landing | 161 / 120 | warm (first landing at 01:43:58 was 130 ms) |
| 02:01:30 | landing, 590 s after its last load | **438** | its 300 s caches expired |

All four predicted spikes appeared, each 6 to 20 times a warm request: **+2.6 s** and **+3.1 s**
at the two boundaries, **+1.65 s** for a 300 s expiry, **+5.4 s** for all caches cold.
The first pass saw the same shape: a car page 30 s after the 13:50:00 boundary took 3,382 ms
**[live-speed.md]**, and my first car page at 13:41:20, 80 s after a boundary, took 4,874 ms.

**Independent repeat [R4]** (another agent, anonymous GETs, `server-causes-raw/r3/r4_live_exp.jsonl`,
analysis by `r4_analyze_live.py`; the pairs are two different car pages a few seconds apart):

| time (UTC, 2026-11-10) | request | origin ms | what it is |
|---|---|---|---|
| 02:30:08 / 02:30:09 | `/health`, `/api/ready` (controls) | 97 / 127 | no caches involved |
| 02:30:10 | car, first after a quiet spell | **3,014** | rarity bucket flipped at 02:30:00 |
| 02:30:15 | a second car | 242 | warm |
| 02:35:38 | car, 328 s after the previous car, same bucket | **3,016** | 300 s caches expired (rarity warm) |
| 02:35:42 | a second car | 800 | warm, slow |
| 02:40:08 | car, first in the new bucket (270 s after the previous reload) | **2,939** | bucket flipped (300 s caches still valid) |
| 02:40:17 | a second car | 279 | warm |

Reading both sets together: a bucket flip alone cost 2,896, 3,335 and 2,939 ms live (+2.6 to +3.1 s over a
warm car page) and a 300 s expiry alone 1,923 and 3,016 ms (+1.6 to +2.8 s), against 0.7-0.8 s each locally
(4.2): the live reload is about 2 to 4 times slower than on this laptop (production runs the same SQL on
Railway's Postgres and the same loop on Railway's CPU; I cannot split the difference further). The R4 probes at 03:21-03:26 UTC saw 500-720 ms for
warm car pages (my warm figure is 270-310 ms), so the origin was slower for those minutes for a reason I
cannot see.

How often a real visitor hits one depends on traffic, which I cannot see: the first landing page
this evening was warm (130 ms), so someone had loaded it within 5 minutes, while every car page probe that
came after a gap of five minutes or more found cold caches: 5 of 5 (13:41 UTC on 2026-11-09, then 01:48,
01:59, 02:30 and 02:35 UTC on 2026-11-10). That is five observations, not a traffic estimate. Section 4.8
turns the TTL rules into a share of visitors as a function of traffic.

### 4.2 What each reload costs locally (corrected in round 3)

**Correction.** The first version of this table came from `ttl_loaders.py`, which called the private
`price_plausibility._load()` (it returns the tables but does not fill the module cache; `_ensure_loaded()`
does) and then `landing_featured_cars()`. Serialising the first featured card calls `implausible_price()`,
which runs `_ensure_loaded()`, which loads again. So the "864 ms" for the featured cars was a second
plausibility load, and the featured query itself costs about 8 ms. The corrected figures, from a warm
process with each loader forced to reload five times (`h_warm_loaders.py`, `out/warm_loaders_r3.log`;
median, [min-max]; run on the busy laptop):

| cache (module) | TTL | reload, local |
|---|---|---|
| `rarity_score._load_inventory_counts` (one SELECT of 214,546 rows, then a Python loop; no lock, so every concurrent caller runs it) | 600 s clock bucket | **711 ms** [702-1,210] |
| `price_plausibility._load` (2x `percentile_cont` over all active cars; the module lock is held while it loads) | 300 s | **775 ms** [739-821] (813-1,814 ms in earlier runs; it follows Postgres' cache state) |
| `grid_cards_repo._fallback_dealer_urls` | 300 s | 147 ms [141-186] |
| `listings_repo._count_public_listings_uncached` | 300 s | 52 ms [51-56] |
| `deal_score_cache._load_bands` (4,188 bands; lock) | 300 s | 7.6 ms [7.2-7.9] |
| `landing_featured_cars`, plausibility cache warm | 300 s | **7.5 ms** [7.2-8.4] |
| `landing_featured_cars`, plausibility cache expired | | 741 ms [734-755]: the plausibility load, run inside it |

Proof of the double count (`h_double_count.py`, two runs per mode, `out/double_count_rerun.log`). "As
harness" calls `pp._load()` in the plausibility step, as the old table did; "real path" calls
`pp._ensure_loaded()`, as a web request does.

| mode | plausibility step | featured step | `pp._load` calls |
|---|---|---|---|
| as harness | 754.8 / 1,519.6 ms | 887.9 / 803.7 ms | 2 (the second inside the featured step: 812.3 / 771.5 ms) |
| real path | 777.3 / 793.5 ms | 33.7 / 31.9 ms | 1 |

What a page pays after each event (`h_carpage_ttl.py`, Flask test client, three runs per row,
`out/carpage_ttl_r3.log`; the loaders are wrapped to record their own time):

| event before the request | car page | loaders it ran |
|---|---|---|
| rarity bucket flipped | 759 / 787 / 826 ms | rarity 680-699 ms |
| 300 s expiry (plausibility) | 805 / 840 / 862 ms | plausibility 733-748 ms |
| both | 1,470 / 1,526 / 1,724 ms | plausibility 725-735 ms + rarity 672-694 ms |
| deal-score bands only | 62 / 78 / 87 ms | bands 6.2-6.6 ms |

and for the landing page: warm 4-5 ms; public count and featured cars expired with plausibility warm
63-71 ms; with plausibility expired too 793-860 ms (count 52-55 ms + plausibility 725-791 ms). So the car
page needs two reloads (rarity, plausibility) plus the bands, the landing needs plausibility through its
featured cars, and **everything reloading together costs about 711 + 775 + 147 + 52 + 8 = 1.7 s**,
not the 3.5 s the old table's rows summed to (1,521 + 864 + 758 + 190 + 91 + 77). The two to four times larger
live figures in 4.1 therefore point at the live database and CPU (or at extra work I did not see), not at
extra loaders on the car page. Two smaller loaders from the first table are left as they were because they
were not re-measured: `_attribution_read` (60 s TTL) 59 ms and `grid_scope_token` (60 s resample) 11 ms.

A process that was warmed and left idle (`python $SP/server/ttl_probe.py`, Flask test client,
second run `ttl_probe_r2.json`): first request after 70 s idle: `/` 9.9 ms, car 81.6, dealership
**370** (its own 30 s card cache, `_DEALER_CARDS_TTL_S`), `/listings` 132 (40 ms warm; cause not
isolated); first after 310 s idle: `/` **1,010 ms** (`_landing_featured_cars` = 936 ms of it, which is
the plausibility reload running inside it, see the correction above), car
**1,570** (profiled), dealership 121, `/listings` 132; the repeat request after each was 5-183 ms.

### 4.3 The herd

The rarity cache has no lock. Four threads calling it while the bucket is cold (`herd.py`, local,
two runs in round 2):

| threads | loader runs | per-thread wait |
|---|---|---|
| 1 | 1 | 847-872 ms |
| 2 | 2 | 1,240-1,407 ms |
| 4 | **4** | 1,214-2,503 ms; wall 2.4-2.5 s |

Round 3 repeat (`h_herd.py`, `out/herd_r3.json`): rarity alone 717 ms; four threads at once: the loader ran
**4 times**, per-thread waits 1,981 / 2,280 / 2,400 / 2,433 ms. Plausibility alone 788 ms; four threads at
once: the loader ran **once** and all four waited 751 ms (they queue on its lock). The same thing in the
whole request path (4 request threads on car pages, 5 interleaved repetitions, `swr_demo_r3c.json`): the
baseline arm's rarity loader ran 4 times, the slowest of the four first requests took **3.3-4.0 s**, and
the 3.5 s window completed 8 requests where the control window without an expiry completed 127 (4.6).

At a boundary, concurrent car pages each run the whole load (and each fetches 214k rows), so the
wait grows with the number of shoppers instead of staying at one load. `price_plausibility` and
the bands hold a lock, so they queue instead (same total wait, one load of work).

### 4.4 The first search in an area (S1)

`GET /api/listings/cars?zip=&radius=` reads stored cards from `listings_grid_cards`
(`grid_cards_repo`). A card is served but queued for background re-serialization when it is
**aged** (`_is_aged`: `built_day < today - CARD_MAX_AGE_DAYS`, with `CARD_MAX_AGE_DAYS = 1`, or
`day_dep` and built on another day) or **changed** (its `aux_key`, a hash that includes
`CARD_REV`, the attribution state and the market-stats generation, or its content differs). Any
queued card marks the response `partial` (`Cache-Control: public,
no-cache`, `X-Listings-Partial: 1`). The client refetches after 3, 6, 12, 24, 30 and 30 s
(`listings/zip_state.js` `partialRefetchDelayMs`); each refetch pays SQL, concat and gzip again
once the background thread moves `store_generation()`.

Live (anonymous GET, client schedule 0/+3/+6/+12 s, `X-Listings-Partial: 1` on every response):

| area | request 1 | 2 | 3 | 4 | body |
|---|---|---|---|---|---|
| Irvine 92618 / 25 mi (first pass) | **13,447** | 4,439 | 2,887 | 1,287 | 18,933 cars, 20.9 MB raw, 2.0 MB gz |
| Dallas 75201 / 25 mi (solo) | **4,383** | 81 | 92 | **1,295** | 857-862 KB gz |

The Irvine second sample overlapped another agent's 28105/50 build for about a second (flagged in
`contam_out.txt`); the first and third were clean. For Dallas, requests 2 and 3 were served from
the in-process LRU (the partial entry, same ETag), and request 4 rebuilt after the refresh moved
the generation (new ETag, 5.9 KB larger body).

Local breakdown (`python $SP/server/scope_breakdown.py 92618 25 28105 50`, `scope_breakdown_r2.jsonl`):

| step | 92618 / 25 mi | 28105 / 50 mi |
|---|---|---|
| candidate SQL (bbox join with the card store) | 584 ms, 27,516 rows, **33,975,516 B of `card_json`** | 174 ms, 10,397 rows, 11,295,395 B |
| `cards_near` (SQL, fallback URLs, resolve) | 967 ms; 20,888 cards, all changed and deferred, `partial` | 108 ms; all fresh but aged, queued |
| body concat / gzip level 6 / ETag | 6.4 ms / **167 ms** / 2.2 ms -> 27,308,154 B raw, 2,972,095 B gz | 3.0 / 74 / 0.9 ms -> 11,531,045 B raw, 1,247,954 B gz |
| background re-serialization queued | 20,888 x 0.994 ms = **20.8 s** CPU | 10,397 x 0.481 ms = 5.0 s |

Through gunicorn on a cold start the same Irvine request took 2,552 ms to first byte
(`scope_seq_w1_r2.jsonl`; wire 2,972,095 B, 20,888 cards, partial).

**Contention while the refresh runs.** The refresh thread is CPU-bound and shares the GIL
(`_BG_YIELD_ROWS = 100`, `_BG_YIELD_S = 0.002`: it sleeps 2 ms every 100 cards). 
`refresh_thread_probe.py` (local, two runs): car page 38-43 ms alone; after one dealership view
the `grid-cards-refresh` thread was alive for 12+ s. Run 1: the next car views took 126, 221, 233,
133 ms, and 277 / 222 / 78 ms twelve seconds later. Run 2 (`refresh_thread_probe_r1.txt`): 43, 43,
40, **171** ms, then 63 / 48 / 38 ms. `interleave_probe.py` (two runs): car p50 42-45 ms alone,
**89-123 ms** when each view follows a dealership view, 43-44 ms when it follows another car.
The local database is read-only, so the refresh never persists and is re-queued by every
dealership view; on production a refresh persists and does not repeat until the cards age again.
The mechanism is real (it is the same thread that took 5-21 s of CPU for a metro), but the
repeated cost is exaggerated here. In the first pass the live car page took 770 ms during the
Irvine refresh against ~313 steady (n = 1, shared window).

### 4.5 The card revision changes in 1.5.11, 1.5.12 and 1.5.16

`CARD_REV` is a hash of 12 serializer source files (`_REV_SOURCES`). Any difference makes every
stored card "changed". I computed it for each revision without checking anything out
(`python card_rev.py`, which reproduces the real value for `main`: 280f0b3088c53fd9):

| release | `CARD_REV` |
|---|---|
| 1.5.8 (live), 1.5.9, 1.5.10 | `280f0b3088c53fd9` |
| 1.5.11 (`phase/N6b`) | `950f2e4872e28704` (the pending flag in `listings_repo.serialize_car_for_listings_grid`) |
| 1.5.12 (`phase/AF`) | `e0efb64b90da865e` (`utils/interior_color_buckets.py`) |
| 1.5.16 (`phase/N37a`) | `db7ce599699b6d8f` (`utils/market_price.py`, changed between 1.5.12 and 1.5.16) |

(Which input file changed at each step: `card_rev.py` compared the 12 `_REV_SOURCES` between
`main` -> `phase/N6b` -> `phase/AF` -> `phase/N37a`.) `CARD_REV` is folded into every card's
`aux_key` (`blake2b` of `CARD_REV`, the incomplete flag, the attribution state, and the market-stats
generation), and `_resolve` calls a card fresh only if `row_ver` and `aux_key` both match. So a
deploy of 1.5.11 or later turns the whole card store stale: each area's first search then
behaves like Irvine above (a full-row fetch for every card, stale cards served, all queued for
re-serialization in the web process, `partial`, up to 6 refetches) until the cards are rebuilt.
The same happens after every market-stats recompute, which the scanner's post step follows with
the builder. The builder is `python -m backend.scripts.build_listings_grid_cards`. It took 149.5 s,
151.6 s and 171 s (268,868 active cars) as the post step on Railway **[docs]**
(`docs/RAILWAY_SCANNING.md` lines 382 and 551), about 0.6 ms per card. `docs/RELEASING.md` does not
mention it (`git grep build_listings_grid_cards phase/N37a -- docs/RELEASING.md` is empty).

Local state for scale (read-only queries): of the 214,679 cards, 11,364 were built on 2026-11-04
(day 20761) and the other 203,315 between 2026-09-28 and 10-09 (176,485; 6,605; 20,225). The
28105/50 area's 10,397 cards are fresh (their key matches the current one) and the Irvine area's
20,888 are all changed (`resolve_stats` in `scope_breakdown_r2.jsonl`). Today is day 20767, so
even the newest cards are aged (6 days).

**The UTC day roll.** `_today()` is `int(time.time() // 86400)`, and it is part of the scope cache token and
of the card age test (`_is_aged`), so at 00:00 UTC (19:00 Eastern in winter, 20:00 in summer) every cached
scope body is invalid, every card whose output depends on the day (`day_dep`, the "price dropped N days ago"
label) built before the new day counts as aged at once, and every other card one day later. The first
shopper in each area after the roll pays the rebuild: 169 ms (warm process) to 463 ms (cold process)
locally when the cards are current, and the S1 cost when they are not (`$SP/challenge_dayroll/sim_28105_r50.log`,
scenarios S1 and S2 against S3 and S4, a simulation I read but did not run). This is measured live and
analysed in **[redis.md]** (sections 3.1 and 3.7, rows 16-18). It matters for reading the live numbers: my solo
pass ran 20:43-21:07 Eastern, after the roll.

### 4.6 A prototype of the fix: serve the last value, reload once, in the background

**What was tested.** A monkeypatch inside the test process (no repository file changed) replaces
`rarity_score.get_inventory_counts` and `price_plausibility._ensure_loaded` with a holder that loads inline
only the very first time. After an expiry it returns the last value at once and starts **one** background
thread to reload, then swaps the new value in. That is single-flight plus stale-while-revalidate, the same
pattern the card store already uses for cards. Harness: `r3_swr_demo.py` (appendix). The Flask test client
requests `/car/<id>` back to back from 1 or 4 request threads (the web worker has 4) for 3.5 s after an
expiry of both caches; the arms are interleaved in every repetition and their order rotates; a control
window with no expiry sits in every repetition. Postgres is read-only, the card writers are stubbed, and
the machine was shared (control p50 41-45 ms with one thread, 105-117 ms with four).

Arms: **baseline** (as shipped: the rarity `lru_cache` is cleared, the plausibility stamp zeroed, so the first
caller reloads inline); **swr** (the shipped loaders reload in the background thread); **swr_si** (the same
with the interpreter's switch interval lowered from 5 ms to 0.5 ms); **swr_grp** (the rarity counts come from a
`GROUP BY make, model, trim, exterior_color` and a Python loop over the 52,119 grouped rows, 476 ms under load
against 895 ms for the shipped loop); **swr_yield** (the shipped loop with a 1 ms sleep every 5,000 rows).
The last two return exactly the shipped loader's counters (checked, `loader_equal_to_shipped` in the JSON).

Medians over the repetitions (ms; "slowest first" is the largest first-request latency among the threads):

| arm | 1 thread: first request | worst request | requests done in 3.5 s | 4 threads: slowest first | worst request | requests done |
|---|---|---|---|---|---|---|
| control, no expiry | 56 | 243 | 62 | 151 | 202 | 127 |
| **baseline, as shipped** | **1,829** | 1,829 | 28 | **3,418** | 3,418 | **8** |
| swr | 45 | 597 | 55 | 156 | 730 | 101 |
| swr_grp | 55 | **234** | 58 | 143 | **335** | **113** |
| swr_yield | 41 | 343 | 55 | 163 | 756 | 99 |
| swr_si (separate run) | 56 | 548 | 58 | 162 | 781 | 90 |

(`swr_demo_r3c.json`, 5 repetitions: control, baseline, swr, swr_grp, swr_yield. `swr_demo_r3b.json`, 6
repetitions, has swr_si next to its own baseline and swr: baseline 1,566 / 3,467, swr worst 597 / 724, control
x4 requests done 117. `swr_demo_r3.json`, 5 repetitions, the first run: baseline 1,577 / 3,414, swr 41 / 150.)

What it shows:

* With serve-stale and a single flight, the first request after an expiry is as fast as any other: 37-93 ms
  with one thread (1.5 to 2.6 s as shipped) and 132-201 ms with four (3.3 to 4.6 s as shipped; the control's
  first request is 151 ms because four threads share the interpreter). This held in all 32 swr event
  windows (5 + 6 + 5 repetitions, 1 and 4 threads) of the three runs.
* The herd goes away: one rarity run and one plausibility run per expiry instead of four rarity runs.
* The shipped site as measured here nearly stops for the length of the reload: 8 requests finish in the
  window instead of 127. With the prototype 99-113 finish.
* It is not free: one request in the window is slow (597 ms with one thread, 730 ms with four) because the
  reload's Python loop keeps the interpreter for most of its 0.9 s (4.7). Counting in SQL makes that loop
  short (swr_grp: worst request 234 / 335 ms, about the control's own worst). A periodic sleep did less
  (swr_yield: 343 / 756 ms), and the lower switch interval did nothing (swr_si: 548 / 781 ms).
* Not covered here: the first load after a restart still happens inline unless a warm-up runs before the
  health check passes (S8); the live reload would run two to four times longer (4.1), so the background
  thread would hold the process for 2-3 s; a failed refresh must keep serving the old value. A refresher in
  another process (the Redis design in **[redis.md]**) removes the thread, and with it the one slow request.

### 4.7 What stalls a request thread: a long Python loop, not the database

Round 2 showed a car page slowing while a background thread worked (4.4) and the prototype above leaves one
request slow. This isolates the cause. `r3_refresh_convoy.py`: one foreground thread requests car pages
through the Flask test client; a background thread runs one job at a time; arms are interleaved and rotated
(4 repetitions, 5 for the facet arm; Postgres read-only, card writer stubbed).

| background job in a thread | job wall time | foreground requests that overlapped it | their p50 | worst of them |
|---|---|---|---|---|
| none (control, 2 s windows) | | 26-34 | 52 / 44 ms | 225 / 132 ms |
| a pure-Python busy loop, **no database**, 700 ms; switch interval 5 ms (default) | 700 ms | **1** (4 of 4 runs) | 769 ms | **721-902 ms** |
| the same loop, switch interval 0.5 ms | 700 ms | **1** (4 of 4 runs) | 723 ms | 710-757 ms |
| `price_plausibility._load()`: two `percentile_cont` queries, database-heavy | 1,020 ms | 19 | 65 ms | 85-164 ms |
| the shipped card refresher: `refresh_cards(2,000 ids, yield_gil=True)`, interval 5 ms | 489 ms | 4 | 130 ms | 167-303 ms |
| the same, interval 0.5 ms | 437 ms | 4 | 120 ms | 162-214 ms |
| `_build_filter_options_uncached()` (what `facet-options-refresh` runs) | 2,656 ms | 28 | 48 ms | **508-921 ms** (one request per rebuild) |

(`out/refresh_convoy_r3.json` and `out/refresh_convoy_r3b.json`; the first run's control p50 is 52 ms and the
facet run's 44 ms, both from their own idle arms.)

* A 700 ms Python loop in a second thread **stopped the request thread for its whole length**: the one
  request that started with it finished at 710-902 ms (45-50 ms idle), in 8 of 8 runs, whichever switch
  interval was set. On this macOS laptop the 0.5 ms interval changed nothing. I could not run Linux
  (no container was started), so I do not know whether production's interpreter hands the lock over per
  blocking call (about 5 ms each; my guess is on the order of a hundred blocking calls in a car page, so
  about 0.5 s, not measured) or stalls completely as here; both give a stall of the same order as the loop.
* Database load in the same process did nothing (p50 65 ms against 52 ms, no long request).
* A voluntary sleep works: the shipped card refresher sleeps 2 ms every 100 cards and costs the car page
  about 2.5 times its p50 (120-130 ms against 52) but never stops it. The prototype's `swr_yield` arm (a
  1 ms sleep every 5,000 rows; I did not tune the spacing) helped less than counting in SQL.
* The facet rebuild thread, unthrottled on 1.5.8 and limited to one per 300 s from 1.5.11, stalls one
  overlapping request for about half a second (508-547 ms in 4 of 5 runs, 921 ms in the first) and leaves
  the rest alone; presumably that is its Python post-processing, since the SQL part releases the
  interpreter (not separated).
* It is not a C call that holds the interpreter: `cur.fetchall()` on the rarity query's 214,546 rows holds
  it for 27 ms (a ticker thread's largest gap, 27.3 ms median of 5; `fetchmany(2,000)` slices 6.7 ms; the
  `GROUP BY` result's 52,119 rows 7.6 ms; `execute` 189 ms with the interpreter free, largest gap median
  2.8 ms [0.3-8.8]; `r3_gil_fetchall.py`). The 0.5-0.7 s is the Python loop over those rows.

Consequence for the design: every refresher that stays in the web process (the prototype above, a Redis
refresher thread, the facet and card jobs) must count in SQL, yield often, or run in another process.

### 4.8 How often does a visitor pay? A traffic model (assumptions, not a measurement)

Traffic is the one thing I cannot see from outside, so this simulates the code's own TTL rules for an assumed
arrival rate. Rules: the rarity cache reloads on the first car page in each wall-clock 600 s bucket; the
plausibility cache reloads on the first car page more than 300 s after its previous load. Car-page views
arrive as a Poisson process (60 simulated days per row; pinger rows average 24 independent pinger phases).
`r3_traffic_model.py`, `out/traffic_model_r3.txt`; the closed forms agree with the simulation (for example
6 views per hour: 63.2% and 66.7% analytic, 63.3% and 66.8% simulated).

| car-page views per hour, whole site | pay a rarity reload | pay a plausibility reload | pay at least one | same, with a keep-warm GET every 60 s | every 20 s | expected extra wait per view (estimate) |
|---|---|---|---|---|---|---|
| 1 | 91% | 92% | 94% | 20% | 8% | about 4.3 s |
| 3 | 79% | 80% | 84% | 20% | 7% | about 3.7 s |
| 6 | 63% | 67% | 74% | 19% | 8% | about 3.1 s |
| 12 | 44% | 50% | 60% | 19% | 8% | about 2.2 s |
| 30 | 20% | 29% | 38% | 15% | 7% | about 1.1 s |
| 60 | 10% | 17% | 23% | 12% | 6% | about 0.6 s |
| 120 | 5% | 9% | 13% | 9% | 5% | about 0.3 s |
| 600 | 1% | 2% | 3% | 3% | 2% | about 0.07 s |

The last column multiplies the two shares by the live single-event costs (+2.7 s for a bucket flip, +2.0 s for
a 300 s expiry; from 4.1, rounded) and ignores that two events in one request overlap a little. It is an
estimate that depends on those two live medians. How to read it: below about 30 car-page views an hour, most
visits pay at least one multi-second reload; a site with its owner and a handful of visitors is in that
region. A keep-warm GET
(any car page every 20-60 s from outside) moves most of that cost to the pinger, but it only narrows the
window (a visitor who arrives between an expiry and the next ping still pays), keeps a request thread busy
for about 3 s per expiry, and cannot warm anything after a restart before the ping starts. It is a stop-gap
for a few weeks, not the fix. Requests per hour are in Cloudflare Analytics; that would turn this table into a
single row.

---

## 5. A new Postgres connection for every statement (S4)

Path, live and staged: `base_repo.db_conn()` -> `get_conn()` ->
`inventory_compat.open_inventory_connection()` -> `inventory_pg.pg_connect()` ->
`psycopg.connect(dsn, autocommit=False[, connect_timeout=10 on 1.5.11+])`. Every repository call
opens and closes its own connection. `borrow_read_connection()` exists but only the incomplete-index
rebuild and scripts use it (`git grep borrow_read_connection phase/N37a`); its docstring says
"Serving code never enters a borrow block". No `psycopg_pool` outside tests, and
`requirements-web.txt` does not list it.

### 5.1 Local cost

`python -I $SP/conn_cost2.py 1270413 40` (`conn_cost2_r2.json`), medians:

| operation | unix socket | TCP 127.0.0.1 |
|---|---|---|
| connect + `SELECT 1` + close | 1.43 ms | 1.65 |
| connect + car-row SELECT + close (fresh backend) | 2.21 | 2.36 |
| same SELECT on a reused connection | 0.12 | 0.19 |
| **8 statements on 8 fresh connections** | **15.6** | 17.7 |
| 8 statements on one reused connection | 0.56 | 0.83 |
| 8 statements with 8 pool checkouts (`psycopg_pool`) | **0.53** | 0.74 |

### 5.2 Whole pages with the app's own code (interleaved A/B)

`python $SP/server2/conn_ab3.py 80 <out.json>`, run twice in a quiet period (`conn_ab3_qa.json`,
`conn_ab3_qb.json`). One process; for every repetition each page is requested once in each of three
modes, so machine-load drift hits all modes equally:

* **fresh**: as shipped (a `psycopg.connect` per repository call);
* **pool**: `inventory_pg.pg_connect` hands out `psycopg_pool` connections (`close()` = rollback and
  return);
* **borrow**: each request wrapped in the repo's own `borrow_read_connection()` (one connection per
  request thread).

p50 ms (run a / run b), connections opened per request in brackets:

| page | fresh | pool | borrow |
|---|---|---|---|
| `/` | 10.2 / 10.6 (1) | **4.4 / 4.3** (0) | 9.9 / 10.2 (1) |
| `/car/1468626` | 52.6 / 52.4 (10.5) | **17.9 / 17.4** (0) | 23.2 / 24.7 (1) |
| `/dealership/535` | 37.8 / 40.5 (6) | **17.4 / 16.9** (0) | 36.5 / 37.7 (1) |
| `/car/1270413` | 120.9 / 123.1 (8) | 81.8 / 94.1 (0) | 72.8 / 62.2 (1.5-1.7) |

The normalised page body was identical across the three modes in every run (one distinct body per
page). `/car/1270413` was slower in these interleaved runs than alone (42 ms) because the
dealership view in each repetition leaves the card refresh thread running (section 4.4); the
absolute saving from the pool (27-59 ms) is in line with the other pages.

Reading: a pool removes ~35 ms from a car page and ~20 ms from a dealership page locally, where a
connection costs 1.5 ms. On production a connection costs 15-38 ms, so the same change removes
roughly 120-160 ms from a car page, 75-90 ms from a dealership page and ~35 ms from the landing
page (estimate, from section 3). A pool also lifts the thread-bound ceiling: with 4 threads each
held ~197 ms by a car page, the worker tops out near 4 / 0.197 = **20 car pages per second**
(estimate); at ~42 ms of CPU per page the CPU limit would then be ~24 per second.

`borrow` is a smaller change (it already exists) and helped the car pages (-29 to -48 ms) but not
the dealership page or the landing page in this test. It has a hazard a pool does not: its
wrapped `close()` rolls back the one shared connection, so a nested `get_conn()` inside an open
write transaction would discard the outer work. I did not search the web code for such nesting.

### 5.3 Staged releases

1.5.11 adds `connect_timeout` (default 10 s, `PG_CONNECT_TIMEOUT`); it bounds a dead-database hang
and changes no cost. N37a adds one connection per car page: `details_pending()` ->
`incomplete_listings_db.is_car_pending()` is its own `get_conn()` and one `SELECT` (its
`_ensure_schema` is skipped after the first call per process). The `dealerships` row is still read
2-3 times per car page **[eval]**.

Open check for the owner: whether the **web** service's `INVENTORY_DATABASE_URL` is the private URL
(`postgres.railway.internal`) and whether TLS is negotiated on each connection (the database runs the
`postgres-ssl:18` image, so libpq's default `sslmode=prefer` would start a TLS handshake on every new
connection). *Correction in round 3:* the first version cited `docs/RAILWAY_SCANNING.md` line 86 as saying the
web service uses the private URL. That line is in the `scanner-nightly` service's variable table
(`${{Postgres.DATABASE_URL}}`, "private-network URL"); the repo documents nothing for the web service's
variable, and the ops log notes the database also has a public TCP proxy on 5432
(`docs/SCANNING_OPS_LOG.md` line 62, **[docs]**). The 20-38 ms per connection measured live (3.2) is
consistent with a same-region private connection with TLS and password authentication, but does not prove it.

---

## 6. One worker, four threads (S3)

Config, live and staged (`scripts/docker-entrypoint-web.sh`; unchanged in substance):
`gunicorn -c gunicorn.conf.py -w ${GUNICORN_WORKERS} --threads 4 --preload --timeout 120
--graceful-timeout 30 --max-requests 0`. Production sets `GUNICORN_WORKERS=1` (entrypoint comment
and the 1.5.16 deploy note, "checked 2026-11-02"). `--threads 4` selects the gthread worker: one
process, 4 request threads, one GIL shared with every background thread (`grid-cards-refresh`,
`grid-cars-refresh`, `facet-options-refresh`, `listings-prewarm`, and on N37a `registry-backfill`).
Railway health-checks `/health` within 300 s, restart `ON_FAILURE` up to 5 times, 1 replica
(`deploy/railway/service_settings.json`). The web service's CPU and memory limits are not in the
repo (the file lists limits only for scanner-nightly: 16 vCPU / 16 GB).

### 6.1 Throughput and latency (local, same flags)

```
$SP/server2/cold_start.sh 8765 1 4 w1_r2        # gunicorn -w 1 --threads 4 (production flags), read-only DB
python -I $SP/server/loadgen.py 8765 8 <C> <path> <label>     # closed loop, keep-alive, 8 s per level
$SP/server2/cold_start.sh 8766 4 4 w4_r2        # 4 workers x 4 threads; warm-up 8 clients x 12 s first
```

| page | users | 1 worker: req/s | p50 | p95 | 4 workers: req/s | p50 | p95 |
|---|---|---|---|---|---|---|---|
| `/car/1270413` | 1 | 23.2 | 40.2 | 75.9 | 23.0 | 42.0 | 46.8 |
| | 4 | 32.8 | 119.4 | 160.9 | 74.3 | 45.8 | 69.4 |
| | 16 | **35.5** | **419.8** | 630.2 | **141.2** | 110.8 | 138.9 |
| `/listings` | 1 | 22.3 | 39.3 | 79.3 | 23.3 | 38.7 | 74.1 |
| | 4 | 29.9 | 130.8 | 186.7 | 87.0 | 40.6 | 81.4 |
| | 16 | 29.0 | **549.1** | 643.8 | 109.6 | 137.4 | 233.6 |

(first pass: car 34.8 / 33.6 req/s and p50 468 ms at 4 / 16 users on one worker; `/listings`
30.0 / 29.3 and 542.8 ms.) The 4-worker warm-up (8 clients, 12 s, right after the cold sequence)
had p50 57 ms and a worst request of 6,081 ms (cold builds in each worker).

What one worker means: on a steady flow it is enough (derived ceiling ~20 car pages per second,
section 5.2). The damage comes from **head-of-line blocking**: one CPU-bound job holds the GIL and
slows every other request.

* A reload at a TTL boundary: 0.7-0.8 s locally per loader, 2.6-3.1 s live, with other threads waiting
  (4.2, 4.3): with four request threads the window completed 8 requests instead of 127 (4.6).
* The card refresh: 5-21 s of CPU per area (4.4); car page 40 -> 126-277 ms while it runs. It sleeps 2 ms
  every 100 cards, which is why it slows the page about 2.5 times instead of stopping it (4.7).
* The first-pass contention test (`contention.py`, `contention_w1t4.json`): car page p50 46.7 ms
  idle, **81.6** and p95 **428** right after a cold scope build; four concurrent cold builds
  (677 / 937 / 1,111 / 6,245 ms) used all 4 threads and `/health` max was **626 ms**.
* Gunicorn's thread count caps requests in flight at 4 (queued connections wait).
* Threads do not isolate requests from a background job in the same process: a 700 ms Python loop with no
  database use in a second thread stopped the foreground request for its whole length (710-902 ms, 8 of 8
  runs; 4.7), and the facet rebuild thread stalls one request by about 0.5 s per rebuild. Two or more workers
  limit the blast radius to the process that runs the job; they do not remove the stall, and they multiply
  the 3.3 GB footprint and the in-process rate limits.

### 6.2 Memory per worker

| run | worker RSS |
|---|---|
| 1 worker after the cold sequence (`ps -o rss`, MiB) | 622 MiB (637,072 KB) |
| in-process, warmed with facets, 2 scopes, car and dealership pages (psutil, decimal MB) | **729 MB** (695 MiB); 135 MB after import (`gc_probe_r1.json`) |
| 4 workers after the sweeps (`ps -o rss`, MiB) | 622 / 537 / 608 / 454 MiB + arbiter 125 MiB = **2.29 GiB** |
| production | ~3.3 GB warm with 1 worker; 13.7 GB with 3; 4.92 GB before the arbiter stopped prewarming **[eval, entrypoint comment]** |

The first pass measured 485 / 632 / 447 / 454 MiB (4 workers) and 603-727 MiB (1 worker). Four
workers cost about 3.5x the memory of one locally; production's per-worker footprint is larger and
its breakdown was not measured (no shell in the container).

The 1.5.16 note makes this harder: the IP rate limiter is in-process only, so with more than one
worker every limit (login lockout, the 20-per-minute cache-miss limit from SEC-A-005, smart
search) multiplies by the worker count.

---

## 7. Edge cache and cookies (S5)

Re-checked on today's responses (`live/bodies/*.hdr`, session values never printed):

| response | `set-cookie` | `Vary` | `Cache-Control` | `cf-cache-status` |
|---|---|---|---|---|
| `/` | `session` | Accept-Encoding, Cookie | none | DYNAMIC |
| `/listings` | `session` | Accept-Encoding, Cookie | none | DYNAMIC |
| `/car/<id>` | `session` | Accept-Encoding, Cookie | `private, max-age=180` | DYNAMIC |
| `/dealership/<key>` | `session` | Accept-Encoding, Cookie | none | DYNAMIC |
| `/api/listings/filter-options` | none | Accept-Encoding | none | DYNAMIC |
| `/api/listings/cars` (partial) | none | Accept-Encoding, Cookie | `public, no-cache` | DYNAMIC |

The cookie is created by a template context processor on every render:
`inject_csrf_and_flags()` calls `ensure_csrf_token()`, which stores a token in the session
(`backend/web/templating.py`, `backend/utils/csrf.py`). The scope view reads `session.get("user_id")`
(hidden dealers), so Flask appends `Vary: Cookie`. For a complete (not partial) body the code asks
for `public, max-age=0, s-maxage=60, stale-while-revalidate=30`, but Cloudflare reports `DYNAMIC`
for every repeat: it does not cache extension-less JSON without a cache rule **[live-speed.md]
saw the same on 25 responses**. Static assets are fine: CSS, JS and the font are `cf-cache-status:
HIT` with 0 ms at the origin (`static.jsonl`).

Consequence: all HTML and all data go to the one origin worker, 79 ms away, every time. HTML
cannot be shared (per-request CSP nonce, CSRF token); the listings JSON could be.

---

## 8. Writes and DDL on GET (S7), boot, and warm-up (S8)

### 8.1 Writes and DDL

| what | evidence | state at N37a |
|---|---|---|
| Card-store writes from GET (`INSERT INTO listings_grid_cards ... ON CONFLICT`, 500 per batch) | 74 refused batches in the 61,369 GETs of the first-pass 1-worker run (`grep -c "cannot execute INSERT in a read-only transaction" $SP/server/gunicorn_w1.log`); 5 more in the second TTL probe | unchanged |
| DDL on every dealership view: `CREATE TABLE IF NOT EXISTS dealer_reviews` + 2 `CREATE INDEX IF NOT EXISTS` (SHARE lock even when the index exists) | 3 of 8 statements per view **[eval]**; seen again in the profile (`interleave_prof.py`: `CREATE TABLE IF NOT EXISTS dealer_reviews` 15 times in 15 views) | **fixed by 1.5.10**: `runtime_ddl.pg_legacy_ddl_needed()` is False once `schema_is_current()` (one check per process), and `reviews.store` caches "objects exist". The entrypoint's `MIGRATE_ON_BOOT=1` brings prod to V029 at the deploy |
| DDL at every boot: `init_job_queue_schema` (2 CREATE TABLE + 2 CREATE INDEX) | **[eval]** | gated by 1.5.10 |
| Registry backfill (per host, `UPDATE cars SET dealership_registry_id ... LIKE`, one transaction) | read-only equivalent: 543 hosts, median **51.6 ms** per host, **28 s** projected (`backfill_cost_r2.json`; the first pass measured 132 ms and 72 s on a colder cache). The code comment gives ~621 hosts x 0.34 s = **210 s** on production **[docs/code]** | 1.5.8 runs it inside the first `GET /api/listings/geo-coords` per process: a sibling's live request got a **524 after 125.2 s** **[redis.md, row 12]**. 1.5.11 moves it to a thread started in each worker (`_prewarm_listings_inventory_cache`), so it no longer blocks a request, but it still runs a fleet-wide UPDATE after every restart |
| Users store (SQLite, SQLCipher in prod) writes on GET: search history, car views for signed-in users | `users_conn_count_r2.json`: an anonymous text search opens the users DB once (52 ms); signed in, twice (9.2) | unchanged |

### 8.2 Cold start after a deploy (S8)

Railway switches traffic as soon as `/health` returns 200, and `/health` touches nothing
(`site_misc.health`). The prewarm that remains only builds the EPA dictionary index
(`_prewarm_listings_inventory_cache` docstring: "Deliberately left OFF -- do not add a grid
prewarm back"); facets, rarity, price plausibility, bands, featured cars and scope bodies are all
built on the first requests. Local, production flags (`cold_start.sh`, `cold_seq_w1_r2.jsonl`):

| first hit after boot | 1st | 2nd | 3rd |
|---|---|---|---|
| `/health` | 2.4 ms | 1.1 | 0.8 |
| `/` | **895 ms** | 5.2 | 4.5 |
| `/listings` (whole-fleet facet build) | **2,967 ms** | 79.5 | 39.3 |
| `/car/1270413` | **1,335 ms** | 64.1 | 46.1 |
| `/car/1468626` | 214 | 42.2 | 43.0 |
| `/dealership/535` | 178 | 26.5 | 26.8 |
| `/api/listings/cars?zip=92618&radius=25` | **2,552 ms** (partial) | | |

`/health` answered 200 after **2.0 s** (1 worker) and **1.4 s** (4 workers); the first pass saw
5.4 s on a colder file cache. On Railway the entrypoint also runs the vault preload,
`migrate --apply` and `bootstrap_site_admin.py` before gunicorn, which I did not time. With 4
workers each builds its own copy at the same moment (first-pass warm-up p95 **10.7 s**, max
10.8 s). Only the card store persists across a deploy, and it goes stale when `CARD_REV` changes
(4.5).

Releases 1.5.11 and 1.5.14 remove two boot-time downloads that 1.5.8 has: the pgeocode US table
(5.6 MB from GeoNames, now baked at `/opt/pgeocode`) and the MiniLM embedding model (now baked,
`HF_HUB_OFFLINE=1`, CPU-only torch). Torch and sentence-transformers still import on first
semantic search: 450-454 MB maxrss and 2.5-4.6 s locally (first pass,
`python -c "import torch; import sentence_transformers"`).

---

## 9. Payload construction (S9) and signed-in visitors (S6)

### 9.1 `/listings` and the scope body

* `/listings` returns 1,404,532 B of HTML locally (159.5 KB gzip); live it is 189.4 KB after
  Cloudflare's brotli and 1.55 MB decoded **[live-speed.md]**. It runs no SQL in steady state and
  costs 39 ms locally (`pack_car_rows` over 23,156 cascade rows ~12 ms, Jinja ~35 ms profiled,
  gzip ~10 ms **[eval]**). The app gzips every response over 2 KB itself at level 5
  (`web/templating.py` `_gzip_large_json`).
* Live it costs 202-422 ms at the origin, in two clusters: 202-265 ms (six of the 13 solo samples:
  `live3_run.jsonl` 202, 209, 211, 224, 233, 265) and 342-422 ms (the other seven). I did not find
  the cause. What is known: a pure-CPU endpoint (`filter-options`) runs 1.7x local, so a slow CPU
  alone predicts ~66 ms, not 120-340; the nightly card builder on Railway ran at ~0.6 ms per card,
  as fast as my laptop **[docs]**; the response size does not matter (a 1.6-2.0 MB cached JSON
  body costs +9 ms). Candidates: a CPU quota on the web service, GIL waits behind a background
  thread, or memory behaviour of the 3.3 GB process. S11 (a `Server-Timing` header) would settle it.
* The scope body for an area is 14-21 MB of JSON per area (`28105`/50: 13,298 cars, 14,546,046 B raw,
  1,585,587 B gzip; Irvine 25: 18,933 cars, 20,942,563 B raw, 2,008,283 B gzip), built from
  34 MB of `card_json` read from Postgres per cold Irvine build (584 ms). Gzip level 6 of the
  27 MB local body takes 167 ms (level 1: 79 ms).

### 9.2 Signed-in visitors (S6)

* Production keeps `users.db` encrypted (SQLCipher, SEC-088); `users_sqlite._connect_sqlcipher`
  runs `PRAGMA key` on every open. On a scratch encrypted file: **50.3 ms** median per
  connect + key + SELECT + close (48.3-60.8, 15 runs; plain sqlite3 0.063 ms; kdf_iter 256,000;
  cipher 4.12.0 community). First pass: 48.1 ms.
* Opens per page with an encrypted scratch users DB (`users_conn_count_r2.json`, anonymous /
  signed in): `/` 0 / 0; `/api/listings/filter-options` 0 / 0; `/listings` 0 / 1 (48 ms);
  car page 0 / **2 (97.5 ms of the page's 149 ms)**; dealership 0 / 1 (49 ms); a text search
  `/listings?q=white+bmw&zip_code=28105&radius=50` 1 (52 ms) / 2 (97 ms). First pass: car page 2
  opens, 99 ms.
* The car page for a premium or admin visitor with a sticker-eligible car posts
  `POST /api/cars/<id>/packages/ensure?vision=0` on load (`static/car_packages.js` `startEnsure`)
  and re-posts at 2, 3, 5 and 8 s while the server says a fetch is in flight. The route holds a
  request thread for up to the budget (`_PACKAGES_ENSURE_DEFAULT_BUDGET = 6.0` s) while a worker
  thread may re-scrape a dealer page (cooldown 900 s per car); the module's own comment records a
  car with no sticker source taking 28 s cold and 12 s warm, and tail work taking 10.5 s beside a
  live worker. **Not measured**: the route is POST-only and behind a login.

---

## 10. Smaller items and results that ruled out a suspect

* **Garbage collection is not a cause.** After warming facets, two scopes and four page types the
  worker held 308,918 tracked objects, 729 MB RSS. A full `gc.collect()` takes **43 ms** (43.1,
  42.2, 43.2). During 300 mixed requests plus two cold scope builds (19.4 s) the automatic pauses
  were: generation 0, 899 of them, max 7.0 ms; generation 1, 82, max 23.7 ms; generation 2, 7,
  max **53 ms** (`gc_probe_r1.json`). That bounded only the local worker, so round 3 grew the heap on
  purpose. (1) In the real app (`r3_gc_ballast.py`, `out/gc_ballast_r3.log`, Python 3.14.7): ballast of
  0 / 1 M / 3 M / 6 M GC-tracked lists, 40 `/listings` and 40 car pages at each size. `/listings` p50 was
  39.5 / 38.5 / 38.7 / 39.4 ms; the car page 42-49 ms (the first, unballasted step ran during other load
  and read 86 ms). An explicit full collection took 39 / 63 / 112 / 202 ms at 0.28 / 1.28 / 3.28 / 6.28 M
  tracked objects, and the automatic generation-2 pauses seen in the request windows were 65, 97, 212 ms and
  none (they get rarer as the heap grows: CPython collects generation 2 only when the new survivors reach
  a quarter of what is already there). (2) The production interpreter (`r3_gc_synth.py` under
  `/opt/homebrew/bin/python3.12`, 3.12.13, stdlib only, a synthetic request that builds 23,156 row lists like
  `pack_car_rows`): request p50 8.9 / 9.3 / 9.4 / 9.7 ms at the same four sizes. Generation-2 pauses per
  100 requests: 24 of at most 1.4 ms with no ballast, then 7 (max 25 ms) at 1 M, 2 (max 68 ms) at 3 M and
  1 (140 ms) at 6 M objects; on 3.14.7 the maxima were 22 and 66 ms and none at 6 M. A production worker
  holding 6 M tracked objects would
  see one request in about a hundred pay about 140 ms and nothing at the median, so GC does not explain
  `/listings` being 3-9 times slower than locally. A rough guess for the production count is 1.5 M
  (3.3 GB against 0.73 GB for 0.31 M locally); I could not measure it.
* **The interpreter version does not explain production being slower.** The production image is
  `python:3.12-slim` (`Dockerfile.web` line 1); I measured on 3.14.7. A stdlib-only bench shaped
  like the hot loops (`pyver_bench.py`): on 3.12.13 the rarity-style pass took 215.9 ms vs 235.0
  on 3.14.7, the JSON round trip 119.8 vs 138.8 ms, gzip level 6 17.0 vs 16.8 ms, string
  concat 65.6 vs 71.9 ms. 3.12 is 8-14% faster on the Python-level loops (gzip is the same), so
  local numbers are, if anything, slightly pessimistic.
* **Cloudflare is not the source of the fixed cost** (3.1): 145 ms to first byte through it vs
  135 ms direct.
* **bcrypt at boot** (two cost-13 hashes, ~780 ms **[eval]**) still runs at every web import
  on N37a (`admin_users_db.init_admin_db` hashes before checking the row exists). It delays a
  boot, not a visitor, because the old container serves until the new one is healthy.
* `--max-requests 0` and unbounded memos (`_VPIC_MEMO` **[eval]**) mean memory only grows until
  the next deploy.
* `dealership_page._dealer_grid_cars` calls `json.loads` on every cached card of the store on
  every view **[eval]**; its card cache lives 30 s (`_DEALER_CARDS_TTL_S`), which is the +330 ms
  the dealership page showed after 70 s idle (4.2: 370 ms against 41 warm).
* The tap-prefetch script requests every uncacheable page twice **[live-speed.md]**, which doubles
  the load on the one worker for each navigation.

Checked in round 3 and found not to be a cause (code at `phase/N37a`, same as `main` for these):

* **No outbound HTTP on an anonymous GET.** `git grep -l -E "requests\.(get|post|request|Session)|urllib\.request|httpx\.|urlopen\(|aiohttp" phase/N37a -- backend ':!backend/scanner' ':!backend/scripts' ':!backend/tests' ':!backend/dev' ':!backend/dealer/admin'`
  lists 28 files; the ones reachable from a public route are the NHTSA recall lookup (`/nhtsa-recalls`,
  which the car page only links to, and its JSON twins, rate-limited by IP),
  `api_car_vehicle_history_intelligence` (premium, only on a button click in `car_page.js`, lines 566-576),
  the sticker ensure POST (9.2) and the OAuth callbacks. The car page reads `nhtsa_vpic_cache` and never
  calls NHTSA; `decode_missing_vins` and `fetch_vpic_batch` are scanner post-steps.
* **The static-file `?v=` stamp** is computed once per process (`web/static.py`, `_static_cache_ver_memo`;
  it re-walks the tree on every call only when `app.debug` is set).
* **The request hooks are cheap.** `main.py` installs three `before_request` hooks (CSP nonce, CSRF for
  mutations, billing gate) and `after_request` hooks for CSP headers, static cache headers and the app's own
  gzip (`web/templating.py`, level 5 for HTML and JSON over 2 KB; the scope body uses level 6 at
  `listings_api.py:322`). `/health` takes 0.8 ms in the app, so the hooks cost well under 1 ms.
* **No background starvation was visible live during the solo pass.** 29 `/health` samples over 24 minutes
  (01:43:57-02:07:26 UTC): median 79 ms, largest 147 ms, next 116 and 99. A non-yielding background job
  (4.7) would have shown as a multi-hundred-millisecond `/health`; none did, so the `/listings` gap (9.1) is
  not a background job caught in the act at those times.

---

## 11. What 1.5.11-1.5.16 already fix (CHANGELOG at `phase/N37a`)

1.5.9 and 1.5.10 are not live either (the live `/health` reports 1.5.8), so the 1.5.10 change that
matters here is listed too. Read from `git show phase/N37a:CHANGELOG.md` (sections 1.5.9-1.5.16) and
the code; not run.

| release | server-side change | effect on the findings above |
|---|---|---|
| 1.5.10 | Hot-path runtime DDL gated on the migration chain (`reviews.store`, `recipe_store`, `job_queue`) | Removes the 3 DDL per dealership view and the boot job-queue DDL **once prod is at the chain head** (S7) |
| 1.5.11 (N6b) | Facet background rebuilds throttled to one per 300 s (`FACET_REBUILD_MIN_INTERVAL_S`) | Helps only while a scanner writes. The cold facet build (~2.4-3.0 s) is unchanged |
| 1.5.11 | Registry backfill moved from the first `/api/listings/geo-coords` request into a thread at worker start | Removes the 125-210 s stall from the request path; still a fleet-wide UPDATE after each restart (S7, S8) |
| 1.5.11 | `pg_connect` `connect_timeout` (10 s) | Bounds a dead-database hang; per-connection cost unchanged (S4) |
| 1.5.11 | pgeocode US table baked into the image | Removes the 5.6 MB download on the first ZIP lookup per container (S8) |
| 1.5.11 (N6.4) / 1.5.12 (B-029) | Pending flag in the card `aux_key` | Pending cars no longer rebuilt on every request. **Changes `CARD_REV`** (4.5). Aged-card churn and the partial-refetch loop unchanged (S1) |
| 1.5.12 (SEC-A-005) | Per-IP limit of **20** cache-miss scope builds per minute (`RATE_LIMIT_LISTINGS_BUILD_PER_MIN`), 30 per minute on `/api/coords-to-zip` | Protects the one worker from loops; a normal shopper's 4-6 refetch builds still happen (S1) |
| 1.5.12 | Audit fixes touching `utils/interior_color_buckets.py` | Changes `CARD_REV` again (4.5); `utils/market_price.py` changes it a third time between 1.5.12 and 1.5.16 |
| 1.5.14 (N29) | Embedding model baked (`HF_HUB_OFFLINE=1`); CPU-only torch; web image without Playwright | Removes the model download on the first semantic search per container (S8); torch still imports on first use |
| 1.5.16 (N37.11) | IP rate limiter in-process only | With more than 1 worker, every limit multiplies by the worker count; one more reason a shared store is needed before raising workers (S3) |
| 1.5.13, 1.5.15, rest of 1.5.16 | Test tiers, rooftop evidence, Postgres-only slice | No web-speed change. N37a adds one connection per car page (S4) |

Not addressed by any staged release: S1 (in-process card re-serialization, partial refetch
loop, day-aging), S2 (TTL reloads, herd), S3 (one worker), S4 (no pool), S5 (edge cache), S6
(SQLCipher per open), S8 (the healthcheck passes before caches are warm; no step rebuilds cards
after a `CARD_REV` change), S9 (payload), S10 (region), S11 (no timing header).

### 11.1 Re-check of the "not fixed" column against the code diff (round 3)

`git diff --stat main phase/N37a -- <file>` for the files each finding depends on (`main` = 1.5.8 = live,
`phase/N37a` = `faea2b985`; the whole range is 349 files, 30,607 insertions, 5,309 deletions, 117 commits).
Files not listed in the table with "yes" had no change at all.

| file | changed since 1.5.8? | what changed | finding it bears on |
|---|---|---|---|
| `backend/utils/rarity_score.py`, `backend/utils/price_plausibility.py`, `backend/intelligence/deal_score_cache.py` | no | | S2: the loaders and TTLs are identical |
| `backend/db/repositories/base_repo.py`, `backend/db/inventory_compat.py` | no | | S4: still one connection per repository call |
| `backend/db/inventory_pg.py` | yes, 201 lines | `connect_timeout` (10 s default); a quote- and comment-aware rewrite of the `?` to `%s` adapter (memoised by 1.5.11); still one `psycopg.connect` per `pg_connect` | S4 unchanged |
| `requirements-web.txt` | new file, 53 lines | `psycopg[binary]>=3.2`, `redis>=8.0.1`; no `psycopg_pool` | S4 |
| `gunicorn.conf.py` | yes, 8 lines | comments only; `post_fork` still starts only the prewarm | S3, S8 |
| `scripts/docker-entrypoint-web.sh` | yes, 41 lines | comments and the removal of the `RATE_LIMIT_SQLITE_PATH` default; the gunicorn flags are identical: `-w ${GUNICORN_WORKERS:-3} --threads 4 --preload --timeout 120 --graceful-timeout 30 --max-requests 0` | S3 |
| `deploy/railway/service_settings.json` | new file, 36 lines | web: `numReplicas 1`, health check `/health` 300 s, restart ON_FAILURE x5; no CPU or memory limit for web (only scanner-nightly has one) | S3, S8 |
| `Dockerfile.web` | yes, 39 lines | pgeocode table and embedding model baked, CPU-only torch, no browser | S8 (boot downloads gone) |
| `backend/db/repositories/grid_cards_repo.py`, `listings_repo.py`, `backend/utils/market_price.py` | yes, 16 / 39 / 26 lines | pending flag in the card `aux_key`; facet rebuild throttle (300 s); the files are inputs to `CARD_REV` | S1: every stored card goes stale (4.5) |
| `backend/routes/listings_api.py` | yes, 17 lines | per-IP limit on cache-miss scope builds and on `coords-to-zip` | S1: protects against loops only |
| `backend/main.py` | yes, 36 lines | the registry backfill moves into a thread started by `post_fork` | S7, S8 |
| `backend/web/templating.py`, `backend/utils/csrf.py`, `backend/routes/health.py` | no | | S5 (cookie and `Vary`), S8 (`/health` still touches nothing) |

So the "not fixed" column stands: nothing in the staged range changes the TTL loaders, the connection path,
the worker flags, the card aging rule or the cookie/`Vary` behaviour. Two staged changes move the
picture the wrong way and should be on the release checklist: `CARD_REV` changes three times (4.5), and the
car page gains one more connection (5.3).

---

## 12. Redis: what it removes and what it does not

Today Redis is only an optional readiness probe: `Config.redis_url()` and `routes/health.py`
(`redis-py` is in `requirements-web.txt`, `redis>=8.0.1`). Production has no `REDIS_URL`: the live
`/api/ready` says `"redis":"skipped"`. Redis round-trip time on Railway was not measured here. The
design, memory sizing and failure behaviour are in `redis.md`; this table only ties each use to a
cost measured in this report.

| use | replaces | measured cost it removes | notes |
|---|---|---|---|
| Shared store for the **fleet aggregates** (rarity counts, price-plausibility medians, deal bands, featured cars, public count, fallback URLs), written by one refresher | Inline TTL reloads (S2) | +1.6 to +3.1 s live per event (+5.4 s all cold); about 1.7 s of local reloads in total (711 + 775 + 147 + 52 + 8 ms, 4.2; the first version double counted this) | A Postgres table stores them as well, and the in-process prototype in 4.6 already removes the stall (first request 1.8 s to 0.06 s; worst request 0.23-0.34 s when the count is done in SQL). The win is moving the compute off the request and out of the herd, not Redis itself. A separate process writing the result (Redis or a table) also removes the one slow request that a background thread still causes (4.7) |
| **Facet options and the packed `/listings` rows** as bytes keyed by the grid token | Each worker's own 2.4-3.0 s build and +227-246 MB of objects **[eval]** | 2,967 ms first `/listings` per deploy per worker | Makes 2-3 workers affordable with the next row |
| **Scope bodies** (gzip bytes, 0.8-3.0 MB) keyed by token, zip, radius, hidden set | Per-worker LRU (64 entries / 96 MB) | Cold builds after a deploy and duplicate builds across workers | Valid only while the token holds (it moves every 60 s while a scanner writes) |
| **Single-flight locks** (`SET NX PX`) across workers | Per-process locks (and none on the rarity cache) | The herd (4.3) and 4-worker cold start p95 10.7 s | Needed as soon as workers > 1 |
| **Rate limits and lockout counters** (`INCR` + `EXPIRE`) | In-process limiter (1.5.16) | Correctness, not speed | Prerequisite for raising `GUNICORN_WORKERS` |

What Redis does not fix:

* **Per-statement connections (S4).** That needs a pool: 15.6 -> 0.53 ms for 8 statements
  locally, 52.6 -> 17.9 ms for a whole car page, ~120-160 ms on production (estimate).
* **The distance (S10).** Redis in the same region does not move the app east.
* **CPU-bound card building in the web process (S1).** A cache of results does not help while every
  aged card is re-serialized on the first request; the work has to leave the process.
* **SQLCipher key derivation per open (S6)** and **the 14-21 MB scope payload (S9).**
* **The GIL (S3).** Only more processes help; Redis only makes those processes cheaper.

---

## 13. Suggested order (server side)

Gains are estimates from the measurements above unless marked measured.

1. **Add server-side timing (S11).** An `after_request` hook that sets
   `Server-Timing: app;dur=<ms>, db;dur=<ms>;desc="<n> connections"` on every response. Wrap
   `pg_connect` with a timer. Effort S. It turns the open questions (the `/listings` gap, the
   per-connection cost, how much of the floor is Railway) into one live request each.
2. **Pool connections (S4).** `psycopg_pool` behind `pg_connect` / `open_inventory_connection`;
   `close()` returns the connection after a rollback; min 2-4, max ~8. Drop the repeated
   `dealerships` and `incomplete_listings` reads on the car page. Expected: car page about 120-160 ms
   faster, dealership 75-90 ms, landing ~35 ms. Effort S-M. Risk: session state leaking between
   borrowers (the pool resets it); watch the transaction-per-request behaviour of code that holds a
   connection open while calling another repository function.
3. **Take the TTL reloads off the request (S2).** Compute rarity counts, price plausibility, bands,
   the public count and the fallback URLs (the featured cars only read plausibility) on one timer or
   after each scan; serve the last value while refreshing; one refresh at a time; start them in
   `post_fork` before the health check passes. Expected: removes the +1.6 to +5.4 s spikes. Measured
   with a prototype (4.6): first request after an expiry 1.8 s to 0.06 s with one shopper and 3.4 s to 0.14 s
   with four. Count the rarity table in SQL (`GROUP BY`, same result, checked) or make the loop yield: with
   the shipped Python loop in the refresh thread one request per expiry still stalls 0.6-0.7 s (4.7), with
   the SQL count the worst request fell to 0.23-0.34 s. Effort S-M. A stop-gap while this is built: a
   keep-warm GET of a car page every 20-60 s from outside cuts the share of visitors who pay from about 74%
   to 8-19% at 6 views an hour (4.8); it does nothing after a restart.
4. **Take card building off the web process (S1).** Run `build_listings_grid_cards` after every
   scan and nightly; stop day-aging cards whose content did not change, and compute
   "price dropped N days ago" in the browser; serve aged-but-valid cards as complete (no `partial`,
   no refetch loop). Add the builder to `docs/RELEASING.md` right after any deploy that changes
   `CARD_REV` (150-171 s on Railway). Effort M. Risk: one-day staleness of date labels unless
   computed client-side.
5. **Warm deploys (S8).** Warm the facets and the aggregates in `post_fork`; point the Railway
   healthcheck at a route that turns 200 only when warm (the 300 s timeout allows it). Effort S.
   Risk: a slow boot fails the deploy, so cap the warm-up.
6. **Edge cache (S5).** A Cloudflare cache rule for `/api/listings/cars` and
   `/api/listings/filter-options` that honours origin `Cache-Control`; stop touching the session for
   anonymous callers there; issue the CSRF token lazily. Effort S. Never cache bodies that depend
   on a session.
7. **Region (S10).** Confirm visitor geography first (Cloudflare Analytics). If they are mostly in
   the East, move the web service and Postgres together to an East US region. Effort M (a ~5.6 GB
   database, **[docs]** ops log volume row); do it after the pool, because it keeps the connection
   cost where it is and removes ~50-60 ms from every request.
8. **`/listings` shell and payload (S9).** Cache the rendered shell per token; send a first page
   of results with a continuation instead of 14-21 MB; slim the 38-key cards. Effort M.
9. **Workers (S3).** With 3 and 4 done, re-measure RSS and move to 2-3 workers; rate limits and
   the single-flight locks must be shared first. Effort M-L.
10. **Signed-in costs (S6).** Reuse one users connection per thread (or a raw-key `PRAGMA key`),
    or finish N37.10; make the sticker `ensure` call asynchronous server-side. Effort S.

## 14. What I could not measure, and owner checks

* The live web container's CPU quota and memory limit: `cat /sys/fs/cgroup/cpu.max
  /sys/fs/cgroup/memory.max` (the entrypoint already asks for `memory.max`).
* The live Region of the web service and of Postgres (Railway service settings), and whether
  `INVENTORY_DATABASE_URL` is the private URL.
* Real traffic: requests per minute and the visitor country mix (Cloudflare Analytics or
  `railway metrics`). The S2 spikes hit whoever arrives first in a window; how large a share of
  visits pays them depends on this.
* Redis round-trip time on Railway, and the container's 3.3 GB breakdown (a `tracemalloc` or RSS
  log around each cache build would show it).
* `POST packages/ensure` cost, and anything behind a login.
* The staged releases' speed on production: only code and the CHANGELOG were read. The local database is
  at migration V025 and the staged code needs V026-V029, so I did not run it.
* How the Linux interpreter in production hands the lock over (4.7): a stall of the whole loop length, as
  on this Mac, or about 5 ms per blocking call. Running `r3_refresh_convoy.py` with `ARMS=hog_si5,hog_si05,idle`
  inside a `python:3.12-slim` container would tell; I did not start Docker (it needs your go-ahead).
* Why the live `/listings` costs 202-422 ms at the origin against 39 ms locally (9.1). Round 3 removed
  four candidates: the response size, the Python version, garbage collection at a 6 M-object heap (10),
  and a background job caught in the act (no `/health` outlier in 24 minutes). A `Server-Timing` header
  (S11) is still the way to settle it.
* The reload costs on production are 2 to 4 times the local ones (4.1). The split between the database
  (two scans of 214,675 cars) and the CPU (the Python loop) needs timings from inside the process.

---

## Appendix: commands and raw outputs

`$SP` = `scratchpad`.
`$R2` = `docs/web_speed_search_2026_11/server-causes-raw/r2`; `$SP/server2/` is the working
directory those scripts ran in, and `$R2/live` plus `$R2/local` are copies of its scripts and
outputs. All local runs source `$R2/local/env.sh`
(read-only `PGOPTIONS`, `INVENTORY_DATABASE_URL=postgresql:///cars?host=/tmp`, scratch users DBs,
`DS_SKIP_REGISTRY_BACKFILL=1`, `KMAC_VAULT_AUTO=0`) and run from `$SP/server`.

**Live, first pass (151 requests; counter `$SP/live_budget.txt`)** (scripts and records in
`server-causes-raw/live`):

```
python3 $SP/live_probe.py $SP/live/discovery.jsonl /health /api/health /api/ready /          # 4
python3 $SP/live_probe.py $SP/live/discovery.jsonl /car/2002740 /                             # 2
python3 $SP/live_probe.py $SP/live/rounds.jsonl --plan $SP/live/plan_rounds.txt               # 83
python3 $SP/live_probe.py $SP/live/ab.jsonl --plan $SP/live/plan_ab.txt                       # 50
python3 $SP/live_probe.py $SP/live/ttl1.jsonl / / /car/2002740 /car/2002740 /listings         # 5
python3 $SP/live_probe.py $SP/live/static.jsonl "/static/css/00-base.css?v=1792774194" "/static/nav_perf.js?v=1792774194" "/static/fonts/inter-latin-v20.woff2"   # 3
python3 $SP/live_probe.py $SP/live/scope2.jsonl "/api/listings/cars?zip=28105&radius=50"      # 4, spaced 0/+3/+6/+12 s
python3 $SP/live_summary.py $SP/live/ab.jsonl
```

**Live, second pass (101 requests)**:

```
python3 $SP/server2/live2.py $SP/server2/live2_run.jsonl 2026-11-10T01:50:00     # 61: /health, /, /; 8 x (/health, /api/ready); direct Railway domain /health x6;
                                                                                  #     car page across two 10-min boundaries; landing t0 and +590 s;
                                                                                  #     3 rounds of 5 pages; Dallas 75201/25 at 0/+3/+6/+12 s
python3 $SP/server2/live3.py $SP/server2/live3_run.jsonl                         # 20: /health and /listings alternating x10
python3 $SP/server2/live4.py $SP/server2/live4_run.jsonl 10                      # 20: /health and /api/ready pairs x10
python3 $SP/server2/timeline.py && python3 $SP/server2/contam.py                  # merge all agents' request times; flag overlaps
python3 $SP/server2/analysis.py > analysis_out.txt                                 # medians, pairs, edges
```

**Local, second pass** (outputs in `$R2/local`):

```
python -I $SP/conn_cost2.py 1270413 40                                    -> conn_cost2_r2.json
python $SP/server2/conn_ab3.py 80 conn_ab3_q{a,b}.json                    -> fresh / pool / borrow, interleaved
$SP/server2/cold_start.sh 8765 1 4 w1_r2 ; $SP/server2/cold_start.sh 8766 4 4 w4_r2
python -I $SP/server/loadgen.py <port> 8 {1,4,16} {/car/1270413,/listings} <label>   -> sweep_w1t4_r2.jsonl, sweep_w4t4_r2.jsonl
python $SP/server/ttl_loaders.py                                          -> ttl_loaders_r2.json   (superseded: double counted plausibility, see 4.2 and h_warm_loaders.py)
python $SP/server/ttl_probe.py ttl_probe_r2.json                          (~6.5 min)
python $SP/server/scope_breakdown.py 92618 25 28105 50                    -> scope_breakdown_r2.jsonl
python $SP/server/backfill_cost.py 15                                     -> backfill_cost_r2.json
USERS_DB_ENCRYPTION_KEY=<scratch> python $SP/server/users_conn_count.py   -> users_conn_count_r2.json
python $SP/server2/gc_probe.py gc_probe_r1.json                           -> gc.callbacks pauses, full-pass time
python $SP/server2/herd.py                                                -> herd_r1.json
/opt/homebrew/bin/python3.12 pyver_bench.py ; /opt/homebrew/bin/python3.14 pyver_bench.py   -> pyver_bench_r1.txt
python $SP/server2/interleave_probe.py ; python $SP/server2/refresh_thread_probe.py ; python $SP/server2/interleave_prof.py ; python $SP/server2/lru_probe.py
python $SP/server2/card_rev.py main v1.5.8 v1.5.9 v1.5.10 phase/N6b phase/AF phase/N37a   -> card_rev_out.txt
read-only SQL (psycopg, read-only session): select built_day, count(*) from listings_grid_cards group by 1 order by 1 desc;
```

**Round 3 (2026-11-10 evening Eastern; local only, plus one live request).** `$R3` =
`docs/web_speed_search_2026_11/server-causes-raw/r3`; `$PY` = `.venv/bin/python` (3.14.7). All run from the repo
root after `. $SP/sc3/env.sh` (a copy is `$R3/env.sh`: read-only `PGOPTIONS`, scratch users/admin SQLite files,
`KMAC_VAULT_DISABLE=1`, `DS_SKIP_REGISTRY_BACKFILL=1`). `h_*.py` are the R4 challenge's harnesses, re-run unchanged
(`$SP/sc3/run_r4_rerun.sh`); `r3_*.py` are new. Outputs are in `$R3` and `$R3/out`.

```
$PY $R3/h_double_count.py as_harness out.json ; $PY $R3/h_double_count.py real_path out.json     # twice each    -> out/double_count_rerun.log
$PY $R3/h_warm_loaders.py out/warm_loaders_r3.json            # 5 forced reloads per loader                 -> 4.2
$PY $R3/h_carpage_ttl.py  out/carpage_ttl_r3.json             # car and landing page per expiry event      -> 4.2
$PY $R3/h_herd.py         out/herd_r3.json                    # 4 threads at an expiry                      -> 4.3
MODES=baseline,swr                     $PY $R3/r3_swr_demo.py swr_demo_r3.json  5 3.5     # run 1, 4.6
MODES=baseline,swr,swr_si              $PY $R3/r3_swr_demo.py swr_demo_r3b.json 6 3.5     # run 2
MODES=baseline,swr,swr_grp,swr_yield   $PY $R3/r3_swr_demo.py swr_demo_r3c.json 5 3.5     # run 3
$PY $R3/r3_refresh_convoy.py out/refresh_convoy_r3.json  4 2000 700                      # hog_si5 hog_si05 pgload refresh_si5 refresh_si05 idle, 4.7
ARMS=facet_rebuild,idle $PY $R3/r3_refresh_convoy.py out/refresh_convoy_r3b.json 5 2000 700   # the facet-options-refresh job
$PY $R3/r3_gil_fetchall.py out/gil_fetchall_r3.json 5                                     # GIL held by fetchall / fetchmany / GROUP BY
$PY $R3/r3_gc_ballast.py out/gc_ballast_r3.json 40 0,1000000,3000000,6000000              # 10
/opt/homebrew/bin/python3.12 -I $R3/r3_gc_synth.py 100 0,1000000,3000000,6000000          # production interpreter, 10 (also python3.14)
(cd $R3 && $PY -I r3_traffic_model.py 60 7)                                               # 4.8 -> out/traffic_model_r3.txt
python -I $SP/sc3/live/live_get.py   # probe("/health"): ONE live request, 2026-11-11 01:35:52 UTC -> live/live_log.jsonl
```

R4's live log, quoted in 4.1, is `$R3/r4_live_exp.jsonl` (from `$SP/r4/out/live_exp.jsonl`; its harness is
`docs/web_speed_search_2026_11/r4_challenge_raw/live_experiment.py`). The `/health` series used in section 10
is `r2/live/live2_run.jsonl`, `live3_run.jsonl`, `live4_run.jsonl` (the 29 `sarraficars.com` samples).

**Code and history** (read-only): `git show phase/N37a:<path>`, `git diff main phase/N37a`,
`git diff --stat main phase/N37a -- <file ...>` (11.1), `git grep ... phase/N37a`,
`git show phase/N37a:CHANGELOG.md` (sections 1.5.9-1.5.16, saved at `$SP/sc3/CHANGELOG_n37a.md`),
`git log --oneline main..phase/N37a | wc -l` (117), `git rev-parse main phase/N37a`
(`0fd91872b...`, `faea2b985...`).

**First pass, still cited**: `server-causes-raw/local/` (`sweep_w1t4.jsonl`, `sweep_w4t4.jsonl`,
`contention_w1t4.json`, `pool_ab.jsonl`, `ttl_probe.json`, `ttl_loaders.json`,
`scope_breakdown.jsonl`, `backfill_cost.json`, `users_conn_count.json`, `conn_cost2.json`,
`cold_seq_w1.jsonl`, `scope_seq_w1.jsonl`).
