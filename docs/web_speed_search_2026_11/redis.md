# Redis for the web tier: where it pays and where it does not (2026-11-09, third pass 2026-11-10/11)

Scope: every in-process cache and per-process state in the web app, what each holds, its
size, how it is invalidated, and what breaks with more than one gunicorn worker. For each,
a decision: move to Redis, keep in the process, or keep in Postgres. Then a Redis design
(keys, TTLs, invalidation, compression, failure behaviour, Railway setup and cost) and the
gains it buys, quantified.

Code read: the live release `main` = 1.5.8 (commit `0fd91872b`, what sarraficars.com
runs) and the staged tip `faea2b985` = 1.5.16 (`phase/N37a`), read with `git show` /
`git grep` only; nothing was checked out. File:line references are to 1.5.16 unless
marked. Measurements ran the 1.5.8 working tree (the cache code is the same in both
except where noted) against the LOCAL Postgres 17 `cars` database (214,675 active cars),
with every session forced read-only. The live site got 41 anonymous GETs from this topic in total: 15 in
the first pass (13:42-14:17 UTC on 2026-11-09), 4 in a second pass (01:44-01:48 UTC on
2026-11-10, after the UTC day roll) and 22 in the third (01:33-01:55 UTC on 2026-11-11). Commands,
scripts and raw outputs: section 9, `redis_raw/` and `redis_raw/p3/`.

Second pass (2026-11-09 20:30-21:30 EST), added to the first draft of this file: the
Redis-down harness was re-run (3.8); the fleet-wide aggregates that reload on a shopper's
request were measured (3.11, new); a production gzip-header check proved that the ETag
changes on every rebuild (3.7); the body did not survive the UTC day roll (3.1 rows
16-18, 4.3); Cloudflare was seen passing origin gzip through to a client that offers
`br` (3.1 row 19, 4.5); the Railway Redis template manifest and docs were read (section
6, with URLs); the inventory was re-checked against a grep of every module-level
container at the 1.5.16 tip (3.2 rows 29-32); and the first draft's "12.7 Postgres
connections per car page" was replaced by an exact in-process count of 10 (3.4).

Third pass (2026-11-10 20:20 to 2026-11-11 EST), added to the second draft. The challenge
rounds that attacked this file (R1 ETag, R3 memory and concurrency, R4 aggregates, R5
compression and Cloudflare, and the day-roll simulation; raw outputs in
`r4_challenge_raw/` and the session scratchpad) were read, and every claim of theirs that
this file now relies on was **run again here** (`redis_raw/p3/`, section 9) rather than
quoted. What changed:
- **Corrected**: the memory headline (73% was a worker that never builds; a real
  cluster saves 8% with the store alone and 23% with streaming assembly, 3.12, 3.14); the
  concurrency framing (3.13); the featured-cars cost (a double count of the plausibility
  reload, 3.11) and the aggregates' real price on a request (3.11, live 3.16).
- **New measurements**: gunicorn 2 and 3 workers with a shared store, with and without
  streaming assembly, and the deploy-restart reader profile (3.12); the 1, 2, 4 worker load
  test again (3.13); what a builder keeps, with `tracemalloc` (3.14); a reference module and
  its tests: Lua sliding window, single-flight, 304 from one `HGET`, eight failure shapes
  (3.15); live checks of compression per `Accept-Encoding` and of the 10-minute boundary
  (3.16); the day-roll simulation (4.3).
- **New design**: alternatives to a Redis service (4.1), the facet key without `CARD_REV`
  (4.2), `SET ... GET` in place of the deprecated `GETSET` (4.2, 4.4), the producer outside
  the web workers (4.8, R4), the rollout order (7).
- **Resolved**: Cloudflare passes the origin's brotli through for static assets and does
  not upgrade the API's gzip (3.16); the Railway prices, Redis guide and template manifest
  were fetched again and match section 6.

---

## 1. Short answer

**Add Redis, in phases, and expect from it what the cluster measurements say.** One Redis
service in the existing Railway project, private network only, cache-only (`maxmemory
512mb`, `allkeys-lru`, no snapshots), about $1-6 a month. It does five jobs well. It does
not make one page load faster on a phone, and with the one worker the site runs today it
buys much less than with two or more. Pass 3 of this file re-ran the key experiments
with real gunicorn workers, absorbed the challenge rounds (their raw outputs are in
`r4_challenge_raw/`), and **corrected three headline claims** of the first two passes;
they are marked **corrected** below.

All numbers are local (macOS, Python 3.14, the real app on a read-only copy of the
production-shaped database, load average 8-12 from other agents). There is no Redis
server on this machine: the shared store in the cluster runs is a directory with the same
who-builds-what behaviour (3.12), and the Redis client, Lua and failure paths were tested
separately (3.15).

1. **One build per body for the whole cluster.** The same traffic needed 28-30 builds for
   21 distinct objects (the facets and 20 metros) with 2 workers today, 31 with 3, and 21
   with a shared store. A shopper whose request lands on the other worker gets the stored
   body in a median of 3.3 ms instead of rebuilding it (the single-process A/B: 774 ms
   median first hit when the worker builds). Each distinct body, about 1.25 MB gzipped
   for 28105 r50 (1.59 MB live), is built once per token and reused by every worker.
2. **Cold start after a deploy that keeps the token and `CARD_REV`.** New workers over a
   populated store served 8 simultaneous `/listings` in a median of **194 ms** (2,387 ms
   while populating) and 20 metros with a median of **7.6 ms** and 0.6 s of total waiting
   (1,088 ms and 31.3 s). Their first car page still took up to **3.1 s**, because the
   aggregates and the per-worker lookup caches are not in the store (3.11). Releases
   1.5.11, 1.5.12 and 1.5.16 each change `CARD_REV`, so each of those deploys invalidates
   the stored bodies anyway; facets keyed by the fingerprint alone survive them (4.2).
3. **Limits that are actually enforced.** The 1.5.16 rate limiter keeps its counters in
   process. With N workers every limit is N times larger, and every deploy resets the
   login lockout and the 24-hour AI-chat budgets. The Lua sliding window of 4.7 gives an
   exact shared limit (5 allowed of 8 attempts over 2 workers, 3.15), refunds only the
   caller's own slot and falls back to the in-process window when Redis is down.
4. **Billable calls survive a deploy.** LLM narrations (30 days) and Google Places lookups
   (6 h) are cached per worker and lost on every deploy today.
5. **Single-flight and shared claims**: one build per scope, one card refresh, one sticker
   fetch per car, one registry backfill, across workers.

**Corrected 1: memory.** The first draft said 73% less at peak. That figure is a worker
that **never builds** (331 MB against 1,297 MB end RSS in the single-process A/B, 3.3,
reproduced here as 331.5 against 1,297.2 MB). A cluster still builds every body once, and
the worker that builds keeps the memory. Two real gunicorn workers, sum of per-worker peak
RSS over two seeds (3.12): **3,145 MB today, 2,898 MB with the shared store alone
(-8%; seeds -19% and +4%), 2,768 MB with streaming assembly alone (-12%), 2,410 MB with
both (-23%; seeds -29% and -17%)**; three workers (one seed) 3,203 MB against 2,489 MB
(-22%). One worker today peaks at 2,199 MB, so two or three workers with the store and
streaming assembly cost about 10-13% more than one worker does now. The challenge round
got -3% (peak) to -10% (end) for the store alone with three seeds. What a builder keeps is
**allocator retention, not data** (3.14): after five big scopes and a cleared cache,
Python held 131 MB while RSS was 1,653 MB; streaming assembly alone ended the same run at
RSS 1,132 MB and USS 424 MB (-32% and -64%). The reader profile (new workers over a
populated store: 328 + 326 MB end, 812 MB sum of peaks) needs the builds to happen
outside the web workers (R4).

**Corrected 2: concurrency.** Car pages per second with 8 clients (3.13, the same load
test as the first draft, whose 1- and 2-worker rows it reproduces within 3%): **30.2 with
one worker, 52.4 with two, 62.5 with four**; p95 332, 253 and 235 ms. With one shopper
loading an uncached big metro the car-page p95 is 751 ms on one worker, 364 on two and 200
on four. Two workers with
the store and streaming assembly: 57.2 req/s, p95 221 ms (+9% and -13% against two workers
today), and 349 ms against 364 ms with the noisy shopper, because that shopper's metros are
all new and nobody else could have built them. Redis makes the second worker **correct**
(limits, locks, claims) and cheaper; it does not make the second worker faster. The
plateau at four workers is not a Redis question: each car page opens about 10 Postgres
connections, and server-causes.md measured a pool cutting a warm car page from 41-44 to
20-26 ms.

**Corrected 3: the fleet aggregates.** The first draft priced the landing page's featured
cars at 767 ms; that number contained a second reload of price plausibility (3.11). The
real picture is worse for the shopper and simpler to fix. Rarity counts (600 s bucket,
no lock), price-plausibility medians (300 s), the public count and the fallback URLs
reload on whichever request finds the TTL expired: a car page then takes **0.83 s**
(rarity), **0.91 s** (plausibility) or **1.86 s** (both) locally instead of about 0.1 s,
the landing page 0.90 s instead of 4 ms when featured cars and plausibility expired
together, and four concurrent callers at a bucket edge ran the rarity loader four times.
**Live**, the first car page after a 10-minute boundary costs 2.5-3.6 s at the origin
against 0.26-0.52 s for the next one (3.16: 2,530 and 3,550 ms against 282 and 522 ms;
server-causes.md 4.1: 2.9 and 3.3 s against 0.26-0.31 s). Fix: compute them off the
request path, with one producer and a Postgres row per document; Redis may hold a copy.
Nothing about this needs Redis, and Redis does not make it go away.

**What Redis does not fix.**
- On a warm cache, the user-visible time is the transfer. The live 28105 r50 body is
  1,585,587 bytes gzipped: 2.56-3.14 s on this machine's link at 13:43 UTC (0.54-0.62
  MB/s, the same as Cloudflare's own speed test then) and 0.44-0.56 s at 01:33 UTC. The
  link decides.
- Brotli-5 instead of gzip-6 makes the 20 measured bodies 48.8% smaller (56.1 MB to 28.7
  MB) and compresses faster. Phone-emulated, 28105 r50 loads in 3,744 ms instead of
  6,480 ms on Slow 4G (3.6, re-run in pass 3). Cloudflare will not
  do it for the API: a browser's `gzip, deflate, br, zstd` got the origin's gzip back, and
  an edge conversion for a br-only client was 1,596,934 B, bigger than the gzip (3.16).
  Brotli has to be produced by the origin; that is the `br` field of the stored body.
- The UTC day roll. The token carries `_today()`, so every cached body, in process or in
  Redis, is invalid from 00:00 UTC (19:00 Eastern in winter), and day-dependent cards
  built the day before count as aged (`_is_aged`), which makes the first body of each
  metro `partial`. Live check (3.1 #17): **TTFB 3.45 s**, then 0.82 s, and a body whose
  decoded JSON was byte-identical to the previous day's. Redis alone changes nothing here;
  stale-while-revalidate across the roll and a producer (R4) do (4.3).
- Per-statement Postgres connections, the 1.4 MB `/listings` HTML, the distance from the
  `us-west2` web service to East Coast visitors: other reports.

**If the site stays at one worker**, Redis buys the cold start after a deploy that does not
change `CARD_REV` (2.4 s to 0.19 s for the first `/listings`, 774 ms median to 3.3 ms for a
metro), limits and billable-call caches that survive a deploy, and nothing for memory or
throughput. R0 is worth more: stable ETags, the aggregates off the request path, streaming
assembly, in-process stale-while-revalidate.

**Recommendation.**
- Ship R0 (no Redis) first, then R1 (shared token, bodies, facets), R2 (rate limiter), R3
  (money savers and search keys), R4 (a producer outside the web workers), then
  `GUNICORN_WORKERS=2`. Go to 3-4 workers only together with a psycopg pool (psycopg-pool
  3.3.3 is already installed in the dev venv).
- One Redis service: project `dealership-scanner`, environment `production`, private
  network only, `maxmemory 512mb`, `allkeys-lru`, snapshots off. No new Python dependency:
  `redis>=8.0.1` and `brotli>=1.1` are already in `requirements-web.txt:23,36` (checked at
  `faea2b985`). The client costs 20 ms and 1.5 MB per worker to import (3.15).
- Rule for where state lives (4.1): derived bytes that the web builds and several workers
  want go to Redis; aggregates a pipeline can compute go to a Postgres table; static
  per-deploy lookups stay in the process; durable facts stay in Postgres.

---

## 2. What was measured, and where

| | |
|---|---|
| Machine | Apple Silicon, 16 cores, 48 GB, on AC power; macOS (Darwin 27.3.0). **Shared with other agents**: load average 8-12 during the pass-3 runs, 15-24 at the end of the load series |
| Python | `.venv` 3.14.7 (production image is `python:3.12-slim`, `Dockerfile.web:1`); redis-py 8.0.1 (no hiredis), brotli 1.2.0, stdlib `compression.zstd`. Pass 3 also used a scratch venv on Python 3.12 (the production minor) with fakeredis 2.40.0 (real Lua through lupa 2.8) and redis-py 8.1.0, outside the repository |
| Database | local Postgres 17.10, `postgresql:///cars?host=/tmp`; 214,675 active cars, `listings_grid_cards` 214,679 rows (530 MB), `nhtsa_vpic_cache` 912 MB |
| Read-only | `PGOPTIONS='-c default_transaction_read_only=on'` on every process, so Postgres refuses any write. The app's own GET-path writes (card-store repair, `row_ver` touch, the registry backfill UPDATE) were refused, and the harnesses turned card-store writes and the background refresher into no-ops. `init_job_queue_schema()` (DDL at import) was a no-op. SQLite users/admin/portal/incomplete DBs were scratch copies (pass 3: new empty scratch files). |
| Redis server | **none on this machine.** Nothing was installed. Client-side behaviour was measured against closed ports, a hung listener and a minimal RESP stub server (sections 3.8 and 3.9). Redis server memory overhead and Railway private-network latency are therefore NOT measured. Section 8 gives the commands to measure them on Railway. Pass 3 added two stand-ins: a shared directory with the same who-builds-what behaviour for the multi-worker runs (3.12, 3.13), and fakeredis plus failing sockets for the command and failure checks (3.15). |
| Live site | 19 anonymous GETs, at least 1.1 s apart (`redis_raw/live_get.sh` enforces the gap and a cap of 120; `live_get_ae.sh` is the same with an `AE` override for `Accept-Encoding`), no cookies sent, no logins, no forms. Pass 3: `redis_raw/p3/live_get.py` (same rules, 1.2 s gap, budget 80), 22 requests |
| Railway facts | Railway docs and the Railway Redis template manifest, read with WebFetch on 2026-11-09 (URLs in section 6). Prices and template settings are quoted, not measured. Pricing, the Redis guide and the manifest were fetched again on 2026-11-11 and match. |

Caveats on local numbers: macOS RSS is not Linux RSS (macOS compresses idle pages, and
several runs showed a worker's RSS fall after the load: in 3.12 the end-of-run sum of one
seed was 1,973 MB against a 3,055 MB sum of peaks). The A/B claims use peak `ru_maxrss`
and the cluster claims the sum of the per-worker peaks sampled every 0.2 s. The laptop was
shared with other agents' Chromium and Python jobs, so timings carry about +-30%. The
production 3.3 GB figure for one warm worker (`gunicorn.conf.py` docstring) predates the
radius-scoped listings and was not re-measured here. Section 8 has the owner check.

---

## 3. Measurements

### 3.1 Live site (sarraficars.com, 1.5.8)

`redis_raw/live_get.sh <label> <path>` is curl with `Accept-Encoding: gzip`, using
`-w` for dns/connect/tls/ttfb/total/size.

| # | time (UTC) | request | result |
|---|---|---|---|
| 1 | 13:42:35 | `GET /api/ready` | `{"checks":{"db":"ok","pgvector":"skipped","redis":"skipped"},"ready":true}`: **REDIS_URL is not set in production** |
| 2 | 13:42:36 | `GET /api/health` | `version 1.5.8`, commit `0fd91872b` |
| 3-5 | 13:42:59-13:43:07 | `GET /api/listings/cars?zip=28105&radius=50` x3 | 200, **1,585,587 B gzip**, ETag `W/"lc-28105-50-82e728fb3aa888a49f33"` on all three, TTFB 0.228-0.282 s, total 2.56-3.14 s, `Cache-Control: public, max-age=0, s-maxage=60, stale-while-revalidate=30`, `Vary: Accept-Encoding, Cookie`, `cf-cache-status: DYNAMIC` (Cloudflare does not cache it) |
| - | | (control) `speed.cloudflare.com/__down?bytes=1585587` x2 | total 2.72 / 2.95 s, 537-583 KB/s: the 2.6-3.1 s is this machine's link, not the site |
| 6 | 13:43:41 | `GET /api/health` | TTFB 0.241 s with TLS done at 0.079 s: about 160 ms is the Cloudflare to Railway round trip plus the app |
| 7 | 13:43:44 | `.../cars?zip=37421&radius=25` (not cached) | TTFB **1.362 s**, 462,311 B, `x-listings-partial: 1`, `public, no-cache` (stale cards served, background refresh queued) |
| 8 | 13:43:47 | same, the next request | TTFB **1.662 s** (rebuilt: the partial entry's process-local `store_generation` moved when the refresh finished), 464,904 B, new ETag, `s-maxage=60` |
| 9 | 13:43:50 | same + `If-None-Match` | **304 in 0.264 s** |
| 10 | 13:56:53 | `GET /api/listings/filter-options` | TTFB 0.287 s, 86,107 B gzip, no ETag, no Cache-Control |
| 11 | 13:56:55 | `GET /listings` | TTFB 0.839 s, 167,791 B gzip (1,548,519 B HTML) |
| 12 | 13:56:57 | `GET /api/listings/geo-coords` | **Cloudflare 524 after 125.2 s.** 1.5.8 runs the ~210 s dealership-registry backfill inside the first call per process (`listings_repo.py` on `main`). 1.5.11 (staged) moved it to a startup thread. This request triggered it once (an idempotent UPDATE of unstamped registry ids, the app's own design), and it was not repeated. |
| 13 | 13:59:03 | 28105 r50 + `If-None-Match` of #3 | **304**, same ETag 16 min later: not rebuilt, so the grid token was stable and the process was not restarted |
| 14 | 14:03:29 | `/static/ds_core.js` with `Accept-Encoding: br, gzip` | gzip, `cf-cache-status: HIT`. That was a cached gzip variant; a cache MISS with a Chrome-like header returned the origin's brotli (3.16). |
| 15 | 14:17:04 | 28105 r50 + `If-None-Match` of #3 | **304**, same ETag **34 min** after #3 (TTFB 0.566 s): the token stayed stable and the body was neither rebuilt nor evicted |
| 16 | 01:44:19 (11-10) | `GET /api/health` | 200, TTFB 0.340 s, still `version 1.5.8`, commit `0fd91872b`: no deploy between #2 and here |
| 17 | 01:44:21 | 28105 r50 + `If-None-Match` of #3 (the ETag was valid for 34 min in #15) | **200, not 304**: TTFB **3.450 s**, total 3.84 s, 1,585,585 B, new ETag `W/"lc-28105-50-5a4d0588a6e3fdc55929"`, **`x-listings-partial: 1`**, `public, no-cache`. The body survived 34 minutes in #15 and did not survive the interval that contains the 00:00 UTC day roll (consistent with `_today()` in the token; from outside, a data write looks the same, section 8) |
| 18 | 01:44:49 | same URL, no validator | 200, TTFB **0.824 s** (an inline rebuild: the gzip header time of the body is 01:44:49), 1,585,587 B, third ETag `W/"lc-28105-50-67fb85d758fd3550f20c"`, complete (`s-maxage=60`, no partial header) |
| 19 | 01:47:54 | same URL, `Accept-Encoding: gzip, br` | 200, **`content-encoding: gzip`**, same ETag as #18, TTFB 0.311 s, total 0.49 s (3.2 MB/s on this link at 20:47 local; #3-5 ran at 0.5-0.6 MB/s). Cloudflare passed the origin's gzip through to a client that offers `br`, and `cf-cache-status: DYNAMIC` again |

Rows 16-19 are in `redis_raw/live_headers/` and `live_timings.log`; the body analysis is
`redis_raw/gz_header_proof.py` (section 3.7).

What this says for Redis:
- Within a UTC day the token is stable for long stretches (34 min in #13-#15), so cached
  bodies stay valid. What throws them away is the process (deploys, restarts), the
  64-entry/96 MB LRU, and, by the code, **the UTC day roll** (#17): the token tuple
  carries `_today()` (`grid_cards_repo.py:212-219`), so at 00:00 UTC every metro is
  rebuilt on its first request, at 19:00 Eastern. #17 fits that (no deploy, #16; a
  stable token for 34 min the day before). In #17-#18 the rebuild produced decoded JSON
  identical to the previous day's (3.7), so whatever moved the token, the rebuild was
  pure cost for this metro.
- A metro that is not cached costs one or two builds of 0.8-3.5 s each on the one worker
  (#7, #8, #17, #18). While a build runs, the worker's other threads are slowed.
- Cloudflare is not a second brotli encoder for this endpoint (#19), and it caches none
  of it (`DYNAMIC` on every row). Origin-side brotli and an edge cache rule are separate
  decisions (4.5).

### 3.2 Inventory: every in-process cache and per-process state

Sizes are local measurements (`redis_raw/harness_inventory.py`,
`harness_inventory2.py`). The "With N workers today" column is what goes wrong with
`GUNICORN_WORKERS` > 1, read from the code. Items 1 and 2 were reproduced with several
workers (3.4, 3.12, 3.13).

| # | State (1.5.16 location) | Holds | Size, measured | Invalidation today | With N workers today | Decision |
|---|---|---|---|---|---|---|
| 1 | `_cars_scope_cache` + striped build locks, `routes/listings_api.py:225-253` | gzipped `/api/listings/cars` bodies per (zip, radius, hidden-dealer set) | 20 metros: **56.1 MB gzip** held (0.30-7.42 MB each, 3.9-67.3 MB raw; table 3.5); cap 64 entries / 96 MB | `grid_scope_token()` (below) plus process-local `store_generation` for partial bodies | N copies, N builds per metro (2 workers: 28-30 builds for 21 distinct objects, 3 workers: 31; 3.12); an OC-sized build adds +150-213 MB RSS to the worker that ran it and the worker keeps it (3.14). ETags differ per worker on Python 3.12 (3.7), so 304s fail when requests alternate workers. | **Redis (L2)** for complete, unhidden bodies; small L1 (32 MB) kept |
| 2 | facets, `get_filter_options()`, `db/repositories/listings_repo.py:102-104, 633-665` | whole-fleet facet lists + 23,156 cascade rows | dict **16.6 MB** (tracemalloc of a loaded copy); build **2.18 s and +212.7 MB RSS**; JSON 3.73 MB / gzip 219 KB; `json.loads` 17.2 ms | grid token; background rebuild at most every 300 s (`FACET_REBUILD_MIN_INTERVAL_S`, staged) | every worker builds (2.2 s, +213 MB) at its first `/listings` and on every rebuild | **Redis** keyed by the fingerprint alone, not `CARD_REV` or the day, so it survives card-code deploys and the roll (gzip JSON + the API body + the packed rows); dict stays as L1 |
| 3 | `_token_cache`, `grid_cards_repo.py:191-224` | the grid token: (pg_stat write fingerprint of 5 tables, day, `CARD_REV`, market-band generation, attribution generation, include-incomplete) | fingerprint query 2.83 ms median (max 12.55); first token 78.6 ms | resampled at most every 60 s per process | workers sample at different moments, so their tokens differ for up to 60 s | **Redis shared epoch** `ds:v1:tok` (60 s) + 2 s L1 |
| 4 | `_dealer_cards_cache`, `routes/dealership_page.py:158-213` | card JSON strings per dealer | top 20 dealers: **37.7 MB** + 1.6 MB str headers (biggest 3.9 MB); cap 128 dealers | token + `store_generation`, TTL 30 s, entries kept until LRU evicts them | N copies | Redis (R3), gzip body per dealer; small L1 |
| 5 | `_attr_cache` (60 s) `:235`, `_market_gen_cache` (60 s) `:319`, `_fallback_urls_cache` (300 s) `:819`, incomplete snapshot `data_quality_repo.py:32`, `listings_geo_coords_maps` `listings_repo.py:119-121` | inputs of a card build; geo maps 114 KB JSON (29 ms) | small (attribution has 0 rows locally) | TTLs | N small copies | **keep in process** (needed by whoever builds; only the resulting token is shared). `_fallback_urls_cache` reloads in 161 ms inline in a scope build (3.11), so it moves with the aggregates in row 31. |
| 6 | background refresher `_bg_queue`/`_bg_pending`/`_store_generation`, `grid_cards_repo.py:530-611` | stale card ids queued for re-serialisation | queue up to 64 chunks of 2,000 ids | per process | each worker re-serialises the same stale cards (0.81 ms per card); a partial body on worker B is not invalidated by A's refresh | keep the queue in process; **Redis claim** (`SET NX`) per scope so one worker refreshes; partial bodies never go to Redis |
| 7 | `_VPIC_MEMO`, `enrichment/knowledge_engine.py:1268` | normalised vPIC decode per VIN, primed for every card serialised | **445 B per entry** (20,000 entries = 8.9 MB); **unbounded**: at 214,675 active VINs that would be ~96 MB per worker (extrapolated) | never | N copies | **keep in process, bound it** (LRU 50k ≈ 22 MB); data stays in Postgres `nhtsa_vpic_cache` |
| 8 | `_PHOTO_COUNT_MEMO`, `utils/car_serialize/serialize.py:326` | photo count per gallery digest | bounded 200,000 | none | N copies | keep |
| 9 | 65 `@lru_cache` functions (EPA, catalog, trim ladders, brochures, model specs, generations, extended specs, vPIC hits, `adapt_sql_for_postgres_execute`, ...) | static reference lookups | after 60 car pages: 36 functions hold **1,175 entries** in total | process life | N small copies | **keep in process**: sub-microsecond hits called thousands of times per page (e.g. `_ladder_matches_car` 1,905 calls per page, runtime-timing.md); even the 0.041 ms loopback round trip measured in 3.9 is far slower than an in-process hit, and the data is static per deploy |
| 10 | deal-score bands `intelligence/deal_score_cache.py:43`, msrp bands `utils/msrp_trust.py:305`, `_epa_makes_cache`, `_host_index_cache` `attribution/place.py:206`, `_static_cache_ver_memo`, gas prices (file mtime) | small reference tables | 584 bands etc.; deal-score bands 4,188 entries, 9.7 ms warm reload (JSON 1.0 MB / gzip 101 KB, 3.11) | TTL / mtime | N small copies | keep (the multi-second reloads are rows 29-31, not these) |
| 11 | rate limiter `utils/ip_rate_limit.py:13-66` (`allow_request`, `refund_event`), 42 `allow_request` call sites | sliding-window timestamps per key | **200 B per key**, 0.6-0.84 µs per call | window | every limit x N; all counters reset on deploy | **Redis** (sorted-set Lua), falls back to in-process |
| 12 | login lockout `auth/pages.py:54-75` (`_ACCT_FAIL_MAX=10` per 900 s, slot reserved before the bcrypt check, refunded on success) | failure slots per account | via #11 | 900 s | 10 x N guesses per 15 min per account; a deploy unlocks everything; `refund_event` may free a parallel caller's slot (documented at `ip_rate_limit.py:62-66`) | **Redis** via #11, refund by member id |
| 13 | AI chat / narrate limits (`ai_chat_bp.py:159-217`, `cars_pages.py:582-718`, `ai_narrate_bp.py:82-86`): per-minute and **86,400 s** windows | per user/IP/listing budgets (40 per user per day, 10 per listing per day by default) | via #11 | window | daily budgets x N, and **reset at every deploy or restart** | **Redis** via #11 |
| 14 | `listings-build:<ip>` limiter, 20/min (SEC-A-005, staged `listings_api.py:422`) | cache-miss builds per IP | via #11 | 60 s | x N | Redis via #11; a Redis hit is not a build and is not counted |
| 15 | LLM narration LRU `routes/ai_narrate_bp.py:43` | narration text per (car, content hash) | up to 2,000 entries | content hash | N x billable model calls; lost on deploy | **Redis**, 30 days |
| 16 | Google Places `_google_cache`, `listings/dealer_locator.py:142` | Places results per rounded centre | up to 256, 6 h | TTL | N x billable Places calls; lost on deploy | **Redis**, 6 h (same policy as now) |
| 17 | market cohorts `_cohort_cache`, `utils/market_price.py:31` (paid feature) | price lists per (zip, radius) | 28105 r50: 4,514 groups, pickle 278 KB / gzip 75 KB; build **0.78 s, +202 MB RSS** (full active-fleet scan per miss) | TTL 900 s | N scans | Redis (short term, JSON); SQL aggregation (long term) |
| 18 | query-parser vocabulary `utils/query_parser.py:117,174`, `hybrid_search.py:78` | make/model pairs (2,974), trims (7,276), colours, packages (11,857) | JSON 777 KB / gzip 182 KB; build **1.86 s, +110 MB RSS** | key = stat of the SQLite file (`backend/inventory.db`), constant under Postgres, so stale until restart (routes-and-views R8) | N builds | **Redis**, keyed by the token day; this also fixes the staleness |
| 19 | packages-ensure `routes/car_detail/packages_ensure.py:88-100` | in-flight sticker thread per car, cooldown, panel cache | bounded 20,000 | 900 s cooldown, 60 s panel | two workers can fetch the same sticker; "fetching" shows wrongly on the other worker, so the client's poll stops | Redis claim and cooldown keys (only when workers > 1). The fetch itself is unchanged. |
| 20 | billing revalidation `billing/entitlements.py:39,66` | plan/premium per user and org | up to 10,000 | 60 s; `invalidate_billing_cache` from webhooks (`:89`) is local | a cancel reaches other workers up to 60 s late | Redis key (60 s) + webhook DEL |
| 21 | recommendations `routes/home_dashboard.py:32` | per-user carousel | up to 500 | 120 s; invalidated on view (local) | up to 120 s stale on other workers | Redis with a per-user version (low priority) |
| 22 | landing `_featured_cars_cache` (300 s) and `_public_count_cache` (fingerprint), `listings_repo.py:236,493` | 12 cards; one count | tiny (4 cards = 4.2 KB JSON); the reload is **10 ms** (featured, plausibility warm; the first draft's 767 ms was a double count) and **56 ms** (count), and **898 ms** on a landing request when plausibility expired with it (3.11) | TTL / fingerprint | N tiny copies, N slow reloads | **compute off-request** (row 31); the size is not the issue |
| 23 | `/dev` job store `dev/routes.py:91-94` | scanner jobs, import queues (admin) | up to 500 / 100 | none | a status poll on another worker answers `job_expired` | **Postgres** table (admin-only, durable; the code comment already says so) |
| 24 | registry backfill thread `main.py:135-166` | one-time UPDATE pass | ~210 s of seq scans (changelog 1.5.11) | process flag | runs once per worker, at the same time | **Postgres advisory lock** (`pg_try_advisory_lock`) |
| 25 | session: Flask signed cookie, `main.py:79-83` | login state, area | client-side | 14 days sliding | none: stateless | **keep cookies**; server-side sessions would add a Redis hit to every request for no gain |
| 26 | sentence-transformer `_st_model`, `vector/pgvector_service.py:33` | embedding model (smart search) | `/api/ready` says `pgvector: skipped` (no `PGVECTOR_URL`); whether smart search loads the model in prod was not checked | none | N model copies if loaded | keep; optional Redis cache of query embeddings if pgvector is used |
| 27 | Postgres-backed shared state: `listings_grid_cards`, `nhtsa_vpic_cache`, `market_price_stats`, `dealer_jobs` (scanner queue), history, reviews | durable shared data | 530 MB / 912 MB tables locally | n/a | already shared | **keep in Postgres**. Redis would duplicate the card store at higher cost and add a second source of truth for the job queue. |
| 28 | `/api/ready` Redis client `routes/health.py:33-48` | lazy client | n/a | n/a | per worker | replace with the shared client (section 4.6) |
| 29 | `rarity_score._counts_cached`, `utils/rarity_score.py:94-100` | whole-fleet make/model/trim/colour frequency Counters (`model` 1,891, `trim` 10,497, `color` 6,280, `color_known` 1,865 entries) | reload **717.8 ms** (median of 5, warm), +32.6 MB RSS; JSON 747 KB / gzip 161 KB; decode 8.2 ms (3.11) | `lru_cache(maxsize=2)` keyed by `int(time.time() // 600)`, no lock | one 0.7-1.0 s load per worker per bucket, paid by the car page that arrives first; no lock: 4 concurrent callers at a bucket edge ran it 4 times, 2.0-2.5 s each (3.11) | **compute off-request** (pipeline step or one refresher), **Postgres table first**, Redis optional (3.11); the inline load stays as the cold fallback |
| 30 | `price_plausibility._load`, `utils/price_plausibility.py:54,91-130` | per-(make, model, trim, year) and per-(make, model) median price, from two `percentile_cont` scans of all active cars | reload **867.5 ms** (median of 5, warm); 22,382 + 1,845 entries; JSON 1.19 MB / gzip 220 KB; decode 12.0 ms | TTL 300 s under a module lock (concurrent callers queue behind it) | one load per worker per TTL, and every grid-card serialisation waits on the lock | as row 29. The medians could ride on the nightly `market_price_stats` job that already feeds the deal-score bands. |
| 31 | other TTL reloads on a request thread: `landing_featured_cars` `listings_repo.py:493-494` (300 s), `grid_cards_repo._fallback_dealer_urls` `:819-827` (300 s), `public_listings_count` `listings_repo.py:236-273` (fingerprint, 300 s), `_attribution_read` `grid_cards_repo.py:235-247` (60 s) | 4 cards (4.2 KB JSON); 80 URLs (7.1 KB); one integer; the attribution verdict map | **10 ms** (plausibility warm), **162 ms**, **56 ms**, **63 ms** (3.11; the 63 ms attribution read is from the first pass) | TTL | N loads | featured cars and public count: published row read by the landing page; fallback URLs: published with the aggregates; attribution map: keep in process (an input to every card build) |
| 32 | legacy whole-fleet grid `_grid_cars_cache_value` + `_grid_serialize_memo`, `listings_repo.py:122,334` | the full serialised fleet (the "~2 GB" in the `gunicorn.conf.py` docstring) | **not built**: no web route calls `listings_grid_serialized_cars` (git grep on 1.5.8 and 1.5.16: only tests, several of which assert it is forbidden on web routes) | n/a | n/a | **never put in Redis**; delete the dead code when convenient |
| 33 | `dev/scan_lab.py:44` `scan_lab_jobs` + `scan_lab_lock` (admin) | scan-lab job records | small | none | a status poll that lands on the other worker misses the job | **Postgres**, with row 23 |
| 34 | `dictionary/enrich_from_dictionary.py:116` `_epa_cache`, `attribution/place.py:202` `HOST_ALIASES`, `utils/client_ip.py` CIDR memos | static lookups | small | process life | N small copies | keep |
| 35 | the thread set of a worker: 4 request threads share one GIL with `grid-cards-refresh`, `grid-cars-refresh`, `facet-options-refresh`, `listings-prewarm`, `registry-backfill` (`main.py:131,166`, `grid_cards_repo.py:582`, `listings_repo.py:488,626`) | threads, not data | - | - | every worker starts its own set; N workers run N refresh threads that re-serialise the same stale cards and N registry backfills | Redis claim for the refresh (row 6), advisory lock for the backfill (row 24) |

Completeness check at the 1.5.16 tip (`redis_raw/p3/inv_grep.py faea2b985`, a `git grep` of
the decorators and module-level containers outside tests): 65 `lru_cache` functions in 37
files (as in row 9), 46 module-level containers in 35 files, 31 module-level locks, 51
thread or executor starts in 23 files, 88 `global` statements in 44 files. Every web-path
hit is in the table above; the scanner-side containers (`scanner/vdp/prefetch.py`,
`scanner/recipes.py`, `scanner/rooftop_ledger.py`) never run in the web process. Flask-
SocketIO is installed but unused (`docs/CLOUD_RESTRUCTURE_PLAN.md:57-66` plans its removal),
so there are no Socket.IO rooms to share. No billing webhook keeps an in-memory
idempotency set.

### 3.3 A/B: one worker, same traffic, today vs "facets and bodies arrive as bytes"

`redis_raw/harness_ab.py build|shared`. Mode `shared` replaces
`get_filter_options()` with `json.loads` of the facets document mode `build` produced.
It also replaces `_build_cars_scope_entry` with reading the gzip bytes mode `build`
produced, which is what a Redis GET returns. The L1 is capped at 32 MB. The traffic is
the same in both modes: `/listings`, `/api/listings/filter-options`, the 20 scopes of 3.5
through the real route, 60 car pages, then 3 more rounds of `/listings` + 5 scopes.

| | today (`build`) | shared store (`shared`) |
|---|---|---|
| first `/listings` | **2,796.6 ms** | **93.3 ms** |
| first hit on 20 metros: median / max / sum | 810.8 / 2,417.9 / 21,179.9 ms | 3.4 / 97.9 / 184.9 ms |
| L1 bytes held at the end | 56.1 MB (20 entries) | 27.7 MB (4 entries, 32 MB cap) |
| RSS at the end | **1,248.6 MB** | **333.2 MB** |
| peak RSS (`ru_maxrss`) | **1,402.8 MB** | **374.1 MB** |

Per scope, today's build adds RSS that is not given back: +213 MB for 92663 r50 (58 MB
raw body), +196 MB for r100, +150 MB for r250 (`redis_raw/ab_build.json`). The transient
is the 58 MB `",".join` string, its 58 MB `.encode()` copy, and the candidate dicts. A
streaming compressor removes most of it (3.6). With Redis, only the worker that builds a
scope pays this, once per token for the whole cluster.

**Read this A/B as the best case.** The `shared` process never builds: it only reads. A
cluster still has to build every body once, and the worker that builds keeps the memory.
Pass 3 reproduced this table (build: end RSS 1,297.2 MB, peak 1,383.7 MB; shared: 331.5 and
371.9 MB; first `/listings` 3,196 ms against 95 ms; scope first-hit median 774.2 against 3.3
ms) and then measured real gunicorn clusters in 3.12, where sharing alone is worth 8% and
sharing plus streaming assembly 23%.

### 3.4 Concurrency: gunicorn 1 vs 2 vs 4 workers (local, read-only DB)

`redis_raw/run_load.sh <workers>` runs gunicorn 26 with `-c gunicorn.conf.py --threads 4
--preload`, then warms up and runs `loadtest.py`. 8 (or 16) client threads loop over 150
car pages for 25 s. `--noisy` adds one client that requests a new Orange County metro
(radius 50, about 48k cars, about 6 MB gzip) back to back: one shopper landing on a big
uncached metro.

**Machine caveat.** These runs shared the laptop with other agents' Chromium, pytest and
load jobs, and `loadtest.py` opens a new TCP connection per request. Read the
1-versus-2-worker rows and the noisy-shopper p95 as the result, and the 4-worker rows as
a lower bound (server-causes.md saw 111.6 req/s at 4 workers, see below).

| workers | clients | car pages/s | p50 | p95 | p99 | with the noisy shopper: pages/s, p50 / p95 / p99 | noisy build, median |
|---|---|---|---|---|---|---|---|
| 1 | 8 | **31.0** | 252 | 337 | 396 | **21.3**, 307 / **763** / 1,081 | 4,259 ms |
| 2 | 8 | **51.3** | 147 | 253 | 349 | **46.4**, 143 / **332** / 620 | 5,009 ms |
| 4 | 8 | 50.3 | 143 | 311 | 391 | **51.7**, 144 / **244** / 329 | 4,492 ms |
| 1 | 16 | 30.1 | 525 | 619 | 655 | - | - |
| 2 | 16 | 54.5 | 287 | 456 | 519 | - | - |
| 4 | 16 | 55.7 | 251 | 525 | 683 | - | - |

Worker RSS:
- After warm-up: 1 worker 749 MB; 2 workers 529 + 579 MB; 4 workers 472, 493, 508 and
  544 MB. The RSS shows that every worker built its own facets.
- After the noisy runs: 1 worker 1,174 MB; 2 workers 1,017 + 1,148 MB; 4 workers 475,
  506, **1,015 and 1,213** MB. Only the two workers that happened to build OC scopes grew,
  by about 0.5-0.7 GB, and they keep that until they restart.

Why 4 workers did not beat 2 in this run is **not established** (`redis_raw/cpu_sample.sh`,
4 workers, 16 clients, 25 s):
- Connections per car page: **median 10, range 7-11** (`redis_raw/harness_conn_per_page.py`:
  100 warm anonymous pages, counted inside the process at `psycopg.connect`; mean 10.09
  statements per page; 29 statements across those pages were refused by the read-only
  session; median page 40.4 ms, p95 59.9 ms). runtime-timing.md counted 8-10. The load run
  itself saw `pg_stat_database.sessions` rise by 13,660 for 1,073 pages (12.7 per page),
  but that counter is database-wide and also counts any other session opened on the shared
  local Postgres during those 25 s, so 12.7 is an upper bound and 10 is the figure to use.
  At 42.5 pages/s, 10 per page is about 425 connects/s.
- Host-wide CPU samples during that run showed 70.4-77.5% busy, 35.5-40.0 points of it
  kernel (sys) time, and all Python processes on the machine together at 2.1-2.7 cores.
  **These are host-wide, so they cannot be attributed to the four workers or to
  connection churn**: the laptop was shared with other agents' Chromium, pytest and
  replay jobs (at the re-check at 21:04 the load average was 9.97, with a pytest run at
  212% CPU and a Chromium at 82%). With the samplers running, this run did 42.5 pages/s.
- server-causes.md measured the same route on the same laptop with keep-alive
  connections and one car URL (`server-causes-raw/local/sweep_w4t4.jsonl`): 4 workers
  reached **111.6 req/s** at 8 clients (p50 67.8 ms) and 87.5 req/s at 16 clients,
  against 34.1 req/s for 1 worker. My 4-worker figure (50.3 req/s at 8 clients, p50
  143 ms; 150 different cars; a new TCP connection per request) is about half of that,
  and I could not rerun the comparison on a quiet machine.

**What is established:** going from 1 to 2 workers raises car-page throughput (31.0 to
51.3 req/s here; 34.1 req/s for 1 worker in server-causes.md) and cuts the p95 of car
pages while someone builds a cold metro (763 to 332 ms). **What is not:** that the gain
stops at 2 workers. Whether 3-4 workers scale on Railway depends on its CPU allocation
and the connection cost to its Postgres. Per-statement connections (10 per page) are the
likely limit, and server-causes.md S4 measured pooling cutting a warm car page from
41-44 to 20-26 ms, so `psycopg_pool` (data-layer report) stays the recommended companion
for 3-4 workers. Redis is the prerequisite for having more than 1 worker at all, both for
memory (3.3) and for correctness (3.2, items 1, 6, 11-14, 19, 20, 23, 24).

Pass 3 repeated this load test (3.13): 30.2, 52.4 and 62.5 car pages per second for 1, 2 and
4 workers, p95 332, 253 and 235 ms; the first draft's 4-worker figure of 50.3 was load-
dependent. "Redis is the prerequisite" above holds for correctness; for memory see 3.12,
which moves the claim from 73% to 8-23%.

### 3.5 Payload sizes: 20 scopes (local data)

`harness_inventory.py`, through `_build_cars_scope_entry`. Every body is "partial"
because the local cards are aged (built on day 20,761 or earlier against a current day of
20,768 and `CARD_MAX_AGE_DAYS = 1`) or changed (3.14), so the stale cards are served. The
sizes are what a shopper would download.

| zip | radius | cars | raw | gzip-6 (today) | brotli-5 | build, local |
|---|---|---|---|---|---|---|
| 28105 | 10 | 3,596 | 3.90 MB | 394,894 | 266,858 | 0.28 s |
| 28105 | 25 | 10,397 | 11.53 MB | 1,247,952 | 702,708 | 0.30 s |
| 28105 | 50 | 10,397 | 11.53 MB | 1,247,954 | 702,705 | 0.16 s |
| 28105 | 100 | 10,797 | 11.97 MB | 1,289,227 | 724,131 | 1.17 s (400 cards serialised inline) |
| 28105 | 250 | 23,701 | 25.88 MB | 3,141,910 | 1,613,775 | 1.07 s |
| 37421 | 25 | 5,246 | 5.68 MB | 579,007 | 338,262 | 0.45 s |
| 37421 | 50 | 6,605 | 7.10 MB | 718,295 | 406,039 | 0.34 s |
| 60659 | 50 | 3,805 | 3.98 MB | 300,426 | 198,819 | 0.26 s |
| 30721 | 50 | 10,909 | 11.74 MB | 1,268,549 | 678,156 | 0.47 s |
| 35816 | 50 | 11,181 | 12.14 MB | 1,307,509 | 725,902 | 0.81 s |
| 91945 | 50 | 8,911 | 9.66 MB | 885,383 | 501,400 | 0.53 s |
| 29745 | 50 | 10,397 | 11.53 MB | 1,248,280 | 702,700 | 0.18 s |
| 78758 | 50 | 11,802 | 12.74 MB | 1,230,697 | 718,637 | 0.77 s |
| 92591 | 50 | 34,933 | 42.50 MB | 4,769,845 | 2,400,942 | 1.75 s |
| 92663 | 25 | 20,181 | 24.08 MB | 2,498,384 | 1,328,563 | 0.92 s |
| 92663 | 50 | 48,352 | 58.13 MB | 6,269,931 | 3,107,637 | 2.13 s |
| 92663 | 100 | 54,515 | 66.81 MB | 7,358,235 | 3,609,882 | 2.25 s |
| 92663 | 250 | 54,937 | 67.27 MB | 7,420,860 | 3,638,152 | 2.17 s |
| 92660 | 50 | 49,269 | 61.11 MB | 6,650,928 | 3,266,018 | 1.94 s |
| 92627 | 50 | 48,352 | 58.13 MB | 6,270,212 | 3,106,845 | 1.92 s |
| **total** | | | **517.4 MB** | **56,098,478** | **28,738,131 (-48.8%)** | |

The local 37421 r25 is 579 KB against 462 KB live: the two databases differ. The
production body for 28105 r50 is 1.59 MB against 1.25 MB locally.

Key-space note: neighbouring ZIPs get separate, almost identical bodies, because
`distance_miles` is computed from each ZIP's own centroid. 92663, 92627, 92660 and the
other OC ZIPs are each 6-7 MB. That is why the 96 MB L1 holds only about 15 OC entries,
and why a 512 MB Redis earns its keep in big metros.

### 3.6 Value codec for the bodies (`harness_compress.py`, real bodies)

| body | codec | bytes | vs gzip-6 | compress | decompress |
|---|---|---|---|---|---|
| 37421 r25 (5.68 MB raw) | gzip-6 (today) | 579,007 | 0 | 34.5 ms | 2.2 ms |
| | brotli-5 | 338,262 | -41.6% | 23.9 ms | 2.8 ms |
| | brotli-9 | 310,417 | -46.4% | 46.3 ms | 2.5 ms |
| | zstd-9 | 340,376 | -41.2% | 22.8 ms | 1.2 ms |
| 28105 r50 (11.53 MB raw) | gzip-1 | 1,701,613 | +36.4% | 32.6 ms | 6.4 ms |
| | gzip-6 (today) | 1,247,954 | 0 | 71.3 ms | 4.4 ms |
| | gzip-9 | 1,195,806 | -4.2% | 104.1 ms | 4.2 ms |
| | brotli-5 | 702,705 | **-43.7%** | **51.6 ms** | 5.5 ms |
| | brotli-9 | 643,387 | -48.4% | 170.8 ms | 9.0 ms |
| | brotli-11 | 570,676 | -54.3% | 8,945 ms | 5.3 ms |
| | zstd-3 | 915,901 | -26.6% | 8.9 ms | 3.4 ms |
| | zstd-19 | 593,486 | -52.4% | 2,237 ms | 2.1 ms |
| 92663 r50 (58.13 MB raw) | gzip-6 (today) | 6,269,931 | 0 | 361.2 ms | 22.6 ms |
| | brotli-5 | 3,107,637 | **-50.4%** | **217.7 ms** | 25.5 ms |
| | brotli-9 | 2,811,972 | -55.2% | 556.2 ms | 23.3 ms |
| | zstd-9 | 3,132,363 | -50.0% | 203.3 ms | 11.8 ms |

On every body measured, brotli-5 is both smaller and faster to compress than today's
gzip-6. zstd matches it in size, but it would be a new wire format. Brotli is already
served by this app: `web/static.py` serves precompressed `.br` siblings of static
assets.

Transient memory of assembling one body (`stream_build_mem.py`, 48,352 OC cards,
tracemalloc peak): today's join + encode + `gzip.compress` peaks at **116.3 MB** (364.3 ms); streaming `zlib.compressobj` fed 500 cards at a time peaks at **12.9 MB** (365.2 ms, the same 6,269,931 B of output); streaming brotli-5 peaks at **6.8 MB** (212.9 ms, 3,241,299 B). That is 89% less transient memory for the same bytes. The 58.1 MB of card strings that any build has to read come on top of this.

Phone-emulated download of the same 28105 r50 body (`phone_codec.py`, Playwright
Chromium, 390x844 mobile viewport, CDP network throttling and 4x CPU throttling,
local server, fetch + `response.json()`, median of 3 runs):

| network (CDP) | codec | bytes on the wire | fetch + parse, median (3 runs) | runs |
|---|---|---|---|---|
| Slow 4G: 150 ms, 1.6 Mbps down | gzip | 1,247,954 | **6,480 ms** | 6479, 6485, 6480 |
| Slow 4G: 150 ms, 1.6 Mbps down | br | 702,705 | **3,744 ms** (-42%) | 3748, 3744, 3744 |
| Fast 4G: 60 ms, 9 Mbps down | gzip | 1,247,954 | **1,255 ms** | 1255, 1256, 1249 |
| Fast 4G: 60 ms, 9 Mbps down | br | 702,705 | **766 ms** (-39%) | 766, 766, 766 |

Re-run in pass 3 (`redis_raw/p3/out/phone/phone_codec_p3.json`, same script, the laptop at
load average 17). The first pass gave 6,488 / 3,760 / 1,264 / 775 ms and the challenge
round 6,477 / 3,747 / 1,252 / 763 ms.

### 3.7 ETags change on every rebuild in production (Python 3.12)

`routes/listings_api.py:322-324` builds the ETag from a digest of `gzip.compress(body,
compresslevel=6)`. On Python 3.12, the production image, `gzip.compress` writes the
current time into the gzip header, so the same body gets a different ETag on every
rebuild:

```
/opt/homebrew/bin/python3.12 -I -c "...gzip.compress(b,6); sleep 1.1; gzip.compress(b,6)..."
-> py3.12 same_gz_bytes_for_same_body_1s_apart= False  mtime header 4bcef16a 4ccef16a
   etag digests fce956fa3633e523697a 36b1d542dfc11d466651
```

**Production proof** (`python3 -I redis_raw/gz_header_proof.py <dir> redis_raw/gz_header_proof.json`,
on the four saved bodies of GET `/api/listings/cars?zip=28105&radius=50`):

| body | built (gzip header MTIME) | gzip bytes | decoded bytes | sha256 of decoded (first 16) | ETag digest (blake2b-10 of the gzip bytes) |
|---|---|---|---|---|---|
| A1, 2026-11-09 13:42 UTC | 13:42:39 | 1,585,587 | 14,546,046 | `1b3287b4ea36f35c` | `82e728fb3aa888a49f33` |
| G2, 2026-11-10 01:44:24 UTC (partial) | 01:44:24 | 1,585,585 | 14,546,045 | `488af2b19cada383` | `5a4d0588a6e3fdc55929` |
| G3, 01:44:49 UTC (complete) | 01:44:49 | 1,585,587 | 14,546,046 | `1b3287b4ea36f35c` | `67fb85d758fd3550f20c` |
| H1, 01:47:54 UTC | 01:44:49 (cache hit) | 1,585,587 | 14,546,046 | `1b3287b4ea36f35c` | `67fb85d758fd3550f20c` |

A1 and G3 decode to **byte-identical JSON** (same sha256, 13,298 cars), yet carry different
ETags, and the only difference in the gzip bytes is the MTIME field of the header (the
build second). So in production today a client that holds A1's ETag gets a full 1.59 MB
`200` instead of a `304` after any rebuild whose content did not change, and with 2
workers the two workers would answer the same body with two different ETags. G2 differs
from A1 only by `"partial":true` (1 byte). The fix is two lines and needs no Redis:
`gzip.compress(body, 6, mtime=0)` and an ETag from the digest of the raw body (same
for gzip and brotli, same on every worker and across rebuilds).

Caveat for browsers: live-speed.md found that the browser's cached copy no longer
matches because the response says `Vary: Accept-Encoding, Cookie` and the session
cookie is rewritten by a later POST. A stable ETag therefore helps multi-worker
consistency and any edge or shared cache now, and helps a returning browser only
together with dropping `Cookie` from `Vary` for anonymous callers (server-causes.md S5).

Python 3.14 (local) defaults `mtime=0` (`inspect.getsource(gzip.compress)`), which is why
this does not show up locally. With more than one worker, or with bodies rebuilt from a
shared store, a client bouncing between workers gets a 200 with 1.25-7 MB instead of a
304.

**Re-run in pass 3.** `redis_raw/gz_header_proof.py` on the same four saved bodies gives the
same table (`redis_raw/p3/out/gz_header_proof_p3.json`). The Python 3.12.13 check of the
production line on the live body A1 (14,546,046 B), 1.1 s apart (`out/gzip_mtime_py312_p3.txt`):
gzip bytes equal **False**, ETag digests `2e67753d28592691be20` and `762dc24335b1524d63b2`,
header mtime 1794362220 and 1794362221; with `mtime=0` the bytes are equal and the digest is
`449d64906dc9ad24c650` both times; an ETag from the raw body is `3dd7c36f6aa6a5326800`. A
fifth live observation: on 11-11 at 01:33 UTC the same URL carries
`W/"lc-28105-50-54954f531adfafc9bff0"` (3.16), a third ETag in three days. The fix is the
same: `gzip.compress(body, 6, mtime=0)` and an ETag computed from the raw body digest, which
is the same for every encoding.

### 3.8 Redis client behaviour when Redis is down (`harness_redis_failure.py`)

redis-py 8.0.1, against a closed port ("refused"), a listener that accepts and never
answers ("hung"), and an unresolvable `*.railway.internal` name ("dns"). Times are to
the exception, median of 5:

| shape | connect 0.05 s / read 0.05 s | connect 0.1 s / read 0.25 s | connect 2 s / read 2 s |
|---|---|---|---|
| refused | 0.34 ms | 0.31 ms | 0.27 ms |
| hung (accepts, never replies) | 51.7 ms | 251.7 ms | 2,001.7 ms |
| connect never completes (backlog) | 51.6 ms | 251.7 ms | 2,001.7 ms |
| dns (`redis-cache.railway.internal`) | 1.66 ms (one 204 ms outlier) | 1.44 ms | 1.80 ms |

**The trap.**
Three runs: the inline check, then `redis_raw/retry_default_check.py` twice (outputs in
`retry_default_check.jsonl` and, from the second pass, `retry_default_check_rerun.jsonl`).
- `redis.Redis(host=..., socket_timeout=0.05, socket_connect_timeout=0.05)` with the
  default retry policy took **4,676 / 3,208 / 5,669 ms** to fail against the hung listener,
  and 4,682 / 4,403 / 4,074 ms against the closed port. `get_retry()._retries` is 10, with
  exponential backoff and jitter, hence the spread.
- `retry_on_timeout=False` did not help: 4,198 / 4,899 / 3,518 ms, plus a DeprecationWarning.
- `Redis.from_url(...)` came back with no retry object and failed in 56.5 / 54.3 / 51.7 ms
  (hung) and 0.6 / 0.6 / 0.5 ms (refused).
- `retry=Retry(NoBackoff(), 0)` failed in 51.6 / 51.3 / 51.3 ms.

So the client must pass `retry=Retry(NoBackoff(), 0)` explicitly. Otherwise a Redis
outage adds 3-5 s to every cached request, depending on which constructor someone
used.

A circuit breaker (30 s open after the first failure) cost **0.095 µs per call** while
open (100,000 calls). The first failure cost 251.5 ms (0.1 s connect / 0.25 s read).
The reference module of 3.15 repeats these shapes inside a full `get_or_build` and adds
five more (reset in the middle of a reply, `-LOADING`, `-OOM`, garbage, an unroutable
address).

### 3.9 Moving bodies through redis-py (`resp_stub_bench.py`)

With no Redis server here, a minimal RESP2 stub (Python, separate process, loopback) was
used. It shows the client-side cost; a real Redis server is faster than the stub.

| op | bytes | median | p95 / p99 |
|---|---|---|---|
| GET token | 86 | 0.041 ms | p99 0.104 ms |
| GET body 37421 r25 | 579,007 | 0.15 ms | 0.28 ms |
| GET body 28105 r50 | 1,247,954 | 0.25 ms | 0.33 ms |
| GET body 92663 r50 | 6,269,931 | 1.34 ms | 1.57 ms |
| SET 6.27 MB body | 6,269,931 | 0.70 ms | |
| pipeline GET token + GET 6.27 MB | | 1.29 ms | |

Against the 774-811 ms median first hit to build (3.3), the client side of a Redis hit is
noise even at 6 MB. On Railway, add the private-network transfer, which was not measured
(section 8).

### 3.10 Other numbers used below

| | value | source |
|---|---|---|
| `import backend.main` (harness process) | 1.195 s, RSS 128 MB; +30 MB for the prewarm thread | `inventory.json` |
| card serialiser | 0.81 ms per card (5,000 cards in 4.05 s) | `inventory.json` |
| 60 anonymous car pages in a fresh process | first 2,428 ms, then median 94.8 ms, p95 176.8 ms | `inventory2.json` |
| `/api/listings/filter-options` | first 2,350 ms (facet build), then 6.5 ms (re-serialises 497 KB JSON every request; 75 KB gzip) | `inventory2.json` |
| `/listings` warm | 39.7-42.0 ms, 159.5 KB gzip | `inventory2.json` |
| rate limiter | 200 B per key, 0.6 µs per call (one key, 20k events), 0.84 µs (distinct keys) | `inventory2.json` |
| smart-search parse | first 90.8 ms after the vocabulary is loaded (vocabulary 1.86 s), second 68.7 ms | `inventory2.json` |

### 3.11 Fleet aggregates that reload on a shopper's request (re-run and corrected in pass 3)

Harnesses: `redis_raw/harness_aggregates.py` (fresh process, each loader once),
`r4_challenge_raw/h_warm_loaders.py` (warm process, 5 forced reloads each),
`h_carpage_ttl.py` (what a page pays after each expiry), `h_herd.py`, `h_gil.py`,
`h_double_count.py`, and `redis_raw/harness_rarity_sql.py`. All were run again in pass 3
(outputs `redis_raw/p3/out/*_p3.json`, log `run_agg.log`; commands in section 9). The
laptop's load average was 8-11 during these runs (other agents), so one number moves by
about 30%; the tables give medians of 5 where the harness takes them.

**Correction to the first draft.** It listed `landing_featured_cars(4)` at 767 ms and
called it the featured cars' own cost. That number contained a second reload of
`price_plausibility`. `harness_aggregates.py` measures plausibility by calling the private
`_load()`, which returns the tables but does not fill the module cache; the first card the
featured step serialised then called `_ensure_loaded()` and loaded them again.
Reproduced with `h_double_count.py`: in the old harness order the featured step took
**2,361.8 ms** and `price_plausibility._load` ran twice (2,710.0 ms in step 2, 2,321.8 ms
inside step 6); with step 2 calling `_ensure_loaded()` as a web process does, the featured
step took **33.1 ms** and `_load` ran once. With plausibility warm, featured cars cost
**10.0 ms** (median of 5); the public count they sit beside costs 55.9 ms. So the featured
cars are cheap; what a landing request waits for is plausibility.

Cost of one reload in a warm process (the figure a long-running worker pays, not the
first-call cost of a fresh interpreter):

| loader (TTL, lock) | reload, median [min-max] of 5 | JSON / gzip-6 / decode median of 5 (`aggregates_p3.json`) |
|---|---|---|
| `rarity_score._load_inventory_counts` (600 s wall-clock bucket, `lru_cache(2)`, **no lock**) | **717.8 ms** [703.0-791.3] | 746,911 B / 160,684 B / 8.2 ms |
| `price_plausibility._load` (300 s, module lock) | **867.5 ms** [788.0-1,070.1] | 1,185,413 B / 220,207 B / 12.0 ms |
| `grid_cards_repo._fallback_dealer_urls` (300 s) | 161.8 ms [159.3-185.5] | 7,125 B / 2,147 B / 0.03 ms |
| `listings_repo._count_public_listings_uncached` (300 s) | 55.9 ms [54.9-78.3] | one integer |
| `deal_score_cache._load_bands` (300 s, lock) | 7.2 ms [6.9-7.3] | 1,015,258 B / 100,950 B / 6.3 ms |
| `landing_featured_cars(4)` with plausibility warm (300 s) | 10.0 ms [8.1-13.4] | 4,197 B / 1,198 B |
| `landing_featured_cars(4)` after plausibility also expired | 1,053.4 ms [935.6-1,094.5] | (= plausibility + 10 ms) |

The five documents add up to about 485 KB gzipped, so storage is not the question. The
first-call numbers of a fresh interpreter in the same run were higher (`aggregates_p3.json`:
rarity 988.5 ms, plausibility 1,964.0 ms at load average 10) because the first loader also
pays imports and the first connection.

What a **page** pays when a TTL has run out (`h_carpage_ttl.py`, Flask test client, three
repeats each, warm car page about 99 ms on the loaded laptop):

| page | what had expired | total, median of 3 | loader time inside it |
|---|---|---|---|
| car page | rarity bucket only | **826 ms** | rarity 719 ms |
| car page | plausibility only | **913 ms** | plausibility 806 ms |
| car page | rarity and plausibility | **1,856 ms** | plausibility 966 + rarity 785 ms |
| car page | bands only | 92 ms | bands 10 ms |
| landing `/` | featured cars and count, plausibility warm | 120 ms | count 89 ms |
| landing `/` | featured cars, count **and** plausibility | **898 ms** | plausibility 820 + count 57 ms |
| landing `/` | nothing | 4 ms | - |

Three more facts from the same harnesses:
- **The herd** (`h_herd.py`, 4 threads, the web worker has 4). The rarity cache has no
  lock: at a bucket edge **4 threads ran the 0.73 s loader 4 times**, and each waited
  2.0-2.5 s (wall 2,475.7 ms). Plausibility holds a lock: the loader ran once and all four
  waited 0.80 s.
- **A background thread is not free on one worker** (`h_gil.py`). While a thread ran the
  shipped rarity loop (901 ms), six warm car pages ran in the main thread: median 48 ms
  (idle: 42-84 ms) but the worst took **591 ms**. With the `GROUP BY` version of the loop
  (453 ms) the worst took 238 ms. A refresher thread removes the wait of every shopper at
  a boundary and costs the few requests that overlap it a delay of up to 0.6 s; a producer
  in another process removes both.
- **`GROUP BY` is a bonus, not the fix** (`harness_rarity_sql.py`, same 214,546 rows): the
  shipped Python loop took 760.4 / 717.9 / 753.6 ms (median 753.6); the four Counters
  built from `SELECT make, model, trim, exterior_color, count(*) ... GROUP BY 1,2,3,4`
  (52,119 groups) took 359.5 / 385.0 / 359.7 ms (median 359.7) and were **identical** on
  all four. 2.1x, and still 0.36 s on whatever thread runs it.

**Live corroboration.** Two boundaries in this pass (`redis_raw/p3/live_idle_cycles.py`,
3.16) and the clock-aligned experiments of server-causes.md 4.1: the first car page after a
600 s boundary costs 2.5-3.6 s at the origin against 0.26-0.52 s for the next one;
server-causes.md also saw +1.65 s when only the 300 s caches ran out and +5.4 s when
everything was cold. The local reloads add up to the same story at a smaller scale
(production is slower per query).

Conclusion for Redis: **who computes and when** matters; where the result is stored does
not. The producer should not be a web thread: a post-scan step (or one refresher service)
computes the five documents and writes one Postgres row each (`fleet_aggregates`: name,
JSON, `at`, release); web workers re-read the 12-byte `at` every 60 s in a background
thread and decode only when it moved (25 ms). Redis may carry the same documents as a
copy (`ds:v1:agg:*`), but the Postgres row is the source and the only thing the scanner
side can always reach (4.1). Until that exists, a lock around the rarity loader and
`stale-while-revalidate` for all five (serve the last value while one thread reloads)
remove the herd and the wait at no infrastructure cost.

### 3.12 Several real gunicorn workers with a shared store (pass 3; replaces the 73% headline)

The A/B in 3.3 is one process that never builds: the best case for a reader. The real
question for "can we run 2-4 workers" is what a **cluster** costs when somebody still has
to build every body once. Pass 3 measured that with the production flags
(`gunicorn -c gunicorn.conf.py -w N --threads 4 --preload`, the real `backend.main` app on
the read-only local database), driven by one client (`redis_raw/p3/e2_client.py`, 4
threads): 8 simultaneous `/listings`, the 20 scopes of 3.5 in a shuffled order, 60 car
pages, then 3 repeat rounds of `/listings` plus 5 scopes. Every worker's RSS was sampled
every 0.2 s. Commands: `redis_raw/p3/run_mw_all.sh` (log `redis_raw/p3/out/mw_series.log`,
rows `mw/results.jsonl`, summary `summarize_mw.py`).

**What the "store" is.** There is no Redis server on this machine, so the shared store is a
directory (`redis_raw/p3/wsgi_p3.py`, derived from the challenge round's `wsgi_e2.py`,
reproduced first): a miss takes an `O_EXCL` lock file (= `SET NX`), builds with the
unchanged production code, writes the finished gzip bytes with an atomic rename; a loser
polls every 100 ms for up to 3 s, then builds itself (4.4); a hit reads the bytes;
facets go through `json.dumps`/`json.loads`; L1 is capped at 32 MB; bodies of signed-in
users never leave the process. It is faithful about **who builds and what bytes arrive**;
it leaves out the network and Redis itself. Partial bodies are shared too (unlike the
design in 4.1) because every Orange County card in the local store is stale, so nothing
else would ever be shared. `P3_STREAM=1` adds the streaming body assembly of 3.6 (feeds
`zlib.compressobj` 500 cards at a time, ETag from the raw-body digest).

Machine: the laptop's load average was 8-11 throughout (other agents), and macOS
compresses idle pages, so the **end-of-run RSS** of a worker can fall well below its
earlier peak (`w2_today_s2`: end 1,973 MB, peak 3,055 MB). The sum of the per-worker
**peaks** is the steadier number and is the one to read.

| run (seed) | worker end RSS, MB | sum end / **sum of peaks**, MB | cold `/listings` x8, median ms | 20 scopes, median / sum | car pages: median / max ms | builds (total / distinct) |
|---|---|---|---|---|---|---|
| 1 worker, today (1) | 1,936 | 1,936 / **2,199** | 4,784 | 1,799 ms / 49.4 s | 193 / 4,416 | 24 / 21 |
| 2 workers, today (1) | 1,851 + 1,153 | 3,004 / **3,235** | 4,032 | 1,059 ms / 37.1 s | 182 / 3,203 | 29 / 21 |
| 2 workers, today (2) | 1,068 + 905 | 1,973 / **3,055** | 2,852 | 1,448 ms / 34.4 s | 183 / 4,055 | 28 / 21 |
| 2 workers, store (1) | 1,323 + 1,250 | 2,574 / **2,625** | 2,511 | 1,039 ms / 28.2 s | 151 / 3,017 | 21 / 21 |
| 2 workers, store (2) | 920 + 466 | 1,386 / **3,171** | 2,484 | 1,183 ms / 33.7 s | 170 / 3,562 | 21 / 21 |
| 2 workers, today + streaming (1) | 1,207 + 1,015 | 2,222 / **2,505** | 3,442 | 1,371 ms / 34.8 s | 154 / 3,154 | 30 / 21 |
| 2 workers, today + streaming (2) | 1,460 + 1,119 | 2,579 / **3,030** | 3,403 | 1,628 ms / 38.5 s | 191 / 3,859 | 28 / 21 |
| 2 workers, store + streaming (1) | 1,072 + 1,063 | 2,135 / **2,292** | 2,504 | 1,043 ms / 30.3 s | 206 / 3,026 | 21 / 21 |
| 2 workers, store + streaming (2) | 1,396 + 857 | 2,253 / **2,527** | 2,385 | 1,048 ms / 29.5 s | 144 / 3,888 | 21 / 21 |
| 3 workers, today (1) | 1,190 + 1,133 + 815 | 3,138 / **3,203** | 3,144 | 1,395 ms / 31.7 s | 189 / 3,795 | 31 / 21 |
| 3 workers, store + streaming (1) | 1,204 + 777 + 271 | 2,252 / **2,489** | 2,445 | 1,748 ms / 34.8 s | 217 / 3,288 | 21 / 21 |

"21 distinct" is the facets plus the 20 scopes; every extra build above 21 is a body or a
facet build that another worker had already done.

Reading the table (sum of peaks, 2 workers, two seeds; the ranges are the two seeds):
- **Duplicate builds vanish**: 28-30 builds for 21 distinct objects today at 2 workers
  (7-9 duplicates, +33-43%), 31 at 3 workers (+48%); 21 of 21 with the store.
- **Memory**: sharing alone changed the sum of peaks by -19% and +4% (mean 2,898 MB
  against 3,145 MB, -7.9%); streaming alone by -23% and -1% (mean -12.0%); both by -29%
  and -17% (mean 2,410 MB, **-23.4%**); at 3 workers, one seed, -22.3%. The challenge
  round's repeat of this experiment (three seeds, store alone) got -9.7% on the end sum
  and -3.0% on the sum of peaks. **Redis alone is a single-digit-percent memory lever at 2
  workers**; with streaming it is about a fifth, not 73%. Memory scales with how much each
  worker builds, and the worker that builds keeps it (3.14).
- **Cold bursts**: 8 simultaneous `/listings` after a start: median 2.5 s with the store
  (one worker builds the facets, the other waits and reads them) against 2.9-4.0 s today
  at 2 workers and 4.8 s at one. Scopes requested once each cost the same either way (a
  first request on the cluster is a build whoever serves it).
- **Car pages did not improve**: median 144-217 ms and a max of 3.0-4.4 s in every row.
  That max is the per-worker cold cost of everything the store does not hold (aggregates
  of 3.11, EPA and trim lookups in `lru_cache`).

**The reader profile (best case, measured).** `redis_raw/p3/run_e8.sh`: session 1 populates
the store; session 2 starts **new** gunicorn workers (a deploy or restart whose token and
`CARD_REV` did not change) on the same store and replays the same traffic.

| 2 workers, store + streaming | populating | fresh workers over the populated store |
|---|---|---|
| cold `/listings` x8, median | 2,387 ms | **194 ms** |
| 20 scopes: median / total wait | 1,088 ms / 31.3 s | **7.6 ms / 0.6 s** |
| car pages: median / max | 176 / 3,211 ms | 154 / **3,149 ms** |
| worker RSS at the end | 1,392 + 859 = 2,251 MB | **328 + 326 = 654 MB** |
| sum of per-worker peaks | 2,508 MB | **812 MB** |

So a worker that only reads costs about 0.33 GB and serves the first 8 `/listings` and 20
metros 12x and 140x faster than a worker that builds, but a first car page still waits up
to 3.1 s. That is why 3.11 matters as much as the store: the car pages' cold cost is the
aggregates and the per-worker lookup caches, not bodies.

Why the cluster average is so far from the reader number: someone builds each body once,
and building is what makes a worker big (3.14). The reader profile is reachable only if
the builds happen **outside the web workers** (a producer, section 4.8 and R4 in 7), or if
a build leaves much less behind (streaming assembly plus a lean candidate fetch).

### 3.13 Throughput with 1, 2 and 4 workers, and with the store (pass 3; reproduces 3.4)

`redis_raw/p3/run_load_all.sh` (log `out/rest.log`, rows `load/results.jsonl` and `rss.jsonl`):
the load test of 3.4 unchanged (8 client threads loop over 150 car pages for 25 s; the
"noisy" run adds one shopper requesting new Orange County metros, radius 50, 48,000 cars,
about 6 MB gzipped each, back to back), through `wsgi_p3` so the shared-store emulation and
the streaming assembly can be switched on. Three warm-up rounds first. The laptop's load
average was 10-12 for the first four rows and **15-24 for the last** (other agents'
Chromium and Python jobs), and every request opens a new TCP connection.

| workers, mode | car pages/s | p50 / p95 / p99 ms | with the noisy shopper: pages/s, p95 ms | noisy builds, median ms | worker RSS after the run, MB (sum) |
|---|---|---|---|---|---|
| 1, today | **30.2** | 260 / 332 / 373 | 20.6, **751** | 6, 4,500 | 1,457 |
| 2, today | **52.4** | 144 / 253 / 311 | 45.4, **364** | 5, 5,186 | 905 + 1,123 (2,028) |
| 2, store + streaming | **57.2** | 132 / 221 / 272 | 46.8, **349** | 5, 5,513 | 855 + 859 (1,714) |
| 4, today | **62.5** | 115 / 235 / 322 | 61.1, **200** | 6, 4,140 | 231 + 918 + 934 + 1,083 (3,166) |
| 4, store + streaming (load average 15-24; not comparable) | 54.8 | 119 / 250 / 370, max 2,986 | 36.6, 359 | 4, 7,488 | 235 + 242 + 850 + 865 (2,192) |

- **The first draft's rows reproduce**: one worker 31.0 then 30.2 pages/s, p95 337 then 332
  ms; two workers 51.3 then 52.4, p95 253 then 253; the noisy shopper's p95 on one worker 763
  then 751 ms and on two 332 then 364 ms. The 4-worker row moved from 50.3 to 62.5
  pages/s (load differs). server-causes.md measured 141 req/s at 4 workers with keep-alive
  connections and one URL, so the plateau here is the generator and the shared laptop; the
  ranking (1 < 2 < 4) is consistent in all three measurements.
- **Going from 1 to 2 workers** is +74% in pages per second and cuts the p95 under a
  noisy shopper from 751 to 364 ms; **to 4**, +107% and 200 ms. That is the argument for
  more workers, and it needs R2 and the claims (limits and locks that are exact across
  workers) before it is safe.
- **The store changes little for throughput**: +9% pages/s and -13% p95 at two workers
  (57.2 against 52.4; 221 against 253 ms), -4% on the noisy p95 (349 against 364 ms). The
  noisy shopper's metros are all new, so nobody else could have built them. The challenge
  round's clone of this test saw the same direction (55.0 against 50.9 pages/s, p95 244
  against 269 ms).
- **Memory after the load**: two workers 2,028 MB today against 1,714 MB (-15%) with the
  store and streaming; four workers 3,166 MB against 2,192 MB (-31%, one run each, the
  second under load). The builders of the noisy metros grow to about 0.85-1.1 GB in every
  row; the others stay near 0.23-0.24 GB.
- **Connections**: each car page opens about 10 Postgres connections (3.4), so 62.5 pages/s is
  about 625 connects/s against a local socket; a managed Postgres link will not allow that
  (server-causes.md 3.2: 20-38 ms per connection). A pool, not Redis, is what lets 3-4
  workers scale.

### 3.14 What a builder keeps: live objects against what the allocator holds (pass 3)

Why does a worker that built a few big metros stay big? `redis_raw/p3/h_retain.py`: one
process on the read-only database, `tracemalloc` (1 frame) started before the import,
`gc.collect()` before each reading, then `/listings` and five scopes in a row (28105 r50,
37421 r50, 92591 r50, 92663 r50, 92663 r100), then the scope cache cleared. "Traced" is
what Python still holds that tracemalloc can see; USS is the memory the process owns
(psutil `memory_full_info`). Commands: `redis_raw/p3/run_retain.sh`; outputs
`redis_raw/p3/out/retain/retain_stream0.json` and `retain_stream1.json`.

| step (today's assembly) | RSS MB | USS MB | traced, live MB | traced, peak during the step MB |
|---|---|---|---|---|
| after import and prewarm | 276.8 | 252.3 | 94.6 | 107.0 |
| after `/listings` (facets) | 602.0 | 471.5 | 109.1 | 201.8 |
| after 28105 r50 (10,397 cars, 1.2 MB gz) | 615.0 | 349.4 | 125.2 | 159.6 |
| after 37421 r50 (6,605 cars, 0.7 MB gz) | 649.2 | 383.6 | 125.9 | 183.7 |
| after 92591 r50 (34,933 cars, 4.5 MB gz) | 1,060.3 | 576.5 | 130.5 | **449.0** |
| after 92663 r50 (48,352 cars, 6.0 MB gz) | 1,314.4 | 830.6 | 136.4 | **560.4** |
| after 92663 r100 (54,515 cars, 7.0 MB gz) | 1,653.3 | 1,169.5 | 143.5 | **626.1** |
| after clearing the scope cache | 1,653.3 | 1,169.5 | **131.1** | 143.6 |

- **The memory a builder keeps is not live data.** With every body released, Python holds
  131 MB (94.6 MB of that existed after import). The process holds 1,169.5 MB of private
  memory and 1,653.3 MB of RSS. About 1.0 GB is the allocator keeping what it was given
  (fragmentation or lazy return). The challenge round's `malloc_zone_pressure_relief`
  after every build returned 0.0 MB (its E3).
- **The cost is a transient of 450-630 MB while the scope is gathered**, not the body: the
  traced peak of a big scope is 3.4-4.4 times what Python holds after the step (131-144
  MB) and 89-100 times the 4.5-7.0 MB gzip it produces. `cards_near` does `cur.fetchall()`
  and then `dict(r)` for every row, and `_resolve` fetches a full `cars` row for every
  card that is not current (`_fetch_full_rows`). `redis_raw/p3/h_cards_stats.py` (one
  `cards_near` call per scope, tracemalloc, output `out/misc/cards_stats.json`) shows which
  path each scope takes:

  | scope | cards | fresh / changed | traced peak MB | per 1,000 cards |
  |---|---|---|---|---|
  | 28105 r50 | 10,397 | 10,397 / 0 | 34.8 | 3.3 MB |
  | 37421 r50 | 6,605 | 0 / 6,605 | 58.4 | 8.8 MB |
  | 92591 r50 | 34,933 | 967 / 33,966 | 323.0 | 9.2 MB |
  | 92663 r50 | 48,352 | 967 / 47,385 | 429.8 | 8.9 MB |

  The changed path costs 2.7 times as much per card as the fresh path. Locally almost
  every card outside the Charlotte area is stale (built on day 20,724-20,735, against a
  current day of 20,768), so the Orange County peaks in the table above (the gather of
  323-430 MB plus about 116 MB of body assembly, 3.6) are the changed path. With a current
  card store a 48,000-card scope would peak near 160 MB (3.3 MB x 48.4, extrapolated, not
  run). Production's regime is not known (section 8): the live site returns
  `x-listings-partial: 1` for first requests (3.1 #7, #17), and every release from 1.5.11
  on turns the whole store into the changed path until the card builder has run
  (server-causes.md 4.5).
- **Streaming assembly changes what stays resident, not the traced peak**
  (`retain_stream1.json`): the traced peaks are identical (449.0, 560.4, 626.1 MB), yet
  the same sequence ends at **RSS 1,131.7 MB and USS 423.9 MB** instead of 1,653.3 MB and
  1,169.5 MB (-32% and -64%), and `ru_maxrss` 1,392 MB instead of 1,665 MB. The 58 MB
  joined string, its 58 MB encoded copy and the gzip output are the large contiguous
  blocks that leave holes the allocator cannot reuse. (The single-process A/B of this
  pass says the same: end RSS 1,306.9 MB plain against 899.9 MB streaming, 1,383 against
  969 MB peak, with the same 56.1 MB of cached bodies and the same build time:
  `ab/ab_plain.json`, `ab/ab_stream.json`.)
- **Consequence for the design.** Redis moves finished bytes. It cannot shrink the worker
  that builds them. What shrinks a builder is a leaner build (streaming assembly, a
  candidate fetch that does not hold 48,000 dicts and full rows at once, no rebuild of
  stale cards inside a request) and, best, builders that are not web workers at all.
- **Caveat on where this ran.** macOS libmalloc, Python 3.14. Production is Debian glibc
  with Python 3.12; its single worker is 3.3 GB warm after the arbiter fix and was 4.92 GB
  before, which says retention exists there too, but the factor is not known. Cheap
  experiments on Railway, none of which needs Redis: `MALLOC_ARENA_MAX=2`, jemalloc through
  `LD_PRELOAD`, and logging RSS around each scope build.

### 3.15 Executable checks of the design: a reference module, the Lua script, and eight failure shapes (pass 3)

There is still no Redis server here. Pass 3 therefore wrote the cache module of section 4
as a **reference prototype** (`redis_raw/p3/shared_proto.py`, about 230 lines, not in the
repository) and tested it two ways with `redis_raw/p3/proto_tests.py`, run in a scratch
venv (Python 3.12, the production minor version; `fakeredis` 2.40.0 with real Lua through
`lupa` 2.8, and redis-py 8.1.0 for the real-client part). Output:
`proto_tests.out`, `lua_validate.out`.

**Logic, on fakeredis (one shared server, one `Shared` instance per simulated worker):**

| check | result |
|---|---|
| 8 workers ask for the same missing body at the same moment (`SET NX PX` lock, 3 s poll) | **1 build**, 7 `built-after-wait`, wall 419 ms for a 400 ms build (421 ms in the first run) |
| `If-None-Match` equal to the stored ETag | `304` from one `HGET`; no body read |
| `Accept-Encoding` with `br` / without | the `br` field (90,000 B in the test) / the `gz` field (200,000 B) |
| a new token for the same scope | the previous body's TTL drops to 120 s, the new body keeps 86,400 s, the `last` pointer names the new one (`SET ... GET`, not the deprecated `GETSET`) |
| a 17 MB value | refused by the size guard; no key written |
| limit 5 per 60 s shared by 2 workers, 8 attempts | exactly **5 allowed**; refunding one member frees exactly one slot; the Redis key is a blake2b hash, no account name in it |
| the Lua sliding window alone (`lua_validate.py`) | 3 per 1 s: 1,1,1,0,0, allowed again after 1.1 s; `PTTL` 1,000 ms, key gone when idle; 8 threads x 10 calls on a limit of 25: exactly 25 allowed |
| Redis down (client raises on every call) | the limiter falls back to the in-process window (3 allowed, then denied), breaker open, 1 error counted and the rest short-circuited |

**Failure path, with the real redis-py client against eight ways of being down** (run twice;
the table gives the stored second run and the first run where it differed). Settings
of 4.6 (connect 0.1 s, read 0.5 s, `Retry(NoBackoff(), 0)`, 30 s breaker). For each: one
`get_or_build` that must return a body, a second one, then 2,000 more:

| Redis is... | first call | second call | median of the next 2,000 | request returned a built body? |
|---|---|---|---|---|
| a hung listener (accepts, never answers) | **503.9 ms** (the 0.5 s read timeout; 513.5 ms in the first run) | 0.022 ms | 0.003 ms | yes |
| unreachable and unroutable (`10.255.255.1`) | 102.6 ms (the 0.1 s connect timeout; 116.3 ms) | 0.007 ms | 0.003 ms | yes |
| an unresolvable `redis-cache.railway.internal` | 2.3 ms (39.1 ms in the first run, before the resolver cached the failure) | 0.006 ms | 0.003 ms | yes |
| a refused connection (closed port) | 0.7 ms | 0.006 ms | 0.004 ms | yes |
| closing the connection in the middle of a reply | 1.0 ms | 0.014 ms | 0.004 ms | yes |
| answering `-LOADING` (restarting) | 0.9 ms | 0.006 ms | 0.003 ms | yes |
| answering `-OOM` | 1.0 ms | 0.022 ms | 0.003 ms | yes |
| answering garbage (an HTTP response) | 1.0 ms | 0.006 ms | 0.003 ms | yes |

In all eight, exactly one error was counted, every later call was short-circuited by the
breaker, the rate limiter fell back to its in-process window, and nothing was raised. The
worst case for a visitor is one extra 0.5 s once per 30 s per worker when Redis hangs. The
library's default retry policy would have made it 3-5 s per failure (3.8).

What this does not prove: server memory and allocator overhead, real GET/SET latency on
Railway's private network, Redis's own eviction behaviour, and any bug in the way the
production code would call the module. Section 8 lists the Railway-side checks.

Also measured: `import redis` costs 20 ms and 1.5 MB RSS inside the app process after
`import backend.main` (108 ms and 20 MB in a bare interpreter, which is mostly the standard
library the app already loaded); a client object is 0.12 MB.

### 3.16 Live checks in pass 3 (sarraficars.com, 2026-11-11 01:33-01:55 UTC)

`redis_raw/p3/live_get.py`: anonymous GET only, no cookies, at least 1.2 s apart, a budget of
80 (the counter stands at 21, plus one stray `curl` of `/api/health` that the helper did not
count: 22 requests in this pass). Rows: `redis_raw/p3/out/live/live.jsonl`; the idle-gap plan
was written before the first request (`live_idle_cycles.py`).

**State.** `GET /api/health` at 01:33:20: version 1.5.8, commit `0fd91872b` (the same as on
11-09 and 11-10: no deploy). `GET /api/ready`: `{"checks":{"db":"ok","pgvector":"skipped",
"redis":"skipped"},"ready":true}`: still no `REDIS_URL`.

**Compression per `Accept-Encoding`** (`/api/listings/cars?zip=28105&radius=50`, then a
static asset). "Origin" is Cloudflare's `server-timing: cfOrigin`.

| label | `Accept-Encoding` | result | total / origin |
|---|---|---|---|
| S1 | `gzip` | `content-encoding: gzip`, 1,585,587 B, ETag `W/"lc-28105-50-54954f531adfafc9bff0"` (a third ETag in three observations, 3.7) | 0.564 s / 217 ms |
| S2 | `br` only | **`content-encoding: br`, 1,596,934 B**, same ETag | 2.031 s / 675 ms |
| S3 | `gzip, deflate, br, zstd` (a browser) | **`content-encoding: gzip`, 1,585,587 B** | 0.444 s / 79 ms |
| S4 | `/static/ds_core.js`, browser header | **`content-encoding: br`, 1,144 B**, `cf-cache-status: MISS` | 0.258 s / 73 ms |
| S5 | `/static/ds_core.js`, `gzip` | `gzip`, 1,376 B, `HIT` | 0.134 s / 0 ms |

- A browser offering `br` gets the origin's gzip for the API (S3): Cloudflare keeps the
  origin's encoding when the client accepts it. A client that accepts only `br` gets an
  edge conversion (S2) that is **larger than the gzip** (1,596,934 against 1,585,587 B), so
  Cloudflare is not a brotli encoder for this endpoint. The origin has to produce it.
- For static files the origin's precompressed `ds_core.js.br` reaches the browser after a
  cache MISS (S4: `content-encoding: br`, and `content-disposition: filename=ds_core.js.br`
  is set by the origin). So Cloudflare offers `br` to this origin and passes an origin
  brotli body through. A gzip-only client got the same file transcoded by Cloudflare (S5:
  `content-encoding: gzip`, still `filename=ds_core.js.br`, a weak ETag), so an origin that
  answers `br` is safe for clients that cannot read it. The API will behave the same once
  its origin sends `br`; that last step is the check in section 8.

**The 10-minute boundary** (`live_idle_cycles.py`: the rarity bucket flips at every
`xx:x0:00` UTC; two boundaries, cars of one dealer, origin times in ms):

| boundary | car A at +15 s | car B at +20 s | `/health` at +22 s | car C at +305 s | car D at +310 s | landing at +312 s |
|---|---|---|---|---|---|---|
| 01:40:00 | **2,530** | 282 | 104 | 2,095 | 897 | 398 |
| 01:50:00 | **3,550** | 522 | 79 | 619 | 2,336 | 91 |

- **Boundary result, reproduced**: the first car page after the boundary costs 2.5 and 3.6 s
  at the origin, the next one 0.28 and 0.52 s, a request that runs no application code 0.08-
  0.10 s. A and B are different cars of one dealer; server-causes.md 4.1 measured 2.9 and
  3.3 s for the same event on one car. That is the rarity reload (0.72-0.79 s locally) plus
  plausibility (0.8-1.0 s) at production's slower query cost, as 3.11 predicts. Nothing in
  R1 changes it; R0 does.
- **The +305 s probes are not clean.** I aimed car C at the 300 s edge of the plausibility
  cache, but that cache was loaded at +15 s, so +305 s is 10 s early; and cars C and D are
  different models. Two of the four probes cost 2.1 s (C, first boundary) and 2.3 s (D,
  second) with no boundary or expiry that I can name. The likely cause is a model's first
  view in the process (EPA and trim lookups in `lru_cache`, 3.2 row 9), but I did not
  isolate it. Take the 300 s effect from server-causes.md 4.1, where one car was requested
  repeatedly (+1.65 s at the 300 s edge, +5.4 s when everything was cold). If the guess is
  right, there is a third per-worker cold cost that Redis cannot hold and only a warm-up in
  `post_fork` removes.

---

## 4. Design

### 4.1 Layers

```
request -> L1 (in-process, small) -> L2 Redis (shared, survives deploys) -> build
           ^ hit: 0 network           ^ hit: one GET            ^ Postgres card store
```

- Only complete, unpersonalised data goes to Redis.
- **Partial bodies** (stale cards being refreshed, tied to the process-local
  `store_generation`) and **hidden-dealer bodies** (per user, `private`) stay in L1 only.
- The `listings_grid_cards` card store stays the source in Postgres. Redis holds only
  derived bytes that can be rebuilt.
- One module, `backend/cache/shared.py`, owns the client, the breaker, the key prefix and
  the codecs. Every caller goes through `shared.get/set/hmget/claim/eval`, which **never
  raise** and return `None`/`False` on any error. (search-promotion.md section 9 names
  the same module `backend/utils/shared_cache.py` and uses the prefix `srch:`; they are
  one module here, and its keys appear in 4.2 as `ds:v1:srch:*`.)

**Where each kind of state lives** (the rule behind the decisions in 3.2):

| kind | home | why | examples |
|---|---|---|---|
| derived bytes the web builds, several workers want, rebuildable, tens of KB to MB | **Redis** (with L1 in front) | cross-worker, cross-deploy, TTL and eviction for free, 0.04-1.3 ms to read (3.9) | listings bodies, facets, dealer cards, query vocabulary, parse cache, narrations, Places results |
| counters and claims that must be exact across workers | **Redis** (Lua / `SET NX`) | atomic, shared clock | rate limits, login lockout, build and refresh claims, packages-ensure |
| aggregates a pipeline step can compute after each scan or on a timer | **Postgres table**, optional Redis copy | the writers (Railway scanner, the Macs over a tunnel) cannot all reach Redis' private network; the table survives a Redis restart; the read is once per refresh, not per request | rarity counts, price-plausibility medians, featured cars, public count |
| static lookups, same until the next deploy | **process** (`lru_cache`) | hits cost nanoseconds and are called thousands of times per page | EPA, trim ladders, brochures, model specs, vPIC hits |
| durable facts, long history, anything with a second source of truth risk | **Postgres** | already shared and durable | card store, vPIC cache, market stats, job queue, reviews, users |
| login state | **signed cookie** | stateless; a server-side store would add a Redis hit to every request for no gain | Flask session |

Invalidation stays on the Postgres write fingerprint (4.3), not on Redis pub/sub or
keyspace events. Not every writer to the production database is on Railway's private
network (scan jobs may run on Railway or from the owner's machines, and a restore
replaces the whole database), so a signal sent through `redis.railway.internal` would
miss some of them. The database counters see all of them.

**Other ways to share finished bytes, and why Redis anyway.** The 3.12 experiments used a
directory as the store, so their memory and duplicate-build results hold for any shared
byte store. The choice between stores rests on what else each one gives:

| store | what it fixes | what it does not |
|---|---|---|
| files in `/tmp` shared by the workers of one container | duplicate builds, finished bodies in every worker's RAM, the second worker's own facet build (3.12 is exactly this) | survival of a deploy or restart (the container disk is fresh), a producer in another container, counters and locks that are exact across containers, TTL and eviction |
| a Postgres table (`UNLOGGED`, `BYTEA`) | the same, survives restarts, reachable from the scanners | a 1-7 MB read costs a database round trip over a link whose connection setup is 20-38 ms on production (server-causes.md 3.2) until there is a pool; vacuum and I/O churn on the database that also serves the card store |
| Redis | all of the above, about 1 ms for a body read | one more service, one more failure mode (handled and tested, 3.15), $1-6 a month |
| nothing (today) | - | everything in this report |

The cheapest first step for memory and duplicate builds alone would be the directory; Redis
is what makes the bodies survive a deploy, lets a producer in another container fill them
(4.8), and carries the limits and locks that must be exact across workers.

### 4.2 Keys, values, TTLs

The prefix is `ds:v1:`. Changing it to `v2` drops every value at once (use it for
value-format changes). Use one Redis per Railway environment and never share one across
environments. `{tok}` is the first 16 hex of blake2b over `repr(grid_scope_token())`;
`{fp}` is the same digest over the write-fingerprint part of the token only (no day, no
`CARD_REV`), used for the facets, which do not depend on cards.
With the day-roll rule of 4.3, `{tok}` in the body keys is written `{tokd}:{day}`, where
`{tokd}` is the same digest over the token tuple **without** its `_today()` part, and
`{day}` is `_today()`. `{rev}` is `LISTINGS_API_VERSION` plus a body-format constant.

| key | type / value | TTL | written by | read by |
|---|---|---|---|---|
| `ds:v1:tok` | string `{tok}` | 60 s (`SET NX EX 60`) | first worker to resample the fingerprint after expiry | every worker (2 s L1) |
| `ds:v1:lc:{rev}:{tok}:{zip}:{r}` | hash `gz` (gzip-6, mtime 0), `br` (brotli-5), `etag`, `n` (count), `at` (built epoch) | 24 h (the token's day part rolls at UTC midnight anyway) | the worker that holds the build lock | `HMGET key etag n br` or `... gz` (one round trip, only the encoding the client takes) |
| `ds:v1:lc:last:{zip}:{r}` | string = name of the newest complete body key | 24 h | the builder: `SET ... GET EX` with the new name (it returns the previous name; `GETSET` is deprecated since Redis 6.2), then `EXPIRE <previous name> 120` so a scan day keeps about one body per scope, not one per token flip (4.4) | stale-while-revalidate across tokens and the day roll (4.3) |
| `ds:v1:lc:hot` | sorted set, member `{zip}:{r}`, score = epoch of the last served request; trimmed to 200 | 7 days | `ZADD` on each served scope request (best effort, skipped when the breaker is open) | the warmer (4.3, 4.8) |
| `ds:v1:warm:lock` | `SET NX EX 300` | 300 s | the worker (or the warmer service of 4.8) that starts a warm pass | other workers skip |
| `ds:v1:lc:lock:{rev}:{tokd}:{day}:{zip}:{r}` | `SET NX PX 15000` | 15 s | the builder | other workers wait up to 3 s for the body, then build themselves |
| `ds:v1:gc:refresh:{tok}:{zip}:{r}` | `SET NX EX 120` | 120 s | the worker that queues the background card refresh | the other workers skip queueing |
| `ds:v1:fac:{fp}` | hash `json_gz` (219 KB), `api_gz` (74 KB, the `/api/listings/filter-options` body with an ETag), `packed` (725 KB, the `car_rows_packed` JSON the template embeds) | 24 h | the facet builder (lock `ds:v1:fac:lock`, NX EX 60) | every worker (dict in L1) |
| `ds:v1:fac:last` | string = newest `ds:v1:fac:{fp}` | 24 h | the builder | serve-stale while one worker rebuilds (the existing 300 s throttle) |
| `ds:v1:dc:{tok}:{dealer_id}` | gzip JSON array (the `/api/dealership/<id>/cars` body) | 1 h | dealership page (R3) | dealership page and its `/cars` API |
| `ds:v1:qv:{day}` | gzip JSON, query-parser vocabulary (182 KB; 1.86 s and +110 MB to build) | 26 h | first smart search of the day | every worker |
| `ds:v1:srch:parse:{vkey}:{sha1(q)}` | parsed filters JSON (under 1 KB). `{vkey}` = first 8 hex of the vocabulary digest, so a new vocabulary orphans old entries | 24 h | smart search (a parse is 69-91 ms of CPU, 3.10) | smart search; L1 LRU of 2,000 in front |
| `ds:v1:srch:ex:{zip3}:{r}`, `ds:v1:srch:relax:{sha1(filters)}:{zip}:{r}`, `ds:v1:srch:pop:{zip3}` | per-area verified example queries with counts / zero-result relaxation counts / "searched near you" | 1 h / 10 min / 1 h | search features proposed in search-promotion.md section 9 (same keys, prefix `ds:v1:`) | same |
| `ds:v1:coh:{tok}:{zip}:{r}` | gzip JSON cohort groups (75 KB) | 900 s (today's TTL) | paid market stats | same |
| `ds:v1:agg:{name}`, `name` in `rarity`, `plaus`, `bands`, `feat`, `fb`, `count` | hash `json_gz`, `at`, `rev` (documents of 2 KB to 220 KB gzipped, 3.11). Optional L2 copy of the Postgres table in 4.1 | 3 x the refresh interval (30 min at a 10 min refresh) | the refresher, holding `ds:v1:agg:lock:{name}` (`SET NX EX 120`) | web, in a background thread: re-read the 12-byte `at` every 60 s, decode only when it moved |
| `ds:v1:rl:{blake2b16(key)}` | sorted set: member = unique slot id, score = ms. The key is hashed because rate-limit keys contain IPs and account names (emails) | the window (`PEXPIRE`) | `allow_request` | `allow_request`, `refund_event` |
| `ds:v1:narr:{car_id}:{hash16}` | narration text | 30 days | narrate | narrate |
| `ds:v1:plc:{lat2}:{lon2}:{r}` | Places JSON | 6 h (today's policy) | dealer locator | same |
| `ds:v1:bill:{uid}` | `plan|premium` | 60 s | entitlements | entitlements; webhook `DEL` |
| `ds:v1:pkg:run:{car_id}` / `ds:v1:pkg:cool:{car_id}` | claim (heartbeat every 20 s) / cooldown | 60 s / 900 s | packages-ensure | packages-ensure (only with workers > 1) |
| `ds:v1:reco:{uid}:{ver}:{hash}` and `ds:v1:reco:ver:{uid}` | carousel JSON / INCR on view | 120 s / 30 days | dashboard | dashboard |

Values are bytes, JSON or gzip/brotli bytes, and **never pickle**. If anyone ever got
write access to Redis, unpickling would mean code execution in the web process.

### 4.3 Invalidation: the pg_stat fingerprint, shared

The token's base is `listings_repo._pg_grid_write_fingerprint()`: `n_tup_ins + n_tup_upd
+ n_tup_del` from `pg_stat_user_tables` for `cars`, `incomplete_listings`,
`market_price_stats`, `car_attribution` and `dealer_feed_scope`. Those are server-wide
counters, so every worker reads the same value, which makes the token a good cross-worker
version. Costs and properties:

- **Cost**: 2.83 ms median per read (local). Today each process reads it at most every
  60 s. With `ds:v1:tok`, the whole cluster reads it about once a minute and every worker
  agrees on the key, so a body built by one worker is a hit for all.
- **Moves when it should**: any committed write to those tables. Card-store writes
  (`listings_grid_cards`) are not in the list, so the web's own card repair does not
  churn the token.
- **Moves when it need not**: a stats reset (crash recovery, `pg_stat_reset`, failover)
  changes it. That only causes a full rebuild, which is safe. The local database showed
  every counter at 0 (stats reset after a restore), which is constant, not wrong.
- **Lags**: Postgres flushes per-backend table stats at transaction end, at most about
  once a second and later under contention (Postgres 15+ behaviour as documented; not
  measured here). A write can therefore take that long to move
  the token. Today's design already accepts this.
- **During a scan** the `cars` counters move on every batch, so the token changes every
  60 s and every metro is rebuilt every minute, today and in Redis alike.
  - Proposal: stale-while-revalidate across tokens. When `ds:v1:lc:{rev}:{tok}:{zip}:{r}`
    is missing and `ds:v1:lc:last:{zip}:{r}` names a body under 120 s old
    (`LISTINGS_MAX_STALE_S`, 0 turns it off), serve that body (`X-Listings-Stale: 1`,
    `public, no-cache`) and rebuild in a background thread under the lock.
  - Shoppers then never wait on a scan-time rebuild, and the cluster builds each metro at
    most once per token.
  - Staleness is at most 120 s, against 60 s today.
- **Deploys**: the key includes `{rev}` and the token includes `CARD_REV` (a hash of the
  serializer sources, `grid_cards_repo.py:57-123`).
  - A deploy that does not touch the card code finds every body already in Redis, if it
    happens inside the same UTC day.
  - A deploy that changes cards misses everything and rebuilds, which is correct.
  - Three staged releases in a row do change them. `redis_raw/p3/card_rev.py` computes the
    revision from git without a checkout (it reproduces the running tree's value for
    1.5.8): 1.5.8 `280f0b3088c53fd9`, 1.5.11 `950f2e4872e28704`, 1.5.12 `e0efb64b90da865e`,
    1.5.16 `db7ce599699b6d8f`. Each of those deploys also turns every stored card into a
    "changed" one, the heavy path of 3.14, until the card builder has run.
- **The UTC day roll** (measured live, 3.1 rows 16-18 and 3.7). The token tuple carries
  `_today()`, so at 00:00 UTC every cached body is invalid, in a worker and in Redis. Cards
  depend on the date only through `price_drop_days_ago` (`day_dep` cards,
  `grid_cards_repo.py:74,650-657`), yet the whole body is rebuilt at 19:00 Eastern, when
  shoppers arrive. Live, the Charlotte r50 body cost 3.45 s TTFB (partial) and then
  0.82 s, and the decoded JSON equalled the previous day's. A shared store alone changes
  one thing here: the cluster rebuilds each metro once, not once per worker.
  What the roll does to a body (`redis_raw/p3/dayroll_sim.py`, 28105 r50, 10,397 cards built
  on day 20,761, `_today()` moved by one or two days, two repeats each, loaded laptop):

  | event | first request after it | partial? | ETag |
  |---|---|---|---|
  | pure roll, warm process | 272 and 319 ms (a rebuild) | no | same (Python 3.14 writes `mtime=0`; on 3.12 it would change, 3.7) |
  | pure roll, cold process | 974 and 918 ms | no | same |
  | cards older than `CARD_MAX_AGE_DAYS` (1 day), warm / cold | 242 and 382 ms / 886 and 792 ms | **yes**, 10,397 cards queued for background re-serialisation | new |
  | market-stats generation change, same day | 869 and 721 ms | **yes**, all 10,397 changed | new |
  | no event, cold process | 123 and 74 ms (no rebuild) | no | same |

  So the roll alone costs one rebuild per metro; what makes the body `partial` (and
  starts the CPU-bound refresh in the web process) is a card that is aged: built more than
  a day ago, or day-dependent (`day_dep`) and built on another day (`_is_aged`,
  `grid_cards_repo.py:650-657`). The live 3.45 s partial body at 01:44 UTC fits that.
  The user-visible fix has three parts, cheapest first:
  1. **Day-roll stale-while-revalidate.** Split the key into `{tokd}` (every token part
     except the day) and `{day}`. When the body for `{tokd}:{today}` is missing but
     `last` names a body with the same `{tokd}` built yesterday, serve it for up to
     `LISTINGS_MAX_STALE_DAYROLL_S` (default 1800, 0 = off) with `X-Listings-Stale: 1` and
     `public, no-cache`, and rebuild in a background thread under the lock. The only
     visible difference is `price_drop_days_ago` a day behind for a few minutes. This
     works in process too (no Redis needed) and is part of R0. In code the token is
     `(base, day, CARD_REV, market_gen, attr_gen, include_incomplete)` (`grid_cards_repo.py:212-219`),
     so an entry is eligible when `entry.token[:1] + entry.token[2:] == token[:1] + token[2:]`
     and `entry.token[1] == token[1] - 1`; the check goes next to `_cars_scope_entry_valid`
     (`listings_api.py:287-295`).
  2. **A warmer.** The process holding `ds:v1:warm:lock` rebuilds the scopes in
     `ds:v1:lc:hot` (the 40 most recently requested), one at a time with a pause between,
     at 00:00:30 UTC and after any non-scan token change. Inside a web worker this competes
     for the GIL (server-causes.md S3; 3.11 measured a worst request of 591 ms during a
     0.9 s background load), so run it as the separate service of 4.8 (R4) or, as a stopgap,
     at a low duty cycle.
  3. **Fix it in the card store** (server-causes.md S1): serve cards whose content is
     unchanged as complete, never `partial`, and compute `price_drop_days_ago` on the
     client; then the day can leave the token and the roll costs nothing.

### 4.4 Building once for the whole cluster

On a miss in both L1 and Redis:
1. `SET ds:v1:lc:lock:... NX PX 15000`.
2. If the lock is won, build exactly as today (the per-IP `listings-build` limiter
   stays here, so only real builds count), `HSET` + `EXPIRE` + `SET last`, release the
   lock.
3. If the lock is lost, poll `EXISTS` every 100 ms for up to 3 s, then build anyway.
   This never deadlocks and never errors if the builder died.

The existing striped in-process locks (`listings_api.py:247-253`) stay for concurrency
inside one worker. Facets work the same way under `ds:v1:fac:lock`.

The read path in full, per request (`get_or_build`):
1. **L1.** A complete entry whose token matches is served. A **partial** entry is not
   final: with Redis up, do one `HGET ds:v1:lc:{rev}:{tok}:{zip}:{r} etag`. If another
   worker has published the complete body, take it. This is how worker B learns that
   worker A's background card refresh finished (`entry.store_gen == store_generation()`
   at `listings_api.py:287-295` compares a counter that only moves in the process whose
   refresher ran, so B would otherwise keep a stale partial body until the token moves).
2. **Conditional request.** If `If-None-Match` equals that `etag`, answer 304 from the
   `HGET` alone and transfer no body (L1 is not filled; a 304 is the cheap case).
3. **L2 hit.** `HMGET key etag n br` (or `gz`), fill L1, serve.
4. **Miss.** Day-roll or scan stale-while-revalidate (4.3) if eligible, otherwise take
   the build lock and build.

**Scan-day churn rule.** The key carries the token, so a token that flips every 60 s
during a scan would leave one 1.95 MB body (1.25 MB gzip + 0.70 MB brotli, 28105 r50) per
flip until the 24 h TTL. With stale-while-revalidate at 120 s that is up to 30 bodies an
hour per hot scope: 30 x 1.95 MB = 58.5 MB an hour, so a 512 MB Redis starts evicting
after about 8.7 hours for one hot scope and about 52 minutes for ten (estimate, the
arithmetic is shown). The builder therefore does `SET ... GET` on the `last` pointer and
`EXPIRE`s the body it names to 120 s (tested in 3.15). The live set stays at one body per
scope plus one that is about to disappear, and the LRU is not asked to do the cleaning.

Streaming assembly: feed `zlib.compressobj(6, wbits=31)` and `brotli.Compressor(5)`
card by card instead of building the 58 MB string and its 58 MB encoded copy. Measured
in 3.6. This caps what a build costs the worker that does it.

### 4.5 Compression and ETags

- Store both `gz` (gzip-6, `mtime=0`) and `br` (brotli-5) in the body hash. For the 20
  scopes that is 56.1 + 28.7 = 84.8 MB, and the build costs one extra compression of
  24-218 ms, once per token.
- Serve `Content-Encoding: br` when `Accept-Encoding` contains `br`, otherwise `gz`.
  Keep `Vary: Accept-Encoding` (already set).
- ETag: `W/"lc-{zip}-{r}-{blake2b10(raw)}"`, the same for both encodings and stable
  across rebuilds and workers. That fixes 3.7.
- Brotli is 48.8% fewer bytes over the 20 scopes. For a phone it is the biggest gain in
  this report (3.6).
- Cloudflare does not do this for you. A request offering `Accept-Encoding: gzip, br`
  came back `content-encoding: gzip` with the origin's 1,585,587 bytes (3.1 row 19), and
  the sibling probes, which also offered `gzip, br`, got origin-sized gzip bodies on every
  scope row (`server-causes-raw/live/rounds.jsonl`: 92618 r25 at about 2.0 MB). The
  origin has to produce the brotli bytes, which is what the `br` field in the body hash
  is for.
- Before relying on brotli for the API, check it after R1 (section 8). Pass 3 found that
  Cloudflare does forward `br` and pass an origin brotli body through for static assets
  (3.16: `content-encoding: br` after a cache MISS, the origin's `ds_core.js.br`), and that
  for the API a br-only client got an edge-converted 1,596,934 B, no smaller than the
  1,585,587 B gzip. If the API's `br` ever does not arrive, keep serving `gz`, which is no
  worse than today.
- Redis and an edge cache rule are complements, not substitutes. A Cloudflare cache rule
  for this endpoint (server-causes.md S5) would answer repeats within `s-maxage=60` from
  the edge, per point of presence, with no origin work, but on a site with this traffic
  the same ZIP and radius rarely repeat inside 60 s from the same colo (hit rate not
  measured; section 8), and the rule needs `Vary: Cookie` gone for anonymous callers. Redis
  keeps the origin's misses cheap for the whole UTC day and across deploys, and gives the
  origin the brotli bytes either way.

### 4.6 Failure behaviour: Redis down must never be an error

Client, created lazily in each worker after fork (the arbiter imports the app with
`--preload`; never open the pool there):

```python
redis.Redis.from_url(
    Config.redis_url(),
    socket_connect_timeout=0.1,   # refused: 0.3 ms; dns failure: ~1.5 ms; blackhole: 0.1 s
    socket_timeout=0.5,           # per recv; 6 MB bodies arrive in many recvs
    retry=Retry(NoBackoff(), 0),  # REQUIRED: the default policy measured 3.2-4.9 s per failure
    health_check_interval=30,
    max_connections=8,            # 4 gthreads per worker
    client_name=f"web-{os.getpid()}",
)
```

- **Breaker**: on any `RedisError`/`OSError`, open for 30 s. While it is open every call
  returns the miss value in 0.095 µs (3.8). Then one probe. Log once per opening, not
  per call.
- **Tested** (3.15): against a hung listener, an unroutable address, an unresolvable
  name, a refused port, a reset in the middle of a reply, `-LOADING`, `-OOM` and garbage, the
  reference module returned a built body every time, the first failure cost 0.5 s at
  most (the read timeout) and every later call 0.004 ms.
- **Fallbacks**, feature by feature, which is today's behaviour:
  - Bodies and facets: build in process (today's latency).
  - Rate limits: `ip_rate_limit` in-process memory (today's semantics).
  - Narrations and Places: call the provider (today's cost).
  - Billing: the per-process 60 s cache.
  - packages-ensure: the per-process dicts.
- **Readiness**: `/api/ready` keeps Redis optional (it never gates; the code already
  does this). It adds `redis_breaker: open|closed` and per-cache hit, miss, error and
  fallback counters.
- **Eviction**: with `allkeys-lru`, an evicted body is a miss, which means a build. Hot
  rate-limit keys are touched constantly, so they are the last to be evicted.
- **Redis restart**: Redis comes back empty (cache-only). The rate-limit windows start
  over, which is what every web deploy does today, but much rarer.
- **Size guard**: refuse to `SET` values over 16 MB. The largest measured gzip body is
  7.4 MB and the largest brotli body 3.6 MB.

### 4.7 Rate limiter: staged 1.5.16 vs Redis

What 1.5.16 does:
- `RATE_LIMIT_SQLITE_PATH` is removed (N37.11). The limiter is in-process only, and
  `production_security.py:104-106` warns when `GUNICORN_WORKERS` is not 1.
- With 1 worker it is exact until the next deploy. Then every window starts over,
  including the 900 s login lockout and the 24 h AI-chat budgets.
- With N workers every limit is N times larger, and the account lockout allows 10 x N
  guesses per 15 minutes.

Redis version: one Lua script per call (sliding-window log). The script reads the clock
with `redis.call('TIME')`, so worker clock skew does not matter.

```lua
-- KEYS[1]=ds:v1:rl:<key>  ARGV: window_ms, max_events, member
local t = redis.call('TIME'); local now = t[1]*1000 + math.floor(t[2]/1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - tonumber(ARGV[1]))
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], now, ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
```

- `allow_request` keeps its signature. It returns the member id to callers that refund.
  `refund_event` then does `ZREM` of that exact member, which fixes the documented "may
  free a parallel caller's slot" caveat.
- Cost: one round trip, only on the 42 limited call sites (auth, chat, narrate, smart
  search, cache-miss builds, recalls, dealer onboarding), not on page views.
- If Redis is unavailable, the call falls back to the in-process window. That is
  today's behaviour, so a Redis outage can never make limits looser than they are now.
- Memory is small: 200 B per key in process; in Redis one ZSET member per event, at most
  `max_events` per key.
- Keys are hashed (`blake2b` 16 bytes) before they reach Redis. Limiter keys contain
  client IPs and account names (emails for the login lockout); they should not sit in
  plain text in a key space that `SCAN`, `MONITOR` or a slow log can print.
- This is the third backend that `docs/CLOUD_RESTRUCTURE_PLAN.md:236` (Phase 4.1) already
  planned ("Redis sorted-set sliding window, selected when `Config.redis_url()` is set,
  fallback Redis, SQLite, memory"). 1.5.16 removed the SQLite middle step (N37.11), so the
  fallback order is now Redis then memory. The plan did not have the Lua script, the
  Redis-side clock, hashed keys or refund by member.

### 4.8 Other shared state

- **packages-ensure** (only with workers > 1): `SET ds:v1:pkg:run:{car} NX EX 60`, with
  the worker thread refreshing it every 20 s, then `ds:v1:pkg:cool:{car}` for 900 s.
  "fetching" then means a worker somewhere is fetching. The OEM sticker fetch itself is
  unchanged (the owner's rule to leave it alone).
- **Billing**: the webhook does `DEL ds:v1:bill:{uid}` and every worker sees the change
  on the next request.
- **`/dev` jobs**: a Postgres table, as `dev/routes.py:85-90` already proposes.
- **Registry backfill**: wrap `ensure_dealership_registry_backfill` in
  `pg_try_advisory_lock(<const>)`, so with N workers one runs it and the others skip.
- **A producer outside the web workers (R4).** 3.12 and 3.14 show that the reader profile
  (0.33 GB per worker, first metro in 7.6 ms) exists only when the worker does not build.
  A small `warmer` loop, run from the same release image as a second Railway service or as
  a step after the scanner's card builder, keeps the stored bodies current:
  1. Every 60 s it reads the shared token (`ds:v1:tok`). When the token or the UTC day
     changed it picks the scopes to rebuild: the 40 most recent members of `ds:v1:lc:hot`
     plus a configured list of large metros.
  2. It builds each one with the production code path (streaming assembly, 3.6), one at a
     time with a pause, and publishes `gz`, `br` and `etag` under the **new** token and day
     before shoppers ask, so the first shopper after the roll or a scan is a hit.
  3. It refreshes the facets (`ds:v1:fac:{fp}`) and the aggregates (3.11) on their own
     timers.
  Web workers keep the in-process build as a fallback behind the single-flight lock, so a
  dead warmer costs speed, not correctness. The warmer is the one process that pays the
  builder's memory (1-2 GB while it builds, locally); restarting it after a fixed number of
  builds bounds that, and Railway bills it only while it holds the memory. Requirements: the
  same release as `web` (so `CARD_REV` and the ETag digest agree), the card builder
  (`python -m backend.scripts.build_listings_grid_cards`, 150-171 s on Railway per
  server-causes.md 4.5) run first after any deploy that changes `CARD_REV`, otherwise every
  warm build takes the full-row path of 3.14. It is also the only fix in this report for the
  19:00 Eastern rebuild that does not depend on serving day-old bodies.
- **Anonymous full-page cache: not proposed.** Every page carries a per-request CSP
  nonce (`web/security.py:35-36`) and a session CSRF token (`web/templating.py:70`).
  Sharing HTML would need placeholder substitution for both, and the car-page hit rate
  over 214k cars is unknown. Measure views per car first.

---

## 5. Gains, quantified

Every figure is local unless marked live; "R0" is the no-Redis step of section 7, "R1" the
shared store; the section numbers point at the measurement.

| | today | Redis alone (R1) | R0 + R1 (streaming assembly, aggregates off-request, shared store) | R0 + R1 + producer (R4) | basis |
|---|---|---|---|---|---|
| Web cluster memory, 2 workers, same traffic, sum of per-worker peaks | 3,145 MB (one worker: 2,199 MB) | 2,898 MB (-8%; per seed -19%, +4%) | **2,410 MB (-23%; per seed -29%, -17%)** | workers read: 812 MB sum of peaks, 654 MB at the end | 3.12 |
| 3 workers (one seed) | 3,203 MB | - | 2,489 MB (-22%) | - | 3.12 |
| End RSS of a worker that builds / one that only reads | 0.9-1.9 GB / - | same | 0.8-1.4 GB / - | - / **0.33 GB** | 3.3, 3.12, 3.14 |
| Builds for the same traffic (21 distinct objects) | 28-30 at 2 workers, 31 at 3 | **21** | 21 | 21, built outside the web workers | 3.12 |
| First 8 `/listings` after a restart, median | 2,387 ms (single A/B: first hit 3,196 ms) | **194 ms** if the token and `CARD_REV` are unchanged | same | same, also after a `CARD_REV` change | 3.12 |
| 20 metros after a restart: median / total wait | 1,088 ms / 31.3 s | **7.6 ms / 0.6 s** | same | same | 3.12 |
| A metro whose body the other worker built | rebuilt: 774 ms median | **3.3 ms median** | same | same | 3.3 |
| First car page after a restart or a TTL edge | max 3.0-4.4 s locally; **live 2.5-3.6 s** at the origin after a 10-minute boundary | unchanged (max 3,149 ms with a warm store) | aggregates read from a row: 0 ms inline, 25 ms background decode | same | 3.11, 3.12, 3.16 |
| Car pages per second, 8 clients, 1 / 2 / 4 workers | 30.2 / 52.4 / 62.5 | 2 workers: 57.2 (+9%) with streaming | same | same | 3.13 |
| Car-page p95 with one shopper on an uncached big metro, 1 / 2 / 4 workers | 751 / 364 / 200 ms | 2 workers: 349 ms | same | same | 3.13 |
| ETag after a rebuild with unchanged content | new ETag, so 200 + 1.59 MB instead of 304 | same ETag | same ETag | same | 3.7 |
| Body bytes on the wire, 20 scopes | 56.1 MB gzip | 28.7 MB brotli (-48.8%) | same | same | 3.5, 3.6 |
| 28105 r50 download, Slow 4G / Fast 4G, phone emulation | 6,480 ms / 1,255 ms | **3,744 ms (-42%) / 766 ms (-39%)** | same | same | 3.6 |
| Rate limits with N workers | x N; reset at every deploy | exact (2 workers: 5 allowed of 8 attempts) | same | same | 3.15 |
| Billable narrations / Places lookups | 1 per worker per process life | 1 per 30 days / 6 h | same | same | 3.2 |

**RAM bill, as an estimate with its arithmetic.** Production is one worker at about 3.3 GB
warm (`gunicorn.conf.py` docstring; not re-measured), which is $33 a month of RAM at $10 per
GB-month. The local ratios of 3.12 are 3,145 / 2,199 = 1.43 for two workers today and 2,410 /
2,199 = 1.10 for two workers with R0 + R1. Applied to 3.3 GB, a second worker adds about 1.4
GB ($14) today and about 0.33 GB ($3) with R0 + R1, plus Redis at most $6. The ratios come from
macOS builds on a locally stale card store, so read them as an order of magnitude, not a
quote; the check is the Railway memory graph after R1 (section 8).

### 5.5 What the shopper feels

- A warm, cached metro is already a 1.7 ms server answer (runtime-timing.md). There Redis
  changes nothing, and the transfer dominates: 2.6-3.1 s for the live 28105 body on this link
  at 13:43 UTC and 0.44-0.56 s at 01:33 UTC. The first byte of a cached hit takes 0.23-0.36 s
  and `/health` (no work at all) takes 0.24-0.34 s: about 160 ms of it is the Cloudflare to
  Railway round trip plus the app (3.1 row 6).
- Brotli cuts the bytes by 32-51% per scope, 48.8% in total (28105 r50 under phone
  emulation, 3.6).
- Redis removes the waits that come from somebody having to build: the first visitor to a
  metro after a deploy or a restart, the second worker, a scan-time token change. With the
  day-roll stale-while-revalidate of 4.3 (or a producer) it also removes the 3.45 s first
  request of each metro at 19:00 Eastern.
- **It does not touch the largest waits a visitor sees today**: the first car page after a
  10-minute boundary (2.5-3.6 s at the origin, live) is the aggregates of 3.11, which a
  Postgres row and a producer fix; the 150 ms edge-to-origin round trip and the 20-38 ms
  Postgres connections are server-causes.md; the 1.4 MB `/listings` HTML and the 1.59 MB
  scope body are live-speed.md and the card work.

---

## 6. Railway setup and cost

Sources read on 2026-11-09 and again on 2026-11-11, unchanged (WebFetch of each page;
quoted, not measured):
[pricing](https://docs.railway.com/reference/pricing/plans),
[Redis guide](https://docs.railway.com/guides/redis),
[private networking](https://docs.railway.com/reference/private-networking),
[how private networking works](https://docs.railway.com/networking/private-networking/how-it-works),
[template manifest](https://railway.com/deploy/redis-1/manifest.json).

- **Service**: in project `dealership-scanner`, environment `production`, add New →
  Database → Redis. The Redis guide says it is "deployed from the redis Docker image".
  The manifest at the URL above (template `redis-1`; confirm in the dashboard that the
  template your click adds matches it) says image `redis:8.2.1`. Name the service
  `redis`.
  - It builds nothing from GitHub, which fits the "no Railway service ever builds from
    GitHub" rule.
  - The `revest-vault` project has its own Redis (docs/SCANNING_OPS_LOG.md:1084) and
    Railway states "Services in different projects cannot communicate over the private
    network", so it cannot be used, and it should not be (it holds vault state).
  - **Region**: create the service in the web service's region. The repo's ops log puts
    web in `us-west2` (server-causes.md S10, quoting the docs); a Redis created elsewhere
    would add a cross-region round trip to every call. The Railway-side latency of a
    same-region call was not measured (section 8).
- **What the template ships, from its manifest**: start command
  `/bin/sh -c "rm -rf $RAILWAY_VOLUME_MOUNT_PATH/lost+found/ && exec docker-entrypoint.sh redis-server --requirepass $REDIS_PASSWORD --save 60 1 --dir $RAILWAY_VOLUME_MOUNT_PATH"`,
  a volume at `/data`, no `maxmemory`, and "TCP port 6379 (TCP proxy enabled)" with a
  `REDIS_PUBLIC_URL`. Variables: `REDISHOST=${{RAILWAY_PRIVATE_DOMAIN}}`,
  `REDISPORT=6379`, `REDISUSER=default`, `REDIS_PASSWORD` (generated),
  `REDISPASSWORD=${{REDIS_PASSWORD}}`, and
  `REDIS_URL=redis://${{REDISUSER}}:${{REDIS_PASSWORD}}@${{REDISHOST}}:${{REDISPORT}}`.
  As shipped it persists (`--save 60 1`) and it has no memory cap, so it would grow
  until the container is killed. Both need changing for a cache.
- **Private networking only.** The guide says "Databases are deployed private by
  default" and that enabling public access "creates a TCP Proxy" for which "you will be
  billed for Network Egress". After the first deploy, open Settings → Networking and
  delete any TCP proxy or public domain on the Redis service, and never put
  `REDIS_PUBLIC_URL` in a variable. Otherwise the only thing between the internet and
  the cache is the password.
  - Private names resolve as `<service>.railway.internal`, so `redis.railway.internal`.
    Railway: "No egress costs: Internal traffic doesn't count toward egress billing."
    Private networking works only at runtime, not during the build.
  - Environments created after 2025-10-16 resolve to IPv4 and IPv6; older ones to IPv6
    only (same page). redis-py resolves through `getaddrinfo`, so both work. **Which kind
    the web service's environment is, and whether private DNS works from it today, was
    not checked.** (`utils/kmac_vault.py:38` names a private vault address, but the vault
    is a different project, `revest-vault` / `vault-api`, docs/SCANNING_OPS_LOG.md:1083,
    and the log says "there is no `kmac-vault` service", so that is not evidence.) Check before the rollout: `railway ssh` into the web
    service, then `getent hosts redis.railway.internal` (section 8).
  - A [community thread](https://station.railway.com/questions/railway-private-network-postgres-connec-711940c8)
    reports Postgres connections that hang silently over `*.railway.internal`. It is one
    user's account and unresolved. It is the "hung listener" case of 3.8, which the
    client settings in 4.6 turn into a 50-250 ms miss instead of a hang.
- **Web variable**: `REDIS_URL` = a reference to the Redis service's `REDIS_URL` (Railway
  reference-variable syntax `${{redis.REDIS_URL}}`; the Variables tab autocompletes it).
  - Check that the host is `redis.railway.internal`, not a `*.proxy.rlwy.net` proxy
    host.
  - Changing a web variable redeploys the web service. Follow the project rule: run the
    web source check first, and redeploy the tagged release with `railway redeploy`.
- **Start command (cache-only)**: replace the template's with
  `/bin/sh -c "exec docker-entrypoint.sh redis-server --requirepass $REDIS_PASSWORD --maxmemory 512mb --maxmemory-policy allkeys-lru --save '' --appendonly no"`.
  - Railway's guide: "Changing the template as shipped, such as its image, start command
    or variables, makes the database yours to manage." The image tag then stays where
    you put it until you change it.
  - Why not persist: the contents are derived and rebuildable, and an RDB snapshot forks
    the process, which can double its memory while multi-MB values are dirty. A restart
    starts empty, which is safe (4.6). Remove the volume, or leave it (0.15 $/GB-month).
- **Memory**: `maxmemory 512mb`. The measured working set is small:
  - 20 metros: 84.8 MB with both encodings stored.
  - Facets: about 1.0 MB with all three fields (219 KB + 74 KB + 725 KB).
  - Fleet aggregates: 0.49 MB for the five documents (3.11).
  - Vocabulary 0.18 MB; cohorts 0.075 MB each.
  - Rate-limit keys: kilobytes.

  So 512 MB holds about 54 OC-sized bodies (9.4 MB each with both encodings) or about
  260 Charlotte-sized ones (1.95 MB) (sizes in 3.5). Watch `used_memory` and
  `evicted_keys` and adjust. The one thing that fills it is scan-day churn (4.4), which
  the `EXPIRE` rule there bounds.
- **Cost** (pricing page, quoted: memory $10 per GB per month, CPU $20 per vCPU per
  month, network egress $0.05 per GB, volume storage $0.15 per GB per month; Hobby $5
  per month including $5 of usage, Pro $20 including $20; billed on usage):
  - Full at about 0.6 GB RSS: 0.6 x $10 = about **$6/month** of RAM.
  - Idle at a few MB: cents.
  - CPU for a mostly-idle cache: a small fraction of a vCPU. Not measured; check the
    Railway metrics after a week.
  - No egress on the private network, and no volume if removed.
- **Then**:
  - `GUNICORN_WORKERS=2` once R1+R2 are live (measure RSS per worker and p95).
  - 3-4 workers only with a psycopg pool (3.4).

---

## 7. Rollout

The order follows the evidence: first what needs no Redis and removes the largest waits
(3.7, 3.11, 3.14), then the shared store, then the limits, then the money savers, then the
producer that makes the workers small, then more workers.

| step | change | flag | test |
|---|---|---|---|
| R0 (**no Redis needed**; helps the single worker today) | (a) `gzip.compress(..., mtime=0)` and an ETag from the digest of the raw body (3.7); (b) the five fleet aggregates off the request path (3.11): a lock around the rarity loader, stale-while-revalidate for all five, then one producer writing a `fleet_aggregates` Postgres row per document (post-scan step or one refresher under `pg_try_advisory_lock`), inline compute only as the cold fallback; (c) streaming assembly of the scope body with `zlib.compressobj` (3.6, 3.14: -32% resident RSS in the retention run); (d) in-process stale-while-revalidate across a scan token change and across the UTC day roll (4.3); (e) advisory lock on the registry backfill; (f) `backend/cache/shared.py` (client, breaker, codecs), unused until R1. `redis` and `brotli` are already in `requirements-web.txt` | `LISTINGS_MAX_STALE_S=120`, `LISTINGS_MAX_STALE_DAYROLL_S=1800`, `AGG_BACKEND=inproc\|pg` | unit: the same body gives the same ETag across calls and rebuilds; the day-roll rule serves the previous day's body only when the other token parts are equal; the aggregate loaders return the same values as today (rarity Counters equal, as in `harness_rarity_sql.py`); four concurrent callers at a bucket edge run the loader once; failure tests (closed port, hung listener) return within 600 ms with no exception (`proto_tests.py` is a starting point) |
| R1 | shared token; L2 bodies with lock, stale-while-revalidate, partial re-check and 304 from one `HGET` (4.4); facets in Redis keyed by the fingerprint alone; refresh claim; `br` field; `lc:hot` list; scan-day `EXPIRE` rule | `REDIS_LISTINGS=1`, `LISTINGS_BR=1` | a pg-tier test with a Redis service container in CI (the `pytest-pg` job gains `services: redis`); the 3.12 series against real Redis; two workers: a body built by one is a hit for the other and carries the same ETag; after R1 `curl -H 'Accept-Encoding: br'` must return `content-encoding: br` (3.16) |
| R2 | rate limiter on Redis (Lua, hashed keys), refund by member | `RATE_LIMIT_BACKEND=redis` (falls back to memory) | two processes share one limit; refund frees its own slot; Redis down falls back to memory (all three already hold in `proto_tests.py`) |
| R3 | narrations, Places, cohorts, vocabulary (keyed by day), parse cache and per-area examples (search-promotion.md section 9), dealer cards, billing invalidation, optional `agg:*` Redis copy | per feature | hit and miss counters in `/api/ready` |
| R4 | the `warmer` producer (4.8): builds the hot scopes and facets for the new token and day before shoppers ask, so web workers read and rarely build; run the card builder first after any `CARD_REV` change | `LISTINGS_WARM=1` | 3.12's reader profile on Railway: web RSS about 0.3-0.5 GB per worker, first request of a hot metro after a roll is a hit |
| R4b (with workers > 1) | packages-ensure claims, `/dev` jobs to Postgres, recommendation versions | none | poll a job from a second worker |
| then | `GUNICORN_WORKERS=2`; with a connection pool, 3-4 | Railway variable | Railway memory graph and p95 |

What to watch after each step: per-worker RSS, `evicted_keys` and `used_memory` in Redis,
the hit, miss, error and fallback counters in `/api/ready`, the number of builds per hour
(should equal the number of distinct hot scopes per token, not that times the worker count),
and the live origin time of the first car page after a 10-minute boundary (2.5-3.6 s today,
3.16).

---

## 8. Not measured here, and how to measure it

| question | why not here | how |
|---|---|---|
| production web RSS today (1.5.8, 1 worker), and whether production retains freed memory the way macOS does (3.14) | no Railway access in this task | Railway memory graph, or `railway ssh` then `ps -o rss= -p 1`; log RSS around each scope build; try `MALLOC_ARENA_MAX=2` and jemalloc through `LD_PRELOAD` |
| Redis server memory per body (allocator rounding), eviction behaviour, restart behaviour | no Redis server here (the shared store of 3.12 is a directory; 3.15 uses fakeredis and failing sockets) | on Railway: `redis-cli MEMORY USAGE ds:v1:lc:...`, `INFO memory`, `INFO stats` (`evicted_keys`) after a day |
| GET latency for a 1.25-6 MB value on the private network | same | run the client part of `redis_raw/resp_stub_bench.py` from a one-off web shell against the real `REDIS_URL` |
| Cloudflare passing an origin **brotli** body through for `/api/listings/cars` | the origin serves only gzip there today | **static assets: yes**, 3.16 (`content-encoding: br` after a cache MISS, `content-disposition: ds_core.js.br` is set by the origin). For the API, after R1: `curl -s -D - -o /dev/null -H 'Accept-Encoding: br' 'https://sarraficars.com/api/listings/cars?zip=28105&radius=50'` must show `content-encoding: br` and a body near 0.9 MB (a gzip-to-brotli conversion at the edge gave 1,596,934 B, no smaller than gzip, 3.16) |
| how many distinct (zip, radius) scopes a day, the repeat rate inside 60 s per colo, the car-page views per car | no access logs (gunicorn does not log access) | count `/api/listings/cars` per scope for one day (log line in `api_listings_cars`); after any edge rule, count `cf-cache-status: HIT` |
| scan-time token churn in production | scanners are not writing to the production database now (token stable for at least 34 min, 3.1 #13 and #15) | log `ds:v1:tok` changes per hour during the first production nightly scan |
| whether the web service's environment can resolve and reach `redis.railway.internal` (legacy IPv6-only or dual-stack) | no Railway access | after creating the service: `railway ssh` into web, `getent hosts redis.railway.internal`, then `python -c "import os,redis;print(redis.Redis.from_url(os.environ['REDIS_URL'],socket_connect_timeout=1).ping())"` |
| which card-store regime production is in (current cards, or stale/aged ones that take the full-row path of 3.14) at a given hour | the live site shows `x-listings-partial: 1` on first requests (3.1 #7, #17), but not why | log `res.stats` (`fresh`, `changed`, `deferred`) for every scope build |
| whether the UTC day roll, and not a data write, moved the token at 01:44 UTC (3.1 #17) | the token value is not exposed over HTTP | log `repr(grid_scope_token())` parts at 00:00 UTC once, or check that the `pg_stat_user_tables` counters of the five tables did not move across the roll. By code and simulation (4.3) the roll alone does invalidate every body |
| the pg_stat flush lag (4.3) | the local database is read-only here (every counter is 0) | on a staging copy: `UPDATE` one `cars` row, then poll the fingerprint query until it moves |
| which Redis template the dashboard adds and that its TCP proxy can be removed | no Railway access | dashboard: service, Settings, Networking; compare the start command with the one in section 6 (manifest and guide re-read on 2026-11-11, unchanged) |
| throughput at 3-4 workers on a quiet machine | the laptop's load average was 8-12 during every run, and `loadtest.py` opens a connection per request | repeat `redis_raw/p3/run_load.sh` when idle; server-causes.md measured 141 req/s at 4 workers with keep-alive and one URL |

---

## 9. Commands and raw outputs

Scripts are in `docs/web_speed_search_2026_11/redis_raw/`. The JSON outputs are copied
there. The large bodies (up to 67 MB) were left in the session scratchpad.

```
# shared env for every local harness (read-only DB, scratch SQLite copies)
S=<scratch>/redis; cd .
export PGOPTIONS='-c default_transaction_read_only=on' KMAC_VAULT_DISABLE=1 DS_SKIP_REGISTRY_BACKFILL=1 \
  USERS_DB_PATH=$S/sqlite/users.db DEV_USERS_DB_PATH=$S/sqlite/dev_users.db \
  DEALER_PORTAL_DB_PATH=$S/sqlite/dealer_portal.db INCOMPLETE_LISTINGS_DB_PATH=$S/sqlite/incomplete_listings.db PYTHONPATH=.

.venv/bin/python redis_raw/harness_inventory.py  out/inventory.json    # 3.2, 3.5, facets, token, dealer cards, vPIC memo
.venv/bin/python redis_raw/harness_inventory2.py out/inventory2.json   # lru occupancy, limiter, cohorts, vocab, routes
.venv/bin/python redis_raw/harness_ab.py build  out/ab_build.json      # 3.3 (writes out/gz/*.gz, out/facets_ab.json)
.venv/bin/python redis_raw/harness_ab.py shared out/ab_shared.json     # 3.3
redis_raw/run_load.sh 1|2|4 ; CLIENTS=16 SKIP_NOISY=1 redis_raw/run_load.sh 1|2|4   # 3.4
redis_raw/cpu_sample.sh                                                # 3.4 sessions per page, CPU split
.venv/bin/python -I redis_raw/harness_compress.py out/compress.json out/body_37421_r25.json out/body_28105_r50.json out/body_92663_r50.json  # 3.6
.venv/bin/python -I redis_raw/br_all.py out/gz out/br_all.json         # 3.5 brotli column
.venv/bin/python -I redis_raw/stream_build_mem.py out/body_92663_r50.json out/stream_build.json   # 3.6
.venv/bin/python -I redis_raw/phone_codec.py out/phone_codec.json out/body_28105_r50.json         # 3.6
.venv/bin/python -I redis_raw/harness_redis_failure.py out/redis_failure.json                     # 3.8
.venv/bin/python -I -W ignore redis_raw/retry_default_check.py                                    # 3.8 (default retry trap)
.venv/bin/python -I redis_raw/resp_stub_bench.py out/resp_stub.json out/body_*.gz                 # 3.9
redis_raw/live_get.sh <label> '<path>'                                 # 3.1 (anonymous GET, >=1.1 s apart)
/opt/homebrew/bin/python3.12 -I -c '<gzip mtime check>'                # 3.7
psql "postgresql:///cars?host=/tmp" -c '<read-only counts>'            # 2, 3.5 ZIP selection

# second pass (2026-11-09 20:30-21:30 EST)
.venv/bin/python -I -W ignore redis_raw/retry_default_check.py | tee redis_raw/retry_default_check_rerun.jsonl   # 3.8 re-run: 5,669 / 3,518 / 4,074 ms default, 51 ms fixed
.venv/bin/python redis_raw/harness_aggregates.py out/aggregates.json    # 3.11 (read-only env above)
.venv/bin/python redis_raw/harness_rarity_sql.py out/rarity_sql.json    # 3.11 GROUP BY vs the shipped loop, 4 Counters compared
.venv/bin/python redis_raw/harness_conn_per_page.py out/load_car_ids.txt out/conn_per_page.json 100   # 3.4 exact connections per car page (median 10)
cd out_live && ../redis_raw/live_get.sh G1_health /api/health          # 3.1 rows 16-18 (>=1.1 s apart, cap enforced by counter.txt)
  ./live_get.sh G2_cars_28105_r50_inm_after_utc_midnight '/api/listings/cars?zip=28105&radius=50' -H 'If-None-Match: W/"lc-28105-50-82e728fb3aa888a49f33"'
  ./live_get.sh G3_cars_28105_r50_followup '/api/listings/cars?zip=28105&radius=50'
AE='gzip, br' redis_raw/live_get_ae.sh H1_cars_28105_r50_ae_gzip_br '/api/listings/cars?zip=28105&radius=50'   # 3.1 row 19
python3 -I redis_raw/gz_header_proof.py <dir with resp_*.body> redis_raw/gz_header_proof.json                 # 3.7 production proof
git grep -n -E '^_[A-Za-z0-9_]+(: [^=]+)? = (\{\}|dict\(\)|OrderedDict\(|defaultdict\(|set\(\)|\[\]|deque\()' faea2b985 -- 'backend/*.py' 'backend/**/*.py'   # 3.2 completeness (rows 29-32 came from this)
git grep -n 'listings_grid_serialized_cars' faea2b985 ; git grep -n 'listings_grid_serialized_cars(' 0fd91872b -- backend   # 3.2 row 32: tests only
git grep -n -i -E '\bredis\b|REDIS_URL|import redis|redis_url' faea2b985 -- 'backend/*.py' 'backend/**/*.py' 'requirements*.txt'   # config.py, routes/health.py, tests only
git show faea2b985:requirements-web.txt | grep -n -E '^(redis|brotli)'  # 23, 36
WebFetch: docs.railway.com/reference/pricing/plans, /guides/redis, /reference/private-networking, /networking/private-networking/how-it-works, railway.com/deploy/redis-1/manifest.json   # section 6
```

Third pass (2026-11-10 20:20 to 2026-11-11 21:00 EST). `P3` is the session scratchpad's `p3/`
directory (`scratchpad/p3`);
every local harness sources `redis_raw/p3/env.sh` (read-only database session, empty scratch
SQLite files, no vault, no registry backfill). Scripts and the small outputs are in
`redis_raw/p3/` and `redis_raw/p3/out/`; the stores of finished bodies (tens of MB) stayed in `P3/out/`.

```
bash redis_raw/p3/run_agg.sh                          # 3.11: aggregates, double count, herd, GIL, warm loaders, page cost after expiry, GROUP BY
                                                      #   -> out/aggregates_p3.json double_count_*_p3.json herd_p3.json gil_p3.json warm_loaders_p3.json carpage_ttl_p3.json rarity_sql_p3.json run_agg.log
bash redis_raw/p3/run_mw_all.sh                       # 3.12: single-process A/B (harness_ab.py build|shared, harness_ab_stream.py plain|stream),
                                                      #   then 11 gunicorn sessions (run_gunicorn.sh) and the restart profile (run_e8.sh) -> out/ab/, out/mw/, mw_series.log
.venv/bin/python -I redis_raw/p3/summarize_mw.py P3/out/mw    # the 3.12 tables
bash redis_raw/p3/run_load_all.sh                     # 3.13: 1, 2, 4 workers, today and store + streaming -> out/load/results.jsonl, rss.jsonl
bash redis_raw/p3/run_retain.sh                       # 3.14: tracemalloc retention, plain and streaming -> out/retain/
.venv/bin/python redis_raw/p3/h_cards_stats.py out/misc/cards_stats.json     # 3.14: fresh / changed path per scope
P3/venv_fake/bin/python redis_raw/p3/lua_validate.py  # 3.15 (python3.12 venv: fakeredis[lua] 2.40.0, redis 8.1.0, outside the repo)
P3/venv_fake/bin/python redis_raw/p3/proto_tests.py   # 3.15: logic on fakeredis + eight failure shapes with the real client -> proto_tests.out
python3 redis_raw/p3/live_get.py <label> <path> [accept-encoding]            # 3.16 (anonymous GET, >= 1.2 s apart, budget 80)
.venv/bin/python redis_raw/p3/live_idle_cycles.py 2 1040505 1193787 1594974 1245819 1040512 1040510 1193794 1040515 1040514 1594684 1040517 1040516
T_FRESH=20761 .venv/bin/python redis_raw/p3/dayroll_sim.py 28105 50 out/misc/dayroll_28105_r50.jsonl 2   # 4.3 (20761 = built_day of the 28105 area's cards, from psql)
.venv/bin/python -I redis_raw/phone_codec.py out/phone/phone_codec_p3.json <scratchpad>/redis/out/body_28105_r50.json   # 3.6 re-run
.venv/bin/python -I redis_raw/p3/inv_grep.py faea2b985 out/inv_grep_tip.json                              # 3.2 completeness (git grep at the staged tip)
python3 -I redis_raw/gz_header_proof.py <scratchpad>/redis/live redis_raw/p3/out/gz_header_proof_p3.json     # 3.7 re-run on the saved production bodies
.venv/bin/python -I redis_raw/p3/card_rev.py 0fd91872b phase/N6b phase/AF faea2b985                          # 4.3: CARD_REV per release, read-only git -> out/card_rev_p3.txt
/opt/homebrew/bin/python3.12 -I - <scratchpad>/redis/live/resp_A1_cars_28105_r50.body                        # 3.7: the production gzip line twice, 1.1 s apart -> out/gzip_mtime_py312_p3.txt
psql "postgresql:///cars?host=/tmp" -c "select g.built_day, count(*) ... where d.latitude between 34.6 and 36.2 and d.longitude between -81.6 and -79.9 group by 1"   # read-only, day-roll sim input
WebFetch: docs.railway.com/reference/pricing/plans, /guides/redis, railway.com/deploy/redis-1/manifest.json   # section 6, 2026-11-11
```

The challenge rounds' outputs that pass 3 re-ran (not quoted): `r4_challenge_raw/h_double_count.py`,
`h_herd.py`, `h_gil.py`, `h_warm_loaders.py`, `h_carpage_ttl.py` (3.11), the E2/E8 gunicorn series
(`challenge_r3/`, re-implemented as `p3/wsgi_p3.py` and `run_gunicorn.sh`, 3.12), `phone_codec.py` (3.6) and
`dayroll_sim.py` (4.3). The challenge round's ETag/gzip-header proof (R1) and Cloudflare compression
probes (R5) agree with 3.7 and 3.16; this pass re-ran the gzip-header proof on the saved production
bodies and the Python 3.12 check (3.7), and repeated the compression probes (3.16).

Screenshots: none. This topic changes no page, and the phone-emulated measurement
(`phone_codec.py`) fetches a JSON body, so there is no visual state to capture.
