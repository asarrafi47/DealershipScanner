# Live site speed: how slow sarraficars.com is, and why (measured 2026-11-09, re-verified 2026-11-10)

Measured against the live site (release 1.5.8, commit `0fd91872`, confirmed by `GET /health` on both days), behind
Cloudflare (colo MIA in the morning samples, IAD and ATL in the later ones) and Railway (`x-railway-edge` mia1, iad1,
atl1), one gunicorn worker. The server side comes from 224 anonymous GET requests (curl, at most one per
second; 189 in the first run and 35 in the resumed re-verification run of section 10); the browser side comes from
Chromium run against a replay of what the live site sent (phone: iPhone 14 viewport, 9 Mbps, 150 ms RTT, 4x CPU
slowdown; desktop: unthrottled). Every number below comes from a command in sections 9 and 10. Nothing in the
repository, git or any database was changed; the only files written are this report and `raw/live-speed/`.

## The short answer

**On a fast laptop with a warm origin the site is not slow: 0.4 to 0.7 s per page (1.4 s for a listings page that
searches by itself), 0.5-0.6 s for a search. On a
phone it is 0.9 to 1.5 s per page and 2.7 to 4.2 s for anything with results, and on any device the first visit
after a quiet spell can take 2 to 5 s.** Four things stack up.

1. **No HTML or data is cached on the way (only static files are), so every page view is a full trip to one origin.**
   Cloudflare answers `DYNAMIC` for every HTML and API response (125 of 125 measured on both days);
   the pages carry a session cookie, `Vary: Cookie` and a per-request CSP nonce. A request that does no work already
   takes 120-176 ms (`/health`; 68-111 ms of that is origin time, 76 ms median); a real page adds 40-250 ms of app
   time (home +41, dealership +146, listings +238, car page +250 ms above that floor), so a phone waits 0.6-0.9 s for
   the first byte of any page.
2. **The first request after a quiet spell pays for cold server caches.** The car page was cold in
   4 of 5 quiet-spell rounds (1.85, 2.19, 2.22 and 2.09 s of origin time against 0.24-0.33 s warm), the first home page of
   the day took 3.0 s, and the first search in an area 1.6-5.5 s (13.4 s for Irvine in the server report). A first
   visitor with a cold origin waits **4.3 s** to open home and **7.5 s** for a search (first-hit times of the
   morning; with this run's 1.6 s first-hit sample for the area the same search takes 4.2 s).
3. **A search downloads and processes the whole area instead of a page of results.** 13,298 cars, 14.5 MB of
   JSON (1.59 MB gzip), and the page keeps 13,966 grafted filter options (1,913 models, 12,013 trims, 40 packages; 55,740 DOM nodes) that every search re-walks. On the phone that is **2.7 s to
   the first card**: 57 % transfer, 14 % before the data request even starts, 17 % main thread. With the first 24
   cars only and the facet lists loaded on demand the same step takes **0.9 s with no long task** (what-if run,
   section 5.2). A returning shopper whose ZIP is remembered waits **4.2 s** for the listings page, which also pulls
   1.34 MB of 1280-pixel dealer photos for the first screen.
4. **Switching pages is a full reload, and the speed-up script works against it.** The prefetch script downloads
   every uncacheable page twice (tapping Listings: 1,561 ms instead of 1,111 ms on the phone, 787 ms instead of
   441 ms on the desktop), a session-cookie rewrite after the search makes the prefetched car pages unusable on a
   fast connection (desktop tap-to-car 495 ms instead of 98 ms), and without the back-forward cache, Back costs 2.0-2.6 s instead of 0.15 s.

The stalls in 2 are what make the site feel broken; 3 and 4 are what make it feel slow on a phone every time. The
15 render-blocking stylesheets (88-94 % unused on every page) and the dealer photos cost less than any of these in
the measurements here (the photos mostly decide when the first card's picture appears, 4.5 s after the tap on the
remembered-ZIP path). Redis fixes the first-search miss and the worker memory problem, not the phone (section 6).

**Re-verified on 2026-11-10 (section 10).** 35 further live requests, local browser re-runs against the same replay and
a recomputation of every table from the raw files reproduce the first run: the phone steps agree within about
5 % (tap Listings 1,570 vs 1,561 ms, search 2,734 vs 2,743 ms, car from results 258 vs 253 ms, Back 150 vs 148 ms),
the live release, headers, payload sizes and `DYNAMIC` status are unchanged, and the cold car-page stall reproduced
on the first request after about 23 quiet hours (2,090 ms of origin time against 237 ms on the repeat). Four
things moved and are corrected in the text (section 10.6): a first search in a cold area took 1.6 s this time
(5.1-5.5 s before), the Back step without the bfcache took 2.3 s (2.05 s before), the page grafts 13,966 filter
options, not "about 7,300", and the two what-ifs the first run had not finished are now in (edge cache A/B, section
5.4; CSS, section 5.3, where the pixel check rejected the first two builds).

## Numbers at a glance

Warm-origin replay unless stated. Phone = iPhone 14 viewport, 9 Mbps, 150 ms RTT, 4x CPU; "usable" is defined in
section 1.2. The re-run column is the median of 3 runs on 2026-11-10 (section 10.3).

| step | phone | phone, re-run | desktop | phone, first visitor with a cold origin |
|---|---|---|---|---|
| open home | 932 ms | 895 ms | 439 ms | 4,323 ms |
| open listings (no ZIP) | 1,456 ms | 1,429 ms | 684 ms | |
| open listings, ZIP remembered | **4,183 ms** | 4,224 ms | 1,355 ms | |
| tap Listings on home | 1,561 ms | 1,570 ms | 787 ms | 2,352 ms |
| search, tap to first cards | **2,743 ms** (first card 2,645) | 2,734 ms | 556 ms | **7,548 ms** |
| open a car from the results | 253 ms (prefetched) | 258 ms | 495 ms | |
| open a car, cold | 1,211 ms | 1,204 ms | 669 ms | |
| open a dealership page | 1,540 ms | 1,524 ms | 579 ms | |
| Back from the car page | 148 ms (bfcache) / **2,054 ms** (no bfcache) | 150 ms / 2,282 ms | 63 ms | |
| live `/health` (no app work), warm connection | 153 ms (147-176) | 120-133 ms | | |
| live API for 28105/50, first request of the day | **5,064 ms** TTFB (123-498 ms after) | 1,635 ms (202 ms after) | | |

## Ranked causes

Ranked by phone seconds per ordinary visit, then by how often the cost is paid. Each row's number was measured;
"what-if" numbers are replay runs where one thing was changed inside the browser (section 5.2). The ranking is mine;
the numbers are the evidence.

| # | Cause | What it costs (measured) | Fix | Effort |
|---|---|---|---|---|
| 1 | **A search fetches and processes the whole area, and the filter lists are grafted into the DOM** | Phone: tap to first cards **2,743 ms** (flow; 2,734 ms re-run) / 2,784 ms (what-if baseline). 57 % is the 1.59 MB transfer (1,503 ms), 14 % the wait before the request starts, 17 % main thread. The 574 KB facet list adds 55,740 DOM nodes and **621 ms** to every search (2,682 to 2,061 ms; re-run 2,789 to 2,108 ms). What-if: first 24 cars only **1,007 ms** (re-run 1,267 ms); plus facets on demand **911 ms, 0 long tasks** (re-run 915 ms). Returning shopper: listings with ZIP **4,183 ms**, 3.4 MB | Send the first 24 cars with the total and the facet counts, load the rest in the background only if the browser still needs it for filtering, load the facet option lists when the Filters panel opens (all filters stay), brotli or zstd on the body (-45 %, 879,659 B), resized photos | M-L (the what-if truncates the body, it does not keep the facet counts correct) |
| 2 | **Cold server caches after a quiet spell** | Car page: **1.85, 2.19, 2.22 and 2.09 s** of origin time in 4 of 5 quiet-spell rounds (the fifth found it warm at 0.34 s; morning 3.38 s; warm 0.24-0.33 s); first home of the day 2.88 s; first search per area **5.06 and 5.53 s** TTFB in the morning, **1.64 s** in the re-run (123-498 ms and 202 ms after); cold-origin visitor: home **4.3 s**, search **7.5 s** | Refresh the TTL caches in the background before they expire, build the area bodies off the request path, warm after deploy (Redis can hold the result, it does not build it) | S-M |
| 3 | **No edge cache and a 120-175 ms floor on every page** | `/health` 120-176 ms (origin part 76 ms); app time above the floor: home +41, dealership +146, listings +238, car +250 ms; Cloudflare `DYNAMIC` on 125 of 125 HTML and API responses; phone TTFB 608-861 ms. What-if (replay, 5 interleaved pairs, phone): the HTML and the two JSON endpoints answered after 15 ms instead of 155-323 ms: usable home 947 to 791 ms (-156), listings 1,497 to 1,139 (-358), listings with ZIP 4,150 to 3,646 (-504), dealership 1,526 to 1,317 ms (-209). | A cache rule for the two listings JSON endpoints (drop `Vary: Cookie` there); cookie-free, nonce-free anonymous HTML so the pages can be cached too; drop `Set-Cookie` where nothing reads the session. Server side, `server-causes.md` S10 estimates the 76 ms floor would fall to 20-30 ms if web and Postgres moved to an East US region (their estimate, not measured here) | S-M |
| 4 | **The prefetch script sends uncacheable pages twice, and a cookie rewrite defeats the car prefetch** | Tap Listings: first byte **1,005 ms vs 546 ms** without the script, usable 1,561 vs 1,111 ms (phone; re-run 1,570 vs 1,097 ms); 787 vs 441 ms (desktop). 189,641 B more per tap. Desktop tap to car 495 ms with the prefetch (499 ms re-run) and 495 ms without it, **98 ms** when the geo POST leaves the cookie alone (`Vary: Cookie` plus the POST's new cookie); returning shopper's listings load 2,256 ms phone / 904 ms desktop vs 1,747 / 631 ms. 2-6 car renders per search on the one worker | Prefetch only cacheable links (car pages), stop rewriting the session cookie in the geo POST (or drop `Vary: Cookie` from car pages) | S |
| 5 | **The listings page itself: 1.55 MB of HTML, 82 % of its wire bytes one JSON island, 46,803 elements** | Phone 1,456 ms usable; island = ~155 KB of the 189,535 B sent, **95 ms** of script per load; no cache headers, so each visit pays it again | Load the fleet-wide facet table on demand (it is not needed before a search) | S-M |
| 6 | **Photos: 1280-pixel stock images used as thumbnails** | Home: one **551 KB** image of 618 KB third-party; results screen: **665 KB + 534 KB** images (1.34 MB) racing the 1.59 MB API body on 9 Mbps; LCP 4,544 ms (4,220 ms re-run) | Resize or serve responsive images from the site's own origin; lazy-load below the fold | M |
| 7 | **15 render-blocking stylesheets and 16 scripts, mostly unused** | 283 KB of CSS, **88-94 %** unused at the phone viewport; 53-68 % of JS bytes never run on the page; home FCP 988 ms = 608 ms TTFB + 380 ms (about 150 ms of that is the round trip for the stylesheets, 134 ms style+layout; estimate); car page style+layout 230 ms. What-if (replay, 6 runs per cell, phone): one merged stylesheet moves first paint by +26 / +24 / +2 ms (home / car / listings), i.e. nothing; a stylesheet with only the rules used at 390 px (4.4-7.4 KB br instead of 51.6 KB; screenshots match, 0.0 / 1.6 / 0.0 % of pixels differ) saves **26 / 38 / 26 ms** of first paint and 13 / 45 / 42 ms of style+layout. | Low priority: a per-page bundle of the used rules, built with coverage at both viewports and checked by screenshots at 390 and 1440 px (the what-if's first two builds changed the rendering); defer the unused scripts | M-L |
| 8 | **Back without the bfcache re-downloads and re-renders the results page** | **2,054 ms** (2,282 ms re-run) instead of 148 ms (190 KB HTML, 5 long tasks, 624 ms) | Make the results page cheap to re-enter (causes 1 and 5 do it) | follows 1, 5 |

## What each change buys on a phone (what-if runs)

| change | before | after | source |
|---|---|---|---|
| first 24 cars from the API (24,445 B JSON) | search to first cards 2,682 ms (re-run 2,789) | **1,007 ms** (-62 %; re-run 1,267 ms, -55 %), 49 KB instead of 1.61 MB | section 5.2 `paged_api` |
| + facet lists on demand (no 574 KB request, 55,740 fewer nodes) | 2,784 ms (re-run 2,789) | **911 ms**, 0 long tasks (-67 %; re-run 915 ms, -67 %) | section 5.2 `both` |
| facet lists on demand alone | 2,682 ms (re-run 2,789) | 2,061 ms (-23 %; re-run 2,108 ms, -24 %) | section 5.2 `no_facets` |
| brotli q5 on the 14.5 MB body (arithmetic) | 1.59 MB, 1,410 ms of transfer | 0.88 MB, about 780 ms | `payload_probe.py` |
| do not prefetch `/listings`, home, dealership pages | tap Listings 1,561 ms | 1,111 ms | section 4.4 |
| JSON island out of `/listings` | 142 ms script per load | 47 ms | section 5.2 `no_island` |
| the geo POST leaves the session cookie alone | desktop tap to car 495 ms; returning shopper's listings load 2,256 ms (phone) | **98 ms**; 1,747 ms (-23 %) | section 4.4 |
| the HTML pages and the two listings JSON endpoints answered at the edge (15 ms of wait instead of 155-323 ms) | usable, phone: home 947, listings 1,497, listings with ZIP 4,150, dealership 1,526 ms | 791 / 1,139 / 3,646 / 1,317 ms (-16 / -24 / -12 / -14 %) | section 5.4 |
| a per-page stylesheet with only the rules used at 390 px (4.4-7.4 KB br instead of 51.6 KB in 15 files) | first paint: home 1,030, car 1,260, listings 1,162 ms | 1,004 / 1,222 / 1,136 ms (-26 / -38 / -26 ms, -2 to -3 %); screenshots identical | section 5.3 |

**Order by gain per effort** (my reading of the two tables above; the gains are the what-if numbers, not promises):

1. **Cause 4, the prefetch policy and the cookie rewrite (S).** About 450 ms off every tap on Listings on a phone and
   346 ms on a desktop, 397 ms off the first tap on a car on a fast connection, and 509 ms off a returning shopper's
   listings load (phone), with no change to what the visitor sees.
2. **Cause 2, cold caches (S-M).** Removes the 2-5 s stalls of the first visit; the car page and the first search in
   an area are the two cases measured.
3. **Cause 5, the fleet-wide facet table out of `/listings` (S-M).** About 155 KB less on the wire and 95 ms less
   script on every load of the page.
4. **Cause 1, results in pages and facet lists on demand (M-L).** 1.5-1.9 s off every search on a phone (what-if),
   the biggest single number in the report, and the one that needs a design decision about where the filter counts
   are computed. All filters stay.
5. **Cause 3, the edge (S-M).** The JSON cache rule first (the response already says `s-maxage=60`), then cacheable
   HTML; 150-360 ms off every page in the what-if.
6. **Causes 6-8 (M, M-L, follows 1 and 5)** after that; cause 7 is worth about 30 ms of first paint in the what-if.

## 1. How this was measured

Everything below was measured on 2026-11-09 (UTC 13:40 to 2026-11-10 02:40) and re-verified on 2026-11-10
(local 20:20 to about 21:15, which is 2026-11-11 01:20 to 02:15 UTC; section 10) against release 1.5.8
(commit `0fd91872`, confirmed by `GET /health` on both days), behind Cloudflare (colo MIA in the morning samples,
IAD and ATL in the later ones) and Railway (`x-railway-edge` mia1, iad1, atl1), one gunicorn worker. Nothing in the repository, git or any database was
changed; the files written are this report and its `raw/live-speed/` folder.

### 1.1 Live requests (curl)

* **One wrapper for every live request** (`raw/live-speed/live/live.py`). GET only (no `-d`, no `-X`),
  anonymous (no login, no form, no cookie except in 2 checks that replayed the site's own anonymous
  session cookie), transfers start at least 1.15 s apart (a lock file enforces it across processes, and
  `curl --rate 1/s` inside a process), and a hard cap of 290 transfers. It logs curl's
  `-w '%{json}'` and `%{header_json}` for every transfer (DNS, TCP connect, TLS, TTFB, total, bytes,
  HTTP version, every response header). **224 of the 300 allowed requests were used** (189 by the first run, 35 by the resumed
  re-verification run; the limit given to that run was 40 further requests; the wrapper's counter reads 224 and
  the log holds 223 records, because one transfer of the first run was counted and left no record).
* **"Cold-ish"** = a fresh curl process: new DNS lookup (the macOS resolver cache answered in 2 ms), new
  TCP connection, new TLS handshake. **"Warm"** = one curl process making 5 transfers on one reused
  HTTP/2 connection (the first of the 5 opens the connection, so warm TTFB below is taken from the 4
  reused-connection transfers). Cold-ish rounds were interleaved across the 5 endpoints, so no
  endpoint ran twice in a row.
* Request headers: a desktop Chrome user agent and `Accept-Encoding: br, gzip`, so `size_download` is
  the compressed byte count on the wire. Cloudflare adds `Server-Timing: cfEdge;dur=,cfOrigin;dur=` to
  HTML responses: `cfOrigin` is the time Cloudflare waited for the Railway origin, which is the
  server-side number that does not depend on the client's link.
* **Client networks differ between samples.** The morning samples (13:40-14:15 UTC) came from this
  MacBook on a T-Mobile IPv6 connection (median TCP handshake 33 ms and TLS 58 ms over 44 new
  connections; the 1.59 MB API body took 2.3-2.5 s after the first byte). The evening samples
  (01:51 UTC on) and the re-verification samples (01:23-01:25 UTC on 2026-11-11) came from a home LAN (IPv4), where
  the same body took 0.2-0.4 s. Server-side numbers
  (`cfOrigin`) compare across both; download times do not.
* **The site is shared with other agents' measurements.** Two other analyses queried the same site in the
  same windows (13:40-14:17 UTC and 01:44-01:48 UTC). Their cache-miss builds (for example a 13.4 s
  first search for Irvine) ran on the same single worker, and their requests also warmed or invalidated
  caches I then measured (for example `zip=28105&radius=50` was already warm in the evening). Slow
  samples below may be partly theirs, and warm samples partly theirs. Where it matters it is stated.

### 1.2 Browser metrics: Playwright against a replay of the live responses

The rules (1 request per second, 300 requests in total) rule out real browser page loads against the
live site: one cold page load fires 20 to 50 requests within a second (home 20 same-origin + 5
third-party, listings 36 + 1, car 33 + 4, dealership 47 + 12), and the flows need dozens of loads. So
the browser tests use **real Chromium 148 against a local replay of what the live site sent**:

1. Record every response once from the live site with the curl wrapper: status, headers and the exact
   br/gzip bytes Cloudflare sent (`replay/build_manifest.py`, 68 recorded URLs).
2. Serve the recordings from a local HTTP/2 + TLS server (`replay/server.mjs`, Node `http2`). Chromium is
   pointed at it with `--host-resolver-rules="MAP sarraficars.com 127.0.0.1:PORT"`, and the replay
   certificate is trusted by SPKI pin (`--ignore-certificate-errors-spki-list`; plain
   `--ignore-certificate-errors` would turn off Chrome's HTTP cache).
3. Every response is delayed by its **live server time**: `time_starttransfer - time_pretransfer - RTT`
   of the connection that carried it. "Warm server" (`manifest_warm.json`) uses the median over all samples
   except the first; "cold server" (`manifest_cold.json`) uses the first-ever sample of each URL
   (home 2,901 ms, listings 714 ms, API 4,914 ms). Live body-streaming time is not replayed (the CDP
   bandwidth cap provides transfer time), so it is not counted twice.
4. The network is modelled twice. **Phone**: iPhone 14 viewport 390x844, DPR 3, touch, iOS user agent;
   CDP `Network.emulateNetworkConditions` at 9 Mbps down, 1.5 Mbps up, latency 150 ms (CDP latency is
   only a minimum and never models connection setup, so the replay server also waits one RTT = 150 ms
   before every response and TCP+TLS = 300 ms before the first response on each connection); CPU
   `Emulation.setCPUThrottlingRate` 4x. **Desktop**: 1440x900, DPR 1, no bandwidth or CPU throttling,
   RTT 31 ms and handshake 91 ms (the TCP RTT and TCP+TLS measured to the Cloudflare edge).
5. Third-party hosts (dealer image CDNs, OpenStreetMap tiles, Cloudflare's beacon script) load from the
   real internet, as they would for a shopper. POSTs the page makes
   (`/api/session/listings-geo`, `/cdn-cgi/rum`) are answered by the replay server and never leave the
   machine; the geo POST answers with a new session cookie, as the live code does.
6. Back-forward cache is on (Playwright's `--disable-back-forward-cache` is removed), except in the
   explicit "bfcache off" run.

Metrics come from in-page observers installed before any page script (`longtask`,
`largest-contentful-paint`, `paint`, `layout-shift`, `event`, `long-animation-frame` (LoAF, which names
the script file), `pageshow`), from CDP `Performance.getMetrics` (ScriptDuration, V8CompileDuration,
LayoutDuration, RecalcStyleDuration) and from CDP `Network` events (`encodedDataLength` is bytes on the
wire). **"Key content visible"** is a selector per page (the shopper's ZIP field, the search box, the
gallery box, the first car card). **"Usable"** is the later of "key content visible" and the end of the
last long task, after waiting for 1 s of quiet. The car page's key element is the gallery box, which is
laid out before the photo arrives, so LCP (the photo) is reported next to it.

Page loads ran **5 times** (phone and desktop), flows 5 times on the phone and 3 on the desktop; tables
show medians and the range of "usable". The machine was shared (load average 5-10 on 16 cores) and
every run is sequential, never concurrent.

### 1.3 What the replay does not capture

Contention between concurrent requests inside the single gunicorn worker (the replay serves every
request in parallel), HTTP/3 (Cloudflare advertises `h3`, curl 8.7.1 here has no HTTP/3 and the browser
runs use h2), a phone's first DNS lookup, real 4G loss and jitter, and a real iPhone's CPU (the 4x
slowdown on an Apple M4 Max is closer to a mid-range Android phone than to an A15).

### 1.4 A harness bug that invalidated the first phone-flow numbers (fixed, re-measured)

The first harness (`raw/live-speed/pw/measure.py`) waited up to 120 s for a `load` event that a
back-forward-cache restore never fires (this made one flow run take 151 s and another 736 s), computed
CDP counter deltas across a document change (CDP counters restart on every new document, which produced
negative `ScriptDuration` values), and counted a restored page's earlier long tasks as the Back step's.
`pw2/measure2.py` fixes all three (probe: `pw2/metric_reset_probe.py`, `pw2/debug_back.py`) and every
browser table in this report comes from it.

### 1.5 The re-verification run

The first run was cut off before its report was assembled. The resumed run (section 10) re-measured a sample of
the live numbers (35 requests), re-ran the browser flows and the in-browser what-ifs against the same replay, ran the
two what-ifs that had been planned and not run (edge cache A/B, CSS served by the replay), and recomputed the tables
from the raw files. It changed no method; where it changed a number or a sentence, section 10.6 lists it.

## 2. Live server timings (curl)

Times in ms. TTFB = `time_starttransfer`. "Warm TTFB" uses the 4 reused-connection transfers. `cfOrigin`
is the origin time Cloudflare reports. Per-run values (DNS, TCP, TLS, TTFB, total, bytes, HTTP version)
for all 50 runs are in Appendix A. The morning client was on a T-Mobile link. The table is the first run's
5 cold-ish + 5 warm samples of 2026-11-09; the re-verification run of 2026-11-10 (section 10.2) repeated a
smaller sample and agrees with it.

| endpoint | cold-ish TTFB median [min-max] | cold-ish total | warm TTFB (reused conn) | warm total | cfOrigin median [min-max] | bytes on wire / decoded |
|---|---|---|---|---|---|---|
| home `/` | 298 [281-**3,017**] | 298 [283-3,019] | 170 [156-184] | 170 | 111 [104-**2,880**] | 4,205 br / 15,701 |
| listings `/listings` | 456 [420-831] | 993 [915-1,381] | 424 [293-534] | 709 | 256 [203-604] | 189,535 br / **1,548,917** |
| listings data API `/api/listings/cars?zip=28105&radius=50` | 322 [267-**5,064**] | 2,767 [2,574-5,558] | 143 [123-189] | 638 | (no Server-Timing on JSON) | **1,585,587** gzip / **14,546,046** |
| car `/car/1733731?zip_code=28105` | 525 [503-**3,536**] | 527 [503-3,540] | 361 [340-406] | 366 | 326 [278-**3,382**] | 10,451 br / 55,720 |
| dealership `/dealership/townandcountrytoyota-com` | 380 [364-451] | 414 [393-484] | 246 [234-512] | 278 | 205 [169-468] | 12,676 br / 90,694 |
| `/health` (no work in the app) | - | - | 153 [147-176] | - | - | 77 br |

The page the browser itself calls for the listings data was found by driving the page in Chromium and
logging its requests: after the shopper enters a ZIP, `main.js` fetches
`GET /api/listings/cars?zip=28105&radius=50` (and `/api/zip-coords?zip=` first, and
`/api/listings/filter-options` when the page loads).

What the numbers say:

* **Each uncached request carries a fixed cost of 120-175 ms.** `/health` runs no SQL and takes 0.3 ms in
  the app (structural eval), yet its TTFB on a warm HTTP/2 connection is 147-176 ms (morning, T-Mobile link,
  32 ms RTT to the edge) and 120-133 ms (re-verification run, home LAN, 25 ms RTT). Cloudflare's own
  `cfOrigin` for `/health` is 68-111 ms (median 76 ms, 6 samples; the server-causes report measured 79 ms over
  29 solo requests). That is the Cloudflare to Railway to gunicorn and back path; the repository's ops log lists
  the web service in `us-west2` (section 8), which would make it a cross-country hop. The home page (3.6 ms of
  app time locally) shows the same floor: warm TTFB 149-191 ms.
* **App time on Railway is 4 to 11 times the local test-client numbers.** Taking `/health`'s 76 ms as the floor,
  the median `cfOrigin` of each page, pooled over the morning, evening and re-verification samples (stalls over
  1 s left out and listed), is: home +41 ms (3.6 ms locally), dealership +146 ms (33.5 ms locally, 4.4x),
  listings +238 ms (37.5 ms locally, 6.3x), car page +250 ms (42.2 ms locally, 5.9x):

| endpoint | n | cfOrigin median | p25-p75 | min-max | above the /health floor (median) | stalls > 1 s left out |
|---|---|---|---|---|---|---|
| health | 6 | 76 | 72-89 | 68-111 | - | none |
| home | 28 | 117 | 108-127 | 97-657 | +41 ms | home_cold 2880 |
| listings | 24 | 314 | 224-468 | 199-604 | +238 ms | none |
| car | 20 | 326 | 287-353 | 237-668 | +250 ms | prefetch_cars 1486, car_cold 3382, idle1_car 1852, idle2_car 2185, idle3_car 2222, rv_a_car 2090 |
| dealership | 19 | 222 | 182-304 | 146-468 | +146 ms | none |


  The structural eval found that a car page opens 8-10 Postgres connections with no pool; over Railway's
  private network each one is a TCP handshake plus authentication (20-38 ms per connection live,
  `server-causes.md` S4), which is about the 250 ms. (The first run's text subtracted the home page's lowest
  `cfOrigin`, 104 ms, from the morning's samples and gave listings 152, car 222 and dealership 101 ms; the figures
  above use the `/health` floor and every sample, so they are 28-86 ms higher. Corrected here, section 10.6.)
* **Stalls of several seconds.** The first home request of the session spent 2,880 ms at the origin
  (later runs 104-148 ms) and one car request 3,382 ms (the other 9 runs 278-356 ms). Section 2.2 shows
  the same pattern on the car page after every quiet spell but one, including the first request of the
  re-verification run (2,090 ms against 237 ms on the repeat).
* **Even a cache-hit API response downloads slowly on a cellular link.** On a fresh connection, 1.59 MB
  took 2.3-2.5 s after the first byte (5-5.5 Mbps, TCP slow start); on a warm connection 0.37-0.82 s.
  On the evening home LAN the same body took 0.2-0.4 s in total.
* **HTTP:** HTTP/2 everywhere. `alt-svc: h3=":443"` is advertised. HTML is br-encoded at Cloudflare. The
  API is gzip-encoded by the app and passed through unchanged (a request offering `br` still gets gzip).

### 2.1 The listings data API: cache misses and payload

| request | TTFB | cars | gzip bytes | JSON bytes |
|---|---|---|---|---|
| `zip=28105&radius=50`, first request of the day (LRU miss) | **5,064 ms** | 13,298 | 1,585,587 | 14,546,046 |
| same, then 9 hits (cold-ish and warm) | 123-498 ms | 13,298 | same | same |
| `zip=37421&radius=50` (Chattanooga), first request | **5,530 ms** | 5,633 | 588,689 | 6,115,350 |
| same, second request | 151 ms | | | |
| `zip=28277&radius=50` (Charlotte metro, other ZIP), first request | 1,039 ms | 13,298 | 1,585,539 | |
| `zip=28202&radius=50`, first request from this client | 503 ms | 13,298 | 1,583,932 | 14,545,187 |
| `zip=27601&radius=50` (Raleigh), first request | 329 ms | 419 | 35,784 | |
| `zip=30303&radius=50` (Atlanta), first request | 603 ms, **`x-listings-partial: 1`**, `Cache-Control: public, no-cache` | 3,724 | 389,922 | 4,097,343 |
| same, second request | 786 ms, complete body, new ETag | 3,724 | 389,923 | 4,097,344 |
| `zip=28105&radius=50` with `If-None-Match` of an earlier response | **304, 0 body bytes, 262 ms** | | 0 | |
| `zip=28105&radius=50`, first request after 23 quiet hours (re-verification run, ETag `W/"lc-28105-50-54954f531adfafc9bff0"`) | **1,635 ms** | 13,298 | 1,585,587 | 14,546,046 |
| same, repeat 9 s later | 202 ms | 13,298 | same | same |

* A **cache miss in a region nobody has searched recently costs 1.6 to 5.5 s** of TTFB: 5,064 ms for 28105 and
  5,530 ms for 37421 on the morning of 2026-11-09, 1,635 ms for 28105 again on the re-verification run 23 hours
  later (its ETag differed from the previous day's), 13.4 s for Irvine in `server-causes.md`. The spread is
  not explained by this client; it depends on what else the single worker has warm at that moment. After a miss
  the per-process LRU answers in 123-498 ms (about 150 ms typical). Every deploy or restart empties the LRU, and
  the 00:00 UTC day roll (19:00 Eastern) invalidates it (`redis.md` section 3.1, rows 16-18).
* **A cold area can come back partial.** The Atlanta request returned in 603 ms with `x-listings-partial: 1`
  (stale cards while the rest is rebuilt) and the next one, 786 ms later, was complete with a different ETag.
  `main.js:2037-2062` refetches up to 6 times with backoff when it sees a partial body, so a shopper in a cold
  area can download and re-render the body more than once (`server-causes.md` section 1b: Irvine
  13,447 / 4,439 / 2,887 / 1,287 ms, 2.0 MB each).
* `Cache-Control: public, max-age=0, s-maxage=60, stale-while-revalidate=30` is set, but Cloudflare reports
  `DYNAMIC` on every API response (27 of 27). Without a Cache Rule Cloudflare does not cache
  extension-less JSON, and the response also carries `Vary: Accept-Encoding, Cookie`, so `s-maxage` has no
  effect.
* **Revalidation works end to end.** A conditional GET with the ETag of an earlier response
(`If-None-Match: W/"lc-28105-50-67fb85d758fd3550f20c"`, sent 12 minutes after that body was built) came back
`304 Not Modified` with 0 body bytes and a TTFB of 262 ms, through Cloudflare (the re-verification run repeated it with the
new day's ETag `W/"lc-28105-50-54954f531adfafc9bff0"`: 304, 0 body bytes, 392 ms). The ETag is stable within a UTC
day and changes on every rebuild (`redis.md` section 3.1). The page's own code sends `If-None-Match` only from
an in-memory variable (`main.js:2005-2011`), so a fresh navigation depends on the browser's HTTP cache; the
revisit probe in section 4.4 shows Chrome doing that revalidation (an 85-byte response instead of 1.59 MB).
* Payload: 13,298 cards of up to 42 fields (38 in the first card) = 1,094 bytes of JSON per car. A first page of 24 cards is 24,445 B of
  JSON (3,501 B gzip). Offline arithmetic on the recorded body (`pw2/payload_probe.py`): gzip -9
  1,518,313 B; brotli q5 879,659 B (-45 % against the live gzip, 61 ms); brotli q9 796,888 B; zstd 9
  886,799 B; only 5.7 % of the card JSON is fields no front-end script reads by name, so slimming fields
  gains little and paging gains a lot.

### 2.2 Cold hits after a quiet spell (evening and idle rounds)

The first sample of the day looked like a one-off. Four more passes were taken to see whether it
recurs: one at 01:51 UTC and three "first visitor after a quiet spell" rounds, each started at least
11 minutes after this agent's previous request (`idle_round.sh`; each round also repeats its first
request 3 s later). `unaccounted` is TTFB minus TLS minus Cloudflare's own `cfEdge` and `cfOrigin`
minus the TCP RTT; large values mean the delay is not explained by Cloudflare's timing (a slow client
link or a queue before Cloudflare). Times are cumulative curl values in ms; this client was on a home LAN.

| label | UTC | DNS | TCP | TLS | TTFB | total | cfEdge | cfOrigin | unaccounted (TTFB - TLS - cfEdge - cfOrigin - RTT) | bytes | cf-cache |
|---|---|---|---|---|---|---|---|---|---|---|---|
| idle1_home | 01:51:02 | 2 | 23 | 59 | 257 | 257 | 38 | 123 | 16 | 4,204 | DYNAMIC |
| idle1_listings | 01:51:17 | 2 | 40 | 98 | 1537 | 1615 | 39 | 516 | 846 | 189,384 | DYNAMIC |
| idle1_car | 01:51:34 | 2 | 29 | 70 | 1990 | 2034 | 19 | 1852 | 23 | 10,450 | DYNAMIC |
| idle1_dealer | 01:51:51 | 2 | 38 | 98 | 432 | 433 | 35 | 255 | 9 | 12,680 | DYNAMIC |
| idle1_api | 01:52:07 | 2 | 32 | 71 | 203 | 401 | - | - | - | 1,585,587 | DYNAMIC |
| idle1_health | 01:52:22 | 2 | 34 | 85 | 231 | 232 | - | - | - | 77 | DYNAMIC |
| idle2_home_first | 02:04:54 | 2 | 55 | 114 | 307 | 308 | 28 | 117 | -5 | 4,204 | DYNAMIC |
| idle2_home_repeat | 02:04:57 | 2 | 55 | 99 | 309 | 309 | 35 | 124 | -2 | 4,204 | DYNAMIC |
| idle2_listings | 02:05:13 | 2 | 27 | 83 | 491 | 532 | 49 | 266 | 68 | 189,420 | DYNAMIC |
| idle2_car | 02:05:28 | 2 | 264 | 336 | 2599 | 2684 | 33 | 2185 | -217 | 10,449 | DYNAMIC |
| idle2_dealer | 02:05:46 | 2 | 28 | 78 | 454 | 454 | 34 | 304 | 11 | 12,680 | DYNAMIC |
| idle2_api | 02:06:02 | 2 | 24 | 66 | 332 | 551 | - | - | - | 1,585,587 | DYNAMIC |
| idle3_listings_first | 02:17:02 | 2 | 39 | 106 | 748 | 818 | 38 | 512 | 55 | 189,403 | DYNAMIC |
| idle3_listings_repeat | 02:17:06 | 3 | 54 | 107 | 695 | 819 | 38 | 460 | 39 | 189,400 | DYNAMIC |
| idle3_home | 02:17:22 | 2 | 35 | 90 | 288 | 288 | 38 | 124 | 3 | 4,204 | DYNAMIC |
| idle3_car | 02:17:37 | 2 | 34 | 88 | 2699 | 2742 | 46 | 2222 | 311 | 10,450 | DYNAMIC |
| idle3_dealer | 02:17:55 | 2 | 35 | 95 | 521 | 522 | 32 | 357 | 5 | 12,677 | DYNAMIC |
| idle3_api | 02:18:10 | 2 | 60 | 97 | 376 | 591 | - | - | - | 1,585,587 | DYNAMIC |
| idle4_car_first | 02:29:11 | 2 | 37 | 78 | 484 | 491 | 32 | 337 | 1 | 10,450 | DYNAMIC |
| idle4_car_repeat | 02:29:14 | 2 | 30 | 77 | 654 | 654 | 31 | 514 | 3 | 10,453 | DYNAMIC |
| idle4_home | 02:29:30 | 2 | 46 | 85 | 296 | 296 | 50 | 121 | -4 | 4,205 | DYNAMIC |
| idle4_listings | 02:29:46 | 2 | 35 | 78 | 705 | 760 | 32 | 526 | 37 | 189,410 | DYNAMIC |
| idle4_dealer | 02:30:01 | 3 | 273 | 341 | 962 | 1143 | 77 | 391 | -117 | 12,740 | DYNAMIC |
| idle4_api | 02:30:18 | 2 | 24 | 72 | 202 | 375 | - | - | - | 1,585,587 | DYNAMIC |


Reading the table (cfOrigin is the origin time in ms; the morning warm medians were home 111, listings 256, car
326, dealership 205):

* **The home page was never cold** (cfOrigin 117-124 ms in all 5 samples, including as the very first
  request of round 2). Whatever expired for it in the morning did not expire here.
* **The car page was cold in 3 of these 4 rounds: 1,852, 2,185 and 2,222 ms of origin time** (TTFB 1,990, 2,599
  and 2,699 ms), against 337 ms in the fourth round and about 330 ms in the morning's warm runs. It was cold
  even though the home, listings and dealership pages had just been loaded, so the car page has its own
  caches that other pages do not warm. The fourth round found it warm; the origin is shared with other
  visitors and agents, so whether a visitor meets the stall depends on whether anyone requested a car page
  in the last few minutes. With the morning's two stalls (home 2,880 ms, car 3,382 ms) that is five first hits of
  1.85-3.38 s of origin time (one home, four car) in the first 13 hours of sampling; the fifth pass below adds
  a sixth (car, 2,090 ms).
* **Listings and dealership pages ran slower in the evening than in the morning** (listings 460-526 ms in
  4 of 5 samples against a morning median of 256 ms; dealership 255-391 ms against 205 ms) without any one
  stall; the repeat 3-4 s after the first request was not faster (listings 460 against 512 ms, car 514
  against 337 ms), so this is not a first-hit cache fill. Other agents were using the same worker in these
  windows; the cause was not isolated.
* **Cloudflare's own split explains the TTFB for 17 of 19 requests** (`unaccounted` within 70 ms, or
  negative because a slow TCP handshake inflated the RTT that was subtracted). The two exceptions are the 846 ms
  of `idle1_listings` and the 311 ms of `idle3_car` (section 8).

**A fifth pass, 23 hours later (the re-verification run, 01:23 UTC on 2026-11-11).** The first request of each
endpoint on a fresh connection, then a repeat 9-11 s later (the table is `summarize_idle.py rv_a rv_b`; the
home LAN again):

| label | UTC | DNS | TCP | TLS | TTFB | total | cfEdge | cfOrigin | unaccounted (TTFB - TLS - cfEdge - cfOrigin - RTT) | bytes | cf-cache |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rv_a_health | 01:23:42 | 2 | 39 | 73 | 213 | 214 | - | - | - | 77 | DYNAMIC |
| rv_a_car | 01:23:44 | 2 | 38 | 80 | 2253 | 2253 | 29 | 2090 | 18 | 10,452 | DYNAMIC |
| rv_a_dealer | 01:23:47 | 2 | 38 | 86 | 515 | 515 | 34 | 365 | -6 | 12,679 | DYNAMIC |
| rv_a_home | 01:23:49 | 2 | 37 | 96 | 260 | 260 | 20 | 97 | 12 | 4,204 | DYNAMIC |
| rv_a_listings | 01:23:50 | 2 | 27 | 70 | 409 | 445 | 55 | 199 | 59 | 189,500 | DYNAMIC |
| rv_a_api | 01:23:52 | 2 | 38 | 69 | 1635 | 1766 | - | - | - | 1,585,587 | DYNAMIC |
| rv_b_car | 01:23:55 | 2 | 26 | 74 | 417 | 420 | 61 | 237 | 22 | 10,450 | DYNAMIC |
| rv_b_dealer | 01:23:56 | 2 | 33 | 70 | 298 | 303 | 40 | 150 | 7 | 12,679 | DYNAMIC |
| rv_b_home | 01:23:58 | 2 | 22 | 59 | 210 | 220 | 17 | 106 | 9 | 4,204 | DYNAMIC |
| rv_b_listings | 01:23:59 | 2 | 40 | 118 | 457 | 515 | 35 | 235 | 30 | 189,497 | DYNAMIC |
| rv_b_api | 01:24:01 | 2 | 66 | 97 | 202 | 334 | - | - | - | 1,585,587 | DYNAMIC |
| rv_b_health | 01:24:02 | 2 | 31 | 64 | 219 | 219 | - | - | - | 77 | DYNAMIC |


* **The car page stalled again:** 2,090 ms at the origin on the first request, 237 ms on the repeat (Cloudflare's
  `cfEdge` 29 ms and `cfOrigin` 2,090 ms account for the 2,253 ms TTFB within 18 ms, so the stall is at the origin).
  Counting every sample of this report that carries a `cfOrigin` value, **6 of 26 car-page requests spent 1.5-3.4 s
  at the origin** (3,382, 2,222, 2,185, 2,090, 1,852 and 1,486 ms, the last on one of the 8 prefetched car
  pages) and the other 20 spent 237-668 ms (median 326 ms). The structural evaluation saw a similar first-hit
  cost on a freshly started local process: 1.9-2.2 s for a car page (and 2.6-2.8 s for `/listings`, which did not
  stall live; `runtime-timing.md`, warm-up column).
* **The home, listings and dealership pages did not stall** (origin time 97, 199 and 365 ms on the first request;
  106, 235 and 150 ms on the repeat). The home page has stalled once in 29 samples.
* **The first API request took 1,635 ms** and its repeat 202 ms (section 2.1).

### 2.3 Static assets: not the problem

All CSS and JS are served **br**, `public, max-age=31536000, immutable`, with an ETag and
`Vary: Accept-Encoding`, and were **cf HIT** on the first request. Warm-connection TTFB is 37-98 ms (that
is the RTT to the edge). What they cost is their number and size on a first visit, and what the CSS costs
in style recalculation (sections 3 and 5).

| page | stylesheets (all render-blocking) | JS files (all `defer`) | font |
|---|---|---|---|
| home | 15 files, 51,644 B br / 285,157 B | 1 file, 2,991 B br / 10,621 B | 48,432 B woff2 (preloaded) |
| listings | 15 files, 51,644 B br / 285,157 B | 16 files, 54,725 B br / 243,598 B (+ `compare.js` preloaded) | same |
| car | 15 files, 51,644 B br / 285,157 B | 12 files, 40,257 B br / 177,721 B | same |
| dealership | 17 files, 57,240 B br / 312,135 B | 18 files, 90,665 B br / 383,687 B (Leaflet 36,938 B br) | same |

Two representative files, the largest script and the largest stylesheet of the car page, were repeated 5 times
cold-ish and 5 times warm (the first transfer of a warm batch opens the connection, so 4 reused-connection
transfers are shown) from the evening home LAN, where the TCP handshake to the edge was 26-45 ms:

| file | run kind | n | DNS | TCP | TLS | TTFB median [min-max] | total median [min-max] | wire bytes | decoded | Content-Encoding | cf-cache-status | Cache-Control | ETag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `/static/main.js` | cold-ish (fresh connection) | 5 | 2 | 42 | 44 | 152 [127-893] | 152 [128-902] | 21,956 | 119,771 | br | HIT | public, max-age=31536000, immutable | yes |
| `/static/main.js` | warm (reused connection, runs 2-5) | 4 | 0 | 0 | 0 | 125 [29-278] | 126 [32-289] | 21,956 | 119,771 | br | HIT | public, max-age=31536000, immutable | yes |
| `/static/css/03-car-page.css` | cold-ish (fresh connection) | 5 | 2 | 26 | 37 | 111 [103-169] | 112 [104-169] | 8,993 | 53,548 | br | HIT | public, max-age=31536000, immutable | yes |
| `/static/css/03-car-page.css` | warm (reused connection, runs 2-5) | 4 | 0 | 0 | 0 | 48 [40-624] | 48 [40-725] | 8,993 | 53,548 | br | HIT | public, max-age=31536000, immutable | yes |


Both files were `cf HIT` on all 20 transfers. The slow outliers (893 ms and 624 ms) are not explained by the
origin, which a HIT does not reach.

Exceptions to the pattern (full per-file table in Appendix B):

* `/static/fonts/inter-latin-v20.woff2` (48 KB), `/static/favicon.svg`, `/static/brand/favicon.ico` and
  `apple-touch-icon.png` were `MISS` the first time and `HIT` on a later check.
* `/static/brand/site.webmanifest` is `DYNAMIC` (204 B; not worth fixing).
* `/api/listings/filter-options` (86,107 B gzip / **574,227 B**) has **no Cache-Control** and no ETag.
  Every visit to the listings page fetches it again and grafts it into the page (section 3.3).
* `/api/fuel/lookup?fuel_tier=regular&state=NC` has no Cache-Control and is **requested twice in a row**
  by every car page (replay server log: two GETs about 300 ms apart).

### 2.4 Response headers that decide what can be cached

| page | Cache-Control | Set-Cookie | Vary | cf-cache-status |
|---|---|---|---|---|
| `/` , `/listings`, `/dealership/<key>` | **none** | `session=...; Secure; HttpOnly; SameSite=Lax` on the first response | `Accept-Encoding, Cookie` | DYNAMIC |
| `/car/<id>` | `private, max-age=180` | same | same | DYNAMIC |
| `/api/listings/cars` | `public, max-age=0, s-maxage=60, stale-while-revalidate=30` | none | `Accept-Encoding, Cookie` | DYNAMIC |
| `/api/listings/filter-options` | none (no ETag either) | none | `Accept-Encoding` | DYNAMIC |
| `/static/*` (CSS, JS, font) | `public, max-age=31536000, immutable` | none | `Accept-Encoding` | HIT |

Three consequences used later: (1) an HTML document with no `Cache-Control` and no validators can never be
reused by the browser, so a prefetch of it is a second download; (2) a car page can be reused for 180 s,
so a prefetch of it works; (3) every response that sets a session cookie, varies on `Cookie` and carries a
per-request CSP nonce is uncacheable at the edge.

## 3. Page loads: iPhone 14 on throttled 4G, and desktop

Five loads per cell, medians, against the **warm-server** replay (every response waits its median live
server time), so these are the best case for the origin. First-hit stalls (section 2.2) come on top.
"cold" = new browser context (empty HTTP cache); "warm-cache" = reload in the same context, so the
static files are cached (HTML is not: it has no Cache-Control). Columns: TTFB, FCP and LCP are
`PerformanceNavigationTiming`/observer values in ms from navigation start; "usable" is defined in
section 1.2; "long tasks" are main-thread tasks over 50 ms (count / total ms / longest ms); TBT is total
blocking time after FCP; "Script (V8 compile)" is CDP `ScriptDuration` (`V8CompileDuration`) in ms;
"style+layout" is CDP `RecalcStyleDuration + LayoutDuration` in ms; bytes are on the wire.

### 3.1 Phone (390x844, DPR 3, 9 Mbps / 1.5 Mbps / 150 ms RTT, CPU 4x)


*phone page loads: median of 5 runs (range of usable); browser 148.0.7778.96, load average at start [5.41552734375, 5.10888671875, 6.14892578125]*

| load | TTFB | FCP | LCP (element) | DCL | load event | key content visible | usable | long tasks (n / total / max) | TBT | Script (V8 compile) | style+layout | bytes total (same-origin / 3rd-party) | requests (same / 3rd) | DOM elements |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (n=5) | 608 | 988 | 996 (P.lps-q) | 888 | 1508 | 694 | **932** (928-947) | 1 / 52 / 52 | 0 | 16 (0) | 134 | 735,885 (117,226 / 618,659) | 20 / 5 | 171 |
| home:warm-cache (n=5) | 308 | 444 | 1312 (P.lps-q) | 359 | 531 | 436 | **436** (419-440) | 1 / 52 / 52 | 0 | 14 (0) | 68 | 4,845 (4,845 / 0) | 20 / 5 | 171 |
| listings:cold (n=5) | 774 | 1160 | 1480 (P.listings-zip-banner__text) | 1443 | 1453 | 1097 | **1456** (1431-1496) | 1 / 154 / 147 | 97 | 142 (0) | 137 | 458,559 (448,081 / 10,478) | 36 / 1 | 46,803 |
| listings:warm-cache (n=5) | 474 | 680 | 904 (P.listings-zip-banner__text) | 866 | 876 | 895 | **895** (879-920) | 1 / 148 / 148 | 98 | 144 (1) | 67 | 276,440 (276,440 / 0) | 36 / 1 | 46,803 |
| listings_zip:cold (n=5) | 861 | 1248 | 4544 (IMG.result-image) | 1526 | 1540 | 4127 | **4183** (4041-4520) | 5 / 622 / 226 | 372 | 402 (1) | 232 | 3,397,578 (2,057,583 / 1,339,995) | 40 / 6 | 47,364 |
| listings_zip:warm-cache (n=5) | 560 | 768 | 2124 (IMG.result-image) | 866 | 878 | 2151 | **2270** (2006-2429) | 5 / 638 / 233 | 388 | 405 (1) | 123 | 276,730 (276,730 / 0) | 40 / 6 | 47,364 |
| car:cold (n=5) | 828 | 1268 | 1268 (IMG#car-gallery-main-img.car-gallery-main-img) | 1211 | 1341 | 908 | **1211** (1200-1252) | 1 / 55 / 55 | 0 | 65 (1) | 230 | 201,283 (166,847 / 34,436) | 33 / 4 | 490 |
| car:warm-cache (n=5) | 529 | 632 | 632 (IMG#car-gallery-main-img.car-gallery-main-img) | 685 | 696 | 720 | **720** (704-741) | 0 / 0 / 0 | 0 | 64 (0) | 179 | 11,131 (11,131 / 0) | 32 / 4 | 490 |
| dealership:cold (n=5) | 689 | 1064 | 1796 (IMG.result-image) | 1256 | 2165 | 1540 | **1540** (1519-2524) | 3 / 163 / 61 | 4 | 113 (0) | 134 | 751,151 (249,677 / 501,474) | 47 / 12 | 1,688 |
| dealership:warm-cache (n=5) | 389 | 508 | 688 (IMG.result-image) | 565 | 672 | 709 | **709** (688-745) | 1 / 57 / 55 | 7 | 114 (0) | 102 | 14,161 (13,405 / 756) | 47 / 18 | 1,688 |

*phone pages: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| car:cold | `static/ds_comments.js / classic-script` | 55 |
| car:cold | `static/car_page.js / classic-script` | 26 |
| car:warm-cache | `static/car_page.js / classic-script` | 28 |
| dealership:cold | `static/main.js / user-callback` | 32 |
| dealership:cold | `dealership/townandcountrytoyota-com / event-listener` | 26 |
| dealership:warm-cache | `static/main.js / user-callback` | 30 |
| dealership:warm-cache | `dealership/townandcountrytoyota-com / event-listener` | 23 |
| listings:cold | `static/main.js / resolve-promise` | 347 |
| listings:cold | `static/main.js / event-listener` | 104 |
| listings:cold | `static/listings_boot.js / event-listener` | 21 |
| listings:warm-cache | `static/main.js / resolve-promise` | 355 |
| listings:warm-cache | `static/main.js / event-listener` | 104 |
| listings:warm-cache | `static/listings_boot.js / event-listener` | 23 |
| listings_zip:cold | `static/main.js / resolve-promise` | 488 |
| listings_zip:cold | `static/main.js / user-callback` | 334 |
| listings_zip:cold | `static/listings_boot.js / event-listener` | 23 |
| listings_zip:cold | `static/main.js / event-listener` | 20 |
| listings_zip:warm-cache | `static/main.js / resolve-promise` | 511 |
| listings_zip:warm-cache | `static/main.js / user-callback` | 342 |
| listings_zip:warm-cache | `static/listings_boot.js / event-listener` | 22 |
| listings_zip:warm-cache | `static/main.js / event-listener` | 20 |

*third-party bytes by host (first run that loaded them)*

| load | host: requests / bytes |
|---|---|
| home:cold | www.cleveland-nissan.com: 3 / 579,764; images.dealer.com: 1 / 28,425; static.cloudflareinsights.com: 1 / 10,478 |
| home:warm-cache | static.cloudflareinsights.com: 1 / 0; images.dealer.com: 1 / 0; www.cleveland-nissan.com: 3 / 0 |
| listings:cold | static.cloudflareinsights.com: 1 / 10,478 |
| listings:warm-cache | static.cloudflareinsights.com: 1 / 0 |
| listings_zip:cold | www.randymariongmc.net: 4 / 1,310,948; images.dealer.com: 1 / 18,789; static.cloudflareinsights.com: 1 / 10,478 |
| listings_zip:warm-cache | static.cloudflareinsights.com: 1 / 0; images.dealer.com: 1 / 0; www.randymariongmc.net: 4 / 0 |
| car:cold | images.dealer.com: 3 / 23,959; static.cloudflareinsights.com: 1 / 10,478 |
| car:warm-cache | images.dealer.com: 3 / 0; static.cloudflareinsights.com: 1 / 0 |
| dealership:cold | images.dealer.com: 5 / 296,650; tile.openstreetmap.org: 6 / 194,386; static.cloudflareinsights.com: 1 / 10,478 |
| dealership:warm-cache | tile.openstreetmap.org: 12 / 798; static.cloudflareinsights.com: 1 / 0; images.dealer.com: 5 / 0 |


### 3.2 Desktop (1440x900, 31 ms RTT, no throttling)


*desktop page loads: median of 5 runs (range of usable); browser 148.0.7778.96, load average at start [6.75390625, 5.8564453125, 6.28125]*

| load | TTFB | FCP | LCP (element) | DCL | load event | key content visible | usable | long tasks (n / total / max) | TBT | Script (V8 compile) | style+layout | bytes total (same-origin / 3rd-party) | requests (same / 3rd) | DOM elements |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (n=5) | 279 | 496 | 496 (P.lps-q) | 439 | 623 | 304 | **439** (413-488) | 0 / 0 / 0 | 0 | 3 (0) | 35 | 735,845 (117,216 / 618,651) | 20 / 5 | 171 |
| home:warm-cache (n=5) | 188 | 232 | 1104 (P.lps-q) | 201 | 238 | 218 | **218** (216-221) | 0 / 0 / 0 | 0 | 3 (0) | 18 | 4,822 (4,822 / 0) | 20 / 5 | 171 |
| listings:cold (n=5) | 446 | 680 | 704 (P.listings-zip-banner__text) | 684 | 685 | 491 | **684** (668-704) | 0 / 0 / 0 | 0 | 34 (0) | 41 | 458,725 (448,247 / 10,478) | 36 / 1 | 46,803 |
| listings:warm-cache (n=5) | 355 | 416 | 452 (P.listings-zip-banner__text) | 426 | 428 | 437 | **437** (434-439) | 0 / 0 / 0 | 0 | 34 (0) | 18 | 276,431 (276,431 / 0) | 36 / 1 | 46,803 |
| listings_zip:cold (n=5) | 533 | 760 | 1380 (IMG.result-image) | 716 | 734 | 1355 | **1355** (1346-1928) | 0 / 0 / 0 | 0 | 82 (0) | 38 | 4,820,952 (2,103,953 / 2,716,999) | 44 / 11 | 47,364 |
| listings_zip:warm-cache (n=5) | 441 | 496 | 876 (IMG.result-image) | 490 | 493 | 888 | **888** (870-892) | 0 / 0 / 0 | 0 | 80 (0) | 30 | 345,305 (345,305 / 0) | 44 / 11 | 47,364 |
| car:cold (n=5) | 500 | 716 | 732 (IMG#car-gallery-main-img.car-gallery-main-img) | 669 | 782 | 520 | **669** (635-704) | 0 / 0 / 0 | 0 | 18 (0) | 60 | 200,864 (166,876 / 33,989) | 33 / 4 | 490 |
| car:warm-cache (n=5) | 410 | 468 | 468 (IMG#car-gallery-main-img.car-gallery-main-img) | 440 | 444 | 454 | **454** (454-456) | 0 / 0 / 0 | 0 | 16 (0) | 43 | 11,131 (11,131 / 0) | 32 / 4 | 490 |
| dealership:cold (n=5) | 360 | 588 | 1112 (IMG.result-image) | 544 | 852 | 579 | **579** (578-584) | 0 / 0 / 0 | 0 | 27 (0) | 36 | 1,791,321 (248,810 / 1,542,522) | 51 / 19 | 1,688 |
| dealership:warm-cache (n=5) | 269 | 320 | 352 (IMG.result-image) | 309 | 332 | 347 | **347** (344-355) | 0 / 0 / 0 | 0 | 26 (0) | 23 | 14,402 (13,542 / 860) | 51 / 25 | 1,688 |

*desktop pages: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| listings:cold | `static/main.js / resolve-promise` | 77 |
| listings:cold | `static/main.js / event-listener` | 37 |
| listings:warm-cache | `static/main.js / resolve-promise` | 76 |
| listings_zip:cold | `static/main.js / resolve-promise` | 111 |
| listings_zip:cold | `static/main.js / user-callback` | 40 |
| listings_zip:warm-cache | `static/main.js / resolve-promise` | 110 |
| listings_zip:warm-cache | `static/main.js / user-callback` | 38 |

*third-party bytes by host (first run that loaded them)*

| load | host: requests / bytes |
|---|---|
| home:cold | www.cleveland-nissan.com: 3 / 579,770; images.dealer.com: 1 / 28,425; static.cloudflareinsights.com: 1 / 10,478 |
| home:warm-cache | static.cloudflareinsights.com: 1 / 0; images.dealer.com: 1 / 0; www.cleveland-nissan.com: 3 / 0 |
| listings:cold | static.cloudflareinsights.com: 1 / 10,478 |
| listings:warm-cache | static.cloudflareinsights.com: 1 / 0 |
| listings_zip:cold | www.randymariongmc.net: 9 / 2,688,106; images.dealer.com: 1 / 18,789; static.cloudflareinsights.com: 1 / 10,478 |
| listings_zip:warm-cache | static.cloudflareinsights.com: 1 / 0; images.dealer.com: 1 / 0; www.randymariongmc.net: 9 / 0 |
| car:cold | images.dealer.com: 3 / 23,529; static.cloudflareinsights.com: 1 / 10,478 |
| car:warm-cache | images.dealer.com: 3 / 0; static.cloudflareinsights.com: 1 / 0 |
| dealership:cold | images.dealer.com: 10 / 1,064,953; pictures.web.dealer.com: 2 / 272,740; tile.openstreetmap.org: 6 / 194,351; static.cloudflareinsights.com: 1 / 10,478 |
| dealership:warm-cache | tile.openstreetmap.org: 12 / 918; static.cloudflareinsights.com: 1 / 0; images.dealer.com: 10 / 0; pictures.web.dealer.com: 2 / 0 |


### 3.3 Reading the tables

* **Every phone page is 0.9 to 1.5 s from tap to usable even with a warm origin; the desktop is 0.4 to
  0.7 s.** Phone cold: home **932 ms**, listings **1,456 ms**, car **1,211 ms**, dealership **1,540 ms**.
  Desktop cold: home 439 ms, listings 684 ms, car 669 ms, dealership 579 ms. A repeat visit with the static
  files cached is about 500-830 ms faster on the phone (home 436, listings 895, car 720, dealership 709 ms)
  because it skips the TCP+TLS handshake (300 ms) and the CSS/JS/font downloads.
* **A phone page waits 0.6 to 0.9 s for its first byte.** Cold TTFB: home 608 ms, dealership 689 ms,
  listings 774 ms, car 828 ms, listings with a ZIP 861 ms. The model for home is 300 ms TCP+TLS + 150 ms RTT + 155 ms live server
  wait; the rest of the page then needs another 380 ms to FCP (988 ms): one more RTT for the 15
  render-blocking stylesheets (51.6 KB br), plus 134 ms of style and layout at 4x CPU for a page with 171
  elements. LCP on the home page is a typing-demo paragraph, so its "warm-cache" LCP (1,312 ms) follows
  the animation, not the load.
* **The listings page is the heavy one.** It is 1,548,917 B of HTML (189,535 B br) of which 1,038,459 B is
  one JSON `<script>` island (`ds-listings-car-rows`, the fleet-wide facet table), it then fetches
  `/api/listings/filter-options` (86,107 B gzip, 574,227 B JSON), and the page ends up with **46,803 DOM
  elements** (`document.getElementsByTagName('*')`; CDP counts 69,176 nodes including text) for a page
  that shows one ZIP field. On the phone: one 147-154 ms long task, TBT 97 ms, Script 142 ms. LoAF
  attributes it to `main.js` promise resolution (347 ms of script, the model/trim/package option lists
  being built after the filter-options fetch) and `main.js` event listeners (104 ms).
* **A listings page with a remembered ZIP is the slowest page of all: 4,183 ms on the phone** (warm-cache
  2,270 ms; desktop 1,355 ms). It downloads 3.4 MB (2.06 MB same-origin: the 1.59 MB API body, the 190 KB
  document, the 86 KB facet list, assets; 1.34 MB of dealer photos), runs 5 long tasks (622 ms in total,
  longest 226 ms, TBT 372 ms, Script 402 ms), and its LCP is the first card's photo at 4,544 ms. This is
  what a returning shopper sees, because the ZIP is remembered and the page searches by itself.
* **Car page: 1,211 ms; the main thread is style and layout.** Script is 65 ms and there is one 55 ms
  long task, but style+layout is 230 ms at 4x for 490 elements (the Chrome trace, section 5, shows 286 ms of
  style+layout of 603 ms main-thread time). The LCP element is the gallery photo (1,268 ms).
* **Photos dominate the bytes of some pages** (`pw2/thirdparty_probe.py`, with the host, type and size of every
  third-party response). Home: 618,521 of 735,885 B (84 %) are third-party, and 551,131 B of that is **one
  1280-pixel stock photo** (`image/webp`, `www.cleveland-nissan.com`) shown in a featured-car slot, plus a
  28 KB placeholder and a 28 KB stock photo; none of it is needed to paint the page, but it holds `load` at
  1,508 ms. Listings with a ZIP: **two 1280-pixel stock images of 664,901 B and 533,785 B** and two inventory
  photos of 56 KB (all `www.randymariongmc.net`): 1.34 MB of third-party bytes for the first screen of
  results, to fill a card 362 CSS pixels wide, competing for the same 9 Mbps as the 1.59 MB API body.
  Dealership page: 12 responses, 501,402 B (one 215 KB photo, 6 map tiles of 29-37 KB each).
* **The desktop numbers are bound by server wait and round trips, not by CPU:** Script 3-82 ms and no long
  tasks on any page, so the 0.4-0.7 s is TTFB (279-533 ms) plus a few round trips.

Third-party responses per cold phone load (one run of `pw2/thirdparty_probe.py`; zero-byte data: URIs omitted):

| page (phone, cold) | third-party responses | bytes | largest responses (bytes, type, host, path) |
|---|---|---|---|
| home | 4 | 618,521 | 551,131 B webp www.cleveland-nissan.com/assets/stock/colormatched_01/transparent/12<br>28,487 B webp www.cleveland-nissan.com/resources/components/missing/photo_unavaila<br>28,425 B jpeg images.dealer.com/autodata/us/large_stockphoto/2016/USC60GMS0<br>10,478 B javascript static.cloudflareinsights.com/beacon.min.js/vce701823e78c4301a35b7e07294d |
| listings_zip | 6 | 1,340,319 | 664,901 B webp www.randymariongmc.net/assets/stock/colormatched_01/transparent/12<br>533,785 B webp www.randymariongmc.net/assets/stock/colormatched_01/transparent/12<br>56,472 B webp www.randymariongmc.net/inventoryphotos/15017/1c4rjfjg0jc418660/ip/<br>55,894 B webp www.randymariongmc.net/inventoryphotos/15017/1c4rjjag8m8203697/ip/ |
| car | 4 | 33,988 | 18,789 B jpeg images.dealer.com/autodata/us/large_stockphoto-color/2012/USC<br>10,478 B javascript static.cloudflareinsights.com/beacon.min.js/vce701823e78c4301a35b7e07294d<br>4,452 B jpeg images.dealer.com/autodata/us/thumb_stockphoto-color/2012/USC<br>269 B gif images.dealer.com/autodata/us/large_stockphoto-color/2012/USC |
| dealership | 12 | 501,402 | 215,360 B jpeg images.dealer.com/autodata/us/color/2019/USC90HYC021C0/Y8.jpg<br>37,040 B png tile.openstreetmap.org/14/4511/6482.png<br>33,624 B png tile.openstreetmap.org/14/4511/6484.png<br>32,917 B png tile.openstreetmap.org/14/4511/6483.png |


## 4. Switching pages: the time from a tap to a usable page

The flow is: open home, tap the "Browse inventory" button (listings), type ZIP 28105 and search, tap the first
car, press Back, tap the first car again. Every step is measured from the tap (`click->`), in one browser
context, so the HTTP cache, the bfcache and the prefetches behave as they would for a shopper. "bfcache"
means Chrome's back-forward cache. The origin is the warm-server replay.

### 4.1 Phone (5 runs)


*phone page switches: median of 5 runs (range of usable); browser 148.0.7778.96, load average at start [7.8603515625, 6.30517578125, 6.38623046875]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=5) | 607 | 988 | 996 | 698 | **942** (883-1061) | 1 / 51 / 51 | 15 | 117,226 / 618,657 | 20 (0) | - | - |
| home->listings (n=5) | 1,005 | 1,190 | 1,582 | 1247 | **1561** (1556-1565) | 1 / 137 / 137 | 136 | 525,400 / 0 | 38 (19) | 0 | - |
| listings:search (n=5) | - | - | - | 2645 | **2743** (2645-2912) | 5 / 728 / 203 | 497 | 1,609,511 / 1,329,633 | 8 (1) | 2 | - |
| results->car (n=5) | 130 | 266 | 266 | 253 | **253** (238-291) | 0 / 0 / 0 | 117 | 36,349 / 5,169 | 34 (24) | 0 | - |
| car->back (n=5) | - | - | - | 148 | **148** (132-172) | 0 / 0 / 0 | 0 | 0 / 0 | 1 (1) | 0 | restored from bfcache |
| results->car(2nd) (n=5) | 100 | 255 | 255 | 253 | **253** (233-302) | 0 / 0 / 0 | 105 | 177 / 0 | 33 (34) | 0 | - |

*phone flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| home->listings | `static/main.js / resolve-promise` | 366 |
| home->listings | `static/main.js / event-listener` | 100 |
| listings:search | `static/main.js / user-callback` | 483 |
| listings:search | `static/main.js / resolve-promise` | 127 |
| results->car(2nd) | `static/ds_comments.js / classic-script` | 22 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 170 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 378 | 689 | 2192 | 1,586,912 | 200 |
| `/api/session/listings-geo` | POST | 879 | 1047 | 1053 | 91 | 200 |
| `/car/1733731?zip_code=28105` | GET | 2551 | 3084 | 3122 | 10,859 | 200 |
| `/car/1708513?zip_code=28105` | GET | 2551 | 3068 | 3108 | 11,540 | 200 |


### 4.2 Desktop (3 runs)


*desktop page switches: median of 3 runs (range of usable); browser 148.0.7778.96, load average at start [7.96923828125, 8.6298828125, 8.16552734375]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=3) | 280 | 492 | 500 | 308 | **431** (426-478) | 0 / 0 / 0 | 3 | 117,216 / 618,670 | 20 (0) | - | - |
| home->listings (n=3) | 743 | 809 | 917 | 787 | **787** (775-787) | 0 / 0 / 0 | 34 | 525,400 / 0 | 38 (19) | 0 | - |
| listings:search (n=3) | - | - | - | 556 | **556** (476-564) | 0 / 0 / 0 | 114 | 1,655,682 / 2,706,520 | 12 (1) | 6 | - |
| results->car (n=3) | 461 | 512 | 512 | 495 | **495** (490-503) | 0 / 0 / 0 | 11 | 47,310 / 4,721 | 34 (23) | 0 | - |
| car->back (n=3) | - | - | - | 63 | **63** (57-69) | 0 / 0 / 0 | 0 | 0 / 0 | 1 (1) | 0 | restored from bfcache |
| results->car(2nd) (n=3) | 55 | 117 | 117 | 94 | **94** (89-98) | 0 / 0 / 0 | 42 | 177 / 0 | 33 (34) | 0 | - |
| car->home(logo) (n=3) | 410 | 448 | 1,318 | 429 | **429** (428-434) | 0 / 0 / 0 | 1 | 9,032 / 0 | 22 (22) | 0 | - |

*desktop flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| home->listings | `static/main.js / resolve-promise` | 78 |
| listings:search | `static/main.js / user-callback` | 82 |
| listings:search | `static/main.js / resolve-promise` | 31 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 68 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 136 | 304 | 340 | 1,586,912 | 200 |
| `/car/1733731?zip_code=28105` | GET | 445 | 837 | 838 | 10,859 | 200 |
| `/car/1708513?zip_code=28105` | GET | 445 | 823 | 824 | 11,517 | 200 |
| `/car/1708520?zip_code=28105` | GET | 445 | 1008 | 1009 | 11,746 | 200 |
| `/car/1708534?zip_code=28105` | GET | 824 | 1186 | 1187 | 11,536 | 200 |
| `/car/1708861?zip_code=28105` | GET | 838 | 2354 | 2355 | 11,420 | 200 |
| `/api/session/listings-geo` | POST | 927 | 967 | 968 | 113 | 200 |
| `/car/1708945?zip_code=28105` | GET | 1009 | 1709 | 1709 | 11,492 | 200 |


### 4.3 Reading the flows

* **Opening listings from home takes 1,561 ms on the phone**, against 1,456 ms for a direct load of the
  same page. The first byte arrives 1,005 ms after the tap, which is slower than the 774 ms of a cold
  direct load that includes the TCP+TLS handshake. Section 4.4 explains why.
* **A search takes 2,743 ms on the phone** (first cards at 2,645 ms; 5 long tasks, 728 ms in total, longest
  203 ms) and moves 1.61 MB from the site plus 1.33 MB of dealer photos. Section 5.1 is the millisecond
  breakdown.
* **Opening a car from the results is fast, 253 ms**, because the page prefetches the car pages of the visible
  cards as soon as results render (2 on the phone) and car pages are cacheable for 180 s. Opening the
  same car again after Back is 253 ms. Cold, a car page needs 1,211 ms on the phone (section 3.1). On the
  desktop the same tap takes 495 ms, for the cookie reason in section 4.4.
* **Back is 148 ms because the results page is restored from the bfcache**: no request, no long task.

### 4.4 The prefetch script: good for cars, bad for the other pages

`frontend/static/nav_perf.js` warms the destination of a link as soon as a touch starts (`touchstart`) or the
pointer rests on it for 65 ms (`mouseover`), and warms the first 8 visible car links when a results grid
appears. It sends the request as a low-priority `fetch()` and expects the browser to reuse the answer. That
works only for responses the browser may cache. Car pages send `Cache-Control: private, max-age=180`.
Home, listings and dealership pages send no `Cache-Control` and no validator, so the prefetched copy can
never be reused (section 2.4) and the navigation sends the same request again.

Evidence from the replay server's own log (`replay/log_phone.jsonl`, during the 5 phone-flow runs, time
window 20:55:05-20:58:18): **10 `GET /listings` for 5 runs**, each 189,499 B, and the two requests of each
run start 474, 474, 475, 476 and 474 ms apart, which is exactly the 472.7 ms server wait of the first one.
The second request waits for the first to finish and is then sent again. The step's bytes agree:
525,400 B same-origin for a page whose HTML is 189,499 B and whose other files are about 150 KB.

To size the effect, the same flow was run with `nav_perf.js` replaced by an empty script
(`replay/manifest_nonav.json`, same replay, HTTP cache enabled):


*phone page switches: median of 5 runs (range of usable); browser 148.0.7778.96, load average at start [8.65576171875, 8.53271484375, 8.1611328125]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=5) | 608 | 992 | 1000 | 696 | **942** (883-954) | 1 / 52 / 52 | 14 | 114,293 / 618,658 | 20 (0) | - | - |
| home->listings (n=5) | 546 | 731 | 1,131 | 788 | **1111** (1091-2076) | 1 / 140 / 140 | 138 | 335,759 / 0 | 37 (19) | 0 | - |
| listings:search (n=5) | - | - | - | 2784 | **2784** (2732-2855) | 5 / 788 / 213 | 539 | 1,587,104 / 1,329,746 | 6 (1) | 0 | - |
| results->car (n=5) | 652 | 801 | 801 | 792 | **792** (743-802) | 0 / 0 / 0 | 46 | 47,318 / 5,172 | 34 (23) | 0 | - |
| car->back (n=5) | - | - | - | 148 | **148** (130-171) | 0 / 0 / 0 | 0 | 0 / 0 | 1 (1) | 0 | restored from bfcache |
| results->car(2nd) (n=5) | 118 | 294 | 294 | 302 | **302** (262-323) | 0 / 0 / 0 | 141 | 177 / 0 | 33 (34) | 0 | - |

*phone flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| home->listings | `static/main.js / resolve-promise` | 366 |
| home->listings | `static/main.js / event-listener` | 102 |
| listings:search | `static/main.js / user-callback` | 526 |
| listings:search | `static/main.js / resolve-promise` | 133 |
| results->car(2nd) | `static/ds_comments.js / classic-script` | 27 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 184 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 404 | 718 | 2236 | 1,586,912 | 200 |
| `/api/session/listings-geo` | POST | 904 | 1062 | 1064 | 113 | 200 |



*desktop page switches: median of 3 runs (range of usable); browser 148.0.7778.96, load average at start [10.8212890625, 9.06591796875, 8.4072265625]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=3) | 280 | 496 | 1364 | 304 | **531** (450-644) | 0 / 0 / 0 | 3 | 114,283 / 618,660 | 20 (0) | - | - |
| home->listings (n=3) | 409 | 458 | 576 | 441 | **441** (440-442) | 0 / 0 / 0 | 34 | 335,736 / 0 | 37 (19) | 0 | - |
| listings:search (n=3) | - | - | - | 573 | **573** (505-584) | 1 / 54 / 54 | 114 | 1,587,113 / 2,706,504 | 6 (1) | 0 | - |
| results->car (n=3) | 458 | 515 | 515 | 495 | **495** (490-507) | 0 / 0 / 0 | 12 | 47,327 / 4,721 | 34 (23) | 0 | - |
| car->back (n=3) | - | - | - | 59 | **59** (56-65) | 0 / 0 / 0 | 0 | 0 / 0 | 1 (1) | 0 | restored from bfcache |
| results->car(2nd) (n=3) | 63 | 117 | 117 | 101 | **101** (94-102) | 0 / 0 / 0 | 47 | 177 / 0 | 33 (34) | 0 | - |
| car->home(logo) (n=3) | 234 | 270 | 1,146 | 252 | **252** (252-254) | 0 / 0 / 0 | 1 | 4,769 / 0 | 21 (22) | 0 | - |

*desktop flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| home->listings | `static/main.js / resolve-promise` | 78 |
| listings:search | `static/main.js / user-callback` | 82 |
| listings:search | `static/main.js / resolve-promise` | 31 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 63 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 142 | 315 | 350 | 1,586,935 | 200 |
| `/api/session/listings-geo` | POST | 938 | 971 | 971 | 91 | 200 |


What the comparison shows (phone medians of 5 runs; desktop of 3):

| step | with `nav_perf.js` (phone) | without (phone) | with (desktop) | without (desktop) |
|---|---|---|---|---|
| tap Listings on home: first byte | 1,005 ms | **546 ms** | 743 ms | **409 ms** |
| tap Listings on home: usable | 1,561 ms | **1,111 ms** | 787 ms | **441 ms** |
| tap Listings: same-origin bytes | 525,400 | **335,759** | 525,400 | **335,736** |
| tap the first car in the results: usable | **253 ms** | 792 ms | 495 ms | 495 ms |
| Back (bfcache) | 148 ms | 148 ms | 63 ms | 62 ms |
| from the car page to home (logo), desktop | | | 429 ms | **252 ms** |

* **For the pages without cache headers the script costs time.** Tapping Listings is 459 ms to first byte and
  450 ms to usable slower on the phone (-29 % without it), and 189,641 B heavier (the second copy of the
  HTML); on the desktop it is 346 ms slower (787 against 441 ms). The home link on the desktop is also slower (429 against 252 ms). On the origin each duplicate is a second render of `/listings` (about 238 ms of app time
  above the `/health` floor on Railway, section 2).
* **For the car page the script helps on the phone and not on the desktop.** On the phone it turns 792 ms
  into 253 ms (-539 ms). On the desktop the same step is 495 ms with it (499 ms in the re-run) and 495 ms without it: the
  prefetch does nothing there. The replay log shows why (desktop run, times after the first request): six car pages
  are prefetched at +4.37 to +4.95 s, `POST /api/session/listings-geo` answers with a **new session cookie at
  +4.877 s**, and the tap at +8.128 s sends `GET /car/1733731?zip_code=28105` again. Car pages say
  `Vary: Accept-Encoding, Cookie`, the cookie changed after the prefetch, so the browser's cached copy no
  longer matches (a `Vary` mismatch is a cache miss). On the phone the results arrive 2.2 s after the tap,
  later than the geo POST (+0.9 s), so the prefetch already carries the new cookie and works. A fast
  connection therefore loses the benefit the script exists for. **Confirmed by a what-if run.** With one change in the replay server (`GEO_NO_COOKIE=1`: the geo POST no
longer answers with a new session cookie; nothing else differs), the same desktop tap on the first car takes
**98 ms** (3 runs, range 94-141 ms) instead of 495 ms, because the six prefetched car pages are now reused. On the phone the same what-if changes nothing for that tap (271 ms
against 253 ms, 3 runs): its prefetch already ran after the cookie change.
The same cookie change also slows a returning shopper's listings page. After a search the shopper opens
`/listings?zip_code=28105&radius=50` again (`pw2/revisit_probe.py`, 3 runs): with the cookie rewritten the
page's cars request has to be revalidated before anything renders (a conditional GET, 304 with an 85-byte
response, +243 to +285 ms); with the cookie kept, Chrome renders from the cached body at once
(`stale-while-revalidate`) and revalidates in the background:

| profile | session cookie after the search | revisit: usable | revisit: key content visible | revisit: same-origin bytes | `/api/listings/cars` on the revisit |
|---|---|---|---|---|---|
| phone | rewritten by the geo POST (as live) | 2256 ms (2098-2432) | 2146 ms | 276,702 | GET Fetch +918 ms, first byte +1203, 85 B, from cache=False |
| phone | left alone (what-if) | 1747 ms (1732-1759) | 1747 ms | 276,535 | GET Fetch +927 ms, first byte +932, 0 B, from cache=True; GET Other +1184 ms, first byte +1568, 0 B, from cache=False |
| desktop | rewritten by the geo POST (as live) | 904 ms (889-909) | 904 ms | 345,296 | GET Fetch +513 ms, first byte +756, 85 B, from cache=False |
| desktop | left alone (what-if) | 631 ms (614-728) | 631 ms | 276,598 | GET Fetch +516 ms, first byte +517, 0 B, from cache=True; GET Other +586 ms, first byte +791, 0 B, from cache=False |


Keeping the cookie is worth **509 ms (-23 %) on the phone and 273 ms (-30 %) on the desktop** for a
returning shopper, and 397 ms for the first tap on a car.
* **The prefetch also spends the origin's time.** Six car pages were prefetched on the desktop and two on the
  phone; at about 250 ms of app time each on Railway (section 2) that is 0.5-1.5 s of the single worker's time
  per search, spent whether or not the visitor opens a car.

A first run of this comparison used a Playwright route to abort `nav_perf.js` instead
(`res2_phone_flow_abort_nocache.json`). It is discarded: Playwright turns the browser's HTTP cache off while a
route is active, so every later step re-downloaded its static files (163 KB for a car page that was
already loaded) and the car steps looked 500 ms slower than they are.

### 4.5 Back without the bfcache

Chrome keeps the previous page in memory only while it can. It cannot for some embedded browsers and
webviews, after memory pressure or a long wait, in other browsers' rules, or when a page opts out. The
results page is also the heaviest page on the site (46,803 elements, 23 MB of JS heap after a search), the
kind of page a phone is quickest to evict; this was not measured. With the back-forward cache disabled
(`--disable-back-forward-cache`, 3 runs):


*phone page switches (bfcache disabled): median of 3 runs (range of usable); browser 148.0.7778.96, load average at start [7.04931640625, 6.2734375, 6.31591796875]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=3) | 608 | 992 | 1000 | 696 | **944** (933-1163) | 1 / 51 / 51 | 14 | 117,226 / 618,649 | 20 (0) | - | - |
| home->listings (n=3) | 1,010 | 1,199 | 1,579 | 1243 | **1561** (1554-1587) | 1 / 133 / 133 | n/a | 525,377 / 0 | 38 (19) | 0 | - |
| listings:search (n=3) | - | - | - | 2683 | **2761** (2683-2789) | 5 / 743 / 210 | 499 | 1,609,511 / 1,329,509 | 8 (1) | 2 | - |
| results->car (n=3) | 113 | 242 | 242 | 229 | **229** (218-252) | 1 / 58 / 58 | n/a | 36,368 / 5,167 | 34 (24) | 0 | - |
| car->back (n=3) | 571 | 750 | 1,798 | 1902 | **2054** (2048-2375) | 5 / 624 / 218 | n/a | 190,204 / 0 | 41 (42) | 2 | reloaded (['Circumstantial', 'BrowsingInstanceNotSwapped']) |
| results->car(2nd) (n=3) | 642 | 791 | 791 | 795 | **795** (785-806) | 1 / 58 / 58 | n/a | 11,256 / 0 | 33 (33) | 0 | - |

*phone flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| car->back | `static/main.js / resolve-promise` | 479 |
| car->back | `static/main.js / user-callback` | 319 |
| home->listings | `static/main.js / resolve-promise` | 344 |
| home->listings | `static/main.js / event-listener` | 97 |
| listings:search | `static/main.js / user-callback` | 481 |
| listings:search | `static/main.js / resolve-promise` | 133 |
| results->car(2nd) | `static/ds_comments.js / classic-script` | 24 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 170 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 366 | 674 | 2185 | 1,586,912 | 200 |
| `/api/session/listings-geo` | POST | 867 | 1047 | 1055 | 112 | 200 |
| `/car/1733731?zip_code=28105` | GET | 2557 | 3090 | 3126 | 10,859 | 200 |
| `/car/1708513?zip_code=28105` | GET | 2557 | 3075 | 3114 | 11,540 | 200 |


* **Back costs 2,054 ms instead of 148 ms** (re-run 2,282 ms, range 2,186-2,611). The listings HTML is downloaded again (190,204 B; the 1.59 MB
  API body came from the HTTP cache), the facet lists are rebuilt (`main.js | resolve-promise` 479 ms), the
  results are re-rendered (`user-callback` 319 ms), five long tasks add up to 624 ms, and the first byte
  needs 571 ms. LCP is the first card photo at 1,798 ms.
* The "Script" column is `n/a` for cross-document steps in this run: Chrome does not swap renderer
  processes when the bfcache is off, so the CDP counters keep accumulating.

### 4.6 A shopper who does not wait

Search, wait 2.5 s, tap the first car, wait 4 s, Back (3 runs per profile):


*phone impatient shopper (search, 2.5 s, tap first car, 4 s, Back); runs=3*

| step | value per run (ms) |
|---|---|
| listings direct load, usable | [1481, 2532.3, 1521.3] |
| listings:search(visible only) | [2789.9, 2858.2, 2778.1] |
| results->car(quick) | [204.1, 221.3, 201.9] |
| car->back(quick): click->visible | [179.3, 173.7, 166.4] |
| car->back(quick): click->usable | [179.3, 173.7, 166.4] |
| car->back(quick): bfcache restored | [True, True, True] |
| car->back(quick): same-origin bytes | [0, 0, 0] |
| car->back(quick): long tasks (n / total) | [0, 0, 0] / [0, 0, 0] |



*desktop impatient shopper (search, 2.5 s, tap first car, 4 s, Back); runs=3*

| step | value per run (ms) |
|---|---|
| listings direct load, usable | [646.4, 697.9, 683.2] |
| listings:search(visible only) | [444.0, 541.2, 537.3] |
| results->car(quick) | [483.0, 476.4, 477.1] |
| car->back(quick): click->visible | [63.9, 62.8, 61.7] |
| car->back(quick): click->usable | [63.9, 62.8, 61.7] |
| car->back(quick): bfcache restored | [True, True, True] |
| car->back(quick): same-origin bytes | [0, 0, 0] |
| car->back(quick): long tasks (n / total) | [0, 0, 0] / [0, 0, 0] |


The impatient flow is no worse than the patient one, because results take 2.8 s to appear on the phone and
the car page is prefetched the moment they do.

### 4.7 What a first visitor sees when the origin is cold

The same phone flow, but each response waits the **first-ever** live server time of its URL (home 2,901 ms,
listings 714 ms, API 4,914 ms, the stalls seen in the first live samples):


*phone page switches: median of 3 runs (range of usable); browser 148.0.7778.96, load average at start [9.98974609375, 8.08642578125, 7.06494140625]*

| step | click->TTFB | click->FCP | click->LCP | click->key content visible | click->usable | long tasks after click (n / total / max) | Script (ms) | bytes (same-origin / 3rd-party) | same-origin requests (served from cache) | car docs prefetched | back/forward |
|---|---|---|---|---|---|---|---|---|---|---|---|
| home:cold (direct load, n=3) | 3353 | 3680 | 3756 | 3440 | **4323** (3666-4865) | 1 / 59 / 59 | 14 | 117,241 / 618,681 | 20 (0) | - | - |
| home->listings (n=3) | 1,787 | 1,973 | 2,371 | 2026 | **2352** (2338-2364) | 1 / 144 / 144 | 144 | 525,422 / 0 | 38 (19) | 0 | - |
| listings:search (n=3) | - | - | - | 7486 | **7548** (7486-7743) | 5 / 807 / 221 | 552 | 1,609,532 / 1,329,848 | 8 (1) | 2 | - |
| results->car (n=3) | 116 | 253 | 253 | 240 | **240** (224-252) | 0 / 0 / 0 | 125 | 36,368 / 5,169 | 34 (24) | 0 | - |
| car->back (n=3) | - | - | - | 155 | **155** (147-162) | 0 / 0 / 0 | 0 | 0 / 0 | 1 (1) | 0 | restored from bfcache |
| results->car(2nd) (n=3) | 106 | 271 | 271 | 269 | **269** (266-288) | 0 / 0 / 0 | 103 | 177 / 0 | 33 (34) | 0 | - |

*phone flow: script time inside long animation frames (LoAF), median ms per run, >= 20 ms*

| page/step | script (invoker type) | ms |
|---|---|---|
| home->listings | `static/main.js / resolve-promise` | 351 |
| home->listings | `static/main.js / event-listener` | 101 |
| home->listings | `static/listings_boot.js / event-listener` | 22 |
| listings:search | `static/main.js / user-callback` | 534 |
| listings:search | `static/main.js / resolve-promise` | 139 |
| results->car(2nd) | `static/ds_comments.js / classic-script` | 26 |

*search step: request timeline relative to the tap (median over runs), ms*

| request | method | start | first byte | done | bytes | status |
|---|---|---|---|---|---|---|
| `/api/zip-coords?zip=28105` | GET | 187 | None | None | 0 | None |
| `/api/listings/cars?zip=28105` | GET | 406 | 5474 | 6995 | 1,586,934 | 200 |
| `/api/session/listings-geo` | POST | 952 | 1112 | 1121 | 114 | 200 |
| `/car/1733731?zip_code=28105` | GET | 7399 | 7937 | 7975 | 10,857 | 200 |
| `/car/1708513?zip_code=28105` | GET | 7399 | 7917 | 7956 | 11,540 | 200 |


The page a shopper opens first takes **4.3 s** to become usable (TTFB 3.35 s), and the search takes
**7.5 s** (against 2.7 s), with everything after it unchanged. These are the stalls of section 2, combined
into one visit; they are worst case, not typical. The re-verification run's first hit of the same area took 1.6 s
instead of 4.9 s; with that wait for the API (the only change) the search takes **4.2 s** (4,195-4,347 ms, 3 runs;
section 10.3), so a cold area costs the phone 1.5 to 4.8 s on top of the 2.7 s.

## 5. Where the time goes

### 5.1 One search on the phone, millisecond by millisecond

Median of 5 runs of the "listings:search" step (tap "Search near this ZIP" with 28105 typed in), warm
origin, times in ms after the tap. The request timeline comes from CDP `Network` events.

| t after tap | what happens | share of the 2,645 ms to first cards |
|---|---|---|
| 0-170 | tap, form handling | |
| 170 | `GET /api/zip-coords?zip=28105` starts; the cars request waits for its answer | |
| 378 | `GET /api/listings/cars?zip=28105&radius=50` starts | **378 ms (14 %) before the data request exists** |
| 689 | first byte of the 1,586,912 B body (RTT 150 + live server hit time 150 + queueing) | 311 ms (12 %) |
| 879-1,053 | `POST /api/session/listings-geo` (stores the ZIP in the session; answers with a new session cookie) | |
| 2,192 | last byte | **1,503 ms (57 %) of download** (1.59 MB at 9 Mbps = 1,410 ms) |
| 2,551 | results exist; the page starts prefetching the car pages of the visible cards (2 on the phone) | |
| 2,645 | first card visible (JSON.parse of 14.5 MB, cascade and facet counts, 24 cards rendered) | 453 ms (17 %) of main-thread work after the last byte |
| 2,743 | main thread quiet ("usable") | |

Main-thread detail from the Chrome trace of the same step (`pw2/trace2_phone.json`, summarised like
DevTools' Summary tab): 1,635 ms of main-thread task time in 3,282 tasks, of which scripting 700 ms
(`main.js` 525 ms, JSON and microtasks with no file 185 ms), other 637 ms, paint 147 ms, style+layout
130 ms, GC 18 ms; five long tasks (205, 187, 156, 127, 102 ms). Per-file LoAF script time in the flow runs:
`main.js | user-callback` 483 ms and `main.js | resolve-promise` 127 ms.


*trace summary, phone*

| step | main-thread task time | long tasks > 50 ms (ms) | self time by activity (ms) | script self time by file (ms) |
|---|---|---|---|---|
| listings_load | 792.8 ms in 953 tasks | [323.7, 145.4, 83.0] | scripting 457.3, other 161.3, style+layout 106.0, parse-html/css 32.1, gc 27.0, paint 9.1 | (no url: microtasks/json/native) 343.1, main.js 97.2, listings_boot.js 23.3, static.cloudflareinsights.com/beacon.min.js/vce701823e78c430 9.2 |
| search | 1635.0 ms in 3282 tasks | [205.1, 187.1, 156.1, 126.5, 102.4] | scripting 699.8, other 636.8, paint 147.1, style+layout 130.3, gc 18.3 | main.js 524.5, (no url: microtasks/json/native) 184.8 |
| car_load | 602.8 ms in 1086 tasks | [] | style+layout 286.3, other 170.6, scripting 88.1, paint 34.8, gc 15.8, parse-html/css 7.3 | (no url: microtasks/json/native) 41.6, car_page.js 26.1, car/car_tco.js 16.7, static.cloudflareinsights.com/beacon.min.js/vce701823e78c430 8.5 |
| back_to_results | 185.7 ms in 434 tasks | [] | other 94.1, style+layout 53.0, scripting 21.7, paint 16.9 | compare.js 8.9 |


So on a 9 Mbps phone about **57 % of the wait is the transfer of data the first screen does not need**, and
about 26 % is a request chain (a ZIP lookup, then the data) plus server wait that a paged API would
shorten only a little. The main thread is 17 %, but see 5.2: most of that is not the JSON.

### 5.2 What each fix is worth: what-if runs on the replay

Each variant changes one thing inside the browser (a Playwright route), then repeats the listings load
and the search. Phone profile, 3 runs per variant, medians (range). The variants (the re-run of 2026-11-10, section 10.3, repeated
`baseline`, `no_facets`, `paged_api` and `both`):
`baseline` as served; `no_facets` the 574 KB `/api/listings/filter-options` request is aborted, so the
model/trim/package lists are never grafted into the page; `paged_api` `/api/listings/cars` returns only the
first 24 cars (24,445 B of JSON; delayed by the same RTT and live hit time, so only the size differs);
`no_island` the 1.04 MB `ds-listings-car-rows` JSON island is replaced by an empty table. The last
variant `both` is `no_facets` + `paged_api` together (5 runs).


| variant (phone) | listings usable | listings DOM nodes | listings ScriptDuration | listings long tasks total | listings bytes same-origin | search click->results visible | search click->usable | search long tasks (n / total / max) | search ScriptDuration | search bytes same-origin |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 1732 (1436-1782) | 69,176 | 142 | 147 | 448,081 | 2682 (2676-2729) | 2682 | 5 / 772 / 212 | 522 | 1,609,614 |
| no_facets | 1452 (1433-1500) | 13,436 | 140 | 191 | 361,549 | 2061 (2053-2090) | 2061 | 2 / 167 / 91 | 81 | 1,610,027 |
| paged_api | 1448 (1447-1474) | 69,176 | 138 | 191 | 448,091 | 1007 (965-1082) | 1082 | 4 / 585 / 200 | 485 | 49,476 |
| no_island | 1453 (1425-1614) | 69,227 | 47 | 149 | 769,412 | 2702 (2505-2719) | 2719 | 5 / 788 / 221 | 536 | 1,610,042 |



| variant (phone) | listings usable | listings DOM nodes | listings ScriptDuration | listings long tasks total | listings bytes same-origin | search click->results visible | search click->usable | search long tasks (n / total / max) | search ScriptDuration | search bytes same-origin |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 1499 (1439-1718) | 69,176 | 152 | 158 | 448,081 | 2784 (2650-2908) | 2784 | 5 / 837 / 230 | 553 | 1,609,637 |
| both | 1535 (1431-2272) | 13,437 | 144 | 198 | 361,572 | 911 (526-936) | 911 | 0 / 0 / 0 | 75 | 49,484 |


The same variants on the desktop profile (3 runs each; no throttling, so the search is already fast):


| variant (desktop) | listings usable | listings DOM nodes | listings ScriptDuration | listings long tasks total | listings bytes same-origin | search click->results visible | search click->usable | search long tasks (n / total / max) | search ScriptDuration | search bytes same-origin |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 854 (663-929) | 69,176 | 35 | 0 | 448,278 | 546 (474-551) | 546 | 1 / 53 / 53 | 117 | 1,655,811 |
| no_facets | 692 (686-2834) | 13,363 | 42 | 53 | 361,816 | 320 (319-356) | 320 | 0 / 0 / 0 | 19 | 1,656,199 |
| paged_api | 716 (711-718) | 69,176 | 36 | 0 | 448,395 | 409 (399-417) | 409 | 1 / 51 / 51 | 104 | 95,506 |
| no_island | 645 (644-1904) | 69,231 | 11 | 0 | 769,399 | 489 (474-490) | 489 | 1 / 55 / 55 | 115 | 1,656,479 |



| variant (desktop) | listings usable | listings DOM nodes | listings ScriptDuration | listings long tasks total | listings bytes same-origin | search click->results visible | search click->usable | search long tasks (n / total / max) | search ScriptDuration | search bytes same-origin |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 648 (635-679) | 69,176 | 39 | 51 | 448,259 | 475 (470-488) | 475 | 0 / 0 / 0 | 106 | 1,655,844 |
| both | 686 (670-709) | 13,363 | 37 | 0 | 361,685 | 317 (312-356) | 317 | 0 / 0 / 0 | 16 | 95,507 |


What they say:

* **Paging the API is the single biggest fix for search: 2,682 ms to 1,007 ms to first cards (-62 %; re-run
  2,789 to 1,267 ms, -55 %)**, and the search then moves 49 KB instead of 1.61 MB. Main-thread work does not shrink (Script 485 ms), because
  something other than the JSON dominates it (next bullet).
* **The facet lists cost more than the 14.5 MB of JSON.** Aborting the filter-options request drops the
  DOM from 69,176 to 13,436 CDP nodes (-55,740), the search handler's `user-callback` from 510 ms to 24 ms,
  the search's Script from 522 ms to 81 ms, and its long tasks from 5 (772 ms) to 2 (167 ms). The search
  gets 621 ms faster (2,682 to 2,061 ms; re-run 681 ms, 2,789 to 2,108 ms) with the 1.59 MB download unchanged. At page load, the same
  abort removes `main.js | resolve-promise` (320 ms of script in LoAF) and one 150 ms long task. The
  measured part is the drop in `user-callback` time (510 to 24 ms). The reason is read from the code, not
  profiled function by function: 13,966 model/trim/package options are built into the DOM
  (`buildLazyFacetLabel`, three elements each, one container per list; counted in section 10.4), and every search
  runs `runCascade()` (`main.js:167`), which walks the option DOM of each facet and sets `style.display`
  per option, then `updateAllCounts()` (13 `querySelectorAll('input[name=..]:checked')` scans), while
  `checked()` scans every checkbox on the page whenever its cache is invalidated (15,462 checkboxes with the
  lists, 1,496 without). With the lists grafted in, that is tens of thousands of elements per pass.
* **The 1.04 MB JSON island costs the listings page about 95 ms of script on every load** (Script 142 to
  47 ms; `event-listener` 104 to 29 ms) and **82 % of its wire HTML** (brotli q5: 128,657 of 157,291 B, so
  roughly 155 KB of the 189,535 B Cloudflare sends; 138 ms of transfer at 9 Mbps). It does not change
  search time.
* **The desktop gains are smaller but real:** search to first cards 475 ms to 317 ms with both changes
  (-33 %), 546 to 320 ms with the facet lists alone, 546 to 409 ms with paging alone.
* **Compression is a cheap partial fix for the transfer.** The recorded 14.5 MB body compresses to
  879,659 B with brotli q5 (-45 % against the live gzip; 61 ms of server CPU) and 886,799 B with zstd 9
  (`pw2/payload_probe.py`). At 9 Mbps that turns 1,410 ms of transfer into about 780 ms (arithmetic, not
  measured in a browser): about 0.6 s of the 2.7 s.

### 5.3 What the 15 stylesheets cost

Chromium's own coverage tools on a 390 px viewport, 2.5 s after load (`pw2/coverage_probe.py`): CSS rule-usage
tracking (a rule is "used" if it matched an element at some point; every other byte of the sheet counts as
unused, including rules in a media query that does not match) and precise JS coverage with block counts. Both
are lower bounds on "needed" (hover states, later interactions and other widths show as unused).

| page | kind | files | total bytes | used bytes | unused bytes | unused % |
|---|---|---|---|---|---|---|
| home | CSS | 15 | 283,202 | 17,076 | 266,126 | 94 % |
| home | JS | 1 | 10,621 | 4,685 | 5,936 | 56 % |
| listings | CSS | 15 | 283,202 | 24,462 | 258,740 | 91 % |
| listings | JS | 16 | 242,618 | 77,035 | 165,583 | 68 % |
| listings_zip (results shown) | CSS | 15 | 283,202 | 27,156 | 256,046 | 90 % |
| listings_zip (results shown) | JS | 16 | 242,618 | 114,114 | 128,504 | 53 % |
| car | CSS | 15 | 283,202 | 34,783 | 248,419 | 88 % |
| car | JS | 12 | 177,675 | 84,352 | 93,323 | 53 % |
| dealership | CSS | 17 | 310,176 | 31,720 | 278,456 | 90 % |
| dealership | JS | 18 | 382,585 | 157,113 | 225,472 | 59 % |


Every page loads 15 stylesheets (17 on the dealership page) of 283,202 B (310,176 B) decoded, and at the
phone viewport **88-94 % of those bytes match nothing on the page**: 17,076 B are used on home (6 %), 24,462 B
on listings (9 %), 27,156 B with results shown (10 %), 34,783 B on the car page (12 %) and 31,720 B on the
dealership page (10 %). The same bytes are parsed, matched against the DOM on every style recalculation, and
(first visit) fetched as 15 render-blocking requests. 53-68 % of the JavaScript bytes loaded by each page never
run on it (listings: 165,583 of 242,618 B). The structural evaluation reached the same conclusion from the
source (`frontend-css-templates.md` F3: one global bundle, "static usable upper bound per page 3-32 %").

| page | variant | n | CSS requests | FCP (range) | LCP | usable (range) | style+layout | Script | same-origin bytes | pixels that differ from the baseline shot |
|---|---|---|---|---|---|---|---|---|---|---|
| home | baseline, 15 stylesheets | 6 | 15 | 1020 (992-1040) | 1032 | 970 (888-1049) | 156 | 16 | 117,226 | (reference) |
| home | one stylesheet, all rules | 6 | 1 | 1046 (1004-1244) | 1930 | 1113 (962-2652) | 153 | 15 | 106,664 | 0.0 % |
| home | one stylesheet, used rules, @media lost (v1, invalid) | 6 | 1 | 1002 (984-1196) | 1012 | 1081 (904-1623) | 142 | 14 | 63,206 | 15.5 % |
| home | baseline, 15 stylesheets (second batch) | 6 | 15 | 1030 (996-1040) | 1912 | 1186 (950-16145) | 140 | 14 | 117,226 | (reference) |
| home | one stylesheet, used rules, @media kept (v2) | 6 | 1 | 1004 (988-1192) | 1892 | 983 (927-1172) | 127 | 14 | 63,198 | 0.0 % |
| car | baseline, 15 stylesheets | 6 | 15 | 1260 (1252-1284) | 1276 | 1237 (1201-1284) | 288 | 80 | 166,889 | (reference) |
| car | one stylesheet, all rules | 6 | 1 | 1284 (1276-1296) | 1288 | 1217 (1207-1226) | 281 | 80 | 156,131 | 0.1 % |
| car | one stylesheet, used rules, @media lost (v1, invalid) | 6 | 1 | 1230 (1200-1428) | 1230 | 1172 (1159-2200) | 293 | 84 | 115,627 | 41.5 % |
| car | baseline, 15 stylesheets (second batch) | 6 | 15 | 1260 (1260-1272) | 1260 | 1200 (1197-1204) | 266 | 66 | 166,886 | (reference) |
| car | one stylesheet, used rules, @media kept (v2) | 6 | 1 | 1222 (1216-1400) | 1228 | 1161 (1156-1651) | 221 | 66 | 115,610 | 1.6 % |
| listings | baseline, 15 stylesheets | 6 | 15 | 1172 (1160-1184) | 1556 | 1532 (1485-1642) | 147 | 150 | 448,108 | (reference) |
| listings | one stylesheet, all rules | 6 | 1 | 1174 (1168-1188) | 1456 | 1435 (1425-1518) | 154 | 147 | 421,245 | 0.7 % |
| listings | one stylesheet, used rules, @media lost (v1, invalid) | 6 | 1 | 1136 (1124-1140) | 1136 | 1452 (1401-3658) | 104 | 147 | 379,054 | 32.6 % |
| listings | baseline, 15 stylesheets (second batch) | 6 | 15 | 1162 (1160-1168) | 1554 | 1532 (1483-2571) | 139 | 141 | 448,081 | (reference) |
| listings | one stylesheet, used rules, @media kept (v2) | 6 | 1 | 1136 (1128-1352) | 1492 | 1464 (1414-1763) | 97 | 140 | 379,092 | 0.0 % |

Noise floor (baseline of batch H against baseline of batch v2, same server, different runs): home 0.0 %, car 0.0 %, listings 0.7 %


**What the what-if is.** The replay server served each page with its 15 `<link rel=stylesheet>` replaced by one, so the
HTTP cache, the network model and the baseline stay valid. (No Playwright routes: the first version of this what-if
used routes, and a route that changed nothing already moved first paint by +176, +144 and +848 ms for home, car and
listings; it was discarded, `css_cf_phone.md`.) Cold phone loads, 2 rounds x 3 runs pooled, 6 per cell, with the
baseline measured in the same batch. The last column compares the end-of-load screenshot with that baseline's: two
baselines from different batches differ by 0.0-0.7 % of pixels, which is the noise floor. The table keeps the invalid
builds for the record; only the rows labelled as baseline, "all rules" and "v2" are results.

* **One merged stylesheet is not faster.** First paint moves by +26 / +24 / +2 ms (home / car / listings) with the
  same render (0.0 / 0.1 / 0.7 % of pixels). 15 requests become 1 and the CSS shrinks from 51,644 B to 47,851 B of
  brotli, but on HTTP/2 the 15 files already travel together, so the one round trip is paid either way.
* **Serving only the rules a page uses saves about 30 ms.** Rule usage was recorded at 390 px (CDP rule-usage
  tracking) and each used rule was kept inside its original `@media` block: 4,390 B (home), 7,350 B (car) and
  5,539 B (listings) of brotli instead of 51,644 B. Against the same batch's baseline (1,030 / 1,260 / 1,162 ms)
  first paint moves by **-26 / -38 / -26 ms**, style+layout by -13 / -45 / -42 ms, and the screenshots match
  (0.0 / 1.6 / 0.0 % of pixels differ). That is about what 47 KB costs at 9 Mbps (42 ms). The "usable" column of the
  table moves by more than that and is noise (one baseline home load took 16 s).
* **The first two builds of that variant were wrong, and the pixel check is what showed it.** CDP reports every
  `@media` rule as "used" with a range that covers only its query list (26 of the 152 "used" entries on the home
  page, text such as `(max-width: 1000px) `). The first build (v1) concatenated the used spans without their
  `@media` blocks; the second kept the blocks but left those fragments in, and a fragment swallows the next rule. Both
  rendered 15.5 / 41.5 / 32.6 % of the pixels differently and are not results (the second build's raw files are
  `res2_css_*firstbuild*.json`). The third build drops the fragments and re-wraps each rule in its `@media` chain.
  It is valid at the 390 px viewport it was recorded at and only there: desktop-only rules and `:hover` states
  were never "used" in that run, so a real bundle needs coverage at both widths and the owner's check of
  screenshots at 390 and 1440 px.
* **So the 380 ms between the first byte and first paint on the home page (988 ms against 608 ms) is not mostly
  CSS bytes or file count.** It is one round trip for the stylesheets (150 ms, paid however many files), the
  4x-slowed style, layout and paint (134 ms of style+layout), and the font and parse steps. Bundling could remove
  about 30 ms of it.

### 5.4 What an edge cache would buy (what-if)

If Cloudflare could answer the anonymous HTML pages and the two listings JSON endpoints, the origin's share
of each request would shrink to the edge's. The replay server can override the server wait of one URL, so the
same page loads were repeated with `/`, `/listings`, `/dealership/<key>`, `/api/listings/filter-options` and
`/api/listings/cars?...` answered after **15 ms** instead of their live medians (155, 323, 237, 120 and 150 ms;
the car page is `private` and is left as recorded). The 150 ms RTT and the handshake are still paid. Baseline and edge runs alternated (5 pairs, one cold load
of each page per run) so both saw the same machine load; medians:

| page (phone, cold) | replayed live server wait, baseline -> edge | n | TTFB | FCP | usable | change in usable |
|---|---|---|---|---|---|---|
| home | 155 -> 15 ms | 5+5 | 608 -> 468 | 1008 -> 856 | 947 -> 791 | **-156 ms** (-16 %) |
| listings | 323 -> 15 ms (+ filter-options 120 -> 15) | 5+5 | 774 -> 468 | 1172 -> 860 | 1497 -> 1139 | **-358 ms** (-24 %) |
| listings_zip | 323 -> 15, filter-options 120 -> 15, API 150 -> 15 ms | 5+5 | 860 -> 467 | 1248 -> 852 | 4150 -> 3646 | **-504 ms** (-12 %) |
| dealership | 237 -> 15 ms | 5+5 | 689 -> 467 | 1068 -> 852 | 1526 -> 1317 | **-209 ms** (-14 %) |


What the table says:

* **The saving is the server wait that is taken away, and a little more.** The replayed wait falls by 140 ms
  (home), 308 ms plus 105 ms for the facet list (listings) and 222 ms (dealership); the first byte moves by 140, 306
  and 222 ms (608 to 468, 774 to 468, 689 to 467 ms), and the visitor's gain is 156, 358 and 209 ms.
* **A search page with a remembered ZIP gains least in relative terms (-12 %)**: its 4.1 s is the 1.59 MB body and
  the dealer photos, which an edge cache of the HTML and JSON does not make smaller.
* **This is an upper bound on what a cache rule can deliver.** The what-if assumes the edge may answer every
  anonymous request. Today the HTML cannot be shared (a per-request CSP nonce, a session cookie created by the CSRF-token
  helper on the first render, `Vary: Cookie`; `server-causes.md` section 7) and the two JSON endpoints could be
  (the cars body is already marked `public, s-maxage=60`; Cloudflare does not cache extension-less JSON without a
  rule, and the first run saw `DYNAMIC` on every one of them). The 150 ms RTT and the handshake are still paid in
  the model, as they would be at a real edge.
* **The car page is the control** (`private`, left at its live wait): it moved by +9 ms on the phone and +4 ms on
  the desktop (+1 %) in the first run's pass below, which is the noise level of that comparison.

The first run also measured the same what-if on the desktop and on the car page, **not interleaved** (the baseline
pages at 20:55 under a load average of about 5, the edge pages at 21:34-21:39 while the load fell from 40 to 5, so
the bias is against the edge). The phone rows agree with the interleaved table (-138 / -318 / -400 / -242 ms against
-156 / -358 / -504 / -209 ms); the desktop gains are larger in relative terms because its 31 ms round trip leaves
the origin wait as most of the page:

| profile | page (cold) | live server wait replayed (baseline) | TTFB base -> edge | FCP base -> edge | usable base -> edge | change in usable |
|---|---|---|---|---|---|---|
| phone | home | 155 ms | 608 -> 467 | 988 -> 856 | 932 -> 794 | -138 ms (-15 %) |
| phone | listings | 323 ms + filter-options 120 ms | 774 -> 468 | 1160 -> 864 | 1456 -> 1138 | -318 ms (-22 %) |
| phone | listings_zip | 323 + 120 + API 150 ms | 861 -> 468 | 1248 -> 860 | 4183 -> 3783 | -400 ms (-10 %) |
| phone | dealership | 237 ms | 689 -> 467 | 1064 -> 848 | 1540 -> 1298 | -242 ms (-16 %) |
| phone | car | 376 ms (private, not edge-cached; control) | 828 -> 829 | 1268 -> 1260 | 1211 -> 1220 | +9 ms (+1 %) |
| desktop | home | 155 ms | 279 -> 139 | 496 -> 356 | 439 -> 317 | -122 ms (-28 %) |
| desktop | listings | 323 ms + filter-options 120 ms | 446 -> 140 | 680 -> 376 | 684 -> 362 | -322 ms (-47 %) |
| desktop | listings_zip | 323 + 120 + API 150 ms | 533 -> 140 | 760 -> 368 | 1355 -> 975 | -380 ms (-28 %) |
| desktop | dealership | 237 ms | 360 -> 140 | 588 -> 372 | 579 -> 360 | -219 ms (-38 %) |
| desktop | car | 376 ms (private, not edge-cached; control) | 500 -> 501 | 716 -> 720 | 669 -> 673 | +4 ms (+1 %) |


## 6. Redis: what these live numbers say about it

Production has **no Redis today**: `GET /api/ready` answers `"redis":"skipped"` (`redis.md` section 3.1, row 1),
because `REDIS_URL` is unset and `backend/config.py` only reads it for the readiness probe. The design,
keys, failure behaviour and cost are in `redis.md`; this section only says which of the costs measured
here a shared cache would remove, and which it would not.

| measured here | what removes it | Redis role |
|---|---|---|
| First search in an area: **5,064 ms** (28105, 13,298 cars) and **5,530 ms** (37421, 5,633 cars) TTFB against 123-498 ms when the per-process LRU holds the body (28105 again 23 hours later: 1,635 ms, then 202 ms); 13.4 s for Irvine (`server-causes.md`) | A finished gzip body (1.59 MB) kept outside the web process, built by something other than the shopper's request | **Yes, as the store.** The LRU is lost at every restart and deploy, at the 00:00 UTC day roll (19:00 Eastern) and on its 64-entry/96 MB bound. It still needs a builder; Redis alone only stores what was built |
| Car page after a quiet spell: origin time **3,382 ms** (morning), **1,852, 2,185 and 2,222 ms** (01:51, 02:05 and 02:17 UTC; the 02:29 UTC round found it warm at 337 ms) and **2,090 ms** (01:23 UTC on the next day, the re-verification run) against 237-353 ms warm | Fleet-wide aggregates (rarity, price plausibility, featured cars) refreshed by a timer or after scans, with the web process only reading them (`server-causes.md` S2) | **Yes, as the place the refreshed aggregates live.** A cache with a TTL that is refilled on a shopper's request keeps the stall |
| More than one gunicorn worker (one worker holds 3.3 GB warm; 3 workers 13.7 GB) | Sharing the fleet caches between workers | **Yes, a prerequisite**, not a latency fix by itself |
| ~150 ms fixed cost per uncached request (`/health` 147-176 ms) | Fewer requests, cacheable HTML, an edge cache rule | **No.** It is the path Cloudflare to Railway to gunicorn; a Redis lookup adds a hop, it does not remove one |
| 189,535 B listings HTML (82 % JSON island), 574 KB facet list, a 46,803-element page, 14.5 MB search body | Slimmer pages and paged results | **No.** Client bytes and client CPU |
| 15 render-blocking stylesheets, 1.3-1.5 MB of dealer photos on a results screen | CSS bundling, image resizing | **No** |
| Tapping Listings fetches `/listings` twice; Back without bfcache costs 2.05 s | Cache headers and prefetch policy | **No** |

So Redis is worth adding for the first two rows and the worker count, but in this ranking it sits below
paging the search, removing the facet DOM, fixing the duplicate prefetch and giving the edge something it
may cache (the ranked table at the top), because those remove seconds on every visit rather than on the first visit after a
restart.

## 7. Screenshots

They are real Chromium renderings of the recorded live responses (replay), taken at the end of the step they are
named after; the `_mid` shot is taken 900 ms after the search tap. Phone shots are 1170x2532 px (390x844 at DPR 3),
desktop shots 1440x900. The key ones are listed here with full paths; Appendix E lists every file
(146, including the same pages under the what-if and re-run prefixes: `live-speed2-nobf_*`, `-coldserver_*`,
`-nonav_*`, `-nocookie_*`, `-edge_*`, the re-run `live-speed3-rv*` and the what-if shots `live-speed3-edgeab-*` and
`live-speed3-css*`, which include the CSS variants whose rendering was checked against the baseline in section 5.3).

| what | file (full path) |
|---|---|
| phone, home, cold load | `scratchpad/web_shots/live-speed2_phone_home_cold.png` |
| phone, listings, cold load (empty state: "Start a search to see cars", two ZIP prompts) | `scratchpad/web_shots/live-speed2_phone_listings_cold.png` |
| phone, listings with ZIP 28105 (deep link, results shown) | `scratchpad/web_shots/live-speed2_phone_listings_zip_cold.png` |
| phone, car page, cold load | `scratchpad/web_shots/live-speed2_phone_car_cold.png` |
| phone, dealership page, cold load | `scratchpad/web_shots/live-speed2_phone_dealership_cold.png` |
| phone flow 1, home | `scratchpad/web_shots/live-speed2_phone_flow_1_home.png` |
| phone flow 2, listings after tapping "Browse inventory" | `scratchpad/web_shots/live-speed2_phone_flow_2_listings.png` |
| phone flow 3, 900 ms after tapping search: "Loading cars near you...", two grey shimmer blocks | `scratchpad/web_shots/live-speed2_phone_flow_3_results_mid.png` |
| phone flow 3, results at 2.7 s ("1-24 of 13,298", first card fills the screen) | `scratchpad/web_shots/live-speed2_phone_flow_3_results.png` |
| phone flow 4, car page opened from the results | `scratchpad/web_shots/live-speed2_phone_flow_4_car.png` |
| phone flow 5, Back (bfcache restore) | `scratchpad/web_shots/live-speed2_phone_flow_5_back.png` |
| phone flow with the bfcache off, Back step (the page is rebuilt) | `scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_5_back.png` |
| phone flow against first-hit server times (cold origin), results | `scratchpad/web_shots/live-speed2-coldserver_phone_flow_3_results.png` |
| phone, impatient shopper, after Back | `scratchpad/web_shots/live-speed2_phone_quickback.png` |
| desktop, home, cold load | `scratchpad/web_shots/live-speed2_desktop_home_cold.png` |
| desktop, listings, cold load | `scratchpad/web_shots/live-speed2_desktop_listings_cold.png` |
| desktop, listings with ZIP 28105 | `scratchpad/web_shots/live-speed2_desktop_listings_zip_cold.png` |
| desktop, car page, cold load | `scratchpad/web_shots/live-speed2_desktop_car_cold.png` |
| desktop, dealership page, cold load | `scratchpad/web_shots/live-speed2_desktop_dealership_cold.png` |
| desktop flow 3, results | `scratchpad/web_shots/live-speed2_desktop_flow_3_results.png` |
| re-run 2026-11-10, phone, results | `scratchpad/web_shots/live-speed3-rv_phone_flow_3_results.png` |
| re-run 2026-11-10, phone, 900 ms after the search tap | `scratchpad/web_shots/live-speed3-rv_phone_flow_3_results_mid.png` |
| re-run 2026-11-10, desktop, results | `scratchpad/web_shots/live-speed3-rv_desktop_flow_3_results.png` |

What the phone shots show that the numbers do not: at 900 ms after the tap the visitor sees the filter bar and two
grey shimmer blocks under "Loading cars near you..."; the card with its photo appears at 2.7 s and fills the whole
screen, so a phone shows about one card at a time. The cold listings page shows two ZIP prompts one above the other
(the "Set your location to unlock filters" banner and the "Start a search to see cars" block). In the top left
corner the menu button sits on top of the first line of the "Save cars you like" banner on the listings and results
screens (spacing items, not speed items; `phone-spacing.md` covers them).

Earlier shots from the first, partly invalid harness (`live-speed_*.png`, listed in Appendix E) show the same pages
and are kept for comparison only.

## 8. What this report does not establish

* **No real phone.** The phone numbers are Chromium on an Apple M4 Max with 4x CPU throttling, 9 Mbps /
  150 ms network emulation and a replay origin. An iPhone 14 (A15) is, I expect, faster than a 4x-throttled M4 Max
  per core; a mid-range Android is about as slow. Treat the phone tables as relative sizes of causes, not as
  a stopwatch for any one device.
* **The origin is replayed, not exercised.** Each response waits its median live server time. Contention
  between concurrent requests in the single gunicorn worker (8 car prefetches plus filter-options plus a
  search, all hitting one worker) is not reproduced. `server-causes.md` section 2 measures the one-worker
  saturation locally (34 car pages per second, p50 42 ms at 1 user, 468 ms at 16).
* **The cold cases are samples, not a rate.** Eight of the 98 live HTML requests (8 %) had a TTFB over 1 s
  (3.5, 3.0, 2.7, 2.6, 2.3, 2.0, 1.5 and 1.5 s), but the set includes rounds designed to catch cold hits, so it is
  not a base rate. The car page was cold in 4 of 5 quiet-spell rounds (1.85-2.22 s of origin time) and warm in the
  fifth. How often a real visitor meets a stall depends on traffic that was not measured; on a site with few
  visitors the quiet-spell case may be the usual one.
* **Two requests could not be explained.** In the 01:51 UTC sample a `/listings` TTFB of 1,537 ms had only
  555 ms accounted for by Cloudflare's `cfEdge` and `cfOrigin` (about 850 ms unaccounted), and an `idle3` car
  request had 311 ms unaccounted; the other 15 requests with a Server-Timing header agree with it to within 70 ms,
  and two more differ only because a slow TCP handshake inflated the RTT subtracted. The first of these happened while a browser suite was
  running on the same machine and link, so a client-side cause is possible; neither was isolated.
* **The origin's region was not read from the live service.** No region setting is committed to `main`, but the
  repository's ops log on branch `phase/N37a` (`docs/SCANNING_OPS_LOG.md` lines 58 and 949, a snapshot of
  2026-10-05) records the web service as `us-west2` (California), and `server-causes.md` section 3.1 found the
  same 77-86 ms from three East Coast Railway edges. This report's 68-111 ms of origin time for `/health` is
  consistent with that; it is an inference from a documentation snapshot, not a measurement of the region.
* **Other agents were querying the same site** in the same windows (section 1.1). Some of the stalls and
  some of the "warm" results may be theirs.
* **Replay noise.** Repeated phone runs agree within about 5 % (section 10.3); the desktop's direct page loads
  moved by up to 100 ms between batches (the same `home:cold` step took 431 ms, 531 ms and 454 ms in three
  batches), so differences under about 100 ms on the desktop are not findings. The phone's `home:cold` step
  showed one batch of 1,165 ms against 942 ms in the others. The tap-Listings difference (346-459 ms) and the
  Back-without-bfcache difference (1.9 s) are several times larger than that noise.
* **Staged releases are not measured.** The live site runs 1.5.8. `git diff main phase/N37a` (1.5.11-1.5.16)
  touches the listing path in `frontend/static/main.js` (27 lines: keep the shopper's page on background
  refreshes, a "Details coming" line on cards), `frontend/static/listings/card.js` (7 lines) and
  `backend/routes/listings_api.py` (17 lines: a per-IP rate limit on cache-miss builds and on
  coords-to-zip). It does not touch `nav_perf.js`, `listings_boot.js`, `listings.html` or the response
  headers, so the ranked causes above are not fixed there. `server-causes.md` section 9 lists what the
  server-side releases do fix.
* **No signed-in visitor, no HTTP/3, no Safari.** The numbers are for an anonymous visitor on h2 in
  Chromium. `server-causes.md` S6 measures 48 ms of SQLCipher key derivation per users-store open for
  signed-in visitors.

## 9. Commands

Scripts, logs and result files are in `raw/live-speed/` next to this report (layout and scrubbing in
`raw/live-speed/README.txt`). They ran from the session scratchpad, shortened to `$SP` below:

```
SP=scratchpad
PY=.venv/bin/python        # Playwright 1.x, Chromium 148.0.7778.96
```

### 9.1 Live requests (curl, through the rate-limited wrapper)

```
# the exact curl that live.py runs for every transfer (GET only; -w gives DNS, connect, TLS, TTFB, total, size, http version)
curl -sS --http2 --rate 1/s -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36' \
     -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8' \
     -H 'Accept-Encoding: br, gzip' -H 'Accept-Language: en-US,en;q=0.9' \
     -w '<<REC>>{"wo":%{json},"hdr":%{header_json}}<<END>>\n' -o BODY URL

# cold-ish: a fresh curl process per transfer (new DNS + TCP + TLS); warm: 5 transfers over one HTTP/2 connection
python3 $SP/speed/live/live.py one  home_cold  https://sarraficars.com/ html
python3 $SP/speed/live/live.py warm home_warm  5 https://sarraficars.com/ html
sh      $SP/speed/live/run_timing.sh        # rounds 2-5 of cold-ish runs for the 5 endpoints, then the 5 warm runs each
# static assets, one connection each batch (URL lists reconstructed from the log in live/url_lists/)
python3 $SP/speed/live/live.py batch assets1 any $(cat $SP/speed/live/url_lists/assets1.txt)
python3 $SP/speed/live/live.py batch assets2 any $(cat $SP/speed/live/url_lists/assets2.txt)
python3 $SP/speed/live/live.py batch assets3 any $(cat $SP/speed/live/url_lists/assets3.txt)
# evening sample, quiet-gap rounds, conditional GET, extra metro, asset repeats
sh $SP/speed/live/idle_sample.sh idle1
sh $SP/speed/live/idle_round.sh idle2 home        # >= 11 min after the previous request; also idle3 listings, idle4 car
LIVE_INM='W/"lc-28105-50-..."' python3 $SP/speed/live/live.py one api_inm_match "https://sarraficars.com/api/listings/cars?zip=28105&radius=50" json
sh $SP/speed/live/live_chain.sh                   # idle3, idle4 and the extras, as run
python3 $SP/speed/live/live.py count              # transfers used so far
python3 $SP/speed/live/analyze_curl.py            # tables in sections 2 and Appendix A/B
python3 $SP/speed/live/summarize_idle.py idle1 idle2 idle3 idle4
```

### 9.2 Replay of the recorded responses

```
python3 $SP/speed/replay/build_manifest.py $SP/speed/replay/manifest_warm.json --mode warm --no-stream
python3 $SP/speed/replay/build_manifest.py $SP/speed/replay/manifest_cold.json --mode cold --no-stream
# certificate: a self-signed cert for sarraficars.com / www.sarraficars.com (key.pem, cert.pem); the pin Chromium trusts:
openssl x509 -in cert.pem -pubkey -noout | openssl pkey -pubin -outform der | openssl dgst -sha256 -binary | base64   # = replay/spki.txt
cd $SP/speed/replay
RTT_MS=150 HANDSHAKE_MS=300 node server.mjs manifest_warm.json 8443 log_phone.jsonl      # phone, warm server
RTT_MS=31  HANDSHAKE_MS=91  node server.mjs manifest_warm.json 8444 log_desktop.jsonl    # desktop, warm server
RTT_MS=150 HANDSHAKE_MS=300 node server.mjs manifest_cold.json 8445 log_phone_cold.jsonl # phone, first-hit server times
```

### 9.3 Browser runs (all local, sequential, under `caffeinate -i`)

```
cd $SP/speed/pw2
$PY -u measure2.py 8443 phone   pages 5 res2_phone_pages.json
$PY -u measure2.py 8444 desktop pages 5 res2_desktop_pages.json
$PY -u measure2.py 8443 phone   flow  5 res2_phone_flow.json
$PY -u measure2.py 8444 desktop flow  3 res2_desktop_flow.json
NOBF=1 SHOT_PREFIX=live-speed2-nobf $PY -u measure2.py 8443 phone flow 3 res2_phone_flow_nobf.json     # bfcache off
$PY -u measure2.py 8443 phone   quickback 3 res2_phone_quickback.json
$PY -u measure2.py 8444 desktop quickback 3 res2_desktop_quickback.json
SHOT_PREFIX=live-speed2-coldserver $PY -u measure2.py 8445 phone flow 3 res2_phone_flow_coldserver.json
ABORT="**/static/nav_perf.js*" SHOT_PREFIX=live-speed2-nonav $PY -u measure2.py 8443 phone flow 5 res2_phone_flow_nonav.json
ABORT="**/static/nav_perf.js*" SHOT_PREFIX=live-speed2-nonav $PY -u measure2.py 8444 desktop flow 3 res2_desktop_flow_nonav.json
cd ../pw
$PY -u trace_steps.py 8443 phone   $SP/speed/pw2/trace2_phone.json
$PY -u trace_steps.py 8444 desktop $SP/speed/pw2/trace2_desktop.json
$PY -u counterfactual.py 8443 phone   3 $SP/speed/pw2/res2_cf_phone.json
$PY -u counterfactual.py 8444 desktop 3 $SP/speed/pw2/res2_cf_desktop.json
VARIANTS=baseline,both $PY -u counterfactual.py 8443 phone 5 $SP/speed/pw2/res2_cf_phone_both.json
cd ../pw2
$PY -u coverage_probe.py 8444 coverage.json
$PY -u css_cf.py 8443 3 res2_css_cf_phone.json
$PY -u thirdparty_probe.py 8443 thirdparty.json
$PY payload_probe.py                                  # offline: compression alternatives, first-page size, unused fields
$PY metric_reset_probe.py; $PY debug_back.py          # the two probes behind section 1.4
sh run_suite.sh; sh run_suite2.sh; sh run_suite3.sh; sh run_suite4.sh   # the batches, as run
sh make_tables.sh                                     # every table in this report comes from tables/*.md
```

### 9.4 Code read (read-only)

`frontend/static/nav_perf.js` (prefetch policy), `frontend/static/main.js` lines 108-351 (facet cascade),
370-470 (lazy facet lists), 1702-1747 (render after search), 1995-2138 (the cars fetch, ETag and partial
refetch), `frontend/static/listings_boot.js` (JSON islands), `backend/config.py` (`redis_url`).
All read at `main` = `0fd91872b`, the live release 1.5.8.

### 9.5 Re-verification run of 2026-11-10 (section 10)

```
# live: 35 requests through the same wrapper (189 -> 224 of 300; the limit for this run was 40)
sh   $SP/speed/live/rv_live.sh > $SP/speed/live/rv_live.log     # 34 requests; labels rv_a_* rv_b_* rv_c1..3 rv_d
LIVE_INM='W/"lc-28105-50-54954f531adfafc9bff0"' python3 $SP/speed/live/live.py one rv_e_api_inm "https://sarraficars.com/api/listings/cars?zip=28105&radius=50" json
python3 $SP/speed/live/live.py count                            # 224
python3 $SP/speed/live/rv_live_table.py compare                 # section 10.2
python3 $SP/speed/live/rv_live_table.py perrun                  # Appendix D
python3 $SP/speed/live/rv_origin_time.py                        # pooled origin time per endpoint, section 2
python3 $SP/speed/live/summarize_idle.py rv_a rv_b              # section 2.2, fifth pass
python3 $SP/speed/live/live_stats.py                            # DYNAMIC counts, TTFB > 1 s, handshake medians
python3 $SP/speed/live/analyze_curl.py > $SP/speed/live/analyze_curl_out_rv.txt   # regenerates Appendix A/B and the section 2 medians
# browser: local only, same replay servers, 3 runs per cell
sh $SP/speed/pw2/rv_browser.sh         # A phone flow as served / nav_perf.js empty (interleaved), B pages, C desktop flow,
                                       # D bfcache off, E API wait 1,529 ms (replay 8470), F in-browser what-ifs
sh $SP/speed/pw2/rv_browser2.sh        # G edge-cache A/B (5 interleaved pairs), H CSS what-if: baseline, all rules in one file,
                                       # and the first (invalid) used-rules build, 2 rounds x 3 runs
cd $SP/speed/replay      # replay servers used by the re-run (the first run started 8443-8446, 8448-8453 and the CSS ones)
RTT_MS=150 HANDSHAKE_MS=300 node server.mjs manifest_warm.json 8470 log_phone_apimiss.jsonl &   # step E; rv_browser.sh sets the API wait to 1,529 ms
# edge-cache what-if: phone replay 8452 (desktop 8453) with the six overrides the first run set; the replies list them
for k in '%2F' '%2Flistings' '%2Fdealership%2Ftownandcountrytoyota-com' '%2Fapi%2Flistings%2Ffilter-options' \
         '%2Fapi%2Flistings%2Fcars%3Fzip%3D28105%26radius%3D50' '%2Flistings%3Fzip_code%3D28105%26radius%3D50'; do
  curl -sk "https://127.0.0.1:8452/__replay/set?key=$k&wait_ms=15"; done
# CSS what-if, corrected build: one stylesheet per page with the rules used at 390 px, each inside its @media chain (ports 8480-8482)
$PY $SP/speed/pw2/build_css_manifests_v2.py
for p in "home 8480" "car 8481" "listings 8482"; do set -- $p; RTT_MS=150 HANDSHAKE_MS=300 node server.mjs manifest_css_used2_$1.json $2 log_css_$2.jsonl & done   # bash/sh
sh $SP/speed/pw2/rv_browser3.sh        # baseline and used-rules variants interleaved, 2 rounds x 3 runs
cd $SP/speed/pw2
$PY css_whatif_table.py > tables/css_cf2.md      # timings of all CSS variants + screenshot pixel check
sh make_rv_tables.sh                   # merges the interleaved single-run files (rv_compare.py merge), then writes tables/rv_flows.md,
                                       # rv_apimiss.md, rv_pages.md, rv_cf_phone.md (aggregate2.py cf) and edge_ab.md (aggregate_edge_ab.py)
$PY dom_facets_probe.py 8443 rv_dom_facets.json    # how many filter options the listings page grafts into the DOM
$PY verify_numbers.py                  # recompute the headline numbers from the raw result files
# cross-checks (read only)
git show phase/N37a:docs/SCANNING_OPS_LOG.md | sed -n '58p;949p'
sed -n '2003,2011p;2037,2062p' frontend/static/main.js
```


## 10. Re-verification, 2026-11-10 (resumed run)

The first run was cut off by a network loss before this report was assembled (the draft in this file still held
eight unfilled placeholders: the ranked causes and seven other sections). The resumed run did not redo the campaign. It re-checked the
numbers the ranking rests on in four ways: **35 further live requests** (`live.py` count 189 to 224 of 300; the
limit for this run was 40), **local browser re-runs** against the same replay, a **recomputation of every table
from the raw files**, and a check of the **cross-references and code lines** quoted in this report. It also ran the
two what-ifs the first run had planned and not finished (the interleaved edge-cache A/B, section 5.4, and the CSS
what-if served by the replay, section 5.3).

### 10.1 What was re-checked and what came out

| # | claim (where it is used) | first run, 2026-11-09 | re-run, 2026-11-10 | verdict |
|---|---|---|---|---|
| 1 | the live release | 1.5.8, commit `0fd91872` | `GET /health`: `{"commit":"0fd91872bedb07a30ecdd7e19773db5fdc53b1a7","status":"ok","version":"1.5.8"}` | unchanged |
| 2 | production has no Redis | `redis.md` 3.1 row 1 | `GET /api/ready`: `{"checks":{"db":"ok","pgvector":"skipped","redis":"skipped"},"ready":true}` | unchanged |
| 3 | nothing dynamic is cached at the edge | `DYNAMIC` on 102 of 102 HTML and API responses | `DYNAMIC` on 125 of 125 (98 HTML + 27 API); static CSS/JS `HIT` | unchanged |
| 4 | the fixed cost of a request (`/health` TTFB, warm connection) | 147-176 ms | 120-133 ms (3 samples; origin part 73-82 ms) | same order; the home LAN is faster than the cellular link |
| 5 | warm TTFB of the four pages | home 156-184, listings 293-534, car 340-406, dealership 234-512 ms | home 149-191, listings 295-443, car 341-347, dealership 199-322 ms (3 samples each) | inside the first run's ranges |
| 6 | payload sizes | listings 189,535 B br / 1,548,917 B; API 1,585,587 B gz / 14,546,046 B | listings 189,460-189,951 B / 1,548,917 B; API 1,585,587 B / 14,546,046 B | unchanged (the API's ETag is new: new UTC day) |
| 7 | the car page stalls on the first request after a quiet spell | 1.85-3.38 s of origin time (4 car-page stalls and 1 home-page stall in the first run) | **2,090 ms** on the first request after 23 quiet hours, 237 ms on the repeat | reproduced |
| 8 | the first search in an area costs about 5 s | 5,064 ms (28105), 5,530 ms (37421) | **1,635 ms** (28105), 202 ms on the repeat | **not reproduced**; the miss costs 1.6-5.5 s here (13.4 s for Irvine in `server-causes.md`) |
| 9 | revalidation works end to end | 304, 0 B, 262 ms | 304, 0 B, 392 ms | unchanged |
| 10 | static files: `public, max-age=31536000, immutable`, br, ETag, HIT | 69 HIT of 75 (4 MISS on first fetch, 2 DYNAMIC) | CSS 8,993 B br and JS 21,956 B br, both HIT | unchanged |
| 11 | `/api/listings/filter-options` has no cache headers | no `Cache-Control`, no ETag, 86,107 B gz / 574,227 B | same | unchanged |
| 12 | every HTML response sets a session cookie and a fresh CSP nonce | headers read | 4 home responses: 4 different nonces, each repeated in the 3 `<script nonce>` tags of that body; `Set-Cookie: session=...; Secure; HttpOnly; Path=/; SameSite=Lax` on all | unchanged |
| 13 | phone page loads, usable (cold) home / listings / listings with ZIP / car / dealership | 932 / 1,456 / 4,183 / 1,211 / 1,540 ms (5 runs) | 895 / 1,429 / 4,224 / 1,204 / 1,524 ms (3 runs) | within 4 % |
| 14 | phone flow: tap Listings / search / car from results / Back | 1,561 / 2,743 / 253 / 148 ms (5 runs) | 1,570 / 2,734 / 258 / 150 ms (3 runs) | within 2 % |
| 15 | the prefetch script costs a second download of `/listings` (phone) | 1,111 ms without it, car tap 792 ms | 1,097 ms without it (3 runs, interleaved with the as-served runs), car tap 786 ms | reproduced |
| 16 | Back without the bfcache | 2,054 ms (2,048-2,375) | 2,282 ms (2,186-2,611) | reproduced: 2.0-2.6 s |
| 17 | desktop flow: tap Listings / search / car / Back | 787 / 556 / 495 / 63 ms | 781 / 569 / 499 / 64 ms | within 2 % |
| 18 | what-ifs, phone search: facets on demand / first 24 cars / both | 2,061 / 1,007 / 911 ms (0 long tasks) | 2,108 / 1,267 / 915 ms (0 long tasks) | reproduced; the paged-only run is 1.0-1.3 s |
| 19 | the search with a cold area (phone) | 7,548 ms with the morning's 4.9 s miss | **4,230 ms** with this run's 1.5 s miss (4,195-4,347) | new: 4.2-7.5 s depending on the miss |
| 20 | the edge-cache what-if | one baseline against one edge run, taken hours apart | interleaved A/B, 5 pairs (section 5.4) | see 5.4 |
| 21 | the CSS what-if | route-based, biased, discarded | served by the replay, 2 rounds x 3 runs (section 5.3) | see 5.3 |

### 10.2 Live: the 35 requests

`rv_live.sh` (via `live.py`, GET only, at least 1.15 s apart): 1 `/health`, then the first hit of car, dealership,
home, listings and the API on fresh connections, an immediate repeat of each, 3 warm batches of 6 transfers on one
HTTP/2 connection, one batch of static CSS, static JS, the facet list and `/api/ready`, and 1 conditional GET.
Medians are in section 2; per-transfer values are in Appendix D. TTFB in ms:

| endpoint | first hit of this run, TTFB (ms) | immediate repeat, TTFB | warm (reused connection) TTFB, 3 samples | cfOrigin, first hit and repeat | cfOrigin, warm samples |
|---|---|---|---|---|---|
| health | 213 | 219 | 133, 121, 120 | - | 82, 73, 74 |
| home | 260 | 210 | 171, 191, 149 | 97, 106 | 126, 118, 108 |
| listings | 409 | 457 | 443, 295, 437 | 199, 235 | 362, 222, 367 |
| car | 2253 | 417 | 341, 346, 347 | 2090, 237 | 269, 300, 303 |
| dealership | 515 | 298 | 322, 199, 273 | 365, 150 | 248, 146, 239 |
| api_cars | 1635 | 202 | - | - | - |


The first hit of the run is the first request this agent made in about 23 hours. Only the car page and the API
paid for it (section 2.2). The `colo` column of Appendix D shows ATL and IAD (the first run's morning samples
came through MIA).

### 10.3 Browser: the same flows, re-run

Same replay, same harness (`pw2/measure2.py`), 3 runs per cell (the first run used 5 for most cells), machine
load average 5-11 during the flows and page loads (up to 19 during the CSS batches; the first run: 5-40). Medians with the range of "usable"; changes under about 50 ms on a step of
about 250 ms and under about 100 ms on the desktop's direct loads are noise (section 8).

| flow | step | 2026-11-09: median (range), n | 2026-11-10 re-run: median (range), n | change |
|---|---|---|---|---|
| as served | home:cold | 942 (883-1061), n=5 | 1165 (941-1171), n=3 | +223 ms (+24 %) |
| as served | home->listings | 1561 (1556-1565), n=5 | 1570 (1555-1582), n=3 | +9 ms (+1 %) |
| as served | listings:search | 2743 (2645-2912), n=5 | 2734 (2721-2902), n=3 | -9 ms (-0 %) |
| as served | results->car | 253 (238-291), n=5 | 258 (249-292), n=3 | +5 ms (+2 %) |
| as served | car->back | 148 (132-172), n=5 | 150 (148-152), n=3 | +2 ms (+1 %) |
| as served | results->car(2nd) | 253 (233-302), n=5 | 280 (278-284), n=3 | +27 ms (+11 %) |
| nav_perf.js served empty | home:cold | 942 (883-954), n=5 | 946 (928-1164), n=3 | +4 ms (+0 %) |
| nav_perf.js served empty | home->listings | 1111 (1091-2076), n=5 | 1097 (1087-1102), n=3 | -14 ms (-1 %) |
| nav_perf.js served empty | listings:search | 2784 (2732-2855), n=5 | 2784 (2674-2806), n=3 | +0 ms (+0 %) |
| nav_perf.js served empty | results->car | 792 (743-802), n=5 | 786 (767-786), n=3 | -6 ms (-1 %) |
| nav_perf.js served empty | car->back | 148 (130-171), n=5 | 144 (138-151), n=3 | -4 ms (-3 %) |
| nav_perf.js served empty | results->car(2nd) | 302 (262-323), n=5 | 251 (238-279), n=3 | -51 ms (-17 %) |
| bfcache off | home:cold | 944 (933-1163), n=3 | 935 (928-1065), n=3 | -9 ms (-1 %) |
| bfcache off | home->listings | 1561 (1554-1587), n=3 | 1589 (1573-1608), n=3 | +28 ms (+2 %) |
| bfcache off | listings:search | 2761 (2683-2789), n=3 | 2923 (2904-3005), n=3 | +162 ms (+6 %) |
| bfcache off | results->car | 229 (218-252), n=3 | 264 (261-298), n=3 | +35 ms (+15 %) |
| bfcache off | car->back | 2054 (2048-2375), n=3 | 2282 (2186-2611), n=3 | +228 ms (+11 %) |
| bfcache off | results->car(2nd) | 795 (785-806), n=3 | 811 (799-862), n=3 | +16 ms (+2 %) |
| desktop, as served | home:cold | 431 (426-478), n=3 | 440 (409-451), n=3 | +9 ms (+2 %) |
| desktop, as served | home->listings | 787 (775-787), n=3 | 781 (775-782), n=3 | -6 ms (-1 %) |
| desktop, as served | listings:search | 556 (476-564), n=3 | 569 (566-584), n=3 | +13 ms (+2 %) |
| desktop, as served | results->car | 495 (490-503), n=3 | 499 (494-508), n=3 | +4 ms (+1 %) |
| desktop, as served | car->back | 63 (57-69), n=3 | 64 (63-69), n=3 | +1 ms (+2 %) |
| desktop, as served | results->car(2nd) | 94 (89-98), n=3 | 96 (82-100), n=3 | +2 ms (+2 %) |
| desktop, as served | car->home(logo) | 429 (428-434), n=3 | 429 (426-436), n=3 | +0 ms (+0 %) |


Page loads (cold cache, usable with the range; TTFB, FCP and LCP are medians):

| profile | load (cold) | usable 11-09 | usable 11-10 | TTFB 11-09 | TTFB 11-10 | FCP 11-09 | FCP 11-10 | LCP 11-09 | LCP 11-10 |
|---|---|---|---|---|---|---|---|---|---|
| phone | home | 932 (928-947) | 895 (864-939) | 608 | 608 | 988 | 988 | 996 | 996 |
| phone | listings | 1456 (1431-1496) | 1429 (1424-1438) | 774 | 775 | 1160 | 1160 | 1480 | 1452 |
| phone | listings_zip | 4183 (4041-4520) | 4224 (4035-4353) | 861 | 861 | 1248 | 1240 | 4544 | 4220 |
| phone | car | 1211 (1200-1252) | 1204 (1202-1207) | 828 | 829 | 1268 | 1264 | 1268 | 1264 |
| phone | dealership | 1540 (1519-2524) | 1524 (1520-1568) | 689 | 688 | 1064 | 1072 | 1796 | 1772 |
| desktop | home | 439 (413-488) | 408 (392-420) | 279 | 279 | 496 | 488 | 496 | 496 |
| desktop | listings | 684 (668-704) | 630 (626-685) | 446 | 447 | 680 | 672 | 704 | 676 |
| desktop | listings_zip | 1355 (1346-1928) | 1355 (1345-1370) | 533 | 532 | 760 | 764 | 1380 | 1372 |
| desktop | car | 669 (635-704) | 650 (632-659) | 500 | 500 | 716 | 716 | 732 | 724 |
| desktop | dealership | 579 (578-584) | 581 (580-581) | 360 | 360 | 588 | 588 | 1112 | 768 |


The search with the area body missing from the server cache (the replay's wait for the API changed; nothing else):

| search on the phone, tap to usable | median (range), n |
|---|---|
| area body in the server cache (API wait 150 ms), re-run | 2734 (2721-2902), n=3 |
| API wait 1,529 ms (this session's first-hit sample) | 4230 (4195-4347), n=3 |
| API wait 4,914 ms (the 2026-11-09 morning's first-hit sample) | 7548 (7486-7743), n=3 |


The in-browser what-ifs, phone, 3 runs (first run: `cf_phone.md` and `cf_both.md` in section 5.2):


| variant (phone) | listings usable | listings DOM nodes | listings ScriptDuration | listings long tasks total | listings bytes same-origin | search click->results visible | search click->usable | search long tasks (n / total / max) | search ScriptDuration | search bytes same-origin |
|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 1424 (1399-1476) | 69,176 | 152 | 147 | 448,114 | 2789 (2772-2861) | 2789 | 5 / 870 / 237 | 587 | 1,609,637 |
| no_facets | 1499 (1473-1524) | 13,435 | 153 | 201 | 361,572 | 2108 (2099-2125) | 2108 | 2 / 209 / 119 | 89 | 1,610,028 |
| paged_api | 1526 (1437-1535) | 69,176 | 147 | 203 | 448,081 | 1267 (1169-1272) | 1267 | 4 / 689 / 240 | 575 | 49,476 |
| both | 1415 (1408-1433) | 13,435 | 143 | 146 | 361,572 | 915 (897-989) | 915 | 0 / 0 / 0 | 68 | 49,483 |


### 10.4 What the recomputation from raw files showed

* **Appendix A is identical to the raw log.** `analyze_curl.py` regenerated the 52 table lines of the per-run
  table from `requests.jsonl`; 0 lines differ from the stored table, and the median table of section 2
  (home 298 [281-3,017] and the rest) matches line for line.
* **Appendix B differs in 5 rows,** and only in the TTFB or `cf-cache-status` of a file that was fetched again later
  (`favicon.svg` and the font were `MISS` on the first fetch and `HIT` on the re-check; the car CSS, `main.js` and the
  web manifest have a later sample from the asset repeats and from this run's `rv_d` batch). The stored table keeps
  the first sample, as Appendix B says.
* **`verify_numbers.py` reproduces the headline browser numbers** from the raw result files, independent of the
  table generator: phone usable 932 / 1,456 / 4,183 / 1,211 / 1,540 ms, desktop 439 / 684 / 1,355 / 669 / 579 ms,
  phone flow 1,561 / 2,743 / 253 / 148 ms, without `nav_perf.js` 1,111 / 2,784 / 792 / 148 ms, bfcache off 2,054 ms,
  cold origin 4,323 / 7,548 ms, desktop without the cookie rewrite 98 ms.
* **The facet lists.** The "about 7,300 entries" of `main.js:371-372` is a code comment. Counted on the page
  (`pw2/dom_facets_probe.py`, desktop viewport, 3 s after load): with the `/api/listings/filter-options` response the
  listings page holds **46,803 elements and 15,462 option checkboxes**; with that request aborted it holds 4,905
  elements and 1,496. The difference is **13,966 option labels** (1,913 models, 12,013 trims, 40 packages), three
  elements each (label, input, span: 41,898 elements). The lists are built into one container each (`options-model`,
  `options-trim`, `options-package`); the `acc-options-*` containers that `hydrateLazyFacet` also looks for are not in
  the page. So the first run's "about 7,300 entries in two containers per list", taken from that comment and a reading
  of the code, was wrong: the comment predates the data, and the page has 13,966 entries in one container each.
  The measured costs (621 ms per search, 55,740 CDP nodes) are unchanged; only the explanation is corrected.

### 10.5 Cross-references and code lines checked

* `server-causes.md`: the 79 ms floor and 20-38 ms connection cost (section 2, rank 3 and 4; section 3.1), Irvine
  13,447 / 4,439 / 2,887 / 1,287 ms (rank 2), 34 car pages per second and p50 468 ms (section 6), 48-50 ms of
  SQLCipher per open (rank 9) and the edge-cache table (section 7) are all there as quoted.
* The region: `git show phase/N37a:docs/SCANNING_OPS_LOG.md` lines 58 and 949 read `us-west2` (snapshot of 2026-10-05).
* The local timings of `runtime-timing.md` used as the "locally" column: `/` 3.6 ms, `/listings` 37.5 ms, car 42.2 ms,
  dealership 33.5 ms, `/health` 0.3 ms (section 2b of that file).
* Code, read at `main` = `0fd91872b`: `frontend/static/main.js` 167 (`runCascade`), 371-372 (the "~7,300 entries"
  comment), 2003-2011 (`If-None-Match` only from `window.__DS_listingsCarsEtag`), 2037-2062 (up to 6 refetches of a
  partial body); `frontend/static/nav_perf.js` (`HOVER_DELAY_MS = 65`, `MAX_PER_PAGE = 12`, `observeCarLinks` limit 8,
  `opts.priority = "low"`, `touchstart` and `mouseover` intent; its own header says car pages are
  `Cache-Control: private, max-age=180` and that hovering a `/listings` link prefetches the document);
  `backend/routes/cars_pages.py:219` (`private, max-age=180`); `backend/config.py:96` (`redis_url`, "Optional Redis
  (readiness probe)"); `backend/tests/test_health_ready.py` (`/api/ready` reports `redis` `skipped` when unset).
* `git rev-parse` and `VERSION` still read `0fd91872b` and `1.5.8` on `main`; the staged releases on
  `phase/N37a` (`faea2b985`) were not measured (section 8).

### 10.6 Corrections made while verifying

| what the first run's text said | what it says now | why |
|---|---|---|
| desktop tap to car "499 ms without the script" (section 4.4) | 495 ms without it (3 runs: 490-507); the re-run with the script gave 499 ms | the raw file and its table say 495 ms; 499 ms is the re-run with the script |
| "142 of 300 requests used" (old draft) | 224 of 300 (189 + 35) | the draft was written before the later passes; the log count is the source |
| "Cloudflare answers DYNAMIC on 102 of 102" | 125 of 125 | the re-run added 23 HTML and API responses |
| app time listings 152, car 222, dealership 101 ms | listings +238, car +250, dealership +146, home +41 ms | the floor is now `/health`'s 76 ms (6 samples), not the home page's lowest `cfOrigin`; pooled over all samples |
| "the origin's region is not known" | ops log says `us-west2` (a snapshot, not read live) | found while cross-checking `server-causes.md` |
| the first search in an area costs "about 5 s" | 1.6-5.5 s (13.4 s in the server report) | the re-run's first hit took 1.6 s |
| "Back costs 2.05 s" | 2.0-2.6 s | the re-run gave 2.28 s |
| cause 1 effort "M" | "M-L" | the what-if cuts the body to 24 cars, which also drops the facet counts the page computes in the browser; a real fix has to keep them correct |
| "the car page was cold in 3 of 4 quiet-spell rounds" | 4 of 5 | the re-run's first hit was cold too |
| `home:cold` step "942 ms" | 942-1,165 ms between batches | one re-run batch measured 1,165 ms (941-1,171) on the same step; the page-load table agrees with 932 (re-run 895 ms) |
| "about 7,300 grafted filter options, in two containers per list" (sections 5.2, short answer) | 13,966 options (1,913 models, 12,013 trims, 40 packages), one container per list | counted in the page by `dom_facets_probe.py`; the 7,300 was the `main.js:371` comment, which predates the data |
| the first CSS what-if (Playwright routes) | replaced by replay-served variants (section 5.3), and the used-rules variant was rebuilt twice | a route that changed nothing (`route_ctl`) already moved first paint by +176, +144 and +848 ms (home, car, listings), more than any variant; the first used-rules builds rendered 15-42 % of pixels differently (section 5.3) |

## Appendix A. Every curl run behind section 2 (morning samples, T-Mobile link)

Columns: DNS = `time_namelookup`; TCP = `time_connect - time_namelookup`; TLS = `time_appconnect - time_connect`;
TTFB = `time_starttransfer` and total = `time_total` (both from the start); wait = TTFB minus `time_pretransfer`;
cfOrigin from `Server-Timing`; wire = `size_download` (compressed); decoded = size after br/gzip; http =
`http_version`. "conn new" = a fresh connection (DNS, TCP, TLS paid); "reused" = a transfer on the
connection opened by warm 1.

| endpoint | run | conn | DNS | TCP | TLS | TTFB | total | wait (TTFB-pretransfer) | cfOrigin | wire B | decoded B | http |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| home | cold-ish 1 | new | 2 | 31 | 52 | 3017 | 3019 | 2932 | 2880 | 4,205 | 15,701 | h2 |
| home | cold-ish 2 | new | 2 | 30 | 54 | 281 | 283 | 195 | 109 | 4,202 | 15,701 | h2 |
| home | cold-ish 3 | new | 2 | 29 | 50 | 298 | 298 | 218 | 113 | 4,203 | 15,701 | h2 |
| home | cold-ish 4 | new | 2 | 42 | 69 | 333 | 333 | 220 | 148 | 4,204 | 15,701 | h2 |
| home | cold-ish 5 | new | 2 | 41 | 59 | 293 | 293 | 191 | 106 | 4,205 | 15,701 | h2 |
| home | warm 1 | new | 2 | 28 | 56 | 289 | 291 | 203 | 139 | 4,207 | 15,701 | h2 |
| home | warm 2 | reused | 0 | 0 | 0 | 156 | 157 | 156 | 105 | 4,204 | 15,701 | h2 |
| home | warm 3 | reused | 0 | 0 | 0 | 158 | 158 | 158 | 104 | 4,205 | 15,701 | h2 |
| home | warm 4 | reused | 0 | 0 | 0 | 184 | 184 | 184 | 139 | 4,204 | 15,701 | h2 |
| home | warm 5 | reused | 0 | 0 | 0 | 182 | 182 | 182 | 109 | 4,203 | 15,701 | h2 |
| listings | cold-ish 1 | new | 2 | 28 | 59 | 831 | 1381 | 742 | 604 | 189,535 | 1,548,917 | h2 |
| listings | cold-ish 2 | new | 24 | 33 | 58 | 456 | 934 | 342 | 246 | 189,533 | 1,548,917 | h2 |
| listings | cold-ish 3 | new | 2 | 36 | 59 | 420 | 915 | 323 | 219 | 189,796 | 1,548,917 | h2 |
| listings | cold-ish 4 | new | 2 | 31 | 56 | 665 | 1149 | 577 | 474 | 189,548 | 1,548,917 | h2 |
| listings | cold-ish 5 | new | 2 | 30 | 58 | 454 | 993 | 363 | 255 | 189,532 | 1,548,917 | h2 |
| listings | warm 1 | new | 2 | 33 | 62 | 451 | 933 | 355 | 257 | 189,532 | 1,548,917 | h2 |
| listings | warm 2 | reused | 0 | 0 | 0 | 534 | 894 | 534 | 420 | 189,530 | 1,548,917 | h2 |
| listings | warm 3 | reused | 0 | 0 | 0 | 346 | 643 | 346 | 203 | 189,544 | 1,548,917 | h2 |
| listings | warm 4 | reused | 0 | 0 | 0 | 501 | 775 | 501 | 446 | 189,803 | 1,548,917 | h2 |
| listings | warm 5 | reused | 0 | 0 | 0 | 293 | 638 | 293 | 218 | 189,499 | 1,548,917 | h2 |
| listings data API | cold-ish 1 | new | 3 | 38 | 71 | 5064 | 5558 | 4952 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | cold-ish 2 | new | 2 | 57 | 52 | 322 | 2583 | 210 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | cold-ish 3 | new | 2 | 35 | 46 | 267 | 2798 | 185 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | cold-ish 4 | new | 2 | 44 | 76 | 498 | 2767 | 375 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | cold-ish 5 | new | 2 | 38 | 78 | 283 | 2574 | 165 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | warm 1 | new | 3 | 28 | 52 | 416 | 2522 | 334 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | warm 2 | reused | 0 | 0 | 0 | 189 | 1010 | 188 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | warm 3 | reused | 0 | 0 | 0 | 135 | 696 | 134 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | warm 4 | reused | 0 | 0 | 0 | 151 | 580 | 151 | None | 1,585,587 | 14,546,046 | h2 |
| listings data API | warm 5 | reused | 0 | 0 | 0 | 123 | 509 | 123 | None | 1,585,587 | 14,546,046 | h2 |
| car page | cold-ish 1 | new | 2 | 29 | 58 | 503 | 503 | 413 | 335 | 10,451 | 55,720 | h2 |
| car page | cold-ish 2 | new | 2 | 29 | 45 | 3536 | 3540 | 3459 | 3382 | 10,451 | 55,720 | h2 |
| car page | cold-ish 3 | new | 2 | 40 | 54 | 551 | 551 | 454 | 345 | 10,452 | 55,720 | h2 |
| car page | cold-ish 4 | new | 3 | 31 | 67 | 514 | 514 | 414 | 337 | 10,449 | 55,720 | h2 |
| car page | cold-ish 5 | new | 2 | 50 | 46 | 525 | 527 | 427 | 356 | 10,451 | 55,720 | h2 |
| car page | warm 1 | new | 2 | 25 | 58 | 442 | 442 | 356 | 278 | 10,453 | 55,720 | h2 |
| car page | warm 2 | reused | 0 | 0 | 0 | 406 | 407 | 406 | 314 | 10,450 | 55,720 | h2 |
| car page | warm 3 | reused | 0 | 0 | 0 | 360 | 364 | 360 | 318 | 10,449 | 55,720 | h2 |
| car page | warm 4 | reused | 0 | 0 | 0 | 340 | 352 | 340 | 283 | 10,452 | 55,720 | h2 |
| car page | warm 5 | reused | 0 | 0 | 0 | 362 | 368 | 362 | 281 | 10,450 | 55,720 | h2 |
| dealership page | cold-ish 1 | new | 2 | 31 | 58 | 383 | 414 | 292 | 222 | 12,676 | 90,694 | h2 |
| dealership page | cold-ish 2 | new | 2 | 26 | 90 | 451 | 484 | 334 | 235 | 12,680 | 90,694 | h2 |
| dealership page | cold-ish 3 | new | 2 | 50 | 54 | 369 | 397 | 263 | 190 | 12,679 | 90,694 | h2 |
| dealership page | cold-ish 4 | new | 2 | 31 | 62 | 364 | 393 | 268 | 199 | 12,678 | 90,694 | h2 |
| dealership page | cold-ish 5 | new | 2 | 35 | 68 | 380 | 416 | 274 | 211 | 12,678 | 90,694 | h2 |
| dealership page | warm 1 | new | 2 | 28 | 48 | 363 | 400 | 284 | 213 | 12,679 | 90,694 | h2 |
| dealership page | warm 2 | reused | 0 | 0 | 0 | 234 | 275 | 234 | 169 | 12,678 | 90,694 | h2 |
| dealership page | warm 3 | reused | 0 | 0 | 0 | 512 | 543 | 512 | 468 | 12,678 | 90,694 | h2 |
| dealership page | warm 4 | reused | 0 | 0 | 0 | 242 | 267 | 242 | 172 | 12,675 | 90,694 | h2 |
| dealership page | warm 5 | reused | 0 | 0 | 0 | 251 | 282 | 251 | 182 | 12,678 | 90,694 | h2 |


## Appendix B. Every CSS, JS, font and image file the four pages load (one fetch each)

52 files x 10 runs would be 520 requests, more than the 300-request limit, so each file was fetched once
(cold-ish, one connection per batch) and two representative files were repeated 5 times cold-ish and 5 times
warm (section 2.3). Columns: wire = bytes on the wire, decoded = after decompression, enc = `Content-Encoding`,
TTFB in ms on that batch's connection.

| asset | wire B | decoded B | enc | Cache-Control | ETag | cf-cache-status | TTFB ms |
|---|---|---|---|---|---|---|---|
| `/static/css/00-base.css` | 1,558 | 4,226 | br | public, max-age=31536000, immutable | yes | HIT | 190 |
| `/static/css/01-nav.css` | 2,833 | 16,809 | br | public, max-age=31536000, immutable | yes | HIT | 82 |
| `/static/css/02-listings.css` | 5,629 | 33,374 | br | public, max-age=31536000, immutable | yes | HIT | 78 |
| `/static/css/03-car-page.css` | 8,993 | 53,548 | br | public, max-age=31536000, immutable | yes | HIT | 64 |
| `/static/css/04-dashboard.css` | 3,149 | 16,863 | br | public, max-age=31536000, immutable | yes | HIT | 60 |
| `/static/css/05-car-tools.css` | 4,060 | 23,297 | br | public, max-age=31536000, immutable | yes | HIT | 72 |
| `/static/css/06-dev.css` | 4,627 | 27,686 | br | public, max-age=31536000, immutable | yes | HIT | 56 |
| `/static/css/07-dealer-admin.css` | 3,839 | 21,801 | br | public, max-age=31536000, immutable | yes | HIT | 65 |
| `/static/css/08-marketing.css` | 883 | 2,933 | br | public, max-age=31536000, immutable | yes | HIT | 52 |
| `/static/css/09-components.css` | 4,021 | 24,506 | br | public, max-age=31536000, immutable | yes | HIT | 63 |
| `/static/css/10-pages.css` | 2,772 | 16,642 | br | public, max-age=31536000, immutable | yes | HIT | 45 |
| `/static/css/11-overrides.css` | 2,739 | 13,218 | br | public, max-age=31536000, immutable | yes | HIT | 82 |
| `/static/css/12-viewers.css` | 3,038 | 15,554 | br | public, max-age=31536000, immutable | yes | HIT | 57 |
| `/static/css/13-landing-auth.css` | 1,772 | 7,951 | br | public, max-age=31536000, immutable | yes | HIT | 84 |
| `/static/css/14-vdp-uniform.css` | 1,731 | 6,749 | br | public, max-age=31536000, immutable | yes | HIT | 68 |
| `/static/favicon.svg` | 312 | 441 | gzip | public, max-age=14400 | yes | MISS | 217 |
| `/static/brand/favicon.ico` | 1,647 | 1,654 | br | public, max-age=14400 | yes | MISS | 201 |
| `/static/brand/apple-touch-icon.png` | 4,973 | 4,973 | none | public, max-age=14400 | yes | MISS | 149 |
| `/static/brand/site.webmanifest` | 204 | 333 | gzip | public, max-age=3600 | yes | DYNAMIC | 134 |
| `/static/fonts/inter-latin-v20.woff2` | 48,432 | 48,432 | none | public, max-age=31536000, immutable | yes | MISS | 188 |
| `/static/nav_perf.js` | 2,991 | 10,621 | br | public, max-age=31536000, immutable | yes | HIT | 54 |
| `/static/compare.js` | 3,605 | 16,537 | br | public, max-age=31536000, immutable | yes | HIT | 44 |
| `/static/sidebar.js` | 780 | 2,990 | br | public, max-age=31536000, immutable | yes | HIT | 47 |
| `/static/ds_core.js` | 1,144 | 3,118 | br | public, max-age=31536000, immutable | yes | HIT | 98 |
| `/static/sc-helpers.js` | 2,420 | 8,849 | br | public, max-age=31536000, immutable | yes | HIT | 55 |
| `/static/listings_boot.js` | 1,194 | 3,792 | br | public, max-age=31536000, immutable | yes | HIT | 73 |
| `/static/market_intel.js` | 1,926 | 6,828 | br | public, max-age=31536000, immutable | yes | HIT | 64 |
| `/static/geo.js` | 2,376 | 9,389 | br | public, max-age=31536000, immutable | yes | HIT | 73 |
| `/static/listings/card.js` | 3,570 | 12,151 | br | public, max-age=31536000, immutable | yes | HIT | 73 |
| `/static/listings/filters.js` | 3,310 | 13,732 | br | public, max-age=31536000, immutable | yes | HIT | 63 |
| `/static/listings/sort.js` | 1,448 | 5,177 | br | public, max-age=31536000, immutable | yes | HIT | 46 |
| `/static/listings/facets.js` | 1,536 | 5,897 | br | public, max-age=31536000, immutable | yes | HIT | 53 |
| `/static/listings/zip_state.js` | 1,338 | 3,819 | br | public, max-age=31536000, immutable | yes | HIT | 55 |
| `/static/listings/chips.js` | 1,419 | 4,403 | br | public, max-age=31536000, immutable | yes | HIT | 49 |
| `/static/main.js` | 21,956 | 119,771 | br | public, max-age=31536000, immutable | yes | HIT | 51 |
| `/static/listings.js` | 3,712 | 16,524 | br | public, max-age=31536000, immutable | yes | HIT | 73 |
| `/static/car_page_helpers.js` | 1,887 | 7,923 | br | public, max-age=31536000, immutable | yes | HIT | 145 |
| `/static/car/cp_canvas.js` | 1,606 | 5,248 | br | public, max-age=31536000, immutable | yes | HIT | 70 |
| `/static/car/car_tco_math.js` | 2,666 | 11,661 | br | public, max-age=31536000, immutable | yes | HIT | 54 |
| `/static/car/car_tco.js` | 4,777 | 26,083 | br | public, max-age=31536000, immutable | yes | HIT | 62 |
| `/static/car/car_finance.js` | 4,309 | 20,332 | br | public, max-age=31536000, immutable | yes | HIT | 63 |
| `/static/car/car_gallery.js` | 3,876 | 17,829 | br | public, max-age=31536000, immutable | yes | HIT | 57 |
| `/static/ds_comments.js` | 4,851 | 19,317 | br | public, max-age=31536000, immutable | yes | HIT | 57 |
| `/static/car_page.js` | 7,765 | 36,062 | br | public, max-age=31536000, immutable | yes | HIT | 81 |
| `/static/vendor/leaflet/leaflet.css` | 2,967 | 14,806 | br | public, max-age=31536000, immutable | yes | HIT | 37 |
| `/static/dealership_page.js` | 1,804 | 6,392 | br | public, max-age=31536000, immutable | yes | HIT | 65 |
| `/static/vendor/leaflet/leaflet.js` | 36,938 | 147,552 | br | public, max-age=31536000, immutable | yes | HIT | 52 |
| `/static/map_tiles.js` | 910 | 2,669 | br | public, max-age=31536000, immutable | yes | HIT | 37 |
| `/api/listings/filter-options` | 86,107 | 574,227 | gzip | (none) | no | DYNAMIC | 148 |
| `/api/zip-coords` | 30 | 30 | none | (none) | no | DYNAMIC | 146 |
| `/api/dealership/townandcountrytoyota-com/cars` | 14,567 | 154,375 | gzip | private, no-cache | yes | DYNAMIC | 172 |
| `/api/dealership/townandcountrytoyota-com/filter-options` | 1,764 | 6,153 | gzip | private, max-age=60 | no | DYNAMIC | 158 |
| `/static/css/15-dealership.css` | 2,629 | 12,172 | br | public, max-age=31536000, immutable | yes | HIT | 167 |
| `/static/vendor/leaflet/images/marker-icon-2x.png` | 2,464 | 2,464 | none | public, max-age=14400 | yes | HIT | 58 |
| `/static/vendor/leaflet/images/marker-icon.png` | 1,466 | 1,466 | none | public, max-age=14400 | yes | HIT | 78 |
| `/static/vendor/leaflet/images/marker-shadow.png` | 618 | 618 | none | public, max-age=14400 | yes | HIT | 66 |
| `/api/fuel/lookup` | 106 | 155 | br | (none) | no | DYNAMIC | 183 |

* css: 17 files, 57,240 B wire, 312,135 B decoded
* other: 13 files, 162,690 B wire, 795,321 B decoded
* js: 27 files, 126,114 B wire, 544,666 B decoded
* total live transfers logged: 133


## Appendix C. Raw files

`raw/live-speed/` next to this report (the scripts and result files are copies; bodies of live responses are
not included). `README.txt` there lists every file. The measurement scripts and logs were produced in the session
scratchpad (path in section 9) and copied here at the end.

* `live/` : `live.py` (rate-limited curl wrapper), `run_timing.sh`, `idle_sample.sh`, `idle_round.sh`,
  `live_chain.sh`, `overlap_probe.py`, `analyze_curl.py`, `summarize_idle.py`, `requests.scrubbed.jsonl` (every
  live transfer: timings and response headers, with `local_ip`, `local_port` and `Set-Cookie`/`report-to` values
  removed), `curl_tables.md`, `url_lists/`.
* `replay/` : `server.mjs`, `build_manifest.py`, `manifest_summary.tsv` (URL, status, recorded server wait, cf
  status, bytes), `spki.txt`.
* `pw/` and `pw2/` : the first (`measure.py`) and the corrected (`measure2.py`) harness, `trace_steps.py`,
  `counterfactual.py`, `cf_lib.py`, `css_cf.py`, `coverage_probe.py`, `thirdparty_probe.py`, `payload_probe.py`,
  `metric_reset_probe.py`, `debug_back.py`, `aggregate*.py`, `make_tables.sh`, `run_*.sh`.
* `results/` : `res2_*.json`, `trace2_*.json`, `coverage.json`, `thirdparty.json`, `payload_probe.out`.
* `tables/` : every markdown table in this report as generated.
* The 2026-11-10 additions: `live/rv_live.sh`, `rv_live_table.py`, `rv_origin_time.py`, `analyze_curl_out_rv.txt`;
  `pw2/rv_browser.sh`, `rv_browser2.sh`, `rv_browser3.sh`, `rv_compare.py`, `make_rv_tables.sh`, `dom_facets_probe.py`,
  `build_css_manifests_v2.py`, `css_whatif_table.py`; `results/rv_*.json`, `results/res2_edgeab_*.json`,
  `results/res2_css_*.json` (including the `firstbuild` files of the rejected used-rules build); `tables/rv_*.md`,
  `tables/idle_rv.md`, `tables/edge_ab.md`, `tables/css_cf2.md`.

## Appendix D. Every live transfer of the re-verification run (2026-11-10, 01:23-01:25 UTC on 2026-11-11, home LAN)

Same columns as Appendix A, plus Content-Encoding, `cf-cache-status`, `Cache-Control`, whether an ETag came back, and
the Cloudflare colo (the suffix of `cf-ray`). `rv_a_*` = first hit on a fresh connection, `rv_b_*` = repeat,
`rv_c1..3` = warm batches (the first transfer of each batch opens the connection), `rv_d` = static files, the facet
list and `/api/ready`, `rv_e_api_inm` = the conditional GET. The `cfOrigin` is `-` where Cloudflare sent no
`Server-Timing` (it does so for `Accept: text/html` requests).

| label | endpoint | conn | DNS | TCP | TLS | TTFB | total | wait (TTFB-pretransfer) | cfOrigin | wire B | decoded B | http | Content-Encoding | cf-cache-status | Cache-Control | ETag | colo |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| rv_a_health#0 | health | new | 2 | 37 | 34 | 213 | 214 | 141 | - | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_a_car#0 | car | new | 2 | 36 | 41 | 2253 | 2253 | 2173 | 2090 | 10,452 | 55,720 | h2 | br | DYNAMIC | private, max-age=180 | no | IAD |
| rv_a_dealer#0 | dealership | new | 2 | 35 | 49 | 515 | 515 | 428 | 365 | 12,679 | 90,694 | h2 | br | DYNAMIC | - | no | ATL |
| rv_a_home#0 | home | new | 2 | 35 | 59 | 260 | 260 | 164 | 97 | 4,204 | 15,701 | h2 | br | DYNAMIC | - | no | IAD |
| rv_a_listings#0 | listings | new | 2 | 25 | 43 | 409 | 445 | 339 | 199 | 189,500 | 1,548,917 | h2 | br | DYNAMIC | - | no | ATL |
| rv_a_api#0 | api_cars | new | 2 | 36 | 31 | 1635 | 1766 | 1566 | - | 1,585,587 | 14,546,046 | h2 | gzip | DYNAMIC | public, max-age=0, s-maxage=60, stale-while-revalidate=30 | yes | ATL |
| rv_b_car#0 | car | new | 2 | 24 | 48 | 417 | 420 | 343 | 237 | 10,450 | 55,720 | h2 | br | DYNAMIC | private, max-age=180 | no | ATL |
| rv_b_dealer#0 | dealership | new | 2 | 31 | 37 | 298 | 303 | 228 | 150 | 12,679 | 90,694 | h2 | br | DYNAMIC | - | no | IAD |
| rv_b_home#0 | home | new | 2 | 20 | 37 | 210 | 220 | 151 | 106 | 4,204 | 15,701 | h2 | br | DYNAMIC | - | no | ATL |
| rv_b_listings#0 | listings | new | 2 | 38 | 78 | 457 | 515 | 339 | 235 | 189,497 | 1,548,917 | h2 | br | DYNAMIC | - | no | IAD |
| rv_b_api#0 | api_cars | new | 2 | 64 | 31 | 202 | 334 | 105 | - | 1,585,587 | 14,546,046 | h2 | gzip | DYNAMIC | public, max-age=0, s-maxage=60, stale-while-revalidate=30 | yes | ATL |
| rv_b_health#0 | health | new | 2 | 29 | 33 | 219 | 219 | 155 | - | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c1#0 | health | new | 2 | 25 | 37 | 188 | 188 | 124 | 78 | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c1#1 | home | reused | 0 | 0 | 0 | 171 | 171 | 171 | 126 | 4,203 | 15,701 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c1#2 | listings | reused | 0 | 0 | 0 | 443 | 488 | 443 | 362 | 189,951 | 1,548,917 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c1#3 | car | reused | 0 | 0 | 0 | 341 | 341 | 341 | 269 | 10,452 | 55,720 | h2 | br | DYNAMIC | private, max-age=180 | no | ATL |
| rv_c1#4 | dealership | reused | 0 | 0 | 0 | 322 | 323 | 322 | 248 | 12,679 | 90,694 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c1#5 | health | reused | 0 | 0 | 0 | 133 | 133 | 132 | 82 | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c2#0 | health | new | 2 | 27 | 60 | 288 | 288 | 198 | 111 | 77 | 86 | h2 | br | DYNAMIC | - | no | IAD |
| rv_c2#1 | home | reused | 0 | 0 | 0 | 191 | 192 | 191 | 118 | 4,204 | 15,701 | h2 | br | DYNAMIC | - | no | IAD |
| rv_c2#2 | listings | reused | 0 | 0 | 0 | 295 | 344 | 295 | 222 | 189,496 | 1,548,917 | h2 | br | DYNAMIC | - | no | IAD |
| rv_c2#3 | car | reused | 0 | 0 | 0 | 346 | 347 | 346 | 300 | 10,451 | 55,720 | h2 | br | DYNAMIC | private, max-age=180 | no | IAD |
| rv_c2#4 | dealership | reused | 0 | 0 | 0 | 199 | 200 | 199 | 146 | 12,679 | 90,694 | h2 | br | DYNAMIC | - | no | IAD |
| rv_c2#5 | health | reused | 0 | 0 | 0 | 121 | 121 | 121 | 73 | 77 | 86 | h2 | br | DYNAMIC | - | no | IAD |
| rv_c3#0 | health | new | 2 | 23 | 43 | 201 | 201 | 133 | 68 | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c3#1 | home | reused | 0 | 0 | 0 | 149 | 149 | 148 | 108 | 4,204 | 15,701 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c3#2 | listings | reused | 0 | 0 | 0 | 437 | 488 | 437 | 367 | 189,460 | 1,548,917 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c3#3 | car | reused | 0 | 0 | 0 | 347 | 349 | 347 | 303 | 10,454 | 55,720 | h2 | br | DYNAMIC | private, max-age=180 | no | ATL |
| rv_c3#4 | dealership | reused | 0 | 0 | 0 | 273 | 276 | 272 | 239 | 12,678 | 90,694 | h2 | br | DYNAMIC | - | no | ATL |
| rv_c3#5 | health | reused | 0 | 0 | 0 | 120 | 120 | 120 | 74 | 77 | 86 | h2 | br | DYNAMIC | - | no | ATL |
| rv_d#0 | static_css | new | 2 | 29 | 32 | 121 | 121 | 57 | - | 8,993 | 53,548 | h2 | br | HIT | public, max-age=31536000, immutable | yes | ATL |
| rv_d#1 | static_js | reused | 0 | 0 | 0 | 55 | 60 | 55 | - | 21,956 | 119,771 | h2 | br | HIT | public, max-age=31536000, immutable | yes | ATL |
| rv_d#2 | filter_options | reused | 0 | 0 | 0 | 145 | 149 | 145 | - | 86,107 | 574,227 | h2 | gzip | DYNAMIC | - | no | ATL |
| rv_d#3 | ready | reused | 0 | 0 | 0 | 148 | 148 | 148 | - | 62 | 75 | h2 | br | DYNAMIC | - | no | ATL |
| rv_e_api_inm#0 | api_cars | new | 74 | 20 | 48 | 392 | 392 | 250 | - | 0 | - (HTTP 304) | h2 | - | DYNAMIC | public, max-age=0, s-maxage=60, stale-while-revalidate=30 | yes | ATL |


## Appendix E. Every screenshot file (146 files)

```
scratchpad/web_shots/live-speed2-coldserver_phone_flow_1_home.png
scratchpad/web_shots/live-speed2-coldserver_phone_flow_2_listings.png
scratchpad/web_shots/live-speed2-coldserver_phone_flow_3_results.png
scratchpad/web_shots/live-speed2-coldserver_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed2-coldserver_phone_flow_4_car.png
scratchpad/web_shots/live-speed2-coldserver_phone_flow_5_back.png
scratchpad/web_shots/live-speed2-edge_desktop_car_cold.png
scratchpad/web_shots/live-speed2-edge_desktop_dealership_cold.png
scratchpad/web_shots/live-speed2-edge_desktop_home_cold.png
scratchpad/web_shots/live-speed2-edge_desktop_listings_cold.png
scratchpad/web_shots/live-speed2-edge_desktop_listings_zip_cold.png
scratchpad/web_shots/live-speed2-edge_phone_car_cold.png
scratchpad/web_shots/live-speed2-edge_phone_dealership_cold.png
scratchpad/web_shots/live-speed2-edge_phone_home_cold.png
scratchpad/web_shots/live-speed2-edge_phone_listings_cold.png
scratchpad/web_shots/live-speed2-edge_phone_listings_zip_cold.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_1_home.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_2_listings.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_3_results.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_3_results_mid.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_4_car.png
scratchpad/web_shots/live-speed2-nobf_phone_flow_nobf_5_back.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_1_home.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_2_listings.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_3_results.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_3_results_mid.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_4_car.png
scratchpad/web_shots/live-speed2-nocookie_desktop_flow_5_back.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_1_home.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_2_listings.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_3_results.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_4_car.png
scratchpad/web_shots/live-speed2-nocookie_phone_flow_5_back.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_1_home.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_2_listings.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_3_results.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_3_results_mid.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_4_car.png
scratchpad/web_shots/live-speed2-nonav_desktop_flow_5_back.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_1_home.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_2_listings.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_3_results.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_4_car.png
scratchpad/web_shots/live-speed2-nonav_phone_flow_5_back.png
scratchpad/web_shots/live-speed2_desktop_car_cold.png
scratchpad/web_shots/live-speed2_desktop_dealership_cold.png
scratchpad/web_shots/live-speed2_desktop_flow_1_home.png
scratchpad/web_shots/live-speed2_desktop_flow_2_listings.png
scratchpad/web_shots/live-speed2_desktop_flow_3_results.png
scratchpad/web_shots/live-speed2_desktop_flow_3_results_mid.png
scratchpad/web_shots/live-speed2_desktop_flow_4_car.png
scratchpad/web_shots/live-speed2_desktop_flow_5_back.png
scratchpad/web_shots/live-speed2_desktop_home_cold.png
scratchpad/web_shots/live-speed2_desktop_listings_cold.png
scratchpad/web_shots/live-speed2_desktop_listings_zip_cold.png
scratchpad/web_shots/live-speed2_desktop_quickback.png
scratchpad/web_shots/live-speed2_phone_car_cold.png
scratchpad/web_shots/live-speed2_phone_dealership_cold.png
scratchpad/web_shots/live-speed2_phone_flow_1_home.png
scratchpad/web_shots/live-speed2_phone_flow_2_listings.png
scratchpad/web_shots/live-speed2_phone_flow_3_results.png
scratchpad/web_shots/live-speed2_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed2_phone_flow_4_car.png
scratchpad/web_shots/live-speed2_phone_flow_5_back.png
scratchpad/web_shots/live-speed2_phone_home_cold.png
scratchpad/web_shots/live-speed2_phone_listings_cold.png
scratchpad/web_shots/live-speed2_phone_listings_zip_cold.png
scratchpad/web_shots/live-speed2_phone_quickback.png
scratchpad/web_shots/live-speed3-css2base_phone_car_cold.png
scratchpad/web_shots/live-speed3-css2base_phone_home_cold.png
scratchpad/web_shots/live-speed3-css2base_phone_listings_cold.png
scratchpad/web_shots/live-speed3-css2used_phone_car_cold.png
scratchpad/web_shots/live-speed3-css2used_phone_home_cold.png
scratchpad/web_shots/live-speed3-css2used_phone_listings_cold.png
scratchpad/web_shots/live-speed3-cssall_phone_car_cold.png
scratchpad/web_shots/live-speed3-cssall_phone_home_cold.png
scratchpad/web_shots/live-speed3-cssall_phone_listings_cold.png
scratchpad/web_shots/live-speed3-cssbase_phone_car_cold.png
scratchpad/web_shots/live-speed3-cssbase_phone_home_cold.png
scratchpad/web_shots/live-speed3-cssbase_phone_listings_cold.png
scratchpad/web_shots/live-speed3-cssused_phone_car_cold.png
scratchpad/web_shots/live-speed3-cssused_phone_home_cold.png
scratchpad/web_shots/live-speed3-cssused_phone_listings_cold.png
scratchpad/web_shots/live-speed3-edgeab-base_phone_dealership_cold.png
scratchpad/web_shots/live-speed3-edgeab-base_phone_home_cold.png
scratchpad/web_shots/live-speed3-edgeab-base_phone_listings_cold.png
scratchpad/web_shots/live-speed3-edgeab-base_phone_listings_zip_cold.png
scratchpad/web_shots/live-speed3-edgeab-edge_phone_dealership_cold.png
scratchpad/web_shots/live-speed3-edgeab-edge_phone_home_cold.png
scratchpad/web_shots/live-speed3-edgeab-edge_phone_listings_cold.png
scratchpad/web_shots/live-speed3-edgeab-edge_phone_listings_zip_cold.png
scratchpad/web_shots/live-speed3-rv_desktop_car_cold.png
scratchpad/web_shots/live-speed3-rv_desktop_dealership_cold.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_1_home.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_2_listings.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_3_results.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_3_results_mid.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_4_car.png
scratchpad/web_shots/live-speed3-rv_desktop_flow_5_back.png
scratchpad/web_shots/live-speed3-rv_desktop_home_cold.png
scratchpad/web_shots/live-speed3-rv_desktop_listings_cold.png
scratchpad/web_shots/live-speed3-rv_desktop_listings_zip_cold.png
scratchpad/web_shots/live-speed3-rv_phone_car_cold.png
scratchpad/web_shots/live-speed3-rv_phone_dealership_cold.png
scratchpad/web_shots/live-speed3-rv_phone_flow_1_home.png
scratchpad/web_shots/live-speed3-rv_phone_flow_2_listings.png
scratchpad/web_shots/live-speed3-rv_phone_flow_3_results.png
scratchpad/web_shots/live-speed3-rv_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed3-rv_phone_flow_4_car.png
scratchpad/web_shots/live-speed3-rv_phone_flow_5_back.png
scratchpad/web_shots/live-speed3-rv_phone_home_cold.png
scratchpad/web_shots/live-speed3-rv_phone_listings_cold.png
scratchpad/web_shots/live-speed3-rv_phone_listings_zip_cold.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_1_home.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_2_listings.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_3_results.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_4_car.png
scratchpad/web_shots/live-speed3-rvapimiss_phone_flow_5_back.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_1_home.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_2_listings.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_3_results.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_3_results_mid.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_4_car.png
scratchpad/web_shots/live-speed3-rvnobf_phone_flow_nobf_5_back.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_1_home.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_2_listings.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_3_results.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_3_results_mid.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_4_car.png
scratchpad/web_shots/live-speed3-rvnonav_phone_flow_5_back.png
scratchpad/web_shots/live-speed_desktop_car_cold.png
scratchpad/web_shots/live-speed_desktop_dealership_cold.png
scratchpad/web_shots/live-speed_desktop_home_cold.png
scratchpad/web_shots/live-speed_desktop_listings_cold.png
scratchpad/web_shots/live-speed_phone_car_cold.png
scratchpad/web_shots/live-speed_phone_dealership_cold.png
scratchpad/web_shots/live-speed_phone_flow_1_home.png
scratchpad/web_shots/live-speed_phone_flow_2_listings.png
scratchpad/web_shots/live-speed_phone_flow_3_results.png
scratchpad/web_shots/live-speed_phone_flow_4_car.png
scratchpad/web_shots/live-speed_phone_flow_5_back.png
scratchpad/web_shots/live-speed_phone_home_cold.png
scratchpad/web_shots/live-speed_phone_listings_cold.png
```
