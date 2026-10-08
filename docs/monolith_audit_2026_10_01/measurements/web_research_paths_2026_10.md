# Car-chat web research: what headless Chromium contributes (P0B.3)

- Unit: P0B.3 (security-10) of `docs/REMEDIATION_PLAN_2026_10.md`. Feeds D-SEC1 and P3.8.
- Date: run 1 2026-10-08 00:00Z to 01:17Z (the evening of 10-07, local time), on battery with owner approval. Re-run of the rows lost to a network outage, the Brave probe and the extraction spot check: 2026-10-08 14:55Z to 15:05Z.
- Code at `dbdbf5cba` (detached worktree). `backend/utils/web_researcher.py` and `backend/intelligence/ai/agent.py` are byte-identical at `afa063a22` (the branch head on 10-08). All line numbers below are at `dbdbf5cba`.
- Host: the MacBook, from the home (residential) IP. Nothing ran on Railway. Prod was read only: Railway HTTP and deploy logs, the web service's variable names (from P0A.1), and one read-only Postgres session (`default_transaction_read_only=on`).
- Nothing was written except this file. No dealership inventory was scanned. One search result (paautosales.com, a used-car dealer's blog) was fetched as an article. It is not a tracked dealer (0 rows in `dealerships`), so no dealer log applies.

## 1. Answer

**Recommend D-SEC1 = (a): take Chromium out of the web image and make WebResearcher HTTP-only by default.** Keep browser mode only for `analyze_trim_adds_with_web.py`. The browser does not produce "a large share of answers", which was the plan's condition for keeping it:

| Accept metric | Value (30 questions, 10 makes) |
|---|---|
| HTTP-only success (option (a), same DuckDuckGo candidates, HTTP fetch only) | **29/30 (96.7%)** answered. 22/30 (73.3%) carry real content about the car; 7/30 are site menus only (section 4.2). |
| Browser-dependent answers (no answer without the Chromium fallback or Brave) | **0/30 (0%)** |
| Browser involved in today's answer (fallback or Brave) | 5/30 (16.7%). 4 of the 5 are Edmunds "403 - Access Denied" pages, accepted and passed to the LLM as research. 1 is a real article, and HTTP-only answered that question from the next candidate (caredge.com). |
| Brave search (Chromium) | Ran in 1/30 questions (q15, after DuckDuckGo served its bot page). It returned 0 links, and so did the 6-query probe: **7/7 Brave bot-verification pages**. |
| Chromium page fallback | Ran in 16/30 questions (16 launches), each time after an HTTP 403. **15/16 got the same block**: 11 KBB "Access Denied" (rejected, under 80 chars) and 4 Edmunds "Access Denied" (accepted). 1/16 got a real page. |
| Median end-to-end latency, current code | 3.16 s (30 questions); 3.07 s without q02's 121 s DuckDuckGo hang, which hits both paths |
| Median end-to-end latency, HTTP-only | 2.02 s (29 answered questions) |
| Where Chromium ran (16 questions) | median 3.58 s now vs 2.02 s HTTP-only. Worst case q26: 11.5 s vs 3.2 s. |
| Chromium page fetch | median 1.44 s (1.02 to 8.20 s, n=16) |
| Brave search | median 2.53 s (2.41 to 3.02 s, n=7) |
| HTTP page fetch / DuckDuckGo HTML search | median 0.29 s (n=40) / 1.24 s (p90 1.90 s, n=30, one 121 s hang) |
| Prod usage of car chat | **0** POSTs to `/api/car/<id>/chat`, `/api/compare/chat` or `/api/ai/chat` in Railway HTTP logs from 2026-09-17 to 2026-10-08 (all 5 web deployments in that window, 350 POSTs in total) |

The browser's measured contribution is 1 real page out of 23 Chromium sessions (16 page fetches plus 7 Brave searches), and HTTP-only answered that same question anyway. Against that, it fed an error page to the model as research in 4/30 questions (13.3%). It added a median 1.56 s, up to 8.2 s, to the 16 questions where it ran. And it launches as root with `--no-sandbox` for any logged-in user (section 2). Option (b), keeping it hardened, would keep that cost for no measured gain. Option (c), a worker, would add infrastructure for the same zero gain.

Caveat (section 6): all of this is from a residential IP. Railway egress is datacenter, where DuckDuckGo and Akamai-fronted sites may block more. That would lower HTTP-only yield on Railway, but the browser could not make up for it: Brave bot-paged headless Chromium 7/7 times, and KBB/Edmunds denied it 15/15 times, even from the residential IP.

## 2. How a chat question reaches the browser

- Trigger: `agent.py:858` (car page) and `:1201` (compare). `_needs_web_research(msg)` (keyword list, :268-289) and not a price question. The policy is `car_chat_policy.py:10-41`: with `CAR_CHAT_WEB_RESEARCH` unset in production, any logged-in session may run research (:41). On Railway web all three research variables are unset as of 2026-10-08 ~14:46Z (`docs/SCANNING_OPS_LOG.md`, STOPGAP-D-SEC1). The approved stopgap `CAR_CHAT_WEB_RESEARCH=0` is not set yet.
- Query: `_build_search_query(car, msg)` (`agent.py:392-441`): year, make, model and trim plus a topic suffix. Compare chat appends `vs <year make model>` for up to two other cars (:1207-1225).
- Call: `researcher.search_and_summarize(query)` (`agent.py:891`, `:1227`) with **no year/make/model**. So the direct guide URLs (`web_researcher.py:511-516`) never run in chat. Only `analyze_trim_adds_with_web.py:110` passes them.
- Candidates (`web_researcher.py:499-532`): DuckDuckGo HTML over urllib (max 8, :519-523). If fewer than 2 candidates come back, Brave Search runs in headless Chromium (:527-529, :534-572).
- Fetch loop (`:655-677`): for each candidate, `fetch_page_text_http` first. If it raises (any 4xx/5xx raises `HTTPError`) or returns under 80 cleaned chars, `_fetch_with_playwright` opens a fresh Chromium for that one URL. The first page with 80 or more cleaned chars wins (`_result_from_page`, :606-624). Nothing checks whether that text is an error page.
- Launch: `playwright.chromium.launch(headless=True, args=_playwright_launch_args())` (:363). The code adds `--no-sandbox` itself only when `PLAYWRIGHT_NO_SANDBOX=1`. Playwright adds it anyway unless `chromiumSandbox` is true: the 1.60.0 driver (`coreBundle.js`) does `if (options2.chromiumSandbox !== true) chromeArguments.push("--no-sandbox")`, and `requirements.txt` pins `playwright>=1.60.0`. `Dockerfile.web` has no `USER` line, so the process is root. Every launch is a fresh browser per URL, with no request routing.
- Knowledge cache: the car-page path reads, then upserts, the result into `car_knowledge_embeddings` (`agent.py:870-905`; `pgvector_service.py:539-618`, keyed by year/make/model/trim, no TTL). In prod that table does not exist: the prod Postgres has the `vector` extension but no knowledge or embedding table, and web's 27 variables include neither `PGVECTOR_URL` nor `DATABASE_URL` (P0A.1). The cache has never written to prod Postgres, so every triggered prod question would run live research. If the cache were ever configured, an accepted block page would overwrite that car's entry.

## 3. Method

- Sample: 10 real active cars from the local DB (read-only), one per make (BMW, Chevrolet, Ford, Honda, Hyundai, Jeep, Mercedes-Benz, Nissan, Subaru, Toyota), and 3 questions each. That gives 30 questions: 27 car-page and 3 compare-chat (with two partner cars each, as `run_compare_chat` builds them). Topics: reliability 12, review 5, compare 3, maintenance 3, recall 3, resale 2, generic 2 (counts by the question's intent, not the suffix the query builder chose). All 30 trigger research and none is a price question. They build 26 distinct queries: q04/q05, q13/q14, q25/q26 and q28/q29 build the same query.
- Script: `run.py`, one process. It imports the worktree's `web_researcher` and `agent` and calls `WebResearcher(timeout_ms=25_000, max_text_chars=2_000).search_and_summarize(query)` exactly as `agent.py:890-891` does. Thin wrappers around `duckduckgo_html_result_links`, `fetch_page_text_http`, `_brave_search_links`, `_fetch_with_playwright` and `_result_from_page` record latency, status, outcome and the accepted text, then call the originals unchanged. The DuckDuckGo wrapper parses up to 40 links from the same single response so the whole result page is recorded. The first 8 are identical to what the code uses.
- HTTP-only counterfactual (option (a)): the same DuckDuckGo candidates in order, HTTP fetch only, first accepted page. Fetches the real flow had already made are reused. Extra HTTP fetches (6 in total) were made only where the real flow stopped before trying a candidate over HTTP. Its latency is the recorded DuckDuckGo time plus the HTTP fetch times up to the accepted page, with no sleeps. Where HTTP-only failed (q15), a second counterfactual tried the direct guide URLs over HTTP with year/make/model passed.
- Brave probe: `_brave_search_links(query, max_results=8)` on every 5th question (q01, q06, q11, q16, q21, q26), because Brave ran only once in the real flow.
- Extraction spot check: three answer URLs whose HTTP snippet was menus only (CR q01, CR q30, CarGurus q18). Each was fetched once over HTTP with `<article>`/`<main>` extraction (BeautifulSoup) and once with the code's own `_fetch_with_playwright` + `_result_from_page`.
- Pacing: 6-10 s between questions, 0.5-1.5 s between counterfactual fetches, 3-5 s between spot-check URLs. In total, counting only requests that reached the network: 51 HTTP page fetches, 31 DuckDuckGo searches, 26 Chromium sessions (16 page fallbacks, 7 Brave searches, 3 spot checks).
- Lost rows: in run 1, q18 (after its DuckDuckGo search) through q25 ran during a network outage. DuckDuckGo failed with `[Errno 8] nodename nor servname provided`, Chromium with `net::ERR_INTERNET_DISCONNECTED`, and every HTTP fetch with `blocked host` (the private-host guard fails closed when DNS fails). Those 8 rows were discarded and re-run on 10-08 with the same script and revision. q15 is from run 1 and is genuine: DuckDuckGo served its bot page (`DuckDuckGo bot verification page`) and Brave served its own.
- Grading: I read every accepted snippet, i.e. the text the LLM receives. Grades: **substantive** (real content about this car), **chrome** (site menus, nav and trim lists only), **block page**, **none**. The script's automatic flags (model name and topic word in the text) passed on page titles alone, so they were not used for grading.
- Raw data (scratch, not in the repo): `/private/tmp/claude-501/phase0/p0b3_run/`, with `results.jsonl` (run 1), `results_rerun.jsonl` (q18-q25), `brave_probe.jsonl`, `extract_check.json`, `run.py`, `run_rerun.py`, `extract_check.py`, and `prod_knowledge*.sql/.out`.

## 4. Results

### 4.1 Where candidates and answers came from

| Stage | Count |
|---|---|
| Candidates from direct guides | 0/30 (chat never passes year/make/model) |
| Candidates from DuckDuckGo | 29/30 questions got 7-8 links. 1/30 (q15) got the DuckDuckGo bot page. |
| Candidates from Brave | 0/30 (ran once, bot page) |
| HTTP fetches in the real flow | 40: 24 accepted, 16 HTTP 403 (kbb.com 11, edmunds.com 4, paautosales.com 1), 0 too short |
| Chromium page fetches | 16, all after an HTTP 403. kbb.com: 11 "Access Denied" pages (103-113 raw chars, rejected as too short). edmunds.com: 4 "403 - Access Denied" pages (229 chars, accepted). paautosales.com: 1 real article (accepted). |
| Answer, current code | HTTP 24 (80.0%), Chromium 5 (16.7%), none 1 (3.3%) |
| Answer, HTTP-only | HTTP 29 (96.7%), none 1 (3.3%) |

The Edmunds text that the current code passes to the LLM as research for q02, q12, q19 and q23, with an instruction to cite `Source: edmunds.com`: "We're sorry, but you don't have permission to access this page. This may be due to network restrictions or unusual activity. If you believe this is an error, please include the reference ID and IP Address when contacting support." The HTTP path never accepts this text, because the 403 raises before any text is read. Only the browser renders a 403 body into "content".

### 4.2 What the LLM receives (graded)

| Grade | Current code | HTTP-only (a), extraction as today | HTTP-only + `<main>`/`<article>` extraction |
|---|---|---|---|
| substantive | 18 (60.0%) | 22 (73.3%) | projected 29 (96.7%); 3 of the 5 distinct menu-only URLs checked, 3/3 recovered |
| chrome (menus only) | 7 (23.3%) | 7 (23.3%) | 0 among the 3 checked |
| block page passed as research | 4 (13.3%) | 0 | 0 |
| none | 1 (3.3%) | 1 (3.3%) | 1, or 0 with direct guide URLs (below) |

- The 7 menu-only answers are Consumer Reports (q01, q03, q04, q05, q16, q30; 4 distinct URLs) and CarGurus (q18). `fetch_page_text_http` flattens the whole document into one line, and `_result_from_page` keeps its first 2,000 chars. For these sites, all 2,000 are navigation, and the one-line text also defeats `_filter_short_lines`. The current code never sends these pages to Chromium, because the HTTP text is longer than 80 chars. So the browser does nothing for them today.
- The content is in the static HTML. CR q30: HTTP `<main>` has "We expect the 2026 Sienna will have about average reliability…", the same sentence Chromium extracted. CR q01: HTTP `<article>` (465 chars of owner comments) holds the same text Chromium returned (352 chars). CarGurus q18: the editorial paragraph ("One of the most popular SUVs in America…") is in the server HTML, at char 2,237 of the flattened text, just past the 2,000-char cut. Chromium found it through `_extract_content`'s 10-word line filter. The gap is extraction, not JavaScript rendering. The unchecked CR URLs (q04/q05 Suburban, q16 Grand Cherokee) use the same CR template, hence "projected".
- q15 with direct guide URLs over HTTP: `motortrend.com/cars/hyundai/tucson/` was accepted (it names the model, but it is the general model page, not repair cost). That needs `year/make/model` passed from `agent.py`, which is outside P3.8's file list.

### 4.3 Latency

| Path | n | Median | Range |
|---|---|---|---|
| DuckDuckGo HTML search (urllib) | 30 | 1.24 s | 0.38 s (bot page) to 2.19 s; one 121.08 s hang (q02) |
| HTTP page fetch | 40 | 0.29 s | 0.08 to 2.88 s |
| Chromium page fetch | 16 | 1.44 s | 1.02 to 8.20 s |
| Brave search in Chromium | 7 | 2.53 s | 2.41 to 3.02 s |
| End to end, current code | 30 | 3.16 s | 1.09 to 122.6 s (11.5 s without q02) |
| End to end, HTTP-only | 29 answered | 2.02 s | 1.06 to 122.5 s (4.0 s without q02) |
| The 16 questions where Chromium ran | 16 | now 3.58 s, HTTP-only 2.02 s | |

q02's DuckDuckGo request took 121 s although `urlopen(..., timeout=20)` (`web_researcher.py:215`). That timeout applies per socket operation, so a slow trickle never trips it. Both paths share this. It needs an overall deadline (section 5).

### 4.4 Prod

- HTTP logs (`railway logs -s web --http --since … --filter '@method:POST'`, one query per deployment): c9872e66 (2026-10-05 to 10-08) 17 POSTs, 62f93905 4, 3c386092 22, c697577b 30, 8996cf49 (2026-09-17 to 09-28) 277. None to a chat path. The current deployment's 17 POSTs were 13 `/xmlrpc.php` probes, 2 `/api/auth/login`, 1 `/login` and 1 `/logout`. Only method, path, status and duration were read, never IPs or user agents. The logs reach back to 2026-09-17, so retention is not the reason for the zero.
- Deploy logs: the current web deployment has 15 log lines, all from startup, and none containing `WebResearcher`. App-level logging does not reach Railway, so prod research outcomes cannot be measured from logs. P3.8's accept step ("a car-chat research answer is served from the HTTP path") will be the first Railway data point.
- Postgres: `car_knowledge_embeddings` does not exist (section 2).

### 4.5 The other Section 8 stopgap (host allowlist)

The plan offers `WEB_RESEARCH_ALLOWED_HOSTS` set to a short list of auto reference sites as an alternative to `CAR_CHAT_WEB_RESEARCH=0`. I applied a 14-host list (caranddriver, motortrend, edmunds, kbb, consumerreports, cars.com, nhtsa.gov, repairpal, carcomplaints, jdpower, autotrader, iseecars, cargurus, carfax) to each question's recorded DuckDuckGo result page:
- In 12/30 questions, only hosts that blocked both paths or gave menus only were left (kbb, edmunds, consumerreports, cargurus).
- In 3/30 (q15, q17, q20) no link was left at all, so the code would go to Brave, which bot-pages.
- Only 8/30 kept a host that gave a substantive HTTP answer here (motortrend, jdpower). The other 7 kept hosts this run never fetched.

The allowlist would push answers toward the sites that block. The owner's choice, `CAR_CHAT_WEB_RESEARCH=0`, is the cleaner stopgap.

## 5. Recommendation for D-SEC1 and notes for P3.8

1. **D-SEC1 = (a).** Chromium leaves `Dockerfile.web`. `WebResearcher` gets `allow_browser=False` by default, gating both `_brave_search_links` (:527-529) and `_fetch_with_playwright` (:665-674). Chat callers keep the default. `analyze_trim_adds_with_web.py:336` passes `allow_browser=True`. This is P3.8 as written. On this sample it costs no answers and removes 4/30 block-page answers.
2. Within P3.8's file (`web_researcher.py`), two cheap HTTP-side changes would close most of the remaining quality gap, with no browser:
   - Extract `<article>`/`[role=main]`/`<main>` before flattening, and keep block-level line breaks so `_filter_short_lines` works. Projected: 7 menu-only answers become substantive (3/3 verified).
   - Have `_result_from_page` reject known block-page text (title "Access Denied" / "403 -", "you don't have permission to access", the DuckDuckGo and Brave bot markers). This matters for `analyze_trim_adds_with_web.py` if it keeps browser mode, since Edmunds and KBB deny Chromium too.
   - Give the DuckDuckGo request an overall deadline (the q02 121 s hang).
3. Outside P3.8 (agent.py), as follow-ups for whichever phase owns car chat:
   - Pass `year/make/model` so direct guide URLs run. This would have rescued q15.
   - Keep the compared model in the query. Today the suffix replaces the message: "Should I buy this or a Tahoe?" searches "… review expert opinion" (q06, q08, q23, q29 lose the rival's name), and the generic fallback keeps the first 6 words ("… is it better to buy or", q12).
   - Side note: the trigger `"long.term"` (`agent.py:280`) is a literal substring with a dot, so it never matches "long term".
4. Stopgap: still set `CAR_CHAT_WEB_RESEARCH=0` on Railway web now (approved 2026-10-07, not yet applied). With 0 chat POSTs in three weeks, it costs nothing visible.

## 6. Limits

- One residential IP, one or two runs per question, 30 questions. DuckDuckGo ordering varied between identical queries: q13/q14 and q28/q29 picked different second and first hosts. Treat the percentages as ±1-2 questions.
- Railway egress was not measured (no code runs on prod in this unit). On a datacenter IP, DuckDuckGo may serve its bot page more often. Then HTTP-only yield drops, and the real fix is a search API, not a browser: Brave already bot-pages headless Chromium from a residential IP.
- Grades are one reader's judgement of the snippet's relevance to the car. Topic fit to the exact question is weaker for both paths, for the query-building reasons in 5.3.
- The extraction projection rests on 3 of 5 distinct menu-only URLs.

## Appendix: per question

"Total s" is the measured wall time of `search_and_summarize`. "HTTP-only s" is DuckDuckGo plus the HTTP fetches up to the accepted page, with no sleeps. q18-q25 are from the 10-08 re-run; the rest are from run 1.

| Q | Car | Question (compare chat marked C) | DDG links | Fetch chain, real flow (PW = Chromium) | Answer now: path, host | Grade | Total s | HTTP-only: host | Grade | HTTP-only s |
|---|---|---|---|---|---|---|---|---|---|---|
| q01 | 2021 BMW 3 Series 330i xDrive | Is this reliable long term? | 8 | consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 1.3 | consumerreports.org | chrome | 1.3 |
| q02 | 2021 BMW 3 Series 330i xDrive | What does it cost to maintain once the warranty ends? | 8 | edmunds.com HTTP 403 → edmunds.com PW Access Denied, accepted | Chromium, edmunds.com | block page | 122.6 | bimmerboom.com | substantive | 122.5 |
| q03 | 2021 BMW 3 Series 330i xDrive | Which of these is more reliable? (C) | 8 | consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 1.4 | consumerreports.org | chrome | 1.3 |
| q04 | 2018 Chevrolet Suburban LT | Any common problems with this year? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 3.5 | consumerreports.org | chrome | 2.0 |
| q05 | 2018 Chevrolet Suburban LT | Are there any open recalls for this model? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 2.4 | consumerreports.org | chrome | 1.1 |
| q06 | 2018 Chevrolet Suburban LT | Should I buy this or a Tahoe? | 8 | motortrend.com HTTP ok | HTTP, motortrend.com | substantive | 3.4 | motortrend.com | substantive | 3.3 |
| q07 | 2025 Ford Expedition King Ranch | What do reviews say about the King Ranch? | 8 | roadandtrack.com HTTP ok | HTTP, roadandtrack.com | substantive | 4.0 | roadandtrack.com | substantive | 4.0 |
| q08 | 2025 Ford Expedition King Ranch | How does it compare to the Chevy Tahoe? | 8 | trimatlas.com HTTP ok | HTTP, trimatlas.com | substantive | 1.7 | trimatlas.com | substantive | 1.6 |
| q09 | 2025 Ford Expedition King Ranch | Known issues with the 3.5 EcoBoost? | 8 | problemsbyvin.com HTTP ok | HTTP, problemsbyvin.com | substantive | 1.1 | problemsbyvin.com | substantive | 1.1 |
| q10 | 2026 Honda Accord SE | Is the hybrid system reliable? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → carproblemzoo.com HTTP ok | HTTP, carproblemzoo.com | substantive | 3.3 | carproblemzoo.com | substantive | 1.5 |
| q11 | 2026 Honda Accord SE | How well does it hold resale value? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → carbuzz.com HTTP ok | HTTP, carbuzz.com | substantive | 2.7 | carbuzz.com | substantive | 1.6 |
| q12 | 2026 Honda Accord SE | Is it better to buy or lease? | 8 | edmunds.com HTTP 403 → edmunds.com PW Access Denied, accepted | Chromium, edmunds.com | block page | 3.1 | motortrend.com | substantive | 2.9 |
| q13 | 2016 Hyundai Tucson SE | Does this year have engine problems? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → autojunket.com HTTP ok | HTTP, autojunket.com | substantive | 2.9 | autojunket.com | substantive | 1.8 |
| q14 | 2016 Hyundai Tucson SE | Any recalls on the 2016 Tucson? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → vehiclefaults.com HTTP ok | HTTP, vehiclefaults.com | substantive | 4.4 | vehiclefaults.com | substantive | 3.4 |
| q15 | 2016 Hyundai Tucson SE | What's the repair cost for the dual-clutch transmission? | 0 (DDG bot page; Brave bot page) | – | none | none | 3.4 | none | none | 0.4 |
| q16 | 2020 Jeep Grand Cherokee Trailhawk | How reliable is the Grand Cherokee? | 8 | consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 2.4 | consumerreports.org | chrome | 2.4 |
| q17 | 2020 Jeep Grand Cherokee Trailhawk | Is this model a lemon? | 8 | cleanvins.com HTTP ok | HTTP, cleanvins.com | substantive | 2.4 | cleanvins.com | substantive | 2.4 |
| q18 | 2020 Jeep Grand Cherokee Trailhawk | Is the Jeep better than the others off-road? (C) | 8 | cargurus.com HTTP ok | HTTP, cargurus.com | chrome | 2.2 | cargurus.com | chrome | 2.2 |
| q19 | 2027 Mercedes-Benz GLC 300 | What do the reviews say? | 8 | edmunds.com HTTP 403 → edmunds.com PW Access Denied, accepted | Chromium, edmunds.com | block page | 3.5 | motortrend.com | substantive | 2.0 |
| q20 | 2027 Mercedes-Benz GLC 300 | Is the GLC reliable? | 7 | roadvitals.com HTTP ok | HTTP, roadvitals.com | substantive | 2.3 | roadvitals.com | substantive | 2.3 |
| q21 | 2027 Mercedes-Benz GLC 300 | What's the cost to own over five years? | 8 | paautosales.com HTTP 403 → paautosales.com PW ok | Chromium, paautosales.com | substantive | 3.7 | caredge.com | substantive | 1.9 |
| q22 | 2026 Nissan Pathfinder SL | Any transmission problems with the new Pathfinder? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → carfactbase.com HTTP ok | HTTP, carfactbase.com | substantive | 3.8 | carfactbase.com | substantive | 2.0 |
| q23 | 2026 Nissan Pathfinder SL | How does it compare to the Toyota Highlander? | 8 | edmunds.com HTTP 403 → edmunds.com PW Access Denied, accepted | Chromium, edmunds.com | block page | 2.9 | motortrend.com | substantive | 1.9 |
| q24 | 2026 Nissan Pathfinder SL | How fast does it depreciate? | 8 | carbuzz.com HTTP ok | HTTP, carbuzz.com | substantive | 2.0 | carbuzz.com | substantive | 2.0 |
| q25 | 2017 Subaru Crosstrek 2.0i Base | Does this have the oil consumption issue? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → vehiclefaults.com HTTP ok | HTTP, vehiclefaults.com | substantive | 4.4 | vehiclefaults.com | substantive | 3.1 |
| q26 | 2017 Subaru Crosstrek 2.0i Base | Is the CVT reliable? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → vehiclefaults.com HTTP ok | HTTP, vehiclefaults.com | substantive | 11.5 | vehiclefaults.com | substantive | 3.2 |
| q27 | 2017 Subaru Crosstrek 2.0i Base | Which of these is the most reliable? (C) | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → autojunket.com HTTP ok | HTTP, autojunket.com | substantive | 5.7 | autojunket.com | substantive | 3.6 |
| q28 | 2026 Toyota Sienna Platinum | What do reviewers think of the Sienna? | 8 | motortrend.com HTTP ok | HTTP, motortrend.com | substantive | 3.2 | motortrend.com | substantive | 3.2 |
| q29 | 2026 Toyota Sienna Platinum | Should I buy this or a Honda Odyssey? | 8 | jdpower.com HTTP ok | HTTP, jdpower.com | substantive | 1.4 | jdpower.com | substantive | 1.4 |
| q30 | 2026 Toyota Sienna Platinum | Any recalls or defects I should know about? | 8 | kbb.com HTTP 403 → kbb.com PW Access Denied, rejected → consumerreports.org HTTP ok | HTTP, consumerreports.org | chrome | 4.5 | consumerreports.org | chrome | 2.6 |
