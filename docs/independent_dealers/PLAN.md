# Independent dealers: plan to include them

Companion to `ANALYSIS.md` (why they are missing). The work is cards L26 to L34 in
`docs/scan_loop/TASKS.md`. Everything here keeps the rules of `docs/NETWORK_SCAN_PROCESS.md` (network
traffic over plain HTTP, no headless browser, every dealer touched gets its logs under
`workspace/dealer_logs/<dealer_id>/`) and of `CLAUDE.md` (NHTSA vPIC outranks the feed; one scanner
per lock file; releases only through `docs/RELEASING.md`). Cloud sessions still never fetch a dealer
site: every probe and scan runs on Railway through request files (`docs/scan_loop/README.md`).

## 0. First step: measure (owner, 5 minutes)

Run the one-line command of `ANALYSIS.md` section 4 and paste the output into a cloud session. It
tells us how many independents are already in the registry, in which states, how many have their
own site, how many already have cars, and how many of their cars production hides. Every number in
section 7 below is replaced by the measured one before any batch runs.

## 1. Where new independents come from, and how we keep non-dealers out

Sources, cheapest first:

1. **Registry rows already known but never scanned.** No new discovery needed. Card L29 lists them
   (read-only), keeps only rows with their own website (not a marketplace or directory page,
   `backend/discovery/normalize.py:87-135`), not a probable non-dealer
   (`backend/discovery/non_dealer_filter.py:166`), not on a host shared with another store, and
   sorted by metro.
2. **Google Places (New) by metro grid**, with the `GOOGLE_MAPS_API_KEY` already on Railway.
   `searchNearby` returns at most 20 places per circle (`backend/discovery/google_places.py:62-63`),
   so card L30 splits any circle that comes back full into four smaller ones until it returns fewer
   than 20 or reaches a minimum radius, and adds `searchText` queries such as "used car dealer in
   <city>" (`backend/discovery/web.py:28`, `:223`). Hard cap per metro with the existing
   `GOOGLE_PLACES_MAX_CALLS` (`google_places.py:67-76`) plus a Google-side daily quota.
3. **State licensed-dealer lists**, where a state publishes them as open data. Only North Carolina
   has a loader today (`backend/discovery/dmv/registry.py:13-15`); which other states publish a
   usable list is **to verify** state by state before any loader is written.

Precision plan (L30): every new discovery batch gets a hand-labelled sample of 50 rows per metro
(car dealer with its own inventory site: yes or no). Target 90% or better; misses become new
patterns in `non_dealer_filter.py` (the earlier restaurant and "Carvana Headquarters" leaks are the
model). Rows also need Google `business_status` OPERATIONAL and a website. The franchise-name filter
of the old Overture discovery is not used again.

## 2. Platforms and templates

Platforms with evidence of serving independents (fixtures and code): **Overfuel** (2 of the 6
fixture independents, `backend/scanner/synth/platforms/overfuel.py`), **server-rendered card pages**
(`html_cards.py:17-24`, Quantum Auto Sales), **Dealer.com** and **Dealer eProcess** (1 each in the
fixtures), **autoWALL** (`autowall.py`, a franchise store today, a generic HTML list), and
**nabthat** (JSON-LD, `nabthat.py`). Platforms said to be common among used lots but with no
fingerprint or template in `backend/scanner`: DealerCenter, Frazer, Carsforsale.com-hosted sites,
Dealer Car Search, AutoManager, V12. All of these are **to verify**: their share is unknown until
a census.

Card L31 does the census first: probe requests (L21 evidence packs) for 30 candidates from L29,
6 per request, fingerprint each, count dealers per platform. Then one template per platform, one
session each, in order of dealers covered. One platform template serves every dealer on that
platform (the reason `docs/scan_loop/README.md` groups the queue by fix unit), so the top two or
three platforms should cover most scannable independents. A site that shows its cars only inside a
third-party iframe or through browser-only JavaScript, with no replayable endpoint, is not scanned;
that dealer goes to the second route (section 5).

## 3. Quality: what "ok" means for a 15-car lot

Today a first scan under 20 cars is `no_rows` (`backend/scanner/pipeline/assess.py:138`, `:148-150`)
and used-only is `inaccurate` unless a person lists it (`assess.py:170-177`). Card L27 changes both,
owner-gated:

- **Small lot.** With the site's own total T from the walk report (L16), the floor becomes the
  smaller of 20 and 90% of T, never under 3; a 15-car site that shows 15 cars and returns 14 passes.
  Without T the 20-row floor stays, but an independent under it is `cannot_assess` for volume, not
  a failure.
- **Used only.** Passes when the dealer is independent by name or host (not by the used-only signal,
  which would be circular) and the site's own condition census or walk shows no new section. The
  allow lists `one_condition_ok.txt` and `accepted_gaps.json` stay for edge cases.
- **No VIN, no row.** Listings without a VIN are not stored (the `cars` table is one row per VIN).
  A lot whose pages carry no VIN goes to the second route.
- **Key fields and NHTSA.** The same price, colour and trim floors (90%) and the vPIC invariants
  apply; independents get no discount on accuracy. Trim and colour gaps are what hide cars on the
  live site (`backend/utils/listing_completeness.py:49-70`), so the VDP pass and the vPIC fill matter
  more here, not less.

## 4. Rollout through the loop on Railway

The loop cannot onboard a dealer today: requests name only roster dealers (`unknown_dealer`,
`docs/scan_loop/BUNDLE.md:226`). Card L28 adds an owner-written onboarding list in the private data
repository (`config/onboard.json`, writer O, like `config/loop.json`) that the roster rule admits for
`probe`, `validate` and `rescan` requests, with the dealer's URL. Onboarding dealers run only as
requested runs, never in the nightly fleet, until they pass twice; then they hold cars and join the
fleet roster by the existing rule.

Batches (L33): 6 dealers per request (the loop's cap), at most 1 onboarding request per day at first
inside the existing 4 cycles, one metro at a time. Per dealer the bundle records platform, cars
stamped, verdict, minutes and requests used; per batch, the yield (dealers ok, cars added).

Cost and time: a full fleet of about 458 dealers costs about 0.75 USD and 160 minutes on 8 shards
(`docs/scan_loop/README.md` stage 1), so a batch of 6 small lots is under 1 cent and a few minutes.
Google Places discovery is billed per call; the price is **to verify** in the Google Cloud console
before the first metro; the call cap bounds it.

**Stop rule.** Stop a metro after 2 batches in a row with fewer than 2 `ok` dealers of 6, or fewer
than 10 cars per added dealer. Stop all onboarding at once, and tell the owner, on any VIN moved from
an existing dealer, a fleet gate HOLD or FAIL that touches an onboarding dealer, a robots or terms
problem, or a complaint from a dealer.

## 5. Second route: dealers with no scannable site

Many small lots have no own site, or only a marketplace page. They can list their own cars: the
dealer portal already exists (`backend/db/dealer_portal_db.py`, table `dealer_vehicles`) and
`backend/dealer/portal_sync.py` already writes portal cars into `cars` through the scanner's own
upsert, with a VIN-collision guard. What is missing is verified sign-up: a dealer claims its registry
row and proves it, for example by a code sent to the phone on its Google listing. That builds on the
phone-verification cards (S1 to S6) of `docs/auth_sms/TASKS.md` and the marketplace cards another
session is writing on branch `claude/decisions-1114` (not on `dev` at the time of writing; link them
by name once merged). Card L34 is the bridge; it does not edit `docs/auth_sms/`.

## 6. Legal, terms, robots, rate limits

- **Own site only.** We read a dealer's own website. We never scrape a marketplace (Cars.com,
  Autotrader, CarGurus, TrueCar, Facebook) or a site whose terms or robots rules forbid it.
- **Robots.** The dealer scanner reads no `robots.txt` today (no match in `backend/scanner`). Card
  L32 adds a check before any onboarding probe: if the inventory path is disallowed, the dealer is
  not scanned and is offered the second route. The rule is recorded in the dealer's `discovery.md`.
- **Terms.** A person reads the terms page of each new platform family once (L31); a platform whose
  terms forbid automated access is not templated. National chains (CarMax, Carvana) stay out by
  default (`ANALYSIS.md` section 6).
- **No evasion.** No login walls, no challenge solving, no browser. The scanner's TLS-impersonated
  fetch (`backend/scanner/synth/http.py:97`) is an existing escalation for franchise sites; for
  onboarding dealers it is off unless the owner decides otherwise (question 4 below).
- **Rate.** Probes keep the existing 4-second spacing (`SCANNER_SYNTH_FETCH_DELAY`,
  `backend/scripts/national_scan.py:28`) and a page cap; one dealer at a time per host.
- **Google data.** Google Maps Platform terms limit how long Places content other than the place ID
  may be stored; whether our registry columns comply is **to verify** before L30 runs.

## 7. Honest expected gain

What we know: in the fixtures 6 of 172 dealers are independents and 1 scans `ok`; with L27, two more
would likely pass (`lbmboaz-com` if it is truly used-only, `overdrive-usa-com` is a group store and
stays out); the two Overfuel lots need price and trim work. What we do not know: how many
independents a metro has, how many have their own site and which platforms they use. The report
(section 0) and the first census (L31) answer that.

Illustration only, not a forecast (every number is an assumption to replace): a metro with 200
independent lots in the registry, half with their own site, half of those on a platform we can
template, gives about 50 dealers; at a typical 20 to 60 cars each that is 1,000 to 3,000 cars. Small
lots add many dealers but few cars per dealer.

Effort: cards L26 to L30 and L32 to L33 are about 7 sessions; each new platform template 1 session
(L31); each metro batch an analysis session. Owner time: the report run, each onboarding list,
releases and the decisions below.

Risks: low yield per batch (stop rule); sites that need a browser (second route); VIN theft from a
shared feed (single-store hosts only, VIN owner guard); new fleet-gate noise (onboarding runs stay
out of the fleet until they pass twice); Places cost (call cap); terms and robots (L32, own site only).

## 8. Owner decisions (yes or no)

1. Include independent lots, read from their own sites only? Default **yes**.
2. Keep CarMax, Carvana and other national chains out? Default **yes, keep out**.
3. Never scrape marketplaces (Cars.com, Autotrader, CarGurus, Facebook)? Default **yes, never**.
4. Allow the TLS-impersonated fetch for onboarding dealers? Default **no**.
5. Spend on Google Places for a metro grid (capped per metro)? Default **yes, one pilot metro first**.
6. Pass small lots (under 20 cars) and used-only lots when the site's own count agrees (L27)? Default **yes**.
7. Start the self-listing route (L34) only after the phone sign-in cards ship? Default **yes**.
