# Phone login and chat limits: task queue for cloud sessions

This file is the work queue for later Claude Code cloud sessions that (a) put the AI chat under real limits, because the whole site is now free and chat is the only thing that costs money per use, and (b) replace shopper email-and-password sign-in with phone number plus text-message (SMS) code, using Twilio Verify. Each card is one session of work, started with the one-line prompt "Do task S3 of docs/auth_sms/TASKS.md". Read it if you are a cloud session taking a card, the owner deciding what to approve, or a reviewer checking a pull request against its card. Designs: `DESIGN.md` (phone login), `CHAT_LIMITS.md` (chat limits). Today's code: `SURVEY.md`. Stripe, kept dormant: `PAY_AND_STRIPE_AUDIT.md`. Line numbers there were true on `dev` 1.5.24; if dev disagrees, follow the code and say so in the PR.

Nothing here is built yet. The earlier pay cards P1 to P5 are gone: with no paid plans there is nothing to harden. Their job is replaced by cards F1 and C1 to C6. Card ids: F (freeze billing), D (shared helper), C (chat limits), S (sign-in).

## Order of work

1. **Chat limits first, they need no login change:** F1 and D1 in any order, then C1, C2, C3 (chat is limited by account and IP at once, restart-proof), then C5 and C6.
2. **Sign-in foundations, any order:** S1 (numbers and settings), S2 (provider interface and fake provider), S12 (admin authenticator; independent), S14 (Terms and Privacy drafts; independent).
3. **Vendor right after the fake provider:** S3 (Twilio adapter and line-type lookup), S4 (Turnstile and the Twilio webhook).
4. **Data and core:** S5 (tables; owner applies), S6 (verification service), S7 (accounts without an email).
5. **Screens and removal:** S8 (phone pages), S9 (remove shopper password sign-in, staff page), S10 (long sessions, sign out everywhere), C4 (phone and device in chat limits; needs S5 and D1), S11 (claim old account), S13 (change number), S15 (admin tools), S16 (observability), S17 (iOS).
6. **Launch:** S18 (staged rollout, owner-led).

OWNER-GATED cards (a step only the owner can take: vendor account, Railway, production database, legal approval): S3, S4, S5, C1, S12 (enrolment), S14, S18.

## Rules for every card

1. Branch `claude/<card id lower case>-<slug>` from `dev`, one card per session and per pull request. Open the PR into `dev` with REST (`gh api repos/asarrafi47/DealershipScanner/pulls`; GraphQL is blocked). Never push `main`, `dev` or tags.
2. Before the PR: `ruff check .`, the offline test tier, and for database cards the Postgres tier through `scripts/cloud/with_pg.sh` (`docs/CLOUD.md`). Run long tiers in the background and poll. Every acceptance bullet becomes at least one named test; a bullet that cannot run offline is checked by hand and the PR says how.
3. Add a line under `## [Unreleased]` in `CHANGELOG.md`. Do not edit `VERSION` by hand; if the pre-push hook demands a bump on a second push, run `scripts/bump_version.sh patch` and say so in the PR.
4. No network from tests and no real text message from a cloud session, ever. The fake provider, a fake Turnstile verifier and a fake HTTP layer are the only things tests may use. No token, key, password, real phone number or database address in the repository, tests, docs or PR text. Test numbers use the fictional US range `+1 202 555 01xx`.
5. New tables are migrations only (`migrations/README.md`): never runtime DDL, never SQLite (`backend/tests/test_no_sqlite.py`). Take the next free migration number when you build the card (`ls migrations`; re-read the registry at `docs/NEXT_PHASES_PLAN_2026_10.md:504-522`; `dev` ends at V029 and V030 to V038 are reserved); never edit an applied file.
6. UI: no cards, boxes or pill badges. Plain headings and lines, underline fields, one flat button, text links, sentences for states (`DESIGN.md` section 15).
7. Everything new is off or inert until its flag is on: phone routes answer 404 while `AUTH_PHONE_LOGIN_ENABLED` is unset; the chat gate is on by default but cannot lock out anyone while limits are at their defaults and the settings table is empty. A test per route proves both.
8. Never log a phone number, a code, a message text or vendor response text (`DESIGN.md` section 17); a test greps captured logs for a test number and a test code.
9. Names of environment variables only, never values, in code comments, docs and PR text.
10. Sizes: S about 150 changed lines or less; M up to about 500 lines or two layers; L larger and must be split before starting.

## Freeze and helpers

### F1. Freeze billing: the site is free (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** `backend/config.py`, `backend/utils/production_security.py`, `backend/billing/access.py` (docstring and one comment block only), `frontend/templates` upsell spots if any still show with billing off, `README.md` or `docs/` note, tests `backend/tests/test_billing_frozen.py`.
- **Build:** a test that proves the shipped default is billing off (`Config.billing_stripe_enabled()` false with no variables) and that with it off no template shows a price, plan name or "upgrade" text, `/premium` and `/billing/*` redirect or 404 as in `PAY_AND_STRIPE_AUDIT.md` section 2, and both Stripe webhooks answer 404. A boot warning if production starts with `BILLING_STRIPE_ENABLED` on ("billing is dormant by owner decision"). Comment in `catalog.py` and `access.py` that features are not gated by plan any more. Do not delete any billing code, column or test.
- **Acceptance:** the test above; the existing billing tests still pass unchanged.
- **Pitfalls:** with billing off every signed-in user already passes every feature check (`B/billing/access.py:329-340`), so no behaviour changes; keep it that way. Leave the `users` plan columns alone (SQLite).
- **Owner steps:** keep `BILLING_STRIPE_ENABLED` unset on Railway.

### D1. Device cookie and the real client address (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** create `backend/utils/device_id.py` (`ensure_device_cookie`, `device_hash`), extend `backend/utils/client_ip.py` with `phone_client_ip(request)`; tests `backend/tests/test_device_id.py`, `test_client_ip_cf.py`.
- **Build:** `sc_dev`: random 128-bit value in a signed, HttpOnly, SameSite=Lax cookie kept 400 days, set by a `before_request` only on the phone and chat routes (not on every page). `phone_client_ip` returns the `CF-Connecting-IP` header only when the existing trusted-proxy check (`TRUST_PROXY_HEADERS`, `TRUSTED_PROXY_CIDRS=cloudflare`) says the request came through Cloudflare's ranges; otherwise it returns `client_ip()`; IPv6 counted by /64. Hashes use `PHONE_HASH_PEPPER` (HMAC-SHA256).
- **Acceptance:** a request with the header and a Cloudflare peer uses the header; the same header from a non-Cloudflare peer is ignored; a forged `X-Forwarded-For` does not change the result; the cookie is signed, HttpOnly, Secure in production; a tampered cookie yields a new device; hashes are stable for one pepper and differ for another.
- **Pitfalls:** `client_ip.py:1-33` explains why a direct hit on the Railway address can forge headers; do not weaken that. Without the pepper in production, fail at boot only when a feature that needs it is on.
- **Owner steps:** confirm the site is proxied by Cloudflare and whether the `*.up.railway.app` address is removed (question 4 in `DESIGN.md`).

## Chat limits (details in `CHAT_LIMITS.md`)

### C1. Settings table and usage table (M)
- **Depends on:** none. **Owner-gated:** yes (apply the migration).
- **Files:** `migrations/V0NN__chat_usage.sql` (`chat_usage_buckets`, `app_settings`, section 4 of `CHAT_LIMITS.md`), `backend/db/repositories/chat_usage_repo.py`, `backend/settings_store.py` (reader with 30-second cache: row, then environment, then default), tests `backend/tests/test_chat_usage_repo.py` (Postgres tier), `test_settings_store.py`.
- **Build:** the two tables; repository functions: reserve (atomic upsert and check across several subjects in one transaction, rollback if any would exceed), true-up, give back, add denied, read day and hour totals, delete rows older than 35 days. Settings reader with validation (non-numbers, negatives and absurd values ignored with one log line).
- **Acceptance:** two concurrent reserves at the limit let exactly one through; a reserve over a limit changes no counter; true-up replaces estimate with real numbers; hour and day buckets start at the right instant in `America/New_York` including the days clocks change; settings precedence row > environment > default; a bad value falls back; cleanup removes only old rows; the migration applies twice cleanly.
- **Pitfalls:** no foreign keys (users are SQLite). Never store message text. Day boundary uses the reset time zone setting, hour boundary is the clock hour.
- **Owner steps:** rehearse the migration on the dev database, take a dump, `migrate --dry-run`, then `--apply` on production before the web deploy that ships the gate (`docs/RELEASING.md`). The PR lists these; the card does not run them.

### C2. The budget gate and real spend recording (M)
- **Depends on:** C1. **Owner-gated:** no.
- **Files:** `backend/chat_budget/__init__.py`, `gate.py`, `messages.py`, change `backend/llm/client.py` (small hook in `complete()` that reports `usage` to an active guard through a context variable), tests `backend/tests/test_chat_budget_gate.py`, `test_chat_budget_messages.py`, `test_llm_usage_hook.py`.
- **Build:** `guard(kind, user_id, phone_hash, device_hash, ip_hash, listing_id, weight)` as a context manager: reserve estimated cost and tokens against account, phone, device, IP, listing and global rows; on exit true-up with real tokens (several model calls in one request add up) or give the message back on failure. Settings as in `CHAT_LIMITS.md` section 5. Fail closed for chat if Postgres is unreachable. Denial object with kind, plain sentence and `resets_at` (hour: next top of the hour; day: next midnight in the reset time zone; site budget: next midnight). Cost uses the price settings; the token cap works without prices. Sentences exactly as in section 7 of `CHAT_LIMITS.md`.
- **Acceptance:** each limit kind denies at exactly the limit and gives the right sentence and reset time; strictest wins when several apply; admins skip per-account limits but not the site budget; a refused model answer or an exception gives the message back; two model calls in one request record both; the site budget stops everyone with the budget sentence; with the hook inactive `complete()` behaves exactly as before (existing LLM tests pass); fake clock used throughout.
- **Pitfalls:** the hook must never raise into the model call. Web-research messages weigh 3 (setting). Do not import Flask in the gate (pass plain values).
- **Owner steps:** none.

### C3. Wire the four chat routes (M)
- **Depends on:** C2, D1. **Owner-gated:** no.
- **Files:** `backend/routes/cars_pages.py`, `backend/routes/ai_chat_bp.py`, `backend/routes/ai_narrate_bp.py`, `backend/utils/car_chat_policy.py`, the chat panel script and template that print errors (find with `user_chat_limit_reached`), tests `backend/tests/test_chat_limits_routes.py`, updates to existing chat tests.
- **Build:** replace the in-memory daily buckets (`chat:daily:*`, `aichat:daily:*`, `cmp:daily:*`) with `guard(...)` in car chat, compare chat, sidebar chat and narration; keep the per-minute in-memory burst limits; keep the old error codes (`user_chat_limit_reached`, `listing_chat_limit_reached`) and add `chat_budget_reached`, `chat_paused`, each with `message` and `resets_at`; the panel prints `message` as plain text. One shared set of buckets for all chat routes (not three).
- **Acceptance:** the 11th question in an hour is refused with the hour sentence and a reset time on the hour; a restart (new app instance, same database) does not reset counts; the sidebar and the car page share one allowance; the site budget stops all four routes; refusal responses are 429 with the existing shape plus the new fields; signed-out requests still get the existing login response.
- **Pitfalls:** `api_ai_chat` may call the model twice (rewrite then answer); count one message. The car chat route checks the listing limit before loading the car; keep order sensible so a 404 does not cost a message. Narration is a GET: it must not be callable cross-site to burn budget (it needs the session cookie, SameSite=Lax; check).
- **Owner steps:** set a spend limit at the model provider's console as the second line of defence.

### C4. Phone and device in chat limits (S)
- **Depends on:** C3, S5, S8. **Owner-gated:** no.
- **Files:** `backend/chat_budget/gate.py`, route call sites, tests `backend/tests/test_chat_limits_phone_device.py`.
- **Build:** pass the account's phone hash (read from the identity row, cached per request) and the device hash into the guard; per phone and per device counters apply across accounts; deleting an account does not clear the phone counter.
- **Acceptance:** two accounts on one device share the device allowance; a new account on a number that already used its allowance gets none until reset; accounts under 24 hours old get half; staff accounts without a phone use account and device only.
- **Pitfalls:** one number belongs to one account, so "two accounts on one number" only happens after delete and re-sign-up; test that path.
- **Owner steps:** none.

### C5. Admin usage page and settings form (M)
- **Depends on:** C2. **Owner-gated:** no.
- **Files:** `backend/dealer/admin/chat_usage_hub.py` (new hub like `users_hub.py`), register in `backend/dealer/admin/routes.py`, template `frontend/templates/admin/chat_usage.html`, nav link, tests `backend/tests/test_admin_chat_usage.py`.
- **Build:** `/admin/chat-usage` as in `CHAT_LIMITS.md` section 8: today so far, last 14 days, top 20 accounts (id and username only), denial counts by kind, settings table with source (row, environment, default) and an edit form, "pause chat now" button. Site admins only (re-checks the database role, like `_require_site_admin`), CSRF form token, every change logged with admin id and the old and new value (no secrets involved).
- **Acceptance:** a non-admin gets the normal redirect; an edit changes the live limit within the cache time; a bad value is refused with a plain sentence; pause stops chat and shows the paused sentence; the page contains no phone number, message text or card/box markup (a test checks the template has none of the box classes).
- **Pitfalls:** add the new POST to the CSRF form lists (`B/web/security.py:49-98`). Plain tables only.
- **Owner steps:** none.

### C6. Nightly totals and alerts (S)
- **Depends on:** C2. **Owner-gated:** no (scheduling is S18).
- **Files:** `backend/scripts/chat_budget_nightly.py`, tests `backend/tests/test_chat_budget_nightly.py`.
- **Build:** prints yesterday's totals (messages, tokens, dollars, denials by kind), deletes rows older than 35 days, and exits non-zero when the budget was reached or an alert percentage was passed, so a scheduled routine can notify the owner. In the web process the gate logs one `chat_budget` line when a day first passes each alert level.
- **Acceptance:** exit codes for under, over-alert and budget-reached days; deletion respects the age; log lines contain no message text or names.
- **Owner steps:** none here; S18 schedules it.

## Sign-in cards

### S1. Number handling and settings (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** create `backend/auth/phone/__init__.py`, `numbers.py`, `config.py`; `requirements-web.txt` (`phonenumbers`); tests `backend/tests/test_auth_phone_numbers.py`, `test_auth_phone_config.py`.
- **Build:** `normalize(raw)` to E.164 with the `phonenumbers` library; refuse invalid, premium-rate, shared-cost and countries outside `SMS_ALLOWED_COUNTRIES` (default `US`); `mask()` gives "ending 23"; `hash_phone()` and `hash_ip()` with `PHONE_HASH_PEPPER`. `config.py` reads every phone variable in `DESIGN.md` section 20 with its default and exposes `enabled()`. Production boot check: flag on with the fake provider, or a missing pepper, Turnstile or Twilio value, refuses to start (extend `backend/utils/production_security.py`).
- **Acceptance:** `(202) 555-0123`, `202-555-0123` and `+12025550123` give one value; a UK number is refused; letters are refused; `enabled()` is false when unset; the boot check fails closed; no function logs its input.
- **Pitfalls:** `+1` also covers Canada and Caribbean numbers; decide by the library's country, not the prefix.
- **Owner steps:** none.

### S2. Provider interface and fake provider (S)
- **Depends on:** S1. **Owner-gated:** no.
- **Files:** `backend/auth/phone/provider.py` (`StartResult`, `CheckResult`, `LineInfo`, `PhoneVerifyProvider`), `fake_provider.py`, `registry.py`; test `backend/tests/test_auth_phone_provider.py`.
- **Build:** the interface in `DESIGN.md` section 3. The fake approves `000000`, keeps an outbox, can fail, throttle, report a VoIP line, or stall; refuses to load in production.
- **Acceptance:** right code approves, wrong code does not; the outbox shows one message; failures return short reason codes with no vendor text; the registry refuses the fake in production.
- **Pitfalls:** no vendor or Flask imports in this package's interface modules.
- **Owner steps:** none.

### S3. Twilio Verify adapter and line-type lookup (M)
- **Depends on:** S2. **Owner-gated:** yes (vendor account and variables).
- **Files:** `backend/auth/phone/twilio_verify.py`, `registry.py`, `requirements-web.txt` (`twilio`), `.env.example` (names only), tests `backend/tests/test_auth_phone_twilio_verify.py` with a fake client object.
- **Build:** `start` = Verify v2 create verification, channel `sms`; `check` = Verify v2 verification check; `lookup` = Lookup v2 with `line_type_intelligence`, mapping fixed and non-fixed VoIP and landline to refuse and caching by phone hash for 30 days (in memory is not enough: use the `auth_phone_sends`/lookup cache from S5, so write it behind a small interface and wire it in S6). Credentials from `TWILIO_ACCOUNT_SID` plus `TWILIO_AUTH_TOKEN`, or `TWILIO_API_KEY_SID` plus `TWILIO_API_KEY_SECRET`, and `TWILIO_VERIFY_SERVICE_SID`. 5-second timeouts; one retry on start for a network error only; vendor error text never leaves the adapter.
- **Acceptance:** with a fake client: approved, wrong code, expired, max attempts, throttled (429), network error and a VoIP lookup each map to the right result; no secret, number or code in logs or exception text; the registry returns the adapter only when the variables exist, and either credential style works.
- **Pitfalls:** a cloud session cannot test the real service; the PR says so. The vendor's template and fraud settings live in its console.
- **Owner steps:** create the Twilio account and a Verify service named `Sarrafi Cars` (code length 6); turn on the service's fraud protection; allow US only in geographic permissions; check in the console whether the login text ("Sarrafi Cars: Your verification code is ... It expires in 5 minutes. Do not share this code with anyone.") needs a custom template and get it approved; confirm the code lifetime matches the text (5 versus 10 minutes, `DESIGN.md` section 1); enable Lookup line type; paste the variable values into Railway on the dev service first, never in chat or the repository; set a usage alert and a spend limit at Twilio under the 15-dollar daily cap; review Twilio's data retention and US text-program registration needs.

### S4. Turnstile check and Twilio webhook (S)
- **Depends on:** S2. **Owner-gated:** yes (Cloudflare keys, webhook setting).
- **Files:** `backend/auth/phone/turnstile.py`, `backend/auth/phone/webhook.py`, route registration in `backend/main.py`, `backend/web/security.py` (CSRF exemption listed with a signature note), tests `backend/tests/test_auth_phone_turnstile.py`, `test_auth_phone_webhook.py`, update `test_csrf_coverage.py`.
- **Build:** `verify_turnstile(token, ip)` posts to Cloudflare's siteverify with `TURNSTILE_SECRET_KEY`, 3-second timeout, **fails closed**, rejects a missing token; a fake verifier for tests. `POST /webhooks/twilio/verify` validates `X-Twilio-Signature` with the `twilio` library's request validator against the public URL (`PUBLIC_BASE_URL`), returns 403 on a bad or missing signature, updates the send row outcome and cost estimate from delivery events, ignores unknown events with 200, is idempotent by event id, and logs no bodies, numbers or codes.
- **Acceptance:** valid token passes; missing, bad, replayed and timeout cases all refuse; webhook with a good signature updates the row; a bad signature gets 403 and nothing changes; a captured-log test finds no number or code; the route answers 404 while the flag is off.
- **Pitfalls:** the signature covers the exact URL the vendor used (scheme, host, path, query); behind proxies use the configured public URL, not the request URL. Turnstile token is single use.
- **Owner steps:** create a Turnstile widget for the site's domain in Cloudflare and paste `TURNSTILE_SITE_KEY` and `TURNSTILE_SECRET_KEY` into Railway; in Twilio point the Verify service's status callbacks at `https://<site>/webhooks/twilio/verify`.

### S5. Tables and repository (M)
- **Depends on:** S1. **Owner-gated:** yes (apply the migration).
- **Files:** `migrations/V0NN__auth_phone.sql`, `backend/db/repositories/auth_phone_repo.py`, tests `backend/tests/test_auth_phone_repo.py` (Postgres tier); schema ledger doc if its test requires it.
- **Build:** the four tables in `DESIGN.md` section 4.2 with unique indexes and the E.164 check, a lookup-cache column or small table, and repository functions: create and fetch challenge by `public_id` and nonce; close approved with `UPDATE ... WHERE status='open' RETURNING`; count sends by phone, IP and device in a window and sum cost; record a send; wrong-tries counter and lock; find identity by number or user; insert identity (catch the unique violation); change number; set `sessions_valid_after`; cleanup by age.
- **Acceptance:** two concurrent approvals of one challenge succeed once; a second identity for the same number or user fails on the index; counts respect time windows; wrong tries are not reset by a new challenge and lock at 5; cleanup removes only rows past retention; the migration applies on an empty database and twice.
- **Pitfalls:** no foreign key (users are SQLite); `public.` schema like V029; never store a code; clear the raw number when a challenge ends.
- **Owner steps:** rehearse on the dev database, dump, `migrate --dry-run`, then `--apply` on production before the web deploy that ships the code.

### S6. Verification service (M)
- **Depends on:** S1, S2, S4, S5, D1. **Owner-gated:** no.
- **Files:** `backend/auth/phone/service.py`, `limits.py`, tests `backend/tests/test_auth_phone_service.py`, `test_auth_phone_limits.py`.
- **Build:** `AuthPhoneService(provider, turnstile, repo, clock, config)` with `start`, `check`, `resend`, `cancel` and the order of checks in `DESIGN.md` section 6; limits and caps from section 5 (phone 3 per 15 minutes and 5 a day; IP 5 an hour and 20 a day; device 5 a day; resend 60 s; 5 wrong tries then a 15-minute lock that new codes do not reset; `SMS_DAILY_SEND_CAP`, 150-send alert, spend cap); 5-minute life; session nonce binding; single use; consent flag required; VoIP and landline refused through the lookup; every refusal recorded as a send row with its outcome. Values returned, never vendor text.
- **Acceptance (each a test):** the right code approves once; five wrong tries lock and a resend does not unlock; limits trip at exactly their numbers; the site cap and spend cap block and record; no consent or no Turnstile means no text; a non-US number sends nothing; VoIP sends nothing; a code from another session fails; expiry is exact at 5 minutes; resend before 60 s refuses; identical reply words for known and unknown numbers; captured logs hold no number or code.
- **Pitfalls:** do the single-use close in the database. In-memory limiters reset on restart: the database counts decide.
- **Owner steps:** none.

### S7. Accounts without an email, and claim helpers (M)
- **Depends on:** S5. **Owner-gated:** no.
- **Files:** `backend/auth/phone/accounts.py` (`is_placeholder_email`, `new_phone_username`), `backend/db/users_db/accounts.py` (`save_phone_user`, `delete_user_by_id`), `backend/auth/password_reset.py`, `backend/auth/email_verification.py`, `backend/auth/session.py`, `backend/auth/pages.py` (`/api/auth/me`), `backend/billing/stripe_billing.py` and `checkout_service.py` (no `customer_email` for placeholders), `backend/db/users_db/admin.py`, `frontend/templates/admin/users.html`, `user_form.html`, `backend/auth/phone/merge.py` (re-key shopper tables from one user id to another), tests `backend/tests/test_auth_phone_accounts.py`, `test_auth_phone_merge.py`.
- **Build:** the rules in `DESIGN.md` section 4.3; `save_phone_user` modelled on `save_oauth_user` (`accounts.py:260`); a by-id delete that works without an email; "no email" display; `merge.py` covering `saved_cars`, `saved_searches`, `user_hidden_dealers`, `user_search_history`, `car_comments`, `dealer_comments`, `comment_attachments`, `dealer_reviews`, `car_view_history`, `car_compare_history`, `compare_sessions`, `search_events` (survey 1.4), with a test that fails if a new table with a `user_id` column is not listed in `merge.py` or an explicit "not merged" list.
- **Acceptance:** a phone user can be created and deleted by id from the admin hub; reset and verification mail skip placeholders and reveal nothing; checkout and session carry no email for them; placeholders are unique and never shown in any page or JSON; merge moves every listed table and handles the unique pairs (`UNIQUE(user_id, car_id)` and similar) without losing the older row; existing email users are unaffected; `test_username_email_collision.py` still passes.
- **Pitfalls:** the unique index is on `lower(email)`; placeholders use random hex. Do not show the generated username as a public review name without letting the person set a display name (`dealer_reviews.py:33-43`). The SQLite `users` schema is not changed.
- **Owner steps:** none.

### S8. Phone pages and routes (M)
- **Depends on:** S6, S7. **Owner-gated:** no.
- **Files:** `backend/auth/phone/routes.py`, registration in `backend/main.py`, `backend/web/security.py` (CSRF lists), `backend/mobile/contract.py`, `ios/docs/API_CONTRACT.md`, templates `login_phone.html`, `login_phone_code.html`, tests `backend/tests/test_auth_phone_routes.py`, updates to `test_csrf_coverage.py` and `test_mobile_api_contract.py`.
- **Build:** the screens and routes of `DESIGN.md` section 15 including the consent checkbox (unticked, with the owner's exact text), the Turnstile widget (`TURNSTILE_SITE_KEY`), the code page with a 60-second resend countdown, "use a different number", JSON twins; after a good code: create or find the account (section 10 of the design), refuse admin and dealer roles with the generic failure, `session.clear()`, `finalize_app_session`, set `auth_method` and `auth_at`, `post_login_redirect()`. Consent text version constant. While the flag is off, `/login` is unchanged.
- **Acceptance:** with the fake provider and fake Turnstile a new number signs up and the second visit signs in as the same account; the send button path refuses without the ticked box; wrong, expired and other-browser codes all fail with one message; admin-role and suspended accounts get the generic failure and a log event; every new route is 404 with the flag off; CSRF coverage and the mobile contract tests pass.
- **Pitfalls:** `login_page` ignores `next` (`SURVEY.md` section 8); do not invent another redirect scheme. Use `phone_client_ip` (D1). No fake-provider hints in production responses.
- **Owner steps:** none.

### S9. Remove shopper password sign-in and add the staff page (M)
- **Depends on:** S8. **Owner-gated:** no.
- **Files:** `backend/auth/pages.py`, `backend/auth/app_registration.py`, `backend/auth/google_oauth.py`, `backend/auth/apple_oauth.py`, `backend/auth/password_reset.py`, `backend/billing/routes.py` and templates that link to `/login` or `/register` (`login.html`, `register.html`, `_oauth_signin.html`, nav partials), `frontend/templates/staff_login.html`, tests `backend/tests/test_shopper_signin_removed.py` and updates to the existing login, registration, OAuth and reset tests.
- **Build:** only when `AUTH_PHONE_LOGIN_ENABLED` is on: `/login` is the phone page; `GET/POST /staff/login` is the old password form and `POST /api/auth/login` accepts only staff and dealer roles (shopper accounts get the same generic failure as a wrong password); `/register` and `POST /api/auth/register` stop creating shopper accounts (redirect; JSON 410 `use_phone_sign_in`); reset and verification pages only for staff and dealers; Google and Apple routes and buttons hidden and refused for shoppers unless `AUTH_OAUTH_SHOPPERS_ENABLED`; dealer register and login unchanged.
- **Acceptance:** a shopper password cannot open a session by any route; a staff or dealer password still can; registration cannot create a shopper; the OAuth buttons are gone; with the flag off every old test passes unchanged; no response tells a caller that an account exists or uses a phone.
- **Pitfalls:** keep the account lockout and dummy-hash timing behaviour (`SURVEY.md` section 5.3). iOS still calls these routes (S17).
- **Owner steps:** answer the Google and Apple question (`DESIGN.md` section 11) before this card is started.

### S10. Long sessions and sign out everywhere (S)
- **Depends on:** S5. **Owner-gated:** no.
- **Files:** `backend/main.py`, `backend/auth/session.py`, `backend/billing/access.py` (profile check), `backend/routes/account.py`, tests `backend/tests/test_session_lifetime.py`.
- **Build:** shopper session lifetime `SESSION_DAYS_SHOPPER` (default 60, sliding); staff and dealer sessions rejected after `SESSION_DAYS_STAFF` (default 14) using `auth_at`; "sign out everywhere" button on the account page sets `sessions_valid_after`; the per-request check rejects an older phone session (60-second cache); set `auth_method` and `auth_at` in all remaining login completions.
- **Acceptance:** a shopper cookie 40 days old works and one 61 days old does not; a staff cookie 15 days old is refused; after sign-out-everywhere an older cookie is refused and a newer one works; no change for sessions without `auth_at` other than the staff cap.
- **Pitfalls:** do not log anyone out on deploy: sessions without `auth_at` stay valid for their cookie lifetime.
- **Owner steps:** none.

### S11. Claim an old account (M)
- **Depends on:** S7, S8. **Owner-gated:** no (needs working email).
- **Files:** `backend/auth/phone/claim.py`, routes in `backend/auth/phone/routes.py`, templates `login_claim.html`, tests `backend/tests/test_auth_phone_claim.py`.
- **Build:** `DESIGN.md` section 12: "had an account before?" line; email form that always answers "if that email has an account, we sent a link"; a 30-minute one-time token (reuse the reset-token pattern, peppered hash); the link asks for the phone number, runs the normal code check with `purpose='claim'`, then attaches the verified number to the old account and signs in as it; if the number already belongs to a new empty account, merge it into the old one (S7 merge) or refuse and tell the person to contact support when it holds data; a list for the owner of shopper accounts with a password and no phone older than 90 days.
- **Acceptance:** the old account keeps all its saved data after the claim; an unknown email gets the same words and no mail; a used or expired link fails; a claim cannot attach a number that belongs to another account with data; rate limits apply per IP and per email; no response reveals whether an email exists.
- **Pitfalls:** the email sender must be configured (`RESEND_API_KEY`, `RESEND_FROM`); in tests use `MFA_DELIVERY_MODE=test`. Never log the email or token.
- **Owner steps:** confirm Resend is set up and the sending domain verified (`docs/REMEDIATION_PLAN_2026_10.md` phase 16 owner step). If you pick "start fresh" instead (`DESIGN.md` section 12), skip this card and only keep the 90-day list.

### S12. Authenticator step for admin accounts (M to L, split if needed)
- **Depends on:** none. **Owner-gated:** yes for enrolment (the owner enrols the real admin accounts).
- **Files:** `backend/db/admin_users_db.py`, `backend/db/users_db/auth.py`, `backend/dev/routes.py`, `backend/auth/pages.py` (`/staff/login`), `backend/dealer/admin/routes.py`, new templates, a new migration for encrypted seeds and recovery codes, tests.
- **Build:** time-based one-time code (authenticator app) as a second step for `admin_users` (the `/dev` console) and for app users with role `admin` (the `/admin` hubs and `/dev` pass-through). Enrol with a QR code, 10 single-use recovery codes, the seed **encrypted at application level** before it is stored (the old plaintext `totp_secret` column is not used; `docs/USERS_PG_CUTOVER.md` encryption section), lockout after 5 wrong codes, per-account rate limit in Postgres, the break-glass password path kept (`ADMIN_USERNAME`, `ADMIN_PASSWORD`, V029). Dealers stay password-only (owner decision).
- **Acceptance:** an enrolled admin's password alone opens neither `/dev` nor `/admin/users`; the pass-through route also asks for the code; recovery codes work once; five wrong codes lock; an admin who has not enrolled is forced through enrolment on next sign-in; the break-glass path is tested.
- **Pitfalls:** the removed 2FA left dead helpers (`SURVEY.md` section 3.6) and the registry plans to drop the old columns; coordinate numbering and do not revive the plaintext column. `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` is a second door into `/dev`; cover both. Session `mfa_ok` is set to true unconditionally today (`B/auth/session.py:110`); make it mean something for staff.
- **Owner steps:** enrol each admin with an authenticator app, store the recovery codes offline, keep one break-glass password in a safe place.

### S13. Change number from the account page (S)
- **Depends on:** S8, S10. **Owner-gated:** no.
- **Files:** `backend/auth/phone/stepup.py` (`require_recent_auth(max_age_s=600)`), routes and template `account_phone.html`, `backend/routes/account.py`, tests `backend/tests/test_auth_phone_change.py`.
- **Build:** change number: fresh code to the old number if the session is older than 10 minutes, then a code to the new number (`purpose='change'`, all the normal limits, Turnstile and lookup apply); replace the identity in one transaction; set `sessions_valid_after`; notice to a real email (last two digits only) when one exists; refuse a number that belongs to another account with the neutral message.
- **Acceptance:** an old session cannot change the number without a fresh code; a fresh one can once; the old number stops working immediately; other sessions are signed out; the notice is sent for a real email and never for a placeholder.
- **Pitfalls:** do not let step-up loop; remember the target in the session, not in the URL.
- **Owner steps:** none.

### S14. Draft Terms, Privacy Policy and consent page (S)
- **Depends on:** none. **Owner-gated:** yes (you approve the wording; lawyer review recommended).
- **Files:** `backend/routes/site_misc.py` (`/terms`, `/privacy`, `/sms-consent`), templates `terms.html`, `privacy.html`, `sms_consent.html`, footer links, tests `backend/tests/test_terms_privacy.py`.
- **Build:** plain pages, no boxes. Content written from what the site really stores (`SURVEY.md`) and `DESIGN.md` section 16: the phone number is personal data and what it is used for (sign-in codes only; no sale or sharing for marketing; vendors named by role: Twilio, Cloudflare); how long each thing is kept; **deletion of account data within 30 days of a request**, and the contact address; the text-program wording (message and data rates, frequency, STOP and HELP, opt-in record); the exact consent line from the phone page; chat-limit and acceptable-use notes; a "last updated" date and a version string that matches `consent_text_version`.
- **Acceptance:** the three pages return 200 and are linked from the footer, the phone page and the staff page; the consent line on the phone page is the same text as on `/sms-consent`; the PR text marks them as drafts for the owner.
- **Pitfalls:** this is not legal advice; do not claim certifications. Text-program registration is done at the vendor.
- **Owner steps:** read, edit and approve the wording; have it reviewed before S18; decide the contact address.

### S15. Admin tools for phone numbers (S)
- **Depends on:** S7, S10. **Owner-gated:** no.
- **Files:** `backend/db/users_db/admin.py`, `backend/dealer/admin/users_hub.py`, `frontend/templates/admin/users.html`, `user_form.html`, tests `backend/tests/test_admin_users_phone.py`.
- **Build:** show only the last four digits; "replace phone number" for support after proof (texts a code to the new number, requires a free-text reason, writes an audit line with admin id and account id but no number, signs the account out everywhere); "clear phone" for abuse; a read-only list of claim conflicts and of old accounts waiting 90 days.
- **Acceptance:** non-admins cannot reach the actions; audit lines contain no number; the user list never shows a full number; a replaced number takes over the old identity row only after its code is approved.
- **Pitfalls:** six copies of `_require_site_admin` exist (`SURVEY.md` section 8); reuse the one in `users_hub.py`.
- **Owner steps:** write the support checklist for "proof" (the design suggests the claim email, a saved search or car, the sign-up date).

### S16. Observability, retention and caps page (S)
- **Depends on:** S6. **Owner-gated:** no (scheduling is S18).
- **Files:** `backend/auth/phone/events.py`, `backend/scripts/auth_phone_nightly.py`, `backend/dev/routes.py` (read-only text page) with template, tests `backend/tests/test_auth_phone_events.py`.
- **Build:** the `auth_phone` event names and fields of `DESIGN.md` section 17; the 150-send alert logged once a day (plus the 300 cap and 15-dollar spend cap levels); nightly job that deletes by the retention in `DESIGN.md` section 16, writes yesterday's totals, and exits non-zero at the alert level; a plain-text `/dev` page of today's sends against the caps.
- **Acceptance:** captured logs for a whole flow contain event names and hashes cut to 8 characters and nothing else personal; cleanup deletes only rows past retention; the page renders without JavaScript.
- **Owner steps:** none here.

### S17. iOS login (M)
- **Depends on:** S8, S9. **Owner-gated:** yes (app release).
- **Files:** `ios/SarrafiCars/...` (`Features/Auth/AuthHeaderBar.swift`, `Networking/APIClient.swift`, `Models.swift`), `ios/docs/API_CONTRACT.md`, `backend/mobile/contract.py`.
- **Build:** first, shoppers open the web phone page in the existing web view (cookies already shared, `SessionCookieBridge.swift`); remove the email and password fields for shoppers; later native screens may call the JSON twins; the server returns `use_phone_sign_in` to old builds.
- **Acceptance:** a shopper can sign in by phone inside the app; old builds show a readable message, not a crash.
- **Pitfalls:** a cloud session cannot build or run the app; the PR says what was and was not tested.
- **Owner steps:** build, test on a phone and submit a new app version; tell me whether the app is already in the App Store.

### S18. Staged rollout (not code; owner-led)
- **Depends on:** F1, C1 to C6, S1 to S11, S13 to S16 merged and released; S12 and S14 done. **Owner-gated:** yes.
- **Files:** none except an optional `docs/auth_sms/ROLLOUT_LOG.md` the session writes from the owner's notes.
- **Steps:**
  1. Release as `docs/RELEASING.md` says; apply the C1 and S5 migrations to production first; confirm `GUNICORN_WORKERS=1`.
  2. Chat limits first (C-cards): watch the admin usage page for a week with the default numbers; set the dollar cap from real numbers; set the model provider's spend limit.
  3. Dev service with `AUTH_PHONE_PROVIDER=fake`: click through the pages on a phone.
  4. Dev service with real Twilio and Turnstile variables and `AUTH_PHONE_ALLOWED_NUMBERS` set to the owner's own number: sign up, sign in, sign out, change number, five wrong codes, the 60-second resend wait, a VoIP number refused, a non-US number refused; check Twilio's usage page shows exactly the sends you made and the cost matches about 6 cents each.
  5. Production, flag on with the allow-list of two or three people and `SMS_DAILY_SEND_CAP` at 50: one week of nightly summaries.
  6. Raise to the real numbers (300 a day, alert at 150, 15 dollars a day), remove the allow-list; announce the change to existing users with the claim link (S11) and keep the staff page for admins and dealers.
  7. Schedule `backend/scripts/auth_phone_nightly.py` and `backend/scripts/chat_budget_nightly.py` on Railway so a failing exit code notifies you.
  8. Rollback: flag off puts the old form back for shoppers with old passwords; phone-created accounts cannot sign in until it is on again, so warn first.
- **Acceptance:** the log shows each step with date and result; no step skipped.
- **Pitfalls:** do not raise caps and widen the audience on the same day.
