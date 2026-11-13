# Phone login and pay hardening: task queue for cloud sessions

This file is the work queue for later Claude Code cloud sessions that build text-message (SMS) phone-number login for the Flask site sarraficars.com, and fix the Stripe gaps the audit found. Each card is one session of work, started with the one-line prompt "Do task S3 of docs/auth_sms/TASKS.md". Read it if you are a cloud session taking a card, the owner deciding what to approve, or a reviewer checking a pull request against its card. Design: `DESIGN.md`. Facts about today's code: `SURVEY.md`. Pay side: `PAY_AND_STRIPE_AUDIT.md`. Line numbers in those files were true on `dev` 1.5.24; if dev disagrees, follow the code and say so in the PR.

Nothing here is built yet. Cards `S1` to `S13` are phone login ("S" for sign-in); `P1` to `P5` are pay fixes ("P" for pay) and are independent of login except where a card says so.

## Order of work

1. Foundations, any order: S1 (numbers and settings), S2 (provider interface and fake provider), P1 to P4 (pay fixes; they do not need login).
2. Data: S3 (migration and repository). Owner applies the migration before anything uses it.
3. Core: S4 (service with limits), S5 (accounts without an email).
4. Screens: S6 (login pages), S7 (account page and step-up), S8 (observability and cleanup), S9 (terms, privacy, consent text).
5. Pay link: S10 (trial gated by phone; needs S3 and P3).
6. Admin: S11 (admin tools for phone; separate card S12 for the authenticator second step).
7. Vendor and rollout: S13 (the real adapter, last code card), then the owner-led staged rollout (S14, not code).

OWNER-GATED cards (a step only the owner can take: Railway, vendor account, production write, legal text): S3, S9, S12 (decision), S13, S14, P5 (Stripe dashboard).

## Rules for every card

1. Branch `claude/<card id lower case>-<slug>` from `dev`, one card per session and per pull request. Open the PR into `dev` with REST (`gh api repos/asarrafi47/DealershipScanner/pulls`; GraphQL is blocked). Never push `main`, `dev` or tags.
2. Before the PR: `ruff check .`, the offline test tier and, for database cards, the Postgres tier through `scripts/cloud/with_pg.sh` (see `docs/CLOUD.md`). Run long test tiers in the background and poll. Every acceptance bullet becomes at least one named test; a bullet that cannot run offline is checked by hand and the PR says how.
3. Add a line under `## [Unreleased]` in `CHANGELOG.md`. Do not edit `VERSION` by hand; if the pre-push hook demands a bump on a second push, run `scripts/bump_version.sh patch` and say so in the PR.
4. No network from tests: the fake provider is the only provider tests may use. No real text is ever sent from a cloud session. No token, key, password, phone number of a real person or database address in the repository, tests, docs or PR text. Test numbers use the reserved fictional US range (`+1 202 555 01xx`).
5. New tables are migrations only (`migrations/README.md`): never runtime DDL, never SQLite (`backend/tests/test_no_sqlite.py`). Take the next free migration number at the time (`ls migrations`, re-read the registry in `docs/NEXT_PHASES_PLAN_2026_10.md:504-522`); never edit an applied file.
6. UI: no cards, boxes or pill badges. Plain headings and lines, underline fields, one flat button, text links, sentences for states (`DESIGN.md` section 15).
7. Everything phone-related is off while `AUTH_PHONE_LOGIN_ENABLED` is unset: routes answer 404, nothing reads the new tables, no provider is constructed. A test per route proves it.
8. Never log a phone number, a code or vendor response text (`DESIGN.md` section 18); a test greps captured log output for a test number and a test code.
9. Sizes: S about 150 changed lines or less; M up to about 500 lines or two layers; L larger and must be split before starting.

## Phone login cards

### S1. Number handling and settings (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** create `backend/auth/phone/__init__.py`, `numbers.py`, `config.py`; `requirements-web.txt` (add `phonenumbers`); tests `backend/tests/test_auth_phone_numbers.py`, `test_auth_phone_config.py`.
- **Build:** `normalize(raw, default_region="US") -> (e164, country) | None` using `phonenumbers`; reject invalid, premium-rate and shared-cost types and countries outside `AUTH_PHONE_COUNTRIES` (default `US`). `mask(e164)` returns "ending 23". `hash_phone(e164)` and `hash_ip(ip)` are HMAC-SHA256 with `AUTH_PHONE_PEPPER`. `config.py` reads every variable in `DESIGN.md` section 20 with defaults and exposes `enabled()`; in production it raises at boot if the flag is on with `AUTH_PHONE_PROVIDER=fake` or no pepper (extend `B/utils/production_security.py`).
- **Acceptance:** `(415) 555-0123`, `415-555-0123`, `+14155550123` give one E.164 value; a UK number is refused with the default list; a number with letters is refused; hashes are stable for one pepper and differ for another; `enabled()` is false when the variable is unset; the production check fails closed.
- **Pitfalls:** `+1` also covers Canada and Caribbean countries, so decide by the library's country, not the prefix. Never log the input. Tests must not depend on the real clock or the network.
- **Owner steps:** none.

### S2. Provider interface and fake provider (S)
- **Depends on:** S1. **Owner-gated:** no.
- **Files:** create `backend/auth/phone/provider.py` (`StartResult`, `CheckResult`, `PhoneVerifyProvider`), `fake_provider.py`, `registry.py` (`get_provider(name)`); test `backend/tests/test_auth_phone_provider.py`.
- **Build:** the interface in `DESIGN.md` section 3, including the `generates_code` flag. `FakeProvider` keeps an in-memory outbox, approves a configurable code (`000000` default), can be set to fail, to throttle (`retry_after_s`) or to stall. `get_provider("twilio_verify")` raises "not installed" until S13.
- **Acceptance:** start then check with the right code approves; the wrong code does not; the outbox shows exactly one message; failure injection returns `ok=False` with a short `reason` and no vendor text; the registry refuses the fake in production.
- **Pitfalls:** keep the module free of vendor imports and of Flask imports so it can be tested alone.
- **Owner steps:** none.

### S3. Tables and repository (M)
- **Depends on:** S1. **Owner-gated:** yes (apply the migration).
- **Files:** `migrations/V0NN__auth_phone.sql` (next free number), `backend/db/repositories/auth_phone_repo.py`; tests `backend/tests/test_auth_phone_repo.py` (Postgres tier); update `docs/db_layer/SCHEMA_LEDGER_2026_10.md` if the repo's schema ledger test requires it.
- **Build:** the four tables in `DESIGN.md` section 4.2 with the unique indexes and the E.164 check. Repository functions: create challenge; fetch open challenge by `public_id` and nonce; increment attempts atomically; close approved with `UPDATE ... WHERE status='open' RETURNING`; count sends by phone, IP, device, and sum spend since a time; record a send; find identity by number; insert identity (catch unique violation); delete identity; add and look up trial mark; cleanup by age.
- **Acceptance:** two concurrent approvals of one challenge succeed once; a second identity for the same number or the same user fails on the index; counts and sums respect the time window; cleanup removes only rows older than the given age; the migration applies on an empty database and again with `IF NOT EXISTS` semantics (idempotent) and is inert (no reads) when the flag is off.
- **Pitfalls:** `user_id` has no foreign key until users reach Postgres (`V018:12-15`). Use `public.` schema like V029. Do not store the code or a raw number in `auth_phone_sends`. Do not put `CREATE TABLE` anywhere else.
- **Owner steps:** rehearse on the dev database, take a dump, run `migrate --dry-run`, then `--apply` on production before the web deploy that ships the code (`docs/RELEASING.md`, `docs/NEXT_PHASES_PLAN_2026_10.md:504`). The PR description lists these steps; the card does not run them.

### S4. Verification service (M)
- **Depends on:** S1, S2, S3. **Owner-gated:** no.
- **Files:** `backend/auth/phone/service.py`, `limits.py`; tests `backend/tests/test_auth_phone_service.py`, `test_auth_phone_limits.py`.
- **Build:** `AuthPhoneService(provider, repo, clock, config)` with `start(e164, purpose, user_id, ctx)`, `check(public_id, nonce, code)`, `resend(public_id, nonce)`, `cancel(...)`. Implements section 6 and 7 of the design: code format, 10-minute expiry, 5 attempts, 10 wrong per phone per hour, resend 30 s then 60 s and 3 sends, per phone/IP/device/session limits from `auth_phone_sends`, daily send and spend caps that block, country allow-list, session nonce binding, single use. In the we-hold-the-code mode store only the HMAC. Responses are values (`ok`, `reason`, `retry_after_s`), never provider text. Clock and randomness are injected.
- **Acceptance (each a test):** right code approves once and never again; wrong code five times kills the challenge; ten wrong across challenges blocks the phone; a code from another session nonce fails; expiry is exact at the boundary; resend before 30 s refuses; cap hit blocks and records `blocked_cap`; non-allowed country records `blocked_country` and sends nothing; a provider error leaves no approved state; the same reply words come back for an unknown and a known number (parametrised); `caplog` has no number or code.
- **Pitfalls:** do the single-use close in the database, not in Python. Count a blocked attempt as an attempt, not as a send. Remember the in-process limiter resets on restart: the database counts decide.
- **Owner steps:** none.

### S5. Accounts without an email (M)
- **Depends on:** S3. **Owner-gated:** no.
- **Files:** `backend/auth/phone/accounts.py` (`is_placeholder_email`, `new_placeholder_email`, `new_phone_username`), `backend/db/users_db/accounts.py` (`save_phone_user`, `delete_user_by_id`), `backend/auth/password_reset.py`, `backend/auth/email_verification.py`, `backend/auth/session.py`, `backend/auth/pages.py` (`/api/auth/me`), `backend/billing/stripe_billing.py` and `checkout_service.py` (no `customer_email` for placeholders), `backend/db/users_db/admin.py`, `frontend/templates/admin/users.html`, `user_form.html`; tests `backend/tests/test_auth_phone_accounts.py`.
- **Build:** the placeholder rules in `DESIGN.md` section 4.3, `save_phone_user` modelled on `save_oauth_user` (`accounts.py:260`), compensating delete on an identity race, a by-id delete that does not need an email (`accounts.py:447-518`, `admin.py:481-487`), "no email" display, `session["user_email"]` empty for placeholders, reset and verification mail skip placeholders.
- **Acceptance:** a phone user can be created and deleted by id from the admin hub; password reset for a placeholder account sends nothing and reveals nothing; checkout for a placeholder account passes no `customer_email`; placeholders are unique and never shown in any template or JSON; existing email users are unchanged (existing tests stay green); `test_username_email_collision.py` still passes.
- **Pitfalls:** the unique index is on `lower(email)`; the placeholder has random hex so it cannot collide. Do not show the generated username in public review names without letting the person change it (`dealer_reviews.py:33-43`). SQLite `users` schema is not changed in this card.
- **Owner steps:** none.

### S6. Login pages and routes (M)
- **Depends on:** S4, S5. **Owner-gated:** no.
- **Files:** `backend/auth/phone/routes.py` (blueprint), registration in `backend/main.py`, `backend/web/security.py` (CSRF lists), `backend/mobile/contract.py`, `ios/docs/API_CONTRACT.md`, templates `frontend/templates/login_phone.html`, `login_phone_code.html`, small link in `login.html`; tests `backend/tests/test_auth_phone_routes.py`, update `test_csrf_coverage.py` expectations, `test_mobile_api_contract.py`.
- **Build:** routes in `DESIGN.md` section 15 (web and JSON), `sc_dev` device cookie, session keys `phone_challenge` and `phone_nonce`, sign-in through `session.clear()` then `finalize_app_session` then `post_login_redirect`, role refusal for admin and dealer roles with the generic message, sign-up path through S5, `auth_method` and `auth_at` in the session (also added to password, Google and Apple completions, one line each). No cards, boxes or pills; consent line placeholder text from S9 (use a constant until S9 lands).
- **Acceptance:** with the flag off every new route is 404 and `login.html` shows no phone link; full happy path with the fake provider creates a user then signs in the second time as the same user; wrong code, expired code and another browser fail with one message; admin-role and suspended accounts get the generic failure and a log event; CSRF missing gives the standard failure; `test_csrf_coverage.py` passes; the JSON twins return 401/400 shapes matching the contract; the response for an unknown and a known number is identical.
- **Pitfalls:** `login_page` ignores `next` today (`SURVEY.md` section 8); do not build a second redirect scheme. Read the client IP with `B/utils/client_ip.py`. Keep the fake-provider code out of production responses.
- **Owner steps:** none.

### S7. Account phone page and step-up (M)
- **Depends on:** S6. **Owner-gated:** no.
- **Files:** `backend/auth/phone/stepup.py` (`require_recent_auth`), routes in `backend/auth/phone/routes.py`, `backend/routes/account.py`, `backend/billing/routes.py` (checkout GET becomes POST with CSRF; portal at `account.py:202`), templates `account_phone.html`, `stepup.html`, `account_profile.html` (phone line); tests `backend/tests/test_auth_phone_account.py`, `test_auth_stepup.py`, update billing route tests.
- **Build:** add, change and remove a number (rules in `DESIGN.md` section 11); `require_recent_auth(max_age_s=600)` at checkout, portal, change of phone, email, password and account delete (`DESIGN.md` section 9); password accepted as step-up if the account has one; "your sign-in number changed" email notice to a real email; the "add an email" line for phone-only users; refusal to remove the last way to sign in.
- **Acceptance:** an old session cannot reach checkout or the portal without a fresh proof; after proof it can, once, for ten minutes; a number owned by another account gives the neutral message and a conflict event; removing the number is refused for a phone-only account; the email notice is sent for a real email and not for a placeholder; checkout works by POST with CSRF and the old GET links are updated in templates and the iOS contract.
- **Pitfalls:** do not let step-up become a login loop; remember the target URL in the session, not in a query string. Keep the iOS web view working (cookies only).
- **Owner steps:** none.

### S8. Observability and cleanup (S)
- **Depends on:** S4. **Owner-gated:** no.
- **Files:** `backend/auth/phone/events.py`, `backend/scripts/auth_phone_nightly.py`, `backend/dev/routes.py` (read-only text page) with a template, tests `backend/tests/test_auth_phone_events.py`.
- **Build:** the `auth_phone` event names and fields of `DESIGN.md` section 18; cap warnings at 50% and 80% once per day; a nightly script that deletes by the retention in section 16 and logs yesterday's totals; a plain-text `/dev` page of today's sends against the caps.
- **Acceptance:** captured logs for a full flow contain event names, hashes cut to 8 characters, and no number, code or vendor text; cleanup deletes only rows past retention; the caps page renders with no JavaScript.
- **Pitfalls:** the daily job must be idempotent. Scheduling it on Railway is an owner step (see S14).
- **Owner steps:** none in this card; S14 schedules the job.

### S9. Terms, privacy and consent text (S)
- **Depends on:** none. **Owner-gated:** yes (wording and legal review).
- **Files:** `backend/routes/site_misc.py` (two routes `/terms`, `/privacy`), templates `terms.html`, `privacy.html`, footer links, consent constant in `backend/auth/phone/config.py` or a template partial; tests `backend/tests/test_terms_privacy.py`.
- **Build:** plain pages (no boxes) written from `DESIGN.md` section 16 and from what the site really stores (`SURVEY.md`); the consent line shown next to the phone field, STOP/HELP wording, message-and-data-rates sentence, retention and the trial-mark disclosure.
- **Acceptance:** both pages return 200 and are linked from the footer, the phone page and the register page; the consent line appears on the phone page whenever the flag is on.
- **Pitfalls:** this is not legal advice. The PR marks the text as a draft for the owner's lawyer. Text-program registration for the sender is handled with the vendor (S13).
- **Owner steps:** approve or edit the wording; have it reviewed before S14.

### S10. Trial gated by phone (M)
- **Depends on:** S3, P3. **Owner-gated:** no.
- **Files:** `backend/billing/checkout_service.py`, `backend/billing/trials.py` (new), `backend/auth/phone/accounts.py`, tests `backend/tests/test_trial_phone_gate.py`.
- **Build:** `trial_eligible(user_id)` per `DESIGN.md` section 17 (verified number present, no trial mark for its hash, and the P3 per-user rule); pass `trial_period_days` only when eligible; write the mark when the subscription is created (webhook, not the redirect).
- **Acceptance:** a user without a verified number gets no trial and a plain line says why; a second account using the same number gets no trial; deleting an account keeps the mark; `trial_period_days` appears in the Stripe call only when eligible (assert the call arguments with a fake `stripe`).
- **Pitfalls:** this is a business choice; it ships behind its own flag `BILLING_TRIAL_REQUIRES_PHONE` default off. Billing is off in production today, so nothing changes there.
- **Owner steps:** decide the rule (owner question 8 in the PR).

### S11. Admin tools for phone numbers (S)
- **Depends on:** S5, S3. **Owner-gated:** no.
- **Files:** `backend/db/users_db/admin.py`, `backend/dealer/admin/users_hub.py`, `frontend/templates/admin/users.html`, `user_form.html`; tests `backend/tests/test_admin_users_phone.py`.
- **Build:** show the last four digits only; an admin-only "replace phone number" action with a verification of the new number by code, a required free-text reason, and an audit event with the admin's name; "clear phone" for abuse; a "merge needed" listing of conflict events (read only).
- **Acceptance:** non-admins cannot reach the actions; replace and clear write audit lines without the number; the user list never shows a full number.
- **Pitfalls:** six copies of `_require_site_admin` exist (`SURVEY.md` section 8); reuse the one in `users_hub.py`.
- **Owner steps:** none.

### S12. Second step for admin accounts (decision, then M)
- **Depends on:** none (independent of phone login). **Owner-gated:** yes (decision: do you want it, and how).
- **Files:** to be fixed after the decision: `backend/db/admin_users_db.py`, `backend/db/users_db/auth.py`, `backend/dev/routes.py`, `backend/auth/pages.py`, templates; tests.
- **Build:** authenticator-app step for `admin_users` and for app users with role admin, recovery codes, seed encrypted at the application level before storing (`docs/USERS_PG_CUTOVER.md` encryption section; do not revive the dead plaintext `totp_secret` column), rate limit and lockout, break-glass password path kept (`ADMIN_PASSWORD`, V029).
- **Acceptance:** an admin password alone does not open `/dev` or `/admin/users` once enrolled; recovery codes work once; tests for enrolment, bad code lockout, and the break-glass path.
- **Pitfalls:** the removed 2FA left pieces behind (`SURVEY.md` section 3.6) and the registry plans to drop the columns (V031); coordinate numbering. `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` lets an app admin into `/dev`; the second step must apply to both doors.
- **Owner steps:** answer the question first. This card is not started without it.

### S13. Real provider adapter (M)
- **Depends on:** S2, S4. **Owner-gated:** yes (vendor account and variables).
- **Files:** `backend/auth/phone/twilio_verify.py` (or the chosen vendor's file name), `registry.py`, tests `backend/tests/test_auth_phone_twilio_verify.py` with a fake HTTP layer; `.env.example` (variable names only), `docs/auth_sms/DESIGN.md` (note the final choice).
- **Build:** the two HTTPS calls (start and check) with a 5-second timeout, mapping vendor statuses to our `reason` codes, no vendor text out, no retry on check, one retry on start for a network error only; credentials read from `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_VERIFY_SERVICE_SID`; `generates_code=True` mode. If the vendor is not Twilio the card renames the file and variables and nothing else changes.
- **Acceptance:** with a fake HTTP layer: approved, wrong code, expired, throttled (429) and network error each map to the right result; no secret or number appears in logs or exceptions; the registry returns the adapter only when its three variables exist.
- **Pitfalls:** do not add the vendor's SDK unless it is needed; the adapter must work with `requests`. A cloud session cannot test the real service (no network); the PR says so. The vendor's own fraud guard and rate settings are set in its dashboard (owner steps).
- **Owner steps:** create the vendor account and a verification service; set the service's code length (6), expiry (10 minutes) and any fraud-guard or geographic permissions to US only; paste the three variables into Railway (web service) and, separately, a test credential set for the dev service; check the vendor's data retention and sender registration needs for US text programs (S9 text may be required); set a usage alert at the vendor lower than our cap.

### S14. Staged rollout (not code; owner-led, one session helps)
- **Depends on:** S1 to S9 and S13 merged, and a release of that code. **Owner-gated:** yes.
- **Files:** none, except a rollout log `docs/auth_sms/ROLLOUT_LOG.md` the session may create from the owner's notes.
- **Steps:**
  1. Release as `RELEASING.md` says; apply the S3 migration to production first; confirm the web service has `GUNICORN_WORKERS=1`.
  2. Dev service, `AUTH_PHONE_PROVIDER=fake`, flag on: click through the pages on a phone.
  3. Dev service with real credentials, `AUTH_PHONE_ALLOWED_NUMBERS` set to the owner's own number: sign up, sign in, sign out, add the number to an email account, change it, try five wrong codes, try the resend cool-down, check the vendor usage page shows exactly the sends you made.
  4. Production with the allow-list of two or three people and a low `AUTH_PHONE_DAILY_SEND_CAP` (for example 50): one week; read the nightly summary every day.
  5. Remove the allow-list; keep the cap at 200 to 500; watch the approval ratio and spend.
  6. Schedule `backend/scripts/auth_phone_nightly.py` on Railway (cron) after step 3.
  7. Rollback at any time: `AUTH_PHONE_LOGIN_ENABLED=0` (routes 404, other sign-in unaffected). Before a long rollback, warn phone-only users, who cannot recover without a phone (`DESIGN.md` section 13).
- **Acceptance:** the log shows each step with date and result; no step skipped.
- **Pitfalls:** do not raise caps and widen the audience on the same day.

## Pay hardening cards (from `PAY_AND_STRIPE_AUDIT.md`)

These matter only before billing is switched on in production; none is needed for phone login.

### P1. Webhook safety and missing events (M)
- **Depends on:** none. **Owner-gated:** migration apply.
- **Files:** `migrations/V0NN__stripe_events.sql` (processed-event table: event id unique, type, created, received), `backend/billing/routes.py`, `backend/billing/webhooks.py` (new), tests `backend/tests/test_billing_webhooks.py`.
- **Build:** record each event id and ignore duplicates; before changing access, retrieve the live subscription from Stripe (or compare the event's `created` against the last applied) so a late `updated` cannot re-grant after `deleted`; add the same stale-subscription guard to the org webhook; check `payment_status` on `checkout.session.completed`; handle `customer.subscription.created`, `invoice.paid`, `invoice.payment_failed`, `invoice.payment_action_required`, `customer.subscription.trial_will_end`, `charge.refunded`, `charge.dispute.created`, `checkout.session.async_payment_succeeded` and `_failed`; look up the user by stored customer or subscription id when metadata is absent; a missing secret returns 500 (not 400) and logs loudly; remove the dead `elif` (`routes.py:292-293`).
- **Acceptance:** tests with a real signed payload (use `stripe.Webhook.generate_test_header`) for every event above; a duplicate delivery changes nothing; an out-of-order sequence (deleted then updated) leaves access revoked; refund and dispute revoke or flag as designed; an unknown event gives 200.
- **Pitfalls:** return 200 quickly for events we do not use; do not make Stripe calls for every event. Keep one endpoint or document both secrets. Never log payloads.
- **Owner steps:** apply the migration; add the new events to the endpoint in the Stripe dashboard (checklist in the audit, section 7).

### P2. One customer, plan changes and plan mapping (M)
- **Depends on:** P1. **Owner-gated:** no.
- **Files:** `backend/billing/checkout_service.py`, `stripe_billing.py`, `backend/billing/catalog.py`, `frontend/templates/premium.html`, tests.
- **Build:** pass `customer=<stored id>` when known and set `client_reference_id`; change plans by updating the existing subscription (proration chosen deliberately) or by sending the person to the portal; map the subscription's price id back to a plan id so a portal change updates `subscription_plan_id`; cancel or refuse a second active subscription; set `api_version`, timeouts and `max_network_retries`; add idempotency keys on session creation.
- **Acceptance:** a second checkout reuses the customer; switching plan leaves one subscription; a portal plan change event updates the stored plan; tests assert the arguments given to the Stripe client.
- **Pitfalls:** price ids differ between test and live; the map comes from the environment variables, not constants.
- **Owner steps:** portal settings (plan switching and proration) in the Stripe dashboard.

### P3. One trial per person (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** `backend/billing/checkout_service.py`, `backend/db/users_db/billing.py`, migration or column per the storage decision, tests.
- **Build:** record that a user has had a trial (and which plan) when a subscription with a trial is created; pass `trial_period_days` only when none was used; keep it per user now, per verified number later (S10).
- **Acceptance:** second checkout by the same user has no trial; cancelling and returning does not restart one; the first still has it.
- **Pitfalls:** users are in SQLite; a column there touches the frozen allowlist, so prefer a Postgres table keyed by user id like the phone tables.
- **Owner steps:** none.

### P4. Small safety fixes (S)
- **Depends on:** none. **Owner-gated:** no.
- **Files:** `backend/billing/routes.py`, `stripe_billing.py`, `checkout_service.py`, `backend/utils/production_security.py`, `requirements-web.txt`, `.env.example`, `deploy/docker-compose.yml`, `deploy/k8s/secret.yaml`, tests.
- **Build:** wrap Stripe calls on the success pages and show a calm retry message; store customer and subscription ids on success; checkout routes become POST with CSRF (and templates link by form); refuse to boot in production when `STRIPE_SECRET_KEY` starts `sk_test_` while a live marker is set (or log the mode at boot); pin `stripe` to a tested range; add `STRIPE_PREMIUM_WEBHOOK_SECRET` to the example files; check `event["livemode"]` against the key mode.
- **Acceptance:** a simulated Stripe outage on a success page returns 200 with a retry line; GET to a checkout route is refused; a test-mode event on a live key is ignored with a log line.
- **Pitfalls:** do not break the iOS web view flow.
- **Owner steps:** none.

### P5. Stripe dashboard and Railway checklist, then a test purchase (not code)
- **Depends on:** P1 to P4 merged and released to the dev service. **Owner-gated:** yes.
- **Steps:** the checklist in `PAY_AND_STRIPE_AUDIT.md` section 7 in test mode, then the 13-step test plan in section 8, writing results next to each step; then repeat in live mode and set `BILLING_STRIPE_ENABLED=1` last.
- **Acceptance:** every step of section 8 passes after P1 to P4 (steps 6, 7, 9, 10 should now behave correctly).
- **Owner steps:** all of it.
