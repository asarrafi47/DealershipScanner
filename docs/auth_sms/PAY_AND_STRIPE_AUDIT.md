# Pay structure and Stripe audit

> **Update 2026-11-13: dormant by owner decision.** The whole website is now free. There are no paid plans for now. The Stripe and plan code stays in the repository, unused, with `BILLING_STRIPE_ENABLED` off, and features are **not** gated by plan any more. This file is kept as a record of what the code does and as the checklist to use **if paid plans ever return**. The work cards that came out of it (P1 to P5) were replaced by cards for chat limits, because the AI chat is the only thing that now costs money per use: see `CHAT_LIMITS.md` and `TASKS.md` (F1, C1 to C6). Findings H1 to H4 and M1 to M8 below stay valid for a future revival; none of them matters while billing is off. Sections 7 and 8 are "if revived" lists, not to-dos. Card references to P1 to P5 inside this file now mean "future revival work", not queued cards.

Audit of `dev` at VERSION 1.5.24 (2026-11-13). Documentation only. Paths relative to the repo root; `B/` is `backend/`.

**What I could only read in code.** This VM cannot reach Stripe or Railway. Nothing here was checked against a real Stripe account, a real webhook delivery, or Railway's variable list. Every statement about "what Stripe does" is from Stripe's documented behavior and from reading our code. Section 7 is the list the owner must tick in the dashboards.

## 0. The short version

- **Four plans in code, billing off in production.** Free, Research $4.99, Assistant $9.99, Complete $14.99 a month (`B/billing/catalog.py:76-113`). With `BILLING_STRIPE_ENABLED` off, every signed-in user gets every paid feature, and nobody can pay (`B/billing/access.py:329-340`; `docs/SECURITY_MASTER_TODO.md:973-981`).
- **Is Stripe "properly set up"? Not yet, for going live.** The happy path (checkout, a success page that checks with Stripe, two webhooks that grant and revoke access) is sound and has an unusually good success-page check. But before switching billing on, the audit finds four serious gaps: no replay or out-of-order protection on webhooks; no handling of failed payments, refunds or disputes; changing plan makes a second subscription (and a new Stripe customer) instead of changing the existing one, and a plan change made in Stripe's customer portal never reaches our database; and every checkout gets a fresh free trial. Details and fixes are in sections 5 and 6.
- **Two webhooks, two secrets**, and the second one's variable name is missing from `.env.example`, the k8s secret and docker-compose (section 7).
- **Phone login (DESIGN.md) does not change any of this**, because plans and Stripe key on `users.id` (section 8).

## 1. Plan catalog

All in `B/billing/catalog.py`.

| Plan | Price (cents / month) | Trial | Price id variable | Features |
|---|---|---|---|---|
| `free` | 0 (`:77-84`) | none | none | none paid |
| `research` | 499 (`:85-93`) | 7 days | `STRIPE_PRICE_RESEARCH` | market intel, vehicle history, nearby dealers, saved searches |
| `assistant` (highlighted) | 999 (`:94-103`) | 7 days | `STRIPE_PRICE_ASSISTANT` | Research plus AI chat about a car and about a comparison |
| `complete` | 1499 (`:104-112`) | 14 days | `STRIPE_PRICE_COMPLETE` (falls back to `STRIPE_PREMIUM_PRICE_ID`, `:130-142`) | Assistant plus window sticker and factory packages |

Monthly only; no yearly price. Feature ids (`:29-36`): `ai_car_chat`, `ai_compare_chat`, `window_sticker`, `vehicle_history`, `market_intel`, `nearby_dealers`, `packages_ensure`, `saved_searches`. Bundles: `:51-74`. These are plain ids, not environment flags. The pricing rows are built at `:166-216`; `GET /billing/plans` serves them (`B/billing/routes.py:311-314`); `/premium` renders them (`B/routes/listings_api.py:86-102`, `frontend/templates/premium.html:60-92`) and shows "Not available" for a plan whose price variable is empty.

A separate **dealer-group (org) subscription** uses one price, `STRIPE_PRICE_ID` (`B/billing/stripe_billing.py:31-32,77`). It gates dealer pages only (section 2).

Free forever: browsing, search, specs, saving cars, comments, hidden dealers, history.

## 2. Who may do what (entitlement and access checks)

One policy file: `B/billing/access.py`.

- `build_access_context` (`:405-482`) reads the user's row **from the database on every request** (cached per request on `flask.g`, `:485-502`) and decides. Precedence (`:463-468`): the stored `subscription_plan_id` features, then the old `is_premium` flag (treated as Complete), then an active org subscription (Complete). Admin gets Complete (`:456`); a `/dev` operator gets everything (`:454`). If the database read fails, normal users fail open to the cookie's claims (`:441-446`); admins fail closed.
- Helpers, not decorators: `check_feature`, `can_use`, `shows`, `sees_paid_ui`, `is_site_admin` (`:515-533`). A refused call returns 401 `login_required`, or 403 `premium_required` with an upgrade link (`:536-552`).
- Which route uses which feature: `market_intel` `B/routes/listings_api.py:577`; `saved_searches` `listings_api.py:853,865,883` and `B/routes/account.py:48`; `ai_car_chat` `B/routes/ai_chat_bp.py:151` and `cars_pages.py:575`; `ai_compare_chat` `cars_pages.py:684`; `window_sticker` `cars_pages.py:94,234`; `vehicle_history` `cars_pages.py:301`; `packages_ensure` `cars_pages.py:485`; `nearby_dealers` `B/routes/dealers_recalls.py:25`.
- Older shims in `B/billing/entitlements.py:118-190` are still used when a session is created (`B/auth/session.py:81`); they cache for 60 seconds per worker (`:38,66-86`), so another worker can show old access for up to a minute.
- **Org paywall:** a `before_request` hook sends org members without an active or trialing org subscription to `/billing/required`, only for `app_home`, `dashboard`, `dealer_portal.*`, `store_admin.*` (`B/web/security.py:120-143`). It reads the status stored in the cookie (`B/auth/session.py:27-35`), not the database.

### Where subscription state is stored

- `users`: `is_premium`, `subscription_plan_id`, `premium_stripe_customer_id`, `premium_stripe_session_id`, `premium_stripe_subscription_id` (`B/db/users_db/schema.py:139-150`). **No status, no period end, no cancel flag, no trial end** for a person. So the account page cannot show "renews on".
- `orgs`: `stripe_customer_id`, `stripe_subscription_id`, `stripe_subscription_status`, `stripe_current_period_end` (`schema.py:212-219`; `B/db/users_db/accounts.py:23-85`).
- Grant: `grant_user_premium` (`B/db/users_db/billing.py:4-48`), called from `B/billing/routes.py:210` (legacy success), `:264` (webhook), `:297` (renewal), `:357` (plan success). Revoke: `revoke_user_premium` (`billing.py:51-68`), only from the premium webhook (`routes.py:287`).
- These columns move with the users database. Today that is encrypted SQLite on the web volume; V013 has the same columns for the Postgres cutover.

### When billing is off (every effect)

`Config.billing_stripe_enabled()` (`B/config.py:78-79`; compose default off, `deploy/docker-compose.yml:72`):
- all feature APIs open to any signed-in user; signed-out users are still refused in production (`access.py:334-337`);
- `/billing/required`, `/billing/checkout`, `/billing/success` redirect home (`routes.py:61,75,99`); the user checkouts redirect to `/premium` (`:184,319`); both webhooks return 404 (`:117,233`); success pages show `billing_disabled` (`:222-223,367-368`); creating a session raises "Billing is disabled" (`stripe_billing.py:72,110,222`; `checkout_service.py:33-34`);
- no org paywall (`security.py:121`; `B/auth/session.py:30-31,53-57`);
- the account billing page says billing is not enabled (`B/routes/account.py:176-198`); upsell text is hidden in templates (`B/web/templating.py:77`);
- a production boot warning exists (`B/utils/production_security.py:93-101`).

## 3. Trials and promo codes

- **Trial:** only Stripe's `trial_period_days` on the plan checkout (`B/billing/checkout_service.py:247-248`). The old premium checkout and the org checkout have no trial. A trialing subscription counts as active (`stripe_billing.py:43-44`; `access.py:301-302`). **There is no "one trial per person" rule** (section 5, finding H4).
- **Promo codes:** `B/billing/discounts.py`. Code shape `^[A-Za-z0-9_-]{3,32}$` (`:314`). A variable `BILLING_PROMO_<CODE>` holds a Stripe coupon id (`:335-351`); a known code pre-applies that coupon, an unknown one falls back to Stripe's own promotion-code box (`resolve_discount`, `:354-377`). The code arrives in the URL (`?promo=` or `?code=`) on `GET /billing/plan/checkout` (`routes.py:325`). The old and org checkouts always allow Stripe's promotion-code box (`stripe_billing.py:99,131`). No redemption cap or rate limit in our code; the limits must be set on the coupon in Stripe.

## 4. What the integration does (as built)

**Checkout creation.** Three flavors, all `mode=subscription`, hosted by Stripe:
- plan checkout, `B/billing/checkout_service.py:210-261`: metadata `{user_id, plan_id, premium: "1"}` on the session and on the subscription (`:244-246`), `customer_email` from the cookie (`:255`), trial days (`:248`), discount (`:258`), success URL `.../billing/plan/success?session_id={CHECKOUT_SESSION_ID}&plan=<id>` (`:240`), cancel URL `/premium?plan=...` (`:241`);
- old single premium, `stripe_billing.py:104-133`;
- org, `stripe_billing.py:65-101`, with `client_reference_id` = org id (`:88`).
Not set anywhere: `customer`, `client_reference_id` (user checkouts), `payment_method_types`, `automatic_tax`, `tax_id_collection`, `billing_address_collection`, idempotency key, request timeout or retry settings, `api_version`. The web library is `stripe>=8` (`requirements-web.txt:24`), unpinned at the top, and no `stripe.api_version` is set, so a new account's default API version applies. Base URL: `PUBLIC_BASE_URL`, else the request host (`stripe_billing.py:47-52`; boot warning `production_security.py:40-48`).

**Success pages** (`routes.py:198-228` legacy, `:344-374` plan). Strong: they fetch the session from Stripe and require status complete, `payment_status` paid or no_payment_required, mode subscription, metadata `user_id` equal to the signed-in user, matching `plan_id`, and a subscription that is active or trialing (`checkout_service.py:264-296`, `stripe_billing.py:165-206`). Then they grant access. So access does not wait for the webhook. Weaknesses: no try/except around the Stripe call (a Stripe outage gives a 500), and the grant does not store the customer or subscription id (`routes.py:210,357`), so until a webhook arrives the portal link and the old-subscription guard have nothing to use.

**Webhooks.** Two Flask routes, signature checked with the raw body and `stripe.Webhook.construct_event` at the default 5-minute tolerance:
- `POST /billing/webhook` for orgs (`routes.py:115-177`), secret `STRIPE_WEBHOOK_SECRET`;
- `POST /billing/premium/webhook` for people, plan checkouts included (`routes.py:231-306`), secret `STRIPE_PREMIUM_WEBHOOK_SECRET` (`stripe_billing.py:27-40,136-217`).
Bad signature, or any error including a missing secret, returns 400 `invalid_signature` (`:123-125,240-241`); a missing secret therefore looks like an attack. Billing off returns 404. Both routes are CSRF-exempt by being absent from the lists (`B/web/security.py:49-98`; `backend/tests/test_csrf_coverage.py`).

**Handled events:**

| Event | Org webhook | Person webhook |
|---|---|---|
| `checkout.session.completed` | stores customer and subscription ids (`:134-165`) | grants access, no `payment_status` check (`:256-272`) |
| `customer.subscription.created` | stores status and period end | **ignored** |
| `customer.subscription.updated` | stores status and period end | revokes when status is not active or trialing; otherwise re-grants if metadata `premium=1` (`:274-304`) |
| `customer.subscription.deleted` | status `canceled` | revokes unless the ended subscription is not the one we hold (B-041 guard, `:283-286`) |
| everything else | 200, ignored | 200, ignored |

Org writes look the org up by `metadata.org_id` only; an unknown org is silently ignored. There is a duplicated dead `elif` at `:292-293`.

**Customer portal.** `billing_portal.Session.create(customer, return_url=/account/billing)` (`stripe_billing.py:220-238`), reached by `GET /account/billing/portal` (`B/routes/account.py:202-226`), only for people with a stored `premium_stripe_customer_id`. Orgs have no portal. What the portal may do (cancel, switch plans, proration) is set in the Stripe dashboard, not in the repo.

**Upgrades, downgrades, proration, cancel at period end.** No code. The "Switch" link on `/premium` (`premium.html:76-77`) starts a **new** checkout. Cancelling is left to the portal; `cancel_at_period_end` is never read; access ends on the `deleted` event.

**Refunds, tax, card authentication.** No refund code, no refund event, no tax settings in code (grep for `automatic_tax`, `tax_id`, `refund`, `dispute`, `payment_intent`, `requires_action`, `billing_address` found nothing under `B/billing/`). Card authentication (3-D Secure) is handled by Stripe's hosted page; our code never sees it, and `invoice.payment_action_required` is not handled.

**Test mode and live mode.** Nothing in code tells them apart: no check of the key prefix (`sk_test_` or `sk_live_`), and `event["livemode"]` is never read. The mode is whatever key and secrets sit in the variables. Price ids differ between modes, so a test key with live price ids (or the reverse) fails at checkout with a Stripe error rather than at boot.

**Logging.** User ids, truncated session ids, subscription ids and warnings with stack traces on a bad signature; no card data; the email goes only to Stripe.

## 5. Audit findings

Severity means: High = fix before switching billing on; Medium = fix soon after; Low = tidy.

**High**
- **H1. No replay or ordering protection.** No table of processed event ids, no use of the event's `created` time. A late `customer.subscription.updated` (active) can re-grant access after `deleted`, because the "held subscription" guard only protects the revoke branch (`routes.py:274-304`). The org webhook has no guard at all (`:167-175`). Fix: store event ids (unique), and compare against the live subscription from Stripe (retrieve it) before changing state.
- **H2. Missing events.** Nothing handles `invoice.paid`, `invoice.payment_failed`, `invoice.payment_action_required`, `customer.subscription.trial_will_end`, `charge.refunded`, `charge.dispute.created`, `checkout.session.async_payment_succeeded` and `_failed`, `checkout.session.expired`, `customer.deleted`. The person webhook grants on `checkout.session.completed` without looking at `payment_status`. A failed renewal ends up as `past_due` (writes status for orgs, revokes people) only through `subscription.updated`; refunds do not remove access unless the subscription is also cancelled. A comment at `B/billing/entitlements.py:34-36` talks about a "refund webhook" that does not exist.
- **H3. Changing plan creates a second subscription and a second customer.** The "Switch" link starts a new checkout with only `customer_email` set (`checkout_service.py:250-260`), so Stripe creates a new customer each time; the old subscription is not cancelled by code. People can pay twice. Also, `subscription_plan_id` comes only from session metadata, so a plan change made in the customer portal arrives as `subscription.updated` but the plan in our database stays the old one (`routes.py:295-296`). Fix: reuse the customer (`customer=` the stored id); change plans with a subscription update (proration set deliberately) or the portal; and map the subscription's price id to a plan, instead of trusting metadata.
- **H4. Repeat free trials.** Every plan checkout gets the full `trial_period_days` (`checkout_service.py:247-248`), a new customer each time, and no record of past trials. Someone can cancel and start again forever, or start three trials at once. Phone verification (DESIGN.md section 19) helps but is not a fix; store `trial_used_at` per user and per verified phone and skip the trial.

**Medium**
- **M1. No renewal date, status or cancel flag for people** (users table). The account page cannot say "renews on" or "ends on"; dunning mail is impossible.
- **M2. Customer lookup is metadata only.** Events that carry no metadata (invoices, subscriptions created in the portal) cannot be mapped to a user; there is no lookup by `premium_stripe_customer_id`.
- **M3. Success pages call Stripe without error handling** (`stripe_billing.py:183`, `checkout_service.py:280`), and do not save customer and subscription ids.
- **M4. No test/live guard**, no `api_version` pin, `stripe>=8` unpinned, no timeouts, retries or idempotency keys on session creation. A library upgrade can change webhook payloads.
- **M5. Misconfiguration looks like an attack:** a missing webhook secret returns the same 400 as a bad signature (`routes.py:123-125`), visible only in logs.
- **M6. Checkout starts on GET** (`routes.py:73,182,317`): a link on another site can start a session for a signed-in user. Low impact, but it should be a POST with CSRF.
- **M7. Two webhook endpoints and two secrets**, and `STRIPE_PREMIUM_WEBHOOK_SECRET` appears in no example file, k8s secret or compose file (`.env.example:145`, `deploy/k8s/secret.yaml:24` name only `STRIPE_WEBHOOK_SECRET`). Easy to forget one in production. Simplest fix: one endpoint that routes by metadata.
- **M8. 60-second per-worker cache** can show stale access for up to a minute on another worker (production runs one worker, so low today).

**Low**
- No tax (`automatic_tax`), no tax ids or billing address collection. Whether the owner must collect sales tax on a $4.99 to $14.99 digital subscription depends on states and thresholds; ask an accountant, then turn on Stripe Tax if needed.
- No `client_reference_id` on user checkouts; dead duplicate `elif` (`routes.py:292-293`); promo codes ride in a GET URL; no redemption caps in our code.
- The org webhook never records a subscription for an org that has no metadata.

**What is good:** raw-body signature check; no card data touches the server; the success-page verification (owner check, plan check, status check) is better than most; old-subscription guard on revoke (B-041); default off with clean 404s; every billing page degrades when off.

## 6. What the tests cover

`backend/tests/`: `test_billing_catalog.py` (plans, supersets, price variable fallback, promo format and mapping); `test_billing_gate.py` (admin bypass, invalid signature gives 400 at `:159`, disabled gives 404 at `:177`, an old subscription ending keeps the newer one at `:196`, active without `premium` metadata does not grant at `:216`); `test_billing_portal.py`; `test_entitlements_revalidation.py` (cancelled plan loses features, database failure fails open, 60-second cache, org status); `test_premium_checkout.py` and `test_stripe_premium_verify.py` (success page verification, replay of a cancelled subscription rejected); `test_paid_access_parity.py` (access matrix, billing on and off, plan plus org); `test_ux_premium_gates.py`, `test_dev_operator_premium.py`.

**Not covered:** a webhook with a valid signature through the whole handler; org webhook writes; `checkout.session.completed` granting through the person webhook; `subscription.updated` downgrading a plan; the arguments given to `Checkout.Session.create` (trial days, metadata, discount); `GET /billing/plan/checkout`; Stripe errors on success pages; portal session creation; duplicate or out-of-order events; promo codes end to end. Nothing exists for refunds, disputes or invoices because they are not implemented.

## 7. Owner checklist (Stripe dashboard and Railway)

Do all of it in **test mode first**, then repeat in live mode. Nothing below can be done from code.

Stripe dashboard, test mode (toggle at the top):
- [ ] Products and prices: create Research ($4.99 / month), Assistant ($9.99 / month), Complete ($14.99 / month), recurring monthly, USD. Copy each price id (they start with `price_`) to a safe place. Optionally a fourth price for the dealer-group plan.
- [ ] Do not set trial days on the price itself; the code adds them (7, 7, 14).
- [ ] Coupons: create one per promo code you want; set a maximum number of redemptions and an expiry. Note each coupon id.
- [ ] Webhook endpoint 1: `https://<your site>/billing/premium/webhook`, events `checkout.session.completed`, `customer.subscription.updated`, `customer.subscription.deleted` (the code today uses only these; after the fixes in `TASKS.md` also add `customer.subscription.created`, `invoice.paid`, `invoice.payment_failed`, `customer.subscription.trial_will_end`, `charge.refunded`, `charge.dispute.created`). Copy its signing secret (`whsec_...`).
- [ ] Webhook endpoint 2 (dealer groups, only if you sell it): `https://<your site>/billing/webhook`, events `checkout.session.completed`, `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`. Copy its own signing secret.
- [ ] Customer portal (Settings, Billing, Customer portal): allow cancel (at end of period), update payment method, view invoices; decide whether to allow plan switching (if yes, list the three prices and choose proration); set the return link to `https://<your site>/account/billing`.
- [ ] Tax: decide with an accountant; if needed, enable Stripe Tax and set the product tax category (software as a service).
- [ ] Emails (Settings, Emails): turn on receipts, failed-payment and renewal reminders so you do not have to send them.
- [ ] Radar and card checks: leave defaults; confirm 3-D Secure is "request when required".
- [ ] Developers, API keys: copy the test secret key (`sk_test_...`).

Railway, web service variables (names only; paste values in Railway, never in chat or the repo):
- [ ] `BILLING_STRIPE_ENABLED` (set to `1` last, only after the test purchase passes)
- [ ] `STRIPE_SECRET_KEY`
- [ ] `STRIPE_PRICE_RESEARCH`, `STRIPE_PRICE_ASSISTANT`, `STRIPE_PRICE_COMPLETE`
- [ ] `STRIPE_PREMIUM_WEBHOOK_SECRET` (endpoint 1)
- [ ] `STRIPE_WEBHOOK_SECRET` (endpoint 2, only if dealer groups pay)
- [ ] `STRIPE_PRICE_ID` (dealer-group price, only if used) and `STRIPE_PREMIUM_PRICE_ID` (old single plan fallback, optional)
- [ ] `BILLING_PROMO_<CODE>` one per coupon, for example `BILLING_PROMO_LAUNCH20` (value is the coupon id)
- [ ] `PUBLIC_BASE_URL` (the https address of the site; checkout return links use it)
- [ ] `SECRET_KEY`, `SESSION_COOKIE_SECURE` already needed for sign-in
- [ ] `GUNICORN_WORKERS` = 1 (rate limits and the 60-second caches assume it)
- [ ] `STRIPE_PUBLISHABLE_KEY` is documented but not read by code; not needed today.

Going live: repeat the checklist in live mode with `sk_live_...`, live price ids and live signing secrets; change the three or four variables together; then run one real small purchase and refund it.

## 8. Test plan for one full purchase in Stripe test mode

Run on a staging or dev deploy with test keys, never with live keys.
1. Set the variables above with test values, `BILLING_STRIPE_ENABLED=1`, redeploy. Open `/premium`: three plans show prices, none says "Not available".
2. Sign in as a new test shopper. Choose Assistant. Stripe's page shows a 7-day trial. Pay with card `4242 4242 4242 4242`, any future date, any CVC and ZIP.
3. You return to `/billing/plan/success`; the account shows Assistant; AI chat works; window stickers do not (Complete only).
4. In Stripe, Developers, Webhooks, the endpoint shows `checkout.session.completed` delivered with status 200. Check the users database row has a plan and customer id (an admin can see `stripe_linked` in `/admin/users`).
5. Card authentication: repeat with `4000 0025 0000 3155` (asks for 3-D Secure); complete it.
6. Failed payment: `4000 0000 0000 0341` (attaches, then fails on renewal). Use Stripe's test clock to advance past the trial and see `invoice.payment_failed` and what the site does. Today the code does nothing for that event; this step shows the gap.
7. Switch plan: click Switch to Complete. Today this makes a second subscription and a second customer in Stripe. Record it; the fix is in `TASKS.md`.
8. Portal: open Account, Billing, Manage. Cancel at period end. Confirm the site keeps access until the period ends, then loses it on the `deleted` event (advance the test clock).
9. Refund: refund the payment in Stripe; see whether access stays (today it does).
10. Replay: in the dashboard resend an old `customer.subscription.updated` after the cancel; today it can re-grant access (finding H1).
11. Promo: open `/billing/plan/checkout?plan=research&promo=<CODE>` for a code you mapped; the coupon shows on the Stripe page.
12. Bad signature: send a webhook with the wrong secret (Stripe CLI `stripe trigger` with a different secret, or a curl with a made-up header); expect 400.
13. Switch `BILLING_STRIPE_ENABLED` off and confirm the site is back to "everyone signed in gets everything" and webhooks return 404.
Write what you saw next to each step; steps 6, 7, 9 and 10 are expected to show today's gaps.

## 9. How this relates to phone login and to the free site

Plans, trials and Stripe key on `users.id`, not on email. Phone login adds a verified number per user; it does not touch `users.id`, the plan columns or webhook mapping. A phone-only user checks out without an email on file; Stripe then asks for one, which becomes the receipt address (Stripe's customer record, not ours). See `DESIGN.md` section 19.

With the site free, `users.id` is the only key that matters. Chat (the one paid-for resource) is limited per account, phone number and device in Postgres (`CHAT_LIMITS.md`), not by plan. If plans return, a verified, unique phone number is a good key for "one trial per person" (`DESIGN.md` section 19).
