# Phone-number login with text-message codes: design

Status: proposal, nothing built. First written 2026-11-13, **revised the same day after the owner's decisions** (listed in section 1). Written against `dev` 1.5.25. **Design review 2026-11-13:** a four-lens attack changed sections 4, 5, 6, 7, 8, 9, 10, 13, 15, 16, 17 and 20; see the Decisions log in `TASKS.md`. **Owner answers 2026-11-14:** anyone may post a car to sell (private sellers) and dealerships sign up by themselves through a verification process, guarded against bots and spam (new section 23, cards S19 to S30); texts are capped at 30 sends and 3 dollars a day for the first month (section 5). Read `SURVEY.md` first for what exists; this file cites it as "survey". Cards are in `TASKS.md`; chat limits are in `CHAT_LIMITS.md`. Paths relative to the repo root; `B/` is `backend/`.

In one paragraph: a shopper types a phone number, ticks the consent box, passes a Cloudflare Turnstile check, and Twilio Verify texts a 6-digit code. The shopper types the code and the site signs them in with the same session cookie it uses today. A new number creates an account; a known number signs in. **For shoppers this is the only way in.** Admin and dealer accounts keep password sign-in. Private sellers post cars and dealerships verify themselves, behind the guard in section 23.

## 1. Decisions made (owner, 2026-11-13) and what I still need

Decided:
- The whole site is free; Stripe and plan code stay in the repo but dormant, billing stays off (`PAY_AND_STRIPE_AUDIT.md`). Chat is the only thing with a per-use cost (`CHAT_LIMITS.md`).
- Shoppers sign in by phone number plus text code only, from day one. Email-and-password sign-in for shoppers is removed.
- Admin and dealer-portal accounts stay as they are; admins get an authenticator step (card S12); dealers stay password-only for now.
- Provider: Twilio Verify (runner-up Telnyx Verify), 20 dollar prepaid balance, no auto-recharge, no callbacks (webhooks) at launch. Twilio Lookup line-type check blocks VoIP numbers. Cloudflare Turnstile is checked on the server for every send request.
- US numbers only. Support may replace the number on an account after proof. Made-up internal emails for phone accounts are fine.
- Defaults and texts as given (sections 5, 6, 14 and 16).

Decided 2026-11-14 (owner):
- **A marketplace:** anyone signed in with a verified phone may post a car to sell (private seller), and dealerships sign up by themselves and go through a verification process (verified dealership). The plan to close `/dealer/register` (`DEALER_SELF_REGISTER_ENABLED`, default off) is withdrawn. Misuse is made hard by section 23: a trust ladder, dealership verification, checks before publishing, the owner's review of each private seller's first listing, and public posting off (`MARKETPLACE_POSTING_ENABLED`) until the guard cards are live.
- **Texts, first month: 30 sends and 300 cents a day** (`SMS_DAILY_SEND_CAP`, `SMS_DAILY_SPEND_CAP_CENTS`), which are now the code defaults; alert at half (15). 300 sends and 1500 cents stay only as later targets. Chat budget numbers do not change.
- Owner steps before launch, agreed: a monthly spend limit with a 50 percent alert in the Anthropic console; Twilio low-balance emails at 10 and 5 dollars; Twilio geographic permissions US only; RCS off on the Verify service (card S18).
- Migration numbering stays as built (V030 chat usage, V031 S5, V032 S12, queued rows moved up by three); the shipped chat price guess is accepted until real spend is known; the Cloudflare header name `X-Origin-Secret` is accepted.

Still open, with my recommendation (also repeated at the end):
- **Google and Apple sign-in for shoppers:** I recommend turning them **off** for shoppers at launch (section 11). Please confirm.
- **Code lifetime:** you gave "expires in 5 minutes" as the default but the text message says "expires in 10 minutes". I recommend 5 minutes in both (section 5).
- **Old accounts:** I recommend the "claim your old account" email link, with a start-fresh fallback (section 12).
- Questions 1 and 5 from the first round you will answer separately.

## 2. What changes and what does not

Does not change: `users.id`, the signed session cookie, CSRF, the access helpers, `/dev`, the Stripe code (dormant). The dealer portal does change, but only by the guard in section 23. Phone login ends by calling the same `finalize_app_session(user_id)` (`B/auth/session.py:67`) that every login calls.

Changes:
- New package `backend/auth/phone/`, new Postgres tables, new routes and plain pages.
- `/login` becomes the phone page. The old password form moves to a staff page (section 10).
- `/register` and `/api/auth/register` stop creating shopper accounts (section 10).
- Google and Apple buttons are hidden for shoppers (section 11).
- Session cookies last longer (section 9) because every login costs money.
- iOS login must change (section 15).

## 3. Provider: Twilio Verify, behind a small interface

Everything vendor-specific lives in one file, `backend/auth/phone/twilio_verify.py`, and a few environment variables. The rest of the code talks to an interface, so Telnyx Verify (the runner-up) could replace it by writing one more file.

```python
# backend/auth/phone/provider.py  (sketch)
@dataclass(frozen=True)
class StartResult:
    ok: bool
    provider_ref: str | None   # the vendor's verification id
    retry_after_s: int | None
    reason: str                # our short code; never vendor text

@dataclass(frozen=True)
class CheckResult:
    approved: bool
    reason: str                # approved | wrong_code | expired | too_many | unknown

@dataclass(frozen=True)
class LineInfo:                # from the line-type lookup
    ok: bool
    line_type: str | None      # mobile | landline | fixedVoip | nonFixedVoip | ...
    country: str | None
    reason: str

class PhoneVerifyProvider(Protocol):
    name: str
    def lookup(self, e164: str) -> LineInfo: ...
    def start(self, e164: str, *, locale: str) -> StartResult: ...
    def check(self, e164: str, *, code: str, provider_ref: str | None) -> CheckResult: ...
```

- **Twilio adapter:** uses the Python package `twilio` (add to `requirements-web.txt`). Start is Verify v2 "create verification" with channel `sms`; check is Verify v2 "verification check". Lookup is Lookup v2 with the `line_type_intelligence` field. 5-second timeouts. One retry on start for a network error only; none on check. Vendor error text is mapped to our `reason` codes and never shown or logged.
- **Twilio holds the code.** We never see or store it (so "stored hashed" applies only to the fake provider, which makes its own). Our own challenge row still enforces lifetime, attempts, single use and session binding on top of the vendor's rules, so the rules stay the same if the vendor changes.
- **Fake provider** (`fake_provider.py`): for tests and local development; approves `000000`, keeps an in-memory outbox, can simulate failure, throttling, VoIP and slowness. Refused when `FLASK_ENV` is production.
- **Verify settings in the Twilio console (owner steps):** a Verify service whose friendly name is `Sarrafi Cars` so the text starts with the brand; code length 6; the vendor's own fraud protection on; geographic permissions US only. **To check in the console before launch:** Verify's default text is, to my knowledge, "Your <friendly name> verification code is: 123456". The text you want ("Sarrafi Cars: Your verification code is 482913. It expires in 5 minutes. Do not share this code with anyone.") may need a custom Verify template, which Twilio may need to approve; I could not check this from here (no network).
- **Webhook** `POST /webhooks/twilio/verify` (section 17).

## 4. Data model and migration

### 4.1 Where it lives

New tables go in **inventory Postgres, never SQLite.** `backend/tests/test_no_sqlite.py` freezes SQLite use (`CHANGELOG.md` N37.0), and `migrations/README.md` forbids runtime DDL: a new table is a new migration. Same pattern as `saved_cars`, `saved_searches`, `user_hidden_dealers`, `user_search_history`: `user_id BIGINT NOT NULL`, **no foreign key** while `users` is still SQLite (`V018:12-15`, `V021:10-13`). When users move to Postgres (`docs/USERS_PG_CUTOVER.md`; ids are preserved, V013 header), a later migration adds the key as `NOT VALID` and then validates it. No phone column is added to `users`.

### 4.2 Tables

```sql
CREATE TABLE auth_phone_identities (           -- one verified number per account, one account per number
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,                            -- NULL while state = reserving
    phone_e164 TEXT NOT NULL,                  -- +14155550123, stored in clear on purpose (section 16)
    phone_hash TEXT NOT NULL,                  -- HMAC(PHONE_HASH_PEPPER, e164)
    country TEXT NOT NULL,
    line_type TEXT,                            -- from lookup at sign-up
    verified_at TIMESTAMPTZ NOT NULL,
    last_login_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'active',      -- reserving | active (account creation order, card S6)
    dormant_user_id BIGINT, detached_at TIMESTAMPTZ,  -- number detached after AUTH_PHONE_DORMANT_DAYS without sign-in
    consent_at TIMESTAMPTZ NOT NULL,           -- when the box was ticked
    consent_text_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT auth_phone_e164_shape CHECK (phone_e164 ~ '^\+[1-9][0-9]{6,14}$')
);
CREATE UNIQUE INDEX auth_phone_identities_phone_key ON auth_phone_identities (phone_e164);
CREATE UNIQUE INDEX auth_phone_identities_user_key  ON auth_phone_identities (user_id);

CREATE TABLE auth_phone_challenges (           -- one row per code request; no code, no raw number kept after it ends
    id BIGSERIAL PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    phone_hash TEXT NOT NULL,
    phone_e164 TEXT,                           -- cleared when the challenge ends
    purpose TEXT NOT NULL,                     -- login | link | claim | step_up | change | seller_verify
    user_id BIGINT,
    session_nonce TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_ref TEXT,
    attempts INT NOT NULL DEFAULT 0,
    sends INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'open',       -- open | approved | failed | expired | cancelled
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_send_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE auth_phone_sends (                -- one row per text and per refused send; drives every limit and the spend cap
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    phone_hash TEXT NOT NULL,
    ip_hash TEXT NOT NULL,
    device_hash TEXT,
    country TEXT NOT NULL,
    est_cost_cents NUMERIC(8,3) NOT NULL DEFAULT 0,   -- set at insert from SMS_EST_COST_CENTS, never from callbacks
    lookup_cost_cents NUMERIC(8,3) NOT NULL DEFAULT 0, -- SMS_EST_LOOKUP_CENTS, counted even for refused numbers
    purpose TEXT NOT NULL,                     -- same values as the challenge purpose
    pool TEXT NOT NULL,                        -- known | new | resend | seller (set at reserve time; section 5 pools)
    ip_prefix_hash TEXT NOT NULL,              -- HMAC of the /24 (IPv4) or /48 (IPv6) group
    has_device_cookie BOOLEAN NOT NULL,        -- the no-cookie bucket of card S6 (f)
    channel TEXT NOT NULL DEFAULT 'sms',       -- sms | call (the voice code of section 23.4 step 4)
    outcome TEXT NOT NULL                      -- pending | sent | unavailable | provider_error | blocked_turnstile | blocked_voip | blocked_country | blocked_limit | blocked_cap
);
-- indexes on (at), (phone_hash, at), (ip_hash, at), (device_hash, at), (ip_prefix_hash, at), (pool, at)

CREATE TABLE auth_phone_wrong_tries (          -- wrong-code counter per phone that a new code does NOT reset
    phone_hash TEXT PRIMARY KEY,
    tries INT NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ
);
```

Chat limit tables (`chat_usage_buckets`, `app_settings`) are in `CHAT_LIMITS.md` section 4 and ship in their own migration. The earlier "trial marks" table is dropped: with no paid plans there are no trials.

**Migration number:** `dev` ends at `V029`. Numbers are claimed in the registry (`docs/NEXT_PHASES_PLAN_2026_10.md:514-522`) in the same PR as the migration, not guessed from `ls`: integ files already queued (`users_auth_epoch`, `users_drop_dead_mfa_columns` and later) collide with a plain "next free" rule. Allocation (owner confirmed 2026-11-14): C1 = V030 (built), S5 = V031, S12 = V032; the integ rows moved up by three. The marketplace tables of section 23.10 are **not** in V031 (S5 must stay reviewable); card S21 claims the next free number when it is built, by rule 5 of `TASKS.md`. Session revocation uses a new table `auth_session_epochs` in S5, and `sessions_valid_after` is dropped from the identities table. Never edit an applied file. `docs/ALL_POSTGRES_PLAN_2026_10.md` is still not in the repository (question 5, which you will answer).

### 4.3 Accounts without an email

`users.email` and `users.username` are NOT NULL and unique (survey 9), so a phone account gets a username `user-<8 random letters and digits>` and an internal email `p-<24 random hex>@phone.invalid` (the `.invalid` name can never receive mail; you agreed this is fine), plus a random unusable password. `is_placeholder_email()` recognises it, and everything that uses email must treat it as "no email": no mail is sent to it, checkout and Stripe get no `customer_email`, the session stores an empty email, `/api/auth/me` returns an empty email, the admin hub shows "no email", and the number is never shown. Deleting an account must work by id (`delete_user_by_email` needs an `@`, `accounts.py:447-518`). A profile line lets people add a real email and a display name; comments and reviews show the display name, never the number.

## 5. Rules for codes and limits (owner's defaults)

| Rule | Value |
|---|---|
| Code | 6 digits, single use |
| Lifetime | **5 minutes**, enforced by our challenge row (`AUTH_PHONE_CODE_TTL_S`, 300). The login text must say the same number (see section 1) |
| Wrong tries | 5 per challenge, reserved **before** the vendor call. The 5-try lock is per (phone, browser session or device); a phone-wide lock only after about 15 to 20 wrong tries a day across all challenges, so a stranger cannot lock the real holder out. A new code does not reset the count (`auth_phone_wrong_tries`) |
| Resend | wait 60 seconds |
| Per phone | at most 2 sends in 15 minutes and 3 a day |
| Per IP | 3 an hour and 5 a day |
| Per device | 3 a day |
| Per /24 (IPv4) or /48 (IPv6) | 6 a day (one fifth of the day) |
| Whole site | **First month (owner decision 2026-11-14): `SMS_DAILY_SEND_CAP` = 30 and `SMS_DAILY_SPEND_CAP_CENTS` = 300.** These are the code defaults (`backend/auth/phone/config.py`), so an unset variable on release day means 30 and 300, never more. 300 sends and 1500 cents are only later targets the owner may choose after a month of real numbers, never in the same step as removing the allow-list. Alert at half the send cap (`SMS_ALERT_SEND_THRESHOLD`; unset means 15). A prepaid guard stops sends at 90 percent of `sms.prepaid_cents` (default 2000) spent since `sms.topup_at`, counted from the app's own rows. Day boundary: midnight in `CHAT_RESET_TZ` |
| Pools (shares of the day) | a number that already has an identity may use the whole shopper share; new numbers at most 50 percent (15 sends, `SMS_NEW_NUMBER_SHARE_PCT`); resends at most 20 percent (6); dealer and seller verification codes have their own reserved 10 percent (3 sends, `SMS_DEALER_SIGNUP_SHARE_PCT`, section 23.3): shoppers can never use those 3 and verification codes can never use more, so the shopper share is 27 |
| Countries | `SMS_ALLOWED_COUNTRIES` = `US` |
| Line type | VoIP numbers (fixed and non-fixed) are refused for sign-up and sign-in via the lookup; landlines cannot receive texts and are refused too |
| Fresh code before a number change | the account must have proved itself within the last 10 minutes |
| Cookie | shoppers 30 days, admins 12 hours (section 9) |

Each login costs about 6 cents for Verify plus about 1 cent for Lookup on a new number, so 30 sends cost about 2 dollars; the 3-dollar spend cap is the safety stop behind the send cap (it also catches a price rise). The 20 dollar balance buys roughly 300 sign-ups in all; at 3 dollars a day one bad day spends at most 15 percent of it. Twilio's own low-balance emails (10 and 5 dollars) are an owner step (agreed 2026-11-14).

**What a normal person can do in a day.** One number gets 3 codes a day, for example signing in on a phone and on a laptop plus one "send again". A household on one home address gets 5 codes a day between all its people; an office or campus network 6. Because the shopper cookie lasts 30 days, most people need one code a month. No single phone, device or address group can use more than one fifth of the whole day (6 of 30); inside the smaller pools the limits are tighter (below).

**Lock-out risk: an attacker burns the day's texts on purpose.** With 30 sends a day this is cheap, and the pools make some parts cheaper still; written down plainly:
- **New people (the new-number pool, 15 sends):** about 5 real US mobile numbers (3 each), spread over 3 address groups and 5 devices, use it up, and then no new person can sign up or become a seller until midnight. To slow this, any one /24 or /48 may use at most 2 sends of the new-number pool, so it takes at least 8 address groups.
- **Dealer and seller verification (the reserved pool, 3 sends):** any one account, phone, device or dealership request may use at most 1 send of it (voice included), so it takes 3 attackers; the 4th real person that day is told "Verification by text is busy today. We saved your request; try again tomorrow, or we will review it by hand." and a dealership request goes to admin review with "call back" noted. Resends count inside the pool.
- **People who already have an account (the rest, 12 or more):** the attacker must type real customers' numbers, which texts those people and shows at once in the nightly ratio of sends to approved codes. In the first month almost nobody has a phone identity yet, so nearly every sign-in competes for the 15 new-number sends: this is the real limit of the first month.
Defences, in order: Turnstile on every send; VoIP and landline refused, so the attacker needs real mobile numbers; at most 3 sends per number, 3 per device, 5 per IP and 6 per /24 or /48 overall (no single source more than one fifth of the whole day; the pool limits above are tighter); refusals at a pool limit or the cap also count against that address group's and device's limits, and a burst of `blocked_cap` from one group trips the `cap_hit` alert. What the person sees at a cap or pool limit: the same busy sentence for everyone, "Sign-in by text is busy right now. Please try again after midnight Eastern time." (section 6; never a different sentence for known numbers, which would reveal accounts); people already signed in are not affected (30-day cookie). The owner gets the `cap_hit` email at once (section 17) and can raise the cap or `SMS_NEW_NUMBER_SHARE_PCT` the same hour (below). Residual risks: buying about 5 mobile numbers is a one-day denial of new sign-ups; carrier-grade NAT puts many real phones behind one IPv4 group, so on a busy day the group limit can refuse a real person on mobile data (they can switch to Wi-Fi or try later). The per-IP limit keys on the single IPv4 address or IPv6 /64 (section 8); the group limit on the /24 or /48.

**Can the owner raise the cap without a deploy?** Today, only through an environment variable. `backend/auth/phone/config.py` reads `SMS_DAILY_SEND_CAP`, `SMS_DAILY_SPEND_CAP_CENTS` and `SMS_ALERT_SEND_THRESHOLD` from the process environment on every call, and `backend/settings_store.py` has no `sms.*` entry (its `SPECS` list holds only chat settings), so there is no settings row for the text caps yet. To raise it now: change the two variables on the Railway web service. That is not a code release, but Railway restarts the service with the same image and the new values (about a minute or two; signed-in people stay signed in because the cookie does not change; record the change in the release log as `docs/RELEASING.md` asks). A running process never sees a new variable without that restart. Card S6 adds `sms.daily_send_cap` and `sms.daily_spend_cap_cents` to `settings_store.SPECS` (row, then environment, then the default above) with env-only ceilings `SMS_DAILY_SEND_HARD_MAX` (default 100) and `SMS_DAILY_SPEND_HARD_MAX_CENTS` (default 600), and card C5's admin page edits them: from then on the owner can raise the day from the admin page on a phone, live within 30 seconds, up to the ceiling, with no restart. Raising above the ceiling still needs the Railway variable, on purpose, so a stolen admin session cannot open the bill.

Guess odds: 5 tries on a million codes is 1 in 200,000 per lock window.

## 6. Order of checks for "send me a code"

1. Flag on, request has a valid session cookie and CSRF token.
2. Consent box ticked (server checks it; the text version is stored).
3. **Turnstile:** the browser's token is verified on the server with Cloudflare's `siteverify` call (`TURNSTILE_SECRET_KEY`), sending the client IP; 3-second timeout; **fails closed** (no token, bad token, or Cloudflare unreachable means no text). A token works once.
4. Normalise to E.164 and check the country list.
Steps 5 and 6 are **reserve first**: in one Postgres transaction take `pg_advisory_xact_lock(hashtext('auth_phone_send_cap'))`, count the limits and pool shares in `auth_phone_sends`, check the wrong-tries lock and the site send, spend and prepaid caps, and INSERT the send row `pending` with its estimated cost (Lookup cost included even for refused numbers); commit.
7. **Lookup** (line type): block VoIP and landline; cache the result by phone hash for 30 days (also for refused numbers); skipped for known numbers.
8. Start the Verify text, update the send row to `sent` or `provider_error` (cost 0 if rejected), create the challenge, put `public_id` and a random nonce in the session cookie.

Provider down, balance empty or a cap hit: `unavailable` and cap outcomes cost 0 and do not count against a person's limits; after 5 in a row a circuit breaker skips Twilio for 5 minutes; the owner gets one email a day per event (`cap_hit`, `cap_50pct`, breaker) at `OWNER_ALERT_EMAIL`. A runtime switch `auth_phone.sends_paused` stops sends without a release.

Refusals are recorded as send rows with a `blocked_*` outcome so they appear in the counts. A refusal about the number itself (VoIP, landline, non-US, a per-number, per-device or per-address limit) always reads "We could not send a code to that number. Check it and try again, or try later." When the site send cap, the spend cap or any pool blocks a send, **every** caller sees the same busy sentence, "Sign-in by text is busy right now. Please try again after midnight Eastern time.", whether the number has an account or not (changed 2026-11-14: a different sentence for known numbers would tell anyone which numbers have accounts).

## 7. Codes: checking

`POST` verify: the challenge must be open, unexpired, match this session's nonce, and the wrong-tries lock must not apply; then ask Twilio to check the code. The attempt is reserved before the vendor call (`UPDATE ... SET attempts=attempts+1 WHERE status='open' AND attempts<5 AND expires_at>now() RETURNING`) and refunded only on network error or throttle. Vendor statuses map: approved; `max_attempts_reached` is too-many; `expired`, `deleted`, `failed` are expired; `pending` and `canceled` are wrong or unknown. A wrong code adds one to `auth_phone_wrong_tries`. An approved code closes the challenge in the same transaction (`UPDATE ... WHERE status='open' RETURNING`) so it works once. A code typed in another browser fails.

## 8. Device cookie and the real client address

- **Device:** a rate-limit key only, never a trust signal (it never skips a code or marks a browser trusted). A random 128-bit signed cookie `sc_dev`, set before the first phone request and kept 400 days. Its hash is the "device" in limits. Clearing cookies gives a new device, which is why IP, phone and the whole-site cap exist too.
- **IP address:** you asked to read `CF-Connecting-IP`. The site is already built to sit behind Cloudflare and then Railway's edge (`B/utils/client_ip.py:1-30`, `TRUSTED_PROXY_HOPS`, `TRUSTED_PROXY_CIDRS=cloudflare`). A header anyone can send is only safe if the request really came through Cloudflare, and the file's own notes say a request sent straight to the Railway address could forge it until that address is removed and the origin locked (`client_ip.py:30-33`). **Design:** a new function `phone_client_ip()` returns `CF-Connecting-IP` only when the existing trusted-proxy check says the request arrived through Cloudflare's ranges; otherwise it falls back to `client_ip()`; it also requires a shared-secret header, `X-Origin-Secret` (owner accepted the name 2026-11-14; `ORIGIN_SECRET_HEADER` in `B/utils/client_ip.py`), added by a Cloudflare Transform Rule (`CF_ORIGIN_SECRET`), because any sender using Cloudflare's own servers can set the header. The same function serves chat limits. Owner question: is `sarraficars.com` proxied by Cloudflare (orange cloud) and has the `*.up.railway.app` address been removed?
- IPv6 addresses are counted by /64 (as login limits are today, `B/auth/pages.py:35-51`).

## 9. Sessions: longer, with a way to cut them

- Lifetimes (owner decision 2026-11-13): shoppers **30 days, sliding** (`SESSION_DAYS_SHOPPER`, default 30); **admins and `/dev` operators 12 hours, absolute** (`SESSION_HOURS_ADMIN`, default 12); dealers and other staff 14 days, absolute (`SESSION_DAYS_STAFF`). Today the lifetime is a fixed 14 days (`B/main.py:82`). The cookie lifetime is global (30 days); one `before_request` hook reads the role from the database and applies the limit from `auth_at`, set inside `finalize_app_session`, `_finalize_dev_session` and both OAuth completions. See card S10.
- A signed cookie cannot be revoked (survey 4). With 60-day cookies that matters more, so phone accounts get a row in `auth_session_epochs` (S5): a "sign out everywhere" button, a number change, a claim, an admin action, suspend, delete and a password change set `valid_after` to now; the session hook rejects any session whose `auth_at` is older (cache 5 s for admin and dealer, 30 s for shoppers; fails closed for admin and dealer). `build_access_context` (`B/billing/access.py:405-482`) is not the place for this; the new hook is. When the planned `users.auth_epoch` lands, move this onto it.
- Session keys added at login: `auth_method` (`phone`, `password`) and `auth_at`.
- After a number change or "sign out everywhere", other devices must sign in again (and pay the 6 cents).

## 10. Who signs in how

| Account | Sign-in |
|---|---|
| Shopper (`general_user`) | phone and text code only |
| Dealer (`dealership_owner`, `dealership_admin`, `dealership_member`) | password through the existing dealer login (`/dealer/login`, `B/dealer/routes.py:74-111`), with the sign-in rules of section 23.7. Dealers sign up by themselves at `/dealer/register` and verify the dealership (section 23.4) |
| Site admin (app role `admin`) | password on a **staff sign-in page** `/staff/login` (the old form, same code as `B/auth/pages.py:81-114`), plus the authenticator step from card S12 |
| Operators (`/dev`, `admin_users`) | password, plus the authenticator step (S12) |

What happens to the old routes:
- `GET/POST /login` becomes the phone page. A small plain link, "Staff sign-in", leads to `/staff/login`.
- `/staff/login` and `POST /api/auth/login` accept **only** staff and dealer roles. A shopper account that submits a password gets the same generic failure as any wrong password (no hint that the account exists or uses a phone), and the page header says "Shoppers sign in with a phone number."
- `/register` and `POST /api/auth/register` no longer create shopper accounts: they redirect to the phone page (JSON: 410 `use_phone_sign_in`). Dealer registration stays open to anyone, behind the guard of section 23.3 (Turnstile, then the dealership verification of section 23.4; nothing of the account is public until verified).
- `/forgot-password`, `/reset-password`, email verification and `/resend-verification` stay available only for staff and dealer accounts and are hidden from shoppers.
- Phone login **refuses** every account that fails `is_phone_eligible` (stored role `general_user`, or an empty role only with no dealer id, registry id and org id; active; not an env-admin) with the same generic failure and a log event; the claim flow (section 12) and number change use the same test. `/dealer/login` and `/api/auth/login` accept only `dealership_*` roles; `/staff/login` is the only password door for `admin` and then needs the S12 step; Google and Apple never open `/admin` or `/dev` without the authenticator step; `/dealer/register` stays open with the guard and verification of section 23 (owner decision 2026-11-14; the earlier closed-by-default flag is withdrawn).
- Google and Apple routes: section 11.

## 11. Google and Apple sign-in: my recommendation

**Recommend: off for shoppers at launch** (flag `AUTH_OAUTH_SHOPPERS_ENABLED`, default off; code stays). Reasons:
1. The chat limits count per phone number so new accounts cannot dodge them (`CHAT_LIMITS.md`). A Google or Apple account has no verified phone, so every free Google account would get a fresh chat allowance and the dodge is back. (If you want them later, require a verified phone before chat, which keeps the limits sound but makes Google sign-in a two-step flow.)
2. Fewer ways in means less to secure and test. The Apple fake relay email (`B/auth/apple_oauth.py:206-209`) and email-based account linking (`B/auth/oauth_link.py`) are the trickiest part of today's login.
3. Cost goes the other way: Google and Apple sign-ins are free and there would be fewer texts at 6 cents each. That is the case for keeping them.
4. The iOS app: Apple's store rules want Sign in with Apple whenever another third-party login is offered; with neither offered the rule does not apply. Please check this against current App Store rules before the iOS release.

Existing Google and Apple shoppers use "claim your old account" (section 12) the same way as password shoppers. **Question: confirm off for shoppers?**

## 12. Existing shopper accounts: the least painful migration

Facts: shopper data hangs on `users.id` (saved cars, saved searches, hidden dealers, search history, view and compare history, comments, reviews; survey 1.4). I do not know how many real accounts there are (no production access from here); you said very few.

**Recommended: "claim your old account" by email link, with start-fresh as the fallback.** No bulk mailing needed.
1. After signing in by phone, a new person sees one plain line: "Had an account here before? Add it to this number." (also on the phone page for signed-out people).
2. They type their old email. We always answer "If that email has an account, we sent a link" (no guessing which emails exist). The link is a one-time token (the existing reset-token pattern, `B/auth/password_reset.py:173-187`, 30 minutes; sent with the existing email sender, `B/utils/mfa_delivery.py:56-180`).
3. The link only stores the token in the claimant's browser session (a `GET` changes nothing). The page asks for the phone number and runs the normal text-code check (`purpose='claim'`); the token is consumed in the same transaction as the approved code. When it passes, the phone number is attached **to the old account** (the old `user_id` keeps all its data), the old password, `google_sub`, `apple_sub` and email tokens are cleared, other sessions are cut (`auth_session_epochs`), the old email gets a notice, and the person is signed in as that account. Claim is for eligible shoppers only. Nothing is copied or merged. Google and Apple accounts work the same way, because the token proves the email.
4. If the person had already signed up fresh with that number, the new empty account is merged into the old one by a small admin-reviewed action: re-key the shopper tables listed in survey 1.4 from the new id to the old id, then delete the new row. Card S5 includes the table list and a test that every `user_id` table is covered. If the new account already holds data, support decides.
5. Requirements: transactional email must work (`RESEND_API_KEY`, `RESEND_FROM` set; `PASSWORD_RESET_ENABLED` is not required for this flow but the sender is; survey 5).
6. After 90 days, unclaimed shopper accounts with a password and no phone are listed for the owner to delete or anonymise (comments and reviews keep a display name, per the privacy text). The claim link keeps working until then.

**Start fresh instead** (simplest; reasonable with very few users): no claim flow; old shopper accounts are left dormant and deleted after 90 days. **Data that would be lost for each person:** saved cars, saved searches, hidden dealerships, search history, viewing and compare history, their link to comments and reviews (the text stays, the owner of it does not), and their Google or Apple link. Nothing financial is lost (billing was never on).

Cost of the recommendation: card S6 (claim flow), a few hundred lines. If you pick start-fresh, drop S6 and keep only the 90-day clean-up.

## 13. Lost phone and recovery

- Phone-only account, lost or changed number: a person who still has the old number signs in and changes it through the account page (fresh code to the old number, then a code to the new one, `purpose='change'`).
- Lost number and no access: **support may replace the number after proof** (you agreed). Proof to ask for: the email used for a claim, a saved-search name or saved car, the approximate sign-up date. An admin uses a "replace phone number" action that texts a code to the new number, records an audit line (admin id, account id, reason; no number) and signs the account out everywhere.
- If the account has a real email, a "your sign-in number changed" notice goes to it (last two digits only).
- A recycled number: if a number has not been used to sign in for `AUTH_PHONE_DORMANT_DAYS` (default 180), a new holder gets a fresh empty account and the old data stays reachable only through the claim email. Within 180 days a new holder still reaches the account: that residual risk is accepted and written here. Other controls: fresh-code check before number changes; notice to a verified email (verified at least 24 hours earlier; a change started within 24 hours of an email change is held 24 hours); chat limits count the phone hash. Support replaces a number only after confirmation from the account's verified email plus a 24-hour hold and a notice; with no verified email the person starts a new account.
- Staff and dealers recover by the existing reset-password flow.

## 14. Abuse cases and the control for each

| Case | Controls |
|---|---|
| **SMS pumping** (fraudsters trigger texts to numbers they profit from) | Turnstile on every send, failing closed; US only; VoIP and landline block via lookup; per phone (3 a day), device (3), IP (5) and /24 or /48 (6) limits; whole-site 30 sends and 3 dollars a day in the first month, both blocking (code defaults); unknown numbers at most half the day; Twilio's own fraud protection on and geographic permissions US only; alert at 15 sends; a rising ratio of sends to approved codes is the alarm; kill switch. Most an attacker can cost: the 3-dollar day |
| **Burning the day's texts to lock people out** | section 5 "Lock-out risk": per-subject limits of at most one fifth of the day, a known-number share the unknown-number pool cannot touch, the busy sentence, the `cap_hit` email and a cap the owner can raise |
| **Seller and dealer abuse: bait listings, hijacked VINs, spam, curbstoners, fake dealers** | section 23.9 |
| **Finding who has an account** | the page and its words are identical for known and unknown numbers, at the caps too; sign-up and login are one flow; the claim flow always says "if that email has an account"; Turnstile and limits make bulk probing costly. Left: while only the new-number pool is used up, a known number still gets a code and an unknown one does not; each such probe texts the real holder, spends a send and counts against that number's 3 a day, and the nightly ratio of sends to approved codes shows it |
| **Guessing codes** | 6 digits, 5 wrong tries then a 15-minute lock that new codes do not reset, 5-minute life, single use, session binding |
| **SIM swap, number takeover** | cannot be fully stopped by texts. Fresh code before number changes, notice to a real email, admin and dealer roles excluded, "sign out everywhere"; later option: a waiting period before a changed number works |
| **Dodging chat limits with new accounts** | one account per number (unique index), US-only, no VoIP, per phone, device and IP chat counters, whole-site chat budget (`CHAT_LIMITS.md`); Google and Apple off for shoppers |
| **Racing two tabs** | unique index; the loser deletes the account it just made and signs in as the winner |
| **Fake Twilio webhook** | signature check (section 17) |
| **Forged client IP** | `CF-Connecting-IP` only when the request came through Cloudflare (section 8) |
| **Cookie theft** | unchanged from today; "sign out everywhere"; fresh code before number changes |

## 15. Screens and endpoints

UI rule: **no cards, boxes or pill badges** (owner decision `docs/remediation/OWNER_DECISIONS_LOG.md` D-N19; `docs/VISUAL_REVIEW_2026_09_28.md` rows 31-32: one flat primary button, underline fields). Plain headings, one column, sentences for states, text links for secondary actions, inline error lines. The Turnstile widget is a small embedded element the vendor draws; use its compact size and put it inline under the field.

Phone page, top to bottom: heading "Sign in", one underline field "Mobile phone number", the Turnstile widget, an unticked checkbox with the consent text next to it, the flat button "Text me a code", a plain link "Staff sign-in". **Consent text (as you wrote it; draft for the lawyer in card S14; it must also name the program "Sarrafi Cars", link to Terms and Privacy beside the box, give a support contact for HELP, and say carriers are not liable for delayed or undelivered messages):** "I agree to receive one-time login codes by text message from Sarrafi Cars at the number above. Message and data rates may apply. Message frequency: only when I request a code. Reply STOP to opt out or HELP for help. See our Terms and Privacy Policy. We do not sell or share your phone number or opt-in data with third parties for marketing." The button stays disabled until the box is ticked, and the server checks it too.

Provider sentences (card S6 reasons): unavailable: "Sign-in by text is not available right now. Please try again later." Opted out: "Texts to this number are blocked. Reply START to the number, or use another number." At a cap or pool limit every number gets the same busy sentence (section 6).

Code page: "We sent a 6-digit code to the number ending 23." One field, button "Sign in", text links "Send again" (counts down 60 seconds) and "Use a different number".

**Login text** (set in the Twilio template, not in our code): "Sarrafi Cars: Your verification code is 482913. It expires in 5 minutes. Do not share this code with anyone." (see section 1 about 5 versus 10).

| Method and path | Purpose |
|---|---|
| `GET /login` | phone page |
| `POST /login/phone` | start (form CSRF token and Turnstile token) |
| `GET/POST /login/phone/code`, `POST /login/phone/resend`, `POST /login/phone/change` | enter code, resend, use a different number |
| `GET/POST /login/claim`, `GET /login/claim/<token>` | claim an old account (section 12) |
| `GET/POST /account/phone`, `POST /account/phone/verify`, `POST /account/sign-out-everywhere` | change number (fresh code first), sign out everywhere |
| `POST /logout` | unchanged |
| `GET /staff/login`, `POST /staff/login` | password sign-in for staff and dealers (old form) |
| `POST /api/auth/phone/start`, `/verify`, `/resend` | JSON twins for the iOS app (header CSRF token) |
| `POST /webhooks/twilio/verify` | vendor callbacks (signed; section 17) |

All phone routes answer 404 while `AUTH_PHONE_LOGIN_ENABLED` is off. New POST routes go into the CSRF lists (`B/web/security.py:49-98`; `test_csrf_coverage.py` fails otherwise); the Twilio webhook is exempt like the Stripe ones, protected by its signature instead. JSON routes go in `B/mobile/contract.py:27-47` and `ios/docs/API_CONTRACT.md`.

**iOS app.** It logs in with email and password through `/api/auth/login` and registers with email (`ios/.../APIClient.swift:70-110`, survey 9.3). After this change shoppers cannot do that. First step: the app opens the web phone page in its existing web view (cookies are shared, `SessionCookieBridge.swift`); later, native screens call the JSON twins. Owner question: is the app already in the App Store? If yes, old builds will show a login error until updated; the API should return a clear `use_phone_sign_in` message the old app can display.

## 16. Privacy

- **Stored:** the phone number in E.164, **in clear text on purpose** (needed to text it and to show "ending 23"; the hash only keeps raw numbers out of limit, send, counter and log rows and does not protect against a leak of both the database and the pepper), a hash for limits, the consent time and text version, the line type. Hashes only in send rows, chat counters and logs. The code is never ours (Twilio holds it). No message text, no vendor text.
- **Retention:** challenges 7 days after they end; send rows 30 days; wrong-try rows 24 hours after unlock; chat counters 35 days; identities until the account is deleted. A nightly job deletes by age.
- **Deletion:** the person can ask to delete the account; the identity row and personal rows are deleted **within 30 days** (the Terms and Privacy pages say so). The chat counter for the phone hash can stay for 35 days then goes with the nightly job. Twilio keeps its own records under its own policy: review at account setup.
- **Consent record:** `consent_at` and `consent_text_version` per identity; changing the consent text increments the version.
- **Terms, Privacy Policy and consent page:** none exist on the site today (no template). Card S14 drafts them for owner approval; the pages and the phone page show a DRAFT banner and the boot check refuses the flag until `LEGAL_PAGES_APPROVED_VERSION` equals the consent text version: phone number treated as personal data, purposes (sign-in codes only), no sale or sharing for marketing, retention, deletion within 30 days, STOP/HELP, contact address. I am not a lawyer; the PR marks them as drafts.
- Admin lists show the last four digits only.
- **Pepper runbook:** rotating `PHONE_HASH_PEPPER` resets counters and locks for at most 35 days (acceptable; login is unaffected because identities are found by `phone_e164`, never by hash; do not look identities up by hash). Sub-keys per domain (phone, ip, device) are derived from it.
- **Deletion:** `purge_user_data` (card S7) removes the identity and every listed shopper table; chat counters and send rows keep only hashes and expire at 35 and 30 days (they are not deleted on request, because deleting them would reset allowances).

## 17. Twilio webhook and logs

- **Not used at launch.** Costs, caps and delivery counts come from the app's own send rows (`SMS_EST_COST_CENTS` times sends), never from callbacks. The route stays in the code, inert (404 unless `TWILIO_WEBHOOK_ENABLED`; the validator uses `TWILIO_AUTH_TOKEN` only). When enabled: `POST /webhooks/twilio/verify` reads the form body, validates `X-Twilio-Signature` with the `twilio` library's request validator and `TWILIO_AUTH_TOKEN` (or the API key secret), using the public URL (`PUBLIC_BASE_URL`) the vendor was given; a bad or missing signature gets 403 and a count. Accepts delivery-status events (sent, delivered, failed, undeliverable) to update the send row outcome and cost estimate. **No message bodies, phone numbers or codes are logged** (only event type, our hashed reference, outcome). Unknown events return 200 and are ignored. Idempotent by vendor event id. CSRF-exempt by being listed as signature-protected.
- Logger `auth_phone`, key=value lines: `event`, `provider`, `country`, `purpose`, `reason`, `latency_ms`, and a 6-character hash prefix only on `blocked_*` and `check_locked` events. Events: start, blocked_turnstile, blocked_voip, blocked_country, blocked_limit, blocked_cap, provider_error, check_ok, check_wrong, check_locked, check_expired, signup, login, claim, change, cap_50pct (the alert at half the send cap: 15 sends at launch), cap_hit, webhook_bad_signature.
- A nightly job (`backend/scripts/auth_phone_nightly.py`) writes yesterday's totals (sends, approvals, ratio, cost estimate, blocked by reason) and exits non-zero at the alert level so a scheduled routine can notify the owner.
- A read-only operator page in `/dev` shows today's sends against the day's caps (30 sends and 3 dollars in the first month) and the pool shares.

## 18. Feature flag and rollout

- `AUTH_PHONE_LOGIN_ENABLED`, default off. Off: phone routes 404, `/login` still shows the old form (so turning the flag on is the switch from email to phone). Because shopper password sign-in is removed from day one, **the flag flip is the launch**; before it, shoppers sign in the old way.
- `AUTH_PHONE_ALLOWED_NUMBERS` (optional list) restricts phone sign-in to named numbers for the first test days (owner and friends) while the old form is still the default for everyone else; with it set, only listed numbers can use the phone page and the old form stays visible.
- Rollout is card S18. Rollback: flag off puts the old form back for shoppers whose accounts still have a password; accounts created by phone have no usable password and **cannot sign in until the flag is back on**; warn before rolling back after launch.

## 19. Pay structure and this design

With everything free (free means no plan gating; every feature still needs a phone sign-in in production), there are no plans, trials or Stripe flows to protect: `users.id` is the only key. The plan columns, `B/billing/` and the checkout pages stay in the repository, dormant, with `BILLING_STRIPE_ENABLED` off (a test keeps it off and keeps the paid pages hidden). Phone verification is still useful later if paid plans or trials return: the verified, unique number is a good "one trial per person" key. See `PAY_AND_STRIPE_AUDIT.md` (kept as the revival checklist).

## 20. Environment variables (names only)

Names you listed: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN` (or `TWILIO_API_KEY_SID` and `TWILIO_API_KEY_SECRET`), `TWILIO_VERIFY_SERVICE_SID`, `TURNSTILE_SITE_KEY`, `TURNSTILE_SECRET_KEY`, `SMS_DAILY_SEND_CAP` (first month and code default 30; owner decision 2026-11-14), `SMS_ALLOWED_COUNTRIES` (`US`), `PHONE_HASH_PEPPER` (random, 32 or more characters).

Names I add (all with safe defaults, so you only set them if you want to change a number): `AUTH_PHONE_LOGIN_ENABLED`, `AUTH_PHONE_PROVIDER` (`fake` or `twilio_verify`), `AUTH_PHONE_ALLOWED_NUMBERS`, `AUTH_PHONE_CODE_TTL_S` (300), `AUTH_PHONE_MAX_WRONG_TRIES` (5), `AUTH_PHONE_LOCK_MINUTES` (15), `AUTH_PHONE_RESEND_WAIT_S` (60), `SMS_DAILY_SPEND_CAP_CENTS` (300), `SMS_ALERT_SEND_THRESHOLD` (unset means half the send cap: 15), `SMS_DAILY_SEND_HARD_MAX` (100) and `SMS_DAILY_SPEND_HARD_MAX_CENTS` (600) (env-only ceilings for the settings rows of card S6), `SMS_DEALER_SIGNUP_SHARE_PCT` (10), `SMS_EST_COST_CENTS` (6), `AUTH_OAUTH_SHOPPERS_ENABLED` (off), `SESSION_DAYS_SHOPPER` (30), `SESSION_HOURS_ADMIN` (12), `SESSION_DAYS_STAFF` (14), `AUTH_PHONE_DORMANT_DAYS` (180), `SMS_EST_LOOKUP_CENTS` (1), `SMS_NEW_NUMBER_SHARE_PCT` (50), `TWILIO_WEBHOOK_ENABLED` (off), `LEGAL_PAGES_APPROVED_VERSION`, `CF_ORIGIN_SECRET`, `OWNER_ALERT_EMAIL`, plus the marketplace settings of section 23.10 (`DEALER_SELF_REGISTER_ENABLED` is withdrawn: dealer sign-up stays open, guarded), plus the chat names in `CHAT_LIMITS.md` section 5. The per-phone, per-IP, per-device and per-/24 send limits are settings rows with the defaults in section 5 (same mechanism as chat). Production refuses to start with the flag on and the fake provider, a missing pepper, or a missing Turnstile or Twilio value.

## 21. Risks

- Texts are a weak login against SIM swap; that is why staff are excluded and number changes need a fresh code.
- Every login costs money, including attackers' attempts; Turnstile first, then the caps, protect the bill. Set a spend alert at Twilio too.
- With password sign-in removed, the vendor, Cloudflare or Twilio being down means shoppers cannot sign in; existing 30-day cookies keep most people in.
- The two-store write (SQLite user, Postgres identity) is not atomic; the compensating delete handles the race, and the users cutover removes the problem.
- A person who loses a phone number and has no email or support proof loses the account.

## 22. Questions for the owner

1. Google and Apple for shoppers: off at launch (my recommendation), or keep with a verified phone before chat?
2. Code lifetime and text: both say 5 minutes (my recommendation), or both 10?
3. Old accounts: claim-by-email flow (my recommendation) or start fresh? Roughly how many real shopper accounts exist?
4. Is `sarraficars.com` proxied by Cloudflare, and has the `*.up.railway.app` address been removed?
5. Is the iOS app in the App Store?
6. Lock after 5 wrong tries for 15 minutes: right?
7. Site-wide chat budget of 5 dollars and 2,000,000 tokens a day to start: right?
8. Keep consent-record text versions in the database (as designed) so you can prove who agreed to what?
9. Dealer guard (section 23.12): the yes/no questions there.

## 23. Marketplace: private sellers and verified dealerships (owner 2026-11-14)

The owner's words: "i want a verification process for dealerships, the idea is to be like cars.com or marketplace, anyone can post a car on this website to sell their car, but i also dont want bots mass creating or spamming cars or abusing this system". This section replaces the earlier plan to close `/dealer/register` (`DEALER_SELF_REGISTER_ENABLED`, withdrawn). There are two kinds of seller: a **private seller** (any signed-in, phone-verified account; free to post) and a **verified dealership** (a business that finished the verification of 23.4). Dealers wait for **verification**; a private seller's **first** listing waits for review, later ones do not. Public posting stays **off in production** (`MARKETPLACE_POSTING_ENABLED`, default off) until the guard cards S19 to S29 are released. Cards are in `TASKS.md`. Nothing here is built. The cards and their waves: S19 and S20 close today's open doors in wave 1 (no migration); S21 to S29 build the marketplace; S30 hardens dealer sign-in.

### 23.1 What exists today on `dev` 1.5.32 (checked in the code)

- **Anyone can register a dealer account** at `/dealer/register` (`B/dealer/routes.py:114-161`): username, email, password and an organisation name. No bot check, no email or phone check, no review. The account gets role `dealership_owner` and a new organisation and is signed in at once. The only brake is 20 attempts a minute per address, kept in memory (`:69-70,117-124`). The form is CSRF-protected like every `dealer_portal.` POST (`B/web/security.py:98`).
- **Dealer sign-in** `/dealer/login` (`:74-111`): 20 a minute per address and the shared lock of 10 failures in 15 minutes per account (`B/auth/pages.py:63-78`, memory, per process). It accepts any role, shoppers too.
- **Any signed-in account can post cars, not only dealers.** The `/inventory` routes check only that the account exists and is active (`_require_login`, `:164-179`); there is no role check. Since shopper sign-up at `/register` is open, a shopper account can post today.
- **A posted car is public at once.** "Add by VIN" (`:222-260`) checks only the VIN's shape (17 letters and digits, `B/enrichment/nhtsa_vpic.py:22,37-42`, no check digit), fills fields from the NHTSA decode, saves a `dealer_vehicles` row and copies it into the public `cars` table with `dealer_id` `dealer-portal-<user id>` (`B/dealer/portal_sync.py:42-46,120-171`). Search shows it (`search/where.py:141`). The ZIP-based listings grid does not, because portal rows have no `dealership_registry_id` or `dealer_url` and the grid finds coordinates only through those (`B/db/repositories/grid_cards_repo.py:1035-1042,919-935`).
- **Edits are public at once and unchecked** (`:352-418`): make, model and year can be changed away from the VIN, any price is accepted, and `notes` (up to 2000 characters, shown as the description) may hold links, phone numbers or emails. There is no limit on how many cars an account posts (only 20 VIN adds a minute per address).
- **Photos** (`:263-316`): up to 8 per request, 24 per car, 8 MB each. The type is the browser's claim (`f.mimetype`), not checked from the bytes, and location data in the photo is not removed. Files go to the web container's disk (`UPLOAD_ROOT`, `:61-62`), which a redeploy wipes, and are served only to the uploader (`:450-473`). So the public never sees them (broken images): a bug today, not yet a safety hole.
- **Already safe:** every portal query is scoped by the signed-in user's id (`ddb.get_vehicle(uid, …)`), so one account cannot touch another's rows; a portal VIN that already belongs to a scanned car or another account is not copied to `cars` (`portal_sync.py:99-117`), but the dealer is not told and the private row stays; deletes retire only the account's own `cars` rows (`:179-208`).
- **Unsafe:** (1) shoppers can post; (2) no bot check or verification on dealer sign-up; (3) no review, caps or content checks before going public; (4) suspending an account in the admin hub (`B/dealer/admin/users_hub.py:79`) stops its sign-in but leaves its cars public (only deleting the account retires them, `B/db/users_db/accounts.py:471-474`); (5) the scanner can take a user's car over (below).
- **The scanner and users' cars** (the nightly scan starts at `backend/scripts/fleet_scan.py`): the roster takes every `dealer_id` with an active car (`B/scanner/manifest.py:250-275`), so a `dealer-portal-*` id joins the nightly scan list; the VIN upsert (`B/scanner/upsert/sql.py:62-210`, `dealer_id=excluded.dealer_id` at `:95`) lets a scanned dealer take a portal row when the owner is inactive, the 48-hour guard window has passed, or the claimant is "strong" (`B/scanner/upsert/ownership.py`, `guard.py`); `delete_cars_with_dummy_placeholder_vins` deletes by VIN alone (`B/db/repositories/cars_repo.py:38`); `backend/scripts/resolve_photo_attribution.py` can move any active car whose photo text names a dealer domain. Nothing outside `portal_sync.py` knows the `dealer-portal-` prefix.

### 23.2 Trust ladder (cautious numbers; all are settings rows, section 23.10)

| | New seller | Established seller | Verified dealership |
|---|---|---|---|
| Who | any signed-in account with a verified phone (shopper phone sign-in, or the seller phone step of 23.3) | a private seller that passed the bar below | finished 23.4 |
| Active listings | 2 | 5 | 300 own posts, plus the scanner's cars of its own claimed dealership |
| New listings a day | 1 | 2 | 50 in the first 30 days after verification, then 200 |
| Photos | at least 3, at most 12 | at least 3, at most 20 | at least 1, at most 40 |
| Goes live | **first listing waits for owner review**; the second and later go live at once if the first was approved and the automatic checks pass | at once after the automatic checks | at once after the automatic checks (holds still apply) |
| Free text | no links, phone numbers or emails; buyers write through the site (23.5) | same | same, except a link to its own verified website |
| How it is reached | phone verified | account 7 days or older, 2 listings approved or live without a hold, no report upheld in 30 days | 23.4 approval, renewed yearly |

**Volume needs verification.** A private seller is asked to verify as a dealership, and every further post is held, when any of these is true: more than 5 cars posted in 12 months, more than 3 different VINs in 7 days, or dealer-like text (stock number, "financing available", "we finance", "buy here pay here", "dealer fees", a business name such as "Motors", "Auto Sales", "LLC" or "Inc"). **Curbstoners** (unlicensed dealers posing as private sellers) are caught by the same signals plus three more: the VIN was listed by a scanned dealer within the last 60 days (bought at auction or traded in, then flipped), the same photos (perceptual hash) or text appear under another account, and the same device or address group posts for several phone accounts. The response is a hold and the plain line "This looks like a dealership. To keep posting, verify your dealership.", never a public accusation.

### 23.3 Becoming a seller

- **Private seller:** after phone sign-in launches (card S8), any shopper account already has a verified phone. Before that, and for dealer staff who sign in by password, a seller phone step texts a code with purpose `seller_verify` from the reserved dealer and seller pool (10 percent of the day, 3 texts; section 5). Mobile numbers only (Lookup refuses VoIP and landline), one seller account per phone number, Turnstile on the step.
- **Sign-up and posting friction for everyone:** Turnstile on dealer sign-up, on seller start and on every publish (not on drafts); per account, device, address and /24 or /48 limits that halve for 24 hours after a refused attempt (failed Turnstile, refused VIN, held post); `seller.signup_paused` and `marketplace.posts_paused` stop new sellers and new posts at once.
- `/dealer/register` stays open: it creates a plain account (role `dealership_owner` kept for existing code) that can sign in, set its profile and save drafts, and then starts the dealership verification of 23.4; nothing of it is public until verified. **Grandfathered** means an account that had at least one `dealer_vehicles` row before the S19 cut-off (`GRANDFATHER_CUTOFF`, recorded by card S19, which lists the accounts by id for the owner to check). Their live cars stay live and they are asked to verify. A role alone never grants a tier: a grandfathered account becomes an established seller only after the `seller_verify` phone step (one seller per phone); until then new posts use new-seller limits and its first new listing goes to review. Every account created by `/dealer/register` after the cut-off is a new seller until it passes `seller_verify` or the verification of 23.4. Scanner-owned dealers and cars are never changed by any of this.

### 23.4 Dealership verification (about 10 minutes for a dealer, about 3 for the admin)

1. **Start** ("Register a dealership", signed in): dealership name, street address, main phone, website, state, dealer license number, contact person (name and role). One screen, about 3 minutes.
2. **Match** against our scanner data (`dealerships` table, `migrations/V001__baseline.sql:722-753`: `website_url`, `dealer_website_url`, `phone`, `street_address`, `city`, `state`, `zip_code`): same registrable website domain, or same normalised phone, or same address. A match makes this a **claim** of that dealership (like claiming a business on Google); otherwise it is a **new** dealership. **A claim is proven only by control of the registry row's own domain** (the registrable domain of its `website_url` or `dealer_website_url`). A match by phone or address while the entered website's domain differs is shown to the admin as "possible claim, different website": it is never verified as a claim and never auto-approved. A dealership already claimed by a verified account is never auto-approved; the admin sees both.
3. **Proof of control of the website** (one is enough):
   - an emailed 6-digit code to an address at the **same registrable domain** as the website (sales@ the dealer's domain); free-mail and disposable domains are refused; 15 minutes, 5 tries, 3 sends a day; email costs nothing, so this is the default;
   - or a token we show, placed as a meta tag `<meta name="sarraficars-verify" content="…">` on the home page, as a file at `/.well-known/sarraficars-verify.txt`, or as a DNS TXT record `sarraficars-verify=…`.
   For a claim, the email address, meta tag, file and TXT record must be on the **registry row's** domain, not just the entered one.
   **The website fetch is SSRF-safe:** https on port 443 only; the host must be the entered domain or its `www.`; we resolve it ourselves and refuse every address that is not globally routable (private, loopback, link-local, carrier NAT, multicast, `0.0.0.0/8`, `fc00::/7`, IPv4-mapped, NAT64 and 6to4 forms, cloud metadata), then connect to that checked address while TLS still checks the certificate against the host name; at most 2 redirects, each re-checked and only within the same registrable domain; 5-second total time, 256 KB read, no cookies; 3 checks per dealership an hour and 10 per account a day, and at most 200 fetches and 100 verification emails a day across the site. It runs in a background thread with the job's own timeout, not inside the web request, so the single worker is never held. Tests use a fake fetcher, never the network.
4. **Business phone:** the main phone is compared with our `dealerships.phone` first, then with the public number on the dealer's own website (the same fetch, reading `tel:` links); for a claim, only the registry row's website and phone count, never a site the applicant supplied. A match passes at no cost. With no match, a **voice** code (business lines are often landlines) is offered from the dealer and seller pool (3 a day; about 6 cents) if Twilio Verify voice is switched on (owner step); otherwise it is left to the admin, who may call the public number.
5. **License:** number and state are stored, one dealership per (state, license). The admin checks it by hand on the state's public lookup page (the review screen links to the state's page from a small list kept in the repository); we do not scrape state sites.
6. **Admin review screen** `/admin/dealerships/review/<id>`: plain rows, no cards, boxes or pill badges: the entered details; claim of which scanned dealership or new; the registry domain next to the proof domain, with a sentence saying whether they match; each automatic check as a sentence with passed or failed (domain email code, website token, phone matches public number, license unique, free-mail none, account age, phone verified); the license lookup link; buttons Approve, Reject and Revoke, each with a required reason. Every action goes to `marketplace_audit`.
7. **Owner minutes:** about 3 minutes per dealership (most of it the license lookup). 5 dealerships a week: about 15 minutes a week. 50 a week: about 150 minutes a week, about 20 a day; at that level, turn on auto-approval for requests where every automatic check passed and the license field is filled, and look only at the rest (`dealer.approval_mode`, default `manual`). Auto-approval covers only new dealerships and claims whose proof domain equals the registry domain; never a claim matched only by phone or address. The dealer's waiting screen says: "Thank you. We check every dealership by hand, usually within two business days. You can add cars now; they go public after approval."
8. **Re-verify** every 365 days (notice 30 days before; on lapse the dealership drops to established-seller limits and loses "Verified dealer", its cars stay), and at once when the website or main phone changes (new posts are held until done). **Revoke** on abuse: every listing hidden, claim released, sessions ended (`auth_session_epochs`).
9. **What a verified dealership gets:** the higher limits; posts live at once after the automatic checks; its dealership page shows the plain text "Verified dealer"; and, for a verified **claim** (card S26), the right to manage the scanner's cars of **that** dealership: mark sold and correct fields. Corrections are stored as overrides (`dealer_listing_overrides`), applied after every scan, so the nightly scan does not undo them; "sold" holds until the car leaves the dealer's own site. An account can never edit, hide or delete scanner cars of a dealership it has not verified (a test per route).
   **The scanner must never delete or overwrite a user-posted car.** Files that change (card S20, wave 1, because the scanner can already take today's portal rows): `backend/scanner/manifest.py` (leave `dealer-portal-*` out of the roster), `backend/scanner/upsert/ownership.py`, `backend/scanner/upsert/guard.py` and the SQL backstop in `backend/scanner/upsert/sql.py` (a scanned dealer never takes a VIN whose owner is a user, whatever the window, rank or activity; the conflict is logged to `vin_owner_conflicts` and the user's car is held for review), `backend/db/repositories/cars_repo.py` (the dummy-VIN delete skips user rows), `backend/scripts/resolve_photo_attribution.py` and `backend/scripts/apply_attribution_moves.py` (skip user rows). The reconcile and retire paths (`backend/scanner/inventory_reconcile.py`, `backend/scanner/pipeline/reconcile.py`, `backend/attribution/disown.py`) are scoped by the scanned `dealer_id` and are safe once the roster excludes user ids; a test proves it.

### 23.5 Anti-bot and anti-spam layers

| Layer | What it stops | What it costs us | What is left |
|---|---|---|---|
| **Identity cost:** phone code, mobile only (Lookup line type), one account per phone, device and address limits, the 30-a-day text cap | mass accounts, throwaway sellers | about 7 cents per new number. The day allows at most about 15 new phone accounts (new-number pool, shoppers and sellers together) plus 3 dealer or seller verification codes. At a cap everyone sees "Sign-in by text is busy right now. Please try again after midnight Eastern time." | someone who buys real mobile numbers; each gets only the low new-seller limits, but about 5 bought numbers also deny new sign-ups for that day (section 5); the answer is the `cap_hit` email and raising the limit |
| **Turnstile** on sign-up, seller start and every publish | scripts | free; one extra tap now and then | paid human solvers |
| **Rate limits** per account, device, address and /24 or /48, halved for 24 hours after a refusal | bursts | none | a patient attacker; the ladder caps keep it slow |
| **Pre-publish checks:** VIN check digit; NHTSA decode agrees with make, model and year; VIN not live elsewhere on the site or from the scanner, and not on a scanned dealer in the last 60 days; same photo (perceptual hash) or same text under another account; price far under the market (under 60 percent of the median of at least 5 similar active cars, or under 1000 dollars for a car 15 years old or newer with no comparables); no links, phone numbers or emails in free text; scam phrases ("gift card", "wire", "Zelle only", "shipping agent", "deposit before viewing", "sold as is overseas", "escrow") | bait prices, hijacked VINs, stolen photos, phishing, scams | the NHTSA decode call per new VIN (free, cached) | new wording the lists miss; reports catch it |
| **First listing reviewed by the owner**; the second and later go live at once; the established step as in 23.2 (account 7 days old, 2 listings approved or live without a hold, no upheld report in 30 days) | the first-post spammer | about 1 minute per new seller (23.8) | a seller who behaves once then turns: caps, reports and re-checks on every edit |
| **Reports with auto-hide that cannot be used for sabotage** | scams that pass the checks | reviewing hidden listings | coordinated reporters from many accounts and networks |
| **Expiry** after 30 days, renew button, "Mark sold" | stale and abandoned listings | none | none |
| **Suspend and ban** by phone hash and device hash (and license for dealers) | returning offenders | none | new numbers and devices, at the identity cost again |
| **Contact path** hides phone and email (below) | scraping sellers' numbers, spam to sellers | a small message table and email notice | sellers who share their own number in a reply |
| **Kill switch** `marketplace.posts_paused` | a live attack | none | posts wait until it is off |
| **Daily owner digest and spike alert** | slow abuse no one notices | one email a day | none |

Reports: any visitor can press "Report this listing" (Turnstile when signed out; reasons: scam, wrong car or price, offensive, sold, other). Each report carries a weight: established accounts 1; accounts newer than 7 days 0.5, at most one per /24 per listing; signed-out visitors 0 for auto-hide (their report still reaches the queue); accounts tied to a verified dealership or a seller of a similar car near the same ZIP 0 for auto-hide (their report still reaches the queue, so competitors cannot hide each other); a reporter whose reports were found unfounded gets 0 for 30 days. A listing is hidden pending review when the weights reach 3 from at least 3 different /24s with at least 2 established reporters (5 for verified dealerships and for sellers older than 30 days with no upheld report). Scanner cars are never auto-hidden (they mirror the dealer's own site); their reports go to the data-quality list. A reporter makes at most 5 reports a day.

Contact path: today there is no contact form; the car page links only to the dealer's own site and shows no phone or email (`frontend/templates/car.html:177-179,1352,1412-1416`). For user-posted cars a signed-in, phone-verified buyer writes through a form; the message is stored and shown on the seller's `/inventory` Messages list (newest first, plain rows); an email notice with a link goes only to a **verified** email (section 4.3, card S7 (c)); placeholder emails are never mailed and no text message is ever sent for marketplace notices (the same rule for review results, holds, expiry and the dealership re-verify notice); neither side sees the other's phone or email unless they share it; links and numbers are stripped from first messages; a buyer starts at most 5 new conversations a day (20 if established); signed-out visitors and scrapers see no contact details at all.

Digest and alert: a nightly email to `OWNER_ALERT_EMAIL`: new sellers, dealership requests, holds by reason, reports, bans, posts. A spike alert when posts in the last hour exceed both 10 and three times the hourly average of the last 7 days: one email, and new-seller posts pause automatically for an hour (established and verified sellers go on).

### 23.6 Where user listings live, and how only live ones reach shoppers

- **Decision: keep `dealer_vehicles` (Postgres, `migrations/V012__dealer_portal.sql`) as each seller's own record (draft, held, live, hidden, expired, sold) and copy only live listings into `cars`** through the existing `portal_sync.py` path, as today. Reason: search (`search/where.py:141-144`) and the grid builder (`grid_cards_repo.py:94`) already show only `listing_active = 1` and `marked_for_review = 0` rows, and the car page (`B/routes/cars_pages.py`, `include_inactive=False`) only `listing_active = 1` rows (it does not check `marked_for_review`, so a held listing must be retired, not only flagged), and `cars.vin` is unique (`V001:1566`), which blocks duplicate VINs by itself. A separate public table would mean changing every reader. Any state other than live sets `listing_active = 0` (the existing retire path); going live sets it to 1. A sold or expired user row no longer blocks the VIN: the scanner may take a user row with `listing_active = 0` that has been retired for more than 7 days (card S20).
- `dealer_vehicles` gains no columns: state lives in a new `seller_listing_state` table keyed by its id (section 23.10), so the SQLite sidecar fallback of `dealer_portal_db.py` is not touched.
- `cars` has no column that says "posted by a user"; the `dealer-portal-` prefix of `dealer_id` stays the marker (one helper, `is_dealer_portal_dealer_id`), and every scanner path of 23.4 step 9 checks it.
- **The ZIP grid:** portal rows carry no `dealership_registry_id` or `dealer_url`, so `cards_near` cannot place them. The seller enters a ZIP code at posting; the copy writes `zip_code`, and the grid builder gains a branch that places user rows by the ZIP centre (verified dealerships by their claimed `dealership_registry_id`). Private sellers' exact address is never stored or shown.
- **Photos: our own hosting, Cloudflare R2** (S3-compatible, no charge for downloads; to my knowledge 10 GB stored is free and then about 1.5 cents per GB a month: check the current price). Estimate: 1000 listings times 12 photos times 300 KB is about 3.6 GB, inside the free part. Alternative: a Railway volume (simple, but tied to one service, billed per GB, and no CDN). **Owner-gated** (account, bucket, keys as variables; it may cost money later). Until then, posting stays off.
- **Photo rules:** JPEG, PNG or WebP checked from the file's first bytes, at most 5 MB each and 12 per request; Pillow decompression-bomb limit; every photo re-encoded to JPEG or WebP at most 1600 pixels wide, all metadata (including GPS location) removed; a 64-bit perceptual hash stored (difference hash with Pillow, no new library). **Image work never stalls the single web worker:** the request only checks size and the first bytes and stores the original in a private staging area; resizing, metadata removal and hashing run in a background worker thread, one photo at a time, and a listing cannot go live until its photos are processed. Originals are deleted after processing.
- **Terms:** card S14 gets a seller section (seller duties, prohibited listings, takedown, photo copyright and the licence the seller gives us to show photos, curbstoning not allowed). Owner-gated; a lawyer should read it; this is not legal advice.

### 23.7 Dealer sign-in

The password sign-in stays for dealers. It already has per-address throttling and the per-account lock (above); card S30 moves the per-account failure count into Postgres so a restart does not reset it, uses the 14-day absolute session (`SESSION_DAYS_STAFF`, card S10), shows one generic error, and offers the S12 authenticator to dealer accounts (optional; the owner decided dealers stay password-only, so it is not required). Existing dealer accounts are grandfathered; scanner-owned dealer rows are never touched. **Recommendation: dealers keep password sign-in, not phone codes.** A phone code costs 6 cents per sign-in and would draw on the 30-a-day text cap, and dealership staff often share a computer; their phone was already proved at verification. This changes nothing already decided, so it is not a question for the owner.

### 23.8 Owner's time and the waiting seller

- **Private seller on a phone:** sign in by phone (1 minute), "Sell your car", type the VIN (fields filled from the decode), mileage, price, ZIP, a short description, 3 to 12 photos: about 5 to 8 minutes. "Sell your car" asks once for an optional email: "Add an email so we can tell you when a buyer writes or your listing changes." (verified by link as in card S7 (c)); without one, `/inventory` says "You have no email on file, so check this page for buyer messages, review results and expiry dates." The first listing then shows "Waiting for review. Most listings are checked within one day. You can still edit it." with the reason, never a code; after 48 hours it says "Review is taking longer than usual." Later listings go live within a minute (after photo processing). A held listing says why in one plain sentence and what to do.
- **Owner:** first listings about 1 minute each; held listings about 1 minute; reports about 1 minute; dealerships about 3 minutes. At 5 new sellers and 1 dealership a day: about 10 minutes a day. The text cap does **not** limit this queue: people who already signed in by phone, and grandfathered accounts, become sellers without a new text. A settings row `marketplace.first_listings_per_day` (default 10) bounds it; past it, "Sell your car" saves the draft and says "New sellers are full for today. Your draft is saved; you can submit it tomorrow." So the owner's worst day is about 10 first listings plus held listings, reports and dealership reviews: about 20 to 30 minutes.

### 23.9 Abuse table

| Attack | Defence | What is left |
|---|---|---|
| Bait prices | price under market held; re-checked on every edit of a live listing; first listing reviewed | a believable price on a car that does not exist: reports, photos hash, VIN check |
| Hijacking scanned VINs | a VIN live from the scanner or on a scanned dealer in 60 days is held; only the verified dealership of that dealer may manage it; the scanner never takes a live user car (23.4 step 9) | front-running: a seller posts a dealer's brand-new VIN before the nightly scan finds it. When the scan later finds the VIN on that dealer's own site, the conflict in `vin_owner_conflicts` hides the user listing pending review (card S20) |
| Spam or phishing links in listings | no links, numbers or emails in free text; scam phrases held; contact through the site only | images with text in them: reports |
| Password guessing on dealer accounts | per-address throttle, per-account lock in Postgres, optional authenticator, generic error | slow guessing of weak passwords; minimum length already enforced |
| Text pumping through seller and dealer verification | Turnstile, mobile-only Lookup, reserved pool of 3 a day with at most 1 per account, phone, device or request, voice codes only after a failed public-number match | at most 3 texts (about 20 cents) a day; three attackers can use up a day's verification codes (section 5) |
| Mass account creation | one account per phone, the 30-a-day text cap, device and address limits, Turnstile | at most about 15 new phone accounts a day across the site |
| Spammer with 50 phone numbers and a script | Turnstile on every publish; 1 post a day and 2 active per new seller; first listing reviewed; device, address and /24 limits; duplicate photo and text checks across accounts; spike alert and auto-pause | 50 numbers take at least 4 days of the new-number pool (50 over 15 a day), one reviewed post each, then at most one unreviewed post a day each (2 active) once the first is approved; the first-listing queue limit (23.8) also slows it |
| Curbstoner | volume and dealer-text signals, the 60-day scanned-dealer VIN signal, cross-account photo and device matches, hold plus "verify your dealership" | a careful curbstoner with few cars; reports |
| Fake dealer claiming a real dealership's page | a claim needs control of the registry row's own domain (email code or token on that domain), the registry's phone, a license check by a human; a phone or address match with another domain is never a claim; a claim of an already-claimed dealership never auto-approved | a person who controls the dealer's website or mailbox (insider): revoke on report |
| Photo abuse (stolen, offensive, malware, location leaks) | type checked from bytes, re-encoded, metadata removed, size and count limits, perceptual hash across accounts, reports | offensive images until reported (first listings are seen by the owner) |
| False reports against a competitor | trust-weighted reports, dealer-linked reporters do not count for auto-hide, one per /24, unfounded reporters muted | a large coordinated group: the owner reviews hidden listings |
| Scraping sellers | no phone or email shown; contact needs a phone-verified account; per-account message limits | listing content itself is public by design |

### 23.10 Data, settings and migration

New tables (Postgres only, no foreign key to `users`): `seller_profiles` (user id, tier, phone hash, ZIP, approved and held counts, established_at, banned), `seller_listing_state` (`dealer_vehicles` id, state, hold reasons, expires_at, reviewed_by, reviewed_at), `dealership_verifications` (user id, matched `dealerships.id` or none, name, address, main phone, website and domain, state, license, contact, each check's result, state `started|submitted|approved|rejected|revoked|lapsed`, verified_at, expires_at; unique (state, license) for approved rows), `dealer_listing_overrides` (`cars.vin`, dealer id, field, value, set_by, set_at), `listing_reports`, `listing_messages`, `seller_bans` (kind `phone|device|license`, hashed value, reason, until), `photo_hashes` (listing id, perceptual hash), `marketplace_audit` (who, what, which listing or dealership, when; no message text). **They do not fit in S5 (V031) and keep it reviewable**, so card S21 claims the next free number when it is built, by rule 5 of `TASKS.md`.

Settings rows (row, then environment, then default; admin page C5): every number of the ladder in 23.2, `marketplace.first_listings_per_day` (10), `marketplace.posts_paused`, `seller.signup_paused`, `dealer.approval_mode` (`manual`), report weights and thresholds, the spike rule. Environment only: `MARKETPLACE_POSTING_ENABLED` (default off), `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`, `R2_PUBLIC_BASE_URL` (names only).

### 23.11 What was cut, and why

- Scraping state license sites: refused (fragile, terms of use); a human checks.
- Automatic image moderation (nudity, text in images): not now; first listings are reviewed and reports cover the rest.
- Escrow, payments or deposits: not offered; the Terms say we never handle money between buyer and seller.

### 23.12 Questions for the owner (default in brackets)

1. You review each new private seller's first listing? (yes)
2. A human (you) checks each dealer license number on the state's lookup page? (yes)
3. Photos on Cloudflare R2 (free at first, may cost a little later), not a Railway volume? (yes, R2)
4. Auto-approval stays off for dealerships until there are more than about 10 requests a week? (yes)
5. Switch on Twilio Verify voice codes for dealer business phones that do not match public data (about 6 cents each, from the 3-a-day pool)? (no at first: the admin calls the public number instead)
6. Promise sellers "most listings are checked within one day"? (yes)
