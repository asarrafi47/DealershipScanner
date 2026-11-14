# Phone-number login with text-message codes: design

Status: proposal, nothing built. First written 2026-11-13, **revised the same day after the owner's decisions** (listed in section 1). Written against `dev` 1.5.25. **Design review 2026-11-13:** a four-lens attack changed sections 4, 5, 6, 7, 8, 9, 10, 13, 15, 16, 17 and 20; see the Decisions log in `TASKS.md`. Read `SURVEY.md` first for what exists; this file cites it as "survey". Cards are in `TASKS.md`; chat limits are in `CHAT_LIMITS.md`. Paths relative to the repo root; `B/` is `backend/`.

In one paragraph: a shopper types a phone number, ticks the consent box, passes a Cloudflare Turnstile check, and Twilio Verify texts a 6-digit code. The shopper types the code and the site signs them in with the same session cookie it uses today. A new number creates an account; a known number signs in. **For shoppers this is the only way in.** Admin and dealer accounts keep password sign-in.

## 1. Decisions made (owner, 2026-11-13) and what I still need

Decided:
- The whole site is free; Stripe and plan code stay in the repo but dormant, billing stays off (`PAY_AND_STRIPE_AUDIT.md`). Chat is the only thing with a per-use cost (`CHAT_LIMITS.md`).
- Shoppers sign in by phone number plus text code only, from day one. Email-and-password sign-in for shoppers is removed.
- Admin and dealer-portal accounts stay as they are; admins get an authenticator step (card S12); dealers stay password-only for now.
- Provider: Twilio Verify (runner-up Telnyx Verify), 20 dollar prepaid balance, no auto-recharge, no callbacks (webhooks) at launch. Twilio Lookup line-type check blocks VoIP numbers. Cloudflare Turnstile is checked on the server for every send request.
- US numbers only. Support may replace the number on an account after proof. Made-up internal emails for phone accounts are fine.
- Defaults and texts as given (sections 5, 6, 14 and 16).

Still open, with my recommendation (also repeated at the end):
- **Google and Apple sign-in for shoppers:** I recommend turning them **off** for shoppers at launch (section 11). Please confirm.
- **Code lifetime:** you gave "expires in 5 minutes" as the default but the text message says "expires in 10 minutes". I recommend 5 minutes in both (section 5).
- **Old accounts:** I recommend the "claim your old account" email link, with a start-fresh fallback (section 12).
- Questions 1 and 5 from the first round you will answer separately.

## 2. What changes and what does not

Does not change: `users.id`, the signed session cookie, CSRF, the access helpers, the dealer portal, `/dev`, the Stripe code (dormant). Phone login ends by calling the same `finalize_app_session(user_id)` (`B/auth/session.py:67`) that every login calls.

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
    purpose TEXT NOT NULL,                     -- login | link | claim | step_up | change
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
    outcome TEXT NOT NULL                      -- pending | sent | unavailable | provider_error | blocked_turnstile | blocked_voip | blocked_country | blocked_limit | blocked_cap
);
-- indexes on (at), (phone_hash, at), (ip_hash, at), (device_hash, at)

CREATE TABLE auth_phone_wrong_tries (          -- wrong-code counter per phone that a new code does NOT reset
    phone_hash TEXT PRIMARY KEY,
    tries INT NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ
);
```

Chat limit tables (`chat_usage_buckets`, `app_settings`) are in `CHAT_LIMITS.md` section 4 and ship in their own migration. The earlier "trial marks" table is dropped: with no paid plans there are no trials.

**Migration number:** `dev` ends at `V029`. Numbers are claimed in the registry (`docs/NEXT_PHASES_PLAN_2026_10.md:514-522`) in the same PR as the migration, not guessed from `ls`: integ files already queued (`users_auth_epoch`, `users_drop_dead_mfa_columns` and later) collide with a plain "next free" rule. Allocation: C1 = V030 (built in PR 15), S5 = V031, S12 = V032; the integ rows move up by three unless the owner decides the other order. Session revocation uses a new table `auth_session_epochs` in S5, and `sessions_valid_after` is dropped from the identities table. Never edit an applied file. `docs/ALL_POSTGRES_PLAN_2026_10.md` is still not in the repository (question 5, which you will answer).

### 4.3 Accounts without an email

`users.email` and `users.username` are NOT NULL and unique (survey 9), so a phone account gets a username `user-<8 random letters and digits>` and an internal email `p-<24 random hex>@phone.invalid` (the `.invalid` name can never receive mail; you agreed this is fine), plus a random unusable password. `is_placeholder_email()` recognises it, and everything that uses email must treat it as "no email": no mail is sent to it, checkout and Stripe get no `customer_email`, the session stores an empty email, `/api/auth/me` returns an empty email, the admin hub shows "no email", and the number is never shown. Deleting an account must work by id (`delete_user_by_email` needs an `@`, `accounts.py:447-518`). A profile line lets people add a real email and a display name; comments and reviews show the display name, never the number.

## 5. Rules for codes and limits (owner's defaults)

| Rule | Value |
|---|---|
| Code | 6 digits, single use |
| Lifetime | **5 minutes**, enforced by our challenge row (`AUTH_PHONE_CODE_TTL_S`, 300). The login text must say the same number (see section 1) |
| Wrong tries | 5 per challenge, reserved **before** the vendor call. The 5-try lock is per (phone, browser session or device); a phone-wide lock only after about 15 to 20 wrong tries a day across all challenges, so a stranger cannot lock the real holder out. A new code does not reset the count (`auth_phone_wrong_tries`) |
| Resend | wait 60 seconds |
| Per phone | at most 3 sends in 15 minutes and 5 a day |
| Per IP | 5 an hour and 20 a day |
| Per device | 5 a day |
| Whole site | **Launch defaults for the first month: `SMS_DAILY_SEND_CAP` = 80 and `SMS_DAILY_SPEND_CAP_CENTS` = 500** (long-run targets 300 and 1500, raised only after a week of nightly totals and never in the same step as removing the allow-list). Alert at half. A prepaid guard stops sends at 90 percent of `sms.prepaid_cents` (default 2000) spent since `sms.topup_at`, counted from the app's own rows. Day boundary: midnight in `CHAT_RESET_TZ`. Pools: a number that already has an identity may use 100 percent, new numbers at most 50 percent (`SMS_NEW_NUMBER_SHARE_PCT`), resends 20 percent; per /24 (IPv4) or /48 (IPv6) at most 40 sends a day |
| Countries | `SMS_ALLOWED_COUNTRIES` = `US` |
| Line type | VoIP numbers (fixed and non-fixed) are refused for sign-up and sign-in via the lookup; landlines cannot receive texts and are refused too |
| Fresh code before a number change | the account must have proved itself within the last 10 minutes |
| Cookie | shoppers 30 days, admins 12 hours (section 9) |

Each login costs about 6 cents for Verify plus about 1 cent for Lookup on a new number. The 20 dollar balance buys roughly 300 sign-ups in all, so a 15-dollar daily cap would let one bad day spend most of it; that is why the launch defaults are 5 dollars and 80 sends. Twilio's own low-balance emails (10 and 5 dollars) are an owner step.

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

Refusals are recorded as send rows with a `blocked_*` outcome so they appear in the counts. Every refusal looks the same to the person ("We could not send a code to that number. Check it and try again, or try later."), except the cap and busy message.

## 7. Codes: checking

`POST` verify: the challenge must be open, unexpired, match this session's nonce, and the wrong-tries lock must not apply; then ask Twilio to check the code. The attempt is reserved before the vendor call (`UPDATE ... SET attempts=attempts+1 WHERE status='open' AND attempts<5 AND expires_at>now() RETURNING`) and refunded only on network error or throttle. Vendor statuses map: approved; `max_attempts_reached` is too-many; `expired`, `deleted`, `failed` are expired; `pending` and `canceled` are wrong or unknown. A wrong code adds one to `auth_phone_wrong_tries`. An approved code closes the challenge in the same transaction (`UPDATE ... WHERE status='open' RETURNING`) so it works once. A code typed in another browser fails.

## 8. Device cookie and the real client address

- **Device:** a rate-limit key only, never a trust signal (it never skips a code or marks a browser trusted). A random 128-bit signed cookie `sc_dev`, set before the first phone request and kept 400 days. Its hash is the "device" in limits. Clearing cookies gives a new device, which is why IP, phone and the whole-site cap exist too.
- **IP address:** you asked to read `CF-Connecting-IP`. The site is already built to sit behind Cloudflare and then Railway's edge (`B/utils/client_ip.py:1-30`, `TRUSTED_PROXY_HOPS`, `TRUSTED_PROXY_CIDRS=cloudflare`). A header anyone can send is only safe if the request really came through Cloudflare, and the file's own notes say a request sent straight to the Railway address could forge it until that address is removed and the origin locked (`client_ip.py:30-33`). **Design:** a new function `phone_client_ip()` returns `CF-Connecting-IP` only when the existing trusted-proxy check says the request arrived through Cloudflare's ranges; otherwise it falls back to `client_ip()`; it also requires a shared-secret header added by a Cloudflare Transform Rule (`CF_ORIGIN_SECRET`), because any sender using Cloudflare's own servers can set the header. The same function serves chat limits. Owner question: is `sarraficars.com` proxied by Cloudflare (orange cloud) and has the `*.up.railway.app` address been removed?
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
| Dealer (`dealership_owner`, `dealership_admin`, `dealership_member`) | password through the existing dealer login (`/dealer/login`, `B/dealer/routes.py:74-111`), unchanged |
| Site admin (app role `admin`) | password on a **staff sign-in page** `/staff/login` (the old form, same code as `B/auth/pages.py:81-114`), plus the authenticator step from card S12 |
| Operators (`/dev`, `admin_users`) | password, plus the authenticator step (S12) |

What happens to the old routes:
- `GET/POST /login` becomes the phone page. A small plain link, "Staff sign-in", leads to `/staff/login`.
- `/staff/login` and `POST /api/auth/login` accept **only** staff and dealer roles. A shopper account that submits a password gets the same generic failure as any wrong password (no hint that the account exists or uses a phone), and the page header says "Shoppers sign in with a phone number."
- `/register` and `POST /api/auth/register` no longer create shopper accounts: they redirect to the phone page (JSON: 410 `use_phone_sign_in`). Dealer registration is unchanged.
- `/forgot-password`, `/reset-password`, email verification and `/resend-verification` stay available only for staff and dealer accounts and are hidden from shoppers.
- Phone login **refuses** every account that fails `is_phone_eligible` (stored role `general_user`, or an empty role only with no dealer id, registry id and org id; active; not an env-admin) with the same generic failure and a log event; the claim flow (section 12) and number change use the same test. `/dealer/login` and `/api/auth/login` accept only `dealership_*` roles; `/staff/login` is the only password door for `admin` and then needs the S12 step; Google and Apple never open `/admin` or `/dev` without the authenticator step; `/dealer/register` is closed by default (`DEALER_SELF_REGISTER_ENABLED`).
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
| **SMS pumping** (fraudsters trigger texts to numbers they profit from) | Turnstile on every send, failing closed; US only; VoIP and landline block via lookup; per phone, IP and device limits; whole-site 300 sends and 15 dollars a day, both blocking; Twilio's own fraud protection on; alert at 150 sends; a rising ratio of sends to approved codes is the alarm; kill switch |
| **Finding who has an account** | the page and its words are identical for known and unknown numbers; sign-up and login are one flow; the claim flow always says "if that email has an account"; Turnstile and limits make bulk probing costly |
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

Provider sentences (card S6 reasons): unavailable: "Sign-in by text is not available right now. Please try again later." Opted out: "Texts to this number are blocked. Reply START to the number, or use another number." Known numbers at a cap get the busy sentence; unknown numbers get the neutral failure.

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
- Logger `auth_phone`, key=value lines: `event`, `provider`, `country`, `purpose`, `reason`, `latency_ms`, and a 6-character hash prefix only on `blocked_*` and `check_locked` events. Events: start, blocked_turnstile, blocked_voip, blocked_country, blocked_limit, blocked_cap, provider_error, check_ok, check_wrong, check_locked, check_expired, signup, login, claim, change, cap_50pct (the 150-send alert), cap_hit, webhook_bad_signature.
- A nightly job (`backend/scripts/auth_phone_nightly.py`) writes yesterday's totals (sends, approvals, ratio, cost estimate, blocked by reason) and exits non-zero at the alert level so a scheduled routine can notify the owner.
- A read-only operator page in `/dev` shows today's sends against the 300 and 15-dollar caps.

## 18. Feature flag and rollout

- `AUTH_PHONE_LOGIN_ENABLED`, default off. Off: phone routes 404, `/login` still shows the old form (so turning the flag on is the switch from email to phone). Because shopper password sign-in is removed from day one, **the flag flip is the launch**; before it, shoppers sign in the old way.
- `AUTH_PHONE_ALLOWED_NUMBERS` (optional list) restricts phone sign-in to named numbers for the first test days (owner and friends) while the old form is still the default for everyone else; with it set, only listed numbers can use the phone page and the old form stays visible.
- Rollout is card S18. Rollback: flag off puts the old form back for shoppers whose accounts still have a password; accounts created by phone have no usable password and **cannot sign in until the flag is back on**; warn before rolling back after launch.

## 19. Pay structure and this design

With everything free (free means no plan gating; every feature still needs a phone sign-in in production), there are no plans, trials or Stripe flows to protect: `users.id` is the only key. The plan columns, `B/billing/` and the checkout pages stay in the repository, dormant, with `BILLING_STRIPE_ENABLED` off (a test keeps it off and keeps the paid pages hidden). Phone verification is still useful later if paid plans or trials return: the verified, unique number is a good "one trial per person" key. See `PAY_AND_STRIPE_AUDIT.md` (kept as the revival checklist).

## 20. Environment variables (names only)

Names you listed: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN` (or `TWILIO_API_KEY_SID` and `TWILIO_API_KEY_SECRET`), `TWILIO_VERIFY_SERVICE_SID`, `TURNSTILE_SITE_KEY`, `TURNSTILE_SECRET_KEY`, `SMS_DAILY_SEND_CAP` (start 300), `SMS_ALLOWED_COUNTRIES` (`US`), `PHONE_HASH_PEPPER` (random, 32 or more characters).

Names I add (all with safe defaults, so you only set them if you want to change a number): `AUTH_PHONE_LOGIN_ENABLED`, `AUTH_PHONE_PROVIDER` (`fake` or `twilio_verify`), `AUTH_PHONE_ALLOWED_NUMBERS`, `AUTH_PHONE_CODE_TTL_S` (300), `AUTH_PHONE_MAX_WRONG_TRIES` (5), `AUTH_PHONE_LOCK_MINUTES` (15), `AUTH_PHONE_RESEND_WAIT_S` (60), `SMS_DAILY_SPEND_CAP_CENTS` (1500), `SMS_ALERT_SEND_THRESHOLD` (150), `SMS_EST_COST_CENTS` (6), `AUTH_OAUTH_SHOPPERS_ENABLED` (off), `SESSION_DAYS_SHOPPER` (30), `SESSION_HOURS_ADMIN` (12), `SESSION_DAYS_STAFF` (14), `AUTH_PHONE_DORMANT_DAYS` (180), `SMS_EST_LOOKUP_CENTS` (1), `SMS_NEW_NUMBER_SHARE_PCT` (50), `TWILIO_WEBHOOK_ENABLED` (off), `LEGAL_PAGES_APPROVED_VERSION`, `CF_ORIGIN_SECRET`, `OWNER_ALERT_EMAIL`, `DEALER_SELF_REGISTER_ENABLED` (off), plus the chat names in `CHAT_LIMITS.md` section 5. The per-phone, per-IP and per-device send limits are settings rows with the defaults in section 5 (same mechanism as chat). Production refuses to start with the flag on and the fake provider, a missing pepper, or a missing Turnstile or Twilio value.

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
