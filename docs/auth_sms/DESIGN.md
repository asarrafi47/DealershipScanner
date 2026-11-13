# Phone-number login with text-message codes: design

Status: proposal, nothing built. Written 2026-11-13 against `dev` 1.5.24. Read `SURVEY.md` first for what exists; this file cites it as "survey". Work cards are in `TASKS.md`. Paths are relative to the repo root; `B/` is `backend/`.

The idea in one paragraph: a shopper types a phone number, we ask a text-message provider to send a 6-digit code, the shopper types the code, and the site signs them in with the same session it uses today. A number that is new creates an account; a number we know signs in. Existing email-and-password, Google and Apple sign-in keep working. Everything sits behind `AUTH_PHONE_LOGIN_ENABLED`, default off.

## 1. Decisions I need from the owner (short list; full list at the end)

1. Why was app 2FA removed in 2026? No document says (survey 3.7). It affects how much weight we put on texts.
2. Should phone login also be allowed to open dealer and admin accounts? **Proposed: no.** Texts are the weakest login there is (SIM swap); admins and dealer staff keep password (and I propose adding an authenticator app for admins, section 12).
3. Phone-only accounts have no email. OK to keep a made-up internal address and ask for a real email later? **Proposed: yes** (section 4).
4. Which provider? Open; this design keeps the provider to one small adapter (section 3).

## 2. What changes and what does not

Does not change: `users.id`, sessions (a signed 14-day cookie, survey 4), CSRF (survey 4), the access and plan checks (`B/billing/access.py`), Stripe mapping (keyed on user id), the iOS cookie model. Phone login ends by calling the same `finalize_app_session(user_id)` (`B/auth/session.py:67`) that password, Google and Apple call.

Changes: new files under `backend/auth/phone/`, two or three new Postgres tables, a few routes and templates, new environment variables, new CSRF list entries, and a few "has no email" guards listed in section 4.

## 3. Provider interface (the only part that knows a vendor)

Package `backend/auth/phone/` (new, standard library plus the vendor's HTTP; no vendor SDK is required, the adapter can use `requests`, which the app already ships).

```python
# backend/auth/phone/provider.py  (sketch, not final)
@dataclass(frozen=True)
class StartResult:
    ok: bool
    provider_ref: str | None      # vendor's id for this verification; may be None
    retry_after_s: int | None     # vendor throttled us
    reason: str                   # short code, never text from the vendor body

@dataclass(frozen=True)
class CheckResult:
    approved: bool
    reason: str                   # "approved" | "wrong_code" | "expired" | "too_many" | "unknown"

class PhoneVerifyProvider(Protocol):
    name: str
    generates_code: bool          # True: vendor makes and checks the code (Twilio Verify style)
    def start(self, e164: str, *, locale: str, code: str | None) -> StartResult: ...
    def check(self, e164: str, *, code: str, provider_ref: str | None) -> CheckResult: ...
```

Two honest modes, because vendors differ:
- **Vendor holds the code** (`generates_code = True`, e.g. a verification service). We send no code and store none; our challenge row keeps only counters, expiry and the session binding. "Store hashed" does not apply because we never see the code.
- **We hold the code** (`generates_code = False`: the fake provider, or a plain "send this text" service). `AuthPhoneService` makes the code with `secrets.randbelow`, stores `HMAC-SHA256(AUTH_PHONE_PEPPER, challenge_id + ":" + code)` only, passes the plain code to `start()` once for the message body, and compares in constant time.

Implementations:
- `FakeProvider` (default in tests and development): approves a fixed code (`000000`) in memory, keeps a list of "sent" messages for tests, can be told to fail or be slow. Used whenever `AUTH_PHONE_PROVIDER=fake`; **refused when the app runs in production** (same pattern as `production_security`, `B/utils/production_security.py`).
- `TwilioVerifyProvider` (the leading candidate, `backend/auth/phone/twilio_verify.py`, last card): two HTTPS calls (start, check) with a 5-second timeout, no retries on check, one retry on start for a network error only. Variables: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_VERIFY_SERVICE_SID`. Nothing else in the repo names Twilio. Swapping vendors means writing one more file with the same two methods and changing `AUTH_PHONE_PROVIDER`.
- `AuthPhoneService` (`service.py`) owns everything else (limits, challenge rows, session binding, events) and takes a provider in its constructor, so tests inject the fake.

Vendor error text is never shown to the shopper or logged; only our short `reason` code.

## 4. Data model and migration

### 4.1 Where it lives

New tables go in **inventory Postgres, never SQLite.** `backend/tests/test_no_sqlite.py` freezes SQLite use with a shrink-only allowlist (`CHANGELOG.md` N37.0), and `migrations/README.md` forbids runtime DDL: a new table is a new migration. The same pattern is already used for `saved_cars`, `saved_searches`, `user_hidden_dealers`, `user_search_history`: a `user_id BIGINT NOT NULL` with **no foreign key**, because `users` is still in SQLite (`V018:12-15`, `V021:10-13`). When users move to Postgres (`docs/USERS_PG_CUTOVER.md`; ids are preserved with `OVERRIDING SYSTEM VALUE`, `V013` header), a later migration adds the foreign key as `NOT VALID` and validates it after a dangling-row check, (the pattern the migration registry plans for catalog constraints).

We do **not** add a phone column to `users`: it would need a SQLite ALTER (touching the frozen allowlist and the copier) and V013's twin, and it would couple phone numbers to the file that is about to disappear.

### 4.2 Tables (Postgres)

```sql
-- one verified number per account, one account per number
CREATE TABLE auth_phone_identities (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,                 -- users.id; no FK until the users cutover
    phone_e164    TEXT   NOT NULL,                 -- +14155550123
    country       TEXT   NOT NULL,                 -- 'US'
    verified_at   TIMESTAMPTZ NOT NULL,
    last_login_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT auth_phone_e164_shape CHECK (phone_e164 ~ '^\+[1-9][0-9]{6,14}$')
);
CREATE UNIQUE INDEX auth_phone_identities_phone_key ON auth_phone_identities (phone_e164);
CREATE UNIQUE INDEX auth_phone_identities_user_key  ON auth_phone_identities (user_id);   -- one number per user (first version)

-- one row per code request; no code, no raw number
CREATE TABLE auth_phone_challenges (
    id            BIGSERIAL PRIMARY KEY,
    public_id     TEXT NOT NULL UNIQUE,            -- random, kept in the session
    phone_hash    TEXT NOT NULL,                   -- HMAC(AUTH_PHONE_PEPPER, e164)
    phone_e164    TEXT NOT NULL,                   -- needed to call check(); cleared on finish
    purpose       TEXT NOT NULL,                   -- 'login' | 'link' | 'step_up'
    user_id       BIGINT,                          -- set for 'link' and 'step_up'
    session_nonce TEXT NOT NULL,                   -- must equal the value in the session cookie
    code_hash     TEXT,                            -- only when we hold the code
    provider      TEXT NOT NULL,
    provider_ref  TEXT,
    attempts      INT  NOT NULL DEFAULT 0,
    sends         INT  NOT NULL DEFAULT 1,
    status        TEXT NOT NULL DEFAULT 'open',    -- open | approved | failed | expired | cancelled
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ NOT NULL,
    last_send_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ
);

-- one row per text sent: drives the per-phone, per-IP, per-device and daily-spend limits
CREATE TABLE auth_phone_sends (
    id          BIGSERIAL PRIMARY KEY,
    at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    phone_hash  TEXT NOT NULL,
    ip_hash     TEXT NOT NULL,
    device_hash TEXT,
    country     TEXT NOT NULL,
    est_cost_cents NUMERIC(8,3) NOT NULL DEFAULT 0,
    outcome     TEXT NOT NULL                      -- 'sent' | 'provider_error' | 'blocked_*'
);
CREATE INDEX auth_phone_sends_at ON auth_phone_sends (at);
CREATE INDEX auth_phone_sends_phone ON auth_phone_sends (phone_hash, at);
CREATE INDEX auth_phone_sends_ip ON auth_phone_sends (ip_hash, at);

-- one row per number that ever started a trial, kept as a hash only (section 16)
CREATE TABLE auth_phone_trial_marks (
    phone_hash TEXT PRIMARY KEY,
    first_trial_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Numbering ("ask what the right place is"): `dev` stops at `V029__admin_users.sql`. The registry in `docs/NEXT_PHASES_PLAN_2026_10.md:504-522` already reserves V030-V038 for other units (`users.auth_epoch` V030 and "drop dead MFA columns from users" V031 are in N30, not yet on dev) and lists "next" after V038. `docs/ALL_POSTGRES_PLAN_2026_10.md` is named in the brief but is not in the repository. **Proposed:** the migration takes the first free number at the time the card is done (today that would be V030, but it will probably be V039 or later by then); the card must run `ls migrations` and re-read the registry, never reuse a number, and never edit an applied file (`migrations/README.md`). It is additive, inert while the flag is off, and should be applied to dev and prod by the owner before the web deploy that ships the code (web boot applies pending migrations, `scripts/docker-entrypoint-web.sh:21`, `MIGRATE_ON_BOOT`). Owner question 5.

### 4.3 Accounts that have no email

`users.email` and `users.username` are NOT NULL and unique (survey 9). Until users reach Postgres and the constraint can be relaxed, a phone-only account gets:
- username `user-` plus 8 random lowercase letters and digits (no `@`, so it passes `B/auth/registration_validation.py:18-29`);
- email `p-<24 random hex>@phone.invalid` (the `.invalid` top-level name is reserved and can never receive mail); a pure function `is_placeholder_email()` recognises it;
- a random bcrypt password nobody knows, as Google users get (`accounts.py:269,334`).

Everything that uses the email must treat a placeholder as "no email". The cards list each place: password reset and verification skip it (`B/auth/password_reset.py:226-227`); `session["user_email"]` is stored empty (`B/auth/session.py:67-121`) so checkout sends no `customer_email` (`B/billing/stripe_billing.py:89,128`; `checkout_service.py:65`); the admin hub shows "no email"; `/api/auth/me` returns an empty email; `delete_user_by_email` needs a by-id path (`accounts.py:447-518`, `admin.py:481-487`); the display name used in reviews (`dealer_reviews.py:33-43`) is the generated username, never the number. The profile page lets the person set a real email (this clears nothing else) and a display name.

## 5. Phone numbers: format, allow-list

- Normalise with a small function (digits only, `+1` for 10-digit US input, E.164 output), backed by the `phonenumbers` library (add to `requirements-web.txt`; pure Python, no network) for validity and country. Reject anything not valid for an allowed country.
- Allow-list `AUTH_PHONE_COUNTRIES`, default `US`. First version: United States numbers only. The `+1` plan also covers Canada and Caribbean countries, which are the classic text-fraud destinations; the library tells the country, so the allow-list works on the real country, not on the `+1` prefix.
- Reject: numbers the library calls not possible, premium-rate and shared-cost types, and (if the provider offers a line-type lookup at an acceptable cost) virtual and VoIP numbers for new sign-ups. The lookup is a switch (`AUTH_PHONE_BLOCK_VOIP`, default off) because it costs money and can block real users.

## 6. Code, expiry, attempts, resend

| Rule | Value (env override) |
|---|---|
| Code | 6 digits |
| Expires | 10 minutes after the last send (`AUTH_PHONE_CODE_TTL_S`, 600) |
| Wrong guesses | 5 per challenge, then the challenge is dead and a new one is needed (`AUTH_PHONE_MAX_ATTEMPTS`) |
| Resend | not before 30 seconds, then 60 seconds, at most 3 sends per challenge |
| One use | an approved challenge is closed in the same database transaction that reads it (`UPDATE ... WHERE status='open' RETURNING`), so a code cannot be used twice or by two requests |
| Wrong-guess spread | 10 wrong codes per phone per hour across all challenges, so opening new challenges does not reset the guess budget |

Guess odds: 5 guesses on 1,000,000 codes is 1 in 200,000 per challenge, and 10 per hour per number caps a long attack.

## 7. Rate limits and money limits

The existing limiter (`B/utils/ip_rate_limit.py`) is in process memory, per worker and lost on restart (survey 5), so it cannot be the only guard for something that costs money. Phone limits count rows in `auth_phone_sends` (durable) and also use the in-memory limiter as a cheap first gate.

| Limit | Default | Counts |
|---|---|---|
| Per phone | 5 sends per hour, 10 per day | `phone_hash` |
| Per IP (IPv6 by /64) | 10 sends per hour, 30 per day | `ip_hash` (HMAC of `B/utils/client_ip.py` result) |
| Per device | 5 sends per hour | long-lived signed cookie `sc_dev` (random 128 bits), issued before the first request to the phone page; a request without one gets one and counts as new-device |
| Per session | 3 challenges per 15 minutes | session nonce |
| Global per day | `AUTH_PHONE_DAILY_SEND_CAP` (default 500) and `AUTH_PHONE_DAILY_SPEND_CAP_CENTS` (default 1500) using `AUTH_PHONE_EST_COST_CENTS` per country | sum over today (UTC) |
| Global alarm | at 50% and 80% of the daily cap write a warning event (section 18) | |
| Kill switch | `AUTH_PHONE_LOGIN_ENABLED=0` stops sends instantly; `AUTH_PHONE_SEND_PAUSED=1` keeps the page but answers "try later" | |

When a cap is hit the page says "Text sign-in is busy. Use email sign-in or try again later" and the user can still use password, Google or Apple. Caps are blocking, not just alerting: a pumping attack must not be able to spend more than one day's cap.

Optional later, owner decision: a bot check (for example Cloudflare Turnstile) before the first send from a new device when the IP is over half its limit.

## 8. Session binding, and what a good code does

1. `POST` start: create a challenge with a random `public_id`, store in the cookie session `phone_challenge = public_id` and `phone_nonce = <random>`; the nonce is also in the row (`session_nonce`).
2. `POST` verify: the row must be `open`, unexpired, belong to this nonce, and the code must check out. A code entered in another browser or after the cookie is lost fails (the cookie is signed and `SameSite=Lax`, `B/main.py:79-81`).
3. On approval, in one transaction: close the challenge; for `login` find the identity (or create the user, section 10); `session.clear()` (the same fixation defence as every login, survey 4); `finalize_app_session(user_id)`; set `session["auth_method"]="phone"` and `session["auth_at"]=<epoch>`; update `last_login_at`; redirect with the existing `post_login_redirect()` (`B/auth/session.py:53`).
4. Password, Google and Apple login also set `auth_method` and `auth_at` (one line each), so section 9 can ask "how recently did this person prove themselves".

Because the session is still a signed cookie with no revocation list (survey 4), a lost phone does not sign anyone out. The planned `users.auth_epoch` (registry V030) fixes that for every method; phone login should adopt it when it lands.

## 9. Step-up before sensitive actions

Sensitive means: start a checkout or open the billing portal (`B/billing/routes.py:73,182,317`; `B/routes/account.py:202`), change or remove the phone number, change email or password (`B/routes/account.py:99-165`), delete the account.

Rule: allowed only if `auth_at` is within 10 minutes; otherwise the page asks for a fresh proof, and for a phone-verified account that is a new code sent to the number on file (`purpose='step_up'`). If the account also has a password, the password is accepted instead. This is a function `require_recent_auth(max_age_s=600)` returning a redirect to the step-up page and back, applied as an inline call (the codebase has no login decorator, survey 8) at the six places above. The billing checkout GET routes should become POST with CSRF in the same card (`PAY_AND_STRIPE_AUDIT.md`, M6).

This does not stop a SIM-swap attacker who holds the number now (section 14); it stops a stolen laptop or an old session.

## 10. Who can sign in with a phone, and sign-up

One page, "login or sign up". After a correct code:
- **Number known** and the account is active and its role is `general_user` (or `dealer_staff` read as general): sign in.
- **Number known but the account is `is_active=0`**: same generic failure as a bad code ("That code did not work").
- **Number known and the role is admin or a dealership role:** refused with the generic message and a log event `phone_login_blocked_role`. These accounts keep password sign-in (section 12).
- **Number new:** create a `general_user` (generated username, placeholder email, random password) through a new `save_phone_user()` that follows `save_oauth_user` (`B/db/users_db/accounts.py:260`), insert the identity, sign in. Creation order across two stores: create the user (SQLite), then insert the identity (Postgres); if the insert hits the unique index (two tabs raced), delete the user just created and sign in as the winner. The consent line (section 17) must be on the page, and the time of acceptance goes in the event log (no number).

Unknown and known numbers get the same screens, the same messages and similar timing, so the page cannot be used to find out who has an account (section 14).

## 11. Existing email, Google and Apple users

- **Add a number:** signed-in user opens `/account/phone`, types a number, passes step-up, gets a code (`purpose='link'`), types it; the identity row is inserted. If the number already belongs to another account: say "That number could not be added" with no detail, and log `phone_link_conflict` (hash only). Never merge automatically; a merge is an admin action (card).
- **Do not link by guessing:** no matching on email text, and Google or Apple sign-in never attaches a phone.
- A user with both can use either. Removing the number is allowed only if another way to sign in exists (a real email with a known password, or Google or Apple linked); phone-only users cannot remove it, but can change it (verify old, then new).
- **Prompt phone-only users to add a real email** after sign-in (one plain line, dismissible); it enables receipts, recovery and the "your number changed" notice.
- The login-form "Email or username" label stays; a plain "Use a text message code instead" link sits under it.

## 12. Admin and dealer accounts

Survey 0 and 3.6: **today admins have a password only; there is no authenticator.** `/dev` operators and `/admin` site admins are separate paths (survey 7) and are unaffected by phone login because phone login refuses admin and dealer roles. What I propose, as its own later card, not part of phone login:
- bring back an authenticator-app second step for `admin_users` and for app users with role admin (enrolment, recovery codes, the TOTP seed encrypted at the application level before it is stored; V013 header and SEC-088 explain why plaintext seeds in Postgres are unacceptable);
- keep a break-glass password path (`ADMIN_USERNAME`, `ADMIN_PASSWORD`, V029).
Reason: a text code on an admin account would be weaker than a password.

## 13. Lost phone and recovery

- Account has a real email with a known password, or Google, or Apple: sign in that way, then `/account/phone` (step-up with that factor) to replace the number (verify the new number; the old identity row is replaced in one transaction). If the account has a real email, send a "your sign-in number was changed" notice to it (`send_transactional_email`, survey 5) with no number in the body beyond the last two digits.
- Account has a real email but no password: use "forgot password" (`PASSWORD_RESET_ENABLED` must be on) to set one, then replace the number.
- **Phone-only with nothing else:** cannot be recovered by the site on its own, on purpose. Support path, recorded here so it is not improvised: the owner asks for proof only the true owner has (the date and last four digits of a card from a Stripe receipt, a saved-search name, a saved car), then an admin uses a new "replace phone number" action in the admin user form (`frontend/templates/admin/user_form.html`), which writes an audit event with the admin's name. No self-service, no support-by-email override.
- Number recycled by the carrier: the new holder can request a code and sign in. Mitigations: paid and phone-only accounts get the "add an email" prompt more firmly; a number-change notice to the email on file; step-up for billing (section 9). Residual risk is accepted and written down.

## 14. Abuse cases and the control for each

| Case | What the attacker does | Controls |
|---|---|---|
| **SMS pumping** (paying fraudsters make us send texts to numbers they earn from) | script the start form with premium or foreign numbers | country allow-list (US); line-type reject; per phone, IP, device and session limits; global daily send and spend caps that block; request must carry the signed session and a CSRF token (survey 4); optional bot check; the verification-to-send ratio is the alarm (many sends, few approvals); the vendor's built-in fraud guard if it has one (check at selection time); kill switch |
| **Finding who has an account** | enter numbers and watch the reply | start always answers the same words and the same screen; a challenge is created and a text is attempted even for a known number; wrong-code messages are the same everywhere; sign-up and login are one flow; no "number already used" text (section 11 returns the same words) |
| **Guessing codes** | try many codes | 6 digits, 5 attempts per challenge, 10 wrong per phone per hour, expiry 10 minutes, session binding, single use |
| **SIM swap / number takeover** | move the victim's number to a new SIM | cannot be fully stopped by texts. Controls: step-up and a fresh code before billing or number changes (limits damage to what a plain login can already do); notice to the email on file for number changes; admin and dealer roles excluded; encourage a real email; stage 2 option: a 24-hour wait with notices before a changed number takes effect on a paid account; a passkey option later |
| **Free-trial farming with many numbers** | one person, many numbers, many trials | one account per number (unique index); US-only; virtual-number block (switch); trial marks per number hash kept after account deletion (section 16); the trial needs a verified number and no earlier mark for it; global per-IP and per-device send caps slow bulk number use; the pay-side fix of one trial per customer in Stripe (`PAY_AND_STRIPE_AUDIT.md` H4) is still needed because numbers are cheap |
| **Racing two tabs** | create two accounts for one number | unique index; compensating delete (section 10) |
| **Code in the log or in a screenshot** | | codes are never logged or stored in the vendor-holds-code mode; hashed otherwise |
| **Cookie theft** | | unchanged from today; step-up for money; `auth_epoch` later |

## 15. Screens and endpoints

UI rule: **no cards, boxes or pill badges** (owner decision `docs/remediation/OWNER_DECISIONS_LOG.md` D-N19: "one plain text line ..., with no card or box"; `docs/VISUAL_REVIEW_2026_09_28.md` rows 31-32 push login toward one flat primary button and underline inputs). Use plain headings, a single column, lines of text, underline fields (`.rgs-field`) and one flat button, inline forms, text links for secondary actions, inline red text for errors. No avatar chips, no rounded status badges. Errors and states are sentences.

| Method and path | Purpose | CSRF |
|---|---|---|
| `GET /login/phone` | enter phone (one underline field, the consent line below it, one flat button "Text me a code") | n/a |
| `POST /login/phone` | start, then redirect to the code page | form token |
| `GET /login/phone/code` | enter code. Plain lines: "We sent a 6-digit code to number ending 23." Field, button "Sign in". Text links: "Send again" (disabled with a seconds counter until allowed), "Use a different number" | n/a |
| `POST /login/phone/code` | verify | form token |
| `POST /login/phone/resend` | resend (cool-down enforced server side) | form token |
| `POST /login/phone/change` | cancel the challenge, back to the number field | form token |
| `POST /logout` | unchanged (`B/auth/pages.py:237`) | existing |
| `GET/POST /account/phone`, `POST /account/phone/verify`, `POST /account/phone/remove` | add, change, remove a number (step-up) | form token |
| `POST /api/auth/phone/start`, `/verify`, `/resend` | JSON twins for the iOS app and scripts, cookie session plus `X-CSRF-Token` like `api_auth_login` | header token |

All routes return 404 when the flag is off. New POST routes must be added to the CSRF lists in `B/web/security.py:49-98` (the test `test_csrf_coverage.py` fails otherwise); JSON routes go in `B/mobile/contract.py:27-47` and `ios/docs/API_CONTRACT.md`. Rate-limit and error responses use fixed words (survey 5 style). `login_page` ignores `next` today (survey 8); phone login should not invent a new redirect mechanism.

The iOS app needs its own change (login field and register flow, `ios/.../AuthHeaderBar.swift`), kept out of the first rollout: the web phone page works inside the app's web view.

## 16. Privacy

- **Stored:** the number in E.164 on the identity row (needed to text it and to show "ending 23"); only hashes in sends, challenges' `phone_hash`, trial marks and logs; the challenge's raw number is cleared when the challenge finishes.
- **Never stored or logged:** the code (vendor mode) or its plain text; the message body; vendor response text.
- **Retention:** challenges 7 days after finish; sends 30 days (spend and abuse review); trial marks 12 months or as long as trials exist, hash only; identities until the account is deleted. A scheduled cleanup (card) deletes by age. The vendor keeps its own records under its own policy; review at selection.
- **Deletion:** deleting an account deletes the identity row and the challenges; the trial mark (hash) stays, disclosed in the privacy text. The `delete_user_by_email` path must also call a by-id variant for phone-only accounts.
- **Consent line next to the phone field (draft, owner and counsel to approve):** "By continuing, you agree to receive a one-time text message from Sarrafi Collection at this number to sign you in. Message and data rates may apply. We do not use your number for marketing. Reply STOP to opt out. Terms and Privacy." The site has no terms or privacy page today (no template in `frontend/templates`), and carriers and vendors require them for text programs; a card creates them (plain pages, no boxes).
- Admin user list shows the last four digits only; full numbers are visible to nobody in the admin hub.

## 17. How the pay structure can change later without redoing login

- Plans stay in `B/billing/catalog.py`; login knows only "who is this" (`users.id`). Prices, trial lengths and features can change in the catalog and in Stripe without touching phone login.
- Phone verification can gate a trial: `trial_eligible(user_id)` = a verified identity exists and no row in `auth_phone_trial_marks` for its hash. The plan checkout (`checkout_service.py:247-248`) passes `trial_period_days` only when eligible, and writes the mark when the subscription is created. This also covers the existing audit finding that every checkout currently gets a trial.
- If the owner later wants "phone required for any paid plan" or "no phone needed for free": a one-line rule in the same function, not a login redesign.
- Stripe receipts: phone-only buyers type an email on Stripe's page; nothing changes in the webhooks (`metadata.user_id`).

## 18. Observability without personal data

- One logger name, `auth_phone`, JSON-ish key=value lines: `event` (`start`, `blocked_country`, `blocked_limit`, `provider_error`, `check_ok`, `check_wrong`, `check_expired`, `signup`, `login`, `link`, `step_up`, `cap_50`, `cap_80`, `cap_hit`), `provider`, `country`, `purpose`, `reason`, `latency_ms`, `phone_hash` truncated to 8 characters for correlation, `ip_hash` truncated. Never the number, the code or a name.
- A nightly job (same style as the nightly card job, 1.5.24) writes yesterday's totals to the log: sends, approvals, approval ratio, spend estimate, blocked by reason, top countries.
- A read-only operator page in `/dev` (plain table text) shows today's sends against the caps.
- Alarms: owner variables at the vendor (usage alert) as the second line of defence, since the cap lives in our code; a stuck cap at 80% is the signal to look.

## 19. Feature flag and rollout

- `AUTH_PHONE_LOGIN_ENABLED` default off. With it off: routes 404, links hidden, no table is read, no provider is built.
- `AUTH_PHONE_ALLOWED_NUMBERS`: optional comma list of E.164 numbers (staging and the owner's own) honoured when set, so the first production day can run for two or three real phones only. Details in `TASKS.md` rollout card.
- Stages: fake provider in tests and development; a real provider with the test or sandbox setup and the allow-list; widen to all US numbers with a low daily cap; raise caps after a clean week. Roll back by setting the flag to 0; existing sessions stay valid; phone accounts stay and can still use "forgot" only if they add an email (so warn before rollback).

## 20. Environment variables (names only)

`AUTH_PHONE_LOGIN_ENABLED`, `AUTH_PHONE_PROVIDER` (`fake` or the adapter name), `AUTH_PHONE_PEPPER` (random, 32+ characters), `AUTH_PHONE_COUNTRIES`, `AUTH_PHONE_CODE_TTL_S`, `AUTH_PHONE_MAX_ATTEMPTS`, `AUTH_PHONE_DAILY_SEND_CAP`, `AUTH_PHONE_DAILY_SPEND_CAP_CENTS`, `AUTH_PHONE_EST_COST_CENTS`, `AUTH_PHONE_SEND_PAUSED`, `AUTH_PHONE_BLOCK_VOIP`, `AUTH_PHONE_ALLOWED_NUMBERS`; for the leading vendor: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_VERIFY_SERVICE_SID`. Production refuses to start with the flag on and a fake provider or a missing pepper.

## 21. Risks I want you to know

- Texts are a weak second factor and a decent first one for low-value accounts; SIM swap is real. That is why money actions get a fresh code and admin accounts are excluded.
- Cost: every text costs money, including attacker texts; the caps, not the vendor, protect the bill.
- The two-store write (SQLite user, Postgres identity) is not atomic; the compensating delete handles the race, and the users cutover removes the problem.
- Nothing in the rate limiter survives a restart, so phone limits use the database.
- The 14-day signed cookie cannot be revoked until `auth_epoch` ships.

## 22. Questions for the owner

1. Why was app 2FA removed in 2026? Is there a reason that should shape this?
2. Phone-only accounts get a made-up internal email: OK?
3. Keep admin and dealer accounts password-only (plus a new authenticator step I propose), outside phone login?
4. US numbers only at first?
5. Migration number: take the first free number when the card is built (today V030, likely later)? `docs/ALL_POSTGRES_PLAN_2026_10.md` is not in the repository; where is it?
6. Daily limits: 500 texts and $15 a day to start: right size?
7. Do you want a bot check on first send from a new device?
8. Who writes the terms and privacy pages and approves the consent line?
9. Should one number be allowed on only one account forever (proposed), and may a phone-only person ever be unlinked by support?
10. Provider: still open; the vendor adapter is the last code card, so the choice can wait.
