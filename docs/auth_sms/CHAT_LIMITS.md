# AI chat limits: design

Written 2026-11-13 after the owner decided the whole site is free (no paid plans for now). The AI chat is now the only thing that costs money each time someone uses it, so it needs real limits. Read with `DESIGN.md` (phone login) and `TASKS.md` (cards C1 to C6). Paths relative to the repo root; `B/` is `backend/`.

## 1. What exists today

Chat features and where each one is gated (all use the in-process limiter `B/utils/ip_rate_limit.py`, which is per web worker and forgets everything on restart; `SURVEY.md` section 5):

| Feature | Route | Code | Limits today |
|---|---|---|---|
| Car page chat | `POST /api/car/<id>/chat` | `B/routes/cars_pages.py` (limits `:585-620`, model call through `B/intelligence/ai/agent.py:313`) | per minute: global (off by default), per IP 48, per IP and car 24; per day: 40 per account (or per IP when signed out), 10 per account and car |
| Compare chat | `POST /api/compare/chat` | `B/routes/cars_pages.py:684-760` | per minute per IP; 40 a day per account in its own bucket |
| Sidebar chat and search helper | `POST /api/ai/chat` | `B/routes/ai_chat_bp.py:146-304` (one request can make two model calls: a search rewrite, then an answer) | same numbers, third set of buckets |
| Car narration | `GET /api/car/<id>/narrate` | `B/routes/ai_narrate_bp.py:74-86` | per IP per minute only |

The daily numbers come from `CAR_CHAT_MAX_PER_USER_DAILY` (default 40) and `CAR_CHAT_MAX_PER_LISTING_DAILY` (default 10) in `B/utils/car_chat_policy.py:44-76`. The smart search box does not call the model (`B/routes/listings_api.py:698`, "no Claude"). Web research inside the chat is allowed only for signed-in users (`car_chat_policy.py:18-36`).

Problems this design fixes: counters live in memory (a restart or a second worker resets them); nothing limits money, only message counts; three separate buckets mean one person can use 3 times the budget; a new account (or a signed-out visitor on a new IP) starts from zero; and there is no place to look at usage.

All model calls go through one function, `complete()` in `B/llm/client.py:422-515`, which returns the token counts (`usage`, `:502`). That is the one place to measure real spend.

## 2. The rules

1. **Chat needs a signed-in account.** Already true in production (`B/billing/access.py:334`). With phone login every shopper account has a verified phone number.
2. **One gate for all four routes** (a function, `chat_budget.guard(...)`, used the same way everywhere). The old in-memory daily buckets (`chat:daily:*`, `aichat:daily:*`, `cmp:daily:*`) are removed. The short per-minute in-memory limits stay as a burst guard.
3. **Limits are counted per account, per phone number, per device, per IP, and globally**, and **the strictest one wins**. Per phone number and per device are what stop someone from making new accounts to get a new allowance: a phone number is unique per account (`DESIGN.md` section 4) and its counter is kept by hash even after the account is deleted; the device is a long-lived cookie (`sc_dev`, `DESIGN.md` section 8).
4. **Counters are stored in Postgres**, one small row per subject and time bucket, never in memory.
5. **A money cap for the whole site, per day**, in dollars and in tokens. When it is reached the chat stops for everyone until the reset, and says so in plain words. Everything else on the site keeps working.
6. **Plain words when a limit is hit, with the time it resets.**
7. **Numbers are changeable without a release**: a small settings table in Postgres, then environment variables, then code defaults (section 5).
8. **A small admin page** shows usage (section 7).
9. A message counts when it is accepted. A model failure or a refusal that returns no answer gives the message back (decrement) so users are not charged for our errors.

## 3. Default numbers (proposed; the owner can change any of them later in the settings table)

| Limit | Default | Why |
|---|---|---|
| Per account | 10 messages an hour, 40 a day | 40 a day is today's number (`car_chat_policy.py:44-58`) |
| Per account and car | 10 a day | today's number |
| Per phone number | 40 a day | same as one account; survives account deletion and re-sign-up |
| Per device | 60 a day, across accounts | a family can share a device; a farm cannot |
| Per IP address | 100 a day | back-stop only; many homes and phones share one address |
| New accounts (under 24 hours old) | half of the account numbers | slows throw-away accounts |
| Site-wide per day | **$5 and 2,000,000 tokens** (whichever comes first), alert at 50% and 80% | a guess; I do not have this site's real cost per message |
| Web research inside a chat answer | counts as 3 messages | it is the expensive path (`B/utils/car_chat_policy.py:18-36`) |
| Admins | exempt from per-account numbers, not from the site-wide cap | |

About the dollar figure: I could not measure what one chat message costs here (no network, no production data). Card C2 records real token counts from the first day, and the admin page shows dollars per day and per message; then set the cap from real numbers. Until then the token cap works without any price list. Prices per million tokens are settings (`chat.price_in_micro_per_mtok`, `chat.price_out_micro_per_mtok`) that the owner or a session fills from the current Anthropic price page for the model in `ROLE_MODELS["chat"]` (`B/llm/client.py:57-80`); they are not hard-coded.

## 4. Data (new Postgres migration, next free number when built)

```sql
-- one row per (period, bucket, subject)
CREATE TABLE chat_usage_buckets (
    period        TEXT NOT NULL,            -- 'hour' | 'day'
    bucket_start  TIMESTAMPTZ NOT NULL,     -- start of the hour, or of the day in the reset time zone
    subject_kind  TEXT NOT NULL,            -- 'user' | 'phone' | 'device' | 'ip' | 'listing' | 'global'
    subject_key   TEXT NOT NULL,            -- user id, or an HMAC hash; 'all' for global; 'uid:car' for listing
    messages      INT    NOT NULL DEFAULT 0,
    tokens_in     BIGINT NOT NULL DEFAULT 0,
    tokens_out    BIGINT NOT NULL DEFAULT 0,
    cost_micro    BIGINT NOT NULL DEFAULT 0,  -- millionths of a dollar
    denied        INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (period, bucket_start, subject_kind, subject_key)
);

-- small editable settings (key / value text), used by chat and later by other features
CREATE TABLE app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by BIGINT
);
```

No message text and no model reply is stored. Phone, device and IP subjects use the same keyed hash as the login tables (`PHONE_HASH_PEPPER`). Rows older than 35 days are deleted by the nightly job; the totals page only needs recent days. Like the phone tables, `user_id` has no foreign key until users move to Postgres (`DESIGN.md` section 4.1).

## 5. Settings: changeable without a release

Order of precedence: row in `app_settings` > environment variable > code default. The code reads the table at most every 30 seconds per worker (a small cache), so a change takes effect within a minute. Keys and the environment names that give the same default:

| Setting key | Env name | Default |
|---|---|---|
| `chat.enabled` | `CHAT_ENABLED` | on |
| `chat.user_per_hour` | `CHAT_USER_PER_HOUR` | 10 |
| `chat.user_per_day` | `CAR_CHAT_MAX_PER_USER_DAILY` (existing name kept) | 40 |
| `chat.listing_per_day` | `CAR_CHAT_MAX_PER_LISTING_DAILY` (existing) | 10 |
| `chat.phone_per_day` | `CHAT_PHONE_PER_DAY` | 40 |
| `chat.device_per_day` | `CHAT_DEVICE_PER_DAY` | 60 |
| `chat.ip_per_day` | `CHAT_IP_PER_DAY` | 100 |
| `chat.new_account_factor_pct` | `CHAT_NEW_ACCOUNT_FACTOR_PCT` | 50 |
| `chat.budget_usd_per_day_cents` | `CHAT_BUDGET_CENTS_PER_DAY` | 500 |
| `chat.budget_tokens_per_day` | `CHAT_BUDGET_TOKENS_PER_DAY` | 2000000 |
| `chat.est_cost_micro_per_message` | `CHAT_EST_COST_MICRO` | set from real data after C2 |
| `chat.price_in_micro_per_mtok`, `chat.price_out_micro_per_mtok` | `CHAT_PRICE_IN_MICRO_PER_MTOK`, `CHAT_PRICE_OUT_MICRO_PER_MTOK` | from the price page |
| `chat.reset_tz` | `CHAT_RESET_TZ` | `America/New_York` |
| `chat.alert_pcts` | `CHAT_ALERT_PCTS` | `50,80` |

A zero means "no limit" for the per-subject numbers, as it does today; a zero budget means "chat off". Bad values (not a number, negative, absurdly large) are ignored with a log line and the next level is used. Every change through the admin page records who and when.

## 6. How a message is checked

1. Build the subjects: the account id, its phone hash, the device hash, the IP hash (the real client address, section 8 of `DESIGN.md`), and for car chat the (account, car) pair.
2. One short database transaction: for each subject add one to the hour and day rows (upsert with `messages = messages + weight`), check against the limits, and add the estimated cost and tokens to the global day row. If any limit would be exceeded, roll the whole thing back, add one to `denied` in its own small write, and answer with the denial.
3. Run the model. The `complete()` function reports tokens for each call made inside the guard (a small hook that only acts when a guard is active, `B/llm/client.py:495-515`), so a request that makes two calls records both.
4. At the end, true up: replace the estimate with the real tokens and cost; on a failure with no answer give the message back.
5. Because the estimate is reserved first, many requests at the same moment cannot all slip under the cap together. The worst overshoot is bounded by the estimate error, not by the number of parallel requests.

If Postgres is unreachable, the gate fails **closed** for chat (a calm "chat is not available right now") and open for everything else. A chat outage is cheaper than an open tap.

## 7. What the person sees

Plain sentences in the chat panel, no popups, no coloured badges. The reset time uses the reset time zone and says how long that is:

- Hour limit: "You have used your 10 questions for this hour. You can ask again at 3:00 PM Eastern, in about 25 minutes."
- Day limit: "You have used your 40 questions for today. They come back at 12:00 AM Eastern, in about 6 hours."
- Car limit: "You have asked 10 questions about this car today. You can ask about other cars, or come back tomorrow at 12:00 AM Eastern."
- Device or phone limit: the same words as the day limit; it never says which hidden counter was hit or mentions other accounts.
- Site budget: "The assistant has reached its budget for today and will be back at 12:00 AM Eastern. Searching and browsing still work."
- Paused: "The assistant is paused for now. Searching and browsing still work."

The JSON for the app has `error` (`user_chat_limit_reached`, `listing_chat_limit_reached`, `chat_budget_reached`, `chat_paused`; the first two exist today), `message`, and `resets_at` (UTC time). The 429 status stays. The chat panel prints `message` as text.

## 8. Admin page (small, plain)

`/admin/chat-usage`, site admins only (the same `_require_site_admin` helper as the other hubs, `SURVEY.md` section 8). Plain headings and tables, no cards:

- Today so far: messages, tokens in and out, estimated dollars, percent of the budget, whether the chat is on, paused or stopped by the budget, next reset.
- Last 14 days: one line per day (messages, tokens, dollars, how many people hit a limit).
- Top 20 accounts today by messages (account id and username, never a phone number or message text), and counts of denials by kind (account, phone, device, IP, listing, budget).
- Settings: the table in section 5 with the current value, its source (settings row, environment, default) and an edit form for the numbers; a button "pause chat now" (sets `chat.enabled` to off). Changes need the CSRF form token and are logged with the admin's id.

## 9. Alerts without personal data

A nightly job (same style as the existing nightly card job) writes yesterday's totals to the log and exits non-zero if the budget was reached or usage passed an alert percentage, so the owner's scheduled routine can send a notification. Inside the web process the guard logs one `chat_budget` line when a day first passes 50% and 80% (once per level per day). Logs contain counts, percentages and hashes cut to 8 characters, never message text, phone numbers or names.

## 10. Risks

- Estimates can be wrong at first; the cap is a safety net, not a bill. Set a spend limit at the model provider's console as the second line of defence (owner step).
- Limits by IP can punish shared networks; that is why IP is a back-stop with a higher number.
- A person with several real phone numbers still gets several allowances. The site-wide cap bounds the cost.
- Back-office model use (scanner enrichment, brochure reading, vision) is not part of this budget; it has its own controls (`B/vision/claude_rate_limit.py`).
