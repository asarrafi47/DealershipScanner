# Full audit — May 2026

**Date:** 2026-05-22 UTC  
**Scope:** Registration UX, frontend/backend bugs, security review  
**Status:** Fixes applied 2026-05-22 (see SEC-071–074 in `docs/SECURITY_MASTER_TODO.md`)

---

## 1. Registration (regular / free accounts)

### Verified end-to-end (live app, port 5001)

Headless browser test via `/register` → dashboard:

| Step | Result |
|------|--------|
| Register page loads | Pass |
| POST with Free plan | Pass |
| Redirect to `/dashboard` | Pass |
| Dashboard welcome UI | Pass |
| User row in `users.db` | Pass |
| `role = general_user` | Pass |
| `is_premium = 0` | Pass |

**Demo account (still valid):**

- Email: `demo_free@example.com`
- Username: `demo_free`
- Password: `Demo-Free-Test-9!`

Create new accounts at `/register` with any unused email; duplicate email/username shows the expected error.

### Registration code path (healthy)

- CSRF on POST (`register.html` hidden field + `before_request`)
- Rate limit 10/min per IP (`main.py`)
- Min password 8 chars (`registration_validation.py`)
- bcrypt hash (`password_hash.py`)
- Post-register redirect → dashboard for free plan (`_post_login_redirect`)
- Session cleared before login (`session.clear()`)

---

## 2. Functional bugs & issues

### High priority

| ID | Issue | Location |
|----|--------|----------|
| B1 | **Email uniqueness is case-sensitive at insert, case-insensitive at login** — `Test@x.com` and `test@x.com` can both register; login matches either | `users_db.py` UNIQUE on raw email vs `lower(email)` lookup |
| B2 | **Save car button swallows errors** — failed save gives no user feedback | `car.html:494` `.catch(() => {})` |
| B3 | **Shared chat daily cap is per listing, not per user** — one user can exhaust quota for everyone on that car | `main.py:1219-1225` |

### Medium priority

| ID | Issue | Location |
|----|--------|----------|
| B4 | **`data-skip-ensure="1"` still POSTs `packages/ensure`** — misleading name; extra API calls | `car_packages.js:244-253` |
| B5 | **Billing OFF: guests can hit chat/packages APIs with CSRF only** while UI hides those sections | `main.py` `_require_premium_feature`, `car.html:281` |
| B6 | **Dead premium upsell blocks** in `car.html` (inner hints never render because outer gate hides section for free users) | `car.html:284-285`, `449-450` |
| B7 | **Dealer website URLs not sanitized** before `href` | `car.html:275`, `dealerships_db.py` |

### Low priority

| ID | Issue | Location |
|----|--------|----------|
| B8 | Premium users may still see “Upgrade to Premium” on `/premium` | `premium.html`, E2E note |
| B9 | External dealer gallery images often abort in headless runs (CDN) | Not an app bug |
| B10 | No max length on username/email at registration | `registration_validation.py` |

---

## 3. Security audit

### Strengths (verified)

- CSRF on login/register POST + API mutations (`csrf.py`, `main.py`)
- bcrypt passwords; legacy plaintext upgraded on login
- Session cookies: HttpOnly, SameSite=Lax, Secure in production
- Rate limits on login (30/min), register (10/min), smart search, car chat (layered)
- Parameterized SQL in inventory search; dynamic SQL uses column allowlists only
- No `\|safe` in public templates; listings/car grids use `escapeHtml` / `\|tojson`
- Stripe premium grant requires verified paid Checkout Session (`SEC-066`)
- Premium APIs return `403 premium_required` when billing on (`SEC-067`)
- Dev scanner subprocess: list args, no `shell=True`, URL validation (`SEC-068`)
- Window sticker fetch limited to OEM candidate URLs (`SEC-069`)

### High severity

| ID | Finding | Location |
|----|---------|----------|
| S1 | **Admin self-registration** — email in `APP_ADMIN_EMAILS` or username in `APP_ADMIN_USERNAMES` gets admin on register | `main.py:518-524` |
| S2 | **CDJR sticker PNG preview bypass** — free users can GET `/car/<id>/window-sticker-preview.png` for Stellantis/CDJR while UI hides sticker | `main.py:298-302` |
| S3 | **Billing accidentally off in production** opens LLM chat + sticker fetch to anyone with CSRF | `main.py:337-341` |
| S4 | **`CAR_CHAT_WEB_RESEARCH_PUBLIC=1`** allows Playwright for anonymous users | `car_chat_policy.py` |

### Medium severity

| ID | Finding |
|----|---------|
| S5 | Legacy plaintext password verification still supported (`verify_or_legacy`) |
| S6 | Dev DB seeds admin with default `ChangeMe2026!` when `ALLOW_DEFAULT_APP_USER` |
| S7 | Unvalidated dealer `href` URLs (stored XSS / phishing if DB poisoned) |
| S8 | App MFA removed; password-only for all accounts |
| S9 | Shared chat daily limit (DoS) — same as B3 |

### Low severity

| ID | Finding |
|----|---------|
| S10 | `listings-geo` API has no login requirement (session preference only) |
| S11 | CSP off by default in non-production (expected) |
| S12 | Registration lacks max field length |

### Tests run

```bash
python -m pytest backend/tests/test_billing_gate.py backend/tests/test_app_security_basics.py -q
# 15 passed

python scripts/e2e_user_audit.py
# 31 passed (isolated server, billing on)
```

---

## 4. Recommended fix order

1. **B1 / SEC-071** — Normalize email to lowercase on register; unique index on `lower(email)`
2. **S2 / SEC-072** — Align CDJR preview PNG with premium policy (or document as intentional free tier)
3. **B2** — Save button error feedback
4. **B3 / S9** — Per-user chat daily cap
5. **S3 / SEC-073** — Require login for chat/packages in production even when billing off
6. **S1** — Admin bootstrap: invite-only or block self-register for admin emails
7. **B7 / S7** — `normalize_optional_url` on dealer website links

---

## 5. Car detail gating (current behavior)

| User | Billing ON | Billing OFF |
|------|------------|-------------|
| Guest | Specs + gallery only | Same |
| Free logged-in | Specs + gallery + Save; no Packages/chat | Full packages/chat UI |
| Premium | Full experience | Full experience |

This matches product intent from recent changes.
