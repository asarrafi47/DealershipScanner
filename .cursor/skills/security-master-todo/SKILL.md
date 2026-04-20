---
name: security-master-todo
description: >-
  Enforces DealershipScanner security workflow using docs/SECURITY_MASTER_TODO.md.
  Use when the user or task involves security, auth, secrets, XSS, CSRF, sessions,
  admin/dev routes, rate limits, LLM/chat APIs, or subprocess safety. Instructs
  the agent to read the master todo, add SEC- items if missing, implement changes,
  run Validation steps, and update Status, Last verified, and Changelog in the
  same delivery as code.
---

# Security master todo skill

## Instructions

1. Open `docs/SECURITY_MASTER_TODO.md` at the start of the session (or when security is in scope).
2. Map the user request to existing **SEC-xxx** rows. If none fit, **add** a row in the correct phase with **Status** `Not started` or `In progress`, **Outcome**, and **Validation** before coding.
3. Implement the smallest change that satisfies **Outcome**.
4. Execute **Validation** (tests, manual steps, or greps as described in the row). Record failures until they pass.
5. Update the document in the **same change** as code:
   - **Status** → `Done` (or `Blocked` with reason in Changelog)
   - **Last verified** → today’s date (per user_info or actual run date)
   - **Changelog** → one line describing what changed
6. If work is deferred, leave **Status** as `Not started` and add a Changelog note explaining deferral—do not claim completion.

## ID convention

- New items: **SEC-0xx** for secrets/config, **SEC-01x** sessions/auth, **SEC-02x** input validation, **SEC-03x** frontend, **SEC-04x** APIs/LLM, **SEC-05x** dev/ops—or extend consistently and document in Changelog.

## Examples

- User: “add CSRF to login” → find or add **SEC-011**, implement tokens, validate with replay test, mark Done + Changelog.
- User: “rate limit chat” → **SEC-041**, add limiter, validate flood → 429, update doc.

## Anti-patterns

- Merging security code without updating `docs/SECURITY_MASTER_TODO.md`.
- Marking **Done** without running the row’s **Validation**.
- Splitting doc updates to a “later” PR without user approval.
