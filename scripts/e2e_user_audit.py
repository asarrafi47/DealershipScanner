#!/usr/bin/env python3
"""
Headless browser audit: guest, free, and premium user flows.

Usage:
  python scripts/e2e_user_audit.py [--base-url http://localhost:5099] [--no-start-server]

Writes docs/E2E_USER_AUDIT_REPORT.md with findings.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_PORT = 5099
PASSWORD = "E2eTest-Password-9!"
RAM_CAR_ID = 3809
BMW_CAR_ID = 778
TOYOTA_CAR_ID = 1


@dataclass
class Finding:
    severity: str  # bug | warn | improve | remove
    area: str
    message: str
    detail: str = ""


@dataclass
class AuditState:
    findings: list[Finding] = field(default_factory=list)
    passed: list[str] = field(default_factory=list)
    console_errors: list[str] = field(default_factory=list)
    failed_requests: list[str] = field(default_factory=list)

    def add(self, severity: str, area: str, message: str, detail: str = "") -> None:
        self.findings.append(Finding(severity, area, message, detail))

    def ok(self, message: str) -> None:
        self.passed.append(message)


def _wait_health(base: str, timeout: float = 45.0) -> bool:
    import urllib.request

    deadline = time.time() + timeout
    url = f"{base}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.4)
    return False


def _start_server(port: int, users_db: Path) -> subprocess.Popen[Any]:
    env = os.environ.copy()
    env["PORT"] = str(port)
    env["PUBLIC"] = "0"
    env["BILLING_STRIPE_ENABLED"] = "1"
    env["USERS_DB_PATH"] = str(users_db)
    env["MFA_DELIVERY_MODE"] = "log"
    env.pop("USERS_DB_ENCRYPTION_KEY", None)
    py = sys.executable
    return subprocess.Popen(
        [py, str(ROOT / "run.py")],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _grant_premium(email: str, users_db: Path) -> None:
    os.environ["USERS_DB_PATH"] = str(users_db)
    from backend.db.users_db import get_user_by_login, grant_user_premium, init_users_db

    init_users_db()
    u = get_user_by_login(email)
    if not u:
        raise RuntimeError(f"user not found: {email}")
    grant_user_premium(int(u["id"]))


def _register_and_login(page, base: str, username: str, email: str, plan: str = "free") -> None:
    page.goto(f"{base}/register", wait_until="networkidle")
    page.fill('input[name="username"]', username)
    page.fill('input[name="email"]', email)
    page.fill('input[name="password"]', PASSWORD)
    if plan == "premium":
        page.locator('label[for="plan-premium"]').click()
    page.locator("form.auth-form button[type='submit']").click()
    page.wait_for_load_state("networkidle")


def _login(page, base: str, login: str) -> None:
    page.goto(f"{base}/login", wait_until="networkidle")
    page.fill('input[name="login"]', login)
    page.fill('input[name="password"]', PASSWORD)
    page.locator("form.auth-form button[type='submit']").click()
    page.wait_for_load_state("networkidle")


def _csrf_from_page(page) -> str:
    meta = page.locator('meta[name="csrf-token"]')
    if meta.count():
        return meta.get_attribute("content") or ""
    hidden = page.locator('input[name="csrf_token"]')
    if hidden.count():
        return hidden.first.get_attribute("value") or ""
    return ""


def _attach_listeners(page, state: AuditState) -> None:
    def on_console(msg):
        if msg.type in ("error", "warning"):
            text = msg.text or ""
            if "favicon" in text.lower() or "404" in text and "favicon" in text.lower():
                return
            state.console_errors.append(f"[{msg.type}] {text}")

    def on_request_failed(req):
        if req.failure:
            state.failed_requests.append(f"{req.method} {req.url} — {req.failure}")

    page.on("console", on_console)
    page.on("requestfailed", on_request_failed)


def run_audit(base: str, state: AuditState) -> None:
    from playwright.sync_api import sync_playwright

    suffix = uuid.uuid4().hex[:8]
    free_email = f"e2e_free_{suffix}@example.com"
    prem_email = f"e2e_prem_{suffix}@example.com"
    free_user = f"e2e_free_{suffix}"
    prem_user = f"e2e_prem_{suffix}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()
        _attach_listeners(page, state)

        # --- Guest ---
        page.goto(f"{base}/", wait_until="networkidle")
        landing_html = page.content() or ""
        if re.search(r"\bAI\b", landing_html):
            state.add("bug", "copy", 'Landing page still contains "AI" wording')
        elif page.locator("text=Intelligent Listing Assistant").count():
            state.ok("Landing uses intelligent wording (no AI label)")
        else:
            state.add("warn", "copy", "Landing missing Intelligent Listing Assistant feature title")

        if "/dashboard" in page.url:
            state.add("warn", "guest", "Home redirects logged-out users to dashboard URL", page.url)
        elif "login" not in page.url.lower() and page.locator("text=Sign in").count() == 0:
            state.add("warn", "guest", "Landing may lack obvious sign-in", page.url)
        else:
            state.ok("Guest landing loads")

        page.goto(f"{base}/listings", wait_until="networkidle")
        if page.locator("#results-grid").count() == 0:
            state.add("bug", "listings", "Listings page missing #results-grid")
        else:
            state.ok("Guest listings page renders grid shell")

        page.goto(f"{base}/dashboard", wait_until="networkidle")
        if "login" not in page.url:
            state.add("bug", "auth", "Unauthenticated /dashboard should redirect to login", page.url)
        else:
            state.ok("Dashboard requires login")

        # --- Guest: bare minimum on car detail (no packages / chat) ---
        page.goto(f"{base}/car/{TOYOTA_CAR_ID}", wait_until="networkidle")
        if page.locator("#car-packages-section").count():
            state.add("bug", "guest", "Guest should not see Packages section on car detail")
        else:
            state.ok("Guest car detail hides Packages section")
        if page.locator("#car-chat-section").count():
            state.add("bug", "guest", "Guest should not see Ask about this car section")
        else:
            state.ok("Guest car detail hides car chat section")

        # --- Free account ---
        _register_and_login(page, base, free_user, free_email, plan="free")
        if "dashboard" not in page.url:
            state.add("bug", "auth", "Free user post-register should reach dashboard", page.url)
        else:
            state.ok("Free registration lands on dashboard")

        page.goto(f"{base}/dashboard", wait_until="networkidle")
        if page.locator('a[href="/inventory"]').count():
            state.add("bug", "nav", "Free user dashboard should not show My inventory link")
        else:
            state.ok("Free user dashboard hides dealer inventory nav")
        if page.locator("text=Premium").count() == 0:
            state.add("warn", "nav", "Free user dashboard missing Premium upsell in nav")
        else:
            state.ok("Free user dashboard shows Premium nav CTA")

        page.goto(f"{base}/premium", wait_until="networkidle")
        prem_html = page.content() or ""
        if re.search(r"\bAI\b", prem_html):
            state.add("bug", "copy", 'Premium page still contains "AI" wording')
        elif page.locator("text=Intelligent Car Chat").count():
            state.ok("Premium page uses intelligent wording (no AI label)")
        else:
            state.add("warn", "copy", "Premium page missing Intelligent Car Chat heading")

        page.goto(f"{base}/car/{TOYOTA_CAR_ID}", wait_until="networkidle")
        if page.locator("#car-packages-section").count():
            state.add("bug", "free-user", "Free user should not see Packages section when billing enabled")
        else:
            state.ok("Free user car detail hides Packages section")
        if page.locator("#car-chat-section").count():
            state.add("bug", "free-user", "Free user should not see car chat section when billing enabled")
        else:
            state.ok("Free user car detail hides car chat section")

        page.goto(f"{base}/listings", wait_until="networkidle")
        smart = page.locator("#smart-search-input")
        if smart.count():
            smart.fill("2022 ram 1500")
            smart.press("Enter")
            page.wait_for_timeout(2500)
            cards = page.locator(".car-card, .listing-card, #results-grid a[href*='/car/']")
            if cards.count() == 0:
                state.add("warn", "listings", "Smart search returned no visible cards (may be empty DB filter)")
            else:
                state.ok("Free user smart search returns results UI")
        else:
            state.add("bug", "listings", "Smart search input missing on listings")

        page.goto(f"{base}/car/{RAM_CAR_ID}", wait_until="networkidle")
        if page.locator("text=Engine").count() == 0:
            state.add("warn", "car-detail", "Ram detail missing Engine spec row", page.url)
        else:
            state.ok("Ram car detail shows Engine row")

        if page.locator("#car-window-sticker-wrap").count() > 0:
            state.add("bug", "window-sticker", "Free user should not see window sticker section when billing enabled", page.url)
        else:
            state.ok("Free user car detail hides window sticker block")

        page.goto(f"{base}/car/{BMW_CAR_ID}", wait_until="networkidle")
        if page.locator("#car-window-sticker-wrap").count() > 0:
            state.add("bug", "window-sticker", "Free user should not see window sticker on BMW", page.url)
        else:
            state.ok("Free user BMW detail hides window sticker block")

        page.goto(f"{base}/car/{TOYOTA_CAR_ID}", wait_until="networkidle")
        if page.locator("#car-chat-section").count() or page.locator("#car-chat-input").count():
            state.add("bug", "car-chat", "Free user should not see car chat section when billing enabled")
        else:
            state.ok("Free user car detail hides car chat section")

        if page.locator("text=/premium subscription/i").count():
            state.add("bug", "free-user", "Free user car page should not show premium upsell copy")
        else:
            state.ok("Free user car page has no premium upsell blocks")

        save_btn = page.locator("#save-btn")
        if save_btn.count():
            state.ok("Free user car detail shows Save button")
        else:
            state.add("warn", "car-detail", "Save button missing for logged-in free user")

        csrf = _csrf_from_page(page)
        if not csrf:
            state.add("bug", "csrf", "Missing CSRF token on car page for API calls")
        else:
            api_resp = page.request.post(
                f"{base}/api/car/{TOYOTA_CAR_ID}/chat",
                headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
                data=json.dumps({"message": "test"}),
            )
            if api_resp.status == 403:
                body = api_resp.json() if api_resp.headers.get("content-type", "").startswith("application/json") else {}
                if body.get("error") == "premium_required":
                    state.ok("Car chat API returns premium_required for free user")
                else:
                    state.add("warn", "car-chat", f"Car chat 403 but error={body.get('error')}")
            else:
                state.add("bug", "car-chat", f"Free user car chat API status {api_resp.status}")

        # Logout
        logout = page.locator('form[action*="logout"] button, form[action*="logout"] input[type="submit"]')
        if logout.count():
            logout.first.click()
            page.wait_for_load_state("networkidle")
            state.ok("Logout works")
        else:
            state.add("warn", "auth", "Logout control not found on car page")

        # --- Premium account ---
        users_db = Path(os.environ.get("USERS_DB_PATH", ROOT / "users.db"))
        _register_and_login(page, base, prem_user, prem_email, plan="free")
        try:
            _grant_premium(prem_email, users_db)
        except Exception as e:
            state.add("bug", "setup", f"Could not grant premium: {e}")
            browser.close()
            return

        page.context.clear_cookies()
        _login(page, base, prem_email)
        if "dashboard" not in page.url:
            state.add("bug", "auth", "Premium user login should reach dashboard", page.url)
        else:
            state.ok("Premium user reaches dashboard")

        page.goto(f"{base}/car/{RAM_CAR_ID}", wait_until="networkidle")
        if page.locator("text=/premium subscription required/i").count():
            state.add("bug", "premium", "Premium user still sees premium-required upsell on car page")
        else:
            state.ok("Premium car detail without premium upsell wall")

        csrf = _csrf_from_page(page)
        if csrf:
            chat_resp = page.request.post(
                f"{base}/api/car/{RAM_CAR_ID}/chat",
                headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
                data=json.dumps({"message": "Is this 4WD?"}),
            )
            if chat_resp.status == 200:
                data = chat_resp.json()
                if data.get("ok") and (data.get("reply") or data.get("answer")):
                    state.ok("Premium car chat API returns reply")
                elif data.get("ok"):
                    state.add("warn", "car-chat", "Premium chat ok but empty reply", str(data)[:200])
                else:
                    state.add("bug", "car-chat", "Premium chat API not ok", str(data)[:300])
            elif chat_resp.status == 503:
                state.add("warn", "car-chat", "Premium chat unavailable (LLM not configured)", chat_resp.text()[:200])
            else:
                state.add("bug", "car-chat", f"Premium chat status {chat_resp.status}", chat_resp.text()[:200])

            if page.locator("#car-packages-section").count() == 0:
                state.add("bug", "premium", "Premium user should see Packages section")
            else:
                state.ok("Premium user sees Packages section")
            if page.locator("#car-chat-section").count() == 0:
                state.add("bug", "premium", "Premium user should see car chat section")
            else:
                state.ok("Premium user sees car chat section")

            pkg_resp = page.request.post(
                f"{base}/api/cars/{RAM_CAR_ID}/packages/ensure?vision=0",
                headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
            )
            if pkg_resp.status == 200:
                pdata = pkg_resp.json()
                if pdata.get("ok"):
                    state.ok("Premium packages/ensure succeeds")
                else:
                    state.add("warn", "packages", "packages/ensure ok=false", str(pdata)[:200])
            elif pkg_resp.status == 403:
                state.add("bug", "packages", "Premium user packages/ensure forbidden")
            else:
                state.add("warn", "packages", f"packages/ensure status {pkg_resp.status}")

            prev = page.request.get(f"{base}/car/{RAM_CAR_ID}/window-sticker-preview.png")
            if prev.status in (200, 404):
                if prev.status == 200 and prev.headers.get("content-type", "").startswith("image/"):
                    state.ok("Window sticker preview PNG reachable for premium")
                elif prev.status == 404:
                    state.add("warn", "window-sticker", "No cached sticker PDF for test Ram (404 preview)")
            else:
                state.add("bug", "window-sticker", f"Preview PNG status {prev.status}")

        page.goto(f"{base}/premium", wait_until="networkidle")
        if page.locator("text=Upgrade to Premium").count() and page.locator("text=Payment Successful").count() == 0:
            upgrade_visible = page.locator("a[href*='premium/checkout'], .primary-button").filter(
                has_text=re.compile("Upgrade", re.I)
            )
            if upgrade_visible.count():
                state.add(
                    "improve",
                    "premium",
                    "Premium user still sees Upgrade CTA on /premium",
                )
            else:
                state.ok("Premium page does not push upgrade CTA")
        else:
            state.ok("Premium page state looks correct")

        page.goto(f"{base}/dashboard", wait_until="networkidle")
        inv_link = page.locator('a[href="/inventory"]')
        if inv_link.count():
            state.add("bug", "nav", "Premium consumer dashboard still shows My inventory link")
        else:
            state.ok("Premium consumer dashboard hides dealer inventory nav")

        page.goto(f"{base}/car/{RAM_CAR_ID}", wait_until="networkidle")
        if page.locator(".car-market-panel").count() or page.get_by_text("Market intelligence").count():
            state.ok("Premium car detail shows market intelligence block")
        else:
            state.add("warn", "market", "Market intelligence UI not visible on premium Ram detail")

        browser.close()


def write_report(path: Path, state: AuditState, base: str) -> None:
    bugs = [f for f in state.findings if f.severity == "bug"]
    warns = [f for f in state.findings if f.severity == "warn"]
    improves = [f for f in state.findings if f.severity in ("improve", "remove")]

    lines = [
        "# E2E user audit report",
        "",
        f"**Base URL:** `{base}`  ",
        f"**Generated:** {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}  ",
        "",
        "## Summary",
        "",
        f"| Result | Count |",
        f"|--------|------:|",
        f"| Passed checks | {len(state.passed)} |",
        f"| Bugs | {len(bugs)} |",
        f"| Warnings | {len(warns)} |",
        f"| Improvements | {len(improves)} |",
        "",
    ]
    if bugs:
        lines.append("## Bugs (fix first)")
        lines.append("")
        for f in bugs:
            lines.append(f"- **{f.area}:** {f.message}")
            if f.detail:
                lines.append(f"  - {f.detail}")
        lines.append("")
    if warns:
        lines.append("## Warnings")
        lines.append("")
        for f in warns:
            lines.append(f"- **{f.area}:** {f.message}")
            if f.detail:
                lines.append(f"  - {f.detail}")
        lines.append("")
    if improves:
        lines.append("## Improvements / removals")
        lines.append("")
        for f in improves:
            lines.append(f"- **{f.area}:** {f.message}")
            if f.detail:
                lines.append(f"  - {f.detail}")
        lines.append("")
    lines.append("## Passed")
    lines.append("")
    for p in state.passed:
        lines.append(f"- {p}")
    lines.append("")
    if state.console_errors:
        lines.append("## Browser console (errors/warnings)")
        lines.append("")
        for e in state.console_errors[:30]:
            lines.append(f"- `{e[:240]}`")
        lines.append("")
    if state.failed_requests:
        lines.append("## Failed network requests")
        lines.append("")
        for r in state.failed_requests[:30]:
            lines.append(f"- `{r[:240]}`")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-start-server", action="store_true")
    parser.add_argument("--report", default=str(ROOT / "docs" / "E2E_USER_AUDIT_REPORT.md"))
    args = parser.parse_args()

    port = args.port
    base = args.base_url or f"http://localhost:{port}"
    proc = None
    tmp = ROOT / ".e2e_audit"
    tmp.mkdir(exist_ok=True)
    users_db = tmp / "users.db"

    if not args.no_start_server:
        if users_db.exists():
            users_db.unlink()
        proc = _start_server(port, users_db)
        os.environ["USERS_DB_PATH"] = str(users_db)
        if not _wait_health(base):
            err = proc.stderr.read().decode() if proc.stderr else ""
            print("Server failed to start:", err[:2000], file=sys.stderr)
            proc.kill()
            return 1

    state = AuditState()
    try:
        run_audit(base, state)
    except Exception as e:
        state.add("bug", "runner", f"Audit crashed: {e}")
        import traceback

        state.findings[-1].detail = traceback.format_exc()[:800]
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_report(report_path, state, base)
    print(f"Report: {report_path}")
    bugs = sum(1 for f in state.findings if f.severity == "bug")
    print(f"Passed: {len(state.passed)} | Bugs: {bugs} | Warnings: {sum(1 for f in state.findings if f.severity == 'warn')}")
    return 1 if bugs else 0


if __name__ == "__main__":
    raise SystemExit(main())
