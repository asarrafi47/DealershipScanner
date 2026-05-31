#!/usr/bin/env python3
"""Headless browser smoke / abuse test against local Sarrafi Cars app."""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

BASE = "http://localhost:5001"

PUBLIC_PATHS = [
    "/",
    "/login",
    "/register",
    "/listings",
    "/premium",
    "/search",
    "/dashboard",  # expect redirect when logged out
]

VIEWPORTS = [
    ("desktop", 1280, 800),
    ("mobile", 390, 844),
]


@dataclass
class Issue:
    severity: str  # error | warn | info
    page: str
    kind: str
    detail: str


@dataclass
class RunReport:
    issues: list[Issue] = field(default_factory=list)
    pages_ok: list[str] = field(default_factory=list)
    car_id_tested: int | None = None

    def add(self, severity: str, page: str, kind: str, detail: str) -> None:
        self.issues.append(Issue(severity, page, kind, detail))


def _attach_listeners(page, report: RunReport, label: str) -> None:
    def on_console(msg):
        if msg.type in ("error", "warning"):
            text = msg.text
            if "favicon" in text.lower():
                return
            report.add(
                "error" if msg.type == "error" else "warn",
                label,
                "console",
                f"{msg.type}: {text}",
            )

    def on_page_error(exc):
        report.add("error", label, "pageerror", str(exc))

    def on_request_failed(req):
        if req.resource_type in ("image", "font", "media"):
            return
        fail = req.failure
        reason = fail if isinstance(fail, str) else (fail or "unknown")
        report.add("error", label, "request_failed", f"{req.method} {req.url} — {reason}")

    page.on("console", on_console)
    page.on("pageerror", on_page_error)
    page.on("requestfailed", on_request_failed)


def _goto(page, path: str, report: RunReport) -> bool:
    url = urljoin(BASE, path)
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(400)
        if resp and resp.status >= 400:
            report.add("error", path, "http", f"status {resp.status}")
            return False
        report.pages_ok.append(path)
        return True
    except PlaywrightError as e:
        report.add("error", path, "navigation", str(e))
        return False


def _spam_click_buttons(page, path: str, report: RunReport, rounds: int = 8) -> None:
    buttons = page.locator("button:visible, [role='button']:visible, .pill-trigger:visible")
    try:
        n = min(buttons.count(), 25)
    except PlaywrightError:
        return
    for r in range(rounds):
        for i in range(n):
            try:
                buttons.nth(i).click(timeout=500, force=True)
                page.wait_for_timeout(30)
            except PlaywrightError:
                pass
    # After spam, page should still have body content
    try:
        body_text = page.locator("body").inner_text(timeout=2000)
        if len(body_text.strip()) < 20:
            report.add("error", path, "ui", "page body nearly empty after button spam")
    except PlaywrightError as e:
        report.add("error", path, "ui", f"body unreadable after spam: {e}")


def _check_layout(page, path: str, report: RunReport, viewport_name: str) -> None:
    checks = page.evaluate(
        """() => {
        const issues = [];
        const vw = window.innerWidth;
        const docW = document.documentElement.scrollWidth;
        if (docW > vw + 24) issues.push('horizontal_overflow:' + docW + '>' + vw);
        const nav = document.querySelector('.topbar, .lp-body nav, nav');
        if (nav) {
            const r = nav.getBoundingClientRect();
            if (r.width < 50 || r.height < 20) issues.push('nav_tiny');
        }
        const imgs = [...document.querySelectorAll('img[src]')];
        for (const img of imgs.slice(0, 40)) {
            if (img.complete && img.naturalWidth === 0 && img.src && !img.src.startsWith('data:')) {
                issues.push('broken_img:' + img.src.slice(0, 120));
            }
        }
        const links = [...document.querySelectorAll('a[href]')];
        let empty = 0;
        for (const a of links) {
            const t = (a.textContent || '').trim();
            if (!t && !a.querySelector('img, svg')) empty++;
        }
        if (empty > 8) issues.push('many_empty_link_labels:' + empty);
        return issues;
    }"""
    )
    for c in checks:
        report.add("warn", path, f"layout_{viewport_name}", c)


def _test_login_spam(page, report: RunReport) -> None:
    path = "/login"
    if not _goto(page, path, report):
        return
    for _ in range(6):
        page.fill('input[name="login"]', "not-a-real-user@example.com", timeout=5000)
        page.fill('input[name="password"]', "wrong-password-12345", timeout=5000)
        try:
            page.locator('form button[type="submit"], form input[type="submit"]').first.click(timeout=2000)
            page.wait_for_timeout(150)
        except PlaywrightError:
            pass
    content = page.content()
    if "Too many login attempts" in content:
        report.add("info", path, "rate_limit", "login rate limit triggered (expected under spam)")
    elif "Invalid username" in content or "Enter username" in content:
        report.add("info", path, "form", "login errors shown correctly under spam")
    else:
        report.add("warn", path, "form", "unexpected login page state after spam submits")


def _test_register_validation(page, report: RunReport) -> None:
    path = "/register"
    if not _goto(page, path, report):
        return
    try:
        page.fill('input[name="username"]', "a", timeout=2000)
        page.fill('input[name="email"]', "not-an-email", timeout=2000)
        page.fill('input[name="password"]', "short", timeout=2000)
        page.locator('form button[type="submit"], form input[type="submit"]').first.click(timeout=2000)
        page.wait_for_timeout(300)
    except PlaywrightError as e:
        report.add("error", path, "form", str(e))
        return
    content = page.content().lower()
    if "password" not in content and "email" not in content and "username" not in content:
        report.add("warn", path, "form", "no obvious validation message on bad register")


def _test_listings(page, report: RunReport) -> None:
    path = "/listings"
    if not _goto(page, path, report):
        return

    # Dismiss guest banner if present
    try:
        page.locator(".guest-banner-dismiss").click(timeout=1000)
    except PlaywrightError:
        pass

    # Smart search spam
    try:
        inp = page.locator("#smart-search-input")
        queries = [
            "white BMW X5 under 70000",
            "2024 toyota camry",
            "1HGBH41JXMN109186",  # sample VIN shape
            "!!!@@@",
            "a" * 500,
        ]
        for q in queries:
            inp.fill(q, timeout=2000)
            page.wait_for_timeout(400)
        inp.fill("", timeout=2000)
        page.wait_for_timeout(300)
    except PlaywrightError as e:
        report.add("error", path, "smart_search", str(e))

    # Open pill dropdowns rapidly
    triggers = page.locator(".pill-trigger:visible")
    try:
        tc = min(triggers.count(), 12)
        for i in range(tc):
            for _ in range(3):
                try:
                    triggers.nth(i).click(timeout=400, force=True)
                except PlaywrightError:
                    pass
    except PlaywrightError:
        pass

    _spam_click_buttons(page, path, report, rounds=5)

    # Grid should exist
    try:
        grid = page.locator("#car-grid, .car-grid, [data-car-id]")
        if grid.count() == 0:
            # fallback: any car card link
            cards = page.locator('a[href^="/car/"]')
            if cards.count() == 0:
                report.add("warn", path, "content", "no car cards visible in grid")
    except PlaywrightError:
        pass

    # Capture first car link for detail page test
    try:
        href = page.locator('a[href^="/car/"]').first.get_attribute("href", timeout=3000)
        if href:
            m = re.search(r"/car/(\d+)", href)
            if m:
                return int(m.group(1))
    except PlaywrightError:
        pass
    return None


def _test_car_page(page, car_id: int, report: RunReport) -> None:
    path = f"/car/{car_id}"
    if not _goto(page, path, report):
        return
    _spam_click_buttons(page, path, report, rounds=4)
    # Chat widget if present — spam send without login
    try:
        chat_input = page.locator(
            '#car-chat-input, textarea[name="message"], .car-chat-input, [data-car-chat-input]'
        ).first
        if chat_input.count() > 0:
            for _ in range(4):
                chat_input.fill("What is the horsepower?", timeout=1500)
                send = page.locator(
                    '#car-chat-send, button[type="submit"]:near(#car-chat-input), .car-chat-send'
                )
                if send.count():
                    send.first.click(timeout=800, force=True)
                page.wait_for_timeout(200)
    except PlaywrightError:
        pass


def _test_premium(page, report: RunReport) -> None:
    path = "/premium"
    if not _goto(page, path, report):
        return
    _spam_click_buttons(page, path, report, rounds=6)
    content = page.content()
    if "premium" not in content.lower() and "upgrade" not in content.lower():
        report.add("warn", path, "content", "premium page missing expected keywords")


def run(base: str = BASE) -> int:
    global BASE
    BASE = base.rstrip("/")
    report = RunReport()
    car_id: int | None = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for vp_name, w, h in VIEWPORTS:
            context = browser.new_context(viewport={"width": w, "height": h})
            page = context.new_page()
            _attach_listeners(page, report, vp_name)

            for path in PUBLIC_PATHS:
                label = f"{path}@{vp_name}"
                _attach_listeners(page, report, label)
                ok = _goto(page, path, report)
                if not ok:
                    continue
                if path == "/dashboard" and "/login" not in page.url and "/listings" not in page.url:
                    report.add("warn", label, "auth", f"dashboard without login landed at {page.url}")
                _check_layout(page, path, report, vp_name)
                if path not in ("/login", "/register"):
                    _spam_click_buttons(page, path, report, rounds=4)

            if vp_name == "desktop":
                _test_login_spam(page, report)
                _test_register_validation(page, report)
                car_id = _test_listings(page, report)
                _test_premium(page, report)
                if car_id:
                    report.car_id_tested = car_id
                    _test_car_page(page, car_id, report)

            context.close()

        browser.close()

    # Dedupe issues
    seen: set[tuple] = set()
    unique: list[Issue] = []
    for i in report.issues:
        key = (i.severity, i.page, i.kind, i.detail[:200])
        if key in seen:
            continue
        seen.add(key)
        unique.append(i)
    report.issues = unique

    out = {
        "base": BASE,
        "pages_ok": report.pages_ok,
        "car_id_tested": report.car_id_tested,
        "issue_count": len(report.issues),
        "errors": [i.__dict__ for i in report.issues if i.severity == "error"],
        "warnings": [i.__dict__ for i in report.issues if i.severity == "warn"],
        "info": [i.__dict__ for i in report.issues if i.severity == "info"],
    }
    out_path = Path(__file__).resolve().parent.parent.parent / "debug" / "e2e_smoke_report.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(json.dumps(out, indent=2))
    print(f"\nWrote {out_path}")
    err_n = len(out["errors"])
    return 1 if err_n else 0


if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else BASE
    raise SystemExit(run(base_url))
