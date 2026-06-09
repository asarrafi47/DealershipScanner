#!/usr/bin/env python3
"""Deeper headless UX test: visuals, APIs, car chat, link crawl."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

BASE = "http://localhost:5001"
DEBUG = Path(__file__).resolve().parent.parent.parent / "debug" / "e2e_screenshots"


def main(base: str = BASE) -> int:
    base = base.rstrip("/")
    DEBUG.mkdir(parents=True, exist_ok=True)
    findings: list[dict] = []

    def note(sev: str, where: str, msg: str) -> None:
        findings.append({"severity": sev, "where": where, "message": msg})
        print(f"[{sev}] {where}: {msg}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 900})
        page = ctx.new_page()
        real_console_errors: list[str] = []

        def on_console(msg):
            if msg.type == "error":
                t = msg.text
                if "favicon" not in t.lower() and "ERR_ABORTED" not in t:
                    real_console_errors.append(t)

        page.on("console", on_console)

        # --- Landing ---
        page.goto(f"{base}/", wait_until="networkidle", timeout=45000)
        page.screenshot(path=str(DEBUG / "01_landing.png"), full_page=True)
        for sel in [".lp-headline", ".lp-cta-row", ".lp-marquee-track"]:
            if page.locator(sel).count() == 0:
                note("error", "landing", f"missing {sel}")
        # Broken internal links
        hrefs = page.eval_on_selector_all(
            "a[href]", "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs:
            if not h or h.startswith("#") or h.startswith("mailto:"):
                continue
            if h.startswith("/"):
                r = page.request.get(base + h, timeout=15000)
                if r.status >= 400:
                    note("error", "landing", f"link {h} -> HTTP {r.status}")

        # --- Listings desktop ---
        page.goto(f"{base}/listings", wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(800)
        page.screenshot(path=str(DEBUG / "02_listings.png"), full_page=False)

        # Smart search — single stable query, wait for API
        with page.expect_response(
            lambda r: "/api/search/smart" in r.url and r.request.method == "POST",
            timeout=15000,
        ) as resp_info:
            page.fill("#smart-search-input", "BMW X5 under 80000")
            page.wait_for_timeout(500)
        resp = resp_info.value
        if resp.status != 200:
            note("error", "listings", f"smart search HTTP {resp.status}")
        else:
            data = resp.json()
            n = len(data.get("results") or [])
            note("info", "listings", f"smart search returned {n} results")
            if n == 0:
                note("warn", "listings", "smart search returned zero results for 'BMW X5 under 80000'")

        # Filter form submit
        page.locator("#search-form").evaluate("f => f.requestSubmit()")
        page.wait_for_load_state("networkidle", timeout=30000)
        page.screenshot(path=str(DEBUG / "03_listings_filtered.png"), full_page=False)

        # Pill spam then ensure grid still renders (allow client render after navigation)
        triggers = page.locator(".pill-trigger:visible")
        for i in range(min(triggers.count(), 8)):
            for _ in range(5):
                try:
                    triggers.nth(i).click(timeout=300, force=True)
                except PlaywrightError:
                    pass
        try:
            page.wait_for_selector('a[href^="/car/"]', timeout=12_000)
        except PlaywrightError:
            page.wait_for_timeout(1500)
        cards = page.locator('a[href^="/car/"]')
        n_cards = cards.count()
        if n_cards == 0:
            note("error", "listings", "no car cards after filter interaction")
        else:
            note("info", "listings", f"{n_cards} car cards visible")

        car_href = cards.first.get_attribute("href")
        car_id = int(re.search(r"\d+", car_href or "0").group()) if car_href else 0

        # --- Mobile listings ---
        mctx = browser.new_context(viewport={"width": 390, "height": 844})
        mpage = mctx.new_page()
        mpage.goto(f"{base}/listings", wait_until="domcontentloaded", timeout=45000)
        mpage.wait_for_timeout(600)
        overflow = mpage.evaluate(
            "() => document.documentElement.scrollWidth > window.innerWidth + 20"
        )
        if overflow:
            note("warn", "listings@mobile", "horizontal scroll overflow")
        mpage.screenshot(path=str(DEBUG / "04_listings_mobile.png"), full_page=False)
        mctx.close()

        # --- Car detail ---
        if car_id:
            page.goto(f"{base}/car/{car_id}", wait_until="networkidle", timeout=45000)
            page.screenshot(path=str(DEBUG / "05_car_detail.png"), full_page=True)
            title = page.title()
            if "Sarrafi" not in title and str(car_id) not in title:
                note("warn", "car", f"unexpected title: {title}")
            access = page.evaluate(
                """() => {
                    const el = document.getElementById('car-page-access-json');
                    if (!el || !el.textContent) return null;
                    try { return JSON.parse(el.textContent); } catch { return null; }
                }"""
            )
            has_chat = page.locator("#car-chat-section").count() > 0
            billing_on = bool(access and access.get("billing_stripe_enabled"))
            premium_ui = bool(access and access.get("show_premium_features"))
            if not has_chat and billing_on and not premium_ui:
                note(
                    "info",
                    "car",
                    "ask tab hidden for guest (billing on; login/premium required)",
                )
            elif not has_chat:
                note("error", "car", "missing chat section")
            # Spam save / gallery buttons
            for sel in ["#car-chat-send", ".primary-button", "button"]:
                loc = page.locator(sel)
                for i in range(min(loc.count(), 3)):
                    try:
                        loc.nth(i).click(timeout=400, force=True)
                    except PlaywrightError:
                        pass
            page.wait_for_timeout(300)
            # Chat without premium — expect premium hint or 403, not crash
            inp = page.locator("#car-chat-input")
            if inp.count():
                inp.fill("Is this AWD?")
                with page.expect_response(
                    lambda r: "/api/car/" in r.url and "/chat" in r.url,
                    timeout=12000,
                ) as chat_resp:
                    page.locator("#car-chat-send").click()
                cr = chat_resp.value
                if cr.status == 403:
                    note("info", "car", "chat correctly gated (403) for guest/non-premium")
                elif cr.status == 200:
                    note("info", "car", "chat responded 200")
                elif cr.status == 429:
                    note("info", "car", "chat rate limited under test")
                else:
                    note("warn", "car", f"chat HTTP {cr.status}")

        # --- Premium ---
        page.goto(f"{base}/premium", wait_until="networkidle", timeout=30000)
        page.screenshot(path=str(DEBUG / "06_premium.png"), full_page=True)
        for _ in range(10):
            btns = page.locator("button:visible, a.primary-button:visible")
            if btns.count():
                try:
                    btns.first.click(timeout=500, force=True)
                except PlaywrightError:
                    pass
        content = page.content()
        if page.url.endswith("/premium") and "checkout" not in content.lower() and "stripe" not in content.lower():
            # may still be ok if CTA is subscribe wording
            if "premium" not in content.lower():
                note("warn", "premium", "page content looks thin")

        # --- Register / login UX ---
        page.goto(f"{base}/register", wait_until="domcontentloaded")
        page.screenshot(path=str(DEBUG / "07_register.png"))
        page.goto(f"{base}/login", wait_until="domcontentloaded")
        page.screenshot(path=str(DEBUG / "08_login.png"))

        # --- Legacy MFA routes should redirect, not 500 ---
        for legacy in ["/mfa/verify", "/mfa/setup", "/mfa/choose"]:
            r = page.request.get(base + legacy, max_redirects=5)
            if r.status >= 500:
                note("error", "mfa_legacy", f"{legacy} -> {r.status}")
            elif r.status >= 400 and r.status not in (404, 405):
                note("warn", "mfa_legacy", f"{legacy} -> {r.status}")

        ctx.close()
        browser.close()

    if real_console_errors:
        for e in set(real_console_errors):
            note("error", "console", e)
    else:
        note("info", "console", "no JS console errors detected")

    report = {
        "base": base,
        "screenshots": str(DEBUG),
        "findings": findings,
        "error_count": sum(1 for f in findings if f["severity"] == "error"),
    }
    out = Path(__file__).resolve().parent.parent.parent / "debug" / "e2e_deep_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 1 if report["error_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1 else BASE))
