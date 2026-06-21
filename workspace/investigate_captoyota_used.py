"""
Check Capital Toyota used inventory SRP for JSON-LD.
"""
import asyncio
import re
import json

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=UA)
        page = await context.new_page()

        # First warm up on the main page
        print("Warming up on Capital Toyota main page...")
        await page.goto("https://www.capitaltoyota.com", wait_until="domcontentloaded", timeout=35_000)
        await asyncio.sleep(3)

        # Navigate to used inventory
        print("Navigating to used inventory...")
        try:
            await page.goto("https://www.capitaltoyota.com/used-inventory/index.htm",
                            wait_until="domcontentloaded", timeout=25_000)
        except Exception as e:
            print(f"Error: {e}")

        final_url = page.url
        print(f"Final URL: {final_url}")
        await asyncio.sleep(4)

        html = await page.content()
        print(f"HTML size: {len(html)}")

        # Check for Cloudflare challenge
        is_cf = "challenge" in html.lower() or "cloudflare" in html.lower() or "just a moment" in html.lower()
        print(f"Cloudflare challenge: {is_cf}")

        # Check for JSON-LD
        jsonld = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S)
        print(f"JSON-LD blocks: {len(jsonld)}")

        # Count VINs
        vins = re.findall(r'"vehicleIdentificationNumber"\s*:\s*"([A-HJ-NPR-Z0-9]{17})"', html)
        print(f"VINs in page: {len(vins)}")
        if vins:
            print(f"Sample VINs: {vins[:3]}")

        # Check dealereprocess presence
        print(f"Has dealereprocess: {'dealereprocess' in html.lower()}")

        # Try to get page title
        title = await page.title()
        print(f"Page title: {title}")

        await context.close()
        await browser.close()


asyncio.run(main())
