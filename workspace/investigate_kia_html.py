"""
Investigate Kia SRP HTML to find where inventory data comes from.
"""
import asyncio
import json
import re

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

        # Capture ALL network requests (non-image)
        captured = []

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type") or ""
            if any(x in url for x in [".png", ".jpg", ".gif", ".svg", ".ico", ".woff", ".ttf", ".css"]):
                return
            if "image" in ct or "font" in ct or "css" in ct:
                return
            try:
                body = await response.body()
                text = body.decode("utf-8", errors="replace")
                if len(text) > 100:
                    captured.append({
                        "url": url[:150],
                        "status": response.status,
                        "ct": ct[:40],
                        "len": len(text),
                        "has_vin": bool(re.search(r'[A-HJ-NPR-Z0-9]{17}', text)),
                        "has_vehicle": "vehicle" in text.lower()[:200] or "inventory" in text.lower()[:200],
                        "preview": text[:200],
                    })
            except Exception:
                pass

        page.on("response", on_response)

        print("Loading Kia SRP...")
        try:
            await page.goto("https://www.kiaofcleveland.com/new-inventory/index.htm",
                            wait_until="domcontentloaded", timeout=35_000)
        except Exception as e:
            print(f"Warning: {e}")

        await asyncio.sleep(8)
        html = await page.content()

        print(f"Page URL: {page.url}")
        print(f"HTML size: {len(html)}")

        # Look for VINs in HTML
        vins = re.findall(r'[A-HJ-NPR-Z0-9]{17}', html)
        print(f"VINs in HTML: {len(vins)} — examples: {vins[:5]}")

        # Look for vehicle JSON blocks
        json_blocks = re.findall(r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>', html, re.S)
        print(f"JSON script blocks: {len(json_blocks)}")
        for i, block in enumerate(json_blocks[:3]):
            print(f"  Block {i}: {block[:200]}")

        # Look for JSON-LD
        jsonld_blocks = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.S)
        print(f"JSON-LD blocks: {len(jsonld_blocks)}")
        for i, block in enumerate(jsonld_blocks[:2]):
            print(f"  LD+JSON {i}: {block[:300]}")

        # Look for vehicle-related inline data
        vehicle_matches = re.findall(r'(?:vehicleData|inventory_data|window\.vehicles|window\.inventory)\s*=\s*([^;]{50,})', html)
        print(f"Inline vehicle data vars: {len(vehicle_matches)}")
        for m in vehicle_matches[:2]:
            print(f"  {m[:300]}")

        # Show the non-image network requests, sorted by size
        print(f"\nNetwork responses ({len(captured)} captured):")
        interesting = sorted(
            [r for r in captured if r.get("has_vin") or r.get("has_vehicle")],
            key=lambda x: x["len"], reverse=True
        )
        for r in interesting[:10]:
            print(f"  [{r['len']}] vin={r['has_vin']} {r['url']}")
            print(f"    ct={r['ct']} preview={r['preview'][:200]}")
            print()

        # Also print all non-tiny responses for diagnostic
        print(f"\nAll large non-image responses:")
        for r in sorted(captured, key=lambda x: x["len"], reverse=True)[:15]:
            print(f"  [{r['len']}] status={r['status']} {r['url'][:120]}")

        await context.close()
        await browser.close()


asyncio.run(main())
