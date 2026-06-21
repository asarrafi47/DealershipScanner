"""
Playwright investigation script for Land Rover Chattanooga Algolia config.
Captures the actual /queries POST request body to see what filters the real browser sends.

Run: python workspace/investigate_landrover_algolia.py
"""
import asyncio
import json
import re

from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        algolia_queries = []

        async def on_request(request):
            if "algolia" in request.url:
                body = None
                try:
                    body = request.post_data
                except Exception:
                    pass
                entry = {"url": request.url, "method": request.method, "body": body}
                algolia_queries.append(entry)
                print("REQ URL:", request.url)
                print("REQ METHOD:", request.method)
                if body:
                    print("REQ BODY:", str(body)[:1200])
                try:
                    hdrs = await request.all_headers()
                    algolia_hdrs = {k: v for k, v in hdrs.items() if "algolia" in k.lower()}
                    if algolia_hdrs:
                        print("REQ ALGOLIA HEADERS:", algolia_hdrs)
                except Exception:
                    pass
                print()

        async def on_response(response):
            if "algolia" in response.url:
                try:
                    body = await response.json()
                    results = body.get("results", []) if isinstance(body, dict) else []
                    for r in results:
                        idx = r.get("index", "")
                        nb_hits = r.get("nbHits", "?")
                        hits = r.get("hits", [])
                        print(f"RESP index={idx!r} nbHits={nb_hits}")
                        if hits:
                            print("  First hit keys:", list(hits[0].keys())[:20])
                            print("  type:", hits[0].get("type"), "condition:", hits[0].get("condition"))
                            # Check for location/dealer fields
                            for k in ("dealer_id", "dealer_name", "api_id", "location", "store"):
                                if hits[0].get(k):
                                    print(f"  {k}:", hits[0][k])
                        print("  params from response:", r.get("params", "")[:300])
                except Exception as e:
                    print("RESP parse error:", e)
                print()

        page.on("request", on_request)
        page.on("response", on_response)

        print("=" * 60)
        print("Loading: https://www.landroverchattanooga.com/new-vehicles/")
        print("=" * 60)
        await page.goto(
            "https://www.landroverchattanooga.com/new-vehicles/",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        await asyncio.sleep(7)

        # Check page HTML for Algolia config
        html = await page.content()
        print("\n--- HTML Algolia config search ---")
        patterns = [
            (r'indexName["\s:=]+["\']([^"\']+)["\']', "indexName"),
            (r'_production_inventory[a-zA-Z0-9_-]*', "_production_inventory occurrences"),
            (r'appId["\s:=]+["\']([A-Z0-9]{8,})["\']', "appId"),
            (r'apiKey["\s:=]+["\']([a-f0-9]{20,})["\']', "apiKey"),
        ]
        for pattern, name in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                print(f"Pattern [{name}]:", matches[:5])

        # JS evaluation
        print("\n--- JS window object inspection ---")
        try:
            cfg = await page.evaluate(
                """() => {
                    const result = {};
                    const names = ['mvnAlgoliaConfig', 'algoliaConfig', 'diInventoryConfig', 'dealerinspire_inventory_vars'];
                    for (const n of names) {
                        if (window[n]) result[n] = JSON.stringify(window[n]);
                    }
                    return result;
                }"""
            )
            for k, v in cfg.items():
                print(f"  {k}: {v[:600]}")
        except Exception as e:
            print("JS eval error:", e)

        # Try loading /inventory/ page too
        print("\n" + "=" * 60)
        print("Loading: https://www.landroverchattanooga.com/new-inventory/")
        print("=" * 60)
        await page.goto(
            "https://www.landroverchattanooga.com/new-inventory/",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        await asyncio.sleep(5)

        print("\n--- Summary ---")
        print(f"Total Algolia requests captured: {len(algolia_queries)}")
        for q in algolia_queries:
            print("  URL:", q["url"][:120])
            if q["body"]:
                # Parse body to show indexName and params
                try:
                    parsed = json.loads(q["body"])
                    for req in parsed.get("requests", []):
                        print(f"    indexName={req.get('indexName')!r} params={req.get('params','')[:200]}")
                except Exception:
                    print("  Body (raw):", str(q["body"])[:300])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
