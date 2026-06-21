"""
Playwright investigation script for HIXSON CHEVROLET Algolia config.
Captures all Algolia network requests to find the correct index.

Run: python workspace/investigate_hixson_algolia.py
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

        algolia_requests = []
        algolia_responses = []

        async def on_request(request):
            if "algolia" in request.url:
                body = None
                try:
                    body = request.post_data
                except Exception:
                    pass
                entry = {"url": request.url, "body": body}
                algolia_requests.append(entry)
                print("REQ URL:", request.url)
                if body:
                    print("REQ BODY (first 800):", str(body)[:800])
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
                        print(f"RESP index={idx!r} nbHits={nb_hits} url={response.url[:100]}")
                        if hits:
                            print("  First hit keys:", list(hits[0].keys())[:15])
                            print("  First hit sample:", json.dumps(hits[0])[:300])
                    algolia_responses.append({"url": response.url, "results": [
                        {"index": r.get("index"), "nbHits": r.get("nbHits")} for r in results
                    ]})
                except Exception as e:
                    print("RESP parse error:", e)
                print()

        page.on("request", on_request)
        page.on("response", on_response)

        print("=" * 60)
        print("Loading: https://www.hixsonchevrolet.com/new-vehicles/")
        print("=" * 60)
        await page.goto(
            "https://www.hixsonchevrolet.com/new-vehicles/",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        await asyncio.sleep(6)

        # Check page HTML for Algolia config patterns
        html = await page.content()
        print("\n--- HTML Algolia config search ---")
        patterns = [
            (r'indexName["\s:=]+(["\']([^"\']+)["\'])', "indexName"),
            (r'algolia_index["\s:=]+(["\']([^"\']+)["\'])', "algolia_index"),
            (r'_production_inventory[a-zA-Z0-9_-]*', "_production_inventory"),
            (r'mvnAlgoliaConfig\s*=\s*(\{[^;]{10,500}?\})\s*;', "mvnAlgoliaConfig"),
            (r'appId["\s:=]+(["\']([A-Z0-9]{8,})["\'])', "appId"),
        ]
        for pattern, name in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                print(f"Pattern [{name}] found {len(matches)} match(es):")
                for m in matches[:5]:
                    print("  ", m)

        # Dump any script blocks containing algolia
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)
        for i, s in enumerate(scripts):
            if "algolia" in s.lower():
                print(f"\nScript block {i} (algolia-containing, first 1000 chars):")
                print(s[:1000])
                print("...")

        # Also try JS evaluation
        print("\n--- JS window object inspection ---")
        try:
            cfg = await page.evaluate(
                """() => {
                    const objs = [
                        window.mvnAlgoliaConfig,
                        window.algoliaConfig,
                        window.diInventoryConfig,
                        window.dealerinspire_inventory_vars,
                    ];
                    return objs.map(o => o ? JSON.stringify(o) : null);
                }"""
            )
            for i, c in enumerate(cfg):
                if c:
                    print(f"Window config object {i}: {c[:500]}")
        except Exception as e:
            print("JS eval error:", e)

        print("\n--- Summary ---")
        print(f"Total Algolia requests captured: {len(algolia_requests)}")
        print(f"Total Algolia responses captured: {len(algolia_responses)}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
