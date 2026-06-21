"""
Combined Playwright investigation for:
1. HIXSON CHEVROLET - find correct Algolia index
2. Land Rover Chattanooga - find required Algolia filters

Run: /Users/asarrafi/Projects/DealershipScanner/.venv/bin/python workspace/investigate_both_algolia.py
"""
import asyncio
import json
import re

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def investigate_site(browser, url: str, label: str) -> dict:
    context = await browser.new_context(user_agent=UA)
    page = await context.new_page()

    algolia_reqs = []
    algolia_resps = []

    async def on_request(request):
        if "algolia" in request.url:
            body = None
            try:
                body = request.post_data
            except Exception:
                pass
            hdrs = {}
            try:
                all_hdrs = await request.all_headers()
                hdrs = {k: v for k, v in all_hdrs.items() if "algolia" in k.lower()}
            except Exception:
                pass
            algolia_reqs.append({"url": request.url, "body": body, "algolia_headers": hdrs})

    async def on_response(response):
        if "algolia" in response.url:
            try:
                body = await response.json()
                algolia_resps.append({"url": response.url, "body": body})
            except Exception as e:
                algolia_resps.append({"url": response.url, "error": str(e)})

    page.on("request", on_request)
    page.on("response", on_response)

    print(f"\n{'=' * 60}")
    print(f"[{label}] Loading: {url}")
    print("=" * 60)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=35_000)
    except Exception as e:
        print(f"[{label}] Navigation error: {e}")

    await asyncio.sleep(8)

    html = await page.content()

    # Extract Algolia config from HTML
    index_matches = re.findall(r'indexName["\s:=]+["\']([^"\']+)["\']', html, re.IGNORECASE)
    app_id_matches = re.findall(r'appId["\s:=]+["\']([A-Z0-9]{8,})["\']', html, re.IGNORECASE)
    production_matches = re.findall(r'_production_inventory[a-zA-Z0-9_-]*', html, re.IGNORECASE)

    # JS window objects
    js_config = {}
    try:
        js_config = await page.evaluate(
            """() => {
                const result = {};
                const names = ['mvnAlgoliaConfig', 'algoliaConfig', 'diInventoryConfig',
                               'dealerinspire_inventory_vars', 'DI_inventory_vars'];
                for (const n of names) {
                    if (window[n]) result[n] = JSON.stringify(window[n]).substring(0, 800);
                }
                return result;
            }"""
        )
    except Exception as e:
        js_config = {"error": str(e)}

    result = {
        "label": label,
        "url": url,
        "html_index_names": list(set(index_matches))[:10],
        "html_app_ids": list(set(app_id_matches))[:5],
        "html_production_inventory_refs": list(set(production_matches))[:10],
        "js_window_configs": js_config,
        "algolia_requests": [],
        "algolia_responses_summary": [],
    }

    print(f"\n[{label}] HTML indexName matches: {result['html_index_names']}")
    print(f"[{label}] HTML appId matches: {result['html_app_ids']}")
    print(f"[{label}] HTML _production_inventory refs: {result['html_production_inventory_refs']}")
    print(f"[{label}] JS window configs: {js_config}")

    print(f"\n[{label}] Algolia network requests ({len(algolia_reqs)}):")
    for req in algolia_reqs:
        print(f"  REQ URL: {req['url'][:120]}")
        if req["body"]:
            body_str = str(req["body"])[:1000]
            print(f"  REQ BODY: {body_str}")
        if req["algolia_headers"]:
            print(f"  ALGOLIA HEADERS: {req['algolia_headers']}")
        result["algolia_requests"].append({
            "url": req["url"],
            "body": str(req["body"] or "")[:800],
            "algolia_headers": req["algolia_headers"],
        })

    print(f"\n[{label}] Algolia responses ({len(algolia_resps)}):")
    for resp in algolia_resps:
        body = resp.get("body", {})
        if isinstance(body, dict):
            results_list = body.get("results", [])
            for r in results_list:
                idx = r.get("index", "?")
                nb = r.get("nbHits", "?")
                hits = r.get("hits", [])
                params = r.get("params", "")[:200]
                print(f"  index={idx!r} nbHits={nb} params={params!r}")
                if hits:
                    h0 = hits[0]
                    print(f"  First hit keys: {list(h0.keys())[:20]}")
                    for fk in ("type", "condition", "in_transit", "api_id", "dealer_id", "store_id", "dealer_name"):
                        if fk in h0:
                            print(f"    {fk}={h0[fk]!r}")
                result["algolia_responses_summary"].append({
                    "index": idx, "nbHits": nb, "params": params,
                    "first_hit_sample": {k: h0.get(k) for k in ("type", "condition", "in_transit", "api_id") if k in h0} if hits else {},
                })

    await context.close()
    return result


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # Run both investigations
        hixson = await investigate_site(
            browser,
            "https://www.hixsonchevrolet.com/new-vehicles/",
            "HIXSON CHEVROLET",
        )
        land_rover = await investigate_site(
            browser,
            "https://www.landroverchattanooga.com/new-vehicles/",
            "LAND ROVER CHATTANOOGA",
        )

        await browser.close()

        # Write results
        output = {"hixson": hixson, "land_rover": land_rover}
        out_path = "/Users/asarrafi/Projects/DealershipScanner/workspace/algolia_investigation_results.json"
        with open(out_path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\n\nResults written to {out_path}")

        # Print summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print("\nHIXSON CHEVROLET:")
        print(f"  HTML indices: {hixson['html_index_names']}")
        print(f"  Live Algolia indices (from network): {[r['index'] for r in hixson['algolia_responses_summary']]}")

        print("\nLAND ROVER CHATTANOOGA:")
        print(f"  HTML indices: {land_rover['html_index_names']}")
        print(f"  Live Algolia indices (from network): {[r['index'] for r in land_rover['algolia_responses_summary']]}")
        print(f"  Live Algolia params (from network): {[r['params'] for r in land_rover['algolia_responses_summary']]}")
        print(f"  nbHits per response: {[r['nbHits'] for r in land_rover['algolia_responses_summary']]}")


if __name__ == "__main__":
    asyncio.run(main())
