"""
Direct browser-proxy Algolia query investigation.
Uses page.evaluate(fetch) to bypass referrer restrictions and get real hits.
"""
import asyncio
import json

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def try_algolia_query(page, app_id, api_key, index_name, label, extra_filters=""):
    """Query Algolia via browser-proxy fetch and return results."""
    endpoint = f"https://{app_id.lower()}-dsn.algolia.net/1/indexes/{index_name}/query"
    params = f"hitsPerPage=5&page=0"
    if extra_filters:
        import urllib.parse
        params += f"&filters={urllib.parse.quote(extra_filters)}"

    js = f"""
    async () => {{
        try {{
            const resp = await fetch({json.dumps(endpoint)}, {{
                method: "POST",
                headers: {{
                    "Content-Type": "application/json",
                    "X-Algolia-Application-Id": {json.dumps(app_id)},
                    "X-Algolia-API-Key": {json.dumps(api_key)},
                }},
                body: JSON.stringify({{
                    params: {json.dumps(params)}
                }})
            }});
            if (!resp.ok) {{
                return {{error: resp.status + " " + await resp.text()}};
            }}
            const data = await resp.json();
            return {{
                nbHits: data.nbHits,
                index: data.index || {json.dumps(index_name)},
                params: data.params,
                firstHit: data.hits && data.hits[0] ? {{
                    vin: data.hits[0].vin,
                    type: data.hits[0].type,
                    dealer_id: data.hits[0].dealer_id,
                    account_id: data.hits[0].account_id,
                    api_id: data.hits[0].api_id,
                    dealer_name: data.hits[0].dealer_name,
                    make: data.hits[0].make,
                    model: data.hits[0].model,
                    keys: Object.keys(data.hits[0]).slice(0, 25),
                }} : null,
            }};
        }} catch(e) {{
            return {{error: e.toString()}};
        }}
    }}
    """
    result = await page.evaluate(js)
    print(f"  [{label}] index={index_name!r} extra_filters={extra_filters!r} => {result}")
    return result


async def investigate_hixson(browser):
    print("\n" + "=" * 60)
    print("HIXSON CHEVROLET Investigation")
    print("=" * 60)
    context = await browser.new_context(user_agent=UA)
    page = await context.new_page()
    await page.goto("https://www.hixsonchevrolet.com/new-vehicles/", wait_until="domcontentloaded", timeout=35_000)
    await asyncio.sleep(3)

    app_id = "EHWUW84XVK"
    api_key = "fb58227032e79f03b9b820cbaea7f8fb"

    results = {}

    # Try the embedded DeRidder index - see what's there
    r = await try_algolia_query(page, app_id, api_key, "hixsonchevroletofderidder_production_inventory", "HIXSON/DeRidder")
    results["deridder_index"] = r

    # Try common Chattanooga index name patterns
    for suffix in ["hixsonchevrolet_chattanooga_production_inventory",
                   "hixsonchevroletofchattanooga_production_inventory",
                   "hixson_chattanooga_production_inventory",
                   "hixsonchevrolet_production_inventory"]:
        r2 = await try_algolia_query(page, app_id, api_key, suffix, f"HIXSON/{suffix[:30]}")
        results[suffix] = r2

    # What dealer_ids exist in the DeRidder index? Look at first hit
    print("\n  Now checking DeRidder index for dealer/location fields...")
    js2 = f"""
    async () => {{
        const resp = await fetch("https://{app_id.lower()}-dsn.algolia.net/1/indexes/hixsonchevroletofderidder_production_inventory/query", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json",
                "X-Algolia-Application-Id": "{app_id}",
                "X-Algolia-API-Key": "{api_key}",
            }},
            body: JSON.stringify({{ params: "hitsPerPage=5&page=0" }})
        }});
        const data = await resp.json();
        return {{
            nbHits: data.nbHits,
            hits: data.hits.map(h => ({{
                vin: h.vin,
                make: h.make,
                model: h.model,
                dealer_id: h.dealer_id,
                account_id: h.account_id,
                api_id: h.api_id,
                dealer_name: h.dealer_name,
                type: h.type,
            }}))
        }};
    }}
    """
    sample = await page.evaluate(js2)
    print(f"  DeRidder index sample ({sample.get('nbHits')} hits): {json.dumps(sample.get('hits', []), indent=2)[:2000]}")
    results["deridder_sample"] = sample

    await context.close()
    return results


async def investigate_land_rover(browser):
    print("\n" + "=" * 60)
    print("LAND ROVER CHATTANOOGA Investigation")
    print("=" * 60)
    context = await browser.new_context(user_agent=UA)
    page = await context.new_page()
    await page.goto("https://www.landroverchattanooga.com/new-vehicles/", wait_until="domcontentloaded", timeout=35_000)
    await asyncio.sleep(3)

    app_id = "10APRXOTJR"
    api_key = "003c8cddb5b15f2cfa774c02b7a3b59e"
    index = "landroverchattanooga-sbm0326_production_inventory"

    results = {}

    # No filter
    r = await try_algolia_query(page, app_id, api_key, index, "LR/no-filter")
    results["no_filter"] = r

    # Common filters for new inventory
    for filt in ["type:New", "condition:New", "status:New", "new_used:New"]:
        r2 = await try_algolia_query(page, app_id, api_key, index, f"LR/{filt}", extra_filters=filt)
        results[filt] = r2

    # Sample first 5 hits to see what fields exist
    print("\n  Sampling first 5 hits from Land Rover index...")
    js3 = f"""
    async () => {{
        const resp = await fetch("https://{app_id.lower()}-dsn.algolia.net/1/indexes/{index}/query", {{
            method: "POST",
            headers: {{
                "Content-Type": "application/json",
                "X-Algolia-Application-Id": "{app_id}",
                "X-Algolia-API-Key": "{api_key}",
            }},
            body: JSON.stringify({{ params: "hitsPerPage=5&page=0" }})
        }});
        if (!resp.ok) return {{error: resp.status + " " + await resp.text()}};
        const data = await resp.json();
        return {{
            nbHits: data.nbHits,
            hits: data.hits.map(h => ({{
                vin: h.vin,
                make: h.make,
                model: h.model,
                type: h.type,
                condition: h.condition,
                status: h.status,
                new_used: h.new_used,
                dealer_id: h.dealer_id,
                account_id: h.account_id,
                api_id: h.api_id,
            }}))
        }};
    }}
    """
    sample = await page.evaluate(js3)
    print(f"  Land Rover sample ({sample.get('nbHits')} hits): {json.dumps(sample.get('hits', []), indent=2)[:2000]}")
    results["sample"] = sample

    await context.close()
    return results


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        hixson = await investigate_hixson(browser)
        land_rover = await investigate_land_rover(browser)
        await browser.close()

    out = {"hixson": hixson, "land_rover": land_rover}
    path = "/Users/asarrafi/Projects/DealershipScanner/workspace/algolia_direct_results.json"
    with open(path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nResults written to {path}")


if __name__ == "__main__":
    asyncio.run(main())
