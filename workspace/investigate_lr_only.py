"""Land Rover Chattanooga - direct Algolia query investigation."""
import asyncio
import json

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

APP_ID = "10APRXOTJR"
API_KEY = "003c8cddb5b15f2cfa774c02b7a3b59e"
INDEX = "landroverchattanooga-sbm0326_production_inventory"


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=UA)
        page = await context.new_page()

        print("Loading Land Rover site...")
        await page.goto("https://www.landroverchattanooga.com/new-vehicles/", wait_until="domcontentloaded", timeout=35_000)
        await asyncio.sleep(3)

        # Query with no filter
        result = await page.evaluate(f"""
        async () => {{
            const resp = await fetch("https://{APP_ID.lower()}-dsn.algolia.net/1/indexes/{INDEX}/query", {{
                method: "POST",
                headers: {{
                    "Content-Type": "application/json",
                    "X-Algolia-Application-Id": "{APP_ID}",
                    "X-Algolia-API-Key": "{API_KEY}",
                }},
                body: JSON.stringify({{ params: "hitsPerPage=10&page=0" }})
            }});
            if (!resp.ok) return {{error: resp.status + ": " + await resp.text()}};
            const d = await resp.json();
            return {{
                nbHits: d.nbHits,
                params: d.params,
                hits: (d.hits || []).map(h => ({{
                    vin: h.vin, make: h.make, model: h.model,
                    type: h.type, condition: h.condition, status: h.status,
                    new_used: h.new_used, dealer_id: h.dealer_id,
                    account_id: h.account_id, api_id: h.api_id,
                    keys: Object.keys(h).slice(0, 30),
                }}))
            }};
        }}
        """)

        print(f"\nNo-filter query: nbHits={result.get('nbHits')} error={result.get('error')}")
        if result.get("hits"):
            print(f"First hit: {json.dumps(result['hits'][0], indent=2)}")
            print(f"All hits type/condition: {[(h.get('type'), h.get('condition'), h.get('new_used')) for h in result['hits']]}")
        elif result.get("error"):
            print(f"Error: {result['error']}")

        # Try multi-query (like the DealerInspire scraper does)
        multi_result = await page.evaluate(f"""
        async () => {{
            const resp = await fetch("https://{APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries", {{
                method: "POST",
                headers: {{
                    "Content-Type": "application/json",
                    "X-Algolia-Application-Id": "{APP_ID}",
                    "X-Algolia-API-Key": "{API_KEY}",
                }},
                body: JSON.stringify({{
                    requests: [
                        {{ indexName: "{INDEX}", params: "hitsPerPage=5&page=0" }},
                        {{ indexName: "{INDEX}", params: "hitsPerPage=5&page=0&filters=type%3ANew" }},
                        {{ indexName: "{INDEX}", params: "hitsPerPage=5&page=0&filters=condition%3ANew" }},
                    ]
                }})
            }});
            if (!resp.ok) return {{error: resp.status + ": " + await resp.text()}};
            const d = await resp.json();
            return (d.results || []).map(r => ({{
                params: r.params, nbHits: r.nbHits, error: r.message,
            }}));
        }}
        """)
        print(f"\nMulti-query results: {json.dumps(multi_result, indent=2)}")

        await context.close()
        await browser.close()


asyncio.run(main())
