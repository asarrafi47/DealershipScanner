"""
Extract Land Rover Chattanooga Algolia API key from maven-algolia WordPress plugin.
Then try to query Infiniti of Chattanooga using the same application credentials.
"""
import asyncio
import re
import json
import urllib.request
from playwright.async_api import async_playwright

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
APP_ID = "10APRXOTJR"


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA)
        page = await ctx.new_page()

        # Intercept requests to capture the actual Algolia API call
        algolia_key = [None]

        def on_request(r):
            if "algolia.net" in r.url or "algolia.io" in r.url:
                hdrs = dict(r.headers)
                key = hdrs.get("x-algolia-api-key") or hdrs.get("x-algolia-apikey")
                if key:
                    algolia_key[0] = key
                idx = re.search(r"/indexes/([^?/]+)", r.url)
                if idx:
                    print(f"Algolia request: {r.url[:150]}, key_header={key}")

        page.on("request", on_request)

        # Load Land Rover inventory page to trigger Algolia search
        print("Loading Land Rover inventory to capture Algolia key...")
        try:
            await page.goto(
                "https://www.landroverchattanooga.com/new-inventory/",
                wait_until="networkidle",
                timeout=35000,
            )
            await asyncio.sleep(6)
        except Exception as e:
            print(f"Warning: {e}")

        if algolia_key[0]:
            print(f"Got API key from request: {algolia_key[0][:8]}...")
        else:
            # Fallback: look in HTML / window vars
            html = await page.content()
            keys_in_html = re.findall(r"['\"]([a-f0-9]{32})['\"]", html)
            print(f"32-hex keys in HTML: {keys_in_html[:5]}")

            keys_from_window = await page.evaluate(
                """() => {
                const keys = [];
                const strs = JSON.stringify(window).match(/[a-f0-9]{32}/g) || [];
                return strs.slice(0, 5);
            }"""
            )
            print(f"Keys from window JSON: {keys_from_window}")

        # Also try fetching the algolia frontend JS directly for the key
        print("\nFetching maven-algolia frontend JS...")
        try:
            js_url = "https://www.landroverchattanooga.com/wp-content/plugins/maven-algolia/front/assets/scripts/algolia-frontend-min.js?ver=1781811803"
            result = await page.evaluate(
                f"""
                async () => {{
                    const r = await fetch('{js_url}');
                    const t = await r.text();
                    const keys = t.match(/['"\\`]([a-f0-9]{{32}})['"\\`]/g) || [];
                    const appCtx = (t.match(/.{{50}}10APRXOTJR.{{100}}/g) || []).slice(0,2);
                    return {{size: t.length, hex32_keys: keys.slice(0,5), appIdContext: appCtx}};
                }}
            """
            )
            print(f"JS file size: {result['size']}")
            print(f"Hex32 keys: {result['hex32_keys']}")
            print(f"AppID context: {result['appIdContext']}")
        except Exception as e:
            print(f"Error fetching JS: {e}")

        # If we have a key, try Infiniti index guesses
        api_key = algolia_key[0]
        if not api_key:
            # Try known keys from the hex patterns
            html = await page.content()
            hex_keys = re.findall(r"[\"']([a-f0-9]{32})[\"']", html)
            api_key = hex_keys[0] if hex_keys else None
            if api_key:
                print(f"Using key from HTML: {api_key[:8]}...")

        if api_key:
            print(f"\n=== Trying Infiniti index guesses with app_id={APP_ID} ===")
            suffix_guesses = ["sbm0326", "sbm0120", "sbm0521", "sbm0623", ""]
            for suffix in suffix_guesses:
                for slug in [
                    "infinitichattanooga",
                    "infiniti-chattanooga",
                    "infinitichattanoogaservice",
                    "chattanooga-infiniti",
                ]:
                    if suffix:
                        index = f"{slug}-{suffix}_production_inventory"
                    else:
                        index = f"{slug}_production_inventory"
                    try:
                        result = await page.evaluate(
                            f"""
                            async () => {{
                                const r = await fetch('https://{APP_ID}-dsn.algolia.net/1/indexes/{index}/query', {{
                                    method: 'POST',
                                    headers: {{
                                        'X-Algolia-Application-Id': '{APP_ID}',
                                        'X-Algolia-API-Key': '{api_key}',
                                        'Content-Type': 'application/json'
                                    }},
                                    body: JSON.stringify({{params: 'hitsPerPage=3'}})
                                }});
                                const data = await r.json();
                                return {{status: r.status, hits: (data.hits||[]).length, total: data.nbHits, error: data.message||data.status}};
                            }}
                        """
                        )
                        if result.get("total", 0) > 0 or result.get("hits", 0) > 0:
                            print(f"HIT! {index}: {result}")
                        elif result.get("error") and "does not exist" not in str(result.get("error", "")):
                            print(f"{index}: {result}")
                        # Don't print 404 "index does not exist" results
                    except Exception as e:
                        print(f"{index}: {e}")
        else:
            print("No API key found")

        await ctx.close()
        await browser.close()


asyncio.run(main())
