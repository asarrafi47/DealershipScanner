"""
Capture all JSON network requests from Kia of Cleveland SRP to find the inventory API.
"""
import asyncio
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

        json_requests = []

        async def on_response(response):
            url = response.url
            ct = (response.headers.get("content-type") or "").lower()
            if "json" in ct or url.endswith(".json"):
                try:
                    body = await response.body()
                    text = body.decode("utf-8", errors="replace")
                    if len(text) > 50:  # skip tiny responses
                        # Try to parse as JSON
                        try:
                            data = json.loads(text)
                            json_requests.append({
                                "url": url,
                                "size": len(text),
                                "type": type(data).__name__,
                                "preview": text[:300],
                            })
                        except Exception:
                            # Not valid JSON but content-type says json
                            json_requests.append({
                                "url": url,
                                "size": len(text),
                                "type": "invalid_json",
                                "preview": text[:300],
                            })
                except Exception as e:
                    pass

        page.on("response", on_response)

        print("Loading Kia SRP...")
        await page.goto("https://www.kiaofcleveland.com/new-inventory/index.htm",
                        wait_until="domcontentloaded", timeout=35_000)

        final_url = page.url
        print(f"Final URL after redirect: {final_url}")

        # Wait for dynamic content
        await asyncio.sleep(8)

        # Scroll to trigger lazy loading
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(3)

        print(f"\nCaptured {len(json_requests)} JSON responses:")
        for r in json_requests:
            print(f"  [{r['size']}] {r['type']} — {r['url'][:100]}")
            print(f"    Preview: {r['preview'][:150]}")
            print()

        # Also check what the resrc paths actually return
        print("\n--- Testing resrc paths directly ---")
        for path in [
            "/resrc/inventory/?tp=new",
            "/resrc/inventory/?tp=used",
            "/resrc/vehicleviews/getVehicleViews/?tp=new",
            "/resrc/vehicleviews/getVehicleViewCounts/?tp=new",
            "/resrc/inventory/search_filters/?flag_new=1",
        ]:
            url = "https://www.kiaofcleveland.com" + path
            result = await page.evaluate(f"""
            async () => {{
                try {{
                    const r = await fetch({json.dumps(url)}, {{credentials: 'include', headers: {{'X-Requested-With': 'XMLHttpRequest'}}}});
                    const text = await r.text();
                    return {{status: r.status, preview: text.substring(0, 300)}};
                }} catch(e) {{ return {{error: e.toString()}}; }}
            }}
            """)
            print(f"  {path} → {result}")

        await context.close()
        await browser.close()


asyncio.run(main())
