"""
Investigate Kia vehicleviews API and capture actual inventory.
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

        # Capture vehicleviews requests
        captured = []

        async def on_response(response):
            url = response.url
            if "vehicleview" in url.lower() or "getVehicle" in url:
                try:
                    body = await response.body()
                    text = body.decode("utf-8", errors="replace")
                    captured.append({"url": url, "body": text[:2000]})
                    print(f"  CAPTURED: {url[:100]}")
                    print(f"  Body preview: {text[:300]}")
                except Exception as e:
                    captured.append({"url": url, "error": str(e)})

        page.on("response", on_response)

        print("Loading Kia SRP (watching for vehicleviews requests)...")
        await page.goto("https://www.kiaofcleveland.com/new-inventory/index.htm",
                        wait_until="networkidle", timeout=45_000)

        final_url = page.url
        print(f"Final URL: {final_url}")
        await asyncio.sleep(5)

        print(f"\nCaptured {len(captured)} vehicleviews responses")

        # Try to extract the actual SRP parameters from the page URL and HTML
        srp_params = {}
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(final_url)
            srp_params = parse_qs(parsed.query, keep_blank_values=True)
            print(f"SRP URL params: {srp_params}")
        except Exception:
            pass

        # Extract zip code from URL or page HTML
        cy = srp_params.get("cy", [""])[0]
        print(f"Detected cy (zip): {cy!r}")

        # Try various parameter combinations
        base = "https://www.kiaofcleveland.com"
        test_paths = [
            f"/resrc/vehicleviews/getVehicleViews/?tp=new&cy={cy}",
            f"/resrc/vehicleviews/getVehicleViews/?tp=new&cy={cy}&pg=1&ps=50",
            f"/resrc/vehicleviews/getVehicleViews/?tp=new&pg=1",
            f"/resrc/vehicleviews/getVehicleViews/?tp=new&ps=50",
            f"/resrc/vehicleviews/getVehicleViews/?tp=all&cy={cy}",
            "/resrc/vehicleviews/getVehicleViews/",
        ]

        print("\n--- Testing vehicleviews paths ---")
        for path in test_paths:
            url = base + path
            result = await page.evaluate(f"""
            async () => {{
                try {{
                    const r = await fetch({json.dumps(url)}, {{
                        credentials: 'include',
                        headers: {{'X-Requested-With': 'XMLHttpRequest'}}
                    }});
                    const text = await r.text();
                    return {{status: r.status, len: text.length, preview: text.substring(0, 500)}};
                }} catch(e) {{ return {{error: e.toString()}}; }}
            }}
            """)
            preview = result.get("preview", "")
            status = result.get("status", "?")
            length = result.get("len", 0)
            print(f"  {path}")
            print(f"    status={status} len={length} preview={preview[:200]}")
            print()

        # Also check what initilizes the SRP — look for vehicle data in window object
        window_data = await page.evaluate("""
        () => {
            const keys = Object.keys(window).filter(k =>
                k.toLowerCase().includes('vehicle') ||
                k.toLowerCase().includes('inventory') ||
                k.toLowerCase().includes('kia')
            );
            const result = {};
            for (const k of keys) {
                try {
                    result[k] = JSON.stringify(window[k]).substring(0, 300);
                } catch(e) {}
            }
            return result;
        }
        """)
        if window_data:
            print(f"\nRelevant window vars: {list(window_data.keys())[:10]}")
            for k, v in list(window_data.items())[:5]:
                print(f"  {k}: {v[:150]}")

        # Look for vehicle IDs in the page HTML (they appear on the SRP)
        html = await page.content()
        import re
        vehicle_ids = re.findall(r'"vehicle_id"\s*:\s*(\d+)', html)[:10]
        print(f"\nVehicle IDs in HTML: {vehicle_ids[:10]}")

        await context.close()
        await browser.close()


asyncio.run(main())
