"""
Extract vehicle data from Kia SRP JSON-LD and HTML structure.
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

        print("Loading Kia SRP...")
        try:
            await page.goto("https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new",
                            wait_until="domcontentloaded", timeout=35_000)
        except Exception as e:
            print(f"Warning: {e}")

        await asyncio.sleep(5)
        html = await page.content()
        print(f"HTML size: {len(html)}")

        # Extract ALL JSON-LD blocks
        jsonld_blocks = re.findall(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.S
        )
        print(f"JSON-LD blocks: {len(jsonld_blocks)}")

        vehicles = []
        for i, block in enumerate(jsonld_blocks):
            try:
                data = json.loads(block)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") in ("Vehicle", "Car"):
                            vehicles.append(item)
                elif isinstance(data, dict):
                    if data.get("@type") in ("Vehicle", "Car"):
                        vehicles.append(data)
                    elif data.get("@type") == "@graph":
                        for item in data.get("@graph", []):
                            if isinstance(item, dict) and item.get("@type") in ("Vehicle", "Car"):
                                vehicles.append(item)
            except Exception as e:
                print(f"  Block {i} parse error: {e} — {block[:100]}")

        print(f"Vehicle JSON-LD blocks found: {len(vehicles)}")
        if vehicles:
            print(f"\nFirst vehicle keys: {list(vehicles[0].keys())}")
            print(f"\nFirst vehicle: {json.dumps(vehicles[0], indent=2)[:1500]}")

        # Also look for eProcess vehicle cards in HTML using data attributes
        # Pattern: data-vehicle or data-inventory
        data_vehicle = re.findall(r'data-vehicle[^=]*=["\']({[^"\']+})["\']', html)
        print(f"\ndata-vehicle attributes: {len(data_vehicle)}")
        if data_vehicle:
            print(f"First: {data_vehicle[0][:300]}")

        # Look for vehicle JSON arrays in script tags
        script_tags = re.findall(r'<script[^>]*>(.*?)</script>', html, re.S)
        print(f"\nScript tags: {len(script_tags)}")

        # Search for JSON with vin keys
        vin_pattern = re.compile(r'"vin"\s*:\s*"([A-HJ-NPR-Z0-9]{17})"', re.I)
        all_vins = vin_pattern.findall(html)
        print(f"Real VINs (17-char valid format) in HTML: {len(all_vins)}")
        print(f"Sample VINs: {all_vins[:10]}")

        # Look for vehicle card containers
        # Many DealerEProcess sites use div.vehicle-card or similar
        vehicle_count_js = await page.evaluate("""
        () => {
            const cards = document.querySelectorAll('[data-vehicle-id], .vehicle-card, .inventory-listing, [id^="vehicle-"]');
            const result = {
                count: cards.length,
                firstId: cards[0] ? cards[0].getAttribute('data-vehicle-id') || cards[0].id : null,
                sample: cards[0] ? cards[0].outerHTML.substring(0, 500) : null,
            };
            return result;
        }
        """)
        print(f"\nVehicle card elements: {vehicle_count_js}")

        # Check for a JSON array in a script variable
        for pattern, name in [
            (r'window\.__INITIAL_STATE__\s*=\s*({.+?})\s*;', 'INITIAL_STATE'),
            (r'window\.inventory\s*=\s*(\[.+?\])\s*;', 'window.inventory'),
            (r'var\s+inventory\s*=\s*(\[.+?\])\s*;', 'var inventory'),
            (r'"vehicles"\s*:\s*(\[.+?\])', 'vehicles array'),
            (r'"inventory"\s*:\s*(\[.+?\])', 'inventory array'),
        ]:
            matches = re.findall(pattern, html, re.S)
            if matches:
                print(f"\nFound {name}: {matches[0][:300]}")

        await context.close()
        await browser.close()


asyncio.run(main())
