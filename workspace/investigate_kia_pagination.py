"""
Investigate Kia SRP pagination to understand how to get full inventory.
"""
import asyncio
import json
import re

from playwright.async_api import async_playwright

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def extract_jsonld_vehicles(html: str) -> list[dict]:
    """Extract Vehicle JSON-LD blocks from HTML."""
    vehicles = []
    blocks = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S
    )
    for block in blocks:
        try:
            data = json.loads(block)
            items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                if isinstance(item, dict) and item.get("@type") in ("Vehicle", "Car"):
                    vehicles.append(item)
        except Exception:
            pass
    return vehicles


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # First: check total count from the first page
        context = await browser.new_context(user_agent=UA)
        page = await context.new_page()

        print("Loading Kia SRP page 1...")
        await page.goto("https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new",
                        wait_until="domcontentloaded", timeout=35_000)
        await asyncio.sleep(3)
        html = await page.content()

        vehicles_p1 = extract_jsonld_vehicles(html)
        print(f"Page 1: {len(vehicles_p1)} vehicles")

        # Look for total count indicator
        total_matches = re.findall(r'(?:showing|found|of|total)[^0-9]*(\d+)[^0-9]+(?:vehicle|result|listing)', html, re.I)
        print(f"Count patterns found: {total_matches[:5]}")

        # Check pagination next button
        next_btn = await page.query_selector("a.next, a[rel='next'], .pagination-next, button.next, [aria-label='Next']")
        print(f"Next page button: {'found' if next_btn else 'not found'}")

        # Try different page params
        for pg_url in [
            "https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new&pg=2",
            "https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new&page=2",
            "https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new&start=12",
            "https://www.kiaofcleveland.com/search/new-kia-cleveland-tn/?cy=37311&tp=new&ps=100",
        ]:
            try:
                p2_context = await browser.new_context(user_agent=UA)
                p2 = await p2_context.new_page()
                await p2.goto(pg_url, wait_until="domcontentloaded", timeout=20_000)
                await asyncio.sleep(2)
                h2 = await p2.content()
                v2 = extract_jsonld_vehicles(h2)
                vins_p2 = [v.get("vehicleIdentificationNumber", "") for v in v2]
                vins_p1 = [v.get("vehicleIdentificationNumber", "") for v in vehicles_p1]
                new_count = len(set(vins_p2) - set(vins_p1))
                final_url = p2.url
                print(f"  {pg_url[-60:]} → {len(v2)} vehicles ({new_count} new), final_url={final_url[-60:]}")
                await p2_context.close()
            except Exception as e:
                print(f"  {pg_url[-60:]} → ERROR: {e}")

        # Print first vehicle details to understand the schema
        if vehicles_p1:
            v = vehicles_p1[0]
            print(f"\nFirst vehicle full structure:")
            print(json.dumps(v, indent=2)[:2000])

        await context.close()
        await browser.close()


asyncio.run(main())
