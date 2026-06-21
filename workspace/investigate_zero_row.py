import asyncio, json, re, sys
sys.path.insert(0, '/Users/asarrafi/Projects/DealershipScanner')
from backend.utils.project_env import load_project_dotenv; load_project_dotenv()
from playwright.async_api import async_playwright

DEALERS = [
    ('volvocarschattanooga-com', 'Chattanooga Volvo', 'https://volvocarschattanooga.com'),
    ('audichattanooga-com', 'Audi Chattanooga', 'https://www.audichattanooga.com'),
    ('acuraofchattanooga-com', 'Acura of Chattanooga', 'https://www.acuraofchattanooga.com'),
    ('mercedesbenzatlong-com', 'Mercedes-Benz at Long', 'https://mercedesbenzatlong.com'),
    ('genesisatlongofchattanooga-com', 'Genesis of Chattanooga', 'https://genesisatlongofchattanooga.com'),
    ('kiaofchattanooga-com', 'Kia of Chattanooga', 'https://www.kiaofchattanooga.com'),
    ('porscheofchattanooga-com', 'Porsche of Chattanooga', 'https://porscheofchattanooga.com'),
]

INV_PATHS = ['/new-inventory/index.htm', '/inventory/', '/new-vehicles/', '/vehicles/', '/used-inventory/index.htm']

PLATFORM_HINTS = [
    'dealerinspire', 'dealeron', 'dealer.com', 'eprocess', 'foxdealer', 'sincro',
    'dealerfire', 'pixelmotion', 'cdk', 'reynolds', 'dominion', 'autotrader',
    'cars.com', 'vdp', 'srp', 'algolia', 'vue', 'react', 'angular',
    'nextjs', '__next', 'wordpress', 'squarespace', 'wix',
    'motortrend', 'trader.ca', 'dealersocket', 'activengage', 'dealer.com/widget',
    'ws-inv-data', 'getinventory', 'vhcliaa', 'prsnbaa', 'mvnalgolia',
    'diinventoryconfig', 'vehicle-facts.json', 'canonicallexicon',
    'sincrodigital', 'activix', 'gubagoo', 'tradepending', 'homenet',
    'ddc-', 'ddcwss', 'gforces', 'automotiveui', 'flick-fusion',
    'elead', 'tekion', 'serti', 'dealervault', 'autoipacket',
]

async def investigate_dealer(browser, dealer_id, name, base_url):
    api_urls = []
    all_json_urls = []
    context = await browser.new_context(viewport={'width': 1920, 'height': 1080},
                                         user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    page = await context.new_page()

    def on_response(resp):
        ct = (resp.headers.get('content-type') or '').lower()
        url = str(resp.url)
        if 'json' in ct:
            all_json_urls.append(url)
        if any(k in url.lower() for k in ('inventory', 'vehicle', 'srp', 'api', 'getinventory', 'vhcliaa', 'algolia')):
            api_urls.append(url)

    page.on('response', on_response)

    result = {
        'dealer_id': dealer_id, 'name': name, 'url': base_url,
        'script_srcs': [], 'api_urls': [], 'all_json_urls': [],
        'html_hints': [], 'inv_path_tried': None,
        'generator': None, 'title': None,
        'html_sample': '',
    }

    try:
        await page.goto(base_url, wait_until='domcontentloaded', timeout=18000)
        await asyncio.sleep(2.5)
        html = await page.content()
        result['title'] = await page.title()

        result['script_srcs'] = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.I)[:40]
        result['html_sample'] = html[:6000]

        low = html.lower()
        for hint in PLATFORM_HINTS:
            if hint in low:
                result['html_hints'].append(hint)

        gen = re.search(r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']+)["\']', html, re.I)
        if gen:
            result['generator'] = gen.group(1)

        result['api_urls'] = list(set(api_urls))[:20]
        result['all_json_urls'] = list(set(all_json_urls))[:20]

        # Try inventory paths
        for inv_path in INV_PATHS:
            inv_url = base_url.rstrip('/') + inv_path
            try:
                resp = await page.goto(inv_url, wait_until='domcontentloaded', timeout=14000)
                if resp and resp.status < 400:
                    await asyncio.sleep(2.0)
                    result['inv_path_tried'] = inv_path
                    result['inv_final_url'] = page.url
                    inv_html = await page.content()
                    low2 = inv_html.lower()
                    for hint in PLATFORM_HINTS:
                        key = hint + ':inv'
                        if hint in low2 and hint not in result['html_hints'] and key not in result['html_hints']:
                            result['html_hints'].append(key)
                    result['inv_html_sample'] = inv_html[:4000]
                    result['api_urls'] = list(set(api_urls))[:20]
                    result['all_json_urls'] = list(set(all_json_urls))[:20]
                    break
            except Exception as inv_e:
                result.setdefault('inv_errors', []).append(f'{inv_path}: {str(inv_e)[:80]}')
                continue

    except Exception as e:
        result['error'] = str(e)[:300]
    finally:
        await context.close()

    return result


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        results = []
        for dealer_id, name, url in DEALERS:
            print(f'Investigating {name} ({url})...')
            r = await investigate_dealer(browser, dealer_id, name, url)
            results.append(r)
            print(f'  title={r.get("title","")}')
            print(f'  hints={r.get("html_hints",[])}')
            print(f'  scripts={len(r.get("script_srcs",[]))} | api_urls={r.get("api_urls",[])}')
            print(f'  inv_path={r.get("inv_path_tried")} -> {r.get("inv_final_url","")}')
        await browser.close()

    with open('workspace/chattanooga_zero_row_investigation.json', 'w') as f:
        json.dump(results, f, indent=2)
    print('\nSaved workspace/chattanooga_zero_row_investigation.json')
    return results

asyncio.run(main())
