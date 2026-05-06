#!/usr/bin/env python3
"""
Search web for dealership addresses.
Adds 'address_source' field to each dealer in dealers.json.
Output: dealers_with_addresses.json
"""

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path
from html.parser import HTMLParser


class GoogleResultParser(HTMLParser):
    """Extract first search result URL from Google HTML."""

    def __init__(self):
        super().__init__()
        self.url = None
        self.in_result = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href" and value.startswith("/url?q="):
                    try:
                        url = urllib.parse.unquote(value.split("q=")[1].split("&")[0])
                        if url and not url.startswith("http://webcache"):
                            self.url = url
                    except:
                        pass


def search_address(dealer_name):
    """Search Google for dealership address. Return first result URL or None."""
    try:
        query = f"{dealer_name} address"
        encoded = urllib.parse.quote(query)
        url = f"https://www.google.com/search?q={encoded}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        }
        req = urllib.request.Request(url, headers=headers)

        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode("utf-8")
            parser = GoogleResultParser()
            parser.feed(html)
            return parser.url
    except Exception as e:
        print(f"  Error searching {dealer_name}: {e}")
        return None


def main():
    input_file = Path("dealers.json")
    output_file = Path("dealers_with_addresses.json")

    if not input_file.exists():
        print(f"Error: {input_file} not found")
        return

    print(f"Reading {input_file}...")
    with open(input_file) as f:
        dealers = json.load(f)

    print(f"Found {len(dealers)} dealers. Searching addresses...")
    print("(This may take a few minutes due to rate limiting)\n")

    for i, dealer in enumerate(dealers, 1):
        name = dealer.get("name", "Unknown")
        print(f"[{i}/{len(dealers)}] {name}...", end="", flush=True)

        address_url = search_address(name)
        if address_url:
            dealer["address_source"] = address_url
            print(f" ✓")
        else:
            print(f" ✗")

        # Rate limiting - be nice to Google
        time.sleep(2)

    print(f"\nWriting results to {output_file}...")
    with open(output_file, "w") as f:
        json.dump(dealers, f, indent=2)

    print(f"Done! Processed {len(dealers)} dealers.")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()
