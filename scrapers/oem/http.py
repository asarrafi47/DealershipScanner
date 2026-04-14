"""Shared HTTP helpers for OEM scrapers."""
from __future__ import annotations

import requests

from SCRAPING.constants import USER_AGENT


def oem_requests_session(*, timeout: int = 60, verify_ssl: bool = True) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    s.verify = verify_ssl
    return s
