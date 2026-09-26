"""BMW dealership manifest hints.

Only ``enhance_scraping_for_bmw_dealerships`` survives (2026-09-26): the five
Playwright ``Page`` helpers that used to live here had no callers anywhere and
made importing the orchestrator require a browser install.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Manifest hints for BMW dealerships (kept for the scan-time dealer dict only)
BMW_TIMEOUT_SETTINGS = {"max_wait_time": 120000}

def enhance_scraping_for_bmw_dealerships(dealers: List[Dict]) -> List[Dict]:
    """
    Modify dealer list to include BMW-specific optimization flags.
    """
    enhanced_dealers = []
    
    for dealer in dealers:
        # Check if it's a BMW dealership
        if 'bmw' in dealer.get('name', '').lower() or 'bmw' in dealer.get('url', '').lower():
            dealer['optimize_for'] = 'bmw'
            dealer['extended_timeout'] = True
            dealer['max_wait_time'] = BMW_TIMEOUT_SETTINGS['max_wait_time']
            dealer['dynamic_processing'] = True
        enhanced_dealers.append(dealer)
        
    return enhanced_dealers