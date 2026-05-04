"""
Enhanced Playwright scraper for BMW dealerships with extended waiting times for maximum accuracy.
"""
import asyncio
import json
import logging
import time
import traceback
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, Locator
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Extended timeouts and waits for BMW dealerships
BMW_TIMEOUT_SETTINGS = {
    'page_load_timeout': 60000,  # 60 seconds for page loads
    'element_wait_timeout': 30000,  # 30 seconds for elements
    'max_wait_time': 120000,  # 2 minutes maximum wait time
    'idle_wait_time': 5000,  # 5 seconds idle time
    'retry_attempts': 3,
    'delay_between_attempts': 2000  # 2 seconds delay
}

# BMW-specific selectors and patterns
BMW_SELECTORS = {
    'vehicle_listings': [
        '[data-testid="vehicle-listing"]',
        'div.vehicle-card',
        'div.inventory-item',
        '.vehicle-item'
    ],
    'vehicle_detail': [
        '[data-testid="vehicle-detail"]',
        '.vehicle-details',
        '.inventory-detail'
    ],
    'dynamic_content': [
        '.dynamic-content',
        '[data-async-content]',
        '.load-more-container'
    ]
}

async def wait_for_bmw_content(page: Page, max_time: int = BMW_TIMEOUT_SETTINGS['max_wait_time']) -> bool:
    """
    Wait for BMW-specific content to load completely.
    """
    start_time = time.time()
    
    # Wait for initial content
    try:
        # Check for common BMW patterns
        await page.wait_for_load_state('load', timeout=BMW_TIMEOUT_SETTINGS['page_load_timeout'])
        
        # Wait for vehicle listings to appear
        for selector in BMW_SELECTORS['vehicle_listings']:
            try:
                await page.wait_for_selector(selector, timeout=BMW_TIMEOUT_SETTINGS['element_wait_timeout'])
                logger.info(f"Found vehicle listings with selector: {selector}")
                break
            except PlaywrightTimeoutError:
                continue
        
        # Wait for any dynamic content to load
        for selector in BMW_SELECTORS['dynamic_content']:
            try:
                await page.wait_for_selector(selector, timeout=BMW_TIMEOUT_SETTINGS['element_wait_timeout'])
                logger.info(f"Found dynamic content with selector: {selector}")
                # Wait extra time for dynamic content to process
                await asyncio.sleep(3)
                break
            except PlaywrightTimeoutError:
                continue
                
        # Additional long wait for BMW-specific data
        elapsed = time.time() - start_time
        if elapsed < 10:  # Wait at least 10 seconds for complex BMW layouts
            await asyncio.sleep(10)
            
        return True
        
    except Exception as e:
        logger.warning(f"Timeout waiting for BMW content: {e}")
        return False

async def is_bmw_dealership(url: str) -> bool:
    """
    Check if the dealership URL is a BMW dealership.
    """
    bmw_domains = [
        'bmw.com', 'bmw.ca', 'bmw.co.uk', 'bmw.de',
        'bmw.fr', 'bmw.it', 'bmw.es', 'bmw.nl',
        'bmw.be', 'bmw.pt', 'bmw.se', 'bmw.no',
        'bmw.fi', 'bmw.dk'
    ]
    
    try:
        parsed_url = urlparse(url)
        for domain in bmw_domains:
            if domain in parsed_url.netloc:
                return True
        return False
    except:
        return False

async def get_bmw_vehicle_data(page: Page, base_url: str, dealer_id: str, dealer_name: str, dealer_url: str) -> List[Dict]:
    """
    Enhanced BMW vehicle data extraction with extended waiting times.
    """
    vehicle_data = []
    
    try:
        # Wait for full BMW content to load
        content_ready = await wait_for_bmw_content(page, BMW_TIMEOUT_SETTINGS['max_wait_time'])
        if not content_ready:
            logger.warning("BMW content not fully loaded, continuing with partial data")
            
        # Get vehicle listings
        vehicles = []
        
        # Try multiple selectors for BMW inventory pages
        selectors = BMW_SELECTORS['vehicle_listings']
        for selector in selectors:
            try:
                elements = await page.query_selector_all(selector)
                if elements:
                    logger.info(f"Found {len(elements)} vehicles with selector: {selector}")
                    vehicles.extend(elements)
                    break
            except Exception:
                continue
                
        if not vehicles:
            # Fallback to broader selectors
            logger.info("Using broader selectors for BMW inventory")
            try:
                elements = await page.query_selector_all('div[data-testid*="vehicle"]')
                vehicles.extend(elements)
            except Exception:
                pass
                
        # Extract data from each vehicle element with extended time
        for i, vehicle_element in enumerate(vehicles):
            try:
                # Wait before extracting each vehicle to ensure full processing
                await asyncio.sleep(1)
                
                vehicle_data_point = await extract_bmw_vehicle_details(page, vehicle_element, base_url)
                if vehicle_data_point:
                    vehicle_data.append(vehicle_data_point)
                    
                # Be more patient between vehicles for BMW complex data
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.warning(f"Error extracting BMW vehicle {i}: {e}")
                continue
                
        return vehicle_data
        
    except Exception as e:
        logger.error(f"Error in BMW data extraction: {e}")
        logger.error(traceback.format_exc())
        return vehicle_data

async def extract_bmw_vehicle_details(page: Page, vehicle_element: Locator, base_url: str) -> Optional[Dict]:
    """
    Extract detailed vehicle information with extended processing time.
    """
    try:
        # Wait to ensure full element processing
        await asyncio.sleep(2)
        
        # Get basic vehicle information
        vehicle_info = {
            'url': base_url,
            'dealer_id': 'bmw_dealer',  # Placeholder, should be set properly
            'dealer_name': 'BMW Dealer',  # Placeholder, should be set properly
        }
        
        # Enhanced extraction logic
        try:
            # Get VIN if available
            vin_element = await vehicle_element.query_selector('[data-vin]')
            if vin_element:
                vin = await vin_element.get_attribute('data-vin')
                if vin:
                    vehicle_info['vin'] = vin
                    
            # Get vehicle details
            year_element = await vehicle_element.query_selector('[data-year]')
            if year_element:
                year = await year_element.get_attribute('data-year')
                if year:
                    vehicle_info['year'] = year
                    
            # Get make and model
            make_model = await vehicle_element.text_content()
            if make_model:
                # Enhanced parsing for BMW patterns
                if 'BMW' in make_model:
                    vehicle_info['make'] = 'BMW'
                    # Extract model from content
                    # This would need more sophisticated parsing based on BMW structure
                    
        except Exception as e:
            logger.warning(f"Error getting detailed vehicle info: {e}")
            
        return vehicle_info
        
    except Exception as e:
        logger.error(f"Error extracting BMW vehicle details: {e}")
        return None

async def bmw_optimized_browsing(page: Page, url: str, max_time: int = BMW_TIMEOUT_SETTINGS['max_wait_time']) -> None:
    """
    Extended browsing for BMW dealerships with patience for dynamic content.
    """
    start_time = time.time()
    
    try:
        # Navigate to the URL with extended timeout
        await page.goto(url, wait_until='networkidle', timeout=BMW_TIMEOUT_SETTINGS['page_load_timeout'])
        
        # Wait for BMW-specific elements to load with extended timing
        await wait_for_bmw_content(page, max_time)
        
        # Additional wait for complex BMW layouts
        await asyncio.sleep(3)
        
        # Scrolling to ensure all content loads
        scroll_positions = [0, 0.25, 0.5, 0.75, 1.0]
        for pos in scroll_positions:
            await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pos})")
            await asyncio.sleep(1)
            
        # Final wait for any last dynamic elements
        await asyncio.sleep(2)
        
        logger.info("BMW browsing completed with extended wait times")
        
    except Exception as e:
        logger.error(f"BMW browsing error: {e}")
        logger.error(traceback.format_exc())

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