"""
Claude Haiku inline VDP extraction.

Called from vdp.py during every VDP visit when key fields (price, condition,
colors, packages) are missing after JS/network extraction. Sends visible page
text to Claude Haiku and merges the returned JSON into the vehicle row.

Runs asynchronously inside the Playwright VDP worker — no separate post-scan pass.
Disabled by setting SCANNER_CLAUDE_VDP=0 or when ANTHROPIC_API_KEY is absent.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

_MODEL = "claude-haiku-4-5-20251001"
_MAX_PAGE_CHARS = 6000
_FIELDS_WANTED = (
    "price", "condition", "exterior_color", "interior_color",
    "packages", "description", "transmission", "drivetrain",
    "fuel_type", "cylinders", "body_style",
)

_SYSTEM = """\
You extract structured car listing data from raw dealer website text.
Return ONLY valid minified JSON with exactly these keys. No markdown fences, no prose, no explanation.
Use null for any field not found or uncertain. Never invent values not present in the text.

OUTPUT FORMAT (all keys required):
{"price":32999,"condition":"Used","exterior_color":"Pearl White Tricoat","interior_color":"Jet Black Leather","packages":["Driver Assistance Package","Panoramic Sunroof"],"description":"One-owner certified...","transmission":"8-Speed Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":4,"body_style":"SUV"}

FIELD EXTRACTION RULES:

price (integer, USD, no symbols):
  Use the internet/sale price, not MSRP or sticker. If multiple prices shown, use lowest selling price.
  Strip all $ signs and commas. Ignore monthly payment amounts, "starting at", "as low as".
  Return null if only MSRP or only finance payments shown. Valid range: 500 to 10000000.
  "$29,995" → 29995 | "32,500" → 32500 | "$299/mo" → null | "MSRP $45,000" with no sale price → null

condition (exactly "New", "Used", or "Certified Pre-Owned"):
  New: unregistered, 0 miles, factory-fresh, in-transit, dealer demo with <500 miles.
  Certified Pre-Owned: CPO, certified, manufacturer-certified, factory-certified used.
  Used: all other pre-owned, pre-driven, second-hand. If both "certified" and "used" → "Certified Pre-Owned".
  Return null only if completely absent from page.

exterior_color (full color name exactly as listed):
  Use dealer-listed name including metallic/pearl/tricoat/tinted clearcoat suffixes.
  Never truncate: "Midnight Black Metallic" not "Black". Return null if absent.

interior_color (full interior color and material):
  Include material if listed: "Black Leather", "Tan Perforated Leather", "Gray Cloth".
  Do not infer from trim level. Return null if absent.

packages (JSON array of strings, never null — use [] if none):
  Named option packages, technology groups, appearance packages, dealer-installed accessories.
  Exclude standard base-trim features. Include: "Cold Weather Package", "Navigation System", "Tow Package".
  Return [] if no packages found.

description (string, max 300 chars, or null):
  Main vehicle description paragraph written by dealer. Prefer marketing text over spec lists.
  Truncate at word boundary under 300 chars. Return null if no descriptive text.

transmission (full description string or null):
  Include speed count and type: "8-Speed Automatic", "6-Speed Manual", "CVT", "eCVT", "7-Speed PDK".
  "Automatic" alone is acceptable if speed count absent. Never return drivetrain as transmission.

drivetrain (exactly "FWD", "AWD", "RWD", "4WD", or null):
  FWD: front-wheel drive. AWD: all-wheel drive, xDrive, 4MATIC, Quattro, SH-AWD, e-AWD.
  RWD: rear-wheel drive, sDrive. 4WD: 4x4, four-wheel drive (trucks with transfer case).
  Return null if not mentioned.

fuel_type (exactly "Gasoline", "Hybrid", "Plug-In Hybrid", "Electric", "Diesel", or null):
  Gasoline: gas, petrol, unleaded. Hybrid: HEV, mild hybrid, self-charging (no plug).
  Plug-In Hybrid: PHEV, plug-in hybrid, rechargeable hybrid. Electric: EV, BEV, battery electric.
  Diesel: diesel, TDI, BlueTEC. Return null if not mentioned.

cylinders (integer or null):
  From "4-cylinder", "V6", "V8", "inline-6", "4-cyl". Return 0 for confirmed pure electric (no combustion).
  Valid: 0, 3, 4, 5, 6, 8, 10, 12. Return null if absent or ambiguous.

body_style (exactly one of: "Sedan","SUV","Truck","Coupe","Hatchback","Wagon","Minivan","Convertible","Van", or null):
  Sedan: 4-door saloon. SUV: sport utility, crossover, CUV. Truck: pickup.
  Coupe: 2-door, fastback. Hatchback: 3/5-door hatch, liftback. Wagon: estate, touring, avant.
  Minivan: minivan, people mover. Convertible: cabriolet, roadster, spider. Van: cargo/transit/sprinter.

--- EXTRACTION EXAMPLES ---

Example 1:
Page: "2023 Toyota Camry XSE V6 | Stock #TC4521 | Internet Price: $34,488 MSRP: $37,290 | Condition: Certified Pre-Owned | Mileage: 18,432 | Exterior: Midnight Black Metallic | Interior: Black SofTex | Engine: 3.5L V6 | Transmission: 8-Speed Automatic | Drivetrain: FWD | Fuel: Regular Unleaded | Packages: XSE V6 Premium Package, Blind Spot Monitor, Rear Cross Traffic Alert | This stunning Camry XSE V6 brings sportiness and refinement together. Toyota Certified with 12-month/12,000-mile warranty."
Output: {"price":34488,"condition":"Certified Pre-Owned","exterior_color":"Midnight Black Metallic","interior_color":"Black SofTex","packages":["XSE V6 Premium Package","Blind Spot Monitor","Rear Cross Traffic Alert"],"description":"This stunning Camry XSE V6 brings sportiness and refinement together. Toyota Certified with 12-month/12,000-mile warranty.","transmission":"8-Speed Automatic","drivetrain":"FWD","fuel_type":"Gasoline","cylinders":6,"body_style":"Sedan"}

Example 2:
Page: "2024 BMW X5 xDrive40i Certified Pre-Owned | Our Price: $67,995 | 12,881 Miles | Alpine White | Cognac Nevada Leather | 3.0L TwinPower Turbo Inline 6-Cylinder | Steptronic 8-Speed Automatic | xDrive All-Wheel Drive | Premium Fuel | Packages: Premium Package, Driving Assistance Professional Package, Panoramic Sky Lounge LED Roof, Harman Kardon Surround Sound | Meticulously maintained X5 with xDrive40i powertrain delivering 335 horsepower."
Output: {"price":67995,"condition":"Certified Pre-Owned","exterior_color":"Alpine White","interior_color":"Cognac Nevada Leather","packages":["Premium Package","Driving Assistance Professional Package","Panoramic Sky Lounge LED Roof","Harman Kardon Surround Sound"],"description":"Meticulously maintained X5 with xDrive40i powertrain delivering 335 horsepower.","transmission":"8-Speed Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":6,"body_style":"SUV"}

Example 3:
Page: "PRE-OWNED 2022 Ford F-150 XLT | Sale Price: $42,500 | SuperCrew Cab 4X4 | 5.0L V8 | 10-Speed Automatic | 31,204 miles | Rapid Red Metallic Tinted Clearcoat | Medium Light Slate w/ Unique Cloth | Packages: XLT Sport Appearance Package, Tow Package 11600 lbs Max, FX4 Off-Road Package, Bed Liner, Running Boards, Tonneau Cover | Work-ready F-150 loaded for capability and style."
Output: {"price":42500,"condition":"Used","exterior_color":"Rapid Red Metallic Tinted Clearcoat","interior_color":"Medium Light Slate w/ Unique Cloth","packages":["XLT Sport Appearance Package","Tow Package 11600 lbs Max","FX4 Off-Road Package","Bed Liner","Running Boards","Tonneau Cover"],"description":"Work-ready F-150 loaded for capability and style.","transmission":"10-Speed Automatic","drivetrain":"4WD","fuel_type":"Gasoline","cylinders":8,"body_style":"Truck"}

Example 4:
Page: "2023 Tesla Model Y Long Range AWD | Delivery Price: $51,990 | New | 0 Miles | Pearl White Multi-Coat | All Black Interior | Dual Motor Electric | All-Wheel Drive | 330 miles EPA range | Autopilot included | Glass Roof, 20-inch Induction Wheels, Premium Connectivity"
Output: {"price":51990,"condition":"New","exterior_color":"Pearl White Multi-Coat","interior_color":"All Black","packages":["Autopilot","Glass Roof","20-inch Induction Wheels","Premium Connectivity"],"description":"Dual Motor Electric AWD. 330 miles EPA estimated range. Autopilot, Glass Roof, Premium Connectivity included.","transmission":"Automatic","drivetrain":"AWD","fuel_type":"Electric","cylinders":0,"body_style":"SUV"}

Example 5:
Page: "2021 Honda Accord Hybrid EX-L | $27,888 | Used | 29,341 miles | 1 Owner | Sonic Gray Pearl | Gray Leather Interior | 2.0L Atkinson 4-Cyl + Electric | e-CVT | Front Wheel Drive | 48/48 MPG | Honda Sensing, Wireless Apple CarPlay, Heated Front Seats, Power Moonroof"
Output: {"price":27888,"condition":"Used","exterior_color":"Sonic Gray Pearl","interior_color":"Gray Leather","packages":["Honda Sensing","Wireless Apple CarPlay","Heated Front Seats","Power Moonroof"],"description":"1 Owner. 2.0L Atkinson Hybrid eCVT FWD. 48/48 MPG. Honda Sensing, CarPlay, Heated Seats, Moonroof.","transmission":"eCVT","drivetrain":"FWD","fuel_type":"Hybrid","cylinders":4,"body_style":"Sedan"}

Example 6:
Page: "New 2024 Mercedes-Benz GLE 350 4MATIC SUV | Now: $71,100 MSRP: $72,900 | Selenite Grey Metallic | Macchiato Beige MB-Tex | 3.5L Biturbo V6 | 9G-TRONIC 9-Speed Automatic | 4MATIC AWD | Packages: Premium Package, Parking Assistance Package, Panorama Sunroof, 20-in AMG Wheels"
Output: {"price":71100,"condition":"New","exterior_color":"Selenite Grey Metallic","interior_color":"Macchiato Beige MB-Tex","packages":["Premium Package","Parking Assistance Package","Panorama Sunroof","20-in AMG Wheels"],"description":null,"transmission":"9-Speed Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":6,"body_style":"SUV"}

Example 7:
Page: "2020 Chevrolet Corvette Stingray 3LT Coupe | $72,500 | Pre-Owned | 8,812 Miles | Sebring Orange Tintcoat | Natural w/ Suede Microfiber | 6.2L V8 495HP | 8-Speed Dual-Clutch Automatic | Rear Wheel Drive | Z51 Performance Package, NPP Dual-Mode Exhaust, Front Lift, GT2 Bucket Seats | Stunning low-mileage Corvette with factory warranty remaining."
Output: {"price":72500,"condition":"Used","exterior_color":"Sebring Orange Tintcoat","interior_color":"Natural w/ Suede Microfiber","packages":["Z51 Performance Package","NPP Dual-Mode Exhaust","Front Lift","GT2 Bucket Seats"],"description":"Stunning low-mileage Corvette with factory warranty remaining.","transmission":"8-Speed Dual-Clutch Automatic","drivetrain":"RWD","fuel_type":"Gasoline","cylinders":8,"body_style":"Coupe"}

Example 8:
Page: "2022 Jeep Wrangler Unlimited Sahara 4xe | Sale Price: $48,750 | Used | 22,190 Miles | Sarge Green | Black Interior | 2.0L Turbo 4-Cyl + Electric | 8-Speed Automatic | Command-Trac 4WD | Plug-In Hybrid | 22 Mi Electric Range | Freedom Top Removable Hardtop, Sky One-Touch Power Top, Heated Seats"
Output: {"price":48750,"condition":"Used","exterior_color":"Sarge Green","interior_color":"Black","packages":["Freedom Top Removable Hardtop","Sky One-Touch Power Top","Heated Seats"],"description":"2.0L Turbo 4-Cyl Plug-In Hybrid. 22 Miles Electric Range. Command-Trac 4WD System.","transmission":"8-Speed Automatic","drivetrain":"4WD","fuel_type":"Plug-In Hybrid","cylinders":4,"body_style":"SUV"}

Example 9:
Page: "2023 Porsche Cayenne GTS Coupe | $138,900 | New | Carmine Red | Black Race-Tex/Leather | 4.0L Twin-Turbo V8 460hp | 8-Speed Tiptronic S | Porsche Traction Management AWD | Sport Design Package, Carbon Ceramic Brakes, Panoramic Roof System, Bose Surround Sound, Sport Exhaust System"
Output: {"price":138900,"condition":"New","exterior_color":"Carmine Red","interior_color":"Black Race-Tex/Leather","packages":["Sport Design Package","Carbon Ceramic Brakes","Panoramic Roof System","Bose Surround Sound","Sport Exhaust System"],"description":"4.0L Twin-Turbo V8 460hp. Porsche Traction Management AWD. Sport Exhaust included.","transmission":"8-Speed Tiptronic S Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":8,"body_style":"Coupe"}

Example 10:
Page: "2019 Audi A4 Premium Plus quattro | $26,988 | Pre-Owned | 41,238 mi | Florett Silver Metallic | Rock Gray w/ Pearl Silver Stitching | 2.0L TFSI Turbo 4-Cyl | 7-Speed S tronic Dual-Clutch | quattro AWD | Cold Weather Package, Driver Assistance Package, Bang & Olufsen Sound System | Clean one-owner with full service history."
Output: {"price":26988,"condition":"Used","exterior_color":"Florett Silver Metallic","interior_color":"Rock Gray w/ Pearl Silver Stitching","packages":["Cold Weather Package","Driver Assistance Package","Bang & Olufsen Sound System"],"description":"Clean one-owner Audi with full service history available.","transmission":"7-Speed S tronic Dual-Clutch Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":4,"body_style":"Sedan"}

Example 11:
Page: "2024 Hyundai Tucson Plug-In Hybrid SEL | Dealer Price: $36,290 | New | Shimmering Silver | Black w/ Gray Leatherette | 1.6L Turbo GDI 4-Cyl + Electric | 6-Speed Automatic | HTRAC AWD | 33 Miles Electric Range | 10.25 Navigation, Wireless Charging, BlueLink — no additional packages"
Output: {"price":36290,"condition":"New","exterior_color":"Shimmering Silver","interior_color":"Black w/ Gray Leatherette","packages":[],"description":"1.6L Turbo GDI Plug-In Hybrid. HTRAC AWD. 33 Miles Electric Range. Navigation and Wireless Charging standard.","transmission":"6-Speed Automatic","drivetrain":"AWD","fuel_type":"Plug-In Hybrid","cylinders":4,"body_style":"SUV"}

Example 12:
Page: "2024 Mercedes-Benz EQS 450+ Sedan | EV Price: $94,500 MSRP $104,400 | New | 121 test-drive miles | MANUFAKTUR Diamond White Bright | Silk Beige/Biscuit Nappa Leather | Electric Motor 329HP | Single Speed Automatic | Rear-Wheel Drive | 350 Mile Range | Premium Package, Burmester Surround Sound, Rear Axle Steering, MBUX Hyperscreen, Augmented Reality Navigation"
Output: {"price":94500,"condition":"New","exterior_color":"MANUFAKTUR Diamond White Bright","interior_color":"Silk Beige/Biscuit Nappa Leather","packages":["Premium Package","Burmester Surround Sound","Rear Axle Steering","MBUX Hyperscreen","Augmented Reality Navigation"],"description":"Electric Motor 329HP. 350 Mile Range. MBUX Hyperscreen, Burmester Sound, Rear Axle Steering.","transmission":"Single Speed Automatic","drivetrain":"RWD","fuel_type":"Electric","cylinders":0,"body_style":"Sedan"}

Example 13:
Page: "2022 RAM 1500 Laramie Crew Cab 4x4 | $51,999 Internet Special | Used | 34,100 miles | Diamond Black Crystal Pearl | Mountain Brown/Light Frost Beige Leather | 5.7L HEMI V8 395HP | 8-Speed Automatic | 4x4 Part-Time | Packages: Laramie Level 2 Equipment Group, Blind Spot and Cross Path, Trailer Brake Controller, Night Edition"
Output: {"price":51999,"condition":"Used","exterior_color":"Diamond Black Crystal Pearl","interior_color":"Mountain Brown/Light Frost Beige Leather","packages":["Laramie Level 2 Equipment Group","Blind Spot and Cross Path","Trailer Brake Controller","Night Edition"],"description":"5.7L HEMI V8 395HP, 8-Speed Auto, 4x4. 34,100 miles.","transmission":"8-Speed Automatic","drivetrain":"4WD","fuel_type":"Gasoline","cylinders":8,"body_style":"Truck"}

Example 14:
Page: "2024 Kia Carnival SX Prestige | $47,875 | New | Ebony Black | Nappa Leather Black | 3.5L V6 292HP | 8-Speed Automatic | FWD | SX Prestige Package, Dual Sunroof, Surround View Monitor, HUD, Wireless Phone Charger, 8-Passenger Seating"
Output: {"price":47875,"condition":"New","exterior_color":"Ebony Black","interior_color":"Nappa Leather Black","packages":["SX Prestige Package","Dual Sunroof","Surround View Monitor","HUD","Wireless Phone Charger"],"description":"3.5L V6 292HP, 8-Speed Automatic, FWD. 8-Passenger. Dual Sunroof, HUD, Surround View Monitor.","transmission":"8-Speed Automatic","drivetrain":"FWD","fuel_type":"Gasoline","cylinders":6,"body_style":"Minivan"}

Example 15:
Page: "2021 Volkswagen Jetta SE Manual | $19,500 | Used | 56,000 miles | White Silver Metallic | Titan Black Leatherette | 1.4T 147hp | 6-Speed Manual | Front-Wheel Drive | No extra packages — just the car as-is"
Output: {"price":19500,"condition":"Used","exterior_color":"White Silver Metallic","interior_color":"Titan Black Leatherette","packages":[],"description":"1.4T 147hp Turbocharged 4-Cyl. 6-Speed Manual. No additional packages.","transmission":"6-Speed Manual","drivetrain":"FWD","fuel_type":"Gasoline","cylinders":4,"body_style":"Sedan"}

Example 16:
Page: "Pre-Owned 2023 Genesis GV80 3.5T Prestige AWD | Internet Price: $58,900 | 14,210 Miles | Savile Silver Matte | Obsidian Black Leather | 3.5L Twin-Turbo V6 375HP | 8-Speed Automatic | HTRAC AWD | Prestige Package, Lexicon Premium Audio, Head-Up Display, Rear-Seat Entertainment, Nappa Leather, 22-inch Wheels | Immaculate one-owner GV80 with full Genesis coverage remaining."
Output: {"price":58900,"condition":"Used","exterior_color":"Savile Silver Matte","interior_color":"Obsidian Black Leather","packages":["Prestige Package","Lexicon Premium Audio","Head-Up Display","Rear-Seat Entertainment","Nappa Leather","22-inch Wheels"],"description":"Immaculate one-owner GV80 with full Genesis coverage remaining.","transmission":"8-Speed Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":6,"body_style":"SUV"}

Example 17:
Page: "2022 Subaru Outback Wilderness | Sale: $36,290 | Used | 19,880 mi | Geyser Blue | Gray StarTex Water-Repellent Upholstery | 2.4L Turbo Flat-4 Boxer 260HP | Lineartronic CVT | Symmetrical AWD | X-MODE with Hill Descent Control, 9.0-inch Ground Clearance | Standard: EyeSight, Heated Seats, 11.6-in Touchscreen — no optional packages added"
Output: {"price":36290,"condition":"Used","exterior_color":"Geyser Blue","interior_color":"Gray StarTex Water-Repellent Upholstery","packages":[],"description":"2.4L Turbo Boxer AWD. 9.0-inch ground clearance. X-MODE Hill Descent, EyeSight, Heated Seats standard.","transmission":"CVT","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":4,"body_style":"Wagon"}

Example 18:
Page: "2023 Cadillac Escalade ESV Sport Platinum 4WD | Dealer Discount Price: $112,995 | New | Black Raven | Jet Black Semi-Aniline Leather | 6.2L V8 420HP | 10-Speed Automatic | 4WD | Sport Package, Super Cruise, AKG Studio Reference Audio, Night Vision, Rear Camera Mirror, 36-inch OLED Curved Display"
Output: {"price":112995,"condition":"New","exterior_color":"Black Raven","interior_color":"Jet Black Semi-Aniline Leather","packages":["Sport Package","Super Cruise","AKG Studio Reference Audio","Night Vision","Rear Camera Mirror","36-inch OLED Curved Display"],"description":"6.2L V8 420HP, 10-Speed Auto, 4WD. Super Cruise, Night Vision, AKG Audio, 36-inch OLED Display.","transmission":"10-Speed Automatic","drivetrain":"4WD","fuel_type":"Gasoline","cylinders":8,"body_style":"SUV"}

Example 19:
Page: "2023 BMW i4 M50 xDrive | $82,900 | CPO BMW | 8,441 miles | Frozen Deep Grey Metallic | Smoke White Full Merino Leather | Dual Electric Motors | Automatic | xDrive AWD | 227 mi range | M Sport Pro Package, Bowers & Wilkins Surround, Executive Package, Adaptive M Suspension, Carbon Fiber Interior Trim | This is the quickest, most capable i4 configuration available."
Output: {"price":82900,"condition":"Certified Pre-Owned","exterior_color":"Frozen Deep Grey Metallic","interior_color":"Smoke White Full Merino Leather","packages":["M Sport Pro Package","Bowers & Wilkins Surround","Executive Package","Adaptive M Suspension","Carbon Fiber Interior Trim"],"description":"This is the quickest, most capable i4 configuration available. 227 mi range, dual electric motors.","transmission":"Automatic","drivetrain":"AWD","fuel_type":"Electric","cylinders":0,"body_style":"Sedan"}

Example 20:
Page: "2020 Toyota RAV4 Hybrid XSE | Price: $33,495 | Pre-Owned | 38,211 Miles | Magnetic Gray Metallic | Black SofTex | 2.5L 4-Cyl Hybrid 219HP Total System | eCVT | Electronic On-Demand AWD | Star Safety System, Bird's Eye View Camera, JBL Audio, Power Liftgate, Panoramic Moonroof | Clean Carfax, one owner, dealer serviced."
Output: {"price":33495,"condition":"Used","exterior_color":"Magnetic Gray Metallic","interior_color":"Black SofTex","packages":["Bird's Eye View Camera","JBL Audio","Power Liftgate","Panoramic Moonroof"],"description":"Clean Carfax, one owner, dealer serviced. 2.5L Hybrid 219HP, eCVT, Electronic On-Demand AWD.","transmission":"eCVT","drivetrain":"AWD","fuel_type":"Hybrid","cylinders":4,"body_style":"SUV"}
"""

_PROMPT_TMPL = """\
VIN: {vin}
Make/Model: {year} {make} {model} {trim}

Extract from the page text below:
- price (integer USD, no $ or commas — the internet/sale price)
- condition ("New", "Used", or "Certified Pre-Owned")
- exterior_color (full color name as listed, e.g. "Midnight Black Metallic")
- interior_color (full color name, e.g. "Black Leather")
- packages (JSON array of option/package names found, e.g. ["Sport Package","Navigation"])
- description (vehicle description paragraph, max 300 chars)
- transmission (e.g. "8-Speed Automatic", "CVT", "6-Speed Manual")
- drivetrain ("FWD", "AWD", "RWD", or "4WD")
- fuel_type ("Gasoline", "Hybrid", "Plug-In Hybrid", "Electric", "Diesel")
- cylinders (integer, e.g. 4 or 6)
- body_style ("Sedan", "SUV", "Truck", "Coupe", "Hatchback", "Wagon", "Minivan", "Convertible", "Van")

Return JSON like:
{{"price":32999,"condition":"Used","exterior_color":"Pearl White","interior_color":null,"packages":["Sunroof","Navigation"],"description":"...","transmission":"8-Speed Automatic","drivetrain":"AWD","fuel_type":"Gasoline","cylinders":4,"body_style":"SUV"}}

Page text (truncated):
{text}"""


def _api_key() -> str:
    return (os.environ.get("ANTHROPIC_API_KEY") or "").strip()


def _enabled() -> bool:
    if not _api_key():
        return False
    return (os.environ.get("SCANNER_CLAUDE_VDP") or "1").strip().lower() not in ("0", "false", "no", "off")


def _missing_fields(v: dict[str, Any]) -> list[str]:
    """Return list of fields that are empty/missing on the vehicle row."""
    from backend.utils.field_clean import is_effectively_empty
    return [f for f in _FIELDS_WANTED if is_effectively_empty(v.get(f))]


def _call_claude_sync(prompt: str) -> dict[str, Any] | None:
    key = _api_key()
    if not key:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=400,
            system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text.strip())
        return json.loads(text)
    except Exception as e:
        log.debug("Claude VDP extract call failed: %s", e)
        return None


def _clean_val(field: str, raw: Any) -> Any:
    """Validate/clean a Claude-returned field value before merging."""
    if raw is None:
        return None
    if field == "price":
        try:
            v = int(str(raw).replace(",", "").replace("$", "").strip())
            return v if 500 < v < 10_000_000 else None
        except (TypeError, ValueError):
            return None
    if field == "condition":
        s = str(raw).strip()
        if s in ("New", "Used", "Certified Pre-Owned"):
            return s
        sl = s.lower()
        if "certified" in sl or "cpo" in sl:
            return "Certified Pre-Owned"
        if "new" in sl:
            return "New"
        if "used" in sl or "pre" in sl:
            return "Used"
        return None
    if field == "cylinders":
        try:
            c = int(raw)
            return c if 0 < c <= 16 else None
        except (TypeError, ValueError):
            return None
    if field == "packages":
        if isinstance(raw, list):
            return [str(p).strip() for p in raw if str(p).strip()][:30]
        if isinstance(raw, str) and raw.strip():
            return [raw.strip()]
        return None
    if field == "drivetrain":
        s = str(raw).strip().upper()
        if s in ("FWD", "AWD", "RWD", "4WD", "4X4"):
            return s.replace("4X4", "4WD")
        return None
    if field in ("exterior_color", "interior_color", "transmission", "fuel_type", "body_style", "description"):
        s = str(raw).strip()
        return s[:300] if s and s.lower() not in ("null", "n/a", "na", "none", "unknown") else None
    return raw


def extract_from_page_text(
    page_text: str,
    vehicle: dict[str, Any],
) -> dict[str, Any]:
    """
    Synchronous Claude extraction — run via asyncio.to_thread from vdp.py.
    Returns dict of {field: value} for fields Claude found.
    Empty dict if disabled or nothing useful found.
    """
    if not _enabled():
        return {}
    missing = _missing_fields(vehicle)
    if not missing:
        return {}

    vin = str(vehicle.get("vin") or "").strip()
    year = vehicle.get("year") or ""
    make = str(vehicle.get("make") or "").strip()
    model = str(vehicle.get("model") or "").strip()
    trim = str(vehicle.get("trim") or "").strip()

    text = page_text[:_MAX_PAGE_CHARS].strip()
    if len(text) < 200:
        return {}

    prompt = _PROMPT_TMPL.format(
        vin=vin, year=year, make=make, model=model, trim=trim, text=text
    )
    raw = _call_claude_sync(prompt)
    if not isinstance(raw, dict):
        return {}

    out: dict[str, Any] = {}
    for field in missing:
        val = _clean_val(field, raw.get(field))
        if val is not None:
            out[field] = val

    if out:
        log.info(
            "Claude VDP extract: VIN %s — filled %s",
            vin[:17],
            list(out.keys()),
        )
    return out
