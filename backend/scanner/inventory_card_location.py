"""
Scrape per-vehicle lot location from inventory listing cards (VLP DOM).

Sister-store groups often show the true rooftop only on the card (image watermark,
Location: field, dealer banner) while the JSON feed omits or mislabels it.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger("scanner")

_VIN17_RE = re.compile(r"\b([A-HJ-NPR-Z0-9]{17})\b", re.I)

INVENTORY_CARD_LOCATION_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  function push(vin, loc, source) {
    if (!vin || !loc) return;
    const v = String(vin).trim().toUpperCase();
    const l = String(loc).trim().replace(/\s+/g, " ");
    if (v.length !== 17 || l.length < 4 || l.length > 220) return;
    const k = v + "|" + l.toLowerCase();
    if (seen.has(k)) return;
    seen.add(k);
    out.push({ vin: v, location: l.slice(0, 200), source: source || "card" });
  }
  function cardVin(card) {
    if (!card) return "";
    const attrs = ["data-vin", "data-vehicle-vin", "data-vin-number", "data-vinnumber"];
    for (const a of attrs) {
      const v = (card.getAttribute(a) || "").trim().toUpperCase();
      if (v.length === 17) return v;
    }
    const txt = (card.innerText || card.textContent || "").slice(0, 2500);
    const m = txt.match(/\bVIN\s*[:#]?\s*([A-HJ-NPR-Z0-9]{17})\b/i);
    if (m) return m[1].toUpperCase();
    const m2 = txt.match(/\b([A-HJ-NPR-Z0-9]{17})\b/);
    return m2 ? m2[1].toUpperCase() : "";
  }
  function cardLocation(card) {
    if (!card) return "";
    const locAttrs = ["data-location", "data-dealer-name", "data-lot-name", "data-store-name"];
    for (const a of locAttrs) {
      const v = (card.getAttribute(a) || "").trim();
      if (v.length >= 4) return v;
    }
    const txt = (card.innerText || card.textContent || "").slice(0, 3500);
    const patterns = [
      /(?:^|\n)\s*location\s*:\s*([^\n|]{4,120})/i,
      /located\s+at\s+([^\n.]{4,120})/i,
      /(?:^|\n)\s*dealer\s*:\s*([^\n|]{4,120})/i,
    ];
    for (const re of patterns) {
      const m = txt.match(re);
      if (m && m[1]) return m[1].trim();
    }
    const banners = card.querySelectorAll(
      "[class*='dealer-banner'], [class*='dealerBanner'], [class*='watermark'], " +
      "[class*='store-name'], [class*='dealer-name'], [class*='lot-location'], " +
      "figcaption, [class*='vehicle-location'], [class*='inventory-location']"
    );
    for (const el of banners) {
      const t = (el.textContent || "").trim().replace(/\s+/g, " ");
      if (t.length >= 6 && t.length <= 160 && /bmw|chevrolet|chrysler|dodge|jeep|ram|ford|toyota|honda|dealer|motors|automotive/i.test(t)) {
        return t;
      }
    }
    const imgs = card.querySelectorAll("img[alt], img[title]");
    for (const img of imgs) {
      const alt = ((img.getAttribute("alt") || "") + " " + (img.getAttribute("title") || "")).trim();
      if (alt.length >= 8 && alt.length <= 120 && /bmw|chevrolet|chrysler|dodge|jeep|ram|ford|toyota|honda|dealer|motors/i.test(alt)) {
        return alt.replace(/\s+/g, " ");
      }
    }
    return "";
  }
  const cardSelectors = [
    "[data-vin]",
    "[data-vehicle-vin]",
    "[class*='vehicle-card']",
    "[class*='inventory-card']",
    "[class*='srp-list-item']",
    "[class*='vehicle-listing']",
    "article[class*='vehicle']",
    "li[class*='vehicle']",
    ".vehicle-card",
    ".inventory-list-item",
    ".srp-list-item",
  ];
  const cards = new Set();
  for (const sel of cardSelectors) {
    try {
      document.querySelectorAll(sel).forEach((el) => cards.add(el));
    } catch (e) {}
  }
  for (const card of cards) {
    const vin = cardVin(card);
    const loc = cardLocation(card);
    if (vin && loc) push(vin, loc, "card_dom");
  }
  return out;
}
"""


async def scrape_inventory_card_locations(page: Any) -> dict[str, str]:
    """Return VIN → location text from visible listing cards on the current page."""
    try:
        rows = await page.evaluate(INVENTORY_CARD_LOCATION_JS)
    except Exception as e:
        logger.debug("Inventory card location scrape skipped: %s", e)
        return {}
    if not isinstance(rows, list):
        return {}
    out: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        vin = str(row.get("vin") or "").strip().upper()
        loc = str(row.get("location") or "").strip()
        if _VIN17_RE.fullmatch(vin) and loc:
            out[vin] = loc
    return out


def apply_card_locations_to_vehicles(
    vehicles: list[dict[str, Any]],
    card_locations: dict[str, str],
) -> int:
    """Set ``_lot_location`` on vehicles when card DOM has a location hint. Returns count updated."""
    if not card_locations:
        return 0
    updated = 0
    for v in vehicles:
        vin = str(v.get("vin") or "").strip().upper()
        if not vin:
            continue
        loc = card_locations.get(vin)
        if not loc:
            continue
        existing = str(v.get("_lot_location") or "").strip()
        if existing and len(existing) >= len(loc):
            continue
        v["_lot_location"] = loc
        updated += 1
    return updated
