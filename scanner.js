#!/usr/bin/env node
/**
 * Dealership inventory scanner — Node.js + Puppeteer (stealth), optimized for speed.
 * Shares inventory.db with the Python Flask app. Run: node scanner.js
 */
const path = require("path");
const fs = require("fs-extra");
const sqlite3 = require("sqlite3").verbose();
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

puppeteer.use(StealthPlugin());

const ROOT = path.resolve(__dirname);
const DB_PATH = process.env.INVENTORY_DB_PATH || path.join(ROOT, "inventory.db");
const MANIFEST_PATH = process.env.DEALERS_MANIFEST
  ? path.isAbsolute(process.env.DEALERS_MANIFEST)
    ? process.env.DEALERS_MANIFEST
    : path.join(ROOT, process.env.DEALERS_MANIFEST)
  : path.join(ROOT, "dealers.json");
const DEBUG_DIR = path.join(ROOT, "debug");

const FALLBACK_IMAGE_URL = "/static/placeholder.svg";
const DEFAULT_STR = "N/A";
const TARGET_PAGE_SIZE = Math.min(500, parseInt(process.env.INVENTORY_PAGE_SIZE || "500", 10) || 500);
const MAX_EXTRA_PAGES = 50;

const INVENTORY_PATHS = [
  "/new-inventory/index.htm",
  "/used-inventory/index.htm",
  "/certified-inventory/index.htm",
];

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
];

function randomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function randomDelayMs() {
  return 1000 + Math.floor(Math.random() * 2000);
}

function normStr(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normInt(v) {
  if (v == null) return 0;
  const s = String(v).replace(/,/g, "").trim();
  if (!s) return 0;
  const n = parseInt(String(parseFloat(s)), 10);
  return Number.isFinite(n) ? n : 0;
}

function normFloat(v) {
  if (v == null) return 0;
  let s = String(v).replace(/,/g, "").replace(/\$/g, "").trim();
  let n = parseFloat(s);
  if (Number.isFinite(n) && n > 0) return n;
  // Embedded text: "Internet Price: $58,000" or "58,000 USD"
  const m = s.match(/-?\d[\d,]*\.?\d*/);
  if (m) {
    n = parseFloat(m[0].replace(/,/g, ""));
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function findTrackingAttr(arr, nameWant) {
  if (!Array.isArray(arr)) return null;
  const want = String(nameWant).trim().toLowerCase();
  for (const item of arr) {
    if (!item || typeof item !== "object") continue;
    const n = item.name ?? item.key ?? item.id;
    if (n == null) continue;
    if (String(n).trim().toLowerCase() === want) {
      return item.value ?? item.text ?? null;
    }
  }
  return null;
}

function cleanImageUrl(url, baseUrl) {
  if (!url || typeof url !== "string") return "";
  url = url.trim();
  if (url.startsWith("//")) return "https:" + url;
  if (url.startsWith("/")) {
    const base = baseUrl.replace(/\/$/, "");
    if (base.includes("://")) return base + url;
    return "https:" + base + url;
  }
  return url;
}

function findVehicleList(obj, minVinCount = 3) {
  if (obj == null) return null;
  if (Array.isArray(obj)) {
    const vinCount = obj.filter((i) => i && typeof i === "object" && ("vin" in i || "VIN" in i)).length;
    if (vinCount >= minVinCount) return obj;
    for (const item of obj) {
      const found = findVehicleList(item, minVinCount);
      if (found) return found;
    }
    return null;
  }
  if (typeof obj === "object") {
    for (const v of Object.values(obj)) {
      const found = findVehicleList(v, minVinCount);
      if (found) return found;
    }
  }
  return null;
}

function hasVehicleIdent(o) {
  if (!o || typeof o !== "object") return false;
  if (o.vin || o.VIN || o.stockNumber) return true;
  const tp = o.trackingPricing;
  if (tp && typeof tp === "object" && (tp.internetPrice || tp.retailPrice)) return true;
  const p = o.pricing;
  if (p && typeof p === "object" && (p.retailPrice || p.internetPrice || p.salePrice)) return true;
  return false;
}

function extractTitle(obj, year, make, model) {
  const fallback = `${year || ""} ${make || ""} ${model || ""}`.trim() || DEFAULT_STR;
  const raw = obj.title ?? obj.name;
  if (raw == null) return fallback;
  if (Array.isArray(raw)) {
    const s = raw.map((x) => (x != null ? String(x).trim() : "")).filter(Boolean).join(" ");
    return s || fallback;
  }
  const s = normStr(raw);
  return s || fallback;
}

function firstPositivePrice(...vals) {
  for (const v of vals) {
    if (v == null || v === false) continue;
    if (typeof v === "string" && v.toLowerCase().includes("contact")) continue;
    const n = normFloat(v);
    if (n > 0) return n;
  }
  return 0;
}

function extractPrice(obj) {
  const tracking = obj.trackingPricing || obj.tracking_pricing;
  const pricing = obj.pricing && typeof obj.pricing === "object" ? obj.pricing : null;
  let raw = firstPositivePrice(
    tracking && tracking.internetPrice,
    tracking && tracking.internetPriceUnformatted,
    tracking && tracking.internet_price,
    pricing && pricing.internetPrice,
    pricing && pricing.internet_price,
    pricing && pricing.finalPrice,
    pricing && pricing.final_price,
    pricing && pricing.salePrice,
    pricing && pricing.sale_price,
    pricing && pricing.msrp,
    obj.price,
    obj.internetPrice,
    pricing && pricing.retailPrice,
    pricing && pricing.retail_price
  );
  if (raw === 0) {
    const arr = obj.trackingAttributes || obj.tracking_attributes || obj.attributes;
    if (Array.isArray(arr)) {
      const v2 = findTrackingAttr(arr, "price") ?? findTrackingAttr(arr, "msrp");
      if (v2 != null && String(v2).trim()) raw = normFloat(v2);
    }
  }
  return Math.round(raw);
}

function extractMsrp(obj) {
  const tracking = obj.trackingPricing || obj.tracking_pricing;
  const pricing = obj.pricing && typeof obj.pricing === "object" ? obj.pricing : null;
  let raw = firstPositivePrice(
    pricing && pricing.msrp,
    pricing && pricing.MSRP,
    pricing && pricing.retailMsrp,
    tracking && tracking.msrp,
    tracking && tracking.MSRP,
    tracking && tracking.retailMsrp,
    obj.msrp
  );
  if (raw === 0) {
    const arr = obj.trackingAttributes || obj.tracking_attributes || obj.attributes;
    if (Array.isArray(arr)) {
      const v2 = findTrackingAttr(arr, "msrp");
      if (v2 != null && String(v2).trim()) raw = normFloat(v2);
    }
  }
  return Math.round(raw);
}

function extractMileage(obj) {
  const arr = obj.trackingAttributes || obj.tracking_attributes;
  let v = findTrackingAttr(arr, "odometer");
  if (v != null && v !== "") return normInt(v);
  v = obj.odometer ?? obj.mileage;
  if (v == null && Array.isArray(arr)) v = findTrackingAttr(arr, "mileage");
  return normInt(v);
}

function bestImageUrlFromItem(item, baseUrl) {
  if (!item || typeof item !== "object") return "";
  const keys = [
    "xxlargeUri",
    "xlargeUri",
    "largeUri",
    "fullUri",
    "hiResUri",
    "uri",
    "url",
    "URL",
    "imageUrl",
    "thumbnailUri",
    "thumbUrl",
  ];
  for (const k of keys) {
    const u = item[k];
    if (u && typeof u === "string" && u.trim()) return cleanImageUrl(u.trim(), baseUrl);
  }
  return "";
}

function extractGallery(obj, baseUrl) {
  const images = obj.images || obj.Images;
  if (Array.isArray(images) && images.length > 0) {
    const out = [];
    const seen = new Set();
    for (const item of images) {
      const u = bestImageUrlFromItem(item, baseUrl);
      if (u && !seen.has(u)) {
        seen.add(u);
        out.push(u);
      }
    }
    if (out.length) return out;
  }
  for (const key of ["featuredImage", "featured_image", "thumbnail", "Thumbnail", "primaryImage", "primary_image"]) {
    const val = obj[key];
    if (typeof val === "string" && val.trim()) return [cleanImageUrl(val.trim(), baseUrl)];
    if (val && typeof val === "object") {
      const u = val.uri || val.url || val.URL;
      if (u && typeof u === "string" && u.trim()) return [cleanImageUrl(u.trim(), baseUrl)];
    }
  }
  return [];
}

function extractHistoryHighlights(obj) {
  const out = [];
  const seen = new Set();
  function add(s) {
    const t = normStr(s);
    if (!t) return;
    const k = t.toLowerCase();
    if (seen.has(k)) return;
    seen.add(k);
    out.push(t);
  }
  for (const key of ["callout", "callouts", "badges", "Badges", "historyBadges", "history_badges"]) {
    const val = obj[key];
    if (Array.isArray(val)) {
      for (const item of val) {
        if (typeof item === "string") add(item);
        else if (item && typeof item === "object")
          add(item.text || item.label || item.name || item.value || "");
      }
    } else if (typeof val === "string") add(val);
  }
  const ha = obj.highlightedAttributes || obj.highlighted_attributes;
  if (Array.isArray(ha)) {
    for (const item of ha) {
      if (typeof item === "string") add(item);
      else if (item && typeof item === "object") {
        const name = item.name || item.key || item.label;
        const value = item.value || item.text;
        if (name && value) add(`${name}: ${value}`);
        else if (value) add(String(value));
        else if (name) add(String(name));
      }
    }
  }
  return out;
}

function safeStr(v, def = DEFAULT_STR) {
  const s = normStr(v);
  return s || def;
}

function mapVehicle(obj, baseUrl, dealerId, dealerName, dealerUrl) {
  if (!obj || typeof obj !== "object") return null;

  let vin = normStr(obj.vin || obj.VIN || "");
  if (!vin) vin = normStr(obj.stockNumber || "");
  if (!vin) vin = `unknown-${Math.abs(hashCode(JSON.stringify(obj))) % 1e8}`;

  let stockNumber = safeStr(obj.stockNumber, "");
  if (stockNumber === DEFAULT_STR) stockNumber = "";

  const year = normInt(obj.year ?? obj.modelYear ?? obj.model_year);
  const make = normStr(obj.make || obj.Make || "");
  const model = normStr(obj.model || obj.Model || obj.modelName || "");

  const title = extractTitle(obj, year, make, model);
  const price = extractPrice(obj);
  const msrp = extractMsrp(obj);

  const mileage = extractMileage(obj);
  let gallery = extractGallery(obj, baseUrl);
  let imageUrl = gallery[0] || "";
  if (!imageUrl || !gallery.length) {
    imageUrl = imageUrl || FALLBACK_IMAGE_URL;
    gallery = gallery.length ? gallery : [FALLBACK_IMAGE_URL];
  }

  const arr = obj.trackingAttributes || obj.tracking_attributes;
  const exteriorColor = (() => {
    const v = findTrackingAttr(arr, "exteriorColor");
    if (v != null && String(v).trim()) return normStr(v) || DEFAULT_STR;
    return safeStr(obj.exteriorColor || obj.exterior_color);
  })();

  const fuelType = safeStr(obj.fuelType || obj.fuel_type);

  const vhrUrl = obj.vhr_url || obj.carfax_token;
  let carfaxUrl = "";
  if (vhrUrl && typeof vhrUrl === "string" && vhrUrl.trim().startsWith("http")) {
    carfaxUrl = normStr(vhrUrl);
  } else if (vhrUrl && vin && !vin.startsWith("unknown")) {
    carfaxUrl = `https://vhr.carfax.com/main?vin=${encodeURIComponent(vin)}`;
  } else {
    carfaxUrl = normStr(
      obj.carfax_url ||
        obj.carfaxUrl ||
        obj.carfaxLink ||
        obj.history_report_url ||
        obj.vehicleHistoryUrl ||
        obj.vehicle_history_url ||
        ""
    );
  }

  let cylinders = normInt(obj.cylinders);
  if (!cylinders) {
    const c = findTrackingAttr(arr, "cylinders");
    if (c != null) cylinders = normInt(c);
  }

  return {
    vin,
    stock_number: stockNumber,
    year,
    make,
    model,
    trim: safeStr(obj.trim || obj.Trim || obj.trimName),
    title,
    price,
    mileage,
    image_url: imageUrl,
    gallery,
    dealer_id: dealerId,
    dealer_name: dealerName,
    dealer_url: dealerUrl,
    zip_code: safeStr(obj.zipCode || obj.zip_code),
    fuel_type: fuelType,
    transmission: safeStr(obj.transmission || obj.transmissionType),
    drivetrain: safeStr(obj.drivetrain || obj.driveType),
    exterior_color: exteriorColor,
    interior_color: safeStr(obj.interiorColor || obj.interior_color),
    carfax_url: carfaxUrl || null,
    history_highlights: extractHistoryHighlights(obj),
    cylinders: cylinders || null,
    msrp: msrp > 0 ? msrp : null,
  };
}

function hashCode(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  return h;
}

function parseJsonList(data, baseUrl, dealerId, dealerName, dealerUrl) {
  const items = findVehicleList(data);
  if (!items) return [];
  const out = [];
  for (const obj of items) {
    if (!obj || typeof obj !== "object") continue;
    if (!hasVehicleIdent(obj)) continue;
    const m = mapVehicle(obj, baseUrl, dealerId, dealerName, dealerUrl);
    if (m) out.push(m);
  }
  return out;
}

function isValidVehicleList(body) {
  return findVehicleList(body) != null;
}

function getTotalCountFromBody(body) {
  if (!body || typeof body !== "object") return null;
  if (body.totalCount != null) return normInt(body.totalCount);
  if (body.total_count != null) return normInt(body.total_count);
  if (body.totalRecords != null) return normInt(body.totalRecords);
  const pi = body.pageInfo || body.page_info || body.pagination;
  if (pi && typeof pi === "object") {
    if (pi.totalCount != null) return normInt(pi.totalCount);
    if (pi.total != null) return normInt(pi.total);
    if (pi.totalRecords != null) return normInt(pi.totalRecords);
  }
  return null;
}

function countVehicles(body) {
  const list = findVehicleList(body);
  return list ? list.length : 0;
}

/** Rewrite getInventory GET URL to use large pageSize */
function rewriteInventoryUrl(urlString) {
  try {
    const u = new URL(urlString);
    if (!/getInventory/i.test(u.href)) {
      return urlString;
    }
    ["pageSize", "size", "perPage", "limit"].forEach((k) => {
      if (u.searchParams.has(k)) u.searchParams.set(k, String(TARGET_PAGE_SIZE));
    });
    if (![...u.searchParams.keys()].some((k) => /pageSize|size|perPage|limit/i.test(k))) {
      u.searchParams.set("pageSize", String(TARGET_PAGE_SIZE));
    }
    return u.toString();
  } catch {
    return urlString;
  }
}

async function setupRequestBlocking(page) {
  await page.setRequestInterception(true);
  page.on("request", (req) => {
    const type = req.resourceType();
    if (["image", "stylesheet", "font", "media"].includes(type)) {
      return req.abort();
    }
    const url = req.url();
    if (req.method() === "GET" && /getInventory/i.test(url)) {
      const next = rewriteInventoryUrl(url);
      if (next !== url) return req.continue({ url: next });
    }
    return req.continue();
  });
}

function isInventoryJsonResponse(response) {
  const u = response.url();
  const ct = (response.headers()["content-type"] || "").toLowerCase();
  return ct.includes("application/json") && /getInventory/i.test(u);
}

/** Broader match for slow fallback (some endpoints omit "getInventory" in the path). */
function isSlowInventoryJsonResponse(response) {
  const u = response.url();
  const ct = (response.headers()["content-type"] || "").toLowerCase();
  return ct.includes("application/json") && /getInventory|inventory/i.test(u);
}

/**
 * Fetch additional JSON pages inside the browser (same cookies / session as the page).
 */
async function fetchAdditionalInventoryPages(page, firstUrl, firstBody) {
  const bodies = [firstBody];
  const total = getTotalCountFromBody(firstBody);
  const firstList = findVehicleList(firstBody);
  const firstCount = firstList ? firstList.length : 0;
  if (!total || firstCount >= total) return bodies;

  let u;
  try {
    u = new URL(firstUrl);
  } catch {
    return bodies;
  }

  const paramPageSize = normInt(
    u.searchParams.get("pageSize") ||
      u.searchParams.get("size") ||
      u.searchParams.get("perPage") ||
      u.searchParams.get("limit")
  );
  const requestedPageSize = paramPageSize || TARGET_PAGE_SIZE;
  // If we asked for N vehicles/page and got fewer than N, this response is the full set for this
  // dealer/query — even if totalCount is larger (often unfiltered global count). Avoids bogus
  // follow-up GETs that return HTTP 404.
  if (firstCount < requestedPageSize && firstCount < total) {
    return bodies;
  }

  const effectivePageSize = Math.max(paramPageSize || 0, firstCount, 1);
  const pagesNeeded = Math.ceil(total / effectivePageSize);
  const extraFetches = Math.min(MAX_EXTRA_PAGES, Math.max(0, pagesNeeded - 1));

  const pageKeys = ["pageNumber", "page", "pageNum", "currentPage"];
  let current = 0;
  for (const k of pageKeys) {
    if (u.searchParams.has(k)) {
      current = normInt(u.searchParams.get(k));
      break;
    }
  }

  let pageParamKey = null;
  for (const k of pageKeys) {
    if (u.searchParams.has(k)) {
      pageParamKey = k;
      break;
    }
  }
  if (!pageParamKey) pageParamKey = "pageNumber";

  for (let p = 1; p <= extraFetches; p++) {
    const nextPage = current + p;
    const u2 = new URL(u.toString());
    u2.searchParams.set(pageParamKey, String(nextPage));

    try {
      const referer = page.url() || "";
      const json = await page.evaluate(
        async ({ fetchUrl, referer: ref }) => {
          const r = await fetch(fetchUrl, {
            credentials: "include",
            headers: {
              Accept: "application/json",
              ...(ref ? { Referer: ref } : {}),
            },
          });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return await r.json();
        },
        { fetchUrl: u2.toString(), referer }
      );

      if (!isValidVehicleList(json)) break;
      bodies.push(json);
      const got = countVehicles(json);
      if (got === 0) break;
      const accumulated = bodies.reduce((sum, b) => sum + countVehicles(b), 0);
      if (accumulated >= total) break;
    } catch (e) {
      const msg = String(e.message || e);
      if (msg.includes("HTTP 404")) {
        console.info(`[turbo] Extra inventory page ${nextPage} not available (${msg}); using loaded pages only.`);
      } else {
        console.warn(`[turbo] fetch page ${nextPage} failed: ${msg}`);
      }
      break;
    }
  }

  return bodies;
}

async function collectVehiclesFromBodies(bodies, baseUrl, dealerId, name, url) {
  let all = [];
  for (const body of bodies) {
    const vehicles = parseJsonList(body, baseUrl, dealerId, name, url);
    for (const v of vehicles) {
      v.dealer_name = name;
      v.dealer_url = url;
    }
    all = all.concat(vehicles);
  }
  return all;
}

const MAX_PAGINATION_CLICKS = 15;

/** Slower human-like path: scroll + click pagination + longer waits */
async function runDealerSlow(browser, dealer) {
  const name = dealer.name || "";
  const url = String(dealer.url || "").replace(/\/$/, "");
  const dealerId = dealer.dealer_id || "";
  const intercepted = [];
  const page = await browser.newPage();
  await page.setUserAgent(randomUserAgent());
  await page.setViewport({ width: 1920, height: 1080 });

  page.on("response", async (response) => {
    try {
      if (!isSlowInventoryJsonResponse(response)) return;
      const body = await response.json();
      if (isValidVehicleList(body)) {
        intercepted.push(body);
        console.info(`[slow] Intercepted: ${name} — ${response.url().slice(0, 80)}`);
      }
    } catch {
      /* ignore */
    }
  });

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await sleep(4000);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
    await sleep(1000);

    for (const invPath of INVENTORY_PATHS) {
      const fullUrl = url + invPath;
      try {
        await page.goto(fullUrl, { waitUntil: "domcontentloaded", timeout: 20000 });
        await page
          .waitForResponse((r) => isSlowInventoryJsonResponse(r) && r.status() === 200, { timeout: 25000 })
          .catch(() => null);
        await sleep(800);

        for (let s = 0; s < 5; s++) {
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
          await sleep(1500);
        }

        for (let p = 0; p < MAX_PAGINATION_CLICKS; p++) {
          const clicked = await page.evaluate(() => {
            const tryClick = (el) => {
              try {
                el.click();
                return true;
              } catch {
                return false;
              }
            };
            const dataNext = document.querySelector('[data-action="next"], .pagination-next, .load-more');
            if (dataNext && tryClick(dataNext)) return true;
            const candidates = [...document.querySelectorAll("button, a, [role='button']")];
            for (const el of candidates) {
              const t = (el.textContent || "").trim();
              if (/next|load more/i.test(t) && el.offsetParent !== null) {
                if (tryClick(el)) return true;
              }
            }
            return false;
          });
          if (!clicked) break;
          await sleep(randomDelayMs());
          await page.waitForResponse((r) => isSlowInventoryJsonResponse(r), { timeout: 20000 }).catch(() => null);
        }
      } catch (e) {
        console.warn(`[slow] ${fullUrl}: ${e.message}`);
      }
    }

    await page.close();
    return collectVehiclesFromBodies(intercepted, url, dealerId, name, url);
  } catch (e) {
    await page.close().catch(() => {});
    throw e;
  }
}

/** Turbo: block assets, rewrite pageSize, waitForResponse, optional fetch pagination */
async function runDealerTurbo(browser, dealer) {
  const name = dealer.name || "";
  const url = String(dealer.url || "").replace(/\/$/, "");
  const dealerId = dealer.dealer_id || "";

  const page = await browser.newPage();
  await page.setUserAgent(randomUserAgent());
  await page.setViewport({ width: 1920, height: 1080 });
  await setupRequestBlocking(page);

  try {
    let firstInvUrl = null;
    let firstBody = null;

    for (const invPath of INVENTORY_PATHS) {
      const fullUrl = url + invPath;
      const invPromise = page.waitForResponse(
        (r) => isInventoryJsonResponse(r) && r.status() === 200,
        { timeout: 35000 }
      );
      await page.goto(fullUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
      const invResponse = await invPromise.catch(() => null);
      if (invResponse) {
        try {
          firstBody = await invResponse.json();
          firstInvUrl = invResponse.url();
          if (isValidVehicleList(firstBody)) {
            console.info(`[turbo] getInventory: ${name} — ${(firstInvUrl || "").slice(0, 100)}`);
            break;
          }
          firstBody = null;
          firstInvUrl = null;
        } catch {
          firstBody = null;
          firstInvUrl = null;
        }
      }
    }

    let bodiesToParse = [];
    if (firstInvUrl && firstBody) {
      bodiesToParse = await fetchAdditionalInventoryPages(page, firstInvUrl, firstBody);
    }

    await page.close();

    if (!bodiesToParse.length) return [];

    const merged = [];
    const seen = new Set();
    for (const b of bodiesToParse) {
      const vehicles = parseJsonList(b, url, dealerId, name, url);
      for (const v of vehicles) {
        v.dealer_name = name;
        v.dealer_url = url;
        if (!seen.has(v.vin)) {
          seen.add(v.vin);
          merged.push(v);
        }
      }
    }
    return merged;
  } catch (e) {
    await page.close().catch(() => {});
    throw e;
  }
}

async function runDealer(browser, dealer) {
  const name = dealer.name || "";
  const url = String(dealer.url || "").replace(/\/$/, "");
  const provider = dealer.provider || "dealer_dot_com";
  const dealerId = dealer.dealer_id || "";

  if (!url || !dealerId) {
    console.warn(`Skipping dealer missing url or dealer_id: ${JSON.stringify(dealer)}`);
    return [];
  }
  if (provider !== "dealer_dot_com") {
    console.info(`Skipping non–Dealer.com (${provider}): ${name}`);
    return [];
  }

  try {
    console.info(`[turbo] Starting: ${name}`);
    const vehicles = await runDealerTurbo(browser, dealer);
    if (vehicles.length) {
      console.info(`[turbo] ${name}: ${vehicles.length} vehicles`);
      return vehicles;
    }
    throw new Error("No vehicles in turbo mode");
  } catch (e) {
    console.warn(`[turbo] ${name} failed (${e.message}), falling back to slow path`);
    try {
      const vehicles = await runDealerSlow(browser, dealer);
      console.info(`[slow] ${name}: ${vehicles.length} vehicles`);
      return vehicles;
    } catch (e2) {
      console.error(`[slow] ${name} failed:`, e2);
      return [];
    }
  }
}

function openDb() {
  return new sqlite3.Database(DB_PATH);
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

async function ensureSchema(db) {
  await run(
    db,
    `CREATE TABLE IF NOT EXISTS cars (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vin TEXT UNIQUE NOT NULL,
      title TEXT,
      year INTEGER,
      make TEXT,
      model TEXT,
      trim TEXT,
      price REAL,
      mileage INTEGER,
      zip_code TEXT,
      fuel_type TEXT,
      cylinders INTEGER,
      transmission TEXT,
      drivetrain TEXT,
      exterior_color TEXT,
      interior_color TEXT,
      image_url TEXT,
      dealer_name TEXT,
      dealer_url TEXT,
      dealer_id TEXT,
      scraped_at TEXT
    )`
  );
  const cols = await new Promise((resolve, reject) => {
    db.all("PRAGMA table_info(cars)", [], (err, rows) => {
      if (err) reject(err);
      else resolve(rows.map((r) => r.name));
    });
  });
  for (const [col, ctype] of [
    ["dealer_id", "TEXT"],
    ["stock_number", "TEXT"],
    ["gallery", "TEXT"],
    ["carfax_url", "TEXT"],
    ["history_highlights", "TEXT"],
    ["msrp", "REAL"],
  ]) {
    if (!cols.includes(col)) {
      await run(db, `ALTER TABLE cars ADD COLUMN ${col} ${ctype}`);
    }
  }
  await run(
    db,
    `CREATE TABLE IF NOT EXISTS model_specs (
      make TEXT NOT NULL,
      model TEXT NOT NULL,
      cylinders INTEGER,
      gears INTEGER,
      transmission TEXT,
      PRIMARY KEY (make, model)
    )`
  );
}

function isNaOrMissingTransmission(t) {
  const s = normStr(t);
  return !s || s === DEFAULT_STR || /^n\/?a$/i.test(s);
}

async function applySelfCorrection(db, vehicle) {
  const make = normStr(vehicle.make);
  const model = normStr(vehicle.model);
  if (!make || !model) return vehicle;

  const row = await get(
    db,
    `SELECT cylinders, gears, transmission FROM model_specs WHERE make = ? AND model = ? COLLATE NOCASE`,
    [make, model]
  );
  if (!row) return vehicle;

  let cyl = vehicle.cylinders;
  if (cyl == null || cyl === 0) {
    if (row.cylinders != null && row.cylinders > 0) cyl = row.cylinders;
  }
  vehicle.cylinders = cyl;

  let trans = vehicle.transmission;
  if (isNaOrMissingTransmission(trans)) {
    if (row.transmission && normStr(row.transmission)) {
      vehicle.transmission = normStr(row.transmission);
    } else if (row.gears != null && row.gears > 0) {
      vehicle.transmission = `${row.gears}-Speed`;
    }
  }

  return vehicle;
}

async function upsertVehicle(db, v) {
  const now = new Date().toISOString();
  const title =
    v.title || `${v.year || ""} ${v.make || ""} ${v.model || ""} ${v.trim || ""}`.trim();

  const price = v.price != null ? Math.round(Number(v.price)) || 0 : 0;
  const mileage = v.mileage != null ? normInt(v.mileage) : 0;

  const galleryJson = JSON.stringify(Array.isArray(v.gallery) ? v.gallery : []);
  const highlightsJson = JSON.stringify(Array.isArray(v.history_highlights) ? v.history_highlights : []);

  const sql = `
    INSERT INTO cars (
      vin, title, year, make, model, trim, price, mileage,
      image_url, dealer_name, dealer_url, dealer_id, scraped_at,
      zip_code, fuel_type, cylinders, transmission, drivetrain,
      exterior_color, interior_color, stock_number, gallery, carfax_url, history_highlights, msrp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(vin) DO UPDATE SET
      title=excluded.title, year=excluded.year, make=excluded.make,
      model=excluded.model, trim=excluded.trim, price=excluded.price,
      mileage=excluded.mileage, image_url=excluded.image_url,
      dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
      dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
      zip_code=excluded.zip_code, fuel_type=excluded.fuel_type,
      cylinders=excluded.cylinders, transmission=excluded.transmission,
      drivetrain=excluded.drivetrain, exterior_color=excluded.exterior_color,
      interior_color=excluded.interior_color, stock_number=excluded.stock_number,
      gallery=excluded.gallery, carfax_url=excluded.carfax_url, history_highlights=excluded.history_highlights,
      msrp=excluded.msrp
  `;

  const params = [
    v.vin,
    title,
    v.year ?? null,
    v.make || "",
    v.model || "",
    v.trim || "",
    price,
    mileage,
    v.image_url || "",
    v.dealer_name || "",
    v.dealer_url || "",
    v.dealer_id || "",
    now,
    v.zip_code === DEFAULT_STR ? null : v.zip_code || null,
    v.fuel_type === DEFAULT_STR ? null : v.fuel_type || null,
    v.cylinders ?? null,
    v.transmission === DEFAULT_STR ? null : v.transmission || null,
    v.drivetrain === DEFAULT_STR ? null : v.drivetrain || null,
    v.exterior_color === DEFAULT_STR ? null : v.exterior_color || null,
    v.interior_color === DEFAULT_STR ? null : v.interior_color || null,
    v.stock_number || "",
    galleryJson,
    v.carfax_url || "",
    highlightsJson,
    v.msrp != null && v.msrp > 0 ? v.msrp : null,
  ];

  await run(db, sql, params);
}

async function upsertAll(db, vehicles) {
  const byVin = {};
  for (const v of vehicles) {
    const vin = normStr(v.vin);
    if (vin) byVin[vin] = v;
  }
  let count = 0;
  for (const v of Object.values(byVin)) {
    await applySelfCorrection(db, v);
    await upsertVehicle(db, v);
    count++;
  }
  return count;
}

async function main() {
  console.info("Manifest:", MANIFEST_PATH);
  console.info("Database:", DB_PATH);
  const manifest = await fs.readJson(MANIFEST_PATH);
  const dealers = manifest.filter((d) => (d.provider || "dealer_dot_com") === "dealer_dot_com" && d.url && d.dealer_id);

  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  try {
    const results = await Promise.all(dealers.map((d) => runDealer(browser, d)));
    const flat = results.flat();

    if (!flat.length) {
      await fs.mkdir(DEBUG_DIR, { recursive: true });
      console.warn("No vehicles scraped.");
    }

    const db = openDb();
    await ensureSchema(db);
    const n = await upsertAll(db, flat);
    db.close();
    console.info(`Upserted ${n} unique vehicles (total rows parsed: ${flat.length})`);
  } finally {
    await browser.close();
  }
  console.info("Scanner finished.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
