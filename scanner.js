#!/usr/bin/env node
/**
 * Dealership inventory scanner — Node.js + Puppeteer (stealth), optimized for speed.
 * Shares inventory.db with the Python Flask app. Run: node scanner.js
 *
 * Warmup (aligned with scanner.py): SCANNER_WARMUP_POST_GOTO_SEC (idle cap, default 4),
 * SCANNER_WARMUP_SIGNAL_TIMEOUT_MS (default 12000), SCANNER_WARMUP_SCROLL_SEC (default 1),
 * SCANNER_WARMUP_DOM_SELECTORS (optional CSV).
 */
const path = require("path");
const { spawnSync } = require("child_process");
const fs = require("fs-extra");
const sqlite3 = require("sqlite3").verbose();
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

puppeteer.use(StealthPlugin());

const ROOT = path.resolve(__dirname);
const DB_PATH = process.env.INVENTORY_DB_PATH || path.join(ROOT, "inventory.db");
const MANIFEST_PATH = path.join(ROOT, "dealers.json");
const DEBUG_DIR = path.join(ROOT, "debug");
const SCRAPE_SAMPLES_PATH = path.join(DEBUG_DIR, "last_scrape_samples.json");
const MAX_SCRAPE_SAMPLES = 5;
/** Last N raw Dealer.com objects + parsed snapshots for dev audit (flushed in upsertAll). */
const scrapeSamplesBuffer = [];

const FALLBACK_IMAGE_URL = "/static/placeholder.svg";
const DEFAULT_STR = "N/A";
const TARGET_PAGE_SIZE = Math.min(500, parseInt(process.env.INVENTORY_PAGE_SIZE || "500", 10) || 500);
const MAX_EXTRA_PAGES = 50;
/** Max VDP pages to open for analytics / dataLayer ep extraction (0 = off; default 10 matches scanner.py). */
const SCANNER_VDP_EP_MAX = Math.min(100, parseInt(process.env.SCANNER_VDP_EP_MAX || "10", 10) || 0);
const EP_RESPONSE_MAX_BYTES = 2 * 1024 * 1024;

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

// Import the data collection agent
const { process_dealer_scraping_guidance } = require('./agents/data_collection_agent');
const { runBatchVdpExtraction } = require("./vdp_framework");
const { responseLooksLikeInventoryJsonIntercept } = require("./scanner_intercept");

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

/** Match Python `normalize_optional_str`: placeholders → null for SQLite TEXT columns. */
const SQL_OPTIONAL_EMPTY_RE =
  /^(n\/?a|na|null|none|unknown|undefined|tbd|not specified|unspecified|[\-—]+)$/i;

function sqlOptionalStr(v) {
  const s = normStr(v);
  if (!s || SQL_OPTIONAL_EMPTY_RE.test(s)) return null;
  return s;
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

function looksLikeVin17(v) {
  const s = normStr(v).toUpperCase();
  return /^[A-HJ-NPR-Z0-9]{17}$/.test(s);
}

/** Walk JSON and collect every object stored under an ``ep`` key (analytics / data layer). */
function collectEpPayloadsFromJson(root) {
  const eps = [];
  function walk(o) {
    if (!o || typeof o !== "object") return;
    if (o.ep && typeof o.ep === "object" && !Array.isArray(o.ep)) {
      eps.push(o.ep);
    }
    if (Array.isArray(o)) {
      for (const x of o) walk(x);
    } else {
      for (const v of Object.values(o)) walk(v);
    }
  }
  walk(root);
  return eps;
}

function registerEpByVin(vinToEp, ep) {
  if (!ep || typeof ep !== "object") return;
  const vinRaw = normStr(ep.vin || ep.VIN || "").toUpperCase();
  if (!looksLikeVin17(vinRaw)) return;
  const cur = vinToEp.get(vinRaw) || {};
  vinToEp.set(vinRaw, { ...cur, ...ep });
}

/**
 * Attach listener: JSON responses that contain an ``ep`` object with a VIN are merged into *vinToEp*.
 */
function attachAnalyticsEpResponseListener(page, vinToEp) {
  page.on("response", async (response) => {
    try {
      if (response.status() !== 200) return;
      const ct = (response.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("json")) return;
      const buf = await response.text();
      if (!buf || buf.length > EP_RESPONSE_MAX_BYTES) return;
      if (!buf.includes('"ep"') && !buf.includes("'ep'")) return;
      let parsed;
      try {
        parsed = JSON.parse(buf);
      } catch {
        return;
      }
      const eps = collectEpPayloadsFromJson(parsed);
      for (const ep of eps) registerEpByVin(vinToEp, ep);
    } catch {
      /* ignore */
    }
  });
}

function pickVehicleDetailUrl(obj, baseUrl) {
  const candidates = [
    obj.vdpUrl,
    obj.vdp_url,
    obj.vehicleUrl,
    obj.vehicle_url,
    obj.detailUrl,
    obj.detail_url,
    obj.inventoryUrl,
    obj.inventory_url,
    obj.url,
    obj.href,
    obj.link,
  ];
  for (const raw of candidates) {
    if (typeof raw !== "string" || !raw.trim()) continue;
    let u = raw.trim();
    if (u.startsWith("//")) u = "https:" + u;
    if (!/^https?:\/\//i.test(u)) {
      try {
        u = new URL(u, baseUrl).href;
      } catch {
        continue;
      }
    }
    if (/\/vdp\//i.test(u) || /vehicle-inventory|\/inventory\/|\/used\/|\/new\/|certified/i.test(u)) {
      return u;
    }
  }
  return null;
}

function attachEpToVehicles(vehicles, vinToEp) {
  for (const v of vehicles) {
    if (!v.source_url && v._detail_url) v.source_url = v._detail_url;
    if (!vinToEp || vinToEp.size === 0) continue;
    const vin = normStr(v.vin).toUpperCase();
    if (!looksLikeVin17(vin)) continue;
    if (vinToEp.has(vin)) {
      v._ep_analytics = { ...vinToEp.get(vin) };
    }
  }
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

function normAttrKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

/** Match Dealer.com tracking attribute names with inconsistent spacing/casing. */
function findTrackingAttrLoose(arr, needles) {
  if (!Array.isArray(arr) || !needles || !needles.length) return null;
  for (const item of arr) {
    if (!item || typeof item !== "object") continue;
    const nk = normAttrKey(item.name ?? item.key ?? item.id);
    if (!nk) continue;
    for (const needle of needles) {
      const nd = String(needle).toLowerCase().replace(/[^a-z0-9]+/g, "");
      if (nd.length < 5) continue;
      if (nk === nd || nk.includes(nd) || nd.includes(nk)) {
        const v = item.value ?? item.text;
        if (v != null && String(v).trim()) return String(v).trim();
      }
    }
  }
  return null;
}

const INTERIOR_TRACKING_NEEDLES = [
  "interiorcolor",
  "interiortrim",
  "upholstery",
  "seatcolor",
  "seattrim",
  "cabincolor",
  "leathercolor",
  "intcolor",
  "interiorpackage",
  "cabintrim",
];

const BODY_TRACKING_NEEDLES = ["bodystyle", "bodytype", "vehiclebody", "vehicletype", "vehbodystyle", "bodyshape"];

function extractInteriorColorFromAttrs(arr, obj) {
  const fromArr = findTrackingAttrLoose(arr, INTERIOR_TRACKING_NEEDLES);
  if (fromArr) return sqlOptionalStr(fromArr);
  return sqlOptionalStr(
    obj.interiorColor ||
      obj.interior_color ||
      obj.interiorTrim ||
      obj.interior_trim ||
      ""
  );
}

function extractBodyStyleFromAttrs(arr, obj) {
  const direct = normStr(
    obj.bodyStyle || obj.body_style || obj.bodyType || obj.body_type || ""
  );
  if (direct) return sqlOptionalStr(direct);
  const fromArr = findTrackingAttrLoose(arr, BODY_TRACKING_NEEDLES);
  return sqlOptionalStr(fromArr || "");
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

/**
 * Longest array of objects that look like rows (fallback when strict inventory list is empty).
 */
function findLargestWeakVehicleArray(obj) {
  let best = null;
  let bestLen = 0;
  function weakHint(item) {
    if (!item || typeof item !== "object") return false;
    return !!(
      item.vin ||
      item.VIN ||
      item.stockNumber ||
      item.stock_number ||
      item.year != null ||
      item.make ||
      item.model
    );
  }
  function walk(o) {
    if (o == null) return;
    if (Array.isArray(o) && o.length > 0) {
      const hints = o.filter(weakHint).length;
      if (hints >= 1 && o.length > bestLen) {
        best = o;
        bestLen = o.length;
      }
      for (const item of o) walk(item);
      return;
    }
    if (typeof o === "object") {
      for (const v of Object.values(o)) walk(v);
    }
  }
  walk(obj);
  return best;
}

/**
 * Prefer the largest array of vehicle-like objects; fall back to weak/largest slice (e.g. Special VINs).
 */
function findVehicleList(obj, minVinCount = 1) {
  let best = null;
  let bestScore = 0;
  function walk(o) {
    if (o == null) return;
    if (Array.isArray(o) && o.length > 0) {
      const score = o.filter((i) => i && typeof i === "object" && hasVehicleIdent(i)).length;
      if (score >= minVinCount && score > bestScore) {
        best = o;
        bestScore = score;
      }
      for (const item of o) walk(item);
      return;
    }
    if (typeof o === "object") {
      for (const v of Object.values(o)) walk(v);
    }
  }
  walk(obj);
  if (best && bestScore > 0) return best;
  return findLargestWeakVehicleArray(obj);
}

function hasVehicleIdent(o) {
  if (!o || typeof o !== "object") return false;
  const attrs = o.attributes && typeof o.attributes === "object" ? o.attributes : null;
  if (o.vin || o.VIN || o.vinNumber || o.stockNumber || (attrs && (attrs.vin || attrs.VIN))) return true;
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

/** Normalize OEM name for DB + filter keys (e.g. DODGE → Dodge, matches Python MAKE_TO_COUNTRY). */
function properAutomakerLabel(s) {
  const t = normStr(s);
  if (!t) return "";
  const upper = t.toUpperCase().replace(/\s+/g, " ");
  const asIs = { BMW: "BMW", GMC: "GMC", RAM: "Ram", MINI: "MINI", VW: "Volkswagen" };
  if (asIs[upper]) return asIs[upper];
  const lower = t.toLowerCase();
  return lower.replace(/(^|[\s'-])([a-z])/g, (_, a, b) => a + b.toUpperCase());
}

function getAttrs(obj) {
  const a = obj.attributes;
  return a && typeof a === "object" ? a : null;
}

/** Leading 4-digit model year from title (e.g. "2022 Ram 1500 …"). */
function parseYearFromTitle(title) {
  const m = normStr(title).match(/^(\d{4})\b/);
  return m ? normInt(m[1]) : 0;
}

function extractVinFromPayload(obj) {
  let v = normStr(obj.vin || obj.VIN || obj.vinNumber || obj.vin_number || obj.VINNumber || "");
  const attrs = getAttrs(obj);
  if (!v && attrs) v = normStr(attrs.vin || attrs.VIN || attrs.vinNumber || "");
  const veh = obj.vehicle || obj.Vehicle;
  if (!v && veh && typeof veh === "object") v = normStr(veh.vin || veh.VIN || "");
  return v;
}

/** Ram 1500 / F-150 style tokens mistaken for a make. */
function isSuspiciousMakeToken(s) {
  const t = normStr(s);
  if (!t) return false;
  if (/^\d+$/.test(t)) return true;
  const low = t.toLowerCase();
  if (["1500", "2500", "3500", "4500", "5500", "6500"].includes(low)) return true;
  if (/^f-\d{2,3}$/i.test(low)) return true;
  return false;
}

function getBrandOrManufacturer(obj) {
  return normStr(
    obj.brand ||
      obj.Brand ||
      obj.manufacturer ||
      obj.manufacturerName ||
      obj.oemMake ||
      obj.oem_make ||
      ""
  );
}

/**
 * If make is a model code (1500) or duplicate of model, prefer brand/manufacturer.
 */
function sanitizeMakeModelPair(make, model, obj) {
  let m = normStr(make);
  let mo = normStr(model);
  const brand = getBrandOrManufacturer(obj);

  if (m && mo && m === mo && isSuspiciousMakeToken(m)) {
    m = "";
    mo = "";
  }

  if (isSuspiciousMakeToken(m)) {
    if (brand) {
      if (!mo) mo = m;
      m = properAutomakerLabel(brand);
    } else {
      m = "";
    }
  }

  return { make: m ? properAutomakerLabel(m) : "", model: mo };
}

function parseYmmSegments(obj) {
  const raw = obj.ymm ?? obj.yMMT ?? obj.ymmString ?? obj.ymm_display ?? obj.ymmDisplayName;
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  if (s.includes("|")) {
    const p = s.split("|").map((x) => x.trim()).filter(Boolean);
    if (p.length >= 3) {
      return { year: normInt(p[0]), make: p[1], model: p.slice(2).join(" ") };
    }
    if (p.length === 2) return { year: 0, make: p[0], model: p[1] };
  }
  return null;
}

function extractMakeModelFields(obj) {
  const attrs = getAttrs(obj);

  // Priority 1: explicit make / model / trim (Dealer.com + attributes bag)
  let make = normStr(
    obj.make ||
      obj.Make ||
      (attrs && (attrs.make || attrs.Make)) ||
      ""
  );
  let model = normStr(
    obj.model ||
      obj.Model ||
      (attrs && (attrs.model || attrs.Model)) ||
      ""
  );
  let trim = normStr(
    obj.trim ||
      obj.Trim ||
      obj.trimName ||
      (attrs && (attrs.trim || attrs.trimName)) ||
      ""
  );

  // Priority 2: marketing names (often correct when technical fields are wrong)
  if (!make) make = normStr(obj.marketingMake || obj.marketing_make || "");
  if (!model) model = normStr(obj.marketingModel || obj.marketing_model || "");

  if (!make) {
    make = normStr(
      obj.manufacturer ||
        obj.makeName ||
        obj.make_name ||
        obj.vehicleMake ||
        obj.vehicle_make ||
        ""
    );
  }
  if (!model) {
    model = normStr(
      obj.modelName ||
        obj.model_name ||
        obj.series ||
        obj.vehicleModel ||
        obj.vehicle_model ||
        ""
    );
  }

  const veh = obj.vehicle || obj.Vehicle || obj.vehicleInfo || obj.vehicle_info;
  if (veh && typeof veh === "object") {
    if (!make) {
      make = normStr(
        veh.make || veh.Make || veh.manufacturer || veh.makeName || veh.make_name || veh.brand || ""
      );
    }
    if (!model) {
      model = normStr(
        veh.model || veh.Model || veh.modelName || veh.model_name || veh.series || ""
      );
    }
    if (!trim) trim = normStr(veh.trim || veh.Trim || veh.trimName || "");
  }

  const ymm = parseYmmSegments(obj);
  if (ymm) {
    if (!make && ymm.make) make = ymm.make;
    if (!model && ymm.model) model = ymm.model;
  }

  const arr = obj.trackingAttributes || obj.tracking_attributes;
  if (!make) {
    const x = findTrackingAttr(arr, "make") ?? findTrackingAttr(arr, "Make");
    if (x != null) make = normStr(x);
  }
  if (!model) {
    const x =
      findTrackingAttr(arr, "model") ??
      findTrackingAttr(arr, "Model") ??
      findTrackingAttr(arr, "modelName") ??
      findTrackingAttr(arr, "model_name");
    if (x != null) model = normStr(x);
  }

  return { make: make ? properAutomakerLabel(make) : "", model, trim };
}

function fillMakeModelFromTitle(title, year, make, model) {
  let y = year;
  let m = normStr(make);
  let mo = normStr(model);
  const t = normStr(title);
  if (m && mo) return { make: properAutomakerLabel(m), model: mo, year: y };

  if (t && t !== DEFAULT_STR) {
    const leadYear = t.match(/^(\d{4})\s+/);
    if (leadYear && !y) y = normInt(leadYear[1]);
  }

  if (!t || t === DEFAULT_STR) {
    return { make: m ? properAutomakerLabel(m) : "", model: mo, year: y };
  }

  let rest = t
    .replace(/^pre[-\s]?owned\s+/i, "")
    .replace(/^certified\s+/i, "")
    .replace(/^used\s+/i, "")
    .replace(/^new\s+/i, "")
    .trim();

  const yrStr = y ? String(y) : "";
  if (yrStr && rest.startsWith(yrStr)) rest = rest.slice(yrStr.length).trim();
  const dupYear = rest.match(/^(\d{4})\s+/);
  if (dupYear) {
    if (!y) y = normInt(dupYear[1]);
    rest = rest.slice(5).trim();
  }

  const parts = rest.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    if (!m) m = properAutomakerLabel(parts[0]);
    if (!mo) mo = parts.slice(1).join(" ");
  } else if (parts.length === 1 && !m) {
    m = properAutomakerLabel(parts[0]);
  }
  return { make: m, model: mo, year: y };
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
  const candidates = [];
  function consider(v) {
    if (v == null || v === false) return;
    if (typeof v === "string" && v.toLowerCase().includes("contact")) return;
    const n = normFloat(v);
    if (n > 0) candidates.push(n);
  }

  const tracking = obj.trackingPricing || obj.tracking_pricing;
  const pricing = obj.pricing && typeof obj.pricing === "object" ? obj.pricing : null;
  consider(tracking && tracking.internetPrice);
  consider(tracking && tracking.internetPriceUnformatted);
  consider(tracking && tracking.internet_price);
  consider(pricing && pricing.internetPrice);
  consider(pricing && pricing.internet_price);
  consider(pricing && pricing.finalPrice);
  consider(pricing && pricing.final_price);
  consider(pricing && pricing.salePrice);
  consider(pricing && pricing.sale_price);
  consider(pricing && pricing.msrp);
  consider(pricing && pricing.retailPrice);
  consider(pricing && pricing.retail_price);
  consider(obj.price);
  consider(obj.internetPrice);
  consider(obj.msrp);

  const arr = obj.trackingAttributes || obj.tracking_attributes;
  if (Array.isArray(arr)) {
    const v2 = findTrackingAttr(arr, "price") ?? findTrackingAttr(arr, "internetPrice");
    consider(v2);
    const ms = findTrackingAttr(arr, "msrp");
    consider(ms);
  }

  if (!candidates.length) return 0;
  return Math.round(Math.min(...candidates));
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
    "photoUrl",
    "thumbnailUri",
    "thumbUrl",
  ];
  for (const k of keys) {
    const u = item[k];
    if (u && typeof u === "string" && u.trim()) return cleanImageUrl(u.trim(), baseUrl);
  }
  return "";
}

function imageResolutionScore(url) {
  const u = (url || "").toLowerCase();
  let s = 0;
  if (u.includes("xxlarge") || u.includes("xlarge")) s += 120;
  else if (u.includes("large") || u.includes("full") || u.includes("hires")) s += 60;
  if (u.includes("thumb") || u.includes("thumbnail") || u.includes("small")) s -= 40;
  return s + Math.min((url || "").length, 400) / 400;
}

function extractGallery(obj, baseUrl) {
  const out = [];
  const seen = new Set();

  function pushOne(raw) {
    if (!raw || typeof raw !== "string") return;
    const c = cleanImageUrl(raw.trim(), baseUrl);
    if (c && !seen.has(c)) {
      seen.add(c);
      out.push(c);
    }
  }

  function pushDelimited(s) {
    if (!s || typeof s !== "string") return;
    for (const part of s.split(/[,;]/)) pushOne(part.trim());
  }

  const media = obj.media;
  if (media && typeof media === "object") {
    const photos = media.photos || media.Photos || media.images || media.imageList;
    if (Array.isArray(photos)) {
      for (const item of photos) {
        if (typeof item === "string") pushOne(item);
        else if (item && typeof item === "object") {
          const u = bestImageUrlFromItem(item, baseUrl);
          if (u) pushOne(u);
        }
      }
    }
    if (typeof media.photoUrl === "string") pushOne(media.photoUrl);
    if (typeof media.primaryPhotoUrl === "string") pushOne(media.primaryPhotoUrl);
  }

  if (typeof obj.photoUrl === "string") pushOne(obj.photoUrl);
  if (typeof obj.primaryPhotoUrl === "string") pushOne(obj.primaryPhotoUrl);

  const iu = obj.imageUrls ?? obj.image_urls ?? obj.imageURLList ?? obj.imageUrlList;
  if (typeof iu === "string") pushDelimited(iu);
  else if (Array.isArray(iu)) {
    for (const x of iu) {
      if (typeof x === "string") pushOne(x);
      else if (x && typeof x === "object") {
        const u = bestImageUrlFromItem(x, baseUrl) || normStr(x.url || x.URL || x.uri);
        if (u) pushOne(u);
      }
    }
  }

  const images = obj.images || obj.Images;
  if (Array.isArray(images) && images.length > 0) {
    for (const item of images) {
      if (typeof item === "string") pushOne(item);
      else if (item && typeof item === "object") {
        const u = bestImageUrlFromItem(item, baseUrl);
        if (u) pushOne(u);
      }
    }
  }

  if (!out.length) {
    for (const key of ["featuredImage", "featured_image", "thumbnail", "Thumbnail", "primaryImage", "primary_image"]) {
      const val = obj[key];
      if (typeof val === "string" && val.trim()) {
        pushOne(val);
        break;
      }
      if (val && typeof val === "object") {
        const u = val.uri || val.url || val.URL;
        if (u && typeof u === "string" && u.trim()) {
          pushOne(u);
          break;
        }
      }
    }
  }

  if (!out.length) return [];

  out.sort((a, b) => imageResolutionScore(b) - imageResolutionScore(a));
  return out;
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

  let vin = extractVinFromPayload(obj);
  if (!vin) vin = `unknown-${Math.abs(hashCode(JSON.stringify(obj))) % 1e8}`;

  let stockNumber = safeStr(obj.stockNumber, "");
  if (stockNumber === DEFAULT_STR) stockNumber = "";

  const attrs = getAttrs(obj);
  let year = normInt(obj.year ?? obj.modelYear ?? obj.model_year);
  if (!year && attrs) year = normInt(attrs.year ?? attrs.modelYear ?? attrs.model_year);

  const ymmEarly = parseYmmSegments(obj);
  if (ymmEarly && ymmEarly.year && !year) year = ymmEarly.year;

  const titleProbe = extractTitle(obj, year || 0, "", "");
  if (!year) {
    const yt = parseYearFromTitle(titleProbe);
    if (yt) year = yt;
  }

  let { make, model, trim: trimFromFields } = extractMakeModelFields(obj);
  const mkM = normStr(obj.marketingMake || obj.marketing_make || "");
  const mdM = normStr(obj.marketingModel || obj.marketing_model || "");
  if (isSuspiciousMakeToken(make) && mkM) {
    if (!normStr(model)) model = make;
    make = properAutomakerLabel(mkM);
    if (!normStr(model) && mdM) model = mdM;
  }
  const sanitized = sanitizeMakeModelPair(make, model, obj);
  make = sanitized.make;
  model = sanitized.model;

  let title = extractTitle(obj, year, make, model);
  const filled = fillMakeModelFromTitle(title, year, make, model);
  make = filled.make || make;
  model = filled.model || model;
  if (filled.year) year = filled.year;
  title = extractTitle(obj, year, make, model);

  let trim = normStr(trimFromFields);
  if (!trim) trim = normStr(obj.trim || obj.Trim || obj.trimName || "");
  trim = trim || "";

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
    if (v != null && String(v).trim()) return sqlOptionalStr(v);
    return sqlOptionalStr(obj.exteriorColor || obj.exterior_color);
  })();

  const fuelType = sqlOptionalStr(obj.fuelType || obj.fuel_type);

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

  const _detail_url = pickVehicleDetailUrl(obj, baseUrl);

  return {
    vin,
    stock_number: stockNumber,
    year,
    make,
    model,
    trim: sqlOptionalStr(normStr(trim) || normStr(obj.trim || obj.Trim || obj.trimName || "")),
    title,
    price,
    mileage,
    image_url: imageUrl,
    gallery,
    dealer_id: dealerId,
    dealer_name: dealerName,
    dealer_url: sqlOptionalStr(dealerUrl),
    zip_code: sqlOptionalStr(obj.zipCode || obj.zip_code),
    fuel_type: fuelType,
    transmission: sqlOptionalStr(obj.transmission || obj.transmissionType),
    drivetrain: sqlOptionalStr(obj.drivetrain || obj.driveType),
    exterior_color: exteriorColor,
    interior_color: extractInteriorColorFromAttrs(arr, obj),
    body_style: extractBodyStyleFromAttrs(arr, obj),
    carfax_url: sqlOptionalStr(carfaxUrl),
    history_highlights: extractHistoryHighlights(obj),
    cylinders: cylinders || null,
    msrp: msrp > 0 ? msrp : null,
    ...( _detail_url ? { _detail_url } : {}),
  };
}

function hashCode(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  return h;
}

function recordScrapeSample(rawObj, parsed) {
  if (scrapeSamplesBuffer.length >= MAX_SCRAPE_SAMPLES) return;
  try {
    scrapeSamplesBuffer.push({
      vin: parsed.vin,
      raw_json_sample: JSON.parse(JSON.stringify(rawObj)),
      parsed_snapshot: {
        vin: parsed.vin,
        year: parsed.year,
        make: parsed.make,
        model: parsed.model,
        trim: parsed.trim,
        price: parsed.price,
        image_url: parsed.image_url,
        gallery_preview: Array.isArray(parsed.gallery) ? parsed.gallery.slice(0, 3) : [],
      },
    });
  } catch {
    /* ignore circular / non-JSON data */
  }
}

async function flushScrapeSamplesFile() {
  if (!scrapeSamplesBuffer.length) return;
  await fs.mkdir(DEBUG_DIR, { recursive: true });
  await fs.writeJson(
    SCRAPE_SAMPLES_PATH,
    { generated_at: new Date().toISOString(), samples: scrapeSamplesBuffer },
    { spaces: 2 }
  );
}

function parseJsonList(data, baseUrl, dealerId, dealerName, dealerUrl) {
  const items = findVehicleList(data);
  if (!items) return [];
  const out = [];
  for (const obj of items) {
    if (!obj || typeof obj !== "object") continue;
    if (!hasVehicleIdent(obj)) continue;
    const m = mapVehicle(obj, baseUrl, dealerId, dealerName, dealerUrl);
    if (m) {
      recordScrapeSample(obj, m);
      out.push(m);
    }
  }
  if (out.length > 0) return out;
  for (const obj of items) {
    if (!obj || typeof obj !== "object") continue;
    const m = mapVehicle(obj, baseUrl, dealerId, dealerName, dealerUrl);
    if (m) {
      recordScrapeSample(obj, m);
      out.push(m);
    }
  }
  return out;
}

function isValidVehicleList(body) {
  const list = findVehicleList(body);
  return list != null && list.length > 0;
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

function isDealerComSearchOrVehiclesUrl(href) {
  try {
    const u = new URL(href);
    return /\/api\/inventory\/v1\/(search|vehicles)/i.test(u.pathname);
  } catch {
    return false;
  }
}

/** Rewrite getInventory / Dealer.com v1 search GET URLs to use large pageSize */
function rewriteInventoryUrl(urlString) {
  try {
    const u = new URL(urlString);
    const isGetInv = /getInventory/i.test(u.href);
    const isSearch = isDealerComSearchOrVehiclesUrl(u.href);
    if (!isGetInv && !isSearch) {
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
    if (req.method() === "GET" && (/getInventory/i.test(url) || isDealerComSearchOrVehiclesUrl(url))) {
      const next = rewriteInventoryUrl(url);
      if (next !== url) return req.continue({ url: next });
    }
    return req.continue();
  });
}

/**
 * Inventory JSON intercept gate (same URL policy as Python ``intercept_url_allowed`` +
 * ``config/scanner_intercept_policy.json``). Pass dealer base URL (no trailing slash) from manifest.
 */
function isInventoryInterceptJsonResponse(response, dealerBaseUrl) {
  return responseLooksLikeInventoryJsonIntercept(response, dealerBaseUrl);
}

function readWarmupPostGotoSec() {
  const v = parseFloat(String(process.env.SCANNER_WARMUP_POST_GOTO_SEC || "4").trim());
  return Number.isFinite(v) && v >= 0 ? v : 4;
}

function readWarmupScrollSec() {
  const v = parseFloat(String(process.env.SCANNER_WARMUP_SCROLL_SEC || "1").trim());
  return Number.isFinite(v) && v >= 0 ? v : 1;
}

function readWarmupSignalTimeoutMs() {
  const v = parseInt(String(process.env.SCANNER_WARMUP_SIGNAL_TIMEOUT_MS || "12000").trim(), 10);
  return Number.isFinite(v) && v >= 0 ? v : 12000;
}

/**
 * Same idea as Python ``_warmup_settle_after_base_goto``: race first gated JSON response vs idle cap,
 * optional DOM probes, mid-page scroll, then tail sleep (SCANNER_WARMUP_* env vars).
 */
async function warmupSettleAfterGoto(page, dealerBaseUrl, dealerNameForLog = "") {
  const maxIdleSec = readWarmupPostGotoSec();
  const scrollSec = readWarmupScrollSec();
  const signalMs = readWarmupSignalTimeoutMs();

  const armResponse = () =>
    page
      .waitForResponse((r) => responseLooksLikeInventoryJsonIntercept(r, dealerBaseUrl), {
        timeout: Math.max(1, signalMs),
      })
      .catch(() => null);

  const armCap = () => sleep(Math.max(1, Math.ceil(maxIdleSec * 1000)));

  if (maxIdleSec > 0 && signalMs > 0) {
    await Promise.race([armResponse(), armCap()]);
  } else if (signalMs > 0) {
    await armResponse();
  } else if (maxIdleSec > 0) {
    await armCap();
  }

  const domCsv = (process.env.SCANNER_WARMUP_DOM_SELECTORS || "").trim();
  const defaultDom =
    "[data-vehicle],[data-vin],[data-vehicle-id],.vehicle-card,.inventory-vehicle,a[href*='/inventory/']";
  const selectors = (domCsv || defaultDom)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .slice(0, 8);

  const label = dealerNameForLog ? `[warmup] ${dealerNameForLog}` : "[warmup]";
  for (const sel of selectors) {
    try {
      await page.waitForSelector(sel, { timeout: 1200 });
      console.info(`${label} — DOM signal ${sel.slice(0, 72)}`);
      break;
    } catch {
      /* try next selector */
    }
  }

  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
  if (scrollSec > 0) {
    await sleep(Math.ceil(scrollSec * 1000));
  }
}

async function scrollLazyLoadInventory(page) {
  await page.evaluate(() => {
    const want = /view all|search/i;
    const candidates = [
      ...document.querySelectorAll("button, a, [role='button'], input[type='submit'], input[type='button']"),
    ];
    for (const el of candidates) {
      const t = (el.textContent || el.value || el.getAttribute("aria-label") || "").trim();
      if (!t || !want.test(t)) continue;
      if (el.offsetParent === null) continue;
      try {
        el.click();
        return;
      } catch {
        /* ignore */
      }
    }
  });
  await sleep(800);
  await page.evaluate(() => {
    window.scrollTo(0, document.body.scrollHeight);
  });
  await sleep(3000);
}

function getPageGotoWaitUntil(profile) {
  return profile === "resilient" ? "networkidle0" : "domcontentloaded";
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
async function runDealerSlow(browser, dealer, opts = {}) {
  const name = dealer.name || "";
  const url = String(dealer.url || "").replace(/\/$/, "");
  const dealerId = dealer.dealer_id || "";
  const profile = opts.profile || "default";
  const scrollLazy = opts.scrollLazyLoad !== false && profile !== "bare";
  const intercepted = [];
  const vinToEp = new Map();
  const page = await browser.newPage();
  await page.setUserAgent(randomUserAgent());
  await page.setViewport({ width: 1920, height: 1080 });
  attachAnalyticsEpResponseListener(page, vinToEp);

  page.on("response", async (response) => {
    try {
      if (!isInventoryInterceptJsonResponse(response, url)) return;
      const u = response.url();
      console.log("[dev] Intercepted URL:", u);
      const body = await response.json();
      if (isValidVehicleList(body)) {
        intercepted.push(body);
        console.info(`[slow] Intercepted: ${name} — ${u.slice(0, 80)}`);
      }
    } catch {
      /* ignore */
    }
  });

  try {
    const slowWait = getPageGotoWaitUntil(profile);
    const slowNavTimeout = profile === "resilient" ? 120000 : 30000;
    await page.goto(url, { waitUntil: slowWait, timeout: slowNavTimeout });
    await warmupSettleAfterGoto(page, url, name);
    if (scrollLazy) {
      await scrollLazyLoadInventory(page);
    }

    const invWait = getPageGotoWaitUntil(profile);
    const invNavTimeout = profile === "resilient" ? 120000 : 20000;
    for (const invPath of INVENTORY_PATHS) {
      const fullUrl = url + invPath;
      try {
        await page.goto(fullUrl, { waitUntil: invWait, timeout: invNavTimeout });
        await page
          .waitForResponse((r) => isInventoryInterceptJsonResponse(r, url) && r.status() === 200, { timeout: 25000 })
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
          await page.waitForResponse((r) => isInventoryInterceptJsonResponse(r, url), { timeout: 20000 }).catch(() => null);
        }
      } catch (e) {
        console.warn(`[slow] ${fullUrl}: ${e.message}`);
      }
    }

    const byVin = new Map();
    for (const b of intercepted) {
      for (const v of parseJsonList(b, url, dealerId, name, url)) {
        v.dealer_name = name;
        v.dealer_url = url;
        if (v.vin && !byVin.has(v.vin)) byVin.set(v.vin, v);
      }
    }
    const mergedList = [...byVin.values()];
    if (SCANNER_VDP_EP_MAX > 0 && mergedList.length) {
      await runBatchVdpExtraction(page, mergedList, vinToEp, url, SCANNER_VDP_EP_MAX);
    }
    attachEpToVehicles(mergedList, vinToEp);
    await page.close();
    return mergedList;
  } catch (e) {
    await page.close().catch(() => {});
    throw e;
  }
}

function pickBestInventoryCapture(captures) {
  if (!captures || !captures.length) return null;
  const searchHits = captures.filter((c) => isDealerComSearchOrVehiclesUrl(c.url));
  const pool = searchHits.length ? searchHits : captures;
  let best = pool[0];
  let n = countVehicles(best.body);
  for (const c of pool) {
    const cv = countVehicles(c.body);
    if (cv > n) {
      best = c;
      n = cv;
    }
  }
  return best;
}

/** Turbo: block assets, rewrite pageSize, intercept full search API + getInventory, optional pagination */
async function runDealerTurbo(browser, dealer, opts = {}) {
  const name = dealer.name || "";
  const url = String(dealer.url || "").replace(/\/$/, "");
  const dealerId = dealer.dealer_id || "";
  const profile = opts.profile || "default";
  const scrollLazy = opts.scrollLazyLoad !== false && profile !== "bare";

  const page = await browser.newPage();
  await page.setUserAgent(randomUserAgent());
  await page.setViewport({ width: 1920, height: 1080 });
  await setupRequestBlocking(page);

  const vinToEp = new Map();
  attachAnalyticsEpResponseListener(page, vinToEp);

  const captured = [];
  const seenRespUrls = new Set();
  page.on("response", async (response) => {
    try {
      if (!isInventoryInterceptJsonResponse(response, url) || response.status() !== 200) return;
      const u = response.url();
      console.log("[dev] Intercepted URL:", u);
      if (seenRespUrls.has(u)) return;
      const body = await response.json();
      if (!isValidVehicleList(body)) return;
      seenRespUrls.add(u);
      captured.push({ url: u, body });
      console.info(`[turbo] Intercepted: ${name} — ${u.slice(0, 120)}`);
    } catch {
      /* ignore */
    }
  });

  try {
    const waitUntil = getPageGotoWaitUntil(profile);
    const navTimeout = profile === "resilient" ? 120000 : 45000;
    for (const invPath of INVENTORY_PATHS) {
      const fullUrl = url + invPath;
      await page.goto(fullUrl, { waitUntil, timeout: navTimeout });
      await warmupSettleAfterGoto(page, url, name);
      if (scrollLazy) {
        await scrollLazyLoadInventory(page);
      }
    }

    const bestCap = pickBestInventoryCapture(captured);
    let firstInvUrl = bestCap ? bestCap.url : null;
    let firstBody = bestCap ? bestCap.body : null;

    if (!firstBody && captured.length) {
      const fb = captured.reduce((a, c) =>
        countVehicles(c.body) > countVehicles(a.body) ? c : a
      );
      firstBody = fb.body;
      firstInvUrl = fb.url;
    }

    let bodiesToParse = [];
    if (firstInvUrl && firstBody) {
      bodiesToParse = await fetchAdditionalInventoryPages(page, firstInvUrl, firstBody);
    }

    const byVin = new Map();
    const pushBody = (b) => {
      for (const v of parseJsonList(b, url, dealerId, name, url)) {
        v.dealer_name = name;
        v.dealer_url = url;
        if (v.vin && !byVin.has(v.vin)) byVin.set(v.vin, v);
      }
    };
    for (const b of bodiesToParse) pushBody(b);
    for (const { body } of captured) pushBody(body);

    const mergedList = [...byVin.values()];
    if (SCANNER_VDP_EP_MAX > 0 && mergedList.length) {
      await runBatchVdpExtraction(page, mergedList, vinToEp, url, SCANNER_VDP_EP_MAX);
    }
    attachEpToVehicles(mergedList, vinToEp);

    await page.close();

    return mergedList;
  } catch (e) {
    await page.close().catch(() => {});
    throw e;
  }
}

async function runDealer(browser, dealer, opts = {}) {
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

  scrapeSamplesBuffer.length = 0;

  try {
    console.info(`[turbo] Starting: ${name}`);
    const vehicles = await runDealerTurbo(browser, dealer, opts);
    if (vehicles.length) {
      console.info(`[turbo] ${name}: ${vehicles.length} vehicles`);
      return vehicles;
    }
    throw new Error("No vehicles in turbo mode");
  } catch (e) {
    console.warn(`[turbo] ${name} failed (${e.message}), falling back to slow path`);
    try {
      const vehicles = await runDealerSlow(browser, dealer, opts);
      console.info(`[slow] ${name}: ${vehicles.length} vehicles`);
      return vehicles;
    } catch (e2) {
      console.error(`[slow] ${name} failed:`, e2);
      return [];
    }
  }
}

// Function to get enhanced data collection guidance from AI agent
async function getDealerDataGuidance(browser, dealer) {
  try {
    const page = await browser.newPage();
    await page.setUserAgent(randomUserAgent());
    
    // Navigate to dealer website to get content for analysis
    await page.goto(dealer.url, { waitUntil: 'networkidle0', timeout: 30000 });
    
    // Get page content for agent analysis
    const content = await page.content();
    await page.close();
    
    // Process with data collection agent (this would normally call the Python agent)
    // For now, we'll return a basic guidance structure
    const guidance = {
      dealer_id: dealer.dealer_id,
      website_analysis: {
        primary_data_sources: ["Vehicle listing JSON APIs", "HTML vehicle detail pages"],
        additional_fields: ["Engine specifications", "Fuel efficiency ratings", "Warranty information"],
        special_considerations: ["Check for vehicle history reports", "Look for special offers"],
        image_collection_tips: ["Capture all gallery images", "Include detailed engine bay shots"],
        javascript_handling: ["Handle dynamic content loading", "Wait for AJAX calls"],
        fallback_strategies: ["Use fallback HTML parsing", "Try different API endpoints"]
      },
      recommended_data_fields: [
        {
          field_name: "engine_displacement",
          data_type: "number",
          source: "website",
          priority: "high",
          notes: "Engine size in liters"
        },
        {
          field_name: "fuel_efficiency",
          data_type: "number",
          source: "website",
          priority: "medium",
          notes: "City/Highway MPG ratings"
        },
        {
          field_name: "warranty_info",
          data_type: "string",
          source: "website",
          priority: "medium",
          notes: "Remaining warranty details"
        },
        {
          field_name: "features_list",
          data_type: "array",
          source: "website",
          priority: "high",
          notes: "Additional vehicle features"
        }
      ],
      enhanced_extraction_tips: [
        {
          tip: "Look for technical specifications sections",
          example: "Search for 'Vehicle Specifications' or 'Features' sections",
          website_section: "Vehicle detail page"
        },
        {
          tip: "Check for service history and maintenance records",
          example: "Look for 'Service Records' or 'Maintenance History' tabs",
          website_section: "Vehicle detail page"
        }
      ],
      data_quality_indicators: [
        {
          indicator: "Complete specification set",
          importance: "high",
          how_to_verify: "Compare against manufacturer data"
        },
        {
          indicator: "Vehicle history reports",
          importance: "medium",
          how_to_verify: "Check for CarFax/vehicle history links"
        }
      ]
    };
    
    return guidance;
  } catch (error) {
    console.warn(`Failed to get agent guidance for ${dealer.name}:`, error);
    return null;
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
    ["dealership_registry_id", "INTEGER"],
    ["body_style", "TEXT"],
    ["engine_description", "TEXT"],
    ["condition", "TEXT"],
    ["source_url", "TEXT"],
    ["mpg_city", "INTEGER"],
    ["mpg_highway", "INTEGER"],
    ["is_cpo", "INTEGER"],
    ["model_full_raw", "TEXT"],
    ["data_quality_score", "REAL"],
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
  return sqlOptionalStr(t) == null;
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
      exterior_color, interior_color, stock_number, gallery, carfax_url, history_highlights, msrp,
      dealership_registry_id,
      body_style, engine_description, condition, source_url,
      mpg_city, mpg_highway, is_cpo, model_full_raw, data_quality_score
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(vin) DO UPDATE SET
      title=excluded.title, year=excluded.year, make=excluded.make,
      model=excluded.model, trim=excluded.trim, price=excluded.price,
      mileage=excluded.mileage, image_url=excluded.image_url,
      dealer_name=excluded.dealer_name, dealer_url=excluded.dealer_url,
      dealer_id=excluded.dealer_id, scraped_at=excluded.scraped_at,
      zip_code=excluded.zip_code, fuel_type=excluded.fuel_type,
      cylinders=COALESCE(excluded.cylinders, cylinders), transmission=excluded.transmission,
      drivetrain=excluded.drivetrain, exterior_color=excluded.exterior_color,
      interior_color=excluded.interior_color, stock_number=excluded.stock_number,
      gallery=excluded.gallery, carfax_url=excluded.carfax_url, history_highlights=excluded.history_highlights,
      msrp=excluded.msrp,
      dealership_registry_id=COALESCE(excluded.dealership_registry_id, dealership_registry_id),
      body_style=COALESCE(excluded.body_style, body_style),
      engine_description=COALESCE(excluded.engine_description, engine_description),
      condition=COALESCE(excluded.condition, condition),
      source_url=COALESCE(excluded.source_url, source_url),
      mpg_city=COALESCE(excluded.mpg_city, mpg_city),
      mpg_highway=COALESCE(excluded.mpg_highway, mpg_highway),
      is_cpo=COALESCE(excluded.is_cpo, is_cpo),
      model_full_raw=COALESCE(excluded.model_full_raw, model_full_raw),
      data_quality_score=COALESCE(excluded.data_quality_score, data_quality_score)
  `;

  const mpgCity = v.mpg_city != null && normInt(v.mpg_city) > 0 ? normInt(v.mpg_city) : null;
  const mpgHwy = v.mpg_highway != null && normInt(v.mpg_highway) > 0 ? normInt(v.mpg_highway) : null;
  const isCpo =
    v.is_cpo === 0 || v.is_cpo === 1 ? normInt(v.is_cpo) : v.is_cpo === true ? 1 : v.is_cpo === false ? 0 : null;

  const params = [
    v.vin,
    title,
    v.year ?? null,
    v.make || "",
    v.model || "",
    sqlOptionalStr(v.trim),
    price,
    mileage,
    v.image_url || "",
    v.dealer_name || "",
    sqlOptionalStr(v.dealer_url),
    v.dealer_id || "",
    now,
    sqlOptionalStr(v.zip_code),
    sqlOptionalStr(v.fuel_type),
    v.cylinders ?? null,
    sqlOptionalStr(v.transmission),
    sqlOptionalStr(v.drivetrain),
    sqlOptionalStr(v.exterior_color),
    sqlOptionalStr(v.interior_color),
    v.stock_number || "",
    galleryJson,
    sqlOptionalStr(v.carfax_url),
    highlightsJson,
    v.msrp != null && v.msrp > 0 ? v.msrp : null,
    v.dealership_registry_id != null ? v.dealership_registry_id : null,
    sqlOptionalStr(v.body_style),
    sqlOptionalStr(v.engine_description),
    sqlOptionalStr(v.condition),
    sqlOptionalStr(v.source_url),
    mpgCity,
    mpgHwy,
    isCpo,
    sqlOptionalStr(v.model_full_raw),
    v.data_quality_score != null && Number.isFinite(Number(v.data_quality_score))
      ? Number(v.data_quality_score)
      : null,
  ];

  await run(db, sql, params);
}

async function upsertAll(db, vehicles) {
  const byVin = {};
  for (const v of vehicles) {
    const vin = normStr(v.vin);
    if (vin) byVin[vin] = v;
  }
  let list = Object.values(byVin);
  for (const v of list) {
    await applySelfCorrection(db, v);
  }
  const py = process.env.PYTHON || process.env.PYTHON3 || "python3";
  const mergeScript = path.join(ROOT, "scripts", "merge_ep_batch.py");
  try {
    if (fs.existsSync(mergeScript)) {
      const r = spawnSync(py, [mergeScript], {
        cwd: ROOT,
        input: JSON.stringify(list),
        encoding: "utf8",
        maxBuffer: 50 * 1024 * 1024,
        env: { ...process.env, PYTHONPATH: ROOT },
      });
      if (r.status === 0 && r.stdout) {
        list = JSON.parse(r.stdout);
      } else if (r.status !== 0) {
        console.warn("[ep] merge_ep_batch failed:", (r.stderr || r.stdout || "").slice(0, 400));
      }
    }
  } catch (e) {
    console.warn("[ep] merge_ep_batch error:", e.message || e);
  }
  let count = 0;
  for (const v of list) {
    await upsertVehicle(db, v);
    count++;
  }
  await flushScrapeSamplesFile();
  return count;
}

/** Machine-readable discovery line for the Flask dev dashboard (stdout). */
function emitDiscovery(payload) {
  console.log(`DISCOVERY:${JSON.stringify(payload)}`);
}

function getLaunchOptions(profile, headed = false) {
  const p = profile || "default";
  const common = {
    headless: headed ? false : "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  };
  if (p === "resilient") {
    common.args.push(
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process",
      "--window-size=1920,1080"
    );
    common.ignoreDefaultArgs = ["--enable-automation"];
  } else if (p === "bare") {
    common.args = ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"];
  }
  return common;
}

function looksLikeDomainOrSlugName(name, pageUrl) {
  const n = normStr(name);
  if (!n) return true;
  if (/^[a-z0-9-]+\.[a-z]{2,}$/i.test(n)) return true;
  try {
    const host = new URL(pageUrl).hostname
      .replace(/^www\./i, "")
      .split(".")[0]
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
    const nn = n.toLowerCase().replace(/[^a-z0-9]/g, "");
    if (host && nn === host) return true;
  } catch {
    /* ignore */
  }
  return false;
}

/**
 * Optional Crawl4AI (Python) pass — better JSON-LD / footer capture on some DMS sites.
 * Requires: pip install crawl4ai; then ./scripts/install_scraper_browsers.sh
 * (or: python3 -m playwright install chromium && python3 -m patchright install chromium).
 * Disable with DISABLE_CRAWL4AI=1.
 */
function runCrawl4aiDiscoverySubprocess(pageUrl) {
  if (process.env.DISABLE_CRAWL4AI === "1" || process.env.DISABLE_CRAWL4AI === "true") return null;
  const py = process.env.PYTHON || process.env.PYTHON3 || "python3";
  const r = spawnSync(py, ["-m", "scrapers.crawl4ai_discovery", pageUrl], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 130000,
    maxBuffer: 25 * 1024 * 1024,
  });
  if (r.error) {
    console.warn(`[crawl4ai] ${r.error.message}`);
    return null;
  }
  const lines = (r.stdout || "").split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    const ln = lines[i].trim();
    if (ln.startsWith("CRAWL4AI_DISCOVERY:")) {
      try {
        return JSON.parse(ln.slice("CRAWL4AI_DISCOVERY:".length));
      } catch {
        return null;
      }
    }
  }
  return null;
}

function enrichMetadataWithCrawl4ai(pageUrl, meta) {
  const navFail = meta && meta.retryable && meta.error === "navigation";
  const missingLoc = !navFail && (!normStr(meta.city) || !normStr(meta.state));
  const badName = !navFail && looksLikeDomainOrSlugName(normStr(meta.name), pageUrl);
  if (!navFail && !missingLoc && !badName) return meta;

  emitDiscovery({
    step: "crawl4ai",
    message: "Running Crawl4AI for richer page capture (headless Chromium)…",
  });
  const c4 = runCrawl4aiDiscoverySubprocess(pageUrl);
  if (!c4) {
    emitDiscovery({ step: "crawl4ai", message: "Crawl4AI subprocess returned no payload." });
    return meta;
  }
  if (c4.error === "crawl4ai_not_installed") {
    emitDiscovery({ step: "crawl4ai", message: "Crawl4AI not installed (pip install crawl4ai)." });
    return meta;
  }

  const cn = normStr(c4.name);
  const cc = normStr(c4.city);
  const cs = normStr(c4.state).toUpperCase().slice(0, 2);

  if (navFail) {
    if (cc && cs.length === 2) {
      const nameOut = cn || normStr(meta.name) || slugFromUrl(pageUrl);
      emitDiscovery({
        step: "crawl4ai",
        message: "Recovered dealer metadata after Puppeteer navigation issue.",
      });
      return { name: nameOut, city: cc, state: cs, sources: [].concat(c4.sources || []) };
    }
    return meta;
  }

  let name = normStr(meta.name);
  let city = normStr(meta.city);
  let state = normStr(meta.state).toUpperCase().slice(0, 2);
  if ((looksLikeDomainOrSlugName(name, pageUrl) || !name) && cn) name = cn;
  if (!city && cc) city = cc;
  if ((!state || state.length !== 2) && cs.length === 2) state = cs;
  const sources = [].concat(meta.sources || []);
  if (c4.sources && c4.sources.length) sources.push("crawl4ai");
  return { name, city, state, sources };
}

/**
 * Run the Python Crawl4AI inventory sniffer for non-Dealer.com sites.
 * Returns the parsed CRAWL4AI_INVENTORY payload, or null on failure.
 */
function runCrawl4aiInventorySubprocess(pageUrl) {
  if (process.env.DISABLE_CRAWL4AI === "1" || process.env.DISABLE_CRAWL4AI === "true") return null;
  const py = process.env.PYTHON || process.env.PYTHON3 || "python3";
  const r = spawnSync(py, ["-m", "scrapers.crawl4ai_inventory", pageUrl], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 180000,
    maxBuffer: 50 * 1024 * 1024,
  });
  if (r.error) {
    console.warn(`[crawl4ai_inventory] ${r.error.message}`);
    return null;
  }
  const lines = (r.stdout || "").split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    const ln = lines[i].trim();
    if (ln.startsWith("CRAWL4AI_INVENTORY:")) {
      try {
        return JSON.parse(ln.slice("CRAWL4AI_INVENTORY:".length));
      } catch {
        return null;
      }
    }
  }
  return null;
}

/**
 * Map a vehicle object returned by crawl4ai_inventory.py to the internal
 * schema expected by upsertVehicle.
 */
function mapCrawl4aiVehicle(v, dealer, dealerId) {
  const year = normInt(v.year) || null;
  return {
    vin: normStr(v.vin).toUpperCase(),
    year,
    make: normStr(v.make),
    model: normStr(v.model),
    trim: normStr(v.trim),
    price: normFloat(String(v.price || "0")),
    mileage: normInt(v.mileage),
    image_url: normStr(v.image_url) || FALLBACK_IMAGE_URL,
    dealer_name: dealer.name || dealerId,
    dealer_url: dealer.url || "",
    dealer_id: dealerId,
    zip_code: sqlOptionalStr(v.zip_code),
    fuel_type: sqlOptionalStr(v.fuel_type),
    cylinders: normInt(v.cylinders) || null,
    transmission: sqlOptionalStr(v.transmission),
    drivetrain: sqlOptionalStr(v.drivetrain),
    exterior_color: sqlOptionalStr(v.exterior_color),
    interior_color: sqlOptionalStr(v.interior_color),
    stock_number: normStr(v.stock_number) || "",
    gallery: [],
    carfax_url: "",
    history_highlights: [],
    msrp: null,
    dealership_registry_id: null,
  };
}

function extractCityStateFromText(text) {
  if (!text || typeof text !== "string") return { city: "", state: "" };
  const patterns = [
    /\b([A-Za-z][A-Za-z\s'.-]{2,40}),\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?\b/g,
    /\b([A-Za-z][A-Za-z\s'.-]{2,40}),\s*([A-Z]{2})\b(?!\s*\d)/g,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text)) !== null) {
      const city = normStr(m[1]);
      const st = normStr(m[2]).toUpperCase().slice(0, 2);
      if (city.length >= 2 && st.length === 2) return { city, state: st };
    }
  }
  return { city: "", state: "" };
}

/**
 * Load dealer homepage and infer name, city, state from JSON-LD, Open Graph, title, footer.
 */
async function discoverDealerMetadata(page, url, profile = "default") {
  emitDiscovery({ step: "scan", message: "Loading page for structured data and meta tags…" });
  await page.setUserAgent(randomUserAgent());
  await page.setViewport({ width: 1920, height: 1080 });

  try {
    const discWait = getPageGotoWaitUntil(profile);
    const discTimeout = profile === "resilient" ? 120000 : 90000;
    await page.goto(url, { waitUntil: discWait, timeout: discTimeout });
    await sleep(800);
  } catch (e) {
    emitDiscovery({ step: "error", message: `Navigation failed: ${e.message || e}` });
    return {
      name: "",
      city: "",
      state: "",
      error: "navigation",
      retryable: true,
      detail: String(e.message || e),
    };
  }

  emitDiscovery({ step: "parse", message: "Parsing JSON-LD (AutomotiveBusiness), Open Graph, title, and footer…" });

  const extracted = await page.evaluate(() => {
    function walkJsonLd(obj, out) {
      if (obj == null) return;
      if (Array.isArray(obj)) {
        for (const x of obj) walkJsonLd(x, out);
        return;
      }
      if (typeof obj === "object") {
        out.push(obj);
        if (obj["@graph"]) walkJsonLd(obj["@graph"], out);
        for (const v of Object.values(obj)) {
          if (v && typeof v === "object") walkJsonLd(v, out);
        }
      }
    }

    let name = "";
    let city = "";
    let state = "";
    const sources = [];

    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const s of scripts) {
      let parsed;
      try {
        parsed = JSON.parse(s.textContent || "{}");
      } catch {
        continue;
      }
      const nodes = [];
      walkJsonLd(parsed, nodes);
      for (const node of nodes) {
        if (!node || typeof node !== "object") continue;
        const types = [].concat(node["@type"] || []);
        const auto = types.some((t) =>
          /AutomotiveBusiness|AutoDealer|CarDealer|MotorcycleDealer/i.test(String(t))
        );
        const org = /Organization|LocalBusiness|Store|AutomotiveBusiness/i.test(
          types.join(" ")
        );
        if (auto || (org && node.name)) {
          if (node.name && !name) {
            name = String(node.name).trim();
            sources.push("jsonld-name");
          }
          const addr = node.address;
          if (addr) {
            const list = Array.isArray(addr) ? addr : [addr];
            for (const a of list) {
              if (typeof a === "object" && a) {
                const c = (a.addressLocality || "").trim();
                const r = (a.addressRegion || "").trim();
                if (c && !city) city = c;
                if (r && !state) state = r.replace(/\s/g, "").slice(0, 2).toUpperCase();
              }
            }
            if (city || state) sources.push("jsonld-address");
          }
        }
      }
    }

    const ogSite =
      document.querySelector('meta[property="og:site_name"]') ||
      document.querySelector('meta[name="og:site_name"]');
    if (!name && ogSite && ogSite.getAttribute("content")) {
      name = ogSite.getAttribute("content").trim();
      sources.push("og:site_name");
    }

    const ogTitle = document.querySelector('meta[property="og:title"]');
    if (!name && ogTitle && ogTitle.getAttribute("content")) {
      const t = ogTitle.getAttribute("content").trim();
      if (t) {
        name = t.split(/[|\-–]/)[0].trim();
        sources.push("og:title");
      }
    }

    if (!name && document.title) {
      name = document.title.split(/[|\-–]/)[0].trim();
      sources.push("title");
    }

    const footer =
      document.querySelector("footer") ||
      document.querySelector("[role='contentinfo']") ||
      document.querySelector(".footer, #footer, .site-footer");
    const footText = footer ? (footer.innerText || "").slice(0, 6000) : "";
    const bodyTail = (document.body && document.body.innerText) ? document.body.innerText.slice(-8000) : "";
    const addrBlob = `${footText}\n${bodyTail}`;

    return { name, city, state, addrBlob, sources };
  });

  let name = normStr(extracted.name);
  let city = normStr(extracted.city);
  let state = normStr(extracted.state).toUpperCase().slice(0, 2);

  if ((!city || !state) && extracted.addrBlob) {
    const fromFoot = extractCityStateFromText(extracted.addrBlob);
    if (!city && fromFoot.city) city = fromFoot.city;
    if (!state && fromFoot.state) state = fromFoot.state;
    if (fromFoot.city || fromFoot.state) {
      emitDiscovery({ step: "footer", message: "Extracted location hints from footer / page text." });
    }
  }

  if (name) {
    emitDiscovery({ step: "found_name", message: `Found name: ${name}`, name });
  } else {
    emitDiscovery({ step: "found_name", message: "Could not resolve a business name from meta data; using site slug." });
  }
  if (city && state) {
    emitDiscovery({
      step: "found_location",
      message: `Extracting location: ${city}, ${state}`,
      city,
      state,
    });
  } else {
    emitDiscovery({
      step: "found_location",
      message: "Extracting location: still resolving city/state…",
      city,
      state,
    });
  }

  return { name, city, state, sources: extracted.sources || [] };
}

/** Keep in sync with Python ``backend.dev_dealers.slug_from_url`` (Flask smart-import writes dealers.json). */
function slugFromUrl(urlStr) {
  try {
    const u = new URL(urlStr);
    const s = u.hostname
      .replace(/^www\./i, "")
      .replace(/[^a-z0-9]+/gi, "-")
      .replace(/^-|-$/g, "")
      .toLowerCase();
    return s || "dealer";
  } catch {
    return "dealer";
  }
}

function parseCliArgs(argv) {
  let singleUrl = null;
  let singleName = "";
  let smartImport = false;
  let profile = "default";
  let headed = false;
  const args = argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--url" && args[i + 1]) {
      singleUrl = args[i + 1];
      i++;
    } else if (args[i] === "--name" && args[i + 1]) {
      singleName = args[i + 1];
      i++;
    } else if (args[i] === "--smart-import") {
      smartImport = true;
    } else if (args[i] === "--profile" && args[i + 1]) {
      profile = args[i + 1];
      i++;
    } else if (args[i] === "--headed") {
      headed = true;
    }
  }
  if (process.env.SCANNER_HEADED === "1" || process.env.SCANNER_HEADED === "true") {
    headed = true;
  }
  return { singleUrl, singleName, smartImport, profile, headed };
}

async function main() {
  const { singleUrl, singleName, smartImport, profile, headed } = parseCliArgs(process.argv);

  if (singleUrl) {
    let raw = String(singleUrl).trim();
    if (!/^https?:\/\//i.test(raw)) {
      raw = `https://${raw}`;
    }
    const url = raw.replace(/\/$/, "");
    const dealerId = slugFromUrl(raw);

    const launchOpts = getLaunchOptions(profile, headed);
    const browser = await puppeteer.launch(launchOpts);

    let name = (singleName && String(singleName).trim()) || "";
    let city = "";
    let state = "";
    let pendingExit = 0;

    try {
      if (smartImport) {
        emitDiscovery({ step: "start", message: "Starting smart import: discovering dealership name and location…" });
        const page = await browser.newPage();
        let meta = await discoverDealerMetadata(page, url, profile);
        await page.close().catch(() => {});

        meta = enrichMetadataWithCrawl4ai(url, meta);

        if (meta.retryable && meta.error === "navigation") {
          console.log(
            `SMART_IMPORT_ERROR:${JSON.stringify({
              reason: "navigation",
              detail: meta.detail || "",
              retryable: true,
            })}`
          );
          pendingExit = 3;
        }

        if (!pendingExit) {
          if (!name) name = normStr(meta.name) || dealerId;
          city = normStr(meta.city);
          state = normStr(meta.state).toUpperCase().slice(0, 2);

          emitDiscovery({
            step: "inventory",
            message: "Starting inventory scan (Dealer.com JSON intercept)…",
          });
        }
      } else if (!name) {
        name = dealerId;
      }

      if (!pendingExit) {
        const dealer = {
          name: name || dealerId,
          url,
          provider: "dealer_dot_com",
          dealer_id: dealerId,
        };
        console.info(`[dev] Single-URL mode (${profile}): ${dealer.url} (${dealer.dealer_id})`);

        const dcVehicles = await runDealer(browser, dealer, { profile, scrollLazyLoad: true });

        // ── Crawl4AI inventory sniffer fallback ────────────────────────────
        // When the Dealer.com JSON interceptor finds nothing (non-Dealer.com site
        // or heavily obfuscated DMS), try the Python sniffer that detects
        // Algolia, VinSolutions, CDK, and generic inline-JSON inventory formats.
        let vehicles = dcVehicles;
        if (smartImport && !dcVehicles.length) {
          emitDiscovery({
            step: "crawl4ai_inventory",
            message: "Dealer.com intercept found 0 vehicles; trying Crawl4AI inventory sniffer (Algolia / VinSolutions / CDK / inline-JSON)…",
          });
          const sniffer = runCrawl4aiInventorySubprocess(url);
          if (sniffer && sniffer.ok && Array.isArray(sniffer.vehicles) && sniffer.vehicles.length) {
            emitDiscovery({
              step: "crawl4ai_inventory",
              message: `Crawl4AI found ${sniffer.vehicles.length} vehicle(s) via strategy: ${sniffer.strategy}.`,
              count: sniffer.vehicles.length,
              strategy: sniffer.strategy,
            });
            vehicles = sniffer.vehicles.map((v) => mapCrawl4aiVehicle(v, dealer, dealerId));
          } else {
            const detail = (sniffer && sniffer.error) ? ` (${sniffer.error})` : "";
            emitDiscovery({
              step: "crawl4ai_inventory",
              message: `Crawl4AI inventory sniffer found no vehicles${detail}.`,
            });
          }
        }
        // ──────────────────────────────────────────────────────────────────

        console.log(`SCAN_VEHICLE_COUNT:${vehicles.length}`);
        if (!vehicles.length) {
          await fs.mkdir(DEBUG_DIR, { recursive: true });
          console.warn("No vehicles scraped (single-URL mode).");
        }
        const db = openDb();
        await ensureSchema(db);
        const n = await upsertAll(db, vehicles);
        db.close();
        console.info(`Upserted ${n} unique vehicles (parsed rows: ${vehicles.length})`);

        if (smartImport) {
          const out = {
            name: dealer.name,
            website_url: url,
            city: city || "",
            state: state || "",
          };
          if (!out.city || out.state.length !== 2) {
            console.log(
              `SMART_IMPORT_ERROR:${JSON.stringify({
                reason: "incomplete_location",
                detail: "Could not resolve a US city and 2-letter state for the registry.",
                partial: out,
                retryable: false,
              })}`
            );
            pendingExit = 2;
          } else {
            console.log(`SMART_IMPORT_RESULT:${JSON.stringify(out)}`);
          }
        }
      }
    } finally {
      await browser.close();
    }
    console.info("Scanner finished.");
    if (pendingExit) process.exit(pendingExit);
    return;
  }

  const manifest = await fs.readJson(MANIFEST_PATH);
  const dealers = manifest.filter((d) => (d.provider || "dealer_dot_com") === "dealer_dot_com" && d.url && d.dealer_id);

  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  try {
    const results = await Promise.all(
      dealers.map((d) => runDealer(browser, d, { profile: "default", scrollLazyLoad: true }))
    );
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
