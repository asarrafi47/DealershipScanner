/**
 * Automated VDP (vehicle detail page) extraction for Puppeteer runs.
 * Layers: network JSON scoring, dataLayer / analytics ep.*, inline JSON + JSON-LD, DOM heuristics.
 * Merge is conservative; provenance and diagnostics are attached for observability.
 *
 * Gallery-first harvesting and merge policy for the Playwright scanner live in ``scanner_vdp.py``
 * (Python). This module is used by ``scanner.js``; keep gallery-related DOM/JSON logic aligned
 * when changing either path.
 *
 * Not DevTools — all logic runs inside the scanner process (page.evaluate + response listeners).
 */
/** Keys that indicate a JSON payload is vehicle-related (lowercased for matching). */
const VEHICLE_SIGNAL_KEYS = new Set([
  "vin",
  "vinnumber",
  "transmission",
  "transmissiontype",
  "drivetrain",
  "drive_train",
  "drivetype",
  "engine",
  "engine_description",
  "interior_color",
  "exterior_color",
  "fuel_type",
  "fueltype",
  "mpg",
  "mpgcity",
  "mpghighway",
  "city_fuel_economy",
  "highway_fuel_economy",
  "options",
  "features",
  "vehicleid",
  "vehicle_id",
  "chromestyleid",
  "chrome_style_id",
  "stock_id",
  "stocknumber",
  "stock_number",
  "mf_year",
  "vehicle_make",
  "vehicle_model",
  "body_style",
  "inventory_type",
  "certified",
  "trim",
  "make",
  "model",
  "year",
]);

const BROAD_COLOR_WORDS = new Set([
  "gray",
  "grey",
  "silver",
  "black",
  "white",
  "red",
  "blue",
  "green",
  "brown",
  "beige",
  "tan",
  "gold",
  "orange",
  "yellow",
  "charcoal",
]);

const MAX_JSON_RESPONSE_BYTES = 2 * 1024 * 1024;
const MAX_NETWORK_CANDIDATES = 40;
const VDP_NAV_TIMEOUT_MS = 32000;
const VDP_SETTLE_MS = 2200;

/** Align with backend.utils.vdp_price_merge._SOURCE_PRIORITY */
const VDP_PRICE_SOURCE_PRIORITY = {
  json_ld_offer: 100,
  json_ld_product: 92,
  dataLayer: 88,
  dom_itemprop: 55,
  dom_meta_price: 52,
  dom_dealer: 45,
};

function clampVehiclePrice(n) {
  if (typeof n !== "number" || !Number.isFinite(n)) return null;
  if (n < 500 || n > 2_500_000) return null;
  return n;
}

/**
 * Push a numeric hint (browser-side helpers stringify raw).
 * @param {unknown} raw
 * @param {string} source
 * @param {Array<{ value: number, raw: string, source: string }>} hints
 */
function pushPriceHintNode(raw, source, hints) {
  if (raw === null || raw === undefined) return;
  let num = null;
  if (typeof raw === "number" && Number.isFinite(raw)) {
    num = clampVehiclePrice(raw);
  } else if (typeof raw === "string") {
    const t = raw.replace(/[$,]/g, "").trim();
    if (!t || /call|contact|request|quote|inquire/i.test(t)) return;
    const m = t.match(/(\d{3,7})(?:\.\d{2})?/);
    if (m) num = clampVehiclePrice(parseFloat(m[1]));
  }
  if (num === null) return;
  hints.push({ value: num, raw: String(raw).slice(0, 60), source: String(source || "?") });
}

function listingPriceIsEmpty(vehicle) {
  const p = vehicle && vehicle.price;
  if (p == null) return true;
  const s = normStr(p);
  if (!s) return true;
  const n = Number(s);
  if (Number.isFinite(n)) return n <= 0;
  return true;
}

function priorityForSource(src) {
  const s = String(src || "");
  for (const [k, v] of Object.entries(VDP_PRICE_SOURCE_PRIORITY)) {
    if (s.startsWith(k)) return v;
  }
  if (s.startsWith("dataLayer:")) return VDP_PRICE_SOURCE_PRIORITY.dataLayer;
  if (s.startsWith("network_json:")) return 35;
  if (s.startsWith("network:")) return 30;
  if (s.startsWith("dom_dealer:")) return VDP_PRICE_SOURCE_PRIORITY.dom_dealer;
  if (s.startsWith("dom_meta_price")) return VDP_PRICE_SOURCE_PRIORITY.dom_meta_price;
  return 20;
}

/**
 * @param {Array<{ value: number, raw?: string, source?: string }>|null|undefined} hints
 * @returns {[number|null, Record<string, unknown>|null]}
 */
function pickBestVdpPrice(hints) {
  if (!hints || !hints.length) return [null, null];
  let best = null;
  let bestPri = -1;
  /** @type {{ value: number, raw?: string, source?: string }|null} */
  let bestHint = null;
  for (const h of hints) {
    if (!h || typeof h !== "object") continue;
    const vf = clampVehiclePrice(Number(h.value));
    if (vf == null) continue;
    const pri = priorityForSource(h.source);
    if (pri > bestPri || (pri === bestPri && best != null && vf > best)) {
      best = vf;
      bestPri = pri;
      bestHint = h;
    }
  }
  if (best == null || !bestHint) return [null, null];
  return [
    best,
    {
      source: String(bestHint.source || "?"),
      raw: String(bestHint.raw || "").slice(0, 80),
      candidates: hints.filter((x) => x && x.value != null).length,
    },
  ];
}

/** Keys commonly used across Dealer.com / CDK / Tekion / vAuto-style payloads */
const NETWORK_PRICE_KEYS = new Set([
  "internetPrice",
  "InternetPrice",
  "internet_price",
  "salePrice",
  "SalePrice",
  "sale_price",
  "sellingPrice",
  "SellingPrice",
  "selling_price",
  "price",
  "Price",
  "vehiclePrice",
  "VehiclePrice",
  "vehicle_price",
  "askingPrice",
  "AskingPrice",
  "asking_price",
  "listPrice",
  "ListPrice",
  "list_price",
  "retailPrice",
  "RetailPrice",
  "retail_price",
  "msrp",
  "MSRP",
  "Msrp",
  "finalPrice",
  "FinalPrice",
  "final_price",
  "cashPrice",
  "CashPrice",
  "cash_price",
  "drivePrice",
  "DrivePrice",
  "promotionalPrice",
  "PromotionalPrice",
  "specialPrice",
  "SpecialPrice",
  "dealerPrice",
  "DealerPrice",
  "marketPrice",
  "MarketPrice",
  "bestPrice",
  "BestPrice",
  "primaryPrice",
  "PrimaryPrice",
  "displayPrice",
  "DisplayPrice",
  "priceDisplay",
  "PriceDisplay",
  "paymentPrice",
  "advertisedPrice",
  "AdvertisedPrice",
]);

function appendPriceHintsFromNetworkJson(parsed, hints, depth = 0) {
  if (depth > 14 || parsed == null || typeof parsed !== "object") return;
  if (Array.isArray(parsed)) {
    for (const x of parsed) appendPriceHintsFromNetworkJson(x, hints, depth + 1);
    return;
  }
  for (const k of Object.keys(parsed)) {
    if (NETWORK_PRICE_KEYS.has(k)) {
      pushPriceHintNode(parsed[k], `network_json:${k}`, hints);
    }
  }
  for (const v of Object.values(parsed)) {
    if (v && typeof v === "object") appendPriceHintsFromNetworkJson(v, hints, depth + 1);
  }
}

function normStr(v) {
  if (v == null) return "";
  return String(v).trim();
}

function looksLikeVin17(v) {
  const s = normStr(v).toUpperCase();
  return /^[A-HJ-NPR-Z0-9]{17}$/.test(s);
}

function isPlaceholderStr(s) {
  if (!normStr(s)) return true;
  return /^(n\/?a|na|null|none|unknown|undefined|tbd)$/i.test(normStr(s));
}

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

/**
 * Score JSON subtree for vehicle relevance; return { score, keyHits, epObjects }.
 */
function analyzeJsonForVehicleSignals(obj, depth = 0) {
  let score = 0;
  const keyHits = [];
  const epObjects = [];
  if (obj == null || depth > 18) return { score, keyHits, epObjects };

  if (Array.isArray(obj)) {
    for (const x of obj) {
      const sub = analyzeJsonForVehicleSignals(x, depth + 1);
      score += sub.score;
      keyHits.push(...sub.keyHits);
      epObjects.push(...sub.epObjects);
    }
    return { score, keyHits, epObjects };
  }

  if (typeof obj === "object") {
    if (obj.ep && typeof obj.ep === "object" && !Array.isArray(obj.ep)) {
      epObjects.push(obj.ep);
      score += 25;
    }
    for (const [k, val] of Object.entries(obj)) {
      const lk = k.replace(/\s+/g, "_").toLowerCase();
      if (VEHICLE_SIGNAL_KEYS.has(lk)) {
        if (val != null && val !== "" && !(typeof val === "object" && Object.keys(val).length === 0)) {
          score += 8;
          keyHits.push(k);
        }
      }
      if (typeof val === "object" && val != null) {
        const sub = analyzeJsonForVehicleSignals(val, depth + 1);
        score += sub.score * 0.35;
        keyHits.push(...sub.keyHits);
        epObjects.push(...sub.epObjects);
      }
    }
  }
  return { score: Math.round(score), keyHits, epObjects };
}

function isBroadOnlyColor(phrase) {
  const parts = normStr(phrase)
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length !== 1) return false;
  return BROAD_COLOR_WORDS.has(parts[0]);
}

function shouldSkipExteriorOverwrite(existing, proposed) {
  if (!normStr(proposed)) return true;
  const ex = normStr(existing);
  if (!ex) return false;
  if (isBroadOnlyColor(proposed) && !isBroadOnlyColor(ex) && ex.split(/\s+/).length > 1) return true;
  return false;
}

function fieldQuality(str) {
  const s = normStr(str);
  if (!s) return 0;
  if (isPlaceholderStr(s)) return 0;
  let q = Math.min(40, s.length);
  if (s.split(/\s+/).length > 1) q += 15;
  if (/metallic|pearl|tri-?coat/i.test(s)) q += 20;
  return q;
}

/**
 * Flatten ep-like and common alias keys into a single canonical-ish map for downstream Python merge.
 */
function pickVehicleLikeObject(root, depth = 0) {
  if (root == null || depth > 14) return null;
  if (Array.isArray(root)) {
    for (const x of root) {
      const p = pickVehicleLikeObject(x, depth + 1);
      if (p) return p;
    }
    return null;
  }
  if (typeof root !== "object") return null;
  const v = root.vin || root.VIN;
  if (looksLikeVin17(v)) return root;
  if (root.vehicle) return pickVehicleLikeObject(root.vehicle, depth + 1);
  if (Array.isArray(root.vehicles) && root.vehicles[0]) {
    return pickVehicleLikeObject(root.vehicles[0], depth + 1);
  }
  if (Array.isArray(root.inventory) && root.inventory[0]) {
    return pickVehicleLikeObject(root.inventory[0], depth + 1);
  }
  const keys = Object.keys(root);
  const lowered = new Set(keys.map((k) => k.replace(/\s+/g, "_").toLowerCase()));
  let hits = 0;
  for (const k of VEHICLE_SIGNAL_KEYS) {
    if (lowered.has(k)) hits++;
  }
  if (hits >= 3 && keys.length < 120) return root;
  for (const val of Object.values(root)) {
    if (val && typeof val === "object") {
      const p = pickVehicleLikeObject(val, depth + 1);
      if (p) return p;
    }
  }
  return null;
}

function epLikeFromObject(src, out = {}) {
  if (!src || typeof src !== "object") return out;
  const o = src;
  const pick = (a, b) => {
    if (b != null && !isPlaceholderStr(b)) out[a] = b;
  };
  pick("vin", o.vin || o.VIN);
  pick("stock_id", o.stock_id || o.stockNumber || o.stock_number);
  pick("mf_year", o.mf_year ?? o.year ?? o.vehicle_year);
  pick("vehicle_make", o.vehicle_make || o.make);
  pick("vehicle_model", o.vehicle_model || o.model);
  pick("transmission", o.transmission || o.transmissionType);
  pick("drive_train", o.drive_train || o.drivetrain || o.driveType);
  pick("interior_color", o.interior_color || o.interiorColor);
  pick("exterior_color", o.exterior_color || o.exteriorColor);
  pick("fuel_type", o.fuel_type || o.fuelType);
  pick("city_fuel_economy", o.city_fuel_economy ?? o.mpgCity);
  pick("highway_fuel_economy", o.highway_fuel_economy ?? o.mpgHighway);
  pick("body_style", o.body_style || o.bodyStyle);
  pick("inventory_type", o.inventory_type);
  pick("certified", typeof o.certified === "boolean" ? o.certified : undefined);
  pick("engine", o.engine || o.engine_description);
  pick("trim", o.trim || o.trimName);
  if (o.options != null) out._options_hint = Array.isArray(o.options) ? o.options.slice(0, 30) : o.options;
  if (o.features != null) out._features_hint = Array.isArray(o.features) ? o.features.slice(0, 40) : o.features;
  // Dealer description / seller notes (used for package extraction)
  const descVal = o.description || o.dealer_description || o.sellerNotes || o.seller_notes
    || o.remarks || o.comments || o.dealerNotes || o.dealer_notes || o.sellerComments || o.overview;
  if (descVal && typeof descVal === "string" && descVal.trim().length > 40)
    pick("description", descVal.trim().slice(0, 6000));
  if (o.vehicleId != null || o.vehicle_id != null) out._vehicle_id = o.vehicleId ?? o.vehicle_id;
  if (o.chromeStyleId != null || o.chrome_style_id != null) out._chrome_style_id = o.chromeStyleId ?? o.chrome_style_id;
  return out;
}

/**
 * Merge candidate objects by score; later higher-quality fields can override if we pass ordered list.
 */
function mergeEpCandidates(candidates, preferredVin) {
  /** @type {Record<string, any>} */
  const merged = {};
  const provenance = {};
  const pv = normStr(preferredVin).toUpperCase();

  for (const { source, ep, score } of candidates) {
    if (!ep || typeof ep !== "object") continue;
    const epVin = normStr(ep.vin || ep.VIN).toUpperCase();
    if (pv && epVin && epVin !== pv) continue;
    for (const [k, val] of Object.entries(ep)) {
      if (val == null || val === "") continue;
      const prev = merged[k];
      const prevQ = typeof prev === "string" ? fieldQuality(prev) : prev != null ? 10 : 0;
      const nextQ = typeof val === "string" ? fieldQuality(val) : 20;
      if (k === "exterior_color" && shouldSkipExteriorOverwrite(prev, String(val))) continue;
      if (prev == null || nextQ > prevQ) {
        merged[k] = val;
        provenance[k] = source + (score != null ? `(${score})` : "");
      }
    }
  }
  return { merged, provenance };
}

function fingerprintFromSignals(pageUrl, scriptSrcSamples, metaHints) {
  const hints = [];
  const u = (pageUrl || "").toLowerCase();
  const blob = `${scriptSrcSamples.join(" ")} ${metaHints.join(" ")}`.toLowerCase();

  if (/dealer\.com|dealerinspire|cdn\.dealer\.com/.test(u + blob)) hints.push("dealer_com_cdn");
  if (/cdk|cobalt|coxauto/.test(blob)) hints.push("cdk_family");
  if (/vinsolutions|vauto|vinsolutions\.net/.test(blob)) hints.push("vinsolutions_family");
  if (/reynolds|reyrey/.test(blob)) hints.push("reynolds");
  if (/tekion|dealertrack|routeone/.test(blob)) hints.push("dms_generic");
  if (/googletagmanager|gtm\.js|tealium|adobedtm|launch|segment\.com/.test(blob)) hints.push("tag_manager_analytics");
  if (/sitemap|schema\.org|ld\+json/.test(blob)) hints.push("structured_data");

  return { likelyFamilies: [...new Set(hints)], pageHost: (() => {
    try {
      return new URL(pageUrl).hostname;
    } catch {
      return "";
    }
  })() };
}

/**
 * Browser-side extraction bundle (single evaluate). Keep self-contained — no imports.
 */
function buildPageEvaluateExtractor() {
  return function vdpPageExtract() {
    const result = {
      dataLayerEps: [],
      dataLayerRows: 0,
      globalsTried: [],
      ldJsonVehicle: [],
      inlineJsonHits: [],
      domSpecs: {},
      domFeatures: [],
      domBadges: [],
      dealerDescription: "",
      vdpPriceHints: [],
      scriptSrcSample: [],
      metaGenerator: "",
    };

    try {
      if (window.dataLayer && Array.isArray(window.dataLayer)) {
        result.dataLayerRows = window.dataLayer.length;
        for (const row of window.dataLayer) {
          if (row && typeof row === "object" && row.ep && typeof row.ep === "object") {
            result.dataLayerEps.push(row.ep);
          }
        }
      }
    } catch (e) {
      result.globalsTried.push("dataLayer:error");
    }

    for (const name of ["digitalData", "utag_data", "google_tag_manager", "s_objectID"]) {
      try {
        if (window[name] != null) result.globalsTried.push(name);
      } catch (e) {
        /* ignore */
      }
    }

    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
    for (const s of scripts) {
      try {
        const j = JSON.parse(s.textContent || "{}");
        const stack = Array.isArray(j) ? j : [j];
        for (const node of stack) {
          if (!node || typeof node !== "object") continue;
          const t = [].concat(node["@type"] || []);
          const ts = t.map((x) => String(x).toLowerCase());
          const hasVin = !!(node.vehicleIdentificationNumber || node.vin || node.VIN);
          if (
            ts.some((x) => /vehicle|car|automobile/.test(x)) ||
            (hasVin && ts.some((x) => x === "product"))
          ) {
            result.ldJsonVehicle.push(node);
          }
        }
      } catch (e) {
        /* ignore */
      }
    }

    const mg = document.querySelector('meta[name="generator"]');
    if (mg && mg.getAttribute("content")) result.metaGenerator = mg.getAttribute("content").slice(0, 200);

    const sscripts = document.querySelectorAll("script[src]");
    for (let i = 0; i < Math.min(sscripts.length, 35); i++) {
      const src = sscripts[i].getAttribute("src") || "";
      if (src) result.scriptSrcSample.push(src.slice(0, 220));
    }

    /** Heuristic: large inline scripts with vehicle-ish tokens */
    const inlineScripts = document.querySelectorAll("script:not([src])");
    for (const sc of inlineScripts) {
      const txt = (sc.textContent || "").slice(0, 120000);
      if (txt.length < 80) continue;
      if (!/vin|vehicle|inventory|drivetrain|transmission/i.test(txt)) continue;
      const slice = txt.slice(0, 8000);
      let parsed = null;
      try {
        const m = txt.match(/\{\s*"vin"\s*:\s*"[^"]+"/i);
        if (m && m.index != null) {
          let depth = 0;
          let start = -1;
          for (let i = m.index; i < Math.min(m.index + 25000, txt.length); i++) {
            const c = txt[i];
            if (c === "{") {
              if (depth === 0) start = i;
              depth++;
            } else if (c === "}") {
              depth--;
              if (depth === 0 && start >= 0) {
                const chunk = txt.slice(start, i + 1);
                try {
                  parsed = JSON.parse(chunk);
                  break;
                } catch (e) {
                  parsed = null;
                }
              }
            }
          }
        }
      } catch (e) {
        parsed = null;
      }
      if (parsed && typeof parsed === "object") {
        result.inlineJsonHits.push(parsed);
        if (result.inlineJsonHits.length >= 5) break;
      } else if (/vehicleId|chromeStyleId|inventory/i.test(slice)) {
        result.inlineJsonHits.push({ _rawSnippet: slice.slice(0, 1200) });
        if (result.inlineJsonHits.length >= 5) break;
      }
    }

    /** DOM: definition lists + tables */
    const specSelectors = [
      "dl",
      "dl.vehicle-specs",
      ".vehicle-specs",
      ".specifications",
      "[class*='spec'] table",
      "table.specs",
      ".vdp-specs",
    ];
    for (const sel of specSelectors) {
      try {
        const el = document.querySelector(sel);
        if (!el) continue;
        const rows = el.querySelectorAll("tr, dt");
        rows.forEach((row) => {
          const label =
            row.querySelector("th, dt, .label, .name")?.textContent?.trim() ||
            row.cells?.[0]?.textContent?.trim();
          const val =
            row.querySelector("td, dd, .value")?.textContent?.trim() ||
            row.cells?.[1]?.textContent?.trim();
          if (label && val && label.length < 80 && val.length < 400) {
            const lk = label.toLowerCase();
            if (/trans|drive|exterior|interior|engine|fuel|mpg|vin|stock|body/i.test(lk)) {
              result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
            }
          }
        });
      } catch (e) {
        /* ignore */
      }
    }

    document.querySelectorAll("[class*='feature'], [class*='equipment'], .features li, ul.features li").forEach((el, idx) => {
      if (idx > 60) return;
      const t = (el.textContent || "").trim();
      if (t && t.length < 200) result.domFeatures.push(t);
    });

    document.querySelectorAll(".badge, [class*='badge'], .label-pill, [data-badge]").forEach((el, idx) => {
      if (idx > 25) return;
      const t = (el.textContent || "").trim();
      if (t && t.length < 120) result.domBadges.push(t);
    });

    // Dealer description / seller notes — captured for package extraction in Python
    const DESCRIPTION_SELECTORS = [
      ".vehicle-description", ".vehicleDescription", ".vehicle_description",
      ".dealer-comments", ".dealerComments", ".dealer_comments",
      ".seller-notes", ".sellerNotes", ".seller_notes",
      ".vehicle-overview", ".vehicleOverview", ".vehicle_overview",
      ".about-vehicle", ".aboutVehicle",
      "[class*='description'][class*='vehicle']",
      "[class*='dealer'][class*='comment']",
      "[class*='seller'][class*='note']",
      "#vehicle-description", "#vehicleDescription",
      ".listing-description", ".listingDescription",
      ".vdp-description", ".vdpDescription",
      "[data-test='vehicle-description']",
      "[data-testid='description']",
    ];
    let dealerDesc = "";
    for (const sel of DESCRIPTION_SELECTORS) {
      try {
        const el = document.querySelector(sel);
        if (el) {
          const t = (el.innerText || el.textContent || "").trim();
          if (t.length > 40) { dealerDesc = t.slice(0, 6000); break; }
        }
      } catch (e) { /* ignore */ }
    }
    if (dealerDesc) result.dealerDescription = dealerDesc;

    function pushPriceHint(raw, source) {
      if (raw === null || raw === undefined) return;
      let num = null;
      if (typeof raw === "number" && isFinite(raw)) {
        num = raw;
      } else if (typeof raw === "string") {
        const t = raw.replace(/[$,]/g, "").trim();
        if (!t || /call|contact|request|quote|inquire/i.test(t)) return;
        const m = t.match(/(\d{3,7})(?:\.\d{2})?/);
        if (m) num = parseFloat(m[1]);
      }
      if (num === null || !isFinite(num) || num < 500 || num > 2500000) return;
      result.vdpPriceHints.push({ value: num, raw: String(raw).slice(0, 60), source: String(source || "?") });
    }
    function walkDataLayerPrice(obj, depth, seen) {
      if (depth > 14 || !obj || typeof obj !== "object" || seen.has(obj)) return;
      seen.add(obj);
      const keys = [
        "internetPrice",
        "InternetPrice",
        "salePrice",
        "SalePrice",
        "sellingPrice",
        "price",
        "Price",
        "vehiclePrice",
        "askingPrice",
        "listPrice",
        "retailPrice",
        "finalPrice",
        "cashPrice",
        "msrp",
        "MSRP",
        "primaryPrice",
        "advertisedPrice",
      ];
      for (const k of keys) {
        if (Object.prototype.hasOwnProperty.call(obj, k)) pushPriceHint(obj[k], "dataLayer:" + k);
      }
      for (const v of Object.values(obj)) {
        if (v && typeof v === "object") walkDataLayerPrice(v, depth + 1, seen);
      }
    }
    try {
      if (window.dataLayer && Array.isArray(window.dataLayer)) {
        const seen = new WeakSet();
        for (let i = 0; i < Math.min(40, window.dataLayer.length); i++) {
          walkDataLayerPrice(window.dataLayer[i], 0, seen);
        }
      }
    } catch (e3) {}
    function offersFromLd(node) {
      const out = [];
      if (!node || typeof node !== "object") return out;
      const o = node.offers || node.offer;
      if (!o) return out;
      return [].concat(o);
    }
    for (const node of result.ldJsonVehicle || []) {
      for (const off of offersFromLd(node)) {
        if (!off || typeof off !== "object") continue;
        const p = off.price || off.Price || (off.priceSpecification && off.priceSpecification.price);
        pushPriceHint(p, "json_ld_offer");
      }
      const p2 = node.price || node.Price;
      if (p2) pushPriceHint(p2, "json_ld_product");
    }
    try {
      document.querySelectorAll('[itemprop="price"],[itemprop=price]').forEach((el, idx) => {
        if (idx > 12) return;
        const c = el.getAttribute("content");
        if (c) pushPriceHint(c, "dom_itemprop");
        else pushPriceHint((el.textContent || "").trim(), "dom_itemprop");
      });
    } catch (e4) {}
    try {
      document.querySelectorAll('meta[property="price"],meta[itemprop="price"]').forEach((el, idx) => {
        if (idx > 10) return;
        const c = el.getAttribute("content");
        if (c) pushPriceHint(c, "dom_meta_price");
      });
    } catch (e4b) {}
    const priceSelectors = [
      ".vehicle-price",
      ".internetPrice",
      ".internet-price",
      ".sale-price",
      ".final-price",
      ".primary-price",
      ".price-value",
      ".pricing-price",
      ".price-block",
      ".highlight-price",
      ".srp-price",
      ".priceDisplay",
      "#vehicle-price",
      "#price",
      "[class*='vehicle-price']",
      "[class*='asking-price']",
      "[class*='list-price']",
      "[data-vehicle-price]",
      "[data-price]",
      "[data-selling-price]",
      "[data-internet-price]",
      "[data-msrp]",
      "[data-final-price]",
      "[data-testid*='price']",
      "[class*='PriceDisplay']",
      "[class*='vehiclePrice']",
    ];
    for (const sel of priceSelectors) {
      try {
        const el = document.querySelector(sel);
        if (!el) continue;
        const t = (el.textContent || "").trim();
        if (t && t.length < 80) pushPriceHint(t, "dom_dealer:" + sel.slice(0, 40));
      } catch (e5) {}
    }

    return result;
  };
}

/**
 * Run full VDP extraction for one URL. Attaches a temporary network listener around navigation.
 */
async function runVdpExtraction(page, vdpUrl, options = {}) {
  const expectedVin = normStr(options.expectedVin || "").toUpperCase();
  const t0 = Date.now();
  const networkCandidates = [];
  const responseHandler = async (response) => {
    try {
      if (response.status() !== 200) return;
      const ct = (response.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("json")) return;
      const url = response.url();
      const buf = await response.text();
      if (!buf || buf.length > MAX_JSON_RESPONSE_BYTES) return;
      let parsed;
      try {
        parsed = JSON.parse(buf);
      } catch {
        return;
      }
      const { score, keyHits, epObjects } = analyzeJsonForVehicleSignals(parsed);
      const hintProbe = [];
      appendPriceHintsFromNetworkJson(parsed, hintProbe);
      if (score < 6 && epObjects.length === 0 && hintProbe.length === 0) return;
      networkCandidates.push({
        url: url.slice(0, 500),
        score,
        keyHits: keyHits.slice(0, 25),
        epObjects,
        parsed,
        rawTopKeys:
          parsed && typeof parsed === "object" && !Array.isArray(parsed)
            ? Object.keys(parsed).slice(0, 20)
            : [],
      });
      if (networkCandidates.length > MAX_NETWORK_CANDIDATES) networkCandidates.shift();
    } catch {
      /* ignore */
    }
  };

  page.on("response", responseHandler);
  let navError = null;
  try {
    await page.goto(vdpUrl, { waitUntil: "domcontentloaded", timeout: VDP_NAV_TIMEOUT_MS });
  } catch (e) {
    navError = String(e.message || e);
  }
  await new Promise((r) => setTimeout(r, VDP_SETTLE_MS));
  page.off("response", responseHandler);

  let pageBundle;
  try {
    pageBundle = await page.evaluate(buildPageEvaluateExtractor());
  } catch (e) {
    pageBundle = { error: String(e.message || e) };
  }

  const epFromNetwork = [];
  for (const c of networkCandidates) {
    for (const ep of c.epObjects) {
      epFromNetwork.push({ source: "network:ep", ep, score: c.score });
    }
    if (c.parsed && c.score >= 20 && (!c.epObjects || c.epObjects.length === 0)) {
      const sub = pickVehicleLikeObject(c.parsed);
      if (sub) {
        const o = epLikeFromObject(sub, {});
        if (Object.keys(o).length >= 2) {
          epFromNetwork.push({ source: "network:vehicle_json", ep: o, score: c.score });
        }
      }
    }
  }

  const dataLayerEps = (pageBundle && pageBundle.dataLayerEps) || [];
  for (const ep of dataLayerEps) {
    epFromNetwork.push({ source: "dataLayer:ep", ep, score: 90 });
  }

  const ldVehicles = (pageBundle && pageBundle.ldJsonVehicle) || [];
  for (const node of ldVehicles) {
    const flat = {};
    const name = node.name || node.model || "";
    if (name) flat.vehicle_model = name;
    if (node.vehicleIdentificationNumber) flat.vin = node.vehicleIdentificationNumber;
    if (node.color || node.vehicleInteriorColor) flat.interior_color = node.vehicleInteriorColor || node.color;
    if (node.bodyType) flat.body_style = node.bodyType;
    epFromNetwork.push({ source: "ld+json", ep: flat, score: 35 });
  }

  for (const hit of (pageBundle && pageBundle.inlineJsonHits) || []) {
    if (hit && hit._rawSnippet) continue;
    if (hit && typeof hit === "object") {
      epFromNetwork.push({ source: "inline_json", ep: epLikeFromObject(hit), score: 22 });
    }
  }

  /** DOM specs → synthetic ep */
  const domFlat = {};
  const domSpecs = (pageBundle && pageBundle.domSpecs) || {};
  for (const [label, val] of Object.entries(domSpecs)) {
    const lk = label.toLowerCase();
    if (/vin/i.test(lk)) domFlat.vin = val;
    else if (/trans/i.test(lk)) domFlat.transmission = val;
    else if (/drive|drivetrain/i.test(lk)) domFlat.drive_train = val;
    else if (/exterior|ext\.?\s*color/i.test(lk)) domFlat.exterior_color = val;
    else if (/interior|int\.?\s*color/i.test(lk)) domFlat.interior_color = val;
    else if (/engine/i.test(lk)) domFlat.engine = val;
    else if (/fuel/i.test(lk)) domFlat.fuel_type = val;
    else if (/mpg|fuel economy/i.test(lk)) {
      const m = normStr(val).match(/(\d+)\s*[\/|]\s*(\d+)/);
      if (m) {
        domFlat.city_fuel_economy = m[1];
        domFlat.highway_fuel_economy = m[2];
      }
    }
  }
  if (Object.keys(domFlat).length) {
    epFromNetwork.push({ source: "dom:spec_table", ep: domFlat, score: 18 });
  }

  const { merged: mergedEp, provenance: epProvenance } = mergeEpCandidates(epFromNetwork, expectedVin);

  const allPriceHints = [...((pageBundle && pageBundle.vdpPriceHints) || [])];
  for (const c of networkCandidates) {
    if (c.parsed) appendPriceHintsFromNetworkJson(c.parsed, allPriceHints);
  }
  for (const hit of (pageBundle && pageBundle.inlineJsonHits) || []) {
    if (hit && !hit._rawSnippet && typeof hit === "object") appendPriceHintsFromNetworkJson(hit, allPriceHints);
  }
  const [vdpPrice, vdpPriceMeta] = pickBestVdpPrice(allPriceHints);

  const networkLayerSummary = networkCandidates.slice(-15).map((c) => ({
    url: c.url,
    score: c.score,
    keyHits: c.keyHits,
    epCount: c.epObjects.length,
    rawTopKeys: c.rawTopKeys,
  }));

  const fp = fingerprintFromSignals(
    vdpUrl,
    (pageBundle && pageBundle.scriptSrcSample) || [],
    [pageBundle && pageBundle.metaGenerator].filter(Boolean)
  );

  const diagnostics = {
    durationMs: Date.now() - t0,
    navError,
    networkCandidateCount: networkCandidates.length,
    bestNetworkScore: networkCandidates.reduce((a, c) => Math.max(a, c.score), 0),
    dataLayerRowCount: (pageBundle && pageBundle.dataLayerRows) || 0,
    epFragmentCount: epFromNetwork.length,
    ldJsonVehicleCount: ldVehicles.length,
    inlineJsonHitCount: ((pageBundle && pageBundle.inlineJsonHits) || []).length,
    domSpecKeys: Object.keys(domSpecs).length,
    domFeatureCount: ((pageBundle && pageBundle.domFeatures) || []).length,
    domBadgeCount: ((pageBundle && pageBundle.domBadges) || []).length,
    vdpPriceHintCount: allPriceHints.length,
  };

  if (process.env.SCANNER_VDP_DEBUG === "1" || process.env.SCANNER_VDP_DEBUG === "true") {
    const pstr = vdpPrice != null ? ` price=${Math.round(vdpPrice)}` : "";
    console.info(
      `[vdp] ${normStr(vdpUrl).slice(0, 90)} score=${diagnostics.bestNetworkScore} eps=${diagnostics.epFragmentCount}${pstr} ms=${diagnostics.durationMs}`
    );
  }

  return {
    url: vdpUrl,
    expectedVin: expectedVin || null,
    mergedEp,
    vdpPrice,
    vdpPriceMeta,
    epProvenance,
    platformHints: fp,
    layers: {
      network: networkLayerSummary,
      dataLayerEps,
      ldJson: ldVehicles.slice(0, 5),
      inlineJson: ((pageBundle && pageBundle.inlineJsonHits) || []).slice(0, 3),
      domSpecs,
      domFeatures: ((pageBundle && pageBundle.domFeatures) || []).slice(0, 40),
      domBadges: ((pageBundle && pageBundle.domBadges) || []).slice(0, 20),
      dealerDescription: (pageBundle && pageBundle.dealerDescription) || "",
      pageGlobals: (pageBundle && pageBundle.globalsTried) || [],
    },
    diagnostics,
    pageBundleError: pageBundle && pageBundle.error,
  };
}

/**
 * Attach extraction summary + merged ep onto vehicle; merge into global vin map for batch EP.
 */
function applyExtractionToVehicle(vehicle, extraction, vinToEp) {
  const vin = normStr(vehicle.vin).toUpperCase();
  if (!looksLikeVin17(vin)) return;

  vehicle._vdp_extraction = {
    url: extraction.url,
    diagnostics: extraction.diagnostics,
    platformHints: extraction.platformHints,
    epProvenance: extraction.epProvenance,
    layersSummary: {
      networkCandidates: extraction.layers.network.length,
      dataLayerEps: extraction.layers.dataLayerEps.length,
      ldJson: extraction.layers.ldJson.length,
    },
  };

  if (!vehicle._field_provenance) vehicle._field_provenance = {};
  for (const [k, src] of Object.entries(extraction.epProvenance || {})) {
    if (!vehicle._field_provenance[k]) vehicle._field_provenance[k] = `vdp:${src}`;
  }

  const merged = extraction.mergedEp || {};
  if (Object.keys(merged).length) {
    const prior = vinToEp.get(vin) || {};
    vinToEp.set(vin, { ...prior, ...merged });
    vehicle._ep_analytics = { ...(vehicle._ep_analytics || {}), ...merged };
  }

  // Capture dealer description for package extraction in Python post-pipeline
  const dealerDesc = (extraction.layers && extraction.layers.dealerDescription) || merged.description || "";
  if (dealerDesc && dealerDesc.length > 40 && !vehicle.description) {
    vehicle.description = dealerDesc.slice(0, 6000);
  }

  if (extraction.vdpPrice != null && listingPriceIsEmpty(vehicle)) {
    vehicle.price = Math.round(Number(extraction.vdpPrice));
    vehicle._vdp_price_meta = extraction.vdpPriceMeta || { source: "vdp_listing" };
  }
}

/**
 * Parallel listing fetches using independent browser tabs (Puppeteer ``Browser``).
 * @param {import("puppeteer").Browser} browser
 * @param {Array<{ url: string, sample: Record<string, unknown> }>} entries
 * @param {number} concurrency
 * @param {{ userAgent?: string, viewportWidth?: number, viewportHeight?: number }} [options]
 */
async function fetchVdpExtractionsWithPool(browser, entries, concurrency, options = {}) {
  /** @type {Map<string, Record<string, unknown>>} */
  const results = new Map();
  if (!entries.length || !browser || typeof browser.newPage !== "function") return results;

  const ua = options.userAgent || "";
  const vw = options.viewportWidth || 1920;
  const vh = options.viewportHeight || 1080;
  const nWorkers = Math.max(1, Math.min(Math.max(1, concurrency || 12), 64, entries.length));

  let next = 0;
  async function worker() {
    const page = await browser.newPage();
    try {
      if (ua) await page.setUserAgent(ua);
      await page.setViewport({ width: vw, height: vh });
      while (true) {
        const i = next++;
        if (i >= entries.length) break;
        const { url, sample } = entries[i];
        try {
          const extraction = await runVdpExtraction(page, url, { expectedVin: normStr(sample.vin) });
          results.set(url, extraction);
        } catch (e) {
          console.warn(`[vdp] extraction failed: ${String(e.message || e).slice(0, 140)}`);
        }
      }
    } finally {
      await page.close().catch(() => {});
    }
  }

  await Promise.all(Array.from({ length: nWorkers }, () => worker()));
  return results;
}

/**
 * Batch VDP visits: (1) up to ``maxEpVisits`` unique listing URLs for analytics / ep merge;
 * (2) optional extra visits for vehicles still missing price (``priceFillMax`` unique URLs).
 * Cached by URL so every vehicle with the same ``_detail_url`` gets the same extraction without re-navigation.
 *
 * @param {import("puppeteer").Browser} browser — Puppeteer browser (not a single Page).
 * @param options.parallel — concurrent tabs (default 12).
 */
async function runBatchVdpExtraction(browser, vehicles, vinToEp, _baseUrl, maxEpVisits, priceFillMax, options = {}) {
  const epMax = maxEpVisits != null ? Math.max(0, Number(maxEpVisits)) : 0;
  const pMax = priceFillMax != null ? Math.max(0, Number(priceFillMax)) : 0;
  const parallel = Math.max(1, Math.min(64, Number(options.parallel) || 12));
  if ((!epMax && !pMax) || !vehicles || !vehicles.length) return;
  if (!browser || typeof browser.newPage !== "function") {
    console.warn("[vdp] runBatchVdpExtraction: expected Puppeteer Browser with newPage(); skipping VDP batch");
    return;
  }

  /** @type {Map<string, Record<string, unknown>>} */
  const extractionByUrl = new Map();

  /**
   * @param {(v: Record<string, unknown>) => boolean} predicate
   */
  function orderedUniqueUrls(predicate) {
    const ordered = [];
    const seen = new Set();
    for (const v of vehicles) {
      const u = normStr(v._detail_url);
      if (!u || !u.startsWith("http")) continue;
      if (!predicate(v)) continue;
      if (seen.has(u)) continue;
      seen.add(u);
      ordered.push({ url: u, sample: v });
    }
    return ordered;
  }

  const phase1 = orderedUniqueUrls(() => true).slice(0, epMax);
  const batch1 = await fetchVdpExtractionsWithPool(browser, phase1, parallel, options);
  for (const [k, val] of batch1) extractionByUrl.set(k, val);

  if (pMax > 0) {
    const pending = orderedUniqueUrls((v) => listingPriceIsEmpty(v))
      .filter(({ url }) => !extractionByUrl.has(url))
      .slice(0, pMax);
    const batch2 = await fetchVdpExtractionsWithPool(browser, pending, parallel, options);
    for (const [k, val] of batch2) extractionByUrl.set(k, val);
  }

  for (const v of vehicles) {
    const u = normStr(v._detail_url);
    if (!u || !extractionByUrl.has(u)) continue;
    applyExtractionToVehicle(v, extractionByUrl.get(u), vinToEp);
  }
}

module.exports = {
  VEHICLE_SIGNAL_KEYS,
  runVdpExtraction,
  runBatchVdpExtraction,
  fetchVdpExtractionsWithPool,
  applyExtractionToVehicle,
  analyzeJsonForVehicleSignals,
  mergeEpCandidates,
  pickVehicleLikeObject,
  epLikeFromObject,
  fingerprintFromSignals,
  shouldSkipExteriorOverwrite,
  collectEpPayloadsFromJson,
  listingPriceIsEmpty,
  pickBestVdpPrice,
};
