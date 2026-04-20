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
          if (ts.some((x) => /vehicle|car|product|automobile/.test(x))) {
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
      if (score < 6 && epObjects.length === 0) return;
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
  };

  if (process.env.SCANNER_VDP_DEBUG === "1" || process.env.SCANNER_VDP_DEBUG === "true") {
    console.info(
      `[vdp] ${normStr(vdpUrl).slice(0, 90)} score=${diagnostics.bestNetworkScore} eps=${diagnostics.epFragmentCount} ms=${diagnostics.durationMs}`
    );
  }

  return {
    url: vdpUrl,
    expectedVin: expectedVin || null,
    mergedEp,
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
}

/**
 * Batch: visit up to maxVisits unique VDP URLs and merge into vinToEp + vehicles.
 */
async function runBatchVdpExtraction(page, vehicles, vinToEp, _baseUrl, maxVisits) {
  if (!maxVisits || maxVisits <= 0 || !vehicles || !vehicles.length) return;
  let n = 0;
  const seenUrl = new Set();
  for (const v of vehicles) {
    const u = normStr(v._detail_url);
    if (!u || !u.startsWith("http")) continue;
    if (seenUrl.has(u)) continue;
    seenUrl.add(u);
    const expectedVin = normStr(v.vin);
    try {
      const extraction = await runVdpExtraction(page, u, { expectedVin });
      applyExtractionToVehicle(v, extraction, vinToEp);
    } catch (e) {
      console.warn(`[vdp] extraction failed: ${String(e.message || e).slice(0, 140)}`);
    }
    n++;
    if (n >= maxVisits) break;
  }
}

module.exports = {
  VEHICLE_SIGNAL_KEYS,
  runVdpExtraction,
  runBatchVdpExtraction,
  applyExtractionToVehicle,
  analyzeJsonForVehicleSignals,
  mergeEpCandidates,
  pickVehicleLikeObject,
  epLikeFromObject,
  fingerprintFromSignals,
  shouldSkipExteriorOverwrite,
  collectEpPayloadsFromJson,
};
