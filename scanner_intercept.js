"use strict";

/**
 * Shared inventory JSON URL gate with Python (backend/scrapers/scanner_intercept_filter.py).
 * Defaults merge config/scanner_intercept_policy.json with the same deny/allow env vars.
 */

const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const REPO_ROOT = path.resolve(__dirname);
const POLICY_PATH = path.join(REPO_ROOT, "config", "scanner_intercept_policy.json");

const FALLBACK_ALLOW = [
  "getinventory",
  "getinventoryandfacets",
  "ws-inv-data",
  "/inventory",
  "algolia",
  "algolianet",
  "dealer.com",
  "dealerinspire",
  "cdk",
];

let _cachedFileAllows = null;

function _loadFileAllows() {
  if (_cachedFileAllows) return _cachedFileAllows;
  const merged = new Set(FALLBACK_ALLOW);
  try {
    const raw = fs.readFileSync(POLICY_PATH, "utf8");
    const data = JSON.parse(raw);
    const arr = data.default_allow_url_substrings || [];
    if (Array.isArray(arr)) {
      for (const x of arr) {
        const s = String(x || "")
          .trim()
          .toLowerCase();
        if (s) merged.add(s);
      }
    }
  } catch {
    /* keep fallback */
  }
  _cachedFileAllows = merged;
  return _cachedFileAllows;
}

function resetInterceptPolicyCacheForTests() {
  _cachedFileAllows = null;
}

function hostKey(urlStr) {
  try {
    let h = new URL(urlStr).hostname || "";
    h = String(h).trim().toLowerCase();
    if (h.startsWith("www.")) h = h.slice(4);
    return h;
  } catch {
    return "";
  }
}

function sameDealerSite(responseUrl, dealerBaseUrl) {
  const rh = hostKey(responseUrl);
  const dh = hostKey(dealerBaseUrl);
  if (!rh || !dh) return false;
  if (rh === dh) return true;
  if (rh.endsWith("." + dh)) return true;
  if (dh.endsWith("." + rh)) return true;
  return false;
}

function envCsvSubstrings(name) {
  const raw = String(process.env[name] || "")
    .trim()
    .toLowerCase();
  if (!raw) return [];
  return raw
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function buildAllowSet() {
  const s = new Set(_loadFileAllows());
  for (const x of envCsvSubstrings("SCANNER_INTERCEPT_URL_ALLOW")) s.add(x);
  return s;
}

function interceptUrlAllowed(responseUrl, dealerBaseUrl) {
  if (!responseUrl || !String(responseUrl).trim().toLowerCase().startsWith("http")) return false;
  const low = String(responseUrl).toLowerCase();
  for (const sub of envCsvSubstrings("SCANNER_INTERCEPT_URL_DENY")) {
    if (sub && low.includes(sub)) return false;
  }
  if (sameDealerSite(responseUrl, dealerBaseUrl)) return true;
  for (const sub of buildAllowSet()) {
    if (sub && low.includes(sub)) return true;
  }
  return false;
}

/** True when response is JSON and URL passes the same gate as scanner.py. */
function responseLooksLikeInventoryJsonIntercept(response, dealerBaseUrl) {
  const ct = (response.headers()["content-type"] || "").toLowerCase();
  if (!ct.includes("application/json")) return false;
  return interceptUrlAllowed(response.url(), dealerBaseUrl);
}

module.exports = {
  interceptUrlAllowed,
  responseLooksLikeInventoryJsonIntercept,
  resetInterceptPolicyCacheForTests,
};
