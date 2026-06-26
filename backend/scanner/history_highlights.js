/**
 * Dealer-published history badge phrases (description / VDP badges). No Carfax report scraping.
 */

const { nullableJsonArrayText } = require("./json_column_storage");

const CANONICAL_RULES = [
  [/carfax\s*one[\s-]*owner/i, "CARFAX One-Owner"],
  [/clean\s+carfax|carfax\s+clean/i, "Clean CARFAX"],
  [/no\s+accidents?\s+(?:or\s+damage\s+)?reported/i, "No Accidents Reported"],
  [/accident[\s-]*free/i, "Accident Free"],
  [/personal\s+use\s+only/i, "Personal Use Only"],
  [/clean\s+title/i, "Clean Title"],
  [/^\s*1[\s-]*owner\s*$/i, "1 Owner"],
  [/\bone[\s-]*owner\b/i, "One Owner"],
  [/\bautocheck\b/i, "AutoCheck"],
];

function pruneRedundantHighlights(labels) {
  const lower = new Set(labels.map((x) => x.toLowerCase()));
  const drop = new Set();
  if (lower.has("carfax one-owner")) {
    drop.add("one owner");
    drop.add("1 owner");
    drop.add("carfax");
  }
  if (lower.has("clean carfax")) drop.add("carfax");
  const out = [];
  const seen = new Set();
  for (const label of labels) {
    const key = label.toLowerCase();
    if (drop.has(key) || seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return out;
}

const HISTORY_BADGE_HINT = /carfax|autocheck|owner|accident|title|salvage|lemon|fleet|personal\s+use|damage\s+reported/i;
const BADGE_NOISE = /certified|pre[\s-]*owned|used|new|sale|price|\$|awd|fwd|rwd|4wd|financ|warranty|contact|call\s+us|schedule|test\s+drive|kbb|jd\s+power/i;

function normSpace(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function canonicalFromText(text) {
  const t = normSpace(text);
  if (!t || t.length > 160) return null;
  for (const [rx, label] of CANONICAL_RULES) {
    if (rx.test(t)) return label;
  }
  if (HISTORY_BADGE_HINT.test(t) && !BADGE_NOISE.test(t) && t.length <= 80) return t;
  return null;
}

function textSegments(text) {
  const t = normSpace(text);
  if (!t) return [];
  const out = [t];
  for (const p of t.split(/[.\n|;]+/)) {
    const s = normSpace(p);
    if (s && s.length >= 4) out.push(s);
  }
  return out;
}

function asHighlightList(value) {
  if (value == null || value === "" || value === "[]") return [];
  if (Array.isArray(value)) return value.map((x) => normSpace(x)).filter(Boolean);
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.map((x) => normSpace(x)).filter(Boolean);
    } catch (_e) {
      const one = canonicalFromText(value);
      return one ? [one] : [];
    }
  }
  return [];
}

function extractHistoryHighlightsFromDealerText(...texts) {
  const seen = new Set();
  const out = [];
  function add(label) {
    if (!label) return;
    const key = label.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(label);
  }
  for (const raw of texts) {
    if (!raw || !String(raw).trim()) continue;
    const body = String(raw);
    for (const segment of textSegments(body)) add(canonicalFromText(segment));
    for (const [rx, label] of CANONICAL_RULES) {
      if (rx.test(body)) add(label);
    }
  }
  return pruneRedundantHighlights(out);
}

function extractHistoryHighlightsFromBadges(badges) {
  if (!Array.isArray(badges) || !badges.length) return [];
  const seen = new Set();
  const out = [];
  for (const item of badges) {
    if (typeof item !== "string") continue;
    const label = canonicalFromText(item);
    if (!label) continue;
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(label);
  }
  return pruneRedundantHighlights(out);
}

function mergeHistoryHighlights(...sources) {
  const seen = new Set();
  const out = [];
  function absorb(value) {
    if (value == null) return;
    let items = [];
    if (typeof value === "string") {
      if (!value.trim() || value.trim() === "[]" || value.trim() === "null") return;
      items = extractHistoryHighlightsFromDealerText(value);
    } else if (Array.isArray(value)) {
      for (const entry of value) {
        if (typeof entry === "string") {
          items.push(...extractHistoryHighlightsFromDealerText(entry));
          const canon = canonicalFromText(entry);
          if (canon) items.push(canon);
        } else if (entry != null) {
          items.push(...extractHistoryHighlightsFromDealerText(String(entry)));
        }
      }
    } else {
      return;
    }
    for (const label of items) {
      const key = label.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(label);
    }
  }
  for (const src of sources) absorb(src);
  const pruned = pruneRedundantHighlights(out);
  return pruned.length ? pruned : null;
}

function coalesceHistoryHighlightsForStorage(vehicle) {
  const existing = asHighlightList(vehicle && vehicle.history_highlights);
  const badges = vehicle && Array.isArray(vehicle._vdp_dom_badges) ? vehicle._vdp_dom_badges : null;
  return mergeHistoryHighlights(
    existing,
    extractHistoryHighlightsFromBadges(badges),
    vehicle && vehicle.description
  );
}

module.exports = {
  extractHistoryHighlightsFromDealerText,
  extractHistoryHighlightsFromBadges,
  mergeHistoryHighlights,
  coalesceHistoryHighlightsForStorage,
  historyHighlightsJson(highlights) {
    return nullableJsonArrayText(highlights);
  },
};
