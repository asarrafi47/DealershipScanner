/**
 * Self-contained listings helpers (formatters, predicates, HTML builders,
 * market-cohort math). Extracted verbatim from main.js's DOMContentLoaded
 * closure. Every function here is PURE with respect to that closure — it
 * reads only its arguments and window-level globals, never the closure's
 * shared mutable state or cached DOM element references. Exposed on a single
 * window.SC namespace and loaded BEFORE main.js so main.js call sites can
 * reference them as SC.<fn>. See listings.html for load order.
 */
window.SC = window.SC || {};
(function (SC) {
    "use strict";

    function escapeHtml(s) {
        return String(s ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function fmt(n)  { return Number(n).toLocaleString(); }

    function fmtUSD(n) {
        if (n == null || n === "" || Number(n) === 0) return "Call for Price";
        return "$" + Number(n).toLocaleString("en-US", {maximumFractionDigits: 0});
    }

    /** Resolve URL and allow only http(s) for listing images (mitigates javascript: / data:). */
    function safeImageSrc(url) {
        const raw = String(url || "").trim();
        if (!raw) return "/static/placeholder.svg";
        try {
            const abs = new URL(raw, window.location.origin);
            if (abs.protocol !== "http:" && abs.protocol !== "https:") {
                return "/static/placeholder.svg";
            }
            return abs.href;
        } catch (_) {
            return "/static/placeholder.svg";
        }
    }

    function normFilterStr(v) {
        return (v == null || v === "") ? "" : String(v).trim().toLowerCase();
    }

    function isValidUsZip(zip) {
        return /^\d{5}$/.test(String(zip || "").trim());
    }

    function buildPageNumberWindow(current, total) {
        if (total <= 7) {
            return Array.from({ length: total }, (_, i) => i + 1);
        }
        const pages = new Set([1, total, current, current - 1, current + 1]);
        const sorted = [...pages].filter((p) => p >= 1 && p <= total).sort((a, b) => a - b);
        const out = [];
        let prev = 0;
        for (const p of sorted) {
            if (p - prev > 1) out.push("…");
            out.push(p);
            prev = p;
        }
        return out;
    }

    function skeletonCardsHtml(count) {
        return Array.from({ length: count }, () => (
            `<div class="result-card result-card--skeleton" aria-hidden="true">`
            + `<div class="result-image-wrap"><div class="result-image result-image--skeleton"></div></div>`
            + `<div class="result-content">`
            + `<div class="result-skel-line result-skel-line--title"></div>`
            + `<div class="result-skel-line result-skel-line--price"></div>`
            + `<div class="result-skel-line result-skel-line--meta"></div>`
            + `</div></div>`
        )).join("");
    }

    function mileageBand(mileage) {
        const m = parseInt(mileage, 10);
        if (!Number.isFinite(m)) return "unknown";
        if (m < 0) return "unknown";
        if (m <= 25000) return "0-25k";
        if (m <= 50000) return "25-50k";
        if (m <= 75000) return "50-75k";
        if (m <= 100000) return "75-100k";
        return "100k+";
    }

    function valueInListCI(list, val) {
        if (!list || !list.length) return true;
        const v = normFilterStr(val);
        return list.some(x => normFilterStr(x) === v);
    }

    function valueInListCISmart(list, val) {
        if (!list || !list.length || val == null || val === "") return false;
        const v = String(val).trim().toLowerCase();
        return list.some((x) => String(x).trim().toLowerCase() === v);
    }

    function dealBadgeHtml(mkt) {
        if (!mkt || mkt.delta_pct == null) return "";
        const vs = mkt.vs_market || "near_market";
        const labels = {
            below_market: "Below market",
            above_market: "Above market",
            near_market: "At market",
        };
        const sign = Number(mkt.delta_pct) > 0 ? "+" : "";
        const text = labels[vs] || "At market";
        return `<span class="result-deal-badge result-deal-badge--${escapeHtml(vs)}">`
            + `${escapeHtml(text)} <span class="result-deal-badge-pct">${sign}${escapeHtml(String(mkt.delta_pct))}%</span>`
            + `</span>`;
    }

    function dealScoreBadgeHtml(ds) {
        if (!ds || !ds.label || ds.label === "insufficient_data") return "";
        const cls = ds.label === "at_market" ? "near_market" : ds.label;
        const labels = {
            below_market: "Below market",
            above_market: "Above market",
            at_market: "Fair price",
        };
        // Unknown labels (e.g. payment_listed — the "price" is an advertised
        // payment, not comparable) get NO badge, never a defaulted verdict.
        if (!labels[ds.label]) return "";
        const text = labels[ds.label];
        let pctStr = "";
        if (ds.pct_from_median != null && ds.label !== "at_market") {
            const p = Math.abs(Number(ds.pct_from_median));
            if (Number.isFinite(p)) {
                pctStr = ` <span class="result-deal-badge-pct">${ds.label === "below_market" ? "-" : "+"}${Math.round(p)}%</span>`;
            }
        }
        return `<span class="result-deal-badge result-deal-badge--${escapeHtml(cls)}">${escapeHtml(text)}${pctStr}</span>`;
    }

    function cpoBadgeHtml(c) {
        if (!c.is_cpo) return "";
        return `<span class="result-cpo-badge" title="Certified Pre-Owned">Certified Pre-Owned</span>`;
    }

    function priceDropBadgeHtml(c) {
        const amt = Number(c.price_drop_amount);
        if (!amt || !Number.isFinite(amt) || amt <= 0) return "";
        const days = Number(c.price_drop_days_ago);
        const when = Number.isFinite(days) ? (days <= 0 ? "today" : days === 1 ? "1 day ago" : `${days} days ago`) : "";
        return `<span class="result-price-drop-badge">`
            + `▼ $${Math.round(amt).toLocaleString()}${when ? ` <span class="result-price-drop-badge-when">${escapeHtml(when)}</span>` : ""}`
            + `</span>`;
    }

    function carSmartEquipmentHaystack(c) {
        if (!c || typeof c !== "object") return "";
        return [
            ...(c.package_names || []),
            c.title,
            c.trim,
            c.engine_description,
            c.make,
            c.model,
            c.fuel_type,
            c.drivetrain,
            c.forced_induction,
            c.exterior_color,
            c.interior_color,
            c.body_style,
            c.is_cpo ? "certified pre-owned cpo" : null,
        ]
            .filter(Boolean)
            .join(" ")
            .toLowerCase();
    }

    function marketTrimParts(car) {
        return [
            String(car.make || "").trim().toLowerCase(),
            String(car.model || "").trim().toLowerCase(),
            String(car.trim || "").trim().toLowerCase(),
        ];
    }

    function marketCohortKey(make, model, trim, year, band) {
        const [mk, md, tr] = marketTrimParts({ make, model, trim });
        const ys = year != null ? String(year) : "*";
        return `${mk}|${md}|${tr}|${ys}|${band}`;
    }

    function weightedCohortStats(entries, minSamples) {
        let sum = 0;
        let n = 0;
        for (const e of entries) {
            if (!e) continue;
            const count = Number(e.sample_count);
            const avg = Number(e.avg_price);
            if (!Number.isFinite(count) || count <= 0 || !Number.isFinite(avg)) continue;
            sum += avg * count;
            n += count;
        }
        if (n < minSamples) return null;
        return { avg_price: sum / n, sample_count: n };
    }

    function cohortEntries(cohorts, mk, md, tr, years, bands) {
        const out = [];
        for (const y of years) {
            for (const band of bands) {
                const key = `${mk}|${md}|${tr}|${y}|${band}`;
                if (cohorts[key]) out.push(cohorts[key]);
            }
        }
        return out;
    }

    SC.escapeHtml = escapeHtml;
    SC.fmt = fmt;
    SC.fmtUSD = fmtUSD;
    SC.safeImageSrc = safeImageSrc;
    SC.normFilterStr = normFilterStr;
    SC.isValidUsZip = isValidUsZip;
    SC.buildPageNumberWindow = buildPageNumberWindow;
    SC.skeletonCardsHtml = skeletonCardsHtml;
    SC.mileageBand = mileageBand;
    SC.valueInListCI = valueInListCI;
    SC.valueInListCISmart = valueInListCISmart;
    SC.dealBadgeHtml = dealBadgeHtml;
    SC.dealScoreBadgeHtml = dealScoreBadgeHtml;
    SC.cpoBadgeHtml = cpoBadgeHtml;
    SC.priceDropBadgeHtml = priceDropBadgeHtml;
    SC.carSmartEquipmentHaystack = carSmartEquipmentHaystack;
    SC.marketTrimParts = marketTrimParts;
    SC.marketCohortKey = marketCohortKey;
    SC.weightedCohortStats = weightedCohortStats;
    SC.cohortEntries = cohortEntries;
})(window.SC);
