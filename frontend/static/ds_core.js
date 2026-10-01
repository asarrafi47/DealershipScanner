/**
 * Shared page helpers: window.DS = { escapeHtml, csrfToken, formatUsd }.
 *
 * Load this before any script that calls into window.DS. It defines window.DS and
 * nothing else (no DOM work at load time), and a second load on the same page
 * (e.g. the AI-assistant partial plus the page's own tag) keeps the first copy.
 *
 * Each helper reproduces, byte for byte, the copies it replaced; files whose
 * copy differs on purpose keep their own (see docs/monolith_audit_2026_10_01/
 * frontend.md, F3). Unit tests: tests/js/ds_core.test.js.
 */
(function (root) {
    "use strict";
    if (root.DS && typeof root.DS.escapeHtml === "function") return;

    /** Escape for HTML text and quoted attribute values: & < > " ' (null/undefined -> ""). */
    function escapeHtml(s) {
        return String(s ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    /** The page's CSRF token from <meta name="csrf-token" content="...">, or "". */
    function csrfToken() {
        const m = root.document && root.document.querySelector('meta[name="csrf-token"]');
        return m && m.content ? m.content : "";
    }

    /**
     * US-dollar text. Two families, chosen by opts.strict:
     *
     * Listing prices (default): null, "" and 0 (0 only while zeroIsEmpty is not
     * false) give opts.empty (default "Call for Price"); anything else is coerced
     * with Number() and printed as "$" + en-US grouping, no decimals. A value that
     * coerces to NaN/Infinity prints as-is ("$NaN") unless opts.invalid is given.
     *
     * Computed amounts (strict: true): only a finite number is formatted, anything
     * else gives opts.invalid (default "$--"). Whole dollars via Math.round, or
     * cents: true for "$" + toFixed(2) without grouping, or short: true for
     * "$12.3k" / "$45k" / "$123k" at 1,000 and above (whole dollars below).
     */
    function formatUsd(n, opts) {
        const o = opts || {};
        if (o.strict) {
            if (!Number.isFinite(n)) return o.invalid != null ? o.invalid : "$--";
            if (o.cents) return "$" + n.toFixed(2);
            if (o.short && n >= 1000) {
                const thousands = n / 1000;
                const rounded =
                    thousands >= 100
                        ? Math.round(thousands)
                        : parseFloat(thousands.toFixed(thousands < 10 ? 1 : 0));
                return "$" + rounded.toLocaleString("en-US") + "k";
            }
            return "$" + Math.round(n).toLocaleString("en-US");
        }
        const empty = o.empty != null ? o.empty : "Call for Price";
        if (n == null || n === "") return empty;
        const v = Number(n);
        if (v === 0 && o.zeroIsEmpty !== false) return empty;
        if (!Number.isFinite(v) && o.invalid != null) return o.invalid;
        return "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 });
    }

    root.DS = { escapeHtml: escapeHtml, csrfToken: csrfToken, formatUsd: formatUsd };
})(window);
