/**
 * Listings toolbar markup: active-filter chips, the pager's page buttons and
 * result-count labels, and the zero-results geo message.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL; main.js collects the chip list / page numbers from the
 * DOM and writes the returned strings. Uses SC.escapeHtml and
 * SC.buildPageNumberWindow from sc-helpers.js (resolved at call time).
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});

    function scalarChipLabel(name, val) {
        if (name === "max_price" && val) {
            const n = Number(val);
            return Number.isFinite(n) ? `Under $${n >= 1000 ? Math.round(n / 1000) + "k" : n}` : val;
        }
        if (name === "max_mileage" && val) {
            const n = Number(val);
            return Number.isFinite(n) ? `Under ${n.toLocaleString()} mi` : val;
        }
        if (name === "inventory_condition" && val) {
            if (val === "new") return "New";
            if (val === "pre_owned") return "Pre-owned";
            if (val === "cpo") return "Certified Pre-Owned";
        }
        if (name === "radius" && val) return `${val} mi radius`;
        if (name === "zip_code" && val) return val;
        return val;
    }

    /** The smart-search chip shows the query, cut to 28 characters. */
    function searchChipLabel(q) {
        return q.length > 28 ? q.slice(0, 28) + "…" : q;
    }

    /** chips: [{param, value, label}] -> the #listings-active-chips markup. */
    function chipsHtml(chips) {
        return chips.map((c) => (
            `<button type="button" class="listings-filter-chip" data-chip-param="${SC.escapeHtml(c.param)}" data-chip-value="${SC.escapeHtml(c.value)}">`
            + `<span class="listings-filter-chip-label">${SC.escapeHtml(c.label)}</span>`
            + `<span class="listings-filter-chip-x" aria-hidden="true">×</span>`
            + `</button>`
        )).join("");
    }

    /** Page-number buttons (with ellipses) for the pager. */
    function pageNumbersHtml(page, totalPages) {
        return SC.buildPageNumberWindow(page, totalPages).map((item) => {
            if (item === "…") {
                return `<span class="listings-page-ellipsis" aria-hidden="true">…</span>`;
            }
            const active = item === page ? " listings-page-num--active" : "";
            return `<button type="button" class="listings-page-num${active}" data-page="${item}" aria-label="Page ${item}"${item === page ? ' aria-current="page"' : ""}>${item}</button>`;
        }).join("");
    }

    /**
     * Result-count and page-range labels. `page` must already be clamped to
     * the last page (main.js updatePaginationUI does that).
     */
    function paginationLabels(total, page, perPage) {
        const totalPages = Math.max(1, Math.ceil(total / perPage));
        const start = total === 0 ? 0 : (page - 1) * perPage + 1;
        const end = Math.min(page * perPage, total);
        const count = total === 0
            ? "0 vehicles"
            : (totalPages > 1
                ? `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}`
                : `${total.toLocaleString()} vehicle${total !== 1 ? "s" : ""}`);
        const range = totalPages > 1
            ? `Page ${page} of ${totalPages} · ${perPage} per page`
            : "";
        return { count, range };
    }

    /** Zero results inside the shopper's radius (lists up to four checked makes). */
    function zeroResultsGeoMessage(radiusMi, zipCode, activeMakes) {
        let geoMsg =
            "No listings within " + radiusMi + " mi of " + zipCode + " yet.";
        if (activeMakes.length) {
            geoMsg =
                "No " +
                activeMakes.slice(0, 4).join(", ") +
                (activeMakes.length > 4 ? "…" : "") +
                " within " +
                radiusMi +
                " mi of " +
                zipCode +
                ". Try a larger radius or different makes.";
        }
        return geoMsg;
    }

    DSL.scalarChipLabel = scalarChipLabel;
    DSL.searchChipLabel = searchChipLabel;
    DSL.chipsHtml = chipsHtml;
    DSL.pageNumbersHtml = pageNumbersHtml;
    DSL.paginationLabels = paginationLabels;
    DSL.zeroResultsGeoMessage = zeroResultsGeoMessage;
})(window);
