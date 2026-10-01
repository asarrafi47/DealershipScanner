/**
 * Dealership inventory browser — the two adjustments main.js needs to run
 * dealer-scoped instead of fleet-scoped.
 *
 * Everything else (filter cascade, sort, per-page, pagination, save, compare)
 * is main.js verbatim: dealership.html ships the same DOM contract and the same
 * boot blobs as listings.html, only restricted to one rooftop.
 *
 * Loads BEFORE main.js (see dealership.html) so the facet-options redirect is in
 * place before main.js's hydrateAllLazyFacets() fires.
 */
(function () {
    "use strict";

    var body = document.body;
    if (!body || !document.getElementById("ds-listings-car-rows")) return;

    // ── 1. Dealer-scoped facet options ─────────────────────────────────
    // main.js hard-codes /api/listings/filter-options for its lazy Model / Trim /
    // Package hydration. On this page those lists have to come from ONE dealer:
    // grafting in the fleet-wide 7,300 models would offer options this lot does
    // not stock and would re-import the payload the /listings work just removed.
    // Redirecting that single URL is the smallest change that needs no main.js
    // edit; the durable fix is a `data-facet-options-url` override in main.js,
    // which this page already declares on <body> ready for it.
    var facetUrl = body.getAttribute("data-facet-options-url");
    if (facetUrl && typeof window.fetch === "function") {
        var nativeFetch = window.fetch.bind(window);
        window.fetch = function (input, init) {
            var url = "";
            try {
                url = typeof input === "string" ? input : (input && input.url) || "";
            } catch (_) {
                url = "";
            }
            if (url === "/api/listings/filter-options") {
                return nativeFetch(facetUrl, init);
            }
            return nativeFetch(input, init);
        };
    }

    // ── 2. Hide the synthetic ZIP from the active-filter chips ─────────
    // The hidden #listings-zip-input only exists to satisfy main.js's
    // "no ZIP → no filtering" gate (see _dealer_filter_zip). It is not a filter
    // the visitor chose, and a chip for it would let them clear it and silently
    // freeze every other filter on the page.
    function dropGeoChips() {
        var chips = document.getElementById("listings-active-chips");
        if (!chips) return;
        var junk = chips.querySelectorAll(
            '[data-chip-param="zip_code"], [data-chip-param="radius"]'
        );
        for (var i = 0; i < junk.length; i++) junk[i].remove();
        var left = chips.children.length;
        var wrap = document.getElementById("listings-active-chips-wrap");
        if (wrap) wrap.hidden = left === 0;
        var count = document.getElementById("listings-filters-mobile-count");
        if (count) {
            count.textContent = String(left);
            count.hidden = left === 0;
        }
    }

    function watchChips() {
        var chips = document.getElementById("listings-active-chips");
        if (!chips) return;
        dropGeoChips();
        new MutationObserver(dropGeoChips).observe(chips, { childList: true });
    }

    // ── 3. "Hide this dealership from my searches" toggle ──────────────
    // Signed-in only (the template omits the button otherwise). Talks to the
    // same per-user list the profile page manages (/api/profile/hidden-dealers).
    function csrfToken() {
        return window.DS.csrfToken();
    }

    function wireHideToggle() {
        var btn = document.getElementById("dealer-hide-toggle");
        if (!btn || typeof window.fetch !== "function") return;
        var status = document.getElementById("dealer-hide-status");
        var dealerId = btn.getAttribute("data-dealer-id") || "";
        if (!dealerId) return;

        function paint(hidden) {
            btn.setAttribute("data-hidden", hidden ? "1" : "0");
            btn.setAttribute("aria-pressed", hidden ? "true" : "false");
            btn.textContent = hidden ? "Unhide this dealership" : "Hide this dealership from my searches";
            if (!status) return;
            status.textContent = "";
            if (hidden) {
                status.appendChild(document.createTextNode("Hidden from your searches. "));
                var a = document.createElement("a");
                a.href = "/account/profile#hidden-dealers";
                a.textContent = "Manage";
                status.appendChild(a);
            }
        }

        btn.addEventListener("click", function () {
            var hidden = btn.getAttribute("data-hidden") === "1";
            btn.disabled = true;
            var req = hidden
                ? fetch("/api/profile/hidden-dealers/" + encodeURIComponent(dealerId), {
                    method: "DELETE",
                    credentials: "same-origin",
                    headers: { "X-CSRF-Token": csrfToken() },
                })
                : fetch("/api/profile/hidden-dealers", {
                    method: "POST",
                    credentials: "same-origin",
                    headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken() },
                    body: JSON.stringify({ dealer_id: dealerId }),
                });
            req.then(function (r) { return r.json().then(function (j) { return { status: r.status, body: j }; }); })
                .then(function (res) {
                    if (res.status === 401) {
                        window.location.href = "/login";
                        return;
                    }
                    if (!res.body || !res.body.ok) {
                        if (status) status.textContent = "Could not update. Try again.";
                        return;
                    }
                    paint(!!res.body.hidden);
                })
                .catch(function () {
                    if (status) status.textContent = "Could not update. Try again.";
                })
                .then(function () { btn.disabled = false; });
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            watchChips();
            wireHideToggle();
        });
    } else {
        watchChips();
        wireHideToggle();
    }
})();
