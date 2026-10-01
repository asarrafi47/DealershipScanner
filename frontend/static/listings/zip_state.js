/**
 * Listings ZIP / scope / URL-state parsing: the "zip|radius" scope key the
 * loaded inventory must match, the radius clamp, the cars API URL and error
 * shape, car-link ZIP rewriting, and page / per-page parsing.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL; main.js reads the inputs (ZIP box, radius select,
 * location.search, localStorage) and passes the values in. Uses
 * SC.isValidUsZip from sc-helpers.js (resolved at call time).
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});

    function clampListingsRadius(raw) {
        const n = parseFloat(raw);
        if (!Number.isFinite(n)) return 50;
        return Math.max(5, Math.min(250, n));
    }

    /** "zip|radius" the loaded ALL_CARS must match ("" without a valid ZIP). */
    function listingsCarsScopeKey(zip, radiusRaw) {
        if (!SC.isValidUsZip(zip)) return "";
        return `${zip.trim()}|${clampListingsRadius(radiusRaw)}`;
    }

    /** Cache key for the radius-scoped row set ("" when ZIP or radius is missing). */
    function listingsRadiusFilterKey(zip, radiusRaw) {
        const radiusMi = parseFloat(radiusRaw) || null;
        if (!SC.isValidUsZip(zip) || !radiusMi) return "";
        return `${zip.trim()}|${radiusMi}`;
    }

    /** GET URL for a "zip|radius" scope (the dealer scope's URL comes from the page). */
    function listingsCarsUrl(scope) {
        const parts = String(scope || "").split("|");
        return `/api/listings/cars?zip=${encodeURIComponent(parts[0] || "")}`
            + `&radius=${encodeURIComponent(parts[1] || "")}`;
    }

    function listingsCarsError(code) {
        const err = new Error(code || "cars fetch failed");
        err.code = code || "fetch_failed";
        return err;
    }

    /** What the search-form ZIP inputs hold: digits only, at most five. */
    function zipDigits(value) {
        return String(value || "").replace(/\D/g, "").slice(0, 5);
    }

    /** A result-card link carrying the shopper's ZIP; null when `href` is not a /car/<id> link. */
    function carHrefWithZip(href, zip) {
        const m = String(href || "").match(/^\/car\/(\d+)/);
        if (!m) return null;
        return `/car/${m[1]}?zip_code=${encodeURIComponent(zip)}`;
    }

    /** Backoff before refetching a `partial` scoped payload (try 0 -> 3 s, capped at 30 s). */
    function partialRefetchDelayMs(tries) {
        return Math.min(30000, 3000 * Math.pow(2, tries));
    }

    /** ?page= from a location.search string (1 when absent or invalid). */
    function pageFromSearch(search) {
        const p = parseInt(new URLSearchParams(search).get("page"), 10);
        return Number.isFinite(p) && p > 0 ? p : 1;
    }

    const PER_PAGE_CHOICES = [12, 24, 48];

    /** The per-page select's value as a number, or `fallback` if it is not a choice. */
    function normalizePerPage(raw, fallback) {
        return PER_PAGE_CHOICES.includes(raw) ? raw : fallback;
    }

    /** Initial per-page: the URL's ?per_page= wins, then this browser's saved choice. */
    function pickPerPage(urlPp, saved, fallback) {
        return PER_PAGE_CHOICES.includes(urlPp) ? urlPp : (PER_PAGE_CHOICES.includes(saved) ? saved : fallback);
    }

    DSL.clampListingsRadius = clampListingsRadius;
    DSL.listingsCarsScopeKey = listingsCarsScopeKey;
    DSL.listingsRadiusFilterKey = listingsRadiusFilterKey;
    DSL.listingsCarsUrl = listingsCarsUrl;
    DSL.listingsCarsError = listingsCarsError;
    DSL.zipDigits = zipDigits;
    DSL.carHrefWithZip = carHrefWithZip;
    DSL.partialRefetchDelayMs = partialRefetchDelayMs;
    DSL.pageFromSearch = pageFromSearch;
    DSL.normalizePerPage = normalizePerPage;
    DSL.pickPerPage = pickPerPage;
})(window);
