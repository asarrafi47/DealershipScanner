/**
 * Navigation performance: prefetch car/listings pages + instant click feedback.
 */
(function () {
    "use strict";

    const prefetchedDocs = new Set();
    const warmedApis = new Set();
    let navBarEl = null;

    function sameOriginPath(href) {
        try {
            const u = new URL(href, window.location.origin);
            if (u.origin !== window.location.origin) return null;
            if (u.pathname.startsWith("/static/")) return null;
            if (u.hash && !u.pathname) return null;
            return u.pathname + u.search;
        } catch (_) {
            return null;
        }
    }

    function carIdFromPath(path) {
        const m = String(path || "").match(/^\/car\/(\d+)/);
        return m ? m[1] : null;
    }

    function ensureNavBar() {
        if (navBarEl) return navBarEl;
        navBarEl = document.createElement("div");
        navBarEl.id = "ds-car-nav-bar";
        navBarEl.setAttribute("aria-hidden", "true");
        document.documentElement.appendChild(navBarEl);
        return navBarEl;
    }

    function showCarNavProgress() {
        const bar = ensureNavBar();
        bar.style.width = "35%";
        requestAnimationFrame(function () {
            bar.style.width = "72%";
        });
    }

    function finishCarNavProgress() {
        if (!navBarEl) return;
        navBarEl.style.width = "100%";
        window.setTimeout(function () {
            if (navBarEl) navBarEl.style.width = "0%";
        }, 180);
    }

    function prefetchDocument(path) {
        if (!path || prefetchedDocs.has(path)) return;
        prefetchedDocs.add(path);
        fetch(path, { credentials: "same-origin", headers: { Accept: "text/html" } }).catch(function () {});
        const link = document.createElement("link");
        link.rel = "prefetch";
        link.href = path;
        link.as = "document";
        document.head.appendChild(link);
    }

    function storeCarsEtag(response) {
        const etag = response && response.headers && response.headers.get("ETag");
        if (etag) window.__DS_listingsCarsEtag = etag;
    }

    function applyEarlyCarsPayload(data) {
        if (!data || !data.ok || !Array.isArray(data.cars)) return;
        window.__DS_prefetchCars = data.cars;
        if (typeof window.__DS_buildCarRowsFromCars === "function") {
            window.CAR_ROWS = window.__DS_buildCarRowsFromCars(data.cars);
        }
        window.ALL_CARS = data.cars;
    }

    function applyEarlyGeoPayload(data) {
        if (!data || !data.ok) return;
        if (typeof data.zip_coords === "object") window.ZIP_COORDS = data.zip_coords;
        if (typeof data.dealer_coords === "object") window.DEALER_COORDS = data.dealer_coords;
        if (typeof data.registry_id_by_host === "object") {
            window.REGISTRY_ID_BY_DEALER_HOST = data.registry_id_by_host;
        }
        window.__DS_listingsGeoCoordsReady = true;
    }

    function warmListingsApis() {
        if (warmedApis.has("listings")) return;
        warmedApis.add("listings");

        const headers = {};
        if (window.__DS_listingsCarsEtag) {
            headers["If-None-Match"] = window.__DS_listingsCarsEtag;
        }

        window.__DS_listingsCarsPrefetchPromise = fetch("/api/listings/cars", {
            credentials: "same-origin",
            headers,
        })
            .then(function (r) {
                storeCarsEtag(r);
                if (r.status === 304) return null;
                return r.ok ? r.json() : null;
            })
            .then(function (data) {
                if (data) applyEarlyCarsPayload(data);
            })
            .catch(function () {});

        window.__DS_listingsGeoPrefetchPromise = fetch("/api/listings/geo-coords", {
            credentials: "same-origin",
        })
            .then(function (r) {
                return r.ok ? r.json() : null;
            })
            .then(function (data) {
                applyEarlyGeoPayload(data);
            })
            .catch(function () {});
    }

    function warmDealersApi() {
        if (warmedApis.has("dealers")) return;
        warmedApis.add("dealers");
        fetch("/api/dealer-locator?limit=1", { credentials: "same-origin" }).catch(function () {});
    }

    function warmCarDetail(carId) {
        if (!carId) return;
        const key = "car:" + carId;
        if (warmedApis.has(key)) return;
        warmedApis.add(key);
        const path = "/car/" + carId;
        prefetchDocument(path);
        fetch("/api/cars/" + carId, { credentials: "same-origin" }).catch(function () {});
    }

    function onLinkIntent(el) {
        if (!el || !el.href) return;
        const path = sameOriginPath(el.href);
        if (!path) return;
        const carId = carIdFromPath(path);
        if (carId) {
            warmCarDetail(carId);
            return;
        }
        prefetchDocument(path);
        if (path === "/listings" || path.indexOf("/listings?") === 0) {
            warmListingsApis();
        }
        if (path === "/find-dealers" || path.indexOf("/find-dealers?") === 0) {
            warmDealersApi();
        }
    }

    function wireLink(el) {
        if (!el || el.dataset.dsPrefetchWired || !el.href) return;
        const path = sameOriginPath(el.href);
        if (!path) return;
        const here = window.location.pathname + window.location.search;
        if (path === here) return;
        el.dataset.dsPrefetchWired = "1";
        el.addEventListener("mouseenter", function () {
            onLinkIntent(el);
        }, { once: true, passive: true });
        el.addEventListener("focus", function () {
            onLinkIntent(el);
        }, { once: true });
        el.addEventListener("touchstart", function () {
            onLinkIntent(el);
        }, { once: true, passive: true });
    }

    function scanLinks(root) {
        (root || document).querySelectorAll('a[href^="/car/"], a[href*="' + window.location.origin + '/car/"]').forEach(wireLink);
        (root || document).querySelectorAll("a[href]").forEach(function (el) {
            const path = sameOriginPath(el.href);
            if (!path || !carIdFromPath(path)) return;
            wireLink(el);
        });
    }

    function prefetchVisibleCarLinks(root, limit) {
        const max = limit || 12;
        const seen = new Set();
        const anchors = (root || document).querySelectorAll("a[href]");
        let n = 0;
        anchors.forEach(function (el) {
            if (n >= max) return;
            const path = sameOriginPath(el.href);
            const carId = carIdFromPath(path);
            if (!carId || seen.has(carId)) return;
            seen.add(carId);
            n += 1;
            window.setTimeout(function () {
                warmCarDetail(carId);
            }, n * 40);
        });
    }

    function handleDelegatedIntent(event) {
        const el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        if (!el) return;
        const path = sameOriginPath(el.href);
        if (!path || !carIdFromPath(path)) return;
        onLinkIntent(el);
    }

    function onCarLinkClick(event) {
        const el = event.target && event.target.closest
            ? event.target.closest('a[href^="/car/"], a[href*="/car/"]')
            : null;
        if (!el || !el.href) return;
        if (event.defaultPrevented) return;
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        const path = sameOriginPath(el.href);
        const carId = carIdFromPath(path);
        if (!carId) return;
        onLinkIntent(el);
        el.classList.add("is-car-nav-pending");
        showCarNavProgress();
    }

    function idleWarmCurrentPage() {
        const p = window.location.pathname;
        if (p === "/home" || p === "/dashboard" || p === "/") {
            warmListingsApis();
        }
        if (p === "/home" || p === "/listings") {
            prefetchVisibleCarLinks(null, p === "/home" ? 16 : 8);
        }
    }

    function boot() {
        scanLinks();
        document.addEventListener("mouseover", handleDelegatedIntent, { passive: true });
        document.addEventListener("touchstart", handleDelegatedIntent, { passive: true });
        document.addEventListener("click", onCarLinkClick, true);
        window.addEventListener("pageshow", finishCarNavProgress);
        if (typeof requestIdleCallback === "function") {
            requestIdleCallback(idleWarmCurrentPage, { timeout: 800 });
        } else {
            window.setTimeout(idleWarmCurrentPage, 1);
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }

    window.__DS_warmListingsApis = warmListingsApis;
    window.__DS_warmCarDetail = warmCarDetail;
    window.__DS_wirePrefetchLinks = scanLinks;
    window.__DS_prefetchVisibleCarLinks = prefetchVisibleCarLinks;
})();
