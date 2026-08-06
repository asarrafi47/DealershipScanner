/**
 * Navigation performance: prefetch the destinations a shopper actually takes,
 * plus instant click feedback.
 *
 * Policy, and why:
 *
 *  - Prefetch the EXACT href the link will navigate to, query string included.
 *    Result cards link to /car/<id>?zip_code=<zip> when a ZIP is set (main.js
 *    builds that href). This file used to rebuild the URL as /car/<id>, which is
 *    a different HTTP cache key, so the prefetch never served the navigation and
 *    the server did the work twice.
 *
 *  - One request per prefetch. This used to issue BOTH a fetch() and a
 *    <link rel=prefetch> for the same document; a listings page idle-warming 8
 *    cards produced 16 car-page renders.
 *
 *  - Documents only. It also fetched /api/cars/<id> for every warmed card. That
 *    endpoint does have one consumer -- car_trim_ladder.js hydrates the trim
 *    ladder from it when the server-rendered ladder is absent -- but that runs on
 *    the car page itself, against that page's own car id, and only when the SSR
 *    markup is missing. Firing it from a results grid for cards nobody has opened
 *    yet warmed nothing: measured at ~90-100ms of backend work per card, and a
 *    listings page idle-warming 8 cards spent ~0.8s of server time on responses
 *    that were discarded. The car document is what the navigation needs.
 *
 *  - Never speculatively pull /api/listings/cars. It is the entire active
 *    inventory: 9.36 MB on the wire, ~6s of a 12 Mbit/s link. This file used to
 *    request it on idle from /home, /dashboard and /, and again whenever a
 *    /listings link was hovered, so merely visiting the home page saturated the
 *    connection and every navigation made during that window queued behind it.
 *    The listings page fetches it itself, off the first-paint path, and every
 *    render path there falls back to the server-rendered bootstrap grid until it
 *    lands. Hovering a /listings link prefetches the DOCUMENT (~91 KB), nothing more.
 *
 *  - Bounded and cancellable, and off entirely under Save-Data or a 2g-class
 *    connection.
 *
 * Everything here is an enhancement: with JS disabled or failing, every link is
 * still a plain <a href> and navigates normally.
 */
(function () {
    "use strict";

    var MAX_INFLIGHT = 3;      // concurrent prefetches
    var MAX_PER_PAGE = 12;     // total documents prefetched per page view
    var HOVER_DELAY_MS = 65;   // ignore pointers merely sweeping across a grid

    var prefetched = new Set();
    var inflight = 0;
    var queue = [];
    var budget = MAX_PER_PAGE;
    var controllers = new Set();
    var navBarEl = null;
    var hoverTimer = null;

    // ---- connection policy -------------------------------------------------

    function dataSaverOn() {
        var c = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
        if (!c) return false;
        if (c.saveData) return true;
        var t = String(c.effectiveType || "");
        return t === "slow-2g" || t === "2g";
    }

    // Re-read per call: effectiveType and saveData can change mid-session.
    function prefetchAllowed() {
        return !dataSaverOn() && budget > 0;
    }

    // ---- url helpers -------------------------------------------------------

    function sameOriginUrl(href) {
        try {
            var u = new URL(href, window.location.origin);
            if (u.origin !== window.location.origin) return null;
            if (u.pathname.indexOf("/static/") === 0) return null;
            return u.pathname + u.search;   // keep the query: it is part of the cache key
        } catch (_) {
            return null;
        }
    }

    function isCarPath(path) {
        return /^\/car\/\d+/.test(String(path || ""));
    }

    // ---- prefetch engine ---------------------------------------------------

    function pump() {
        while (inflight < MAX_INFLIGHT && queue.length && budget > 0) {
            var url = queue.shift();
            inflight += 1;
            budget -= 1;
            send(url);
        }
    }

    function send(url) {
        var ctrl = typeof AbortController === "function" ? new AbortController() : null;
        if (ctrl) controllers.add(ctrl);
        var opts = { credentials: "same-origin", headers: { Accept: "text/html" } };
        if (ctrl) opts.signal = ctrl.signal;
        // Chromium honours this and schedules the prefetch beneath anything the
        // user is actually waiting on, including a navigation they just started.
        opts.priority = "low";
        fetch(url, opts)
            .then(function (r) {
                // Drain the body so the response is committed to the HTTP cache.
                // Car pages are Cache-Control: private, max-age=180, so a real
                // navigation to the same URL is served from cache.
                return r && r.ok ? r.text() : null;
            })
            .catch(function () { /* prefetch is best-effort by definition */ })
            .then(function () {
                if (ctrl) controllers.delete(ctrl);
                inflight -= 1;
                pump();
            });
    }

    function prefetchDocument(url) {
        if (!url || prefetched.has(url) || !prefetchAllowed()) return;
        prefetched.add(url);
        queue.push(url);
        pump();
    }

    function cancelAll() {
        queue.length = 0;
        controllers.forEach(function (c) {
            try { c.abort(); } catch (_) {}
        });
        controllers.clear();
    }

    // ---- click affordance --------------------------------------------------

    function ensureNavBar() {
        if (navBarEl && navBarEl.isConnected) return navBarEl;
        navBarEl = document.createElement("div");
        navBarEl.id = "ds-car-nav-bar";
        navBarEl.setAttribute("aria-hidden", "true");
        document.documentElement.appendChild(navBarEl);
        return navBarEl;
    }

    function showCarNavProgress() {
        var bar = ensureNavBar();
        bar.style.width = "35%";
        requestAnimationFrame(function () { bar.style.width = "72%"; });
    }

    function finishCarNavProgress() {
        if (!navBarEl) return;
        navBarEl.style.width = "100%";
        window.setTimeout(function () {
            if (navBarEl) navBarEl.style.width = "0%";
        }, 180);
    }

    // ---- intent ------------------------------------------------------------

    function onLinkIntent(el) {
        if (!el || !el.getAttribute) return;
        var url = sameOriginUrl(el.href);
        if (!url) return;
        if (url === window.location.pathname + window.location.search) return;
        prefetchDocument(url);
    }

    function delegatedIntent(event) {
        var el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        if (!el) return;
        // A pointer crossing a dense result grid touches many cards; only the one
        // it settles on is intent.
        window.clearTimeout(hoverTimer);
        hoverTimer = window.setTimeout(function () { onLinkIntent(el); }, HOVER_DELAY_MS);
    }

    function immediateIntent(event) {
        var el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        if (el) onLinkIntent(el);
    }

    function onCarLinkClick(event) {
        var el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        if (!el || event.defaultPrevented) return;
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        var url = sameOriginUrl(el.href);
        if (!isCarPath(url)) return;
        // The click is committed: let it have the whole connection.
        cancelAll();
        onLinkIntent(el);
        el.classList.add("is-car-nav-pending");
        showCarNavProgress();
    }

    // ---- viewport warming --------------------------------------------------

    var io = null;

    function observeCarLinks(root, limit) {
        if (!prefetchAllowed()) return;
        if (typeof IntersectionObserver !== "function") return;
        if (!io) {
            io = new IntersectionObserver(function (entries) {
                entries.forEach(function (e) {
                    if (!e.isIntersecting) return;
                    io.unobserve(e.target);
                    onLinkIntent(e.target);
                });
            }, { rootMargin: "200px" });
        }
        var max = limit || 8;
        var seen = 0;
        var anchors = (root || document).querySelectorAll('a[href*="/car/"]');
        for (var i = 0; i < anchors.length && seen < max; i++) {
            var url = sameOriginUrl(anchors[i].href);
            if (!isCarPath(url) || prefetched.has(url)) continue;
            seen += 1;
            io.observe(anchors[i]);
        }
    }

    // ---- boot --------------------------------------------------------------

    function boot() {
        document.addEventListener("mouseover", delegatedIntent, { passive: true });
        document.addEventListener("mouseout", function () {
            window.clearTimeout(hoverTimer);
        }, { passive: true });
        document.addEventListener("touchstart", immediateIntent, { passive: true });
        document.addEventListener("focusin", immediateIntent, { passive: true });
        document.addEventListener("click", onCarLinkClick, true);

        window.addEventListener("pageshow", function () {
            // Restored from bfcache: the budget belongs to this page view again.
            budget = MAX_PER_PAGE;
            finishCarNavProgress();
        });
        window.addEventListener("pagehide", cancelAll);

        var warm = function () {
            var p = window.location.pathname;
            if (p === "/home" || p === "/listings" || p === "/dashboard") {
                observeCarLinks(null, 8);
            }
        };
        if (typeof requestIdleCallback === "function") {
            requestIdleCallback(warm, { timeout: 1500 });
        } else {
            window.setTimeout(warm, 250);
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }

    // Public surface. main.js calls __DS_wirePrefetchLinks and
    // __DS_prefetchVisibleCarLinks after it re-renders the results grid.
    window.__DS_wirePrefetchLinks = function (root) { observeCarLinks(root, 8); };
    window.__DS_prefetchVisibleCarLinks = function (root, limit) {
        observeCarLinks(root, limit || 8);
    };
    window.__DS_warmCarDetail = function (carId) {
        if (carId) prefetchDocument("/car/" + carId);
    };
})();
