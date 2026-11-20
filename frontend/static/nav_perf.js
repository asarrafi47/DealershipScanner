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
 *    cards produced 16 car-page renders. Since 2026-11-20 the same rule applies
 *    across mechanisms: where the browser runs speculation rules (Chromium; see
 *    below) the RULES do every prefetch and this file never fetch()es; the
 *    fetch() path below is only the fallback for browsers without them.
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
 *  - Never speculatively pull /api/listings/cars. It used to be the entire
 *    active inventory (9.36 MB on the wire) and this file requested it on idle
 *    from /home, /dashboard and /, saturating the connection. Since 2026-09-28 it
 *    is radius-scoped (?zip=&radius=, 400 without a ZIP) and the listings page
 *    requests it only once the shopper begins a search -- a speculative fetch
 *    would both guess the scope and break that rule. Hovering a /listings link
 *    prefetches the DOCUMENT, nothing more.
 *
 *  - Only side-effect-free documents (2026-11-20; ALLOWED_DOC below, the same list
 *    as frontend/static/speculation-rules.json): /, bare /listings, /car/<id>
 *    (optionally ?zip_code=), /find-dealers. A /listings URL WITH a query records
 *    search history and analytics and writes the remembered area to the session,
 *    /compare?ids= records a compare session, /premium pops a session flag, /home
 *    and /dashboard are per-user and heavy: none of them is speculated. Car pages
 *    record a "view" unless the request says it is a prefetch (X-DS-Prefetch here,
 *    Sec-Purpose for speculation rules; backend/routes/car_detail/viewer.py), and
 *    are never PRERENDERED: a prerender would run the page and its activation
 *    would not reach the server.
 *
 *  - Bounded and cancellable, and off entirely under Save-Data or a 2g-class
 *    connection. Speculation rules cannot read either signal: Chromium skips them
 *    itself under Save-Data (and Battery/Memory Saver, or with preloading turned
 *    off), and for 2g this file sets <html data-ds-nospec>, which every rule in
 *    speculation-rules.json excludes via selector_matches.
 *
 * Speculation rules (Chromium 121+): delivered by the Speculation-Rules response
 * header (backend/web/static.py), so they need no inline script and no CSP nonce
 * and also work on edge-cached HTML. They prefetch the allowed links on hover
 * (moderate eagerness: ~200 ms hover, or pointerdown), prefetch the few car cards
 * this file marks data-ds-warm on /listings, and PRERENDER only links marked
 * data-ds-prerender (the landing's "Browse inventory" -> bare /listings).
 *
 * Everything here is an enhancement: with JS disabled or failing, every link is
 * still a plain <a href> and navigates normally; browsers without speculation
 * rules, view transitions or bfcache simply ignore those parts.
 */
(function () {
    "use strict";

    var MAX_INFLIGHT = 3;      // concurrent prefetches
    var MAX_PER_PAGE = 12;     // total documents prefetched per page view
    var WARM_LIMIT = 3;        // in-view cars warmed by themselves, once the browser is idle
    var WARM_IDLE_TIMEOUT_MS = 3000;
    var HOVER_DELAY_MS = 65;   // ignore pointers merely sweeping across a grid
    var NAV_BAR_GIVE_UP_MS = 15000;  // a navigation that never unloads (204, download)

    // Speculation rules do the prefetching where the browser supports them.
    var SPEC_RULES = !!(window.HTMLScriptElement && typeof HTMLScriptElement.supports === "function" &&
        HTMLScriptElement.supports("speculationrules"));

    var prefetched = new Set();
    var inflight = 0;
    var queue = [];
    var budget = MAX_PER_PAGE;
    var controllers = new Set();
    var navBarEl = null;
    var navBarTimer = null;
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

    // Every speculation rule excludes ":root[data-ds-nospec] *"; Chromium re-evaluates
    // selector_matches when the attribute changes.
    function syncSpeculationSwitch() {
        var root = document.documentElement;
        if (!root) return;
        if (dataSaverOn()) root.setAttribute("data-ds-nospec", "");
        else root.removeAttribute("data-ds-nospec");
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

    // The documents a speculative request may fetch (mirrors speculation-rules.json).
    var ALLOWED_DOC = /^(?:\/|\/listings|\/find-dealers|\/car\/\d+(?:\?zip_code=[^&#]*)?)$/;

    function allowedDocument(url) {
        return ALLOWED_DOC.test(String(url || ""));
    }

    // ---- prefetch engine (fallback for browsers without speculation rules) --

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
        var opts = { credentials: "same-origin", headers: { Accept: "text/html", "X-DS-Prefetch": "1" } };
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
        // With speculation rules the browser prefetches this very href itself; a
        // fetch() here would be a second request for the same document.
        if (SPEC_RULES) return;
        if (!allowedDocument(url) || prefetched.has(url) || !prefetchAllowed()) return;
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

    function showNavProgress() {
        var bar = ensureNavBar();
        bar.style.width = "35%";
        requestAnimationFrame(function () { bar.style.width = "72%"; });
        window.clearTimeout(navBarTimer);
        navBarTimer = window.setTimeout(finishNavProgress, NAV_BAR_GIVE_UP_MS);
    }

    function finishNavProgress() {
        window.clearTimeout(navBarTimer);
        var pending = document.querySelectorAll(".is-car-nav-pending");
        for (var i = 0; i < pending.length; i++) pending[i].classList.remove("is-car-nav-pending");
        if (!navBarEl) return;
        navBarEl.style.width = "100%";
        window.setTimeout(function () {
            if (navBarEl) navBarEl.style.width = "0%";
        }, 180);
    }

    // A click that will load another document in THIS tab: plain left click, no
    // modifier, no target/download, same origin, not just a #fragment of this page.
    function documentNavigationUrl(el, event) {
        if (!el || event.defaultPrevented) return null;
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return null;
        if (el.hasAttribute("download")) return null;
        var target = (el.getAttribute("target") || "").toLowerCase();
        if (target && target !== "_self") return null;
        var u;
        try { u = new URL(el.href, window.location.href); } catch (_) { return null; }
        if (u.protocol !== "http:" && u.protocol !== "https:") return null;
        if (u.origin !== window.location.origin) return null;
        if (u.pathname.indexOf("/static/") === 0 || u.pathname.indexOf("/api/") === 0) return null;
        if (u.hash && u.pathname === window.location.pathname && u.search === window.location.search) return null;
        return u.pathname + u.search;
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

    // Capture phase: a committed click on a car card gets the whole connection.
    function onLinkClickCapture(event) {
        var el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        var url = documentNavigationUrl(el, event);
        if (url) cancelAll();
    }

    // Bubble phase, so a handler that turns the click into something else
    // (preventDefault) is seen and gets no progress bar.
    function onLinkClick(event) {
        var el = event.target && event.target.closest
            ? event.target.closest("a[href]")
            : null;
        var url = documentNavigationUrl(el, event);
        if (!url) return;
        if (isCarPath(url)) el.classList.add("is-car-nav-pending");
        showNavProgress();
    }

    function onFormSubmit(event) {
        var form = event.target;
        if (event.defaultPrevented || !form || form.tagName !== "FORM") return;
        var target = (form.getAttribute("target") || "").toLowerCase();
        if (target && target !== "_self") return;
        showNavProgress();
    }

    // ---- viewport warming --------------------------------------------------

    var io = null;

    function warmLink(el) {
        var url = sameOriginUrl(el.href);
        if (!isCarPath(url) || !allowedDocument(url) || prefetched.has(url) || !prefetchAllowed()) return;
        if (SPEC_RULES) {
            // The "immediate" rule in speculation-rules.json prefetches a[data-ds-warm].
            prefetched.add(url);
            budget -= 1;
            el.setAttribute("data-ds-warm", "");
            return;
        }
        prefetchDocument(url);
    }

    function observeCarLinks(root, limit) {
        if (!prefetchAllowed()) return;
        if (typeof IntersectionObserver !== "function") return;
        if (!io) {
            io = new IntersectionObserver(function (entries) {
                entries.forEach(function (e) {
                    if (!e.isIntersecting) return;
                    io.unobserve(e.target);
                    warmLink(e.target);
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
        syncSpeculationSwitch();
        var conn = navigator.connection;
        if (conn && typeof conn.addEventListener === "function") {
            conn.addEventListener("change", syncSpeculationSwitch);
        }
        document.addEventListener("mouseover", delegatedIntent, { passive: true });
        document.addEventListener("mouseout", function () {
            window.clearTimeout(hoverTimer);
        }, { passive: true });
        document.addEventListener("touchstart", immediateIntent, { passive: true });
        document.addEventListener("focusin", immediateIntent, { passive: true });
        document.addEventListener("click", onLinkClickCapture, true);
        document.addEventListener("click", onLinkClick, false);
        document.addEventListener("submit", onFormSubmit, false);

        window.addEventListener("pageshow", function () {
            // Restored from bfcache: the budget belongs to this page view again,
            // and the progress bar of the navigation that left it is stale.
            budget = MAX_PER_PAGE;
            syncSpeculationSwitch();
            finishNavProgress();
        });
        window.addEventListener("pagehide", cancelAll);

        var warm = function () {
            var p = window.location.pathname;
            // Only the results page warms cars on its own, and only a few of the ones in
            // view. /home and /dashboard never do: each warmed car page counts as a "view"
            // for the shopper, and six of them right after first paint cost the server
            // six renders and reset the home recommendations (2026-11-13).
            if (p === "/listings") {
                observeCarLinks(null, WARM_LIMIT);
            }
        };
        if (typeof requestIdleCallback === "function") {
            requestIdleCallback(warm, { timeout: WARM_IDLE_TIMEOUT_MS });
        } else {
            window.setTimeout(warm, WARM_IDLE_TIMEOUT_MS);
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }

    // Public surface. main.js calls __DS_wirePrefetchLinks and
    // __DS_prefetchVisibleCarLinks after it re-renders the results grid.
    window.__DS_wirePrefetchLinks = function (root) { observeCarLinks(root, WARM_LIMIT); };
    window.__DS_prefetchVisibleCarLinks = function (root, limit) {
        observeCarLinks(root, Math.min(limit || WARM_LIMIT, WARM_LIMIT));
    };
    window.__DS_warmCarDetail = function (carId) {
        if (carId) prefetchDocument("/car/" + carId);
    };
})();
