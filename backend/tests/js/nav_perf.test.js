"use strict";
/**
 * nav_perf.js (2026-11-20): where the browser runs speculation rules, the RULES do every
 * prefetch and nav_perf.js never fetch()es a document (one request per prefetch); the
 * fetch() path is only the fallback, limited to the same side-effect-free documents;
 * Save-Data / 2g switch everything off, including the rules (<html data-ds-nospec>).
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { STATIC_DIR } = require("./_load.js");

const ORIGIN = "https://example.test";

function makeAnchor(href, attrs) {
    const a = {
        tagName: "A",
        href: new URL(href, ORIGIN).href,
        _attrs: Object.assign({ href }, attrs || {}),
        classList: { _s: new Set(), add(c) { this._s.add(c); }, remove(c) { this._s.delete(c); } },
        getAttribute(k) { return Object.prototype.hasOwnProperty.call(this._attrs, k) ? this._attrs[k] : null; },
        setAttribute(k, v) { this._attrs[k] = String(v); },
        hasAttribute(k) { return Object.prototype.hasOwnProperty.call(this._attrs, k); },
        closest(sel) { return sel === "a[href]" ? this : null; },
    };
    return a;
}

/**
 * @param {{spec?: boolean, connection?: object, path?: string, anchors?: object[]}} o
 */
function boot(o) {
    const listeners = {};
    const winListeners = {};
    const fetches = [];
    const timers = [];
    const observed = [];
    const rootAttrs = {};
    const anchors = o.anchors || [];
    const document = {
        readyState: "complete",
        documentElement: {
            setAttribute(k, v) { rootAttrs[k] = v; },
            removeAttribute(k) { delete rootAttrs[k]; },
            hasAttribute(k) { return k in rootAttrs; },
            appendChild() {},
        },
        addEventListener(type, fn, opts) {
            (listeners[type] = listeners[type] || []).push({ fn, capture: opts === true });
        },
        querySelectorAll(sel) {
            if (sel === 'a[href*="/car/"]') return anchors.filter((a) => a.href.includes("/car/"));
            return [];
        },
        createElement() {
            return { style: {}, setAttribute() {}, isConnected: true };
        },
    };
    const sandbox = {
        URL,
        AbortController,
        console,
        Set,
        location: { origin: ORIGIN, pathname: o.path || "/", search: "", href: ORIGIN + (o.path || "/") },
        navigator: { connection: o.connection },
        document,
        HTMLScriptElement: o.spec === undefined ? undefined : { supports: (t) => o.spec && t === "speculationrules" },
        fetch(url, opts) {
            fetches.push({ url, opts });
            return Promise.resolve({ ok: true, text: () => Promise.resolve("") });
        },
        setTimeout(fn) { timers.push(fn); return timers.length; },
        clearTimeout() {},
        requestAnimationFrame(fn) { fn(); },
        requestIdleCallback(fn) { timers.push(fn); },
        IntersectionObserver: function (cb) {
            this.observe = (el) => { observed.push(el); cb([{ isIntersecting: true, target: el }]); };
            this.unobserve = () => {};
        },
        addEventListener(type, fn) { (winListeners[type] = winListeners[type] || []).push(fn); },
    };
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    vm.runInContext(fs.readFileSync(path.join(STATIC_DIR, "nav_perf.js"), "utf8"), sandbox, { filename: "nav_perf.js" });
    const flushTimers = () => { while (timers.length) timers.shift()(); };
    const fire = (type, ev) => (listeners[type] || []).forEach((l) => l.fn(ev));
    const hover = (a) => { fire("mouseover", { target: a }); flushTimers(); };
    return { sandbox, fetches, rootAttrs, hover, fire, flushTimers, observed };
}

test("speculation rules supported: hovering never fetch()es (the rules prefetch it)", () => {
    const env = boot({ spec: true });
    env.hover(makeAnchor("/listings"));
    env.hover(makeAnchor("/car/150?zip_code=28202"));
    env.fire("touchstart", { target: makeAnchor("/car/151") });
    env.fire("focusin", { target: makeAnchor("/find-dealers") });
    assert.equal(env.fetches.length, 0);
    assert.equal("data-ds-nospec" in env.rootAttrs, false);
});

test("no speculation rules: hover fetches the exact href once, with the prefetch header", () => {
    for (const spec of [false, undefined]) {
        const env = boot({ spec });
        const a = makeAnchor("/car/150?zip_code=28202");
        env.hover(a);
        env.hover(a);
        assert.equal(env.fetches.length, 1);
        assert.equal(env.fetches[0].url, "/car/150?zip_code=28202");
        assert.equal(env.fetches[0].opts.headers["X-DS-Prefetch"], "1");
    }
});

test("fallback only prefetches side-effect-free documents", () => {
    const env = boot({ spec: false });
    for (const h of ["/listings?q=bmw", "/api/cars/1", "/static/main.js", "/compare?ids=1,2",
        "/premium", "/home", "/dashboard", "https://other.example/car/1"]) {
        env.hover(makeAnchor(h));
    }
    assert.deepEqual(env.fetches.map((f) => f.url), []);
    env.hover(makeAnchor("/listings"));
    env.hover(makeAnchor("/"));
    // "/" is never speculated: it redirects a signed-in visitor to the per-user /home.
    assert.deepEqual(env.fetches.map((f) => f.url), ["/listings"]);
});

test("save-data and 2g: no fetch and the speculation rules are switched off", () => {
    for (const connection of [{ saveData: true }, { effectiveType: "2g" }, { effectiveType: "slow-2g" }]) {
        for (const spec of [true, false]) {
            const env = boot({ spec, connection });
            assert.equal("data-ds-nospec" in env.rootAttrs, true);
            env.hover(makeAnchor("/car/150"));
            assert.equal(env.fetches.length, 0);
        }
    }
    const fast = boot({ spec: true, connection: { effectiveType: "4g", saveData: false } });
    assert.equal("data-ds-nospec" in fast.rootAttrs, false);
});

test("listings warming: rules mark cards data-ds-warm instead of fetching; fallback fetches", () => {
    const mk = () => [makeAnchor("/car/1"), makeAnchor("/car/2?zip_code=28202"), makeAnchor("/car/3"), makeAnchor("/car/4")];
    const withRules = boot({ spec: true, path: "/listings", anchors: mk() });
    withRules.flushTimers();
    const warmed = withRules.observed.filter((a) => a.hasAttribute("data-ds-warm"));
    assert.equal(warmed.length, 3); // WARM_LIMIT
    assert.equal(withRules.fetches.length, 0);

    const fallback = boot({ spec: false, path: "/listings", anchors: mk() });
    fallback.flushTimers();
    assert.equal(fallback.fetches.length, 3);
    assert.equal(fallback.observed.some((a) => a.hasAttribute("data-ds-warm")), false);
});

test("a committed click never fetch()es the page it is navigating to", () => {
    const env = boot({ spec: false });
    const a = makeAnchor("/car/150");
    const ev = { target: a, button: 0, defaultPrevented: false };
    env.fire("click", ev);
    assert.equal(env.fetches.length, 0);
    assert.equal(a.classList._s.has("is-car-nav-pending"), true);
});
