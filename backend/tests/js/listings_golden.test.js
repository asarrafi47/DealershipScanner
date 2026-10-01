"use strict";
/**
 * Golden for the main.js -> static/listings/*.js split (2026-10-01 audit F1).
 *
 * backend/tests/fixtures/listings_main_js_golden.json was recorded BEFORE the
 * split: the original main.js closure functions (exported by an instrumented
 * copy of the file) were run in Chromium on the /listings page against 200 real
 * cars from GET /api/listings/cars?zip=37421&radius=50 (local DB, read-only)
 * plus 10 synthetic edge rows. Page-only inputs were recorded alongside the
 * outputs: each car's distance from the ZIP (carDistanceMiles) and dealership
 * registry id (geo.js), so the pure DSL functions can be fed exactly what the
 * closure saw. Every DSL output must equal the recorded closure output.
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { loadStatic, plain } = require("./_load.js");

// JSON has no NaN; the one NaN input (a maxPrice of NaN) is stored as "__NaN__".
const GOLD = JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "fixtures", "listings_main_js_golden.json"), "utf8"),
    (_k, v) => (v === "__NaN__" ? NaN : v)
);

const LISTINGS_FILES = [
    "listings/card.js",
    "listings/filters.js",
    "listings/sort.js",
    "listings/facets.js",
    "listings/zip_state.js",
    "listings/chips.js",
];

function load() {
    const w = loadStatic(["ds_core.js", "sc-helpers.js", ...LISTINGS_FILES], { origin: GOLD.origin });
    // geo.js (DOM/coords state) is not loaded here: replay the registry ids the page resolved.
    w.SC.carDealershipRegistryId = (car) => GOLD.regId[String(car.id)];
    // cardMileageNotListed compares against the current year; pin it to the recording date.
    vm.runInContext(
        `(function () {
            const R = Date;
            const FIXED = R.UTC(2026, 9, 1, 18, 0, 0);
            function D(...a) { if (!new.target) return R(); return a.length ? new R(...a) : new R(FIXED); }
            D.prototype = R.prototype; D.now = () => FIXED; D.UTC = R.UTC; D.parse = R.parse;
            Date = D;
        })();`,
        w
    );
    w.VSet = vm.runInContext("Set", w);
    return w;
}

const cars = () => JSON.parse(JSON.stringify(GOLD.cars));
const dist = (c) => GOLD.dist[String(c.id)];

test("fixture holds 200 real cars plus edge rows", () => {
    assert.ok(GOLD.cars.length >= 200);
    assert.equal(GOLD.cardsGuest.length, GOLD.cars.length);
});

test("renderCardHtml matches the pre-split card markup (guest and signed in)", () => {
    const w = load();
    const saved = new w.VSet(GOLD.savedIds);
    for (const [loggedIn, expected] of [[false, GOLD.cardsGuest], [true, GOLD.cardsUser]]) {
        cars().forEach((c, i) => {
            const html = w.DSL.renderCardHtml(c, saved, GOLD.compareIds, {
                loggedIn,
                zipForUrl: GOLD.zip,
                distanceMiles: dist(c),
            });
            assert.equal(html, expected[i], `card ${c.id} loggedIn=${loggedIn}`);
        });
    }
});

test("per-car predicates match (condition, mileage, inventory filter, photos, paint)", () => {
    const w = load();
    const D = w.DSL;
    cars().forEach((c, i) => {
        const got = {
            cond: D.cardConditionToken(c),
            notListed: D.cardMileageNotListed(c),
            listingCond: D.carListingCondition(c),
            inv: ["", "new", "pre_owned", "cpo", "other"].map((k) => D.passesInventoryConditionFilter(c, k)),
            call: D.listingCallForPrice(c),
            photos: D.listingPhotoCount(c),
            realImg: D.listingHasRealImage(c),
            paintExt: D.carMatchesPaintFamilyBuckets(c, "exterior_color", ["black", "white"]),
            paintInt: D.carMatchesPaintFamilyBuckets(c, "interior_color", ["black"]),
        };
        assert.deepEqual(plain(got), GOLD.perCar[i], `car ${c.id}`);
    });
});

test("listingDepriorityCompare and sortListingsCars match every sort mode", () => {
    const w = load();
    const rows = cars();
    const dep = [];
    for (let i = 0; i + 1 < rows.length; i++) dep.push(w.DSL.listingDepriorityCompare(rows[i], rows[i + 1]));
    assert.deepEqual(dep, GOLD.depriority);
    for (const [key, expected] of Object.entries(GOLD.sorts)) {
        const [mode, po] = key.split("|");
        const got = w.DSL.sortListingsCars(cars(), mode, po === "true", dist).map((c) => c.id);
        assert.deepEqual(plain(got), expected, key);
    }
});

test("carMatchesFacetFilters / hidden dealers / dealer subset match", () => {
    const w = load();
    for (const fc of GOLD.facetCases) {
        const hidden = new w.VSet(fc.hidden);
        const set = fc.dealers ? new w.VSet(fc.dealers) : null;
        const ids = cars().filter((c) => w.DSL.carMatchesFacetFilters(c, fc.state, set, hidden)).map((c) => c.id);
        assert.deepEqual(plain(ids), fc.ids, JSON.stringify(fc.state));
        const hiddenIds = cars().filter((c) => w.DSL.carIsFromHiddenDealer(c, hidden)).map((c) => c.id);
        assert.deepEqual(plain(hiddenIds), fc.hiddenIds);
        assert.deepEqual(plain(w.DSL.withoutHiddenDealers(cars(), hidden).map((c) => c.id)), fc.without);
    }
});

test("carMatchesSmartFilters matches every recorded smart-filter payload", () => {
    const w = load();
    for (const sc of GOLD.smartCases) {
        const ids = cars().filter((c) => w.DSL.carMatchesSmartFilters(c, sc.filters)).map((c) => c.id);
        assert.deepEqual(plain(ids), sc.ids, JSON.stringify(sc.filters));
    }
});

test("chip labels, car-row table, facet keys, radius clamp, cars URL, css url match", () => {
    const w = load();
    const D = w.DSL;
    for (const [n, v, label] of GOLD.chips) assert.equal(D.scalarChipLabel(n, v), label, `${n}=${v}`);
    assert.deepEqual(plain(D.buildCarRowsFromCars(cars())), GOLD.carRows);
    for (const [p, e, key] of GOLD.lazyKeys) assert.equal(D.lazyFacetEntryKey(p, e), key);
    const facetsPayload = { model_rows: [["a", "b"]], trim_rows: [["a", "b", "c"]], all_package_names: ["p"] };
    for (const [p, full, emptyRes] of GOLD.lazyEntries) {
        assert.deepEqual(plain(D.lazyFacetEntries(p, facetsPayload)), full);
        assert.deepEqual(plain(D.lazyFacetEntries(p, {})), emptyRes);
    }
    GOLD.carRows.slice(0, 40).forEach((r, i) => {
        assert.deepEqual(["make", "model", "trim", "fuel_type"].map((p) => D.cascadeRowKey(p, r)), GOLD.cascadeKeys[i]);
    });
    for (const [v, r] of GOLD.radius) assert.equal(D.clampListingsRadius(v), r, String(v));
    for (const [s, url] of GOLD.carsUrl) assert.equal(D.listingsCarsUrl(s), url, String(s));
    for (const [u, out] of GOLD.cssUrl) assert.equal(D.cssSingleQuotedUrl(u, GOLD.origin), out, u);
});
