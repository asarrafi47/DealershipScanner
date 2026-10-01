"use strict";
/**
 * Unit tests for the static/listings/*.js helpers that main.js used to inline
 * (the golden in listings_golden.test.js covers the functions that existed as
 * named closure functions before the split; these cover the expressions that
 * were lifted out of larger main.js functions).
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic, plain } = require("./_load.js");

function dsl() {
    const w = loadStatic([
        "ds_core.js",
        "sc-helpers.js",
        "listings/card.js",
        "listings/filters.js",
        "listings/sort.js",
        "listings/facets.js",
        "listings/zip_state.js",
        "listings/chips.js",
    ]);
    // pageFromSearch uses URLSearchParams, which the _load.js sandbox does not provide.
    w.URLSearchParams = URLSearchParams;
    return w.DSL;
}

test("every listings module attaches to one window.DSL", () => {
    const D = dsl();
    for (const name of [
        "renderCardHtml", "cardConditionToken", "carMatchesFacetFilters", "carMatchesSmartFilters",
        "sortListingsCars", "filterCompatibleRows", "buildCarRowsFromCars", "listingsCarsScopeKey",
        "chipsHtml", "pageNumbersHtml",
    ]) {
        assert.equal(typeof D[name], "function", name);
    }
});

test("filterCompatibleRows: empty selections keep everything; lists narrow (make/model/body case-insensitive)", () => {
    const D = dsl();
    const rows = [
        { make: "Ford", model: "F-150", trim: "XLT", fuel: "Gasoline", drive: "4WD", induction: null, body_style: "Truck", cyl: 6 },
        { make: "Tesla", model: "Model 3", trim: "Long Range", fuel: "Electric", drive: "AWD", induction: null, body_style: "Sedan", cyl: 0 },
        { make: "Ford", model: "Mustang", trim: "GT", fuel: "Gasoline", drive: "RWD", induction: null, body_style: "Coupe", cyl: 8 },
    ];
    const none = { makes: [], models: [], trims: [], fuels: [], drives: [], inductions: [], bodies: [], cyls: [] };
    assert.equal(D.filterCompatibleRows(rows, none).length, 3);
    const ids = (sel) => plain(D.filterCompatibleRows(rows, Object.assign({}, none, sel)).map((r) => r.model));
    assert.deepEqual(ids({ makes: ["ford"] }), ["F-150", "Mustang"]);
    assert.deepEqual(ids({ bodies: ["SEDAN"] }), ["Model 3"]);
    assert.deepEqual(ids({ cyls: ["0"] }), ["Model 3"]);
    assert.deepEqual(ids({ fuels: ["gasoline"] }), []); // fuel/drive/induction compare exactly
    assert.deepEqual(ids({ drives: ["RWD"], makes: ["Ford"] }), ["Mustang"]);
});

test("visiblePackageNames: all names without a selection, else make AND model", () => {
    const D = dsl();
    const pk = [
        { make: "Ford", model: "F-150", name: "Tow Pkg" },
        { make: "Ford", model: "Mustang", name: "Track Pack" },
        { make: "Tesla", model: "Model 3", name: "FSD" },
    ];
    assert.deepEqual([...D.visiblePackageNames(pk, [], [])].sort(), ["fsd", "tow pkg", "track pack"]);
    assert.deepEqual([...D.visiblePackageNames(pk, ["ford"], [])].sort(), ["tow pkg", "track pack"]);
    assert.deepEqual([...D.visiblePackageNames(pk, ["ford"], ["mustang"])], ["track pack"]);
    assert.deepEqual([...D.visiblePackageNames(pk, [], ["model 3"])], ["fsd"]);
});

test("cascadeOptionKey reads data-make/data-model from the label", () => {
    const D = dsl();
    const label = { dataset: { make: "Ford", model: "F-150" } };
    assert.equal(D.cascadeOptionKey("trim", label, { value: "XLT" }), D.cascadeRowKey("trim", { make: "Ford", model: "F-150", trim: "XLT" }));
    assert.equal(D.cascadeOptionKey("model", label, { value: "F-150" }), D.cascadeRowKey("model", { make: "Ford", model: "F-150" }));
    assert.equal(D.cascadeOptionKey("fuel_type", null, { value: " Gasoline " }), D.lazyFacetEntryKey("package", " Gasoline "));
});

test("facetMakesFilter: countries supply or narrow the make list", () => {
    const D = dsl();
    const c2m = { Japan: ["Toyota", "Honda"], USA: ["Ford"] };
    assert.deepEqual(plain(D.facetMakesFilter(["Ford"], [], c2m)), ["Ford"]);
    assert.deepEqual(plain(D.facetMakesFilter([], ["Japan"], c2m)), ["Toyota", "Honda"]);
    assert.deepEqual(plain(D.facetMakesFilter(["toyota", "Ford"], ["Japan"], c2m)), ["toyota"]);
    assert.deepEqual(plain(D.facetMakesFilter(["Ford"], ["Japan"], undefined)), ["Ford"]);
    assert.deepEqual(plain(D.facetMakesFilter([], ["Mars"], c2m)), []);
});

test("smartFilterOptionMatches: exact, word/dash prefix, and model stem", () => {
    const D = dsl();
    assert.equal(D.smartFilterOptionMatches("make", "ford", " Ford "), true);
    assert.equal(D.smartFilterOptionMatches("model", "f-150", "F-150 Lightning"), true);
    assert.equal(D.smartFilterOptionMatches("trim", "sport", "Sport-Touring"), true);
    assert.equal(D.smartFilterOptionMatches("trim", "sport", "Sportback"), false);
    assert.equal(D.smartFilterOptionMatches("model", "rav", "RAV4"), true);
    assert.equal(D.smartFilterOptionMatches("model", "r", "RAV4"), false);
});

test("scalarSelectValue: exact option, non-numeric, smallest covering bracket, Any above the top", () => {
    const D = dsl();
    const opts = ["", "20000", "30000", "50000"];
    assert.equal(D.scalarSelectValue(opts, 30000), "30000");
    assert.equal(D.scalarSelectValue(opts, 27500), "30000");
    assert.equal(D.scalarSelectValue(opts, 90000), "");
    assert.equal(D.scalarSelectValue(["", "new", "pre_owned"], "cpo"), "cpo");
    // Number("") is 0, so the "Any" option covers a cap of 0 (unchanged from main.js).
    assert.equal(D.scalarSelectValue(opts, 0), "");
});

test("ZIP scope keys, radius filter key, digits, car links", () => {
    const D = dsl();
    assert.equal(D.listingsCarsScopeKey(" 37421 ", "999"), "37421|250");
    assert.equal(D.listingsCarsScopeKey("37421", ""), "37421|50");
    assert.equal(D.listingsCarsScopeKey("3742", "50"), "");
    assert.equal(D.listingsRadiusFilterKey("37421", "25"), "37421|25");
    assert.equal(D.listingsRadiusFilterKey("37421", ""), "");
    assert.equal(D.listingsRadiusFilterKey("abcde", "25"), "");
    assert.equal(D.zipDigits("37-421 9"), "37421");
    assert.equal(D.zipDigits(null), "");
    assert.equal(D.carHrefWithZip("/car/123?zip_code=11111", "37421"), "/car/123?zip_code=37421");
    assert.equal(D.carHrefWithZip("/dealership/x", "37421"), null);
    const err = D.listingsCarsError("zip_not_found");
    assert.equal(err.code, "zip_not_found");
    assert.equal(err.message, "zip_not_found");
    assert.equal(D.listingsCarsError().code, "fetch_failed");
});

test("partial refetch backoff, page and per-page parsing", () => {
    const D = dsl();
    assert.deepEqual([0, 1, 2, 3, 4, 5].map(D.partialRefetchDelayMs), [3000, 6000, 12000, 24000, 30000, 30000]);
    assert.equal(D.pageFromSearch("?page=3&x=1"), 3);
    assert.equal(D.pageFromSearch("?page=0"), 1);
    assert.equal(D.pageFromSearch(""), 1);
    assert.equal(D.normalizePerPage(48, 24), 48);
    assert.equal(D.normalizePerPage(NaN, 24), 24);
    assert.equal(D.pickPerPage(12, 48, 24), 12);
    assert.equal(D.pickPerPage(NaN, 48, 24), 48);
    assert.equal(D.pickPerPage(7, 9, 24), 24);
});

test("chip markup escapes and the search chip is cut at 28 characters", () => {
    const D = dsl();
    assert.equal(
        D.chipsHtml([{ param: "make", value: 'A"B', label: "<Ford>" }]),
        '<button type="button" class="listings-filter-chip" data-chip-param="make" data-chip-value="A&quot;B">'
            + '<span class="listings-filter-chip-label">&lt;Ford&gt;</span>'
            + '<span class="listings-filter-chip-x" aria-hidden="true">×</span></button>'
    );
    assert.equal(D.chipsHtml([]), "");
    assert.equal(D.searchChipLabel("short"), "short");
    assert.equal(D.searchChipLabel("x".repeat(30)), "x".repeat(28) + "…");
});

test("pager labels and page buttons", () => {
    const D = dsl();
    assert.deepEqual(plain(D.paginationLabels(0, 1, 24)), { count: "0 vehicles", range: "" });
    assert.deepEqual(plain(D.paginationLabels(1, 1, 24)), { count: "1 vehicle", range: "" });
    assert.deepEqual(plain(D.paginationLabels(20, 1, 24)), { count: "20 vehicles", range: "" });
    assert.deepEqual(plain(D.paginationLabels(200, 2, 48)), { count: "49–96 of 200", range: "Page 2 of 5 · 48 per page" });
    const html = D.pageNumbersHtml(2, 3);
    assert.match(html, /data-page="2" aria-label="Page 2" aria-current="page">2<\/button>/);
    assert.match(html, /class="listings-page-num listings-page-num--active"/);
    assert.equal((html.match(/<button/g) || []).length, 3);
});

test("zero-results geo message lists up to four makes", () => {
    const D = dsl();
    assert.equal(D.zeroResultsGeoMessage(50, "37421", []), "No listings within 50 mi of 37421 yet.");
    assert.equal(
        D.zeroResultsGeoMessage(25, "37421", ["A", "B", "C", "D", "E"]),
        "No A, B, C, D… within 25 mi of 37421. Try a larger radius or different makes."
    );
});
