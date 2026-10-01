"use strict";
/**
 * frontend/static/car/car_tco_math.js -- the TCO math split out of car_page.js.
 *
 * The golden half replays fixtures/car_page_split_golden.json ("tco"): outputs the
 * ORIGINAL car_page.js functions produced (HEAD 75671a38f, evaluated in node) for
 * the tco-intelligence-card attributes of 20 real cars plus synthetic edge cards,
 * across every live fuel/electricity rate those cars saw and invalid rates.
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { loadStatic, plain } = require("./_load.js");

const GOLDEN = JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "fixtures", "car_page_split_golden.json"), "utf8")
).tco;

function cp() {
    const win = loadStatic(["ds_core.js", "car/car_tco_math.js"]);
    win.URLSearchParams = URLSearchParams; // buildTcoFuelLookupUrl; the shared vm stub has URL only
    return win.CP;
}

function card(attrs) {
    return { getAttribute: (k) => (Object.prototype.hasOwnProperty.call(attrs, k) ? attrs[k] : null) };
}

// The recorder serialised non-finite numbers as "__NaN" etc. and undefined as "__undef__".
function J(v) {
    if (v === undefined) return "__undef__";
    return JSON.parse(
        JSON.stringify(plain(v === undefined ? null : v), (k, x) =>
            typeof x === "number" && !Number.isFinite(x) ? "__" + String(x) : x
        )
    );
}
function rateOf(r) {
    return typeof r === "string" && r.startsWith("__") ? Number(r.slice(2)) : r;
}

test("golden: per-card outputs match the original car_page.js", () => {
    const CP = cp();
    assert.equal(GOLDEN.cases.length, 27);
    for (const c of GOLDEN.cases) {
        const el = card(c.attrs);
        const makeKey = CP.normalizeTcoMakeKey(c.attrs["data-car-make"]);
        const avgMpg = CP.resolveTcoAvgMpg(el);
        const kwh = CP.resolveTcoEvEfficiency(el);
        const tank = CP.resolveTcoTankGallons(el);
        const maint = CP.lookupTcoMaintenanceCost(makeKey);
        const o = c.out;
        const where = c.source;
        assert.equal(makeKey, o.makeKey, where);
        assert.equal(CP.isTcoElectricVehicle(el), o.isEv, where);
        assert.deepEqual(J(avgMpg), o.avgMpg, where);
        assert.deepEqual(J(kwh), o.kwh, where);
        assert.deepEqual(J(tank), o.tank, where);
        assert.equal(maint, o.maint, where);
        assert.equal(CP.formatTcoCurrency(maint), o.maintTxt, where);
        assert.equal(CP.resolveTcoCarState(el), o.state, where);
        assert.equal(
            CP.buildTcoFuelLookupUrl((c.attrs["data-fuel-tier"] || "regular").trim().toLowerCase(), CP.resolveTcoCarState(el)),
            o.url,
            where
        );
        assert.equal(CP.formatTcoFuelTierLabel(c.attrs["data-fuel-tier"]), o.tier, where);
        assert.deepEqual(J(CP.buildTcoMileageSteps(parseInt(c.attrs["data-car-mileage"] || "0", 10) || 0, 12000)), o.steps, where);
        for (const p of o.per) {
            const r = rateOf(p.rate);
            const at = where + " @ " + p.rate;
            assert.deepEqual(J(CP.computeTcoFuelExpense(r, avgMpg)), p.fuel, at);
            assert.deepEqual(J(CP.computeTcoEvFuelExpense(r, kwh)), p.ev, at);
            assert.deepEqual(J(CP.computeTcoAnnualFuelSpend(r, avgMpg)), p.annualFuel, at);
            assert.deepEqual(J(CP.computeTcoAnnualEvFuelSpend(r, kwh)), p.annualEv, at);
            assert.deepEqual(J(CP.computeTcoFillUpCost(r, tank)), p.fill, at);
            assert.deepEqual(J(CP.buildTcoCumulativeOperationalCosts(maint, CP.computeTcoAnnualFuelSpend(r, avgMpg))), p.cum, at);
            assert.deepEqual(J(CP.buildTcoCumulativeOperationalCosts(maint, CP.computeTcoAnnualEvFuelSpend(r, kwh))), p.cumEv, at);
        }
    }
});

test("golden: per-rate scenarios and labels match the original", () => {
    const CP = cp();
    for (const p of GOLDEN.perRate) {
        const r = rateOf(p.rate);
        assert.deepEqual(J(CP.buildTcoFuelPriceScenarios(r, false)), p.scen, String(p.rate));
        assert.deepEqual(J(CP.buildTcoFuelPriceScenarios(r, true)), p.scenEv, String(p.rate));
        assert.equal(CP.formatTcoLiveFuelLabel("Ohio", "premium", r), p.liveFuel);
        assert.equal(CP.formatTcoLiveElectricityLabel("", r), p.liveElec);
        assert.equal(CP.formatTcoCustomFuelLabel(r), p.custFuel);
        assert.equal(CP.formatTcoCustomElectricityLabel(r), p.custElec);
        assert.equal(CP.formatTcoFillUpCurrency(r * 13.7), p.fillTxt);
    }
});

test("golden: scalar helpers match the original", () => {
    const CP = cp();
    const s = GOLDEN.scalars;
    assert.equal(CP.isTcoElectricVehicle(null), s.isEvNull);
    assert.deepEqual(["mid", "midgrade", "mid-grade", "diesel", "premium", "PREMIUM ", "", null, "e85"].map(CP.formatTcoFuelTierLabel), s.tiers);
    assert.deepEqual(["3.459", " 4 ", "0", "-1", "20", "20.01", "abc", "", null].map((x) => J(CP.parseTcoFuelPriceInput(x))), s.parseFuel);
    assert.deepEqual(["0.176", "2", "2.01", "0", "x", ""].map((x) => J(CP.parseTcoElectricityPriceInput(x))), s.parseElec);
    assert.deepEqual([NaN, 0, 999, 1000, 1049, 9950, 12000, 99999, 100000, 154321].map(CP.formatTcoMileageLabel), s.mileLabels);
    assert.deepEqual([0, 999, 1000, 1550, 12345, 99999, 123456, NaN].map(CP.formatTcoCostCurrencyShort), s.short);
    assert.deepEqual([0, 1814.4, 4993, -12, NaN, null].map(CP.formatTcoCurrency), s.cur);
    assert.deepEqual(
        ["Land Rover", "land-rover", "MERCEDES-BENZ", "mercedes", "Porsche", "gmc", "Lucid", null].map((m) =>
            CP.lookupTcoMaintenanceCost(CP.normalizeTcoMakeKey(m))
        ),
        s.makes
    );
});

test("TCO constants and a worked example", () => {
    const CP = cp();
    assert.equal(CP.TCO.TCO_TOTAL_MILES, 60000);
    assert.equal(CP.TCO.TCO_FORECAST_YEARS, 5);
    assert.ok(Object.isFrozen(CP.TCO));
    // 60,000 mi at 25 mpg and $3.50/gal
    assert.equal(CP.computeTcoFuelExpense(3.5, 25), 8400);
    assert.equal(CP.computeTcoAnnualFuelSpend(3.5, 25), 1680);
    // Toyota maintenance 1814 -> 363/yr; + 1680 fuel = 2043/yr
    assert.deepEqual(plain(CP.buildTcoCumulativeOperationalCosts(1814, 1680)), [0, 2043, 4086, 6129, 8172, 10215]);
    // EV: 600 x 30 kWh/100mi x $0.15
    assert.equal(CP.computeTcoEvFuelExpense(0.15, 30), 2700);
    assert.equal(CP.resolveTcoEvEfficiency(card({ "data-car-mpg-city": "120", "data-car-mpg-highway": "100" })), 30.6);
    assert.equal(CP.computeTcoFillUpCost(3.459, 15.9), 55);
    assert.equal(CP.buildTcoFuelLookupUrl("premium", "TX"), "/api/fuel/lookup?fuel_tier=premium&state=TX");
    assert.equal(CP.buildTcoFuelPriceScenarios(0, false), null);
    assert.equal(plain(CP.buildTcoFuelPriceScenarios(1.2, false))[0].rate, 0.5);
});
