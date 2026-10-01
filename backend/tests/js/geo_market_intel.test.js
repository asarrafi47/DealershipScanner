"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic, plain } = require("./_load.js");

function win() {
    return loadStatic(["ds_core.js", "sc-helpers.js", "market_intel.js", "geo.js"]);
}

test("haversineJS: known distances", () => {
    const w = win();
    assert.equal(w.haversineJS(35, -85, 35, -85), 0);
    // one degree of latitude is ~69.09 mi
    assert.ok(Math.abs(w.haversineJS(35, -85, 36, -85) - 69.09) < 0.1);
});

test("zipCoordsJS / dealerCoordsJS lookups", () => {
    const w = win();
    assert.equal(w.zipCoordsJS("37421"), null);
    w.ZIP_COORDS = { "37421": [35.03, -85.15], bad: [1] };
    assert.deepEqual(plain(w.zipCoordsJS(" 37421 ")), [35.03, -85.15]);
    assert.equal(w.zipCoordsJS("bad"), null);
    w.DEALER_COORDS = { "https://a.example/": [1, 2], "host:b.example": [3, 4] };
    assert.deepEqual(plain(w.dealerCoordsJS("https://a.example/")), [1, 2]);
    assert.deepEqual(plain(w.dealerCoordsJS("https://www.b.example/inventory")), [3, 4]);
    assert.equal(w.dealerCoordsJS("not a url"), null);
});

test("SC.dealerHostKey and carDealershipRegistryId", () => {
    const w = win();
    assert.equal(w.SC.dealerHostKey("https://WWW.Foo.com/x"), "foo.com");
    assert.equal(w.SC.dealerHostKey("nope"), "");
    assert.equal(w.carDealershipRegistryId({ dealership_registry_id: "17" }), 17);
    assert.equal(w.carDealershipRegistryId({ dealer_url: "https://foo.com" }), 0);
    w.REGISTRY_ID_BY_DEALER_HOST = { "foo.com": "9" };
    assert.equal(w.carDealershipRegistryId({ dealer_url: "https://www.foo.com/" }), 9);
});

test("SC.filterCarsInRadius keeps cars inside the radius", () => {
    const w = win();
    w.REGISTRY_COORDS = { "1": [35.0, -85.0], "2": [35.5, -85.0], "3": [37.0, -85.0] };
    const cars = [
        { id: "a", dealership_registry_id: 1 },
        { id: "b", dealership_registry_id: 2 },
        { id: "c", dealership_registry_id: 3 },
        { id: "d" },
    ];
    w.SC.invalidateCarGeoIndex();
    assert.deepEqual(plain(w.SC.filterCarsInRadius(cars, [35.0, -85.0], 50)).map((c) => c.id), ["a", "b"]);
    assert.deepEqual(plain(w.SC.filterCarsInRadius(cars, [35.0, -85.0], 10)).map((c) => c.id), ["a"]);
    assert.deepEqual(plain(w.SC.filterCarsInRadius(cars, null, 50)), []);
});

test("SC.mergeListingsGeoCoordsPayload merges maps and flags readiness", () => {
    const w = win();
    assert.equal(w.SC.listingsDealerCoordsReady(), false);
    w.SC.mergeListingsGeoCoordsPayload({ ok: true, dealer_coords: { "host:x.com": [1, 2] }, registry_id_by_host: { "x.com": 4 } });
    assert.equal(w.SC.listingsDealerCoordsReady(), true);
    assert.equal(w.REGISTRY_ID_BY_DEALER_HOST["x.com"], 4);
    assert.equal(w.__DS_listingsGeoCoordsReady, true);
});

test("SC.marketIntelForCar picks the narrowest cohort and labels the delta", () => {
    const w = win();
    assert.equal(w.SC.marketIntelForCar({ make: "Honda", model: "CR-V", price: 30000 }), null);
    w.__DS_MARKET_STATS = {
        min_samples: 3,
        year_window: 1,
        cohorts: {
            "honda|cr-v|ex|2022|25-50k": { sample_count: 4, avg_price: 500 },
            "honda|cr-v|ex|2021|25-50k": { sample_count: 10, avg_price: 900 },
        },
    };
    const below = w.SC.marketIntelForCar({ make: "Honda", model: "CR-V", trim: "EX", year: 2022, mileage: 30000, price: 450 });
    assert.equal(below.delta_pct, -10);
    assert.equal(below.vs_market, "below_market");
    assert.equal(below.sample_count, 4);
    const near = w.SC.marketIntelForCar({ make: "Honda", model: "CR-V", trim: "EX", year: 2022, mileage: 30000, price: 505 });
    assert.equal(near.vs_market, "near_market");
    assert.equal(near.avg_price_display, "$500");
    // A payment-shaped price (<= $3k and <= 10% of average) gets no delta.
    w.__DS_MARKET_STATS.cohorts["honda|cr-v|ex|2022|25-50k"].avg_price = 39000;
    const pay = w.SC.marketIntelForCar({ make: "Honda", model: "CR-V", trim: "EX", year: 2022, mileage: 30000, price: 800 });
    assert.equal(pay.vs_market, "payment_listed");
    assert.equal(pay.delta_pct, null);
    const car = { make: "Honda", model: "CR-V", trim: "EX", year: 2022, mileage: 30000, price: 39000 };
    w.SC.enrichCarWithMarket(car);
    assert.equal(car.market.vs_market, "near_market");
});
