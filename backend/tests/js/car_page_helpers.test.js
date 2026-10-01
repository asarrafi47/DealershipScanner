"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic, plain } = require("./_load.js");

function cp() {
    return loadStatic(["ds_core.js", "car_page_helpers.js"]).CP;
}

function card(attrs) {
    return { getAttribute: (k) => (Object.prototype.hasOwnProperty.call(attrs, k) ? attrs[k] : null) };
}

test("CP.parseDepreciationMetadata defaults and parsing", () => {
    const CP = cp();
    assert.deepEqual(plain(CP.parseDepreciationMetadata(card({}))), {
        price: 20000, year: 2020, mileage: 0, trim: "", genStart: 2020, genEnd: 2027,
    });
    assert.deepEqual(
        plain(CP.parseDepreciationMetadata(card({
            "data-car-price": "35990", "data-car-year": "2022", "data-car-mileage": "18000",
            "data-car-trim": "  Limited ", "data-gen-start": "2019", "data-gen-end": "2024",
        }))),
        { price: 35990, year: 2022, mileage: 18000, trim: "limited", genStart: 2019, genEnd: 2024 },
    );
});

test("CP mileage helpers", () => {
    const CP = cp();
    assert.equal(CP.computeAnnualMileage({ year: 2022, mileage: 40000 }), 10000);
    assert.equal(CP.computeAnnualMileage({ year: 2025, mileage: 100 }), 12000);
    assert.deepEqual(plain(CP.buildDepreciationMileageSteps(10000, 12000)), [10000, 22000, 34000, 46000, 58000, 70000]);
    assert.equal(CP.formatDepreciationMileage(NaN), "--");
    assert.equal(CP.formatDepreciationMileage(950), "950 mi");
    assert.equal(CP.formatDepreciationMileage(1250), "1.3k mi");
    assert.equal(CP.formatDepreciationMileage(45000), "45k mi");
    assert.equal(CP.snapDepreciationAnnualMileage(12400), 10000);
    assert.equal(CP.snapDepreciationAnnualMileage(1000), 5000);
    assert.equal(CP.snapDepreciationAnnualMileage(90000), 40000);
    assert.deepEqual(plain(CP.buildDepreciationAnnualMileageOptions()), [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]);
    assert.equal(CP.formatDepreciationAnnualMileageOption(15000), "15k mi/yr");
});

test("CP residual and trajectory", () => {
    const CP = cp();
    const base = { year: 2022, genStart: 2020, trim: "" };
    assert.equal(CP.computeDepreciationTargetResidual(base, 12000), 0.5);
    assert.equal(CP.computeDepreciationTargetResidual({ ...base, trim: "lx" }), 0.53);
    assert.equal(CP.computeDepreciationTargetResidual({ ...base, trim: "denali" }, 35000), 0.3);
    assert.ok(Math.abs(CP.computeDepreciationTargetResidual(base, 5000) - 0.58) < 1e-9);
    assert.deepEqual(plain(CP.buildDepreciationTrajectory(10000, 1)), [10000, 10000, 10000, 10000, 10000, 10000]);
    const t = plain(CP.buildDepreciationTrajectory(40000, 0.5));
    assert.equal(t[0], 40000);
    assert.equal(t[5], 20000);
});

test("CP currency formatters go through DS", () => {
    const CP = cp();
    assert.equal(CP.formatDepreciationCurrency(NaN), "$--");
    assert.equal(CP.formatDepreciationCurrency("100"), "$--");
    assert.equal(CP.formatDepreciationCurrency(28449.5), "$28,450");
    assert.equal(CP.formatDepreciationCurrencyShort(999), "$999");
    assert.equal(CP.formatDepreciationCurrencyShort(5432), "$5.4k");
    assert.equal(CP.formatDepreciationCurrencyShort(28449), "$28k");
    assert.equal(CP.formatDepreciationCurrencyShort(250000), "$250k");
    assert.equal(CP.formatDepreciationCurrencyShort(Infinity), "$--");
});

test("CP misc", () => {
    const CP = cp();
    assert.equal(CP.evBatteryThermalScaleFactor("Nissan"), 1.4);
    assert.equal(CP.evBatteryThermalScaleFactor(" TESLA "), 0.85);
    assert.equal(CP.evBatteryThermalScaleFactor(null), 1.0);
    assert.equal(CP.hasPremiumHistoryAccess(null), false);
    assert.equal(CP.hasPremiumHistoryAccess({ has_paid_access: true }), true);
    assert.equal(CP.hasPremiumHistoryAccess({ logged_in: true, billing_stripe_enabled: false }), true);
    assert.equal(CP.hasPremiumHistoryAccess({ logged_in: true, billing_stripe_enabled: true }), false);
    assert.equal(CP.trimMatchesKeyword("", ["x"]), false);
    assert.equal(CP.trimMatchesKeyword("sr5 premium", ["sr5"]), true);
    assert.equal(CP.escHtml(`a"b<c>&'`), "a&quot;b&lt;c&gt;&amp;'");
});
