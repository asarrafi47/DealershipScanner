"use strict";
/**
 * frontend/static/car/car_finance.js -- the payment math split out of car_page.js's
 * initCarFinanceCalculator.
 *
 * The golden half replays fixtures/car_page_split_golden.json ("finance"): the
 * monthly-payment text the ORIGINAL calculator showed in Chromium on 20 real car
 * pages for a grid of down payment x term x credit tier, trade-ins, tax/fees and
 * a ZIP tax lookup. Each must come out of CP.computeFinanceMonthlyPayment unchanged.
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { loadStatic } = require("./_load.js");

const GOLDEN = JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "fixtures", "car_page_split_golden.json"), "utf8")
).finance;

function cp() {
    return loadStatic(["ds_core.js", "car/car_finance.js"]).CP;
}

function paymentText(CP, car, f) {
    const max = Math.floor(car.vehicle_price);
    const payment = CP.computeFinanceMonthlyPayment({
        vehiclePrice: car.vehicle_price,
        downPayment: CP.parseFinanceAmount(f.down, max),
        tier: f.tier,
        term: f.term,
        tradeInValue: CP.computeFinanceTradeInEquity({
            hasTradeIn: !!f.hasTradeIn,
            tradeValue: f.tradeValue,
            maxTradeValue: max,
            highDemand: !!f.highDemand,
            condition: f.condition,
        }),
        taxRatePct: f.tax,
        fees: f.fees,
    });
    return CP.financePaymentText(payment);
}

test("golden: every payment the original calculator showed on 20 real cars", () => {
    const CP = cp();
    assert.equal(GOLDEN.length, 20);
    let checked = 0;
    for (const car of GOLDEN) {
        const tax0 = car.default_tax_rate;
        const fees0 = car.default_fees;
        let last = null;
        for (const rec of car.recorded) {
            let f;
            let want;
            if (rec[0] === "trade") {
                f = { down: "3000", term: "60", tier: "good", tax: tax0, fees: fees0, hasTradeIn: true,
                      tradeValue: rec[1], condition: rec[2], highDemand: rec[3] };
                want = rec[4];
            } else if (rec[0] === "trade-off") {
                f = Object.assign({}, last, { hasTradeIn: false });
                want = rec[1];
            } else if (rec[0] === "taxfees") {
                f = Object.assign({}, last, { tax: rec[1], fees: rec[2] });
                want = rec[3];
            } else if (rec[0] === "zip") {
                f = Object.assign({}, last, { tax: rec[2] });
                want = rec[1];
            } else {
                f = { down: rec[0], term: rec[1], tier: rec[2], tax: tax0, fees: fees0, hasTradeIn: false };
                want = rec[3][0];
            }
            assert.equal(paymentText(CP, car, f), want, "car " + car.car_id + " " + JSON.stringify(rec));
            last = f;
            checked += 1;
        }
    }
    assert.equal(checked, 20 * 68);
});

test("finance input parsers", () => {
    const CP = cp();
    assert.equal(CP.parseFinanceAmount("", 30000), 0);
    assert.equal(CP.parseFinanceAmount(null, 30000), 0);
    assert.equal(CP.parseFinanceAmount(" 2500.5 ", 30000), 2500.5);
    assert.equal(CP.parseFinanceAmount("-1", 30000), 0);
    assert.equal(CP.parseFinanceAmount("abc", 30000), 0);
    assert.equal(CP.parseFinanceAmount("999999", 30000), 30000);
    assert.equal(CP.parseFinanceTerm("60"), 60);
    assert.equal(CP.parseFinanceTerm("0"), null);
    assert.equal(CP.parseFinanceTerm(""), null);
    assert.equal(CP.parseFinanceTaxRatePct(""), 7);
    assert.equal(CP.parseFinanceTaxRatePct("x"), 7);
    assert.equal(CP.parseFinanceTaxRatePct("40"), 25);
    assert.equal(CP.parseFinanceFees(""), 500);
    assert.equal(CP.parseFinanceFees("-3"), 500);
    assert.equal(CP.parseFinanceFees("20000"), 10000);
    assert.equal(CP.financeConditionMultiplier("rough"), 0.6);
    assert.equal(CP.financeConditionMultiplier("bogus"), 0.85);
});

test("trade-in equity and payment worked examples", () => {
    const CP = cp();
    const noTrade = { hasTradeIn: false, tradeValue: "9000", maxTradeValue: 30000, highDemand: true, condition: "clean" };
    assert.equal(CP.computeFinanceTradeInEquity(noTrade), 0);
    assert.equal(CP.computeFinanceTradeInEquity(Object.assign({}, noTrade, { hasTradeIn: true })), 10000);
    assert.equal(CP.computeFinanceTradeInEquity(Object.assign({}, noTrade, { hasTradeIn: true, condition: "rough" })), 6000);
    // $30,000 at 7% tax + $500 fees, $2,000 down, 60 months at 8% APR
    const base = { vehiclePrice: 30000, downPayment: 2000, tier: "good", term: "60", tradeInValue: 0, taxRatePct: "7", fees: "500" };
    const pay = CP.computeFinanceMonthlyPayment(base);
    assert.ok(Math.abs(pay - 620.46) < 0.01, String(pay)); // principal 30,600
    assert.equal(CP.financePaymentText(pay), "620");
    assert.equal(CP.computeFinanceMonthlyPayment(Object.assign({}, base, { term: "" })), null);
    assert.equal(CP.financePaymentText(null), "--");
    assert.equal(CP.computeFinanceMonthlyPayment(Object.assign({}, base, { downPayment: 40000 })), 0);
    assert.equal(CP.financePaymentText(0), "0");
    // unknown tier falls back to "good"
    assert.equal(CP.computeFinanceMonthlyPayment(Object.assign({}, base, { tier: "platinum" })), pay);
    assert.equal(CP.formatFinancePayment(12345.6), "12,346");
    assert.equal(CP.formatFinancePayment(NaN), "--");
});
