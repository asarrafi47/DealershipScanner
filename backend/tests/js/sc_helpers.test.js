"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic, plain } = require("./_load.js");

function sc() {
    return loadStatic(["ds_core.js", "sc-helpers.js"]).SC;
}

test("SC.escapeHtml escapes & < > \" (not ') and maps null to empty", () => {
    const SC = sc();
    assert.equal(SC.escapeHtml(`a"b<c>&`), "a&quot;b&lt;c&gt;&amp;");
    assert.equal(SC.escapeHtml("it's"), "it's"); // listings copy leaves ' alone (text/double-quoted attrs only)
    assert.equal(SC.escapeHtml(null), "");
    assert.equal(SC.escapeHtml(0), "0");
});

test("SC.fmtUSD formats listing prices through DS", () => {
    const SC = sc();
    assert.equal(SC.fmtUSD(28995), "$28,995");
    assert.equal(SC.fmtUSD("41250.4"), "$41,250");
    assert.equal(SC.fmtUSD(0), "Call for Price");
    assert.equal(SC.fmtUSD(null), "Call for Price");
    assert.equal(SC.fmtUSD(""), "Call for Price");
});

test("SC.safeImageSrc allows only http(s)", () => {
    const SC = sc();
    assert.equal(SC.safeImageSrc(""), "/static/placeholder.svg");
    assert.equal(SC.safeImageSrc("javascript:alert(1)"), "/static/placeholder.svg");
    assert.equal(SC.safeImageSrc("data:image/png;base64,AA"), "/static/placeholder.svg");
    assert.equal(SC.safeImageSrc("https://cdn.example.com/a.jpg"), "https://cdn.example.com/a.jpg");
    assert.equal(SC.safeImageSrc("/img/a.jpg"), "https://example.test/img/a.jpg");
});

test("SC.normFilterStr / isValidUsZip / valueInListCI*", () => {
    const SC = sc();
    assert.equal(SC.normFilterStr("  BMW "), "bmw");
    assert.equal(SC.normFilterStr(null), "");
    assert.equal(SC.isValidUsZip("37421"), true);
    assert.equal(SC.isValidUsZip(" 37421 "), true);
    assert.equal(SC.isValidUsZip("3742"), false);
    assert.equal(SC.isValidUsZip("37421-1234"), false);
    assert.equal(SC.valueInListCI([], "x"), true);
    assert.equal(SC.valueInListCI(["Honda", "Toyota"], " toyota "), true);
    assert.equal(SC.valueInListCI(["Honda"], "Ford"), false);
    assert.equal(SC.valueInListCISmart([], "x"), false);
    assert.equal(SC.valueInListCISmart(["AWD"], "awd"), true);
    assert.equal(SC.valueInListCISmart(["AWD"], ""), false);
});

test("SC.buildPageNumberWindow", () => {
    const SC = sc();
    assert.deepEqual(plain(SC.buildPageNumberWindow(1, 5)), [1, 2, 3, 4, 5]);
    assert.deepEqual(plain(SC.buildPageNumberWindow(1, 20)), [1, 2, "…", 20]);
    assert.deepEqual(plain(SC.buildPageNumberWindow(10, 20)), [1, "…", 9, 10, 11, "…", 20]);
    assert.deepEqual(plain(SC.buildPageNumberWindow(20, 20)), [1, "…", 19, 20]);
});

test("SC.mileageBand", () => {
    const SC = sc();
    assert.equal(SC.mileageBand(null), "unknown");
    assert.equal(SC.mileageBand(-5), "unknown");
    assert.equal(SC.mileageBand(0), "0-25k");
    assert.equal(SC.mileageBand(25000), "0-25k");
    assert.equal(SC.mileageBand(25001), "25-50k");
    assert.equal(SC.mileageBand("75000"), "50-75k");
    assert.equal(SC.mileageBand(100000), "75-100k");
    assert.equal(SC.mileageBand(100001), "100k+");
});

test("SC badges", () => {
    const SC = sc();
    assert.equal(SC.dealBadgeHtml(null), "");
    assert.match(SC.dealBadgeHtml({ delta_pct: -5.2, vs_market: "below_market" }), /Below market <span[^>]*>-5\.2%/);
    assert.match(SC.dealBadgeHtml({ delta_pct: 4, vs_market: "above_market" }), />\+4%</);
    assert.equal(SC.dealScoreBadgeHtml({ label: "insufficient_data" }), "");
    assert.equal(SC.dealScoreBadgeHtml({ label: "payment_listed" }), "");
    assert.match(SC.dealScoreBadgeHtml({ label: "at_market", pct_from_median: 1 }), /result-deal-badge--near_market">Fair price<\/span>$/);
    assert.match(SC.dealScoreBadgeHtml({ label: "below_market", pct_from_median: -7.6 }), />-8%</);
    assert.equal(SC.cpoBadgeHtml({ is_cpo: false }), "");
    assert.match(SC.cpoBadgeHtml({ is_cpo: true }), /Certified Pre-Owned/);
    assert.equal(SC.priceDropBadgeHtml({ price_drop_amount: 0 }), "");
    assert.match(SC.priceDropBadgeHtml({ price_drop_amount: 500, price_drop_days_ago: 1 }), /▼ \$500 <span[^>]*>1 day ago</);
    assert.match(SC.priceDropBadgeHtml({ price_drop_amount: 500, price_drop_days_ago: 0 }), />today</);
});

test("SC.carSmartEquipmentHaystack", () => {
    const SC = sc();
    assert.equal(SC.carSmartEquipmentHaystack(null), "");
    assert.equal(
        SC.carSmartEquipmentHaystack({ package_names: ["Tech Pkg"], make: "BMW", model: "X5", is_cpo: true }),
        "tech pkg bmw x5 certified pre-owned cpo",
    );
});

test("SC market cohort math", () => {
    const SC = sc();
    assert.deepEqual(plain(SC.marketTrimParts({ make: " Honda", model: "CR-V ", trim: null })), ["honda", "cr-v", ""]);
    assert.equal(SC.marketCohortKey("Honda", "CR-V", "EX", 2022, "0-25k"), "honda|cr-v|ex|2022|0-25k");
    assert.equal(SC.marketCohortKey("Honda", "CR-V", "EX", null, "0-25k"), "honda|cr-v|ex|*|0-25k");
    const stats = SC.weightedCohortStats(
        [{ sample_count: 2, avg_price: 100 }, { sample_count: 3, avg_price: 200 }, null, { sample_count: 0, avg_price: 1 }],
        3,
    );
    assert.deepEqual(plain(stats), { avg_price: 160, sample_count: 5 });
    assert.equal(SC.weightedCohortStats([{ sample_count: 2, avg_price: 100 }], 3), null);
    const cohorts = { "a|b|c|2020|x": { k: 1 }, "a|b|c|2021|x": { k: 2 } };
    assert.deepEqual(plain(SC.cohortEntries(cohorts, "a", "b", "c", [2020, 2021, 2022], ["x"])), [{ k: 1 }, { k: 2 }]);
});
