"use strict";
/**
 * D-N12 (owner decision 2026-10-18, unit N2.4): the grid card prints no mileage
 * bit for a New, non-CPO car with no odometer value. The server sets
 * ``mileage_line_hidden`` (backend/utils/mileage_display.py); card.js only reads
 * it. Rows here are API-shaped, with the flags the serializer would set; the
 * Python side (test_mileage_line_d_n12.py) feeds real serializer output through
 * the same renderer.
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic } = require("./_load.js");

function dsl() {
    return loadStatic(["ds_core.js", "sc-helpers.js", "listings/card.js"]).DSL;
}

const YEAR = new Date().getFullYear();

function metaLine(D, row) {
    const car = Object.assign(
        { id: 7, make: "Honda", model: "Accord", gallery: [], fuel_type: "Gasoline", drivetrain: "FWD" },
        row
    );
    const html = D.renderCardHtml(car, new Set(), [], { loggedIn: false, zipForUrl: "", distanceMiles: null });
    const m = /<p class="result-meta">(.*?)<\/p>/.exec(html);
    assert.ok(m, "card has a meta line");
    return m[1].replace(/<[^>]+>/g, "");
}

test("cardMileageLineHidden reads only the server flag", () => {
    const D = dsl();
    assert.equal(D.cardMileageLineHidden({ mileage_line_hidden: true }), true);
    assert.equal(D.cardMileageLineHidden({ mileage_line_hidden: false }), false);
    assert.equal(D.cardMileageLineHidden({ mileage_line_hidden: "true" }), false);
    // No flag: no JS copy of the rule, the card keeps the DC-6 wording.
    assert.equal(D.cardMileageLineHidden({ condition: "New", mileage: null, year: YEAR }), false);
    assert.equal(D.cardMileageLineHidden(null), false);
});

test("card meta line per D-N12 case", () => {
    const D = dsl();
    const cases = [
        ["new-null", { condition: "New", mileage: null, year: YEAR, mileage_not_listed: true, mileage_line_hidden: true },
            "New · Gasoline · FWD"],
        ["new-zero-current", { condition: "New", mileage: 0, year: YEAR, mileage_not_listed: false, mileage_line_hidden: false },
            "New · 0 mi · Gasoline · FWD"],
        ["new-zero-old", { condition: "New", mileage: 0, year: YEAR - 3, mileage_not_listed: true, mileage_line_hidden: false },
            "New · Mileage not listed · Gasoline · FWD"],
        ["new-null-old", { condition: "New", mileage: null, year: YEAR - 3, mileage_not_listed: true, mileage_line_hidden: false },
            "New · Mileage not listed · Gasoline · FWD"],
        ["cpo-null", { condition: "New", is_cpo: true, mileage: null, year: YEAR, mileage_not_listed: true, mileage_line_hidden: false },
            "CPO · Mileage not listed · Gasoline · FWD"],
        ["used-zero", { condition: "Used", mileage: 0, year: YEAR - 5, mileage_not_listed: true, mileage_line_hidden: false },
            "Pre-owned · Mileage not listed · Gasoline · FWD"],
        ["used-null", { condition: "Used", mileage: null, year: YEAR - 5, mileage_not_listed: true, mileage_line_hidden: false },
            "Pre-owned · Mileage not listed · Gasoline · FWD"],
        ["used-figure", { condition: "Used", mileage: 34567, year: YEAR - 5, mileage_not_listed: false, mileage_line_hidden: false },
            "Pre-owned · 34,567 mi · Gasoline · FWD"],
        ["no-flags-new-null", { condition: "New", mileage: null, year: YEAR },
            "New · Mileage not listed · Gasoline · FWD"],
    ];
    for (const [name, row, want] of cases) {
        assert.equal(metaLine(D, row).replace(/ &middot; /g, " · "), want, name);
    }
});
