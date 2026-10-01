"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic } = require("./_load.js");

// ---- Reference copies: the per-file implementations window.DS replaced, verbatim
// from the tree before ds_core.js existed. DS must match them byte for byte.
const REF = {
    // car_packages.js esc / car_trim_ladder.js escapeHtml / dev.js escHtml
    escape5(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    },
    // car_dealer_map.js escapeHtml (no null guard; its callers always pass a truthy value)
    escape5NoGuard(s) {
        return String(s)
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    },
    // sc-helpers.js fmtUSD
    scFmtUSD(n) {
        if (n == null || n === "" || Number(n) === 0) return "Call for Price";
        return "$" + Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
    },
    // dev_scan_lab.js formatPrice
    labFormatPrice(n) {
        if (n == null || n === "") return "—";
        const v = Number(n);
        return Number.isFinite(v) ? "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 }) : "—";
    },
    // car_page.js formatTcoCurrency / car_page_helpers.js formatDepreciationCurrency
    tcoCurrency(amount) {
        if (!Number.isFinite(amount)) return "$--";
        return "$" + Math.round(amount).toLocaleString("en-US");
    },
    // car_page.js formatTcoFillUpCurrency
    tcoFillUp(amount) {
        if (!Number.isFinite(amount)) return "$--";
        return "$" + amount.toFixed(2);
    },
    // car_page.js formatTcoCostCurrencyShort / car_page_helpers.js formatDepreciationCurrencyShort
    tcoShort(amount) {
        if (!Number.isFinite(amount)) return "$--";
        if (amount >= 1000) {
            const thousands = amount / 1000;
            const rounded =
                thousands >= 100
                    ? Math.round(thousands)
                    : parseFloat(thousands.toFixed(thousands < 10 ? 1 : 0));
            return "$" + rounded.toLocaleString("en-US") + "k";
        }
        return REF.tcoCurrency(amount);
    },
    // account_profile / dealership_page / find_dealers / main.js / ai_chatbot csrf readers
    csrf(doc) {
        const m = doc.querySelector('meta[name="csrf-token"]');
        return m && m.content ? m.content : "";
    },
};

const NUMBERS = [
    0, -0, 1, 0.4, 0.5, 0.6, 1.5, 2.5, -0.5, -1.5, -2.5, 9.99, 999, 999.4, 999.5, 999.99,
    1000, 1049, 1050, 1949.99, 9949, 9950, 9999, 10000, 12345.678, 99499, 99500, 99999,
    100000, 123456, 1234567.891, -1, -999.5, -1000, -12345.6, 1e21, 0.1 + 0.2,
    NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER, Number.MIN_VALUE,
];
const LOOSE = NUMBERS.concat([
    null, undefined, "", " ", "0", "00", "1234", "1234.5", "  42 ", "$5", "abc", "1e3",
    true, false, [], [7], {}, "Infinity",
]);
const STRINGS = [
    "", "plain", `a"b'c<d>&`, "&amp;", "<script>alert('x')</script>", "it's \"quoted\"",
    "ünïcødé – €", "\n\t", 0, 1, -1, false, true, NaN, [], [1, "<2>"], {}, null, undefined,
];

function ds(opts) {
    return loadStatic(["ds_core.js"], opts).DS;
}

test("ds_core defines exactly window.DS with three functions and nothing else", () => {
    const before = new Set(Object.keys(loadStatic([])));
    const w = loadStatic(["ds_core.js"]);
    const added = Object.keys(w).filter((k) => !before.has(k));
    assert.deepEqual(added, ["DS"]);
    assert.deepEqual(Object.keys(w.DS).sort(), ["csrfToken", "escapeHtml", "formatUsd"]);
});

test("a second load keeps the first DS object", () => {
    const w = loadStatic(["ds_core.js"]);
    const first = w.DS;
    require("node:vm").runInContext(
        require("node:fs").readFileSync(require("node:path").join(__dirname, "../../../frontend/static/ds_core.js"), "utf8"),
        w,
    );
    assert.equal(w.DS, first);
});

test("escapeHtml escapes & < > \" ' and maps null/undefined to empty", () => {
    const D = ds();
    assert.equal(D.escapeHtml(`a"b'c<d>&`), "a&quot;b&#39;c&lt;d&gt;&amp;");
    assert.equal(D.escapeHtml(`"`), "&quot;");
    assert.equal(D.escapeHtml(`'`), "&#39;");
    assert.equal(D.escapeHtml("&amp;"), "&amp;amp;");
    assert.equal(D.escapeHtml(null), "");
    assert.equal(D.escapeHtml(undefined), "");
    assert.equal(D.escapeHtml(0), "0");
    assert.equal(D.escapeHtml(false), "false");
});

test("escapeHtml matches the copies it replaced", () => {
    const D = ds();
    for (const s of STRINGS) {
        assert.equal(D.escapeHtml(s), REF.escape5(s), JSON.stringify(s));
        if (s != null) assert.equal(D.escapeHtml(s), REF.escape5NoGuard(s), JSON.stringify(s));
    }
});

test("csrfToken reads the meta tag and falls back to empty", () => {
    assert.equal(ds({ metas: { "csrf-token": "tok-123_ABC" } }).csrfToken(), "tok-123_ABC");
    assert.equal(ds({ metas: { "csrf-token": "" } }).csrfToken(), "");
    assert.equal(ds({ metas: {} }).csrfToken(), "");
    for (const metas of [{ "csrf-token": "x-Y_9" }, { "csrf-token": "" }, {}]) {
        const w = loadStatic(["ds_core.js"], { metas });
        assert.equal(w.DS.csrfToken(), REF.csrf(w.document));
    }
});

test("formatUsd default (listing price) matches sc-helpers fmtUSD", () => {
    const D = ds();
    for (const n of LOOSE) assert.equal(D.formatUsd(n), REF.scFmtUSD(n), String(n));
    assert.equal(D.formatUsd(null), "Call for Price");
    assert.equal(D.formatUsd(0), "Call for Price");
    assert.equal(D.formatUsd("0"), "Call for Price");
    assert.equal(D.formatUsd(28995), "$28,995");
    assert.equal(D.formatUsd(28995.5), "$28,996");
    assert.equal(D.formatUsd(-2.5), "$-3");
    assert.equal(D.formatUsd("abc"), "$NaN");
});

test("formatUsd empty label override (dev.js 'No price')", () => {
    const D = ds();
    assert.equal(D.formatUsd(null, { empty: "No price" }), "No price");
    assert.equal(D.formatUsd(0, { empty: "No price" }), "No price");
    assert.equal(D.formatUsd(1500, { empty: "No price" }), "$1,500");
});

test("formatUsd lab options match dev_scan_lab formatPrice", () => {
    const D = ds();
    const opts = { empty: "—", zeroIsEmpty: false, invalid: "—" };
    for (const n of LOOSE) assert.equal(D.formatUsd(n, opts), REF.labFormatPrice(n), String(n));
    assert.equal(D.formatUsd(0, opts), "$0");
    assert.equal(D.formatUsd("abc", opts), "—");
});

test("formatUsd strict matches the TCO / depreciation formatters", () => {
    const D = ds();
    for (const n of LOOSE) {
        assert.equal(D.formatUsd(n, { strict: true }), REF.tcoCurrency(n), String(n));
        assert.equal(D.formatUsd(n, { strict: true, cents: true }), REF.tcoFillUp(n), String(n));
        assert.equal(D.formatUsd(n, { strict: true, short: true }), REF.tcoShort(n), String(n));
    }
    // strict never coerces: a numeric string is not a number
    assert.equal(D.formatUsd("1234", { strict: true }), "$--");
    assert.equal(D.formatUsd(1234.5, { strict: true }), "$1,235");
    assert.equal(D.formatUsd(-2.5, { strict: true }), "$-2");
    assert.equal(D.formatUsd(54.321, { strict: true, cents: true }), "$54.32");
    assert.equal(D.formatUsd(12345, { strict: true, cents: true }), "$12345.00");
    assert.equal(D.formatUsd(999, { strict: true, short: true }), "$999");
    assert.equal(D.formatUsd(1050, { strict: true, short: true }), "$1.1k");
    assert.equal(D.formatUsd(45000, { strict: true, short: true }), "$45k");
    assert.equal(D.formatUsd(123456, { strict: true, short: true }), "$123k");
    assert.equal(D.formatUsd(1234567, { strict: true, short: true }), "$1,235k");
});

test("formatUsd matches the references over a random sweep", () => {
    const D = ds();
    let seed = 12345;
    const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648);
    for (let i = 0; i < 5000; i += 1) {
        const mag = Math.pow(10, Math.floor(rnd() * 8));
        const n = (rnd() - 0.2) * mag;
        const r = Math.round(n * 100) / 100;
        for (const v of [n, r, Math.round(n)]) {
            assert.equal(D.formatUsd(v), REF.scFmtUSD(v));
            assert.equal(D.formatUsd(v, { strict: true }), REF.tcoCurrency(v));
            assert.equal(D.formatUsd(v, { strict: true, cents: true }), REF.tcoFillUp(v));
            assert.equal(D.formatUsd(v, { strict: true, short: true }), REF.tcoShort(v));
        }
    }
});
