"use strict";
/** Pure parts of frontend/static/car/car_gallery.js and cp_canvas.js (split from car_page.js). */
const test = require("node:test");
const assert = require("node:assert/strict");
const { loadStatic, plain } = require("./_load.js");

test("CP.filterGalleryUrls drops junk and falls back to the hero src", () => {
    const CP = loadStatic(["car/car_gallery.js"]).CP;
    const good = "https://cdn.example/a.jpg";
    assert.deepEqual(
        plain(CP.filterGalleryUrls([good, "/relative.jpg", "https://x/transferBadge.png", "https://x/Coming Soon.jpg", 7, null, good], "")),
        [good, good]
    );
    assert.deepEqual(plain(CP.filterGalleryUrls([], "https://cdn.example/hero.jpg")), ["https://cdn.example/hero.jpg"]);
    assert.deepEqual(plain(CP.filterGalleryUrls(["https://x/gubagoo/a.png"], "https://x/photoswipe/b.png")), []);
    assert.deepEqual(plain(CP.filterGalleryUrls({ not: "an array" }, null)), []);
    assert.equal(CP.isGalleryJunkUrl("http://idrove.it/x"), true);
    assert.equal(CP.isGalleryJunkUrl("https://cdn.example/ok.jpg"), false);
});

test("CP.setupHiDpiCanvas sizes the backing store and CP.chartPlotArea maps values", () => {
    const win = loadStatic(["car/cp_canvas.js"]);
    win.devicePixelRatio = 2;
    const calls = [];
    const ctx = {
        setTransform: (...a) => calls.push(["setTransform", ...a]),
        clearRect: (...a) => calls.push(["clearRect", ...a]),
    };
    const canvas = {
        style: {},
        closest: () => ({ clientWidth: 600 }),
        getContext: () => ctx,
    };
    const surface = win.CP.setupHiDpiCanvas(canvas, ".frame");
    assert.equal(surface.cssWidth, 408);
    assert.equal(surface.cssHeight, 236);
    assert.equal(canvas.width, 816);
    assert.equal(canvas.style.width, "408px");
    assert.deepEqual(calls[0], ["setTransform", 2, 0, 0, 2, 0, 0]);
    // a frame still too narrow (hidden tab) -> null
    assert.equal(win.CP.setupHiDpiCanvas({ style: {}, closest: () => ({ clientWidth: 100 }), getContext: () => ctx }, ".f"), null);
    const plot = win.CP.chartPlotArea(surface, 0, 1000, 5);
    assert.equal(plot.xAt(0), 44);
    assert.equal(plot.xAt(5), 408 - 12);
    assert.equal(plot.yAt(1000), 16);
    assert.equal(plot.yAt(0), plot.chartBottom);
    assert.deepEqual(plain(win.CP.CHART_YEAR_LABELS), ["Now", "Yr 1", "Yr 2", "Yr 3", "Yr 4", "Yr 5"]);
});
