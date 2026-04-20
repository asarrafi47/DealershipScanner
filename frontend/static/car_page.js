/**
 * Car detail page: gallery, history highlights, back navigation.
 * Loaded only from car.html (no inline script for CSP).
 */
(function () {
    "use strict";

    function initCarBackLink() {
        const el = document.getElementById("car-back-link");
        if (!el) return;
        el.addEventListener("click", function () {
            if (window.history.length > 1) window.history.back();
            else window.location.href = "/listings";
        });
    }

    function initCarGallery() {
        const jsonEl = document.getElementById("car-gallery-json");
        const imgEl = document.getElementById("car-gallery-main-img");
        const heroEl = document.getElementById("car-gallery-hero");
        const prevBtn = document.getElementById("car-gallery-prev");
        const nextBtn = document.getElementById("car-gallery-next");
        const prevHero = document.getElementById("car-gallery-prev-hero");
        const nextHero = document.getElementById("car-gallery-next-hero");
        const counterEl = document.getElementById("car-gallery-counter");
        const thumbsEl = document.getElementById("car-gallery-thumbs");
        if (!imgEl) return;

        let gallery = [];
        try {
            if (jsonEl && jsonEl.textContent) gallery = JSON.parse(jsonEl.textContent);
        } catch (e) {
            gallery = [];
        }
        if (!Array.isArray(gallery)) gallery = [];
        gallery = gallery.filter(function (u) {
            return u && typeof u === "string";
        });
        if (gallery.length === 0) return;

        let activeImageIndex = 0;
        function show() {
            const url = gallery[activeImageIndex];
            if (url && imgEl) imgEl.src = url;
            if (counterEl) counterEl.textContent = activeImageIndex + 1 + " of " + gallery.length;
            if (prevBtn) prevBtn.disabled = false;
            if (nextBtn) nextBtn.disabled = false;
            if (prevHero) prevHero.disabled = false;
            if (nextHero) nextHero.disabled = false;
            if (thumbsEl) {
                const tabs = thumbsEl.querySelectorAll(".car-gallery-thumb");
                tabs.forEach(function (t, i) {
                    t.classList.toggle("active", i === activeImageIndex);
                    t.setAttribute("aria-selected", i === activeImageIndex);
                });
                const activeThumb = thumbsEl.querySelector(".car-gallery-thumb.active");
                if (activeThumb)
                    activeThumb.scrollIntoView({
                        behavior: "smooth",
                        block: "nearest",
                        inline: "nearest",
                    });
            }
        }

        function goPrev(e) {
            if (e) e.preventDefault();
            activeImageIndex = (activeImageIndex - 1 + gallery.length) % gallery.length;
            show();
        }
        function goNext(e) {
            if (e) e.preventDefault();
            activeImageIndex = (activeImageIndex + 1) % gallery.length;
            show();
        }

        if (heroEl) {
            heroEl.addEventListener("click", function (e) {
                if (e.target.closest("button")) return;
                const rect = heroEl.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const w = rect.width;
                if (x < w * 0.33) goPrev(e);
                else if (x > w * 0.67) goNext(e);
            });
        }
        if (prevBtn) prevBtn.addEventListener("click", goPrev);
        if (nextBtn) nextBtn.addEventListener("click", goNext);
        if (prevHero) prevHero.addEventListener("click", goPrev);
        if (nextHero) nextHero.addEventListener("click", goNext);

        if (thumbsEl && gallery.length > 1) {
            gallery.forEach(function (url, i) {
                const t = document.createElement("button");
                t.type = "button";
                t.className = "car-gallery-thumb" + (i === 0 ? " active" : "");
                t.setAttribute("role", "tab");
                t.setAttribute("aria-selected", i === 0);
                t.setAttribute("aria-label", "Image " + (i + 1) + " of " + gallery.length);
                t.style.setProperty("background-image", "url(" + JSON.stringify(url) + ")");
                t.addEventListener("click", function () {
                    activeImageIndex = i;
                    show();
                });
                thumbsEl.appendChild(t);
            });
        }

        document.addEventListener("keydown", function (e) {
            if (e.target.matches("input, textarea")) return;
            if (gallery.length <= 1) return;
            if (e.key === "ArrowLeft") {
                goPrev(e);
                e.preventDefault();
            }
            if (e.key === "ArrowRight") {
                goNext(e);
                e.preventDefault();
            }
        });

        show();
    }

    function initCarHistoryHighlights() {
        const jsonEl = document.getElementById("car-history-json");
        const listEl = document.getElementById("history-highlights-list");
        const noHl = document.getElementById("history-no-highlights");
        if (!jsonEl || !listEl) return;

        const KEY_LABELS = {
            normalFuelType: "Fuel Type",
            fuelType: "Fuel Type",
            type: "Condition",
            condition: "Condition",
            ownerCount: "Owners",
            numberOfOwners: "Owners",
            accidentCount: "Accidents",
            accidents: "Accidents",
            frameRepairs: "Frame Damage",
            titleIssues: "Title Issues",
            ownerHistory: "Owner History",
            usageType: "Usage",
            personalUse: "Personal Use",
            odometer: "Mileage",
            mileage: "Mileage",
            make: "Make",
            model: "Model",
            year: "Year",
            trim: "Trim",
            vin: "VIN",
            stockNumber: "Stock #",
            cylinders: "Cylinders",
            transmission: "Transmission",
            drivetrain: "Drivetrain",
            exteriorColor: "Exterior Color",
            interiorColor: "Interior Color",
            serviceRecords: "Service Records",
            lemonHistory: "Lemon History",
            salvageHistory: "Salvage History",
        };

        const BAD = new Set(["n/a", "na", "", "null", "none", "unknown", "undefined", "-", "--", "—"]);

        function isBad(v) {
            if (v === null || v === undefined) return true;
            return BAD.has(String(v).trim().toLowerCase());
        }

        function isCamelKey(s) {
            return typeof s === "string" && !/\s/.test(s) && /[a-z][A-Z]/.test(s);
        }

        function camelToLabel(key) {
            if (KEY_LABELS[key]) return KEY_LABELS[key];
            return key
                .replace(/([A-Z])/g, " $1")
                .replace(/^./, function (c) {
                    return c.toUpperCase();
                })
                .trim();
        }

        function esc(s) {
            return String(s)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;");
        }

        let raw = [];
        try {
            raw = JSON.parse(jsonEl.textContent || "[]");
        } catch (e) {
            raw = [];
        }
        if (!Array.isArray(raw)) raw = [];

        const rows = [];

        raw.forEach(function (item) {
            if (item === null || item === undefined) return;

            if (typeof item === "string") {
                const s = item.trim();
                if (isBad(s) || isCamelKey(s) || s.length < 2) return;
                rows.push({ label: null, value: s });
            } else if (typeof item === "object" && !Array.isArray(item)) {
                if ("label" in item && "value" in item) {
                    const lbl = String(item.label || "").trim();
                    const val = String(item.value === null ? "" : item.value).trim();
                    if (!isBad(val) && !isCamelKey(val) && val.length >= 1)
                        rows.push({ label: lbl || null, value: val });
                    return;
                }
                Object.keys(item).forEach(function (k) {
                    const v = item[k];
                    if (isBad(v)) return;
                    const vs = String(v).trim();
                    if (vs.length < 1 || isCamelKey(vs)) return;
                    rows.push({ label: camelToLabel(k), value: vs });
                });
            }
        });

        if (rows.length === 0) return;

        let html = "";
        rows.forEach(function (row) {
            if (row.label) {
                html +=
                    '<li class="history-highlight-item">' +
                    '<span class="hh-label">' +
                    esc(row.label) +
                    ":</span> " +
                    '<span class="hh-value">' +
                    esc(row.value) +
                    "</span>" +
                    "</li>";
            } else {
                html += '<li class="history-highlight-item">' + esc(row.value) + "</li>";
            }
        });

        listEl.innerHTML = html;
        listEl.removeAttribute("hidden");
        if (noHl) noHl.style.display = "none";
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            initCarBackLink();
            initCarGallery();
            initCarHistoryHighlights();
        });
    } else {
        initCarBackLink();
        initCarGallery();
        initCarHistoryHighlights();
    }
})();
