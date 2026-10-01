/**
 * Car detail page entry point: back link, share, history highlights,
 * depreciation chart, EV battery, vehicle-history actions, negotiation radar,
 * build-sheet print, lazy dealer map, VDP tabs -- and the boot sequence.
 *
 * The gallery, finance calculator and TCO card live in static/car/
 * (car_gallery.js, car_finance.js, car_tco.js + car_tco_math.js, with the
 * shared canvas frame in cp_canvas.js) and register on window.CarPage; car.html
 * loads them, car_page_helpers.js and ds_core.js before this file.
 * Loaded only from car.html.
 */
(function () {
    "use strict";

    const CarPage = window.CarPage;

    function initCarBackLink() {
        const el = document.getElementById("car-back-link");
        if (!el) return;
        el.addEventListener("click", function () {
            if (window.history.length > 1) window.history.back();
            else window.location.href = "/listings";
        });
    }

    function initCarShareButton() {
        const btn = document.getElementById("car-share-btn");
        if (!btn) return;
        const toast = document.getElementById("car-share-toast");
        let toastTimer = null;

        function showToast(text) {
            if (!toast) return;
            toast.textContent = text;
            toast.hidden = false;
            if (toastTimer) window.clearTimeout(toastTimer);
            toastTimer = window.setTimeout(function () {
                toast.hidden = true;
            }, 2400);
        }

        function fallbackCopyLink(url) {
            function done(ok) {
                showToast(ok ? "Link copied" : "Couldn't copy link — copy it from the address bar");
            }
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(url).then(
                    function () {
                        done(true);
                    },
                    function () {
                        done(false);
                    }
                );
                return;
            }
            // Very old browsers with no Clipboard API: a hidden textarea +
            // execCommand is the last resort.
            try {
                const ta = document.createElement("textarea");
                ta.value = url;
                ta.style.position = "fixed";
                ta.style.opacity = "0";
                document.body.appendChild(ta);
                ta.focus();
                ta.select();
                const ok = document.execCommand("copy");
                document.body.removeChild(ta);
                done(ok);
            } catch (e) {
                done(false);
            }
        }

        btn.addEventListener("click", function () {
            const url = window.location.href;
            const title = btn.getAttribute("data-share-title") || document.title;
            if (navigator.share) {
                navigator.share({ title: title, url: url }).catch(function (err) {
                    // The user dismissing the native share sheet is not a
                    // failure -- only fall back to copy-link for a real error
                    // (e.g. share unsupported for this payload).
                    if (err && err.name === "AbortError") return;
                    fallbackCopyLink(url);
                });
                return;
            }
            fallbackCopyLink(url);
        });
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
                if (isBad(s) || s.length < 2) return;
                const colonIdx = s.indexOf(":");
                if (colonIdx > 0 && colonIdx < s.length - 1) {
                    const key = s.slice(0, colonIdx).trim();
                    const val = s.slice(colonIdx + 1).trim();
                    if (!isBad(val) && key.length >= 1) {
                        rows.push({ label: camelToLabel(key), value: val });
                        return;
                    }
                }
                if (isCamelKey(s)) return;
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

    function getSelectedDepreciationAnnualMileage(card) {
        const select = document.getElementById("dep-annual-mi-select");
        if (select && select.value) {
            const parsed = parseInt(select.value, 10);
            if (Number.isFinite(parsed)) return parsed;
        }
        const meta = CP.parseDepreciationMetadata(card);
        return CP.snapDepreciationAnnualMileage(CP.computeAnnualMileage(meta));
    }

    function initDepreciationAnnualMileageSelect(card) {
        const select = document.getElementById("dep-annual-mi-select");
        if (!select) return;

        if (!select.options.length) {
            const defaultMileage = CP.snapDepreciationAnnualMileage(
                CP.computeAnnualMileage(CP.parseDepreciationMetadata(card))
            );
            CP.buildDepreciationAnnualMileageOptions().forEach(function (mi) {
                const opt = document.createElement("option");
                opt.value = String(mi);
                opt.textContent = CP.formatDepreciationAnnualMileageOption(mi);
                if (mi === defaultMileage) opt.selected = true;
                select.appendChild(opt);
            });
        }

        if (select.dataset.bound === "true") return;
        select.dataset.bound = "true";
        select.addEventListener("change", function () {
            renderDepreciationCurve();
        });
    }

    /** The depreciation value line: shaded area, the line, then the point markers. */
    function drawDepreciationLine(plot, values) {
        const ctx = plot.ctx;
        const lineColor = "#2563eb";
        const points = values.map(function (val, i) {
            return { x: plot.xAt(i), y: plot.yAt(val), val: val };
        });

        const areaGradient = ctx.createLinearGradient(0, plot.pad.top, 0, plot.chartBottom);
        areaGradient.addColorStop(0, "rgba(37, 99, 235, 0.28)");
        areaGradient.addColorStop(1, "rgba(37, 99, 235, 0.03)");
        ctx.beginPath();
        ctx.moveTo(points[0].x, plot.chartBottom);
        points.forEach(function (pt) {
            ctx.lineTo(pt.x, pt.y);
        });
        ctx.lineTo(points[points.length - 1].x, plot.chartBottom);
        ctx.closePath();
        ctx.fillStyle = areaGradient;
        ctx.fill();

        ctx.strokeStyle = lineColor;
        ctx.lineWidth = 2;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.beginPath();
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 1; i < points.length; i += 1) {
            ctx.lineTo(points[i].x, points[i].y);
        }
        ctx.stroke();

        points.forEach(function (pt, i) {
            const radius = i === 0 || i === 5 ? 4.5 : 3.5;
            ctx.beginPath();
            ctx.arc(pt.x, pt.y, radius, 0, Math.PI * 2);
            ctx.fillStyle = "#ffffff";
            ctx.fill();
            ctx.lineWidth = 1.75;
            ctx.strokeStyle = lineColor;
            ctx.stroke();
        });
    }

    function renderDepreciationCurve() {
        const card = document.getElementById("depreciation-forecast-card");
        const canvas = document.getElementById("depreciationCanvas");
        if (!card || !canvas) return;

        const surface = CP.setupHiDpiCanvas(canvas, ".depreciation-chart-frame");
        if (!surface) {
            requestAnimationFrame(renderDepreciationCurve);
            return;
        }

        const meta = CP.parseDepreciationMetadata(card);
        const annualMileage = getSelectedDepreciationAnnualMileage(card);
        const mileageSteps = CP.buildDepreciationMileageSteps(meta.mileage, annualMileage);
        const targetResidual = CP.computeDepreciationTargetResidual(meta, annualMileage);
        const values = CP.buildDepreciationTrajectory(meta.price, targetResidual);
        const price = meta.price;

        const y1MiEl = document.getElementById("dep-y1-mi");
        const y5MiEl = document.getElementById("dep-y5-mi");
        const y1El = document.getElementById("dep-y1");
        const y5El = document.getElementById("dep-y5");
        if (y1MiEl) y1MiEl.textContent = CP.formatDepreciationMileage(mileageSteps[1]);
        if (y5MiEl) y5MiEl.textContent = CP.formatDepreciationMileage(mileageSteps[5]);
        if (y1El) y1El.textContent = CP.formatDepreciationCurrency(values[1]);
        if (y5El) y5El.textContent = CP.formatDepreciationCurrency(values[5]);

        const minVal = Math.min.apply(null, values);
        const maxVal = price;
        const plot = CP.chartPlotArea(surface, minVal, maxVal - minVal || 1, 5);

        CP.drawChartGrid(plot);
        CP.drawChartYTicks(plot, CP.formatDepreciationCurrencyShort);
        drawDepreciationLine(plot, values);
        CP.drawChartXLabels(plot, CP.CHART_YEAR_LABELS, mileageSteps.map(CP.formatDepreciationMileage));

        canvas.setAttribute(
            "aria-label",
            "Estimated future value from " +
                CP.formatDepreciationCurrency(values[0]) +
                " now at " +
                CP.formatDepreciationMileage(mileageSteps[0]) +
                " to " +
                CP.formatDepreciationCurrency(values[5]) +
                " in year five at " +
                CP.formatDepreciationMileage(mileageSteps[5]) +
                ", assuming " +
                CP.formatDepreciationMileage(annualMileage) +
                " per year"
        );
    }

    let depreciationResizeObserver = null;
    function initDepreciationForecast() {
        const card = document.getElementById("depreciation-forecast-card");
        const canvas = document.getElementById("depreciationCanvas");
        if (!card || !canvas) return;

        initDepreciationAnnualMileageSelect(card);
        renderDepreciationCurve();

        const frame = canvas.closest(".depreciation-chart-frame");
        if (frame && typeof ResizeObserver !== "undefined") {
            depreciationResizeObserver = new ResizeObserver(function () {
                requestAnimationFrame(renderDepreciationCurve);
            });
            depreciationResizeObserver.observe(frame);
            return;
        }

        window.addEventListener("resize", function () {
            renderDepreciationCurve();
        });
    }


    function initEvBatteryIntelligence() {
        const block = document.getElementById("ev-battery-intelligence-block");
        if (!block) return;

        const mileage = parseFloat(block.getAttribute("data-mileage"));
        const year = parseInt(block.getAttribute("data-year"), 10);
        const carMake = block.getAttribute("data-car-make");

        const healthEl = document.getElementById("display-battery-health");
        const rangeEl = document.getElementById("display-real-world-range");
        const factoryRangeEl = document.getElementById("display-factory-range");
        const gaugeFill = document.getElementById("battery-gauge-fill");
        if (!healthEl || !rangeEl || !gaugeFill) return;

        const factoryRangeRaw = parseFloat(block.getAttribute("data-factory-range"));
        const safeFactoryRange =
            Number.isFinite(factoryRangeRaw) && factoryRangeRaw > 0 ? factoryRangeRaw : null;
        const safeMileage = Number.isFinite(mileage) && mileage >= 0 ? mileage : 0;
        const safeYear = Number.isFinite(year) && year > 0 ? year : new Date().getFullYear();

        if (factoryRangeEl) {
            factoryRangeEl.textContent = safeFactoryRange
                ? safeFactoryRange.toLocaleString("en-US") + " mi"
                : "—";
        }

        if (!safeFactoryRange) {
            healthEl.textContent = "--";
            rangeEl.textContent = "--";
            gaugeFill.style.width = "0%";
            gaugeFill.classList.remove(
                "battery-gauge-fill--green",
                "battery-gauge-fill--orange",
                "battery-gauge-fill--red"
            );
            return;
        }

        const currentYear = new Date().getFullYear();
        const modelYear = safeYear;
        const agePenalty = Math.max(0, currentYear - modelYear) * 1.0;

        let mileagePenalty;
        if (safeMileage <= 20000) {
            mileagePenalty = safeMileage * 0.00008;
        } else {
            mileagePenalty = 1.6 + (safeMileage - 20000) * 0.00005;
        }

        const basePenalty = agePenalty + mileagePenalty;
        const thermalScale = CP.evBatteryThermalScaleFactor(carMake);
        const scaledPenalty = basePenalty * thermalScale;

        let soh = 100 - scaledPenalty;
        soh = Math.max(65, soh);

        const realWorldRange = Math.round(safeFactoryRange * (soh / 100));
        const sohDisplay = soh.toFixed(1);

        healthEl.textContent = sohDisplay;
        rangeEl.textContent = String(realWorldRange);

        gaugeFill.classList.remove(
            "battery-gauge-fill--green",
            "battery-gauge-fill--orange",
            "battery-gauge-fill--red"
        );
        if (soh >= 85) {
            gaugeFill.classList.add("battery-gauge-fill--green");
        } else if (soh >= 75) {
            gaugeFill.classList.add("battery-gauge-fill--orange");
        } else {
            gaugeFill.classList.add("battery-gauge-fill--red");
        }

        gaugeFill.style.width = "0%";
        requestAnimationFrame(function () {
            gaugeFill.style.width = soh + "%";
        });
    }






    function readCarPageAccess() {
        const el = document.getElementById("car-page-access-json");
        if (!el || !el.textContent) return null;
        try {
            return JSON.parse(el.textContent);
        } catch (_) {
            return null;
        }
    }



    function initVehicleHistoryActions() {
        const block = document.getElementById("vehicle-history-action-block");
        const btn = document.getElementById("btn-premium-history-decode");
        const upsell = document.getElementById("premium-history-upsell");
        const upsellDismiss = document.getElementById("premium-history-upsell-dismiss");
        const resultsEl = document.getElementById("vehicle-history-intelligence-results");
        if (!block || !btn) return;

        const access = readCarPageAccess();
        const carId = access && access.car_id != null ? String(access.car_id) : "";
        const premiumUrl = (access && access.premium_url) || "/premium";

        function hideUpsell() {
            if (upsell) upsell.hidden = true;
        }

        function showUpsell() {
            if (!upsell) {
                window.location.href = premiumUrl;
                return;
            }
            upsell.hidden = false;
            const title = upsell.querySelector(".premium-history-upsell__title");
            if (title) title.focus();
        }

        if (upsellDismiss) {
            upsellDismiss.addEventListener("click", hideUpsell);
        }

        function renderIntelligence(payload) {
            if (!resultsEl) return;
            const flags = Array.isArray(payload.flags) ? payload.flags : [];
            const recalls = Array.isArray(payload.recalls) ? payload.recalls : [];
            if (!flags.length && !recalls.length) {
                resultsEl.innerHTML =
                    '<p class="vehicle-history-intelligence-empty">No government title or recall flags were found for this VIN.</p>';
                resultsEl.removeAttribute("hidden");
                return;
            }

            let html = '<div class="vehicle-history-intelligence-panel">';
            html += '<h3 class="vehicle-history-intelligence-title">Government &amp; title signals</h3>';
            if (flags.length) {
                html += '<ul class="vehicle-history-intelligence-flags">';
                flags.forEach(function (row) {
                    const sev = row.severity === "warning" ? "vehicle-history-flag--warning" : "";
                    html +=
                        '<li class="vehicle-history-flag ' +
                        sev +
                        '"><span class="vehicle-history-flag__label">' +
                        CP.escHtml(row.label || "Flag") +
                        '</span> <span class="vehicle-history-flag__value">' +
                        CP.escHtml(row.value || "") +
                        "</span></li>";
                });
                html += "</ul>";
            }
            if (recalls.length) {
                html += '<details class="vehicle-history-recalls-details"><summary>NHTSA recall details</summary><ul class="vehicle-history-recalls-list">';
                recalls.forEach(function (r) {
                    const head = [r.campaign, r.component].filter(Boolean).join(" — ");
                    html += "<li>";
                    if (head) html += "<strong>" + CP.escHtml(head) + "</strong> ";
                    if (r.summary) html += CP.escHtml(r.summary);
                    html += "</li>";
                });
                html += "</ul></details>";
            }
            html +=
                '<p class="vehicle-history-intelligence-disclaimer">Sourced from NHTSA public data and listing highlights. Not a NMVTIS or Carfax report.</p>';
            html += "</div>";
            resultsEl.innerHTML = html;
            resultsEl.removeAttribute("hidden");
        }

        function setLoading(loading) {
            btn.disabled = !!loading;
            btn.setAttribute("aria-busy", loading ? "true" : "false");
            if (loading) {
                btn.dataset.labelDefault = btn.textContent;
                btn.textContent = "Scanning government databases…";
            } else if (btn.dataset.labelDefault) {
                btn.textContent = btn.dataset.labelDefault;
            }
        }

        btn.addEventListener("click", function () {
            hideUpsell();
            if (!CP.hasPremiumHistoryAccess(access)) {
                showUpsell();
                return;
            }
            if (!carId) return;

            setLoading(true);
            if (resultsEl) {
                resultsEl.innerHTML = '<p class="vehicle-history-intelligence-loading">Loading NHTSA recalls and VIN validation…</p>';
                resultsEl.removeAttribute("hidden");
            }

            fetch("/api/cars/" + encodeURIComponent(carId) + "/vehicle-history-intelligence", {
                credentials: "same-origin",
            })
                .then(function (r) {
                    return r.json().then(function (data) {
                        return { ok: r.ok, data: data };
                    });
                })
                .then(function (res) {
                    if (!res.ok) {
                        if (
                            res.data &&
                            (res.data.error === "premium_required" || res.data.error === "login_required")
                        ) {
                            showUpsell();
                            if (resultsEl) resultsEl.hidden = true;
                            return;
                        }
                        if (resultsEl) {
                            resultsEl.innerHTML =
                                '<p class="vehicle-history-intelligence-error">Could not load government history data. Try again in a moment.</p>';
                        }
                        return;
                    }
                    renderIntelligence(res.data || {});
                })
                .catch(function () {
                    if (resultsEl) {
                        resultsEl.innerHTML =
                            '<p class="vehicle-history-intelligence-error">Network error while loading government history data.</p>';
                    }
                })
                .finally(function () {
                    setLoading(false);
                });
        });
    }

    function initNegotiationRadar() {
        const widget = document.getElementById("negotiation-radar-widget");
        const timelineEl = document.getElementById("history-timeline-list");
        if (!widget) return;

        function parseDate(raw) {
            if (raw == null || String(raw).trim() === "") return null;
            let s = String(raw).trim();
            if (s.endsWith("Z")) s = s.slice(0, -1) + "+00:00";
            const d = new Date(s);
            return Number.isNaN(d.getTime()) ? null : d;
        }

        function formatMoney(amount) {
            return Math.abs(Math.round(amount)).toLocaleString("en-US");
        }

        function formatEventDate(dateStr) {
            const d = parseDate(dateStr);
            if (!d) return String(dateStr || "");
            return d.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
        }

        // Days since first seen and the leverage badge are rendered server-side
        // from first_seen_at (visual review 2026-09-28, IH-09 / DC-3 / ES-3); the
        // "Predictive Local Turnaround" constant and its gradient gauge are gone.

        let history = [];
        try {
            const rawHistory = widget.getAttribute("data-price-history") || "[]";
            history = JSON.parse(rawHistory);
        } catch (_) {
            history = [];
        }
        if (!Array.isArray(history)) history = [];

        history = history
            .filter(function (evt) {
                return evt && typeof evt === "object" && evt.date != null;
            })
            .sort(function (a, b) {
                const da = parseDate(a.date);
                const db = parseDate(b.date);
                if (!da && !db) return 0;
                if (!da) return 1;
                if (!db) return -1;
                return db.getTime() - da.getTime();
            });

        if (!timelineEl) return;

        timelineEl.querySelectorAll(".price-history-empty, .price-history-steps").forEach(function (el) {
            el.remove();
        });

        if (history.length === 0) {
            const empty = document.createElement("p");
            empty.className = "price-history-empty";
            empty.textContent = "No pricing adjustments recorded yet.";
            timelineEl.appendChild(empty);
            return;
        }

        const list = document.createElement("ul");
        list.className = "price-history-steps";

        history.forEach(function (evt, idx) {
            const price = Number(evt.price);
            const older = history[idx + 1];
            const prevPrice = older != null ? Number(older.price) : NaN;
            const li = document.createElement("li");
            li.className = "price-history-step";

            let label = "";
            if (Number.isFinite(price) && Number.isFinite(prevPrice)) {
                const delta = price - prevPrice;
                if (delta < 0) {
                    label =
                        "\u2198 Dropped $" +
                        formatMoney(delta) +
                        " on " +
                        formatEventDate(evt.date);
                } else if (delta > 0) {
                    label =
                        "\u2197 Raised $" +
                        formatMoney(delta) +
                        " on " +
                        formatEventDate(evt.date);
                } else {
                    label = "\u2192 No change on " + formatEventDate(evt.date);
                }
            } else if (Number.isFinite(price)) {
                label = "Listed at $" + formatMoney(price) + " on " + formatEventDate(evt.date);
            } else {
                label = formatEventDate(evt.date);
            }

            li.textContent = label;
            list.appendChild(li);
        });

        timelineEl.appendChild(list);
    }




    function initBuildSheetPrint() {
        const btn = document.getElementById("build-sheet-print-btn");
        const panel = document.getElementById("build-sheet-panel");
        if (!btn || !panel) return;
        btn.addEventListener("click", function () {
            // The build sheet lives inside the Options tab panel, which is
            // [hidden] (and so print-invisible too, regardless of the
            // print-media CSS on the sheet itself) whenever another tab is
            // active. Switch to it first so window.print() has something to
            // render -- the print stylesheet then hides everything else on
            // the page except this one panel.
            const optionsTab = document.querySelector('.car-vdp-tab[data-tab="options"]');
            if (optionsTab && !optionsTab.classList.contains("car-vdp-tab--active")) {
                optionsTab.click();
            }
            window.requestAnimationFrame(function () {
                window.print();
            });
        });
    }

    // Leaflet (147 KB JS + 15 KB CSS + tile requests) used to load with every car
    // page for a map that lives in the hidden Dealership tab. The template now emits
    // the asset URLs as JSON (#car-dealer-map-assets) and this loads them -- stylesheet
    // first, then the scripts in order -- the first time the tab is shown.
    // car_dealer_map.js builds the map as it executes, so by then the panel is visible
    // and Leaflet measures a real container. Same-origin URLs, so CSP 'self' covers
    // the injected tags without a nonce.
    let _dealerMapAssetsPromise = null;
    function ensureDealerMapAssets() {
        if (_dealerMapAssetsPromise) return _dealerMapAssetsPromise;
        const cfgEl = document.getElementById("car-dealer-map-assets");
        if (!cfgEl) return Promise.resolve();
        let cfg = null;
        try {
            cfg = JSON.parse(cfgEl.textContent || "null");
        } catch (_) {
            cfg = null;
        }
        if (!cfg || !Array.isArray(cfg.scripts)) return Promise.resolve();
        if (cfg.icon_base) window.__DS_LEAFLET_ICON_BASE = cfg.icon_base;

        function loadStylesheet(href) {
            return new Promise(function (resolve) {
                const link = document.createElement("link");
                link.rel = "stylesheet";
                link.href = href;
                link.onload = resolve;
                link.onerror = resolve;
                document.head.appendChild(link);
            });
        }
        function loadScript(src) {
            return new Promise(function (resolve, reject) {
                const el = document.createElement("script");
                el.src = src;
                el.async = false;
                el.onload = resolve;
                el.onerror = reject;
                document.head.appendChild(el);
            });
        }

        _dealerMapAssetsPromise = (cfg.css ? loadStylesheet(cfg.css) : Promise.resolve())
            .then(function () {
                return cfg.scripts.reduce(function (chain, src) {
                    return chain.then(function () { return loadScript(src); });
                }, Promise.resolve());
            })
            .catch(function () { /* map stays a blank panel; the address links still work */ });
        return _dealerMapAssetsPromise;
    }

    function initCarVdpTabs() {
        const tablist = document.querySelector(".car-vdp-tabs");
        if (!tablist) return;

        const tabs = tablist.querySelectorAll(".car-vdp-tab");
        const panels = document.querySelectorAll(".car-vdp-panel");
        if (!tabs.length || !panels.length) return;

        const validIds = new Set();
        tabs.forEach(function (t) {
            validIds.add(t.getAttribute("data-tab"));
        });

        function activate(tabId, opts) {
            const pushHash = !(opts && opts.skipHash);
            const target = tabId && validIds.has(tabId) ? tabId : "overview";
            tabs.forEach(function (t) {
                const on = t.getAttribute("data-tab") === target;
                t.classList.toggle("car-vdp-tab--active", on);
                t.setAttribute("aria-selected", on ? "true" : "false");
            });
            panels.forEach(function (p) {
                const on = p.getAttribute("data-panel") === target;
                p.classList.toggle("car-vdp-panel--active", on);
                if (on) {
                    p.removeAttribute("hidden");
                } else {
                    p.setAttribute("hidden", "");
                }
                if (!on) return;
                // Both of these need the panel to be visible first: Leaflet measures its
                // container on resize, and comment threads load lazily on reveal so a
                // shopper who never opens the tab never pays for the request.
                if (target === "dealership") {
                    if (window.__DS_resizeDealerMap) {
                        window.__DS_resizeDealerMap();
                    } else {
                        ensureDealerMapAssets().then(function () {
                            if (window.__DS_resizeDealerMap) window.__DS_resizeDealerMap();
                        });
                    }
                }
                if (window.__DS_loadCommentsIn) {
                    window.__DS_loadCommentsIn(p);
                }
            });
            if (pushHash && target && target !== "overview") {
                try {
                    history.replaceState(null, "", "#" + target);
                } catch (_) {}
            }
        }

        const tabArr = Array.from(tabs);

        tabArr.forEach(function (tab, idx) {
            tab.addEventListener("click", function () {
                activate(tab.getAttribute("data-tab"));
            });
            tab.addEventListener("keydown", function (e) {
                let next = -1;
                if (e.key === "ArrowRight") next = (idx + 1) % tabArr.length;
                if (e.key === "ArrowLeft") next = (idx - 1 + tabArr.length) % tabArr.length;
                if (e.key === "Home") next = 0;
                if (e.key === "End") next = tabArr.length - 1;
                if (next < 0) return;
                e.preventDefault();
                tabArr[next].focus();
                activate(tabArr[next].getAttribute("data-tab"));
            });
        });

        const hash = (window.location.hash || "").replace(/^#/, "");
        if (hash && validIds.has(hash)) {
            activate(hash, { skipHash: true });
        }
    }

    function initCompareTrayResync() {
        if (typeof window.__DS_compareSyncTray === "function") {
            window.__DS_compareSyncTray();
        }
    }

    function initTcoZipResync() {
        window.addEventListener("pageshow", function () {
            CarPage.initTcoIntelligence();
        });
    }

    function deferCarIdle(fn) {
        if (typeof requestIdleCallback === "function") {
            requestIdleCallback(fn, { timeout: 2000 });
        } else {
            window.setTimeout(fn, 1);
        }
    }

    function initCarPageCritical() {
        initCarBackLink();
        initCarShareButton();
        CarPage.initCarGallery();
        initCarVdpTabs();
        initCompareTrayResync();
        CarPage.initCarFinanceCalculator();
    }

    function initCarPageDeferred() {
        initBuildSheetPrint();
        initCarHistoryHighlights();
        initEvBatteryIntelligence();
        CarPage.initTcoIntelligence();
        CarPage.initTcoCostChart();
        initTcoZipResync();
        initDepreciationForecast();
        initNegotiationRadar();
        initVehicleHistoryActions();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            initCarPageCritical();
            deferCarIdle(initCarPageDeferred);
        });
    } else {
        initCarPageCritical();
        deferCarIdle(initCarPageDeferred);
    }
})();
