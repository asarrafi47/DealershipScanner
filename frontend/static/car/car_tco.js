/**
 * Car page TCO (total cost of ownership) card: live fuel / electricity rate
 * lookup, the fuel-vs-maintenance split bar, the shopper's own price input,
 * the fill-up estimate and the five-year operating-cost chart.
 *
 * Moved out of car_page.js (2026-10-01 split). The math lives in
 * car_tco_math.js (CP.*), the shared canvas frame in cp_canvas.js; this file
 * owns the DOM and the module state below. car_page.js boots it through
 * window.CarPage.initTcoIntelligence / initTcoCostChart.
 */
window.CarPage = window.CarPage || {};
(function (CarPage, CP) {
    "use strict";

    const {
        normalizeTcoMakeKey,
        isTcoElectricVehicle,
        lookupTcoMaintenanceCost,
        formatTcoCurrency,
        formatTcoLiveFuelLabel,
        formatTcoLiveElectricityLabel,
        formatTcoCustomFuelLabel,
        formatTcoCustomElectricityLabel,
        parseTcoFuelPriceInput,
        parseTcoElectricityPriceInput,
        computeTcoFuelExpense,
        resolveTcoEvEfficiency,
        computeTcoEvFuelExpense,
        formatTcoFillUpCurrency,
        computeTcoFillUpCost,
        resolveTcoAvgMpg,
        resolveTcoTankGallons,
        resolveTcoCarState,
        buildTcoFuelLookupUrl,
        buildTcoMileageSteps,
        formatTcoMileageLabel,
        formatTcoCostCurrencyShort,
        computeTcoAnnualFuelSpend,
        computeTcoAnnualEvFuelSpend,
        buildTcoCumulativeOperationalCosts,
        buildTcoFuelPriceScenarios,
    } = CP;
    const { TCO_ANNUAL_MILES, TCO_FORECAST_YEARS, TCO_FUEL_PRICE_BAND } = CP.TCO;

    // initTcoIntelligence runs again on pageshow (bfcache / ZIP change); the
    // generation counter drops a stale fetch. The render contexts hold what the
    // price inputs need to re-render after the shopper edits a rate.
    let _tcoInitGen = 0;
    let _tcoRenderCtx = null;
    let _tcoEvRenderCtx = null;
    let tcoCostResizeObserver = null;
    let _tcoCostRenderCtx = null;

    // ---- fill-up estimate + split bar ----

    function updateTcoFillUpEstimate(rate, opts) {
        const o = opts || {};
        const block = document.getElementById("tco-fillup-estimate");
        const costEl = document.getElementById("tco-fillup-cost");
        const detailEl = document.getElementById("tco-fillup-detail");
        if (!block || !costEl) return;

        const fillCost = computeTcoFillUpCost(rate, o.tankGallons);
        if (fillCost == null) {
            block.hidden = true;
            costEl.textContent = "$--";
            if (detailEl) detailEl.textContent = "";
            return;
        }

        block.hidden = false;
        costEl.textContent = formatTcoFillUpCurrency(fillCost);
        if (!detailEl) return;

        const parts = [];
        if (Number.isFinite(o.tankGallons) && o.tankGallons > 0) {
            parts.push("~" + o.tankGallons.toFixed(1) + " gal tank");
        }
        const city = o.mpgCity;
        const hwy = o.mpgHighway;
        if (Number.isFinite(city) && Number.isFinite(hwy)) {
            parts.push(
                Math.round(city) + "/" + Math.round(hwy) + " city/hwy MPG avg " + Math.round(o.avgMpg)
            );
        } else if (Number.isFinite(o.avgMpg) && o.avgMpg > 0) {
            parts.push("~" + Math.round(o.avgMpg) + " MPG avg (city + highway)");
        }
        const miles = Number.isFinite(o.avgMpg) && Number.isFinite(o.tankGallons)
            ? Math.round(o.avgMpg * o.tankGallons)
            : null;
        if (miles != null && miles > 0) {
            parts.push("~ " + miles.toLocaleString("en-US") + " mi per tank");
        }
        detailEl.textContent = parts.length ? " · " + parts.join(" · ") : "";
    }

    function renderTcoDashboard(maintenanceCost, fuelCost, fuelBar, maintenanceBar) {
        const totalCost = maintenanceCost + fuelCost;
        const fuelPct = totalCost > 0 ? (fuelCost / totalCost) * 100 : 50;
        const maintenancePct = totalCost > 0 ? (maintenanceCost / totalCost) * 100 : 50;
        fuelBar.style.width = "0%";
        maintenanceBar.style.width = "0%";
        requestAnimationFrame(function () {
            fuelBar.style.width = fuelPct.toFixed(1) + "%";
            maintenanceBar.style.width = maintenancePct.toFixed(1) + "%";
        });
    }

    // ---- five-year operating-cost chart ----

    function updateTcoCostLegend(scenarios) {
        const legend = document.getElementById("tco-cost-legend");
        if (!legend) return;
        legend.innerHTML = "";
        if (!scenarios || !scenarios.length) {
            legend.hidden = true;
            return;
        }
        legend.hidden = false;
        scenarios.forEach(function (scenario) {
            const item = document.createElement("li");
            const swatch = document.createElement("span");
            swatch.className = "tco-cost-legend__swatch";
            swatch.style.backgroundColor = scenario.color;
            item.appendChild(swatch);
            item.appendChild(document.createTextNode(scenario.label));
            legend.appendChild(item);
        });
    }

    /** One cumulative operating-cost series per fuel-price scenario. */
    function buildTcoCostSeries(scenarios, o, isEv) {
        return scenarios.map(function (scenario) {
            const annualFuel = isEv
                ? computeTcoAnnualEvFuelSpend(scenario.rate, o.kwhPer100)
                : computeTcoAnnualFuelSpend(scenario.rate, o.avgMpg);
            return {
                scenario: scenario,
                values: buildTcoCumulativeOperationalCosts(o.maintenanceCost, annualFuel),
            };
        });
    }

    function drawTcoCostSeries(plot, series) {
        const ctx = plot.ctx;
        series.forEach(function (s) {
            const points = s.values.map(function (val, i) {
                return { x: plot.xAt(i), y: plot.yAt(val), val: val };
            });

            ctx.strokeStyle = s.scenario.color;
            ctx.lineWidth = s.scenario.key === "base" ? 2.25 : 1.75;
            ctx.lineJoin = "round";
            ctx.lineCap = "round";
            ctx.beginPath();
            ctx.moveTo(points[0].x, points[0].y);
            for (let i = 1; i < points.length; i += 1) {
                ctx.lineTo(points[i].x, points[i].y);
            }
            ctx.stroke();

            points.forEach(function (pt, i) {
                const radius = s.scenario.key === "base" && (i === 0 || i === 5) ? 4.5 : 3.5;
                ctx.beginPath();
                ctx.arc(pt.x, pt.y, radius, 0, Math.PI * 2);
                ctx.fillStyle = "#ffffff";
                ctx.fill();
                ctx.lineWidth = 1.75;
                ctx.strokeStyle = s.scenario.color;
                ctx.stroke();
            });
        });
    }

    function setTcoCostAriaLabel(canvas, series) {
        function yearFive(key) {
            const s = series.find(function (x) {
                return x.scenario.key === key;
            });
            return s ? s.values[5] : null;
        }
        const baseY5 = yearFive("base");
        const lowY5 = yearFive("low");
        const highY5 = yearFive("high");
        canvas.setAttribute(
            "aria-label",
            "Five-year operating cost from " +
                formatTcoCurrency(0) +
                " now to " +
                formatTcoCurrency(baseY5) +
                " at current fuel price, ranging from " +
                formatTcoCurrency(lowY5) +
                " if fuel is $" +
                TCO_FUEL_PRICE_BAND +
                " lower to " +
                formatTcoCurrency(highY5) +
                " if fuel is $" +
                TCO_FUEL_PRICE_BAND +
                " higher, assuming " +
                formatTcoMileageLabel(TCO_ANNUAL_MILES) +
                " per year"
        );
    }

    function renderTcoCostCurve(opts) {
        const o = opts || {};
        const canvas = document.getElementById("tcoCostCanvas");
        const subtitle = document.getElementById("tco-cost-subtitle");
        const frame = canvas ? canvas.closest(".tco-cost-chart-frame") : null;
        if (!canvas || !frame) return;

        const isEv = !!o.isEv;
        const bandLabel = isEv ? "±$0.10/kWh electricity" : "±$1 fuel";
        if (subtitle) {
            subtitle.textContent =
                "Projected operating cost by year (" + bandLabel + " sensitivity)";
        }

        const scenarios = buildTcoFuelPriceScenarios(o.baseRate, isEv);
        if (!scenarios) {
            frame.hidden = true;
            if (subtitle) subtitle.hidden = true;
            updateTcoCostLegend(null);
            return;
        }
        frame.hidden = false;
        if (subtitle) subtitle.hidden = false;
        updateTcoCostLegend(scenarios);

        const surface = CP.setupHiDpiCanvas(canvas, ".tco-cost-chart-frame");
        if (!surface) {
            requestAnimationFrame(function () {
                renderTcoCostCurve(o);
            });
            return;
        }

        const mileageSteps = buildTcoMileageSteps(o.startMileage || 0, TCO_ANNUAL_MILES);
        const series = buildTcoCostSeries(scenarios, o, isEv);
        const allValues = [];
        series.forEach(function (s) {
            allValues.push.apply(allValues, s.values);
        });
        const minVal = 0;
        const maxVal = Math.max.apply(null, allValues);
        const plot = CP.chartPlotArea(surface, minVal, maxVal - minVal || 1, TCO_FORECAST_YEARS);

        CP.drawChartGrid(plot);
        CP.drawChartYTicks(plot, formatTcoCostCurrencyShort);
        drawTcoCostSeries(plot, series);
        CP.drawChartXLabels(plot, CP.CHART_YEAR_LABELS, mileageSteps.map(formatTcoMileageLabel));
        setTcoCostAriaLabel(canvas, series);
    }

    function initTcoCostChart() {
        const canvas = document.getElementById("tcoCostCanvas");
        const frame = canvas ? canvas.closest(".tco-cost-chart-frame") : null;
        if (!canvas || !frame) return;

        if (frame && typeof ResizeObserver !== "undefined") {
            if (tcoCostResizeObserver) {
                tcoCostResizeObserver.disconnect();
            }
            tcoCostResizeObserver = new ResizeObserver(function () {
                if (_tcoCostRenderCtx) {
                    requestAnimationFrame(function () {
                        renderTcoCostCurve(_tcoCostRenderCtx);
                    });
                }
            });
            tcoCostResizeObserver.observe(frame);
            return;
        }

        window.addEventListener("resize", function () {
            if (_tcoCostRenderCtx) {
                renderTcoCostCurve(_tcoCostRenderCtx);
            }
        });
    }

    function syncTcoCostCurveRender(opts) {
        const card = document.getElementById("tco-intelligence-card");
        const mileageRaw = card ? parseInt(card.getAttribute("data-car-mileage"), 10) : 0;
        const startMileage = Number.isFinite(mileageRaw) && mileageRaw >= 0 ? mileageRaw : 0;
        _tcoCostRenderCtx = {
            maintenanceCost: opts.maintenanceCost,
            baseRate: opts.baseRate,
            avgMpg: opts.avgMpg,
            kwhPer100: opts.kwhPer100,
            isEv: opts.isEv,
            startMileage: startMileage,
        };
        renderTcoCostCurve(_tcoCostRenderCtx);
    }

    // ---- cost figures ----

    function updateTcoDisclaimerLabel(labelEl, rate, opts) {
        if (!labelEl) return;
        const o = opts || {};
        if (o.isCustom && Number.isFinite(rate)) {
            labelEl.textContent = formatTcoCustomFuelLabel(rate);
            return;
        }
        if (Number.isFinite(rate) && o.regionName) {
            labelEl.textContent = formatTcoLiveFuelLabel(
                o.regionName,
                o.fuelTier,
                rate
            );
            return;
        }
        labelEl.textContent = o.carState
            ? "regional fuel averages in " + o.carState + " (EIA sync pending)"
            : "regional fuel averages (EIA sync pending)";
    }

    /** No usable rate: show maintenance alone and hide the chart. */
    function renderTcoMaintenanceOnly(o, curveOpts) {
        o.fuelEl.textContent = "$--";
        o.totalEl.textContent = formatTcoCurrency(o.maintenanceCost);
        renderTcoDashboard(o.maintenanceCost, 0, o.fuelBar, o.maintenanceBar);
        syncTcoCostCurveRender(curveOpts);
    }

    /** Known fuel expense: fill the figures, the split bar and the chart. */
    function renderTcoWithFuelExpense(o, totalFuelExpense, curveOpts) {
        o.fuelEl.textContent = formatTcoCurrency(totalFuelExpense);
        o.totalEl.textContent = formatTcoCurrency(o.maintenanceCost + totalFuelExpense);
        renderTcoDashboard(o.maintenanceCost, totalFuelExpense, o.fuelBar, o.maintenanceBar);
        syncTcoCostCurveRender(curveOpts);
    }

    function renderTcoEvFuelCosts(rate, opts) {
        const o = opts || {};
        const labelEl = o.labelEl;
        if (!o.fuelEl || !o.totalEl || !o.fuelBar || !o.maintenanceBar) return;
        function curve(baseRate) {
            return { maintenanceCost: o.maintenanceCost, baseRate: baseRate, kwhPer100: o.kwhPer100, isEv: true };
        }

        if (!Number.isFinite(rate) || rate <= 0) {
            renderTcoMaintenanceOnly(o, curve(null));
            if (labelEl) {
                labelEl.textContent = o.carState
                    ? "residential EV charging in " + o.carState + " (EIA sync pending)"
                    : "residential EV charging (EIA sync pending)";
            }
            return;
        }

        const totalFuelExpense = computeTcoEvFuelExpense(rate, o.kwhPer100);
        if (totalFuelExpense == null) {
            renderTcoMaintenanceOnly(o, curve(null));
            return;
        }

        renderTcoWithFuelExpense(o, totalFuelExpense, curve(rate));
        if (labelEl) {
            if (o.isCustom) {
                labelEl.textContent = formatTcoCustomElectricityLabel(rate);
            } else {
                labelEl.textContent = formatTcoLiveElectricityLabel(
                    o.electricityRegionName,
                    rate
                );
            }
        }
    }

    function renderTcoFuelCosts(rate, opts) {
        const o = opts || {};
        if (!o.fuelEl || !o.totalEl || !o.fuelBar || !o.maintenanceBar) return;
        function curve(baseRate) {
            return { maintenanceCost: o.maintenanceCost, baseRate: baseRate, avgMpg: o.avgMpg, isEv: false };
        }
        const labelOpts = {
            isCustom: o.isCustom,
            regionName: o.regionName,
            fuelTier: o.fuelTier,
            carState: o.carState,
        };

        updateTcoFillUpEstimate(rate, {
            tankGallons: o.tankGallons,
            avgMpg: o.avgMpg,
            mpgCity: o.mpgCity,
            mpgHighway: o.mpgHighway,
        });

        if (!Number.isFinite(rate) || rate <= 0) {
            renderTcoMaintenanceOnly(o, curve(null));
            updateTcoDisclaimerLabel(o.labelEl, null, labelOpts);
            return;
        }

        const totalFuelExpense = computeTcoFuelExpense(rate, o.avgMpg);
        if (totalFuelExpense == null) {
            renderTcoMaintenanceOnly(o, curve(null));
            return;
        }

        renderTcoWithFuelExpense(o, totalFuelExpense, curve(rate));
        updateTcoDisclaimerLabel(o.labelEl, rate, labelOpts);
    }

    /** Render a fuel car from its saved context with the given {rate, isCustom}. */
    function renderTcoFuelFromCtx(ctx, effective) {
        renderTcoFuelCosts(effective.rate, {
            maintenanceCost: ctx.maintenanceCost,
            avgMpg: ctx.avgMpg,
            tankGallons: ctx.tankGallons,
            mpgCity: ctx.mpgCity,
            mpgHighway: ctx.mpgHighway,
            fuelEl: ctx.fuelEl,
            totalEl: ctx.totalEl,
            fuelBar: ctx.fuelBar,
            maintenanceBar: ctx.maintenanceBar,
            labelEl: ctx.labelEl,
            isCustom: effective.isCustom,
            regionName: ctx.regionName,
            fuelTier: ctx.fuelTier,
            carState: ctx.carState,
        });
    }

    /** Render an EV from its saved context with the given {rate, isCustom}. */
    function renderTcoEvFromCtx(ctx, effective) {
        renderTcoEvFuelCosts(effective.rate, {
            maintenanceCost: ctx.maintenanceCost,
            kwhPer100: ctx.kwhPer100,
            fuelEl: ctx.fuelEl,
            totalEl: ctx.totalEl,
            fuelBar: ctx.fuelBar,
            maintenanceBar: ctx.maintenanceBar,
            labelEl: ctx.labelEl,
            isCustom: effective.isCustom,
            electricityRegionName: ctx.electricityRegionName,
            carState: ctx.carState,
        });
    }

    // ---- shopper-entered fuel / electricity price ----

    function getTcoEffectiveFuelRate(liveRate) {
        const input = document.getElementById("tco-fuel-price-input");
        if (!input || input.dataset.tcoUserEdited !== "1") {
            return { rate: liveRate, isCustom: false };
        }
        const userRate = parseTcoFuelPriceInput(input.value);
        if (userRate != null) return { rate: userRate, isCustom: true };
        return { rate: liveRate, isCustom: false };
    }

    function getTcoEffectiveElectricityRate(liveRate) {
        const input = document.getElementById("tco-electricity-price-input");
        if (!input || input.dataset.tcoUserEdited !== "1") {
            return { rate: liveRate, isCustom: false };
        }
        const userRate = parseTcoElectricityPriceInput(input.value);
        if (userRate != null) return { rate: userRate, isCustom: true };
        return { rate: liveRate, isCustom: false };
    }

    function syncTcoFuelPriceInput(liveRate) {
        const input = document.getElementById("tco-fuel-price-input");
        if (!input) return;
        if (input.dataset.tcoUserEdited === "1") return;
        if (Number.isFinite(liveRate) && liveRate > 0) {
            input.value = liveRate.toFixed(2);
        } else {
            input.value = "";
        }
    }

    function syncTcoElectricityPriceInput(liveRate) {
        const input = document.getElementById("tco-electricity-price-input");
        if (!input) return;
        if (input.dataset.tcoUserEdited === "1") return;
        if (Number.isFinite(liveRate) && liveRate > 0) {
            input.value = liveRate.toFixed(3);
        } else {
            input.value = "";
        }
    }

    function bindTcoFuelPriceInput() {
        const input = document.getElementById("tco-fuel-price-input");
        if (!input || input.dataset.tcoFuelBound === "1") return;
        input.dataset.tcoFuelBound = "1";

        function applyFromInput() {
            const ctx = _tcoRenderCtx;
            if (!ctx) return;
            input.dataset.tcoUserEdited = "1";
            const userRate = parseTcoFuelPriceInput(input.value);
            const effective =
                userRate != null
                    ? { rate: userRate, isCustom: true }
                    : getTcoEffectiveFuelRate(ctx.liveRate);
            renderTcoFuelFromCtx(ctx, effective);
        }

        input.addEventListener("input", applyFromInput);
        input.addEventListener("change", applyFromInput);
        input.addEventListener("blur", function () {
            const ctx = _tcoRenderCtx;
            if (!ctx) return;
            const userRate = parseTcoFuelPriceInput(input.value);
            if (userRate != null) return;
            delete input.dataset.tcoUserEdited;
            syncTcoFuelPriceInput(ctx.liveRate);
            renderTcoFuelFromCtx(ctx, getTcoEffectiveFuelRate(ctx.liveRate));
        });
    }

    function bindTcoElectricityPriceInput() {
        const input = document.getElementById("tco-electricity-price-input");
        if (!input || input.dataset.tcoElectricityBound === "1") return;
        input.dataset.tcoElectricityBound = "1";

        function applyFromInput() {
            const ctx = _tcoEvRenderCtx;
            if (!ctx) return;
            input.dataset.tcoUserEdited = "1";
            const userRate = parseTcoElectricityPriceInput(input.value);
            const effective =
                userRate != null
                    ? { rate: userRate, isCustom: true }
                    : getTcoEffectiveElectricityRate(ctx.liveElectricityRate);
            renderTcoEvFromCtx(ctx, effective);
        }

        input.addEventListener("input", applyFromInput);
        input.addEventListener("change", applyFromInput);
        input.addEventListener("blur", function () {
            const ctx = _tcoEvRenderCtx;
            if (!ctx) return;
            const userRate = parseTcoElectricityPriceInput(input.value);
            if (userRate != null) return;
            delete input.dataset.tcoUserEdited;
            syncTcoElectricityPriceInput(ctx.liveElectricityRate);
            renderTcoEvFromCtx(ctx, getTcoEffectiveElectricityRate(ctx.liveElectricityRate));
        });
    }

    // ---- boot ----

    function getTcoCostElements() {
        const els = {
            maintenanceEl: document.getElementById("tco-maintenance-cost"),
            fuelEl: document.getElementById("tco-fuel-cost"),
            totalEl: document.getElementById("tco-total-combined"),
            fuelBar: document.getElementById("tco-bar-fuel"),
            maintenanceBar: document.getElementById("tco-bar-maintenance"),
            labelEl: document.getElementById("tco-dynamic-state-label"),
        };
        if (!els.maintenanceEl || !els.fuelEl || !els.totalEl || !els.fuelBar || !els.maintenanceBar) return null;
        return els;
    }

    /** Everything the card's data-* attributes say about this car's running costs. */
    function readTcoCardInputs(card) {
        const mpgCityRaw = parseFloat(card.getAttribute("data-car-mpg-city"));
        const mpgHwyRaw = parseFloat(card.getAttribute("data-car-mpg-highway"));
        return {
            makeKey: normalizeTcoMakeKey(card.getAttribute("data-car-make")),
            avgMpg: resolveTcoAvgMpg(card),
            tankGallons: resolveTcoTankGallons(card),
            mpgCity: Number.isFinite(mpgCityRaw) && mpgCityRaw > 0 ? mpgCityRaw : null,
            mpgHighway: Number.isFinite(mpgHwyRaw) && mpgHwyRaw > 0 ? mpgHwyRaw : null,
            carState: resolveTcoCarState(card),
            fuelTier: (card.getAttribute("data-fuel-tier") || "regular").trim().toLowerCase(),
        };
    }

    /**
     * Live regional rates from /api/fuel/lookup. Resolves null when a newer
     * initTcoIntelligence started meanwhile (gen is stale); any fetch or parse
     * failure leaves the rates null.
     */
    async function fetchTcoLiveRates(fuelTier, carState, gen) {
        const live = { rate: null, electricityRate: null, regionName: null, electricityRegionName: null };
        try {
            const resp = await fetch(buildTcoFuelLookupUrl(fuelTier, carState), {
                credentials: "same-origin",
            });
            if (gen !== _tcoInitGen) return null;
            if (resp.ok) {
                const data = await resp.json();
                live.rate = parseFloat(data.rate);
                live.electricityRate = parseFloat(data.electricity_rate);
                live.regionName = data.region_name;
                live.electricityRegionName = data.electricity_region_name || data.region_name;
            }
        } catch (_err) {
            if (gen !== _tcoInitGen) return null;
            live.rate = null;
            live.electricityRate = null;
        }
        return live;
    }

    function startTcoEvRender(card, maintenanceCost, inputs, live, els) {
        _tcoEvRenderCtx = {
            maintenanceCost: maintenanceCost,
            kwhPer100: resolveTcoEvEfficiency(card),
            liveElectricityRate: live.electricityRate,
            electricityRegionName: live.electricityRegionName,
            carState: inputs.carState,
            fuelEl: els.fuelEl,
            totalEl: els.totalEl,
            fuelBar: els.fuelBar,
            maintenanceBar: els.maintenanceBar,
            labelEl: els.labelEl,
        };
        syncTcoElectricityPriceInput(live.electricityRate);
        renderTcoEvFromCtx(_tcoEvRenderCtx, getTcoEffectiveElectricityRate(live.electricityRate));
    }

    function startTcoFuelRender(maintenanceCost, inputs, live, els) {
        _tcoRenderCtx = {
            maintenanceCost: maintenanceCost,
            avgMpg: inputs.avgMpg,
            tankGallons: inputs.tankGallons,
            mpgCity: inputs.mpgCity,
            mpgHighway: inputs.mpgHighway,
            liveRate: live.rate,
            regionName: live.regionName,
            fuelTier: inputs.fuelTier,
            carState: inputs.carState,
            fuelEl: els.fuelEl,
            totalEl: els.totalEl,
            fuelBar: els.fuelBar,
            maintenanceBar: els.maintenanceBar,
            labelEl: els.labelEl,
        };
        syncTcoFuelPriceInput(live.rate);
        renderTcoFuelFromCtx(_tcoRenderCtx, getTcoEffectiveFuelRate(live.rate));
    }

    async function initTcoIntelligence() {
        const card = document.getElementById("tco-intelligence-card");
        if (!card) return;
        const gen = ++_tcoInitGen;

        const els = getTcoCostElements();
        if (!els) return;
        const inputs = readTcoCardInputs(card);

        const maintenanceCost = lookupTcoMaintenanceCost(inputs.makeKey);
        els.maintenanceEl.textContent = formatTcoCurrency(maintenanceCost);

        bindTcoFuelPriceInput();
        bindTcoElectricityPriceInput();

        const live = await fetchTcoLiveRates(inputs.fuelTier, inputs.carState, gen);
        if (!live || gen !== _tcoInitGen) return;

        if (isTcoElectricVehicle(card)) {
            startTcoEvRender(card, maintenanceCost, inputs, live, els);
            return;
        }
        startTcoFuelRender(maintenanceCost, inputs, live, els);
    }

    CarPage.initTcoIntelligence = initTcoIntelligence;
    CarPage.initTcoCostChart = initTcoCostChart;
})(window.CarPage, window.CP);
