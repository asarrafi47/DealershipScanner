/**
 * Total-cost-of-ownership math for the car page: maintenance by make, fuel and
 * electricity spend over TCO_TOTAL_MILES, fill-up cost, the five-year cumulative
 * operating-cost series and the +/- fuel-price scenarios the chart draws.
 *
 * Moved VERBATIM out of car_page.js (2026-10-01 split). Every function is pure
 * with respect to page state: it reads only its arguments (a card element is
 * read through getAttribute only) and the constants below; the currency
 * formatters call window.DS.formatUsd (ds_core.js) at call time. No DOM work at
 * load time, so node tests load it in a vm (backend/tests/js/car_tco_math.test.js).
 * Exposed on window.CP; the constants on CP.TCO. car_tco.js does the DOM side.
 */
window.CP = window.CP || {};
(function (CP) {
    "use strict";

    const TCO_MAINTENANCE_BY_MAKE = {
        "land rover": 4250,
        porsche: 4000,
        "mercedes-benz": 2850,
        mercedes: 2850,
        audi: 1900,
        bmw: 1700,
        subaru: 1700,
        ford: 3296,
        chevrolet: 3082,
        gmc: 3081,
        honda: 2185,
        toyota: 1814,
        tesla: 1000,
    };

    const TCO_MAINTENANCE_DEFAULT = 2200;
    const TCO_TOTAL_MILES = 60000;
    const TCO_ANNUAL_MILES = 12000;
    const TCO_FORECAST_YEARS = 5;
    const TCO_FUEL_PRICE_BAND = 1;
    const TCO_EV_RATE_BAND = 0.1;
    const TCO_DEFAULT_EV_KWH_PER_100 = 33.7;

    function normalizeTcoMakeKey(raw) {
        return String(raw || "")
            .trim()
            .toLowerCase()
            .replace(/\s+/g, " ");
    }

    function isTcoElectricVehicle(card) {
        if (!card) return false;
        if (card.getAttribute("data-is-ev") === "1") return true;
        const fuel = String(card.getAttribute("data-car-fuel-type") || "").toLowerCase();
        return fuel === "electric" || fuel === "ev";
    }

    function lookupTcoMaintenanceCost(makeKey) {
        if (TCO_MAINTENANCE_BY_MAKE[makeKey] != null) return TCO_MAINTENANCE_BY_MAKE[makeKey];
        const compact = makeKey.replace(/[\s-]+/g, "");
        const keys = Object.keys(TCO_MAINTENANCE_BY_MAKE);
        for (let i = 0; i < keys.length; i += 1) {
            const k = keys[i];
            if (k.replace(/[\s-]+/g, "") === compact) return TCO_MAINTENANCE_BY_MAKE[k];
        }
        return TCO_MAINTENANCE_DEFAULT;
    }

    function formatTcoCurrency(amount) {
        return window.DS.formatUsd(amount, { strict: true });
    }

    function formatTcoFuelTierLabel(fuelTier) {
        const tier = String(fuelTier || "regular").trim().toLowerCase();
        if (tier === "mid" || tier === "midgrade" || tier === "mid-grade") return "Mid-Grade";
        if (tier === "diesel") return "Diesel";
        if (tier === "premium") return "Premium";
        return "Regular";
    }

    function formatTcoLiveFuelLabel(regionName, fuelTier, rate) {
        const tierLabel = formatTcoFuelTierLabel(fuelTier);
        const region = String(regionName || "").trim() || "Regional";
        const price = Number.isFinite(rate) ? rate.toFixed(2) : "--";
        return region + " " + tierLabel + " ($" + price + "/gal from EIA live sync)";
    }

    function formatTcoLiveElectricityLabel(regionName, rate) {
        const region = String(regionName || "").trim() || "Regional";
        const price = Number.isFinite(rate) ? rate.toFixed(3) : "--";
        return region + " residential electricity ($" + price + "/kWh from EIA live sync)";
    }

    function formatTcoCustomFuelLabel(rate) {
        const price = Number.isFinite(rate) ? rate.toFixed(2) : "--";
        return "your fuel price estimate ($" + price + "/gal)";
    }

    function formatTcoCustomElectricityLabel(rate) {
        const price = Number.isFinite(rate) ? rate.toFixed(3) : "--";
        return "your electricity rate estimate ($" + price + "/kWh)";
    }

    function parseTcoFuelPriceInput(raw) {
        const n = parseFloat(String(raw || "").trim());
        if (!Number.isFinite(n) || n <= 0 || n > 20) return null;
        return n;
    }

    function parseTcoElectricityPriceInput(raw) {
        const n = parseFloat(String(raw || "").trim());
        if (!Number.isFinite(n) || n <= 0 || n > 2) return null;
        return n;
    }

    function computeTcoFuelExpense(rate, avgMpg) {
        if (!Number.isFinite(rate) || rate <= 0) return null;
        const mpg = Number.isFinite(avgMpg) && avgMpg > 0 ? avgMpg : 25;
        return Math.round((TCO_TOTAL_MILES / mpg) * rate);
    }

    function resolveTcoEvEfficiency(card) {
        const raw = parseFloat(card.getAttribute("data-ev-efficiency"));
        if (Number.isFinite(raw) && raw > 0) return raw;
        const avgMpg = resolveTcoAvgMpg(card);
        if (Number.isFinite(avgMpg) && avgMpg > 0) {
            return Math.round(((3370 / avgMpg) + Number.EPSILON) * 10) / 10;
        }
        return TCO_DEFAULT_EV_KWH_PER_100;
    }

    function computeTcoEvFuelExpense(rate, kwhPer100) {
        if (!Number.isFinite(rate) || rate <= 0) return null;
        const intensity = Number.isFinite(kwhPer100) && kwhPer100 > 0 ? kwhPer100 : TCO_DEFAULT_EV_KWH_PER_100;
        return Math.round((TCO_TOTAL_MILES / 100) * intensity * rate);
    }

    function formatTcoFillUpCurrency(amount) {
        return window.DS.formatUsd(amount, { strict: true, cents: true });
    }

    function computeTcoFillUpCost(rate, tankGallons) {
        if (!Number.isFinite(rate) || rate <= 0) return null;
        if (!Number.isFinite(tankGallons) || tankGallons <= 0) return null;
        return Math.round(tankGallons * rate * 100) / 100;
    }

    function resolveTcoAvgMpg(card) {
        const cityRaw = parseFloat(card.getAttribute("data-car-mpg-city"));
        const hwyRaw = parseFloat(card.getAttribute("data-car-mpg-highway"));
        const city = Number.isFinite(cityRaw) && cityRaw > 0 ? cityRaw : null;
        const hwy = Number.isFinite(hwyRaw) && hwyRaw > 0 ? hwyRaw : null;
        if (city != null && hwy != null) return (city + hwy) / 2;
        if (city != null) return city;
        if (hwy != null) return hwy;
        const mpgRaw = parseFloat(card.getAttribute("data-car-mpg"));
        return Number.isFinite(mpgRaw) && mpgRaw > 0 ? mpgRaw : 25;
    }

    function resolveTcoTankGallons(card) {
        const raw = parseFloat(card.getAttribute("data-fuel-tank-gal"));
        return Number.isFinite(raw) && raw > 0 ? raw : null;
    }

    function resolveTcoCarState(card) {
        const raw = String(card.getAttribute("data-car-state") || "NC").trim().toUpperCase();
        return /^[A-Z]{2}$/.test(raw) ? raw : "NC";
    }

    function buildTcoFuelLookupUrl(fuelTier, carState) {
        const qs = new URLSearchParams();
        qs.set("fuel_tier", fuelTier);
        qs.set("state", carState || "NC");
        return "/api/fuel/lookup?" + qs.toString();
    }

    function buildTcoMileageSteps(startMileage, annualMileage) {
        const steps = [];
        for (let i = 0; i <= TCO_FORECAST_YEARS; i += 1) {
            steps.push(Math.round(startMileage + annualMileage * i));
        }
        return steps;
    }

    function formatTcoMileageLabel(miles) {
        if (!Number.isFinite(miles)) return "--";
        if (miles >= 1000) {
            const thousands = miles / 1000;
            const rounded =
                thousands >= 100
                    ? Math.round(thousands)
                    : parseFloat(thousands.toFixed(thousands < 10 ? 1 : 0));
            return rounded.toLocaleString("en-US") + "k mi";
        }
        return miles.toLocaleString("en-US") + " mi";
    }

    function formatTcoCostCurrencyShort(amount) {
        return window.DS.formatUsd(amount, { strict: true, short: true });
    }

    function computeTcoAnnualFuelSpend(rate, avgMpg) {
        const fuelCost = computeTcoFuelExpense(rate, avgMpg);
        if (fuelCost == null) return null;
        return Math.round(fuelCost / TCO_FORECAST_YEARS);
    }

    function computeTcoAnnualEvFuelSpend(rate, kwhPer100) {
        const fuelCost = computeTcoEvFuelExpense(rate, kwhPer100);
        if (fuelCost == null) return null;
        return Math.round(fuelCost / TCO_FORECAST_YEARS);
    }

    function buildTcoCumulativeOperationalCosts(maintenanceCost, annualFuelSpend) {
        const annualMaintenance = Math.round(maintenanceCost / TCO_FORECAST_YEARS);
        const annualFuel = Number.isFinite(annualFuelSpend) && annualFuelSpend >= 0 ? annualFuelSpend : 0;
        const annualTotal = annualMaintenance + annualFuel;
        const values = [];
        for (let year = 0; year <= TCO_FORECAST_YEARS; year += 1) {
            values.push(Math.round(annualTotal * year));
        }
        return values;
    }

    function buildTcoFuelPriceScenarios(baseRate, isEv) {
        if (!Number.isFinite(baseRate) || baseRate <= 0) return null;
        const band = isEv ? TCO_EV_RATE_BAND : TCO_FUEL_PRICE_BAND;
        const low = Math.max(isEv ? 0.01 : 0.5, baseRate - band);
        const high = baseRate + band;
        const unit = isEv ? "/kWh" : "/gal";
        const fmt = isEv
            ? function (r) {
                  return "$" + r.toFixed(2) + unit;
              }
            : function (r) {
                  return "$" + r.toFixed(2) + unit;
              };
        return [
            { key: "low", rate: low, color: "#059669", label: fmt(low) + " (-$" + band.toFixed(isEv ? 2 : 0) + ")" },
            { key: "base", rate: baseRate, color: "#2563eb", label: fmt(baseRate) + " (current)" },
            { key: "high", rate: high, color: "#dc2626", label: fmt(high) + " (+$" + band.toFixed(isEv ? 2 : 0) + ")" },
        ];
    }

    CP.normalizeTcoMakeKey = normalizeTcoMakeKey;
    CP.isTcoElectricVehicle = isTcoElectricVehicle;
    CP.lookupTcoMaintenanceCost = lookupTcoMaintenanceCost;
    CP.formatTcoCurrency = formatTcoCurrency;
    CP.formatTcoFuelTierLabel = formatTcoFuelTierLabel;
    CP.formatTcoLiveFuelLabel = formatTcoLiveFuelLabel;
    CP.formatTcoLiveElectricityLabel = formatTcoLiveElectricityLabel;
    CP.formatTcoCustomFuelLabel = formatTcoCustomFuelLabel;
    CP.formatTcoCustomElectricityLabel = formatTcoCustomElectricityLabel;
    CP.parseTcoFuelPriceInput = parseTcoFuelPriceInput;
    CP.parseTcoElectricityPriceInput = parseTcoElectricityPriceInput;
    CP.computeTcoFuelExpense = computeTcoFuelExpense;
    CP.resolveTcoEvEfficiency = resolveTcoEvEfficiency;
    CP.computeTcoEvFuelExpense = computeTcoEvFuelExpense;
    CP.formatTcoFillUpCurrency = formatTcoFillUpCurrency;
    CP.computeTcoFillUpCost = computeTcoFillUpCost;
    CP.resolveTcoAvgMpg = resolveTcoAvgMpg;
    CP.resolveTcoTankGallons = resolveTcoTankGallons;
    CP.resolveTcoCarState = resolveTcoCarState;
    CP.buildTcoFuelLookupUrl = buildTcoFuelLookupUrl;
    CP.buildTcoMileageSteps = buildTcoMileageSteps;
    CP.formatTcoMileageLabel = formatTcoMileageLabel;
    CP.formatTcoCostCurrencyShort = formatTcoCostCurrencyShort;
    CP.computeTcoAnnualFuelSpend = computeTcoAnnualFuelSpend;
    CP.computeTcoAnnualEvFuelSpend = computeTcoAnnualEvFuelSpend;
    CP.buildTcoCumulativeOperationalCosts = buildTcoCumulativeOperationalCosts;
    CP.buildTcoFuelPriceScenarios = buildTcoFuelPriceScenarios;
    CP.TCO = Object.freeze({
        TCO_MAINTENANCE_BY_MAKE: TCO_MAINTENANCE_BY_MAKE,
        TCO_MAINTENANCE_DEFAULT: TCO_MAINTENANCE_DEFAULT,
        TCO_TOTAL_MILES: TCO_TOTAL_MILES,
        TCO_ANNUAL_MILES: TCO_ANNUAL_MILES,
        TCO_FORECAST_YEARS: TCO_FORECAST_YEARS,
        TCO_FUEL_PRICE_BAND: TCO_FUEL_PRICE_BAND,
        TCO_EV_RATE_BAND: TCO_EV_RATE_BAND,
        TCO_DEFAULT_EV_KWH_PER_100: TCO_DEFAULT_EV_KWH_PER_100,
    });
})(window.CP);
