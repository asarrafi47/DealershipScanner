/**
 * Self-contained car-detail-page helpers. Extracted VERBATIM from car_page.js's
 * closure. Every function here is PURE with respect to that closure — it reads
 * only its arguments (including any DOM element passed as an argument) and the
 * private constants moved alongside it, never the closure's shared mutable
 * state, cached DOM references, document/window queries, or event bindings.
 * Exposed on a single window.CP namespace and loaded BEFORE car_page.js so
 * car_page.js call sites can reference them as CP.<fn>. See car.html for order.
 */
window.CP = window.CP || {};
(function (CP) {
    "use strict";

    const DEP_CURRENT_CALENDAR_YEAR = 2026;
    const DEP_DEFAULT_ANNUAL_MILEAGE = 12000;
    const DEP_ANNUAL_MILEAGE_MIN = 5000;
    const DEP_ANNUAL_MILEAGE_MAX = 40000;
    const DEP_ANNUAL_MILEAGE_STEP = 5000;
    const DEP_RESIDUAL_FLOOR = 0.3;
    const DEP_RESIDUAL_CEILING = 0.7;
    const DEP_COMPLEX_TRIM_KEYWORDS = [
        "summit",
        "trackhawk",
        "plaid",
        "performance",
        "overland",
        "denali",
        "autobiography",
    ];
    const DEP_BASE_TRIM_KEYWORDS = ["laredo", "sr5", "lx", "base", "standard"];

    function parseDepreciationMetadata(card) {
        const priceRaw = parseInt(card.getAttribute("data-car-price"), 10);
        const yearRaw = parseInt(card.getAttribute("data-car-year"), 10);
        const mileageRaw = parseInt(card.getAttribute("data-car-mileage"), 10);
        const genStartRaw = parseInt(card.getAttribute("data-gen-start"), 10);
        const genEndRaw = parseInt(card.getAttribute("data-gen-end"), 10);

        const year = Number.isFinite(yearRaw) && yearRaw > 1980 ? yearRaw : 2020;
        const genStart =
            Number.isFinite(genStartRaw) && genStartRaw > 1980 ? genStartRaw : year;
        const genEnd =
            Number.isFinite(genEndRaw) && genEndRaw >= genStart
                ? genEndRaw
                : genStart + 7;

        return {
            price: Number.isFinite(priceRaw) && priceRaw > 0 ? priceRaw : 20000,
            year: year,
            mileage: Number.isFinite(mileageRaw) && mileageRaw >= 0 ? mileageRaw : 0,
            trim: String(card.getAttribute("data-car-trim") || "")
                .trim()
                .toLowerCase(),
            genStart: genStart,
            genEnd: genEnd,
        };
    }

    function trimMatchesKeyword(trim, keywords) {
        if (!trim) return false;
        return keywords.some(function (kw) {
            return trim.indexOf(kw) >= 0;
        });
    }

    function computeAnnualMileage(meta) {
        const currentAge = Math.max(1, DEP_CURRENT_CALENDAR_YEAR - parseInt(meta.year, 10));
        const derived = parseInt(meta.mileage, 10) / currentAge;
        if (Number.isFinite(derived) && derived >= 2000 && derived <= 35000) {
            return Math.round(derived);
        }
        return DEP_DEFAULT_ANNUAL_MILEAGE;
    }

    function buildDepreciationMileageSteps(startMileage, annualMileage) {
        const steps = [];
        for (let i = 0; i <= 5; i += 1) {
            steps.push(Math.round(startMileage + annualMileage * i));
        }
        return steps;
    }

    function formatDepreciationMileage(miles) {
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

    function snapDepreciationAnnualMileage(miles) {
        const snapped = Math.round(miles / DEP_ANNUAL_MILEAGE_STEP) * DEP_ANNUAL_MILEAGE_STEP;
        return Math.min(
            DEP_ANNUAL_MILEAGE_MAX,
            Math.max(DEP_ANNUAL_MILEAGE_MIN, snapped)
        );
    }

    function buildDepreciationAnnualMileageOptions() {
        const options = [];
        for (
            let mi = DEP_ANNUAL_MILEAGE_MIN;
            mi <= DEP_ANNUAL_MILEAGE_MAX;
            mi += DEP_ANNUAL_MILEAGE_STEP
        ) {
            options.push(mi);
        }
        return options;
    }

    function formatDepreciationAnnualMileageOption(miles) {
        return (miles / 1000).toLocaleString("en-US") + "k mi/yr";
    }

    function computeDepreciationTargetResidual(meta, annualMileage) {
        let targetResidual = 0.5;

        const modelYear = parseInt(meta.year, 10);
        const generationStartYear = parseInt(meta.genStart, 10);
        const generationAge = Math.max(0, modelYear - generationStartYear);
        if (generationAge >= 6) {
            targetResidual -= 0.05;
        }

        const milesPerYear = Number(annualMileage);
        if (milesPerYear < 7000) {
            targetResidual += 0.08;
        }
        if (milesPerYear > 18000) {
            targetResidual -= 0.10;
        }
        if (milesPerYear > 30000) {
            targetResidual -= 0.18;
        }

        if (trimMatchesKeyword(meta.trim, DEP_COMPLEX_TRIM_KEYWORDS)) {
            targetResidual -= 0.07;
        } else if (trimMatchesKeyword(meta.trim, DEP_BASE_TRIM_KEYWORDS)) {
            targetResidual += 0.03;
        }

        return Math.min(
            DEP_RESIDUAL_CEILING,
            Math.max(DEP_RESIDUAL_FLOOR, targetResidual)
        );
    }

    function buildDepreciationTrajectory(price, targetResidual) {
        const values = [];
        for (let currentYearStep = 0; currentYearStep <= 5; currentYearStep += 1) {
            values.push(
                Math.round(price * Math.pow(targetResidual, currentYearStep / 5))
            );
        }
        return values;
    }

    function formatDepreciationCurrency(amount) {
        if (!Number.isFinite(amount)) return "$--";
        return "$" + Math.round(amount).toLocaleString("en-US");
    }

    function formatDepreciationCurrencyShort(amount) {
        if (!Number.isFinite(amount)) return "$--";
        if (amount >= 1000) {
            const thousands = amount / 1000;
            const rounded =
                thousands >= 100
                    ? Math.round(thousands)
                    : parseFloat(thousands.toFixed(thousands < 10 ? 1 : 0));
            return "$" + rounded.toLocaleString("en-US") + "k";
        }
        return formatDepreciationCurrency(amount);
    }

    function evBatteryThermalScaleFactor(makeRaw) {
        const make = String(makeRaw || "").trim().toLowerCase();
        if (make === "nissan" || make === "fiat") {
            return 1.4;
        }
        if (
            make === "tesla" ||
            make === "hyundai" ||
            make === "kia" ||
            make === "porsche"
        ) {
            return 0.85;
        }
        return 1.0;
    }

    function parseDealerRating(raw) {
        if (raw == null || String(raw).trim() === "") return null;
        const n = parseFloat(String(raw));
        if (!Number.isFinite(n) || n < 0 || n > 5) return null;
        return n;
    }

    function parseDealerReviewCount(raw) {
        if (raw == null || String(raw).trim() === "") return null;
        const n = parseInt(String(raw), 10);
        if (!Number.isFinite(n) || n < 0) return null;
        return n;
    }

    function buildReputationStars(rating) {
        const rounded = Math.round(Math.max(0, Math.min(5, rating)));
        return "\u2605".repeat(rounded) + "\u2606".repeat(5 - rounded);
    }

    function formatReviewCount(count) {
        if (count == null) return "";
        const n = count;
        const formatted = n.toLocaleString("en-US");
        return "(" + formatted + " Google review" + (n === 1 ? "" : "s") + ")";
    }

    function hasPremiumHistoryAccess(access) {
        if (!access) return false;
        if (access.show_premium_features) return true;
        if (access.has_paid_access) return true;
        if (access.logged_in && !access.billing_stripe_enabled) return true;
        return false;
    }

    function escHtml(s) {
        return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    CP.parseDepreciationMetadata = parseDepreciationMetadata;
    CP.trimMatchesKeyword = trimMatchesKeyword;
    CP.computeAnnualMileage = computeAnnualMileage;
    CP.buildDepreciationMileageSteps = buildDepreciationMileageSteps;
    CP.formatDepreciationMileage = formatDepreciationMileage;
    CP.snapDepreciationAnnualMileage = snapDepreciationAnnualMileage;
    CP.buildDepreciationAnnualMileageOptions = buildDepreciationAnnualMileageOptions;
    CP.formatDepreciationAnnualMileageOption = formatDepreciationAnnualMileageOption;
    CP.computeDepreciationTargetResidual = computeDepreciationTargetResidual;
    CP.buildDepreciationTrajectory = buildDepreciationTrajectory;
    CP.formatDepreciationCurrency = formatDepreciationCurrency;
    CP.formatDepreciationCurrencyShort = formatDepreciationCurrencyShort;
    CP.evBatteryThermalScaleFactor = evBatteryThermalScaleFactor;
    CP.parseDealerRating = parseDealerRating;
    CP.parseDealerReviewCount = parseDealerReviewCount;
    CP.buildReputationStars = buildReputationStars;
    CP.formatReviewCount = formatReviewCount;
    CP.hasPremiumHistoryAccess = hasPremiumHistoryAccess;
    CP.escHtml = escHtml;
})(window.CP);
