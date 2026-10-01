/**
 * Car page finance calculator (estimated monthly payment) and the animated
 * "customize" disclosure around it.
 *
 * Moved out of car_page.js (2026-10-01 split; it was one 351-line function).
 * Two parts:
 *   1. Pure payment math on window.CP (CP.computeFinanceMonthlyPayment and the
 *      input parsers) -- no DOM, unit-tested in backend/tests/js/car_finance.test.js.
 *   2. DOM binding (window.CarPage.initCarFinanceCalculator): reads the inputs,
 *      remembers them in localStorage, looks up a sales-tax estimate by ZIP.
 */
window.CP = window.CP || {};
window.CarPage = window.CarPage || {};

(function (CP) {
    "use strict";

    const FINANCE_ANNUAL_RATES = {
        excellent: 0.065,
        good: 0.08,
        fair: 0.115,
        subprime: 0.15,
    };

    // 7% tax / $500 fees are placeholder US-average estimates, not this
    // car's real jurisdiction -- both fields stay user-editable, and
    // entering a ZIP tries to replace the tax rate with a real (if still
    // approximate) per-state figure from /api/tax-rate/lookup.
    const FINANCE_DEFAULT_TAX_RATE_PCT = 7;
    const FINANCE_DEFAULT_FEES = 500;

    const FINANCE_CONDITION_MULTIPLIERS = {
        clean: 1,
        fair: 0.85,
        rough: 0.6,
    };

    /** Down payment / trade value: blank or invalid -> 0, capped at max. */
    function parseFinanceAmount(raw, max) {
        const trimmed = String(raw == null ? "" : raw).trim();
        if (trimmed === "") return 0;
        const n = parseFloat(trimmed);
        if (!Number.isFinite(n) || n < 0) return 0;
        return Math.min(n, max);
    }

    function parseFinanceTerm(raw) {
        const n = parseInt(String(raw || ""), 10);
        return Number.isFinite(n) && n > 0 ? n : null;
    }

    function parseFinanceTaxRatePct(raw) {
        const trimmed = String(raw == null ? "" : raw).trim();
        if (trimmed === "") return FINANCE_DEFAULT_TAX_RATE_PCT;
        const n = parseFloat(trimmed);
        if (!Number.isFinite(n) || n < 0) return FINANCE_DEFAULT_TAX_RATE_PCT;
        return Math.min(n, 25);
    }

    function parseFinanceFees(raw) {
        const trimmed = String(raw == null ? "" : raw).trim();
        if (trimmed === "") return FINANCE_DEFAULT_FEES;
        const n = parseFloat(trimmed);
        if (!Number.isFinite(n) || n < 0) return FINANCE_DEFAULT_FEES;
        return Math.min(n, 10000);
    }

    function financeConditionMultiplier(conditionKey) {
        return FINANCE_CONDITION_MULTIPLIERS[conditionKey] != null
            ? FINANCE_CONDITION_MULTIPLIERS[conditionKey]
            : FINANCE_CONDITION_MULTIPLIERS.fair;
    }

    /**
     * Trade-in equity: 0 without a trade; else (value + $1,000 high-demand bonus)
     * times the condition multiplier, rounded. o: {hasTradeIn, tradeValue (raw),
     * maxTradeValue, highDemand, condition}.
     */
    function computeFinanceTradeInEquity(o) {
        if (!o.hasTradeIn) return 0;
        let base = parseFinanceAmount(o.tradeValue, o.maxTradeValue);
        if (o.highDemand) {
            base += 1000;
        }
        return Math.round(base * financeConditionMultiplier(o.condition));
    }

    /**
     * Amortized monthly payment, or null when the term is not a positive month
     * count. o: {vehiclePrice, downPayment (parsed), tier, term (raw),
     * tradeInValue, taxRatePct (raw), fees (raw)}. Tax applies to the full price;
     * a non-positive principal is 0.
     */
    function computeFinanceMonthlyPayment(o) {
        const tier = o.tier;
        const annualRate = FINANCE_ANNUAL_RATES[tier] != null ? FINANCE_ANNUAL_RATES[tier] : FINANCE_ANNUAL_RATES.good;
        const termMonths = parseFinanceTerm(o.term);
        if (termMonths == null) return null;

        const taxRate = parseFinanceTaxRatePct(o.taxRatePct) / 100;
        const fees = parseFinanceFees(o.fees);
        const principal = o.vehiclePrice * (1 + taxRate) + fees - o.downPayment - o.tradeInValue;

        if (principal <= 0) return 0;
        if (annualRate <= 0) return principal / termMonths;

        const monthlyRate = annualRate / 12;
        const factor = Math.pow(1 + monthlyRate, termMonths);
        return (principal * (monthlyRate * factor)) / (factor - 1);
    }

    function formatFinancePayment(amount) {
        if (!Number.isFinite(amount)) return "--";
        return Math.round(amount).toLocaleString("en-US");
    }

    /** The text the calculator shows for a computeFinanceMonthlyPayment result. */
    function financePaymentText(payment) {
        if (payment == null) return "--";
        if (payment === 0) return "0";
        return formatFinancePayment(payment);
    }

    CP.FINANCE_ANNUAL_RATES = Object.freeze(Object.assign({}, FINANCE_ANNUAL_RATES));
    CP.FINANCE_DEFAULT_TAX_RATE_PCT = FINANCE_DEFAULT_TAX_RATE_PCT;
    CP.FINANCE_DEFAULT_FEES = FINANCE_DEFAULT_FEES;
    CP.parseFinanceAmount = parseFinanceAmount;
    CP.parseFinanceTerm = parseFinanceTerm;
    CP.parseFinanceTaxRatePct = parseFinanceTaxRatePct;
    CP.parseFinanceFees = parseFinanceFees;
    CP.financeConditionMultiplier = financeConditionMultiplier;
    CP.computeFinanceTradeInEquity = computeFinanceTradeInEquity;
    CP.computeFinanceMonthlyPayment = computeFinanceMonthlyPayment;
    CP.formatFinancePayment = formatFinancePayment;
    CP.financePaymentText = financePaymentText;
})(window.CP);

(function (CarPage, CP) {
    "use strict";

    const STORAGE_KEYS = {
        downPayment: "pref_down_payment",
        creditTier: "pref_credit_tier",
        term: "pref_term",
        hasTradeIn: "pref_has_trade_in",
        tradeValue: "pref_trade_value",
        zip: "pref_finance_zip",
        taxRate: "pref_finance_tax_rate",
        taxRateManual: "pref_finance_tax_rate_manual",
        fees: "pref_finance_fees",
    };

    const VALID_TIERS = Object.keys(CP.FINANCE_ANNUAL_RATES);
    const VALID_TERMS = ["36", "48", "60", "72"];
    const CAR_FINANCE_DETAILS_TRANSITION_MS = 420;

    function readStorage(key) {
        try {
            return localStorage.getItem(key);
        } catch (_) {
            return null;
        }
    }

    function writeStorage(key, value) {
        try {
            localStorage.setItem(key, value);
        } catch (_) {}
    }

    /** The calculator's inputs, or null when any of them is missing. */
    function getFinanceElements() {
        const els = {
            displayEl: document.getElementById("display-monthly-payment"),
            downPaymentEl: document.getElementById("calc-down-payment"),
            creditTierEl: document.getElementById("calc-credit-tier"),
            termEl: document.getElementById("calc-term"),
            tradeToggleNo: document.getElementById("trade-in-toggle-no"),
            tradeToggleYes: document.getElementById("trade-in-toggle-yes"),
            tradePanelEl: document.getElementById("trade-in-inputs-panel"),
            tradeConditionEl: document.getElementById("trade-vehicle-condition"),
            tradeValueEl: document.getElementById("calc-trade-value"),
            tradeHighDemandEl: document.getElementById("trade-high-demand-bonus"),
            zipEl: document.getElementById("calc-zip"),
            taxRateEl: document.getElementById("calc-tax-rate"),
            taxRateHintEl: document.getElementById("calc-tax-rate-hint"),
            feesEl: document.getElementById("calc-fees"),
        };
        if (!els.displayEl || !els.downPaymentEl || !els.creditTierEl || !els.termEl) return null;
        if (
            !els.tradeToggleNo ||
            !els.tradeToggleYes ||
            !els.tradePanelEl ||
            !els.tradeConditionEl ||
            !els.tradeValueEl ||
            !els.tradeHighDemandEl
        )
            return null;
        if (!els.zipEl || !els.taxRateEl || !els.feesEl) return null;
        return els;
    }

    /**
     * One calculator: its elements plus the mutable state the handlers share.
     * taxRateManuallySet is true once the tax-rate field holds a value the user
     * typed (or one restored from a previous visit) rather than the plain 7%
     * default -- a completed ZIP lookup only overwrites the field while this is
     * false, so it never clobbers a rate someone already dialed in.
     */
    function createFinanceCalc(vehiclePrice, els) {
        const calc = {
            els: els,
            vehiclePrice: vehiclePrice,
            maxDownPayment: Math.floor(vehiclePrice),
            maxTradeValue: Math.floor(vehiclePrice),
            hasTradeIn: false,
            taxRateManuallySet: false,
            zipLookupToken: 0,
        };
        els.downPaymentEl.max = String(calc.maxDownPayment);
        els.tradeValueEl.max = String(calc.maxTradeValue);
        return calc;
    }

    function parseDownPayment(calc, raw) {
        return CP.parseFinanceAmount(raw, calc.maxDownPayment);
    }

    function parseTradeValue(calc, raw) {
        return CP.parseFinanceAmount(raw, calc.maxTradeValue);
    }

    function renderMonthlyPayment(calc) {
        const els = calc.els;
        const payment = CP.computeFinanceMonthlyPayment({
            vehiclePrice: calc.vehiclePrice,
            downPayment: parseDownPayment(calc, els.downPaymentEl.value),
            tier: els.creditTierEl.value,
            term: els.termEl.value,
            tradeInValue: CP.computeFinanceTradeInEquity({
                hasTradeIn: calc.hasTradeIn,
                tradeValue: els.tradeValueEl.value,
                maxTradeValue: calc.maxTradeValue,
                highDemand: els.tradeHighDemandEl.checked,
                condition: els.tradeConditionEl.value,
            }),
            taxRatePct: els.taxRateEl.value,
            fees: els.feesEl.value,
        });
        els.displayEl.textContent = CP.financePaymentText(payment);
    }

    function persistPreferences(calc) {
        const els = calc.els;
        writeStorage(STORAGE_KEYS.downPayment, els.downPaymentEl.value);
        writeStorage(STORAGE_KEYS.creditTier, els.creditTierEl.value);
        writeStorage(STORAGE_KEYS.term, els.termEl.value);
        writeStorage(STORAGE_KEYS.hasTradeIn, calc.hasTradeIn ? "1" : "0");
        writeStorage(STORAGE_KEYS.tradeValue, els.tradeValueEl.value);
        writeStorage(STORAGE_KEYS.zip, els.zipEl.value);
        writeStorage(STORAGE_KEYS.taxRate, els.taxRateEl.value);
        writeStorage(STORAGE_KEYS.fees, els.feesEl.value);
    }

    function setTradeInPanelOpen(calc, open) {
        const els = calc.els;
        calc.hasTradeIn = !!open;
        const hasTradeIn = calc.hasTradeIn;
        els.tradeToggleNo.classList.toggle("car-finance-toggle-btn--active", !hasTradeIn);
        els.tradeToggleYes.classList.toggle("car-finance-toggle-btn--active", hasTradeIn);
        els.tradeToggleNo.setAttribute("aria-pressed", hasTradeIn ? "false" : "true");
        els.tradeToggleYes.setAttribute("aria-pressed", hasTradeIn ? "true" : "false");
        if (hasTradeIn) {
            els.tradePanelEl.removeAttribute("hidden");
            requestAnimationFrame(function () {
                els.tradePanelEl.classList.add("car-finance-trade-panel--open");
            });
        } else {
            els.tradePanelEl.classList.remove("car-finance-trade-panel--open");
            els.tradePanelEl.setAttribute("hidden", "");
        }
    }

    function lookupTaxRateForZip(calc, zip) {
        const els = calc.els;
        const taxRateHintEl = els.taxRateHintEl;
        const token = ++calc.zipLookupToken;
        if (taxRateHintEl) taxRateHintEl.textContent = "(looking up your area…)";
        fetch("/api/tax-rate/lookup?zip=" + encodeURIComponent(zip))
            .then(function (r) {
                return r.ok ? r.json() : null;
            })
            .then(function (data) {
                if (!data || token !== calc.zipLookupToken) return;
                if (calc.taxRateManuallySet) {
                    // User typed a rate while the lookup was in flight --
                    // respect it, just update the hint text.
                    if (taxRateHintEl) taxRateHintEl.textContent = "(estimate — edit for your area)";
                    return;
                }
                if (Number.isFinite(data.rate_pct)) {
                    els.taxRateEl.value = String(data.rate_pct);
                    renderMonthlyPayment(calc);
                    persistPreferences(calc);
                }
                if (taxRateHintEl && data.label) {
                    taxRateHintEl.textContent = "(" + data.label.toLowerCase() + " — edit if different)";
                }
            })
            .catch(function () {
                if (token === calc.zipLookupToken && taxRateHintEl) {
                    taxRateHintEl.textContent = "(estimate — edit for your area)";
                }
            });
    }

    function seedFinanceFromStorage(calc) {
        const els = calc.els;
        const savedDown = readStorage(STORAGE_KEYS.downPayment);
        if (savedDown != null && String(savedDown).trim() !== "") {
            els.downPaymentEl.value = String(parseDownPayment(calc, savedDown));
        }

        const savedTier = readStorage(STORAGE_KEYS.creditTier);
        if (savedTier && VALID_TIERS.indexOf(savedTier) >= 0) {
            els.creditTierEl.value = savedTier;
        }

        const savedTerm = readStorage(STORAGE_KEYS.term);
        if (savedTerm && VALID_TERMS.indexOf(savedTerm) >= 0) {
            els.termEl.value = savedTerm;
        }

        const savedHasTrade = readStorage(STORAGE_KEYS.hasTradeIn);
        if (savedHasTrade === "1") {
            setTradeInPanelOpen(calc, true);
        } else {
            setTradeInPanelOpen(calc, false);
        }

        const savedTradeValue = readStorage(STORAGE_KEYS.tradeValue);
        if (savedTradeValue != null && String(savedTradeValue).trim() !== "") {
            els.tradeValueEl.value = String(parseTradeValue(calc, savedTradeValue));
        }

        const savedZip = readStorage(STORAGE_KEYS.zip);
        if (savedZip && /^\d{5}$/.test(savedZip)) {
            els.zipEl.value = savedZip;
        }
        const savedTaxRate = readStorage(STORAGE_KEYS.taxRate);
        if (savedTaxRate != null && String(savedTaxRate).trim() !== "") {
            els.taxRateEl.value = String(CP.parseFinanceTaxRatePct(savedTaxRate));
        }
        // Only treat the rate as "manually set" if it was actually edited
        // by hand last visit (a separate flag) — not merely because a
        // value happened to get persisted alongside unrelated fields
        // (persistPreferences() writes every field on any calculator
        // change, so a stored tax rate doesn't by itself mean the user
        // touched it).
        calc.taxRateManuallySet = readStorage(STORAGE_KEYS.taxRateManual) === "1";
        const savedFees = readStorage(STORAGE_KEYS.fees);
        if (savedFees != null && String(savedFees).trim() !== "") {
            els.feesEl.value = String(CP.parseFinanceFees(savedFees));
        }
        if (els.zipEl.value && /^\d{5}$/.test(els.zipEl.value)) {
            lookupTaxRateForZip(calc, els.zipEl.value);
        }
    }

    function bindFinanceInputs(calc) {
        const els = calc.els;

        function onCalculatorChange() {
            const parsed = parseDownPayment(calc, els.downPaymentEl.value);
            const raw = String(els.downPaymentEl.value).trim();
            if (raw !== "" && parsed !== parseFloat(raw)) {
                els.downPaymentEl.value = String(parsed);
            }
            const parsedTrade = parseTradeValue(calc, els.tradeValueEl.value);
            const rawTrade = String(els.tradeValueEl.value).trim();
            if (rawTrade !== "" && parsedTrade !== parseFloat(rawTrade)) {
                els.tradeValueEl.value = String(parsedTrade);
            }
            renderMonthlyPayment(calc);
            persistPreferences(calc);
        }

        function onTaxRateChange() {
            calc.taxRateManuallySet = true;
            writeStorage(STORAGE_KEYS.taxRateManual, "1");
            if (els.taxRateHintEl) els.taxRateHintEl.textContent = "(estimate — edit for your area)";
            onCalculatorChange();
        }

        function onZipChange() {
            const zip = String(els.zipEl.value || "").replace(/\D/g, "").slice(0, 5);
            if (zip !== els.zipEl.value) els.zipEl.value = zip;
            // A new ZIP means the shopper wants a fresh location-based
            // estimate — let the lookup populate the field again rather than
            // staying stuck on whatever was there for the old ZIP.
            calc.taxRateManuallySet = false;
            writeStorage(STORAGE_KEYS.taxRateManual, "0");
            persistPreferences(calc);
            if (/^\d{5}$/.test(zip)) lookupTaxRateForZip(calc, zip);
        }

        function onTradeToggle(yes) {
            setTradeInPanelOpen(calc, yes);
            onCalculatorChange();
        }

        els.tradeToggleNo.addEventListener("click", function () {
            onTradeToggle(false);
        });
        els.tradeToggleYes.addEventListener("click", function () {
            onTradeToggle(true);
        });

        ["input", "change"].forEach(function (evt) {
            els.downPaymentEl.addEventListener(evt, onCalculatorChange);
            els.creditTierEl.addEventListener(evt, onCalculatorChange);
            els.termEl.addEventListener(evt, onCalculatorChange);
            els.tradeConditionEl.addEventListener(evt, onCalculatorChange);
            els.tradeValueEl.addEventListener(evt, onCalculatorChange);
            els.tradeHighDemandEl.addEventListener(evt, onCalculatorChange);
            els.taxRateEl.addEventListener(evt, onTaxRateChange);
            els.feesEl.addEventListener(evt, onCalculatorChange);
        });
        els.zipEl.addEventListener("change", onZipChange);
        els.zipEl.addEventListener("blur", onZipChange);
    }

    function initCarFinanceCalculator() {
        const container = document.getElementById("car-finance-calculator");
        if (!container) return;

        const vehiclePrice = parseFloat(container.getAttribute("data-vehicle-price"));
        if (!Number.isFinite(vehiclePrice) || vehiclePrice <= 0) return;

        const els = getFinanceElements();
        if (!els) return;

        const calc = createFinanceCalc(vehiclePrice, els);
        bindFinanceInputs(calc);
        seedFinanceFromStorage(calc);
        renderMonthlyPayment(calc);
        initCarFinanceDetailsAnimation();
    }

    function initCarFinanceDetailsAnimation() {
        const details = document.querySelector(".car-finance-estimate-details");
        if (!details || details.dataset.financeAnimBound === "true") return;

        const summary = details.querySelector(".car-finance-customize-trigger");
        const panelWrap = details.querySelector(".car-finance-estimate-panel-wrap");
        if (!summary || !panelWrap) return;

        details.dataset.financeAnimBound = "true";

        const prefersReducedMotion =
            typeof window.matchMedia === "function" &&
            window.matchMedia("(prefers-reduced-motion: reduce)").matches;

        function openDetails() {
            details.setAttribute("open", "");
            if (prefersReducedMotion) {
                details.classList.add("is-open");
                return;
            }
            requestAnimationFrame(function () {
                details.classList.add("is-open");
            });
        }

        function closeDetails() {
            details.classList.remove("is-open");
            if (prefersReducedMotion) {
                details.removeAttribute("open");
                return;
            }
            window.setTimeout(function () {
                if (!details.classList.contains("is-open")) {
                    details.removeAttribute("open");
                }
            }, CAR_FINANCE_DETAILS_TRANSITION_MS);
        }

        summary.addEventListener("click", function (e) {
            e.preventDefault();
            if (details.classList.contains("is-open")) {
                closeDetails();
                return;
            }
            openDetails();
        });
    }

    CarPage.initCarFinanceCalculator = initCarFinanceCalculator;
})(window.CarPage, window.CP);
