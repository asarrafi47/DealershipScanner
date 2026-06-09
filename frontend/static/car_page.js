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
        const counterBadge = document.getElementById("car-gallery-counter-badge");
        const thumbsEl = document.getElementById("car-gallery-thumbs");
        if (!imgEl) return;

        let gallery = [];
        try {
            if (jsonEl && jsonEl.textContent) gallery = JSON.parse(jsonEl.textContent);
        } catch (e) {
            gallery = [];
        }
        if (!Array.isArray(gallery)) gallery = [];
        function isGalleryJunkUrl(u) {
            const sl = String(u || "").toLowerCase();
            if (!sl.startsWith("http")) return true;
            const junk = [
                "transferbadge",
                "directions-icon",
                "photoswipe",
                "default-skin",
                "gubagoo",
                "pureinfluencer",
                "idrove.it",
                "/customwork/",
                "coming soon",
            ];
            return junk.some(function (frag) {
                return sl.indexOf(frag) >= 0;
            });
        }

        gallery = gallery.filter(function (u) {
            return u && typeof u === "string" && !isGalleryJunkUrl(u);
        });
        if (gallery.length === 0) {
            const fallback = imgEl.getAttribute("src") || "";
            if (fallback && !isGalleryJunkUrl(fallback)) gallery = [fallback];
        }
        if (gallery.length === 0) return;

        let activeImageIndex = 0;
        function show() {
            const url = gallery[activeImageIndex];
            if (url && imgEl) imgEl.src = url;
            const counterText = activeImageIndex + 1 + " / " + gallery.length;
            if (counterEl) counterEl.textContent = activeImageIndex + 1 + " of " + gallery.length;
            if (counterBadge) counterBadge.textContent = counterText;
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

    function initCarFinanceCalculator() {
        const container = document.getElementById("car-finance-calculator");
        if (!container) return;

        const vehiclePrice = parseFloat(container.getAttribute("data-vehicle-price"));
        if (!Number.isFinite(vehiclePrice) || vehiclePrice <= 0) return;

        const displayEl = document.getElementById("display-monthly-payment");
        const downPaymentEl = document.getElementById("calc-down-payment");
        const creditTierEl = document.getElementById("calc-credit-tier");
        const termEl = document.getElementById("calc-term");
        const tradeToggleNo = document.getElementById("trade-in-toggle-no");
        const tradeToggleYes = document.getElementById("trade-in-toggle-yes");
        const tradePanelEl = document.getElementById("trade-in-inputs-panel");
        const tradeConditionEl = document.getElementById("trade-vehicle-condition");
        const tradeValueEl = document.getElementById("calc-trade-value");
        const tradeHighDemandEl = document.getElementById("trade-high-demand-bonus");
        if (!displayEl || !downPaymentEl || !creditTierEl || !termEl) return;
        if (!tradeToggleNo || !tradeToggleYes || !tradePanelEl || !tradeConditionEl || !tradeValueEl || !tradeHighDemandEl) return;

        const ANNUAL_RATES = {
            excellent: 0.065,
            good: 0.08,
            fair: 0.115,
            subprime: 0.15,
        };

        const STORAGE_KEYS = {
            downPayment: "pref_down_payment",
            creditTier: "pref_credit_tier",
            term: "pref_term",
            hasTradeIn: "pref_has_trade_in",
            tradeValue: "pref_trade_value",
        };

        const CONDITION_MULTIPLIERS = {
            clean: 1,
            fair: 0.85,
            rough: 0.6,
        };

        const VALID_TIERS = Object.keys(ANNUAL_RATES);
        const VALID_TERMS = ["36", "48", "60", "72"];
        const maxDownPayment = Math.floor(vehiclePrice);
        const maxTradeValue = Math.floor(vehiclePrice);

        let hasTradeIn = false;

        downPaymentEl.max = String(maxDownPayment);
        tradeValueEl.max = String(maxTradeValue);

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

        function parseDownPayment(raw) {
            const trimmed = String(raw == null ? "" : raw).trim();
            if (trimmed === "") return 0;
            const n = parseFloat(trimmed);
            if (!Number.isFinite(n) || n < 0) return 0;
            return Math.min(n, maxDownPayment);
        }

        function parseTerm(raw) {
            const n = parseInt(String(raw || ""), 10);
            return Number.isFinite(n) && n > 0 ? n : null;
        }

        function parseTradeValue(raw) {
            const trimmed = String(raw == null ? "" : raw).trim();
            if (trimmed === "") return 0;
            const n = parseFloat(trimmed);
            if (!Number.isFinite(n) || n < 0) return 0;
            return Math.min(n, maxTradeValue);
        }

        function getConditionMultiplier() {
            const key = tradeConditionEl.value;
            return CONDITION_MULTIPLIERS[key] != null ? CONDITION_MULTIPLIERS[key] : CONDITION_MULTIPLIERS.fair;
        }

        function computeTradeInEquity() {
            if (!hasTradeIn) return 0;
            let base = parseTradeValue(tradeValueEl.value);
            if (tradeHighDemandEl.checked) {
                base += 1000;
            }
            return Math.round(base * getConditionMultiplier());
        }

        function setTradeInPanelOpen(open) {
            hasTradeIn = !!open;
            tradeToggleNo.classList.toggle("car-finance-toggle-btn--active", !hasTradeIn);
            tradeToggleYes.classList.toggle("car-finance-toggle-btn--active", hasTradeIn);
            tradeToggleNo.setAttribute("aria-pressed", hasTradeIn ? "false" : "true");
            tradeToggleYes.setAttribute("aria-pressed", hasTradeIn ? "true" : "false");
            if (hasTradeIn) {
                tradePanelEl.removeAttribute("hidden");
                requestAnimationFrame(function () {
                    tradePanelEl.classList.add("car-finance-trade-panel--open");
                });
            } else {
                tradePanelEl.classList.remove("car-finance-trade-panel--open");
                tradePanelEl.setAttribute("hidden", "");
            }
        }

        function seedFromStorage() {
            const savedDown = readStorage(STORAGE_KEYS.downPayment);
            if (savedDown != null && String(savedDown).trim() !== "") {
                downPaymentEl.value = String(parseDownPayment(savedDown));
            }

            const savedTier = readStorage(STORAGE_KEYS.creditTier);
            if (savedTier && VALID_TIERS.indexOf(savedTier) >= 0) {
                creditTierEl.value = savedTier;
            }

            const savedTerm = readStorage(STORAGE_KEYS.term);
            if (savedTerm && VALID_TERMS.indexOf(savedTerm) >= 0) {
                termEl.value = savedTerm;
            }

            const savedHasTrade = readStorage(STORAGE_KEYS.hasTradeIn);
            if (savedHasTrade === "1") {
                setTradeInPanelOpen(true);
            } else {
                setTradeInPanelOpen(false);
            }

            const savedTradeValue = readStorage(STORAGE_KEYS.tradeValue);
            if (savedTradeValue != null && String(savedTradeValue).trim() !== "") {
                tradeValueEl.value = String(parseTradeValue(savedTradeValue));
            }
        }

        function formatPayment(amount) {
            if (!Number.isFinite(amount)) return "--";
            return Math.round(amount).toLocaleString("en-US");
        }

        function computeMonthlyPayment() {
            const downPayment = parseDownPayment(downPaymentEl.value);
            const tier = creditTierEl.value;
            const annualRate = ANNUAL_RATES[tier] != null ? ANNUAL_RATES[tier] : ANNUAL_RATES.good;
            const termMonths = parseTerm(termEl.value);

            if (termMonths == null) {
                displayEl.textContent = "--";
                return;
            }

            const tradeInValue = computeTradeInEquity();
            const principal = vehiclePrice * 1.07 + 500 - downPayment - tradeInValue;

            if (principal <= 0) {
                displayEl.textContent = "0";
                return;
            }

            if (annualRate <= 0) {
                displayEl.textContent = formatPayment(principal / termMonths);
                return;
            }

            const monthlyRate = annualRate / 12;
            const factor = Math.pow(1 + monthlyRate, termMonths);
            const payment = (principal * (monthlyRate * factor)) / (factor - 1);

            displayEl.textContent = formatPayment(payment);
        }

        function persistPreferences() {
            writeStorage(STORAGE_KEYS.downPayment, downPaymentEl.value);
            writeStorage(STORAGE_KEYS.creditTier, creditTierEl.value);
            writeStorage(STORAGE_KEYS.term, termEl.value);
            writeStorage(STORAGE_KEYS.hasTradeIn, hasTradeIn ? "1" : "0");
            writeStorage(STORAGE_KEYS.tradeValue, tradeValueEl.value);
        }

        function onCalculatorChange() {
            const parsed = parseDownPayment(downPaymentEl.value);
            const raw = String(downPaymentEl.value).trim();
            if (raw !== "" && parsed !== parseFloat(raw)) {
                downPaymentEl.value = String(parsed);
            }
            const parsedTrade = parseTradeValue(tradeValueEl.value);
            const rawTrade = String(tradeValueEl.value).trim();
            if (rawTrade !== "" && parsedTrade !== parseFloat(rawTrade)) {
                tradeValueEl.value = String(parsedTrade);
            }
            computeMonthlyPayment();
            persistPreferences();
        }

        function onTradeToggle(yes) {
            setTradeInPanelOpen(yes);
            onCalculatorChange();
        }

        tradeToggleNo.addEventListener("click", function () {
            onTradeToggle(false);
        });
        tradeToggleYes.addEventListener("click", function () {
            onTradeToggle(true);
        });

        ["input", "change"].forEach(function (evt) {
            downPaymentEl.addEventListener(evt, onCalculatorChange);
            creditTierEl.addEventListener(evt, onCalculatorChange);
            termEl.addEventListener(evt, onCalculatorChange);
            tradeConditionEl.addEventListener(evt, onCalculatorChange);
            tradeValueEl.addEventListener(evt, onCalculatorChange);
            tradeHighDemandEl.addEventListener(evt, onCalculatorChange);
        });

        seedFromStorage();
        computeMonthlyPayment();
        initCarFinanceDetailsAnimation();
    }

    const CAR_FINANCE_DETAILS_TRANSITION_MS = 420;

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
        if (!Number.isFinite(amount)) return "$--";
        return "$" + Math.round(amount).toLocaleString("en-US");
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
        if (!Number.isFinite(amount)) return "$--";
        return "$" + amount.toFixed(2);
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

    let _tcoInitGen = 0;
    let _tcoRenderCtx = null;
    let _tcoEvRenderCtx = null;

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
        if (!Number.isFinite(amount)) return "$--";
        if (amount >= 1000) {
            const thousands = amount / 1000;
            const rounded =
                thousands >= 100
                    ? Math.round(thousands)
                    : parseFloat(thousands.toFixed(thousands < 10 ? 1 : 0));
            return "$" + rounded.toLocaleString("en-US") + "k";
        }
        return formatTcoCurrency(amount);
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

    function setupTcoCostCanvas(canvas) {
        const frame = canvas.closest(".tco-cost-chart-frame");
        const frameInnerWidth = frame
            ? Math.max(frame.clientWidth - 32, 240)
            : Math.max(canvas.clientWidth || 320, 280);
        if (frame && frameInnerWidth <= 240) {
            return null;
        }
        const cssWidth = Math.floor(Math.min(frameInnerWidth, 408));
        const cssHeight = Math.floor(cssWidth * 0.58);
        const dpr = Math.max(window.devicePixelRatio || 1, 1);

        canvas.width = Math.floor(cssWidth * dpr);
        canvas.height = Math.floor(cssHeight * dpr);
        canvas.style.width = cssWidth + "px";
        canvas.style.height = cssHeight + "px";

        const ctx = canvas.getContext("2d");
        if (!ctx) return null;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = "high";
        ctx.clearRect(0, 0, cssWidth, cssHeight);
        return { ctx: ctx, cssWidth: cssWidth, cssHeight: cssHeight };
    }

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

        const surface = setupTcoCostCanvas(canvas);
        if (!surface) {
            requestAnimationFrame(function () {
                renderTcoCostCurve(o);
            });
            return;
        }

        const maintenanceCost = o.maintenanceCost;
        const mileageSteps = buildTcoMileageSteps(o.startMileage || 0, TCO_ANNUAL_MILES);
        const series = scenarios.map(function (scenario) {
            const annualFuel = isEv
                ? computeTcoAnnualEvFuelSpend(scenario.rate, o.kwhPer100)
                : computeTcoAnnualFuelSpend(scenario.rate, o.avgMpg);
            return {
                scenario: scenario,
                values: buildTcoCumulativeOperationalCosts(maintenanceCost, annualFuel),
            };
        });

        const allValues = [];
        series.forEach(function (s) {
            allValues.push.apply(allValues, s.values);
        });
        const minVal = 0;
        const maxVal = Math.max.apply(null, allValues);
        const valSpan = maxVal - minVal || 1;

        const ctx = surface.ctx;
        const cssWidth = surface.cssWidth;
        const cssHeight = surface.cssHeight;
        const pad = { top: 16, right: 12, bottom: 44, left: 44 };
        const chartW = cssWidth - pad.left - pad.right;
        const chartH = cssHeight - pad.top - pad.bottom;

        function xAt(index) {
            return pad.left + (chartW * index) / TCO_FORECAST_YEARS;
        }
        function yAt(val) {
            return pad.top + chartH * (1 - (val - minVal) / valSpan);
        }

        const gridColor = "#e2e8f0";
        const labelColor = "#64748b";
        const mileageColor = "#94a3b8";
        const chartBottom = pad.top + chartH;

        ctx.strokeStyle = gridColor;
        ctx.lineWidth = 1;
        for (let g = 0; g <= 3; g += 1) {
            const gy = pad.top + (chartH * g) / 3;
            ctx.beginPath();
            ctx.moveTo(pad.left, gy);
            ctx.lineTo(pad.left + chartW, gy);
            ctx.stroke();
        }

        ctx.fillStyle = labelColor;
        ctx.font = "600 11px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let g = 0; g <= 3; g += 1) {
            const tickVal = minVal + (valSpan * (3 - g)) / 3;
            const gy = pad.top + (chartH * g) / 3;
            ctx.fillText(formatTcoCostCurrencyShort(tickVal), pad.left - 6, gy);
        }

        const yearLabels = ["Now", "Yr 1", "Yr 2", "Yr 3", "Yr 4", "Yr 5"];
        series.forEach(function (s) {
            const points = s.values.map(function (val, i) {
                return { x: xAt(i), y: yAt(val), val: val };
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

        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        for (let i = 0; i <= TCO_FORECAST_YEARS; i += 1) {
            const x = xAt(i);
            ctx.fillStyle = labelColor;
            ctx.font = "600 11px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
            ctx.fillText(yearLabels[i], x, chartBottom + 6);
            ctx.fillStyle = mileageColor;
            ctx.font = "500 10px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
            ctx.fillText(formatTcoMileageLabel(mileageSteps[i]), x, chartBottom + 20);
        }

        const baseSeries = series.find(function (s) {
            return s.scenario.key === "base";
        });
        const lowSeries = series.find(function (s) {
            return s.scenario.key === "low";
        });
        const highSeries = series.find(function (s) {
            return s.scenario.key === "high";
        });
        const baseY5 = baseSeries ? baseSeries.values[5] : null;
        const lowY5 = lowSeries ? lowSeries.values[5] : null;
        const highY5 = highSeries ? highSeries.values[5] : null;
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

    let tcoCostResizeObserver = null;
    let _tcoCostRenderCtx = null;

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

    function renderTcoEvFuelCosts(rate, opts) {
        const o = opts || {};
        const maintenanceCost = o.maintenanceCost;
        const kwhPer100 = o.kwhPer100;
        const fuelEl = o.fuelEl;
        const totalEl = o.totalEl;
        const fuelBar = o.fuelBar;
        const maintenanceBar = o.maintenanceBar;
        const labelEl = o.labelEl;
        if (!fuelEl || !totalEl || !fuelBar || !maintenanceBar) return;

        if (!Number.isFinite(rate) || rate <= 0) {
            fuelEl.textContent = "$--";
            totalEl.textContent = formatTcoCurrency(maintenanceCost);
            renderTcoDashboard(maintenanceCost, 0, fuelBar, maintenanceBar);
            syncTcoCostCurveRender({
                maintenanceCost: maintenanceCost,
                baseRate: null,
                kwhPer100: kwhPer100,
                isEv: true,
            });
            if (labelEl) {
                labelEl.textContent = o.carState
                    ? "residential EV charging in " + o.carState + " (EIA sync pending)"
                    : "residential EV charging (EIA sync pending)";
            }
            return;
        }

        const totalFuelExpense = computeTcoEvFuelExpense(rate, kwhPer100);
        if (totalFuelExpense == null) {
            fuelEl.textContent = "$--";
            totalEl.textContent = formatTcoCurrency(maintenanceCost);
            renderTcoDashboard(maintenanceCost, 0, fuelBar, maintenanceBar);
            syncTcoCostCurveRender({
                maintenanceCost: maintenanceCost,
                baseRate: null,
                kwhPer100: kwhPer100,
                isEv: true,
            });
            return;
        }

        fuelEl.textContent = formatTcoCurrency(totalFuelExpense);
        totalEl.textContent = formatTcoCurrency(maintenanceCost + totalFuelExpense);
        renderTcoDashboard(maintenanceCost, totalFuelExpense, fuelBar, maintenanceBar);
        syncTcoCostCurveRender({
            maintenanceCost: maintenanceCost,
            baseRate: rate,
            kwhPer100: kwhPer100,
            isEv: true,
        });
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
        const maintenanceCost = o.maintenanceCost;
        const avgMpg = o.avgMpg;
        const fuelEl = o.fuelEl;
        const totalEl = o.totalEl;
        const fuelBar = o.fuelBar;
        const maintenanceBar = o.maintenanceBar;
        const labelEl = o.labelEl;
        if (!fuelEl || !totalEl || !fuelBar || !maintenanceBar) return;

        updateTcoFillUpEstimate(rate, {
            tankGallons: o.tankGallons,
            avgMpg: avgMpg,
            mpgCity: o.mpgCity,
            mpgHighway: o.mpgHighway,
        });

        if (!Number.isFinite(rate) || rate <= 0) {
            fuelEl.textContent = "$--";
            totalEl.textContent = formatTcoCurrency(maintenanceCost);
            renderTcoDashboard(maintenanceCost, 0, fuelBar, maintenanceBar);
            syncTcoCostCurveRender({
                maintenanceCost: maintenanceCost,
                baseRate: null,
                avgMpg: avgMpg,
                isEv: false,
            });
            updateTcoDisclaimerLabel(labelEl, null, {
                isCustom: o.isCustom,
                regionName: o.regionName,
                fuelTier: o.fuelTier,
                carState: o.carState,
            });
            return;
        }

        const totalFuelExpense = computeTcoFuelExpense(rate, avgMpg);
        if (totalFuelExpense == null) {
            fuelEl.textContent = "$--";
            totalEl.textContent = formatTcoCurrency(maintenanceCost);
            renderTcoDashboard(maintenanceCost, 0, fuelBar, maintenanceBar);
            syncTcoCostCurveRender({
                maintenanceCost: maintenanceCost,
                baseRate: null,
                avgMpg: avgMpg,
                isEv: false,
            });
            return;
        }

        fuelEl.textContent = formatTcoCurrency(totalFuelExpense);
        totalEl.textContent = formatTcoCurrency(maintenanceCost + totalFuelExpense);
        renderTcoDashboard(maintenanceCost, totalFuelExpense, fuelBar, maintenanceBar);
        syncTcoCostCurveRender({
            maintenanceCost: maintenanceCost,
            baseRate: rate,
            avgMpg: avgMpg,
            isEv: false,
        });
        updateTcoDisclaimerLabel(labelEl, rate, {
            isCustom: o.isCustom,
            regionName: o.regionName,
            fuelTier: o.fuelTier,
            carState: o.carState,
        });
    }

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

        input.addEventListener("input", applyFromInput);
        input.addEventListener("change", applyFromInput);
        input.addEventListener("blur", function () {
            const ctx = _tcoRenderCtx;
            if (!ctx) return;
            const userRate = parseTcoFuelPriceInput(input.value);
            if (userRate != null) return;
            delete input.dataset.tcoUserEdited;
            syncTcoFuelPriceInput(ctx.liveRate);
            const effective = getTcoEffectiveFuelRate(ctx.liveRate);
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

        input.addEventListener("input", applyFromInput);
        input.addEventListener("change", applyFromInput);
        input.addEventListener("blur", function () {
            const ctx = _tcoEvRenderCtx;
            if (!ctx) return;
            const userRate = parseTcoElectricityPriceInput(input.value);
            if (userRate != null) return;
            delete input.dataset.tcoUserEdited;
            syncTcoElectricityPriceInput(ctx.liveElectricityRate);
            const effective = getTcoEffectiveElectricityRate(ctx.liveElectricityRate);
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
        });
    }

    async function initTcoIntelligence() {
        const card = document.getElementById("tco-intelligence-card");
        if (!card) return;
        const gen = ++_tcoInitGen;

        const maintenanceEl = document.getElementById("tco-maintenance-cost");
        const fuelEl = document.getElementById("tco-fuel-cost");
        const totalEl = document.getElementById("tco-total-combined");
        const fuelBar = document.getElementById("tco-bar-fuel");
        const maintenanceBar = document.getElementById("tco-bar-maintenance");
        const labelEl = document.getElementById("tco-dynamic-state-label");
        if (!maintenanceEl || !fuelEl || !totalEl || !fuelBar || !maintenanceBar) return;

        const makeKey = normalizeTcoMakeKey(card.getAttribute("data-car-make"));
        const avgMpg = resolveTcoAvgMpg(card);
        const tankGallons = resolveTcoTankGallons(card);
        const mpgCityRaw = parseFloat(card.getAttribute("data-car-mpg-city"));
        const mpgHwyRaw = parseFloat(card.getAttribute("data-car-mpg-highway"));
        const mpgCity = Number.isFinite(mpgCityRaw) && mpgCityRaw > 0 ? mpgCityRaw : null;
        const mpgHighway = Number.isFinite(mpgHwyRaw) && mpgHwyRaw > 0 ? mpgHwyRaw : null;
        const carState = resolveTcoCarState(card);
        const fuelTier = (card.getAttribute("data-fuel-tier") || "regular").trim().toLowerCase();

        const maintenanceCost = lookupTcoMaintenanceCost(makeKey);
        maintenanceEl.textContent = formatTcoCurrency(maintenanceCost);

        bindTcoFuelPriceInput();
        bindTcoElectricityPriceInput();

        let rate = null;
        let electricityRate = null;
        let regionName = null;
        let electricityRegionName = null;
        try {
            const resp = await fetch(buildTcoFuelLookupUrl(fuelTier, carState), {
                credentials: "same-origin",
            });
            if (gen !== _tcoInitGen) return;
            if (resp.ok) {
                const data = await resp.json();
                rate = parseFloat(data.rate);
                electricityRate = parseFloat(data.electricity_rate);
                regionName = data.region_name;
                electricityRegionName = data.electricity_region_name || data.region_name;
            }
        } catch (_err) {
            if (gen !== _tcoInitGen) return;
            rate = null;
            electricityRate = null;
        }

        if (gen !== _tcoInitGen) return;

        if (isTcoElectricVehicle(card)) {
            const kwhPer100 = resolveTcoEvEfficiency(card);
            _tcoEvRenderCtx = {
                maintenanceCost: maintenanceCost,
                kwhPer100: kwhPer100,
                liveElectricityRate: electricityRate,
                electricityRegionName: electricityRegionName,
                carState: carState,
                fuelEl: fuelEl,
                totalEl: totalEl,
                fuelBar: fuelBar,
                maintenanceBar: maintenanceBar,
                labelEl: labelEl,
            };
            syncTcoElectricityPriceInput(electricityRate);
            const effective = getTcoEffectiveElectricityRate(electricityRate);
            renderTcoEvFuelCosts(effective.rate, {
                maintenanceCost: maintenanceCost,
                kwhPer100: kwhPer100,
                fuelEl: fuelEl,
                totalEl: totalEl,
                fuelBar: fuelBar,
                maintenanceBar: maintenanceBar,
                labelEl: labelEl,
                isCustom: effective.isCustom,
                electricityRegionName: electricityRegionName,
                carState: carState,
            });
            return;
        }

        _tcoRenderCtx = {
            maintenanceCost: maintenanceCost,
            avgMpg: avgMpg,
            tankGallons: tankGallons,
            mpgCity: mpgCity,
            mpgHighway: mpgHighway,
            liveRate: rate,
            regionName: regionName,
            fuelTier: fuelTier,
            carState: carState,
            fuelEl: fuelEl,
            totalEl: totalEl,
            fuelBar: fuelBar,
            maintenanceBar: maintenanceBar,
            labelEl: labelEl,
        };

        syncTcoFuelPriceInput(rate);
        const effective = getTcoEffectiveFuelRate(rate);
        renderTcoFuelCosts(effective.rate, {
            maintenanceCost: maintenanceCost,
            avgMpg: avgMpg,
            tankGallons: tankGallons,
            mpgCity: mpgCity,
            mpgHighway: mpgHighway,
            fuelEl: fuelEl,
            totalEl: totalEl,
            fuelBar: fuelBar,
            maintenanceBar: maintenanceBar,
            labelEl: labelEl,
            isCustom: effective.isCustom,
            regionName: regionName,
            fuelTier: fuelTier,
            carState: carState,
        });
    }

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

    function getSelectedDepreciationAnnualMileage(card) {
        const select = document.getElementById("dep-annual-mi-select");
        if (select && select.value) {
            const parsed = parseInt(select.value, 10);
            if (Number.isFinite(parsed)) return parsed;
        }
        const meta = parseDepreciationMetadata(card);
        return snapDepreciationAnnualMileage(computeAnnualMileage(meta));
    }

    function initDepreciationAnnualMileageSelect(card) {
        const select = document.getElementById("dep-annual-mi-select");
        if (!select) return;

        if (!select.options.length) {
            const defaultMileage = snapDepreciationAnnualMileage(
                computeAnnualMileage(parseDepreciationMetadata(card))
            );
            buildDepreciationAnnualMileageOptions().forEach(function (mi) {
                const opt = document.createElement("option");
                opt.value = String(mi);
                opt.textContent = formatDepreciationAnnualMileageOption(mi);
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

    function setupDepreciationCanvas(canvas) {
        const frame = canvas.closest(".depreciation-chart-frame");
        const frameInnerWidth = frame
            ? Math.max(frame.clientWidth - 32, 240)
            : Math.max(canvas.clientWidth || 320, 280);
        if (frame && frameInnerWidth <= 240) {
            return null;
        }
        const cssWidth = Math.floor(Math.min(frameInnerWidth, 408));
        const cssHeight = Math.floor(cssWidth * 0.58);
        const dpr = Math.max(window.devicePixelRatio || 1, 1);

        canvas.width = Math.floor(cssWidth * dpr);
        canvas.height = Math.floor(cssHeight * dpr);
        canvas.style.width = cssWidth + "px";
        canvas.style.height = cssHeight + "px";

        const ctx = canvas.getContext("2d");
        if (!ctx) return null;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = "high";
        ctx.clearRect(0, 0, cssWidth, cssHeight);
        return { ctx: ctx, cssWidth: cssWidth, cssHeight: cssHeight };
    }

    function renderDepreciationCurve() {
        const card = document.getElementById("depreciation-forecast-card");
        const canvas = document.getElementById("depreciationCanvas");
        if (!card || !canvas) return;

        const surface = setupDepreciationCanvas(canvas);
        if (!surface) {
            requestAnimationFrame(renderDepreciationCurve);
            return;
        }

        const meta = parseDepreciationMetadata(card);
        const annualMileage = getSelectedDepreciationAnnualMileage(card);
        const mileageSteps = buildDepreciationMileageSteps(meta.mileage, annualMileage);
        const targetResidual = computeDepreciationTargetResidual(meta, annualMileage);
        const values = buildDepreciationTrajectory(meta.price, targetResidual);
        const price = meta.price;

        const y1MiEl = document.getElementById("dep-y1-mi");
        const y5MiEl = document.getElementById("dep-y5-mi");
        const y1El = document.getElementById("dep-y1");
        const y5El = document.getElementById("dep-y5");
        if (y1MiEl) y1MiEl.textContent = formatDepreciationMileage(mileageSteps[1]);
        if (y5MiEl) y5MiEl.textContent = formatDepreciationMileage(mileageSteps[5]);
        if (y1El) y1El.textContent = formatDepreciationCurrency(values[1]);
        if (y5El) y5El.textContent = formatDepreciationCurrency(values[5]);

        const ctx = surface.ctx;
        const cssWidth = surface.cssWidth;
        const cssHeight = surface.cssHeight;

        const pad = { top: 16, right: 12, bottom: 44, left: 44 };
        const chartW = cssWidth - pad.left - pad.right;
        const chartH = cssHeight - pad.top - pad.bottom;
        const minVal = Math.min.apply(null, values);
        const maxVal = price;
        const valSpan = maxVal - minVal || 1;

        function xAt(index) {
            return pad.left + (chartW * index) / 5;
        }
        function yAt(val) {
            return pad.top + chartH * (1 - (val - minVal) / valSpan);
        }

        const gridColor = "#e2e8f0";
        const labelColor = "#64748b";
        const mileageColor = "#94a3b8";
        const lineColor = "#2563eb";
        const chartBottom = pad.top + chartH;

        ctx.strokeStyle = gridColor;
        ctx.lineWidth = 1;
        for (let g = 0; g <= 3; g += 1) {
            const gy = pad.top + (chartH * g) / 3;
            ctx.beginPath();
            ctx.moveTo(pad.left, gy);
            ctx.lineTo(pad.left + chartW, gy);
            ctx.stroke();
        }

        ctx.fillStyle = labelColor;
        ctx.font = "600 11px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let g = 0; g <= 3; g += 1) {
            const tickVal = minVal + (valSpan * (3 - g)) / 3;
            const gy = pad.top + (chartH * g) / 3;
            ctx.fillText(formatDepreciationCurrencyShort(tickVal), pad.left - 6, gy);
        }

        const yearLabels = ["Now", "Yr 1", "Yr 2", "Yr 3", "Yr 4", "Yr 5"];
        const points = values.map(function (val, i) {
            return { x: xAt(i), y: yAt(val), val: val, mi: mileageSteps[i] };
        });

        const areaGradient = ctx.createLinearGradient(0, pad.top, 0, chartBottom);
        areaGradient.addColorStop(0, "rgba(37, 99, 235, 0.28)");
        areaGradient.addColorStop(1, "rgba(37, 99, 235, 0.03)");
        ctx.beginPath();
        ctx.moveTo(points[0].x, chartBottom);
        points.forEach(function (pt) {
            ctx.lineTo(pt.x, pt.y);
        });
        ctx.lineTo(points[points.length - 1].x, chartBottom);
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

        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        for (let i = 0; i <= 5; i += 1) {
            const x = xAt(i);
            ctx.fillStyle = labelColor;
            ctx.font = "600 11px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
            ctx.fillText(yearLabels[i], x, chartBottom + 6);
            ctx.fillStyle = mileageColor;
            ctx.font = "500 10px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
            ctx.fillText(formatDepreciationMileage(mileageSteps[i]), x, chartBottom + 20);
        }

        canvas.setAttribute(
            "aria-label",
            "Estimated future value from " +
                formatDepreciationCurrency(values[0]) +
                " now at " +
                formatDepreciationMileage(mileageSteps[0]) +
                " to " +
                formatDepreciationCurrency(values[5]) +
                " in year five at " +
                formatDepreciationMileage(mileageSteps[5]) +
                ", assuming " +
                formatDepreciationMileage(annualMileage) +
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

        const currentYear = 2026;
        const modelYear = safeYear;
        const agePenalty = Math.max(0, currentYear - modelYear) * 1.0;

        let mileagePenalty;
        if (safeMileage <= 20000) {
            mileagePenalty = safeMileage * 0.00008;
        } else {
            mileagePenalty = 1.6 + (safeMileage - 20000) * 0.00005;
        }

        const basePenalty = agePenalty + mileagePenalty;
        const thermalScale = evBatteryThermalScaleFactor(carMake);
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

    function initDealerReputation() {
        const block = document.getElementById("dealer-reputation-block");
        if (!block) return;

        const rating = parseDealerRating(block.getAttribute("data-dealer-rating"));
        const reviewCount = parseDealerReviewCount(block.getAttribute("data-dealer-reviews"));

        if (rating == null) {
            block.hidden = true;
            block.setAttribute("aria-hidden", "true");
            return;
        }

        const starsEl = block.querySelector(".reputation-stars");
        const countEl = document.getElementById("dealer-review-count");
        const badgesEl = document.getElementById("dealer-trust-badges");

        if (starsEl) {
            starsEl.textContent = buildReputationStars(rating);
            starsEl.setAttribute("aria-label", rating.toFixed(1) + " out of 5 stars");
            starsEl.removeAttribute("aria-hidden");
        }

        if (countEl) {
            countEl.textContent = reviewCount != null ? formatReviewCount(reviewCount) : "";
        }

        if (badgesEl) {
            badgesEl.innerHTML = "";
            if (rating >= 4.5 && reviewCount != null && reviewCount > 200) {
                badgesEl.insertAdjacentHTML(
                    "beforeend",
                    '<span class="badge badge-success">\uD83C\uDFC6 Top Rated Dealer</span>'
                );
            }
            if (rating <= 3.7) {
                badgesEl.insertAdjacentHTML(
                    "beforeend",
                    '<span class="badge badge-warning">\u26A0\uFE0F Review Dealer Policies</span>'
                );
            }
        }

        block.removeAttribute("hidden");
        block.removeAttribute("aria-hidden");
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
                        escHtml(row.label || "Flag") +
                        '</span> <span class="vehicle-history-flag__value">' +
                        escHtml(row.value || "") +
                        "</span></li>";
                });
                html += "</ul>";
            }
            if (recalls.length) {
                html += '<details class="vehicle-history-recalls-details"><summary>NHTSA recall details</summary><ul class="vehicle-history-recalls-list">';
                recalls.forEach(function (r) {
                    const head = [r.campaign, r.component].filter(Boolean).join(" — ");
                    html += "<li>";
                    if (head) html += "<strong>" + escHtml(head) + "</strong> ";
                    if (r.summary) html += escHtml(r.summary);
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
            if (!hasPremiumHistoryAccess(access)) {
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
        const daysEl = document.getElementById("days-count");
        const badgeContainer = document.getElementById("negotiation-badge-container");
        const timelineEl = document.getElementById("history-timeline-list");
        if (!widget) return;

        const REFERENCE_DATE = new Date(2026, 5, 2);

        function parseDate(raw) {
            if (raw == null || String(raw).trim() === "") return null;
            let s = String(raw).trim();
            if (s.endsWith("Z")) s = s.slice(0, -1) + "+00:00";
            const d = new Date(s);
            return Number.isNaN(d.getTime()) ? null : d;
        }

        function daysBetween(a, b) {
            const ms = Math.abs(b.getTime() - a.getTime());
            return Math.floor(ms / 86400000);
        }

        function formatMoney(amount) {
            return Math.abs(Math.round(amount)).toLocaleString("en-US");
        }

        function formatEventDate(dateStr) {
            const d = parseDate(dateStr);
            if (!d) return String(dateStr || "");
            return d.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
        }

        const created = parseDate(widget.getAttribute("data-created-at"));
        let daysOnMarket = null;
        if (created) {
            daysOnMarket = daysBetween(created, REFERENCE_DATE);
            if (daysEl) daysEl.textContent = String(daysOnMarket);
        }

        if (badgeContainer && daysOnMarket !== null) {
            if (daysOnMarket > 60) {
                badgeContainer.innerHTML =
                    '<span class="radar-badge high-leverage">High Leverage: Aged Inventory</span>';
            } else if (daysOnMarket >= 31) {
                badgeContainer.innerHTML =
                    '<span class="radar-badge mid-leverage">Moderate Leverage: Standard Cycle</span>';
            } else {
                badgeContainer.innerHTML =
                    '<span class="radar-badge low-leverage">Fresh Inventory: Firm Pricing</span>';
            }
        }

        initMarketVelocityHeatmap(daysOnMarket);

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

    const SEGMENT_BASELINE_DAYS = {
        suv: 42,
        sedan: 58,
        truck: 35,
        coupe: 72,
        convertible: 85,
    };

    const BRAND_MODIFIER_DAYS = {
        toyota: -10,
        honda: -8,
        tesla: -5,
        ford: 2,
        bmw: 12,
        "land rover": 22,
    };

    const DEFAULT_SEGMENT_BASELINE = 45;
    const STAGNATION_THRESHOLD_DAYS = 45;
    const STAGNATION_PENALTY_DAYS = 15;

    function normalizeSegmentKey(rawType) {
        const t = String(rawType || "")
            .trim()
            .toLowerCase();
        if (!t) return "sedan";
        if (t.includes("suv") || t.includes("crossover") || t.includes("wagon")) return "suv";
        if (t.includes("truck") || t.includes("pickup")) return "truck";
        if (t.includes("convert")) return "convertible";
        if (t.includes("coupe")) return "coupe";
        if (t.includes("sedan") || t.includes("hatch")) return "sedan";
        return t.split(/\s+/)[0] || "sedan";
    }

    function normalizeBrandKey(rawBrand) {
        return String(rawBrand || "")
            .trim()
            .toLowerCase();
    }

    function resolveDaysOnMarket(fallbackDays) {
        const daysEl = document.getElementById("days-count");
        if (daysEl) {
            const parsed = parseInt(String(daysEl.textContent || "").trim(), 10);
            if (Number.isFinite(parsed) && parsed >= 0) return parsed;
        }
        return fallbackDays != null && Number.isFinite(fallbackDays) ? fallbackDays : null;
    }

    function computePredictiveTurnaroundDays(segmentKey, brandKey, daysOnMarket) {
        const baseline =
            SEGMENT_BASELINE_DAYS[segmentKey] != null
                ? SEGMENT_BASELINE_DAYS[segmentKey]
                : DEFAULT_SEGMENT_BASELINE;
        const brandMod =
            BRAND_MODIFIER_DAYS[brandKey] != null ? BRAND_MODIFIER_DAYS[brandKey] : 0;
        let projected = baseline + brandMod;
        if (daysOnMarket != null && daysOnMarket > STAGNATION_THRESHOLD_DAYS) {
            projected += STAGNATION_PENALTY_DAYS;
        }
        return Math.max(1, Math.round(projected));
    }

    function mapVelocityMarker(projectedDays) {
        const marker = document.getElementById("velocity-marker");
        if (!marker) return;

        let leftPct;
        let color;

        if (projectedDays < 30) {
            const span = 29;
            const dayOffset = Math.max(1, Math.min(projectedDays, 29));
            leftPct = 85 + ((30 - dayOffset) / span) * 15;
            color = "#059669";
        } else if (projectedDays > 75) {
            const excess = projectedDays - 75;
            const range = 45;
            leftPct = Math.max(0, 25 - (excess / range) * 25);
            color = "#334155";
        } else {
            leftPct = 25 + ((74 - projectedDays) / 43) * 60;
            color = "#d97706";
        }

        marker.style.left = Math.max(0, Math.min(100, leftPct)) + "%";
        marker.style.backgroundColor = color;
        marker.style.borderColor = color;
    }

    function initMarketVelocityHeatmap(daysOnMarketFromRadar) {
        const card = document.getElementById("market-velocity-card");
        if (!card) return;

        const segmentKey = normalizeSegmentKey(card.getAttribute("data-car-type"));
        const brandKey = normalizeBrandKey(card.getAttribute("data-car-brand"));
        const daysOnMarket = resolveDaysOnMarket(daysOnMarketFromRadar);
        const projectedDays = computePredictiveTurnaroundDays(
            segmentKey,
            brandKey,
            daysOnMarket
        );

        const projectionEl = document.getElementById("velocity-days-projection");
        if (projectionEl) projectionEl.textContent = String(projectedDays);

        mapVelocityMarker(projectedDays);
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
            initTcoIntelligence();
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
        initCarGallery();
        initCarVdpTabs();
        initCompareTrayResync();
        initCarFinanceCalculator();
    }

    function initCarPageDeferred() {
        initCarHistoryHighlights();
        initDealerReputation();
        initEvBatteryIntelligence();
        initTcoIntelligence();
        initTcoCostChart();
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
