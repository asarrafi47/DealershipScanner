/**
 * Smart search: instant client filter on every keystroke + debounced authoritative POST.
 */
function listingsCsrfToken() {
    const m = document.querySelector('meta[name="csrf-token"]');
    return m ? (m.getAttribute("content") || "").trim() : "";
}

const SMART_SEARCH_STOP_WORDS = new Set([
    "with", "and", "or", "under", "below", "the", "a", "an", "in", "on", "for", "to",
    "inside", "outside", "miles", "mile", "loaded", "fully", "display", "camera",
]);

const SMART_SEARCH_FULL_DEBOUNCE_MS = 50;
const _VIN_FULL_RE = /^[A-HJ-NPR-Z0-9]{17}$/i;
const _LISTING_ID_RE = /^(?:#|(?:id|car|carid)\s*:\s*)?(\d{1,10})\s*$/i;

function smartFiltersLookActionable(filters, q) {
    const keys = filters && typeof filters === "object" ? Object.keys(filters) : [];
    if (keys.length) return true;
    const s = (q || "").trim();
    if (!s) return false;
    if (_VIN_FULL_RE.test(s.replace(/\s+/g, ""))) return true;
    if (/^\d{1,10}$/.test(s)) return true;
    if (_LISTING_ID_RE.test(s)) return true;
    return false;
}

document.addEventListener("DOMContentLoaded", () => {
    const guestDismiss = document.getElementById("guest-banner-dismiss");
    if (guestDismiss) {
        guestDismiss.addEventListener("click", () => {
            guestDismiss.closest(".guest-banner")?.remove();
        });
    }

    const input = document.getElementById("smart-search-input");
    const chips = document.getElementById("smart-parse-chips");
    const form = document.getElementById("search-form");
    const searchWrap = input && input.closest(".smart-search-wrap");
    if (!input || typeof window.__DS_renderCarGrid !== "function") return;

    if (typeof window.__DS_ensureListingsCarsLoaded === "function") {
        window.__DS_ensureListingsCarsLoaded().catch(() => {});
    }

    let fullSearchTimer = null;
    let parseController = null;
    let searchController = null;
    let inputSeq = 0;
    let pendingFullSearch = false;

    function setSearchLoading(on) {
        input.classList.toggle("smart-search-input--loading", on);
        if (searchWrap) searchWrap.classList.toggle("smart-search-wrap--loading", on);
        input.setAttribute("aria-busy", on ? "true" : "false");
    }

    function formScalar(name) {
        const vals = [...document.querySelectorAll(`#search-form [name="${name}"]`)]
            .map((el) => (el.value || "").trim())
            .filter(Boolean);
        return vals[0] || "";
    }

    function clearHighlights() {
        document.querySelectorAll(".smart-parse-active").forEach((el) => {
            el.classList.remove("smart-parse-active");
        });
        if (chips) {
            chips.classList.remove("smart-parse-active");
            chips.innerHTML = "";
        }
    }

    function applyHighlights(highlightParams) {
        clearHighlights();
        if (!highlightParams || !highlightParams.length) return;
        highlightParams.forEach((param) => {
            if (param === "max_price" || param === "max_mileage") {
                document.querySelectorAll(`[data-filter-param="${param}"]`).forEach((el) => {
                    el.classList.add("smart-parse-active");
                });
                return;
            }
            if (param === "year") {
                if (chips) chips.classList.add("smart-parse-active");
                return;
            }
            ["trigger", "acc-trigger"].forEach((prefix) => {
                const el = document.getElementById(`${prefix}-${param}`);
                if (el) el.classList.add("smart-parse-active");
            });
        });
    }

    function fillParseChips(filters) {
        if (!chips) return;
        chips.innerHTML = "";
        const parts = [];
        const vehicleOr = filters.vehicle_or;
        if (Array.isArray(vehicleOr) && vehicleOr.length) {
            vehicleOr.forEach((vf) => {
                const label = [vf.make, vf.model, vf.trim_contains].filter(Boolean).join(" ");
                if (label) parts.push(label);
            });
        } else {
            const makes = filters.make;
            const models = filters.model;
            const makeList = Array.isArray(makes) ? makes : (makes ? [makes] : []);
            const modelList = Array.isArray(models) ? models : (models ? [models] : []);
            if (makeList.length && modelList.length) {
                makeList.forEach((mk, i) => {
                    parts.push([mk, modelList[i] || modelList[0]].filter(Boolean).join(" "));
                });
            } else {
                makeList.forEach((mk) => parts.push(String(mk)));
                modelList.forEach((md) => parts.push(String(md)));
            }
        }
        const trims = filters.trim_contains;
        const trimList = Array.isArray(trims) ? trims : (trims ? [trims] : []);
        trimList.forEach((t) => parts.push(`Trim: ${t}`));
        const pkgAll = filters.packages_json_contains_all;
        if (Array.isArray(pkgAll) && pkgAll.length) {
            parts.push(`Equipment (all): ${pkgAll.join(" · ")}`);
        } else {
            const pkgList = filters.packages_json_contains_list;
            if (Array.isArray(pkgList) && pkgList.length) {
                pkgList.forEach((p) => parts.push(`Equipment: ${p}`));
            } else if (filters.packages_json_contains) {
                parts.push(`Equipment: ${filters.packages_json_contains}`);
            }
        }
        const hasYear = filters.min_year != null || filters.max_year != null;
        if (hasYear) {
            if (filters.min_year != null && filters.max_year != null && filters.min_year === filters.max_year) {
                parts.push(`Year: ${filters.min_year}`);
            } else if (filters.min_year != null && filters.max_year == null) {
                parts.push(`Year: ${filters.min_year}+`);
            } else {
                parts.push(`Year: ${filters.min_year}–${filters.max_year}`);
            }
        }
        if (filters.max_price != null) {
            const p = Number(filters.max_price);
            parts.push(Number.isFinite(p) ? `Under $${p.toLocaleString()}` : `Under $${filters.max_price}`);
        }
        if (filters.max_mileage != null) {
            const m = Number(filters.max_mileage);
            parts.push(Number.isFinite(m) ? `Under ${m.toLocaleString()} mi` : `Under ${filters.max_mileage} mi`);
        }
        const extColors = filters.exterior_color;
        const extList = Array.isArray(extColors) ? extColors : (extColors ? [extColors] : []);
        extList.forEach((c) => parts.push(`Exterior: ${c}`));
        const intColors = filters.interior_color;
        const intList = Array.isArray(intColors) ? intColors : (intColors ? [intColors] : []);
        intList.forEach((c) => parts.push(`Interior: ${c}`));
        if (filters.drivetrain) {
            const d = filters.drivetrain;
            parts.push(`Drive: ${Array.isArray(d) ? d.join(", ") : d}`);
        }
        if (filters.body_style) {
            const b = filters.body_style;
            parts.push(`Body: ${Array.isArray(b) ? b.join(", ") : b}`);
        }
        if (filters.fuel_type) parts.push(`Fuel: ${filters.fuel_type}`);
        if (filters.cylinders != null) parts.push(`${filters.cylinders}-cylinder`);
        if (filters.fully_loaded) parts.push("Fully loaded");
        parts.forEach((text) => {
            const span = document.createElement("span");
            span.className = "smart-parse-chip";
            span.textContent = text;
            chips.appendChild(span);
        });
        if (parts.length) chips.classList.add("smart-parse-active");
    }

    /** Immediate token filter while parse/API are in flight. */
    function runTokenPreview(q) {
        const source =
            typeof window.__DS_getListingsInventorySource === "function"
                ? window.__DS_getListingsInventorySource()
                : [];
        if (!source.length) return;
        const terms = q
            .toLowerCase()
            .split(/\s+/)
            .filter((t) => t.length >= 2 && !SMART_SEARCH_STOP_WORDS.has(t));
        if (!terms.length) return;
        const preview = source.filter((c) => {
            const hay = [
                c.make,
                c.model,
                c.trim,
                c.title,
                c.fuel_type,
                c.drivetrain,
                c.exterior_color,
                c.interior_color,
                c.engine_description,
                ...(c.package_names || []),
            ]
                .filter(Boolean)
                .join(" ")
                .toLowerCase();
            return terms.every((t) => hay.includes(t));
        });
        window.__DS_renderCarGrid(preview, { preserveOrder: false, resetPage: true });
    }

    function applyParsedFilters(filters, opts) {
        const q = (input.value || "").trim();
        if (!smartFiltersLookActionable(filters, q)) return;
        applyHighlights((opts && opts.highlight) || []);
        fillParseChips(filters);
        if (typeof window.__DS_applySmartFilters === "function") {
            window.__DS_applySmartFilters(filters, { skipCascade: true });
        }
        if (typeof window.__DS_renderFromSmartFilters === "function") {
            window.__DS_renderFromSmartFilters(filters, {
                emptyMessage: (opts && opts.emptyMessage) || null,
            });
        }
    }

    function runParseRequest(seq) {
        const q = (input.value || "").trim();
        if (!q || seq !== inputSeq) return;

        if (parseController) parseController.abort();
        parseController = new AbortController();
        const params = new URLSearchParams({ query: q });
        const zip_code = formScalar("zip_code");
        const radius = formScalar("radius");
        if (zip_code) params.set("zip_code", zip_code);
        if (radius) params.set("radius", radius);

        fetch(`/api/search/smart/parse?${params.toString()}`, {
            credentials: "same-origin",
            signal: parseController.signal,
        })
            .then((r) => (r.ok ? r.json() : null))
            .then((data) => {
                if (seq !== inputSeq) return;
                parseController = null;
                if (!data || !data.ok) return;
                applyParsedFilters(data.filters || {}, { highlight: data.highlight || [] });
                if (!pendingFullSearch) setSearchLoading(false);
            })
            .catch((err) => {
                if (err.name === "AbortError") return;
                if (seq !== inputSeq) return;
                parseController = null;
            });
    }

    function runFullSmartSearch(seq) {
        const q = (input.value || "").trim();
        if (!q || seq !== inputSeq) return;

        pendingFullSearch = true;
        if (searchController) searchController.abort();
        searchController = new AbortController();
        const signal = searchController.signal;
        setSearchLoading(true);

        const headers = { "Content-Type": "application/json" };
        const t = listingsCsrfToken();
        if (t) headers["X-CSRF-Token"] = t;
        const payload = { query: q };
        const zip_code = formScalar("zip_code");
        const radius = formScalar("radius");
        if (zip_code) payload.zip_code = zip_code;
        if (radius) payload.radius = radius;

        fetch("/api/search/smart", {
            method: "POST",
            headers,
            credentials: "same-origin",
            signal,
            body: JSON.stringify(payload),
        })
            .then((r) => {
                if (!r.ok) throw new Error("search failed");
                return r.json();
            })
            .then((data) => {
                if (seq !== inputSeq) return;
                searchController = null;
                pendingFullSearch = false;
                setSearchLoading(false);
                const filters = data.filters || {};
                window.__DS_renderCarGrid(data.results || [], {
                    preserveOrder: true,
                    resetPage: true,
                    emptyMessage: data.empty_message || null,
                });
                applyHighlights(data.highlight || []);
                fillParseChips(filters);
                const syncSidebar = () => {
                    if (typeof window.__DS_applySmartFilters === "function") {
                        window.__DS_applySmartFilters(filters);
                    }
                };
                if (typeof requestIdleCallback === "function") {
                    requestIdleCallback(syncSidebar, { timeout: 800 });
                } else {
                    setTimeout(syncSidebar, 0);
                }
            })
            .catch((err) => {
                if (err.name === "AbortError") return;
                if (seq !== inputSeq) return;
                searchController = null;
                pendingFullSearch = false;
                setSearchLoading(false);
            });
    }

    function onSmartInput() {
        const q = (input.value || "").trim();
        const seq = ++inputSeq;

        clearTimeout(fullSearchTimer);
        if (parseController) parseController.abort();
        if (searchController) searchController.abort();
        parseController = null;
        searchController = null;
        pendingFullSearch = false;

        if (!q) {
            setSearchLoading(false);
            clearHighlights();
            if (typeof window.__DS_applySmartFilters === "function") {
                window.__DS_applySmartFilters({});
            }
            if (typeof window.__DS_runFilterRenderInstant === "function") {
                window.__DS_runFilterRenderInstant();
            } else if (typeof window.__DS_runFilterRender === "function") {
                window.__DS_runFilterRender();
            }
            return;
        }

        requestAnimationFrame(() => {
            if (seq !== inputSeq) return;
            runTokenPreview(q);
        });

        requestAnimationFrame(() => {
            if (seq !== inputSeq) return;
            runParseRequest(seq);
        });

        fullSearchTimer = setTimeout(() => runFullSmartSearch(seq), SMART_SEARCH_FULL_DEBOUNCE_MS);
    }

    if (form) {
        form.addEventListener("submit", () => {
            const h = document.getElementById("form-q-sync");
            if (h && input) h.value = (input.value || "").trim();
        });
    }

    input.addEventListener("input", onSmartInput);

    if ((input.value || "").trim()) {
        const boot = () => onSmartInput();
        if (typeof window.__DS_whenListingsGeoReady === "function") {
            window.__DS_whenListingsGeoReady(boot);
        } else {
            boot();
        }
    }

    const hintText = document.querySelector(".smart-search-hint__text");
    if (hintText) {
        const examples = [
            "Accord with heated seats under $30k · Premium Package · paste a VIN",
            "BMW X5 with M Sport under $70k · AWD · under 50k miles",
            "Silverado crew cab with tow package · under 80k miles",
            "White RAV4 with sunroof · hybrid · under $35k",
        ];
        let hintIdx = 0;
        setInterval(() => {
            hintIdx = (hintIdx + 1) % examples.length;
            hintText.textContent = examples[hintIdx];
        }, 8000);
    }
});
