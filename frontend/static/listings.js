/**
 * Smart search: instant client preview + debounced POST /api/search/smart.
 */
function listingsCsrfToken() {
    const m = document.querySelector('meta[name="csrf-token"]');
    return m ? (m.getAttribute("content") || "").trim() : "";
}

const SMART_SEARCH_STOP_WORDS = new Set([
    "with", "and", "or", "under", "below", "the", "a", "an", "in", "on", "for", "to",
    "inside", "outside", "miles", "mile", "loaded", "fully", "display", "camera",
]);

document.addEventListener("DOMContentLoaded", () => {
    const input = document.getElementById("smart-search-input");
    const chips = document.getElementById("smart-parse-chips");
    const form = document.getElementById("search-form");
    const searchWrap = input && input.closest(".smart-search-wrap");
    if (!input || typeof window.__DS_renderCarGrid !== "function") return;

    if (typeof window.__DS_ensureListingsCarsLoaded === "function") {
        window.__DS_ensureListingsCarsLoaded().catch(() => {});
    }

    let debounceTimer = null;
    let activeController = null;
    let previewSeq = 0;

    function setSearchLoading(on) {
        input.classList.toggle("smart-search-input--loading", on);
        if (searchWrap) searchWrap.classList.toggle("smart-search-wrap--loading", on);
        input.setAttribute("aria-busy", on ? "true" : "false");
    }

    function carsForInstantPreview() {
        if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) return window.ALL_CARS;
        if (Array.isArray(window.INITIAL_GRID_CARS) && window.INITIAL_GRID_CARS.length) {
            return window.INITIAL_GRID_CARS;
        }
        if (Array.isArray(window.BOOTSTRAP_GRID_CARS) && window.BOOTSTRAP_GRID_CARS.length) {
            return window.BOOTSTRAP_GRID_CARS;
        }
        return [];
    }

    function runInstantPreview(q) {
        const cars = carsForInstantPreview();
        if (!cars.length) return;
        const terms = q.toLowerCase().split(/\s+/).filter((t) => t.length >= 2 && !SMART_SEARCH_STOP_WORDS.has(t));
        if (!terms.length) return;
        const preview = cars.filter((c) => {
            const hay = [
                c.make,
                c.model,
                c.trim,
                c.title,
                c.fuel_type,
                c.drivetrain,
                c.exterior_color,
                c.interior_color,
                ...(c.package_names || []),
            ].filter(Boolean).join(" ").toLowerCase();
            return terms.every((t) => hay.includes(t));
        });
        if (preview.length) {
            window.__DS_renderCarGrid(preview.slice(0, 100), { preserveOrder: false, resetPage: true });
        }
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
        const pkgList = filters.packages_json_contains_list;
        if (Array.isArray(pkgList) && pkgList.length) {
            pkgList.forEach((p) => parts.push(`Equipment: ${p}`));
        } else if (filters.packages_json_contains) {
            parts.push(`Equipment: ${filters.packages_json_contains}`);
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

    if (form) {
        form.addEventListener("submit", () => {
            const h = document.getElementById("form-q-sync");
            if (h && input) h.value = (input.value || "").trim();
        });
    }

    function formScalar(name) {
        const vals = [...document.querySelectorAll(`#search-form [name="${name}"]`)]
            .map((el) => (el.value || "").trim())
            .filter(Boolean);
        return vals[0] || "";
    }

    function runSmartSearch() {
        const q = (input.value || "").trim();
        if (!q) {
            if (activeController) { activeController.abort(); activeController = null; }
            setSearchLoading(false);
            clearHighlights();
            if (typeof window.__DS_applySmartFilters === "function") {
                window.__DS_applySmartFilters({});
            }
            if (typeof window.__DS_runFilterRender === "function") {
                window.__DS_runFilterRender();
            }
            return;
        }

        const seq = ++previewSeq;
        if (activeController) activeController.abort();
        activeController = new AbortController();
        const signal = activeController.signal;
        setSearchLoading(true);

        const headers = { "Content-Type": "application/json" };
        const t = listingsCsrfToken();
        if (t) headers["X-CSRF-Token"] = t;
        const zip_code = formScalar("zip_code");
        const radius = formScalar("radius");
        const payload = { query: q };
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
                if (seq !== previewSeq) return;
                activeController = null;
                setSearchLoading(false);
                const filters = data.filters || {};
                window.__DS_renderCarGrid(data.results || [], {
                    preserveOrder: true,
                    resetPage: true,
                    emptyMessage: data.empty_message || null,
                });
                applyHighlights(data.highlight || []);
                fillParseChips(filters);
                if (typeof window.__DS_applySmartFilters === "function") {
                    window.__DS_applySmartFilters(filters);
                }
            })
            .catch((err) => {
                if (err.name === "AbortError") return;
                if (seq !== previewSeq) return;
                activeController = null;
                setSearchLoading(false);
                clearHighlights();
                if (typeof window.__DS_runFilterRender === "function") {
                    window.__DS_runFilterRender();
                }
            });
    }

    input.addEventListener("input", () => {
        const q = (input.value || "").trim();
        clearTimeout(debounceTimer);
        if (q) runInstantPreview(q);
        debounceTimer = setTimeout(runSmartSearch, 200);
    });

    if ((input.value || "").trim()) {
        if (typeof window.__DS_whenListingsGeoReady === "function") {
            window.__DS_whenListingsGeoReady(runSmartSearch);
        } else {
            runSmartSearch();
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
