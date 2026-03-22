/**
 * Smart search: debounced POST /api/search/smart + filter highlights.
 */
document.addEventListener("DOMContentLoaded", () => {
    const input = document.getElementById("smart-search-input");
    const chips = document.getElementById("smart-parse-chips");
    const form = document.getElementById("search-form");
    if (!input || typeof window.__DS_renderCarGrid !== "function") return;

    let debounceTimer = null;

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

    function fillYearChips(filters) {
        if (!chips) return;
        chips.innerHTML = "";
        const hasYear = filters.min_year != null || filters.max_year != null;
        if (!hasYear) return;
        const span = document.createElement("span");
        span.className = "smart-parse-chip";
        if (filters.min_year != null && filters.max_year != null && filters.min_year === filters.max_year) {
            span.textContent = `Year: ${filters.min_year}`;
        } else if (filters.min_year != null && filters.max_year == null) {
            span.textContent = `Year: ${filters.min_year} or newer`;
        } else {
            span.textContent = `Year: ${filters.min_year}–${filters.max_year}`;
        }
        chips.appendChild(span);
    }

    function clearSmartBecauseFilters(ev) {
        if (ev && (ev.target === input || ev.target.closest(".smart-search-wrap"))) return;
        if ((input.value || "").trim()) {
            input.value = "";
            clearHighlights();
        }
    }

    if (form) {
        form.addEventListener("change", clearSmartBecauseFilters, true);
        form.addEventListener("input", clearSmartBecauseFilters, true);
    }

    function runSmartSearch() {
        const q = (input.value || "").trim();
        if (!q) {
            clearHighlights();
            if (typeof window.__DS_runFilterRender === "function") {
                window.__DS_runFilterRender();
            }
            return;
        }

        fetch("/api/search/smart", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: q }),
        })
            .then((r) => {
                if (!r.ok) throw new Error("search failed");
                return r.json();
            })
            .then((data) => {
                const filters = data.filters || {};
                window.__DS_renderCarGrid(data.results || []);
                applyHighlights(data.highlight || []);
                fillYearChips(filters);
            })
            .catch(() => {
                clearHighlights();
                if (typeof window.__DS_runFilterRender === "function") {
                    window.__DS_runFilterRender();
                }
            });
    }

    input.addEventListener("input", () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(runSmartSearch, 250);
    });
});
