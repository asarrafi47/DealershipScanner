document.addEventListener("DOMContentLoaded", () => {
    /** Listings boot: JSON blobs (CSP-friendly) — see listings.html */
    (function loadListingsBootFromJson() {
        if (!document.getElementById("ds-listings-car-rows")) return;
        function readJsonScript(id, fallback) {
            const el = document.getElementById(id);
            if (!el) return fallback;
            const raw = el.textContent.trim();
            if (!raw) return fallback;
            try {
                return JSON.parse(raw);
            } catch {
                return fallback;
            }
        }
        window.CAR_ROWS = readJsonScript("ds-listings-car-rows", []);
        window.ALL_CARS = readJsonScript("ds-listings-all-cars", []);
        window.COUNTRY_TO_MAKES = readJsonScript("ds-listings-country-to-makes", {});
        window.INITIAL_GRID_CARS = readJsonScript("ds-listings-initial-grid", []);
    })();

    // Fade in
    document.body.style.opacity = 0;
    setTimeout(() => {
        document.body.style.transition = "opacity 0.5s ease";
        document.body.style.opacity = 1;
    }, 50);

    if (typeof CAR_ROWS === "undefined") return;

    // ── Dock filter bar (Motion.dev animates content sliding right when filter appears) ──
    const filterBar  = document.getElementById("filter-bar");
    const page       = document.querySelector(".page");
    const dashContent = document.getElementById("dash-content");
    const DOCK_AT    = 60;
    const UNDOCK_AT  = 20;
    let isDocked     = false;

    import("https://esm.sh/motion@latest").then(({ animate }) => {
        const dockedInner = document.getElementById("filter-docked-inner");
        const DOCK_MARGIN = 248;
        const DURATION    = 0.35;
        const EASING      = [0.4, 0, 0.2, 1];

        function setDocked(on) {
            if (isDocked === on) return;
            isDocked = on;
            filterBar.classList.toggle("docked", on);
            page.classList.toggle("docked", on);
            if (on) {
                dockedInner.style.pointerEvents = "all";
                animate(dockedInner, { opacity: 1, x: 0 }, { duration: DURATION, ease: EASING });
                animate(dashContent, { marginLeft: `${DOCK_MARGIN}px` }, { duration: DURATION, ease: EASING });
            } else {
                animate(dockedInner, { opacity: 0, x: -20 }, { duration: DURATION, ease: EASING }).finished.then(() => {
                    dockedInner.style.pointerEvents = "none";
                });
                animate(dashContent, { marginLeft: 0 }, { duration: DURATION, ease: EASING }).finished.then(() => {
                    dashContent.style.marginLeft = "";
                });
            }
        }

        function onScroll() {
            if (!isDocked && window.scrollY > DOCK_AT)   setDocked(true);
            if (isDocked  && window.scrollY < UNDOCK_AT) setDocked(false);
        }

        window.addEventListener("scroll", onScroll, { passive: true });
    });

    // ── Helpers ────────────────────────────────────────────────────────

    function normFilterStr(v) {
        return (v == null || v === "") ? "" : String(v).trim().toLowerCase();
    }

    function valueInListCI(list, val) {
        if (!list || !list.length) return true;
        const v = normFilterStr(val);
        return list.some(x => normFilterStr(x) === v);
    }

    // Collect unique checked values (pill + accordion share names, deduplicate)
    function checked(name) {
        const seen = new Set();
        return [...document.querySelectorAll(`input[name="${name}"]:checked`)]
            .map(cb => cb.value)
            .filter(v => seen.has(v) ? false : seen.add(v));
    }

    function compatibleRows(excluding) {
        const makes  = excluding === "make"       ? [] : checked("make");
        const models = excluding === "model"      ? [] : checked("model");
        const trims  = excluding === "trim"       ? [] : checked("trim");
        const fuels  = excluding === "fuel_type"  ? [] : checked("fuel_type");
        const drives = excluding === "drivetrain" ? [] : checked("drivetrain");
        const bodies = excluding === "body_style" ? [] : checked("body_style");
        const cyls   = excluding === "cylinders"  ? [] : checked("cylinders");

        return CAR_ROWS.filter(r => {
            if (makes.length  && !valueInListCI(makes, r.make))        return false;
            if (models.length && !valueInListCI(models, r.model))      return false;
            if (trims.length  && !valueInListCI(trims, r.trim))        return false;
            if (fuels.length  && !fuels.includes(r.fuel))        return false;
            if (drives.length && !drives.includes(r.drive))      return false;
            if (bodies.length && !valueInListCI(bodies, r.body_style)) return false;
            if (cyls.length   && !cyls.includes(String(r.cyl))) return false;
            return true;
        });
    }

    // ── Cascade engine ─────────────────────────────────────────────────
    // Both pill dropdowns and accordion bodies share the same input names
    // so checking one automatically syncs the other — we just need to
    // cascade visibility across all containers with matching option ids.

    function runCascade() {
        cascadeParam("make",        r => r.make,        ["options-make",        "acc-options-make"]);
        cascadeMakeByCountry();
        cascadeParam("model",       r => r.model,       ["options-model",       "acc-options-model"]);
        cascadeParam("trim",        r => r.trim,        ["options-trim",        "acc-options-trim"]);
        cascadeParam("fuel_type",   r => r.fuel,        ["options-fuel_type",   "acc-options-fuel_type"]);
        cascadeParam("drivetrain",  r => r.drive,       ["options-drivetrain",  "acc-options-drivetrain"]);
        cascadeParam(
            "body_style",
            r => (r.body_style != null && String(r.body_style).trim() !== "" ? String(r.body_style) : ""),
            ["options-body_style", "acc-options-body_style"]
        );
        cascadeParam("cylinders",   r => String(r.cyl), ["options-cylinders",   "acc-options-cylinders"]);
        updateCylinders();
        updateAllCounts();
    }

    function cascadeMakeByCountry() {
        if (typeof COUNTRY_TO_MAKES !== "object") return;
        const countries = checked("country");
        const makeContainerIds = ["options-make", "acc-options-make"];
        if (!countries.length) {
            makeContainerIds.forEach(id => {
                const container = document.getElementById(id);
                if (!container) return;
                container.querySelectorAll('.filter-option input[name="make"]').forEach(cb => {
                    cb.closest(".filter-option").style.display = "";
                });
            });
            cascadeParam("make", r => r.make, makeContainerIds);
            return;
        }
        const allowedMakes = new Set(countries.flatMap(c => COUNTRY_TO_MAKES[c] || []));
        makeContainerIds.forEach(id => {
            const container = document.getElementById(id);
            if (!container) return;
            container.querySelectorAll(".filter-option").forEach(label => {
                const cb = label.querySelector('input[name="make"]');
                if (!cb) return;
                if (!allowedMakes.has(cb.value)) {
                    label.style.display = "none";
                    cb.checked = false;
                }
            });
        });
    }

    function cascadeParam(param, rowKey, containerIds) {
        const compatible = new Set(compatibleRows(param).map(rowKey));
        containerIds.forEach(id => {
            const container = document.getElementById(id);
            if (!container) return;
            container.querySelectorAll(".filter-option").forEach(label => {
                const cb = label.querySelector("input");
                const visible = compatible.has(cb.value);
                label.style.display = visible ? "" : "none";
                if (!visible) cb.checked = false;
            });
        });
    }

    // ── Electric cylinder collapse ─────────────────────────────────────

    function updateCylinders() {
        const rows = compatibleRows("cylinders");
        const allElectric = rows.length > 0 && rows.every(r => r.cyl === 0);

        // Update both pill trigger and accordion trigger
        ["trigger-cylinders", "acc-trigger-cylinders"].forEach(id => {
            const trigger = document.getElementById(id);
            const labelEl = document.getElementById(
                id === "trigger-cylinders" ? "label-cylinders" : "acc-label-cylinders"
            );
            const chevron = trigger ? trigger.querySelector(".pill-chevron, .acc-chevron") : null;

            if (!trigger || !labelEl) return;

            if (allElectric) {
                labelEl.textContent = "Electric";
                trigger.classList.add("electric-mode");
                trigger.disabled = true;
                if (chevron) chevron.style.display = "none";
                // close any open panel
                const dropdownId = id === "trigger-cylinders" ? "dropdown-cylinders" : "acc-body-cylinders";
                const panel = document.getElementById(dropdownId);
                if (panel) panel.classList.remove("open");
            } else {
                labelEl.textContent = "Cylinders";
                trigger.classList.remove("electric-mode", "has-selection");
                trigger.disabled = false;
                if (chevron) chevron.style.display = "";
            }
        });

        // Auto-check the 0-cyl box when all-electric, uncheck otherwise if mixed
        document.querySelectorAll("input[name='cylinders']").forEach(cb => {
            if (allElectric) cb.checked = (cb.value === "0");
        });
    }

    // ── Badge counts ───────────────────────────────────────────────────

    function updateCount(param) {
        const seen = new Set();
        const visibleChecked = [...document.querySelectorAll(`input[name="${param}"]:checked`)]
            .filter(cb => {
                const opt = cb.closest(".filter-option");
                if (opt && opt.style.display === "none") return false;
                if (seen.has(cb.value)) return false;
                seen.add(cb.value);
                return true;
            }).length;

        // pill count
        const pillCount = document.getElementById(`count-${param}`);
        const pillTrigger = document.getElementById(`trigger-${param}`);
        if (pillCount) {
            if (visibleChecked > 0) {
                pillCount.textContent = visibleChecked;
                pillCount.style.display = "inline";
                if (pillTrigger && !pillTrigger.classList.contains("electric-mode"))
                    pillTrigger.classList.add("has-selection");
            } else {
                pillCount.style.display = "none";
                if (pillTrigger && !pillTrigger.classList.contains("electric-mode"))
                    pillTrigger.classList.remove("has-selection");
            }
        }

        // accordion count
        const accCount = document.getElementById(`acc-count-${param}`);
        const accTrigger = document.getElementById(`acc-trigger-${param}`);
        if (accCount) {
            if (visibleChecked > 0) {
                accCount.textContent = visibleChecked;
                accCount.style.display = "inline";
                if (accTrigger && !accTrigger.classList.contains("electric-mode"))
                    accTrigger.classList.add("has-selection");
            } else {
                accCount.style.display = "none";
                if (accTrigger && !accTrigger.classList.contains("electric-mode"))
                    accTrigger.classList.remove("has-selection");
            }
        }
    }

    function updateAllCounts() {
        ["country", "make", "model", "trim", "fuel_type", "cylinders",
         "transmission", "drivetrain", "body_style", "exterior_color", "interior_color"]
            .forEach(updateCount);

        // Sidebar total badge
        const totalEl = document.getElementById("docked-total");
        if (totalEl) {
            const total = [...document.querySelectorAll(".filter-option input:checked")]
                .filter(cb => {
                    const opt = cb.closest(".filter-option");
                    return opt ? opt.style.display !== "none" : true;
                }).length;
            totalEl.textContent = total;
            totalEl.style.display = total > 0 ? "inline" : "none";
        }
    }

    // ── Wire all checkboxes → sync twin + cascade + live render ───────

    document.querySelectorAll(".filter-option input[type=checkbox]").forEach(cb => {
        cb.addEventListener("change", () => {
            // Mirror state to the twin checkbox (pill ↔ accordion)
            document.querySelectorAll(`input[type=checkbox][name="${cb.name}"]`).forEach(twin => {
                if (twin !== cb && twin.value === cb.value) twin.checked = cb.checked;
            });
            runCascade();
            renderResults();
        });
    });

    // Wire scalar filters (price, mileage, zip, radius) → live render
    // Read from both pill and sidebar selects; use whichever is non-empty.
    function scalarVal(name) {
        const vals = [...document.querySelectorAll(`[name="${name}"]`)]
            .map(el => el.value.trim()).filter(Boolean);
        return vals[0] || "";
    }

    document.querySelectorAll(".pill-select, .sidebar-select, .pill-zip, .sidebar-input").forEach(el => {
        el.addEventListener("change", renderResults);
        el.addEventListener("input",  renderResults);
    });

    // ── Live results renderer ──────────────────────────────────────────

    const resultsGrid  = document.getElementById("results-grid");
    const resultsCount = document.getElementById("results-count");
    const emptyState   = document.getElementById("empty-state");

    function fmt(n)  { return Number(n).toLocaleString(); }
    function fmtUSD(n) {
        if (n == null || n === "" || Number(n) === 0) return "Call for Price";
        return "$" + Number(n).toLocaleString("en-US", {maximumFractionDigits: 0});
    }

    function escapeHtml(s) {
        return String(s ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    /** Resolve URL and allow only http(s) for CSS background-image (mitigates javascript: / data: in listings). */
    function cssSingleQuotedUrl(url) {
        const raw = String(url || "").trim();
        if (!raw) return "/static/placeholder.svg";
        try {
            const abs = new URL(raw, window.location.origin);
            if (abs.protocol !== "http:" && abs.protocol !== "https:") {
                return "/static/placeholder.svg";
            }
            return abs.href.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        } catch (_) {
            return "/static/placeholder.svg";
        }
    }

    function renderCarGrid(cars) {
        if (!resultsGrid) return;

        if (cars.length === 0) {
            resultsGrid.innerHTML = "";
            if (emptyState) emptyState.style.display = "";
            if (resultsCount) resultsCount.textContent = "";
            return;
        }

        if (emptyState) emptyState.style.display = "none";
        if (resultsCount) {
            resultsCount.textContent = `${cars.length} vehicle${cars.length !== 1 ? "s" : ""} found`;
        }

        resultsGrid.innerHTML = cars.map(c => {
            const gallery = Array.isArray(c.gallery) ? c.gallery : [];
            const imgRaw = (gallery.length && gallery[0]) ? gallery[0] : (c.image_url || "") || "/static/placeholder.svg";
            const imgSrcQuoted = cssSingleQuotedUrl(imgRaw);
            const photoCount = gallery.length;
            const photoLabel = photoCount > 1 ? `${photoCount} photos` : "";
            const idNum = Number(c.id);
            const idStr = Number.isFinite(idNum) && idNum > 0 ? String(Math.floor(idNum)) : "0";
            const dashLike = (v) => {
                const s = String(v || "").trim();
                return !s || s === "\u2014" || s === "-" || s === "--";
            };
            const specBits = [];
            if (!dashLike(c.body_style)) specBits.push(`Body: ${escapeHtml(c.body_style)}`);
            if (!dashLike(c.condition)) specBits.push(`Condition: ${escapeHtml(c.condition)}`);
            if (!dashLike(c.exterior_color)) specBits.push(`Exterior: ${escapeHtml(c.exterior_color)}`);
            const specLine = specBits.length
                ? `<p class="result-meta result-meta--specs">${specBits.join(" &middot; ")}</p>`
                : "";
            return `
            <a href="/car/${idStr}" class="result-card">
                <div class="result-image-wrap">
                    <div class="result-image" style="background-image:url('${imgSrcQuoted}')"></div>
                    ${photoLabel ? `<span class="result-photo-count">${escapeHtml(photoLabel)}</span>` : ""}
                </div>
                <div class="result-content">
                    <h2>${escapeHtml(c.title)}</h2>
                    <p class="result-trim">${escapeHtml(c.trim || "")}</p>
                    <p class="result-price">${fmtUSD(c.price)}</p>
                    <p class="result-meta">
                        ${fmt(c.mileage)} mi
                        &middot; ${escapeHtml(c.fuel_type || "")}
                        &middot; ${escapeHtml(c.drivetrain || "")}
                    </p>
                    ${specLine}
                    <p class="result-dealer">${escapeHtml(c.dealer_name || "")}</p>
                </div>
            </a>`;
        }).join("");
    }

    function renderResults() {
        if (!resultsGrid) return;

        const smartIn = document.getElementById("smart-search-input");
        if (smartIn && (smartIn.value || "").trim()) return;

        const makes       = checked("make");
        const models      = checked("model");
        const trims       = checked("trim");
        const fuels       = checked("fuel_type");
        const cyls        = checked("cylinders");
        const trans       = checked("transmission");
        const drives      = checked("drivetrain");
        const bodies      = checked("body_style");
        const extColors   = checked("exterior_color");
        const intColors   = checked("interior_color");
        const countries   = checked("country");
        const maxPrice    = parseFloat(scalarVal("max_price"))   || null;
        const maxMileage  = parseInt(scalarVal("max_mileage"))   || null;
        const zipCode     = scalarVal("zip_code");
        const radiusMi    = parseFloat(scalarVal("radius"))      || null;

        let makesFilter = makes.slice();
        if (countries.length && typeof COUNTRY_TO_MAKES === "object") {
            const fromCountries = countries.flatMap(c => COUNTRY_TO_MAKES[c] || []);
            makesFilter = makesFilter.length
                ? makesFilter.filter(m => valueInListCI(fromCountries, m))
                : fromCountries;
        }

        let cars = ALL_CARS.filter(c => {
            if (makesFilter.length && !valueInListCI(makesFilter, c.make))     return false;
            if (models.length     && !valueInListCI(models, c.model))          return false;
            if (trims.length      && !valueInListCI(trims, c.trim))            return false;
            if (fuels.length      && !fuels.includes(c.fuel_type))       return false;
            if (cyls.length       && !cyls.includes(String(c.cylinders)))return false;
            if (trans.length      && !trans.includes(c.transmission))    return false;
            if (drives.length     && !drives.includes(c.drivetrain))     return false;
            if (bodies.length     && !valueInListCI(bodies, c.body_style)) return false;
            if (extColors.length  && !extColors.includes(c.exterior_color)) return false;
            if (intColors.length  && !intColors.includes(c.interior_color)) return false;
            if (maxPrice    && c.price   > maxPrice)   return false;
            if (maxMileage  && c.mileage > maxMileage) return false;
            return true;
        });

        if (zipCode && radiusMi && typeof haversineJS === "function") {
            const origin = zipCoordsJS(zipCode);
            if (origin) {
                cars = cars.filter(c => {
                    const dest = zipCoordsJS(c.zip_code);
                    if (!dest) return false;
                    return haversineJS(origin[0], origin[1], dest[0], dest[1]) <= radiusMi;
                });
            }
        }

        renderCarGrid(cars);
    }

    window.__DS_renderCarGrid = renderCarGrid;
    window.__DS_runFilterRender = renderResults;

    runCascade();
    if (typeof INITIAL_GRID_CARS !== "undefined" && Array.isArray(INITIAL_GRID_CARS) && INITIAL_GRID_CARS.length) {
        renderCarGrid(INITIAL_GRID_CARS);
    } else {
        renderResults();
    }

    // ── Pill dropdown open/close ───────────────────────────────────────

    document.querySelectorAll(".pill-trigger").forEach(trigger => {
        const param    = trigger.dataset.param;
        const dropdown = document.getElementById(`dropdown-${param}`);
        if (!dropdown) return;

        trigger.addEventListener("click", e => {
            if (trigger.disabled) return;
            e.stopPropagation();
            const isOpen = dropdown.classList.contains("open");

            document.querySelectorAll(".pill-dropdown.open").forEach(d => d.classList.remove("open"));
            document.querySelectorAll(".pill-trigger.open").forEach(t => t.classList.remove("open"));

            if (!isOpen) {
                dropdown.classList.add("open");
                trigger.classList.add("open");
            }
        });
    });

    document.addEventListener("click", () => {
        document.querySelectorAll(".pill-dropdown.open").forEach(d => d.classList.remove("open"));
        document.querySelectorAll(".pill-trigger.open").forEach(t => t.classList.remove("open"));
    });

    document.querySelectorAll(".pill-dropdown").forEach(d => {
        d.addEventListener("click", e => e.stopPropagation());
    });

    // ── Accordion open/close (docked sidebar) ─────────────────────────

    document.querySelectorAll(".acc-trigger").forEach(trigger => {
        if (trigger.disabled) return;

        const section = trigger.closest(".acc-section");
        const param   = section ? section.dataset.param : null;
        const body    = param ? document.getElementById(`acc-body-${param}`) : null;
        if (!body) return;

        // Auto-open if selection exists on load
        if (trigger.classList.contains("has-selection")) {
            body.classList.add("open");
            trigger.classList.add("open");
        }

        trigger.addEventListener("click", () => {
            if (trigger.disabled) return;
            const isOpen = body.classList.contains("open");
            body.classList.toggle("open", !isOpen);
            trigger.classList.toggle("open", !isOpen);
        });
    });

});
