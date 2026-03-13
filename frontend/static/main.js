document.addEventListener("DOMContentLoaded", () => {

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

    import("https://cdn.jsdelivr.net/npm/motion@latest/+esm").then(({ animate }) => {
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
        const cyls   = excluding === "cylinders"  ? [] : checked("cylinders");

        return CAR_ROWS.filter(r => {
            if (makes.length  && !makes.includes(r.make))        return false;
            if (models.length && !models.includes(r.model))      return false;
            if (trims.length  && !trims.includes(r.trim))        return false;
            if (fuels.length  && !fuels.includes(r.fuel))        return false;
            if (drives.length && !drives.includes(r.drive))      return false;
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
        cascadeParam("model",       r => r.model,       ["options-model",       "acc-options-model"]);
        cascadeParam("trim",        r => r.trim,        ["options-trim",        "acc-options-trim"]);
        cascadeParam("fuel_type",   r => r.fuel,        ["options-fuel_type",   "acc-options-fuel_type"]);
        cascadeParam("drivetrain",  r => r.drive,       ["options-drivetrain",  "acc-options-drivetrain"]);
        cascadeParam("cylinders",   r => String(r.cyl), ["options-cylinders",   "acc-options-cylinders"]);
        updateCylinders();
        updateAllCounts();
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
        ["make", "model", "trim", "fuel_type", "cylinders",
         "transmission", "drivetrain", "exterior_color", "interior_color"]
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
    function fmtUSD(n) { return "$" + Number(n).toLocaleString("en-US", {maximumFractionDigits: 0}); }

    function renderResults() {
        if (!resultsGrid) return;

        const makes       = checked("make");
        const models      = checked("model");
        const trims       = checked("trim");
        const fuels       = checked("fuel_type");
        const cyls        = checked("cylinders");
        const trans       = checked("transmission");
        const drives      = checked("drivetrain");
        const extColors   = checked("exterior_color");
        const intColors   = checked("interior_color");
        const maxPrice    = parseFloat(scalarVal("max_price"))   || null;
        const maxMileage  = parseInt(scalarVal("max_mileage"))   || null;
        const zipCode     = scalarVal("zip_code");
        const radiusMi    = parseFloat(scalarVal("radius"))      || null;

        let cars = ALL_CARS.filter(c => {
            if (makes.length      && !makes.includes(c.make))            return false;
            if (models.length     && !models.includes(c.model))          return false;
            if (trims.length      && !trims.includes(c.trim))            return false;
            if (fuels.length      && !fuels.includes(c.fuel_type))       return false;
            if (cyls.length       && !cyls.includes(String(c.cylinders)))return false;
            if (trans.length      && !trans.includes(c.transmission))    return false;
            if (drives.length     && !drives.includes(c.drivetrain))     return false;
            if (extColors.length  && !extColors.includes(c.exterior_color)) return false;
            if (intColors.length  && !intColors.includes(c.interior_color)) return false;
            if (maxPrice    && c.price   > maxPrice)   return false;
            if (maxMileage  && c.mileage > maxMileage) return false;
            return true;
        });

        // Geo filter (zip + radius) — only if both provided and haversine available
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

        // Render cards
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
            const cylLabel = c.cylinders === 0 ? "Electric" : `${c.cylinders}-cyl`;
            return `
            <a href="/car/${c.id}" class="result-card">
                <div class="result-image" style="background-image:url('${c.image_url || ""}')"></div>
                <div class="result-content">
                    <h2>${c.title}</h2>
                    <p class="result-trim">${c.trim || ""}</p>
                    <p class="result-price">${fmtUSD(c.price)}</p>
                    <p class="result-meta">
                        ${fmt(c.mileage)} mi
                        &middot; ${c.fuel_type || ""}
                        &middot; ${c.drivetrain || ""}
                    </p>
                    <p class="result-dealer">${c.dealer_name || ""}</p>
                </div>
            </a>`;
        }).join("");
    }

    runCascade();
    renderResults();

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
