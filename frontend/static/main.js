document.addEventListener("DOMContentLoaded", () => {

    // Fade in
    document.body.style.opacity = 0;
    setTimeout(() => {
        document.body.style.transition = "opacity 0.5s ease";
        document.body.style.opacity = 1;
    }, 50);

    if (typeof CAR_ROWS === "undefined") return;

    // ── Scroll: dock filter bar when user scrolls past threshold ──────

    const DOCK_THRESHOLD = 80; // px scrolled before docking
    const filterBar = document.getElementById("filter-bar");
    const page = document.querySelector(".page");

    function onScroll() {
        const docked = window.scrollY > DOCK_THRESHOLD;
        filterBar.classList.toggle("docked", docked);
        page.classList.toggle("docked", docked);
    }

    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll(); // run once on load (e.g. if page is refreshed mid-scroll)

    // ── Helpers ────────────────────────────────────────────────────────

    // Collect checked values from BOTH pill inputs and docked accordion inputs
    function checked(name) {
        return [...document.querySelectorAll(`input[name="${name}"]:checked`)]
            .map(cb => cb.value);
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
        const visibleChecked = [...document.querySelectorAll(`input[name="${param}"]:checked`)]
            .filter(cb => {
                const opt = cb.closest(".filter-option");
                return opt ? opt.style.display !== "none" : true;
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

    // ── Wire all checkboxes → cascade ──────────────────────────────────

    document.querySelectorAll(".filter-option input[type=checkbox]").forEach(cb => {
        cb.addEventListener("change", runCascade);
    });

    runCascade();

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
