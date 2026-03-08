document.addEventListener("DOMContentLoaded", () => {

    // Fade in
    document.body.style.opacity = 0;
    setTimeout(() => {
        document.body.style.transition = "opacity 0.5s ease";
        document.body.style.opacity = 1;
    }, 50);

    // ── Helpers ────────────────────────────────────────────────────────

    function checked(name) {
        return [...document.querySelectorAll(`input[name="${name}"]:checked`)]
            .map(cb => cb.value);
    }

    // Given current active selections, return which car rows from CAR_ROWS
    // are still compatible — ignoring the filter dimension being tested.
    function compatibleRows(excluding) {
        const makes  = excluding === "make"      ? [] : checked("make");
        const models = excluding === "model"     ? [] : checked("model");
        const trims  = excluding === "trim"      ? [] : checked("trim");
        const fuels  = excluding === "fuel_type" ? [] : checked("fuel_type");
        const drives = excluding === "drivetrain"? [] : checked("drivetrain");
        const cyls   = excluding === "cylinders" ? [] : checked("cylinders");

        return CAR_ROWS.filter(r => {
            if (makes.length  && !makes.includes(r.make))          return false;
            if (models.length && !models.includes(r.model))        return false;
            if (trims.length  && !trims.includes(r.trim))          return false;
            if (fuels.length  && !fuels.includes(r.fuel))          return false;
            if (drives.length && !drives.includes(r.drive))        return false;
            if (cyls.length   && !cyls.includes(String(r.cyl)))    return false;
            return true;
        });
    }

    // ── Cascade engine ─────────────────────────────────────────────────
    // Called after ANY filter changes. Recomputes visibility for every
    // filterable dropdown based on what's still reachable in CAR_ROWS.

    function runCascade() {
        cascadeOptions("make",       r => r.make,  "options-make");
        cascadeOptions("model",      r => r.model, "options-model");
        cascadeOptions("trim",       r => r.trim,  "options-trim");
        cascadeOptions("fuel_type",  r => r.fuel,  "options-fuel_type");
        cascadeOptions("drivetrain", r => r.drive, "options-drivetrain");
        cascadeOptions("cylinders",  r => String(r.cyl), "options-cylinders");
        updateCylindersButton();
        updateAllCounts();
    }

    function cascadeOptions(param, rowKey, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const compatible = new Set(compatibleRows(param).map(rowKey));

        container.querySelectorAll(".filter-option").forEach(label => {
            const cb = label.querySelector("input");
            const visible = compatible.has(cb.value);
            label.style.display = visible ? "" : "none";
            if (!visible) cb.checked = false;
        });
    }

    // ── Cylinders button: collapse to "Electric" when context is all-electric ──

    function updateCylindersButton() {
        const trigger  = document.getElementById("trigger-cylinders");
        const labelEl  = document.getElementById("label-cylinders");
        const chevron  = document.getElementById("chevron-cylinders");
        const dropdown = document.getElementById("dropdown-cylinders");
        if (!trigger || !labelEl) return;

        // Rows still reachable ignoring cylinders filter
        const rows = compatibleRows("cylinders");
        const allElectric = rows.length > 0 && rows.every(r => r.cyl === 0);

        if (allElectric) {
            labelEl.textContent = "Electric";
            trigger.classList.add("has-selection", "electric-mode");
            trigger.disabled = true;
            chevron.style.display = "none";
            dropdown.classList.remove("open");
            // Auto-check the 0-cyl option and uncheck others
            document.querySelectorAll("input[name='cylinders']").forEach(cb => {
                cb.checked = (cb.value === "0");
            });
        } else {
            labelEl.textContent = "Cylinders";
            trigger.classList.remove("has-selection", "electric-mode");
            trigger.disabled = false;
            chevron.style.display = "";
            // Uncheck the electric option if non-electric context
            const hasNonElectricChecked = checked("make").length > 0 || checked("fuel_type").length > 0;
            if (hasNonElectricChecked) {
                document.querySelectorAll("input[name='cylinders'][value='0']").forEach(cb => {
                    cb.checked = false;
                });
            }
        }
    }

    // ── Badge counts ───────────────────────────────────────────────────

    function updateCount(param) {
        const dropdown = document.getElementById(`dropdown-${param}`);
        const trigger  = document.querySelector(`.filter-trigger[data-param="${param}"]`);
        const countEl  = document.getElementById(`count-${param}`);
        if (!dropdown || !countEl || !trigger) return;

        const visibleChecked = [...dropdown.querySelectorAll("input:checked")]
            .filter(cb => cb.closest(".filter-option").style.display !== "none").length;

        if (visibleChecked > 0) {
            countEl.textContent = visibleChecked;
            countEl.style.display = "inline";
            if (!trigger.classList.contains("electric-mode")) {
                trigger.classList.add("has-selection");
            }
        } else {
            countEl.style.display = "none";
            if (!trigger.classList.contains("electric-mode")) {
                trigger.classList.remove("has-selection");
            }
        }
    }

    function updateAllCounts() {
        ["make", "model", "trim", "fuel_type", "cylinders",
         "transmission", "drivetrain", "exterior_color", "interior_color"]
            .forEach(updateCount);
    }

    // ── Wire all checkboxes → cascade ──────────────────────────────────

    document.querySelectorAll(".filter-option input[type=checkbox]").forEach(cb => {
        cb.addEventListener("change", runCascade);
    });

    // Run on load to restore state after a search POST
    runCascade();

    // ── Dropdown open/close ────────────────────────────────────────────

    document.querySelectorAll(".filter-trigger").forEach(trigger => {
        const param    = trigger.dataset.param;
        const dropdown = document.getElementById(`dropdown-${param}`);
        if (!dropdown) return;

        trigger.addEventListener("click", (e) => {
            if (trigger.disabled) return;
            e.stopPropagation();
            const isOpen = dropdown.classList.contains("open");

            document.querySelectorAll(".filter-dropdown.open").forEach(d => d.classList.remove("open"));
            document.querySelectorAll(".filter-trigger.open").forEach(t => t.classList.remove("open"));

            if (!isOpen) {
                dropdown.classList.add("open");
                trigger.classList.add("open");
            }
        });
    });

    document.addEventListener("click", () => {
        document.querySelectorAll(".filter-dropdown.open").forEach(d => d.classList.remove("open"));
        document.querySelectorAll(".filter-trigger.open").forEach(t => t.classList.remove("open"));
    });

    document.querySelectorAll(".filter-dropdown").forEach(d => {
        d.addEventListener("click", e => e.stopPropagation());
    });

});
