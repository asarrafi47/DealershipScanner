/**
 * Listings facet math: the option-row table, which rows stay compatible with
 * the other checked facets, cascade/lazy-facet identity keys, and which
 * package names a make/model selection leaves visible.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL; main.js keeps the DOM side (reading checked boxes,
 * hiding options, the per-cascade cache). Uses SC.normFilterStr and
 * SC.valueInListCI from sc-helpers.js (resolved at call time).
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});

    /**
     * Rows of the option table (CAR_ROWS shape) that pass the given facet
     * selections. `sel` = {makes, models, trims, fuels, drives, inductions,
     * bodies, cyls}; an empty list means "not filtering on that facet".
     */
    function filterCompatibleRows(rows, sel) {
        const { makes, models, trims, fuels, drives, inductions, bodies, cyls } = sel;
        return rows.filter(r => {
            if (makes.length  && !SC.valueInListCI(makes, r.make))        return false;
            if (models.length && !SC.valueInListCI(models, r.model))      return false;
            if (trims.length  && !SC.valueInListCI(trims, r.trim))        return false;
            if (fuels.length  && !fuels.includes(r.fuel))        return false;
            if (drives.length && !drives.includes(r.drive))      return false;
            if (inductions.length && !inductions.includes(r.induction)) return false;
            if (bodies.length && !SC.valueInListCI(bodies, r.body_style)) return false;
            if (cyls.length   && !cyls.includes(String(r.cyl))) return false;
            return true;
        });
    }

    /** Package names to show for the selected makes/models (all when none selected). */
    function visiblePackageNames(packageRows, activeMakes, activeModels) {
        // Packages to show: those belonging to any selected make AND model (or all if none selected)
        let visibleNames;
        if (activeMakes.length || activeModels.length) {
            visibleNames = new Set(
                packageRows
                    .filter(r =>
                        (!activeMakes.length  || activeMakes.includes(r.make.toLowerCase())) &&
                        (!activeModels.length || activeModels.includes(r.model.toLowerCase()))
                    )
                    .map(r => r.name.toLowerCase())
            );
        } else {
            visibleNames = new Set(packageRows.map(r => r.name.toLowerCase()));
        }
        return visibleNames;
    }

    function cascadeRowKey(param, r) {
        if (param === "trim") {
            return [SC.normFilterStr(r.make), SC.normFilterStr(r.model), SC.normFilterStr(r.trim)].join("\0");
        }
        if (param === "model") {
            return [SC.normFilterStr(r.make), SC.normFilterStr(r.model)].join("\0");
        }
        return SC.normFilterStr(r.make ?? r.model ?? r.trim ?? r.fuel ?? r.drive ?? r.induction ?? r.body_style ?? r.cyl ?? "");
    }

    /** Key for a rendered option: `label` supplies data-make/data-model, `cb` the value. */
    function cascadeOptionKey(param, label, cb) {
        const make = label && label.dataset ? label.dataset.make : "";
        const model = label && label.dataset ? label.dataset.model : "";
        if (param === "trim") {
            return [SC.normFilterStr(make), SC.normFilterStr(model), SC.normFilterStr(cb.value)].join("\0");
        }
        if (param === "model") {
            return [SC.normFilterStr(make), SC.normFilterStr(cb.value)].join("\0");
        }
        return SC.normFilterStr(cb.value);
    }

    /** Container-agnostic identity for a lazy option, used to carry checked state across a rebuild. */
    function lazyFacetEntryKey(param, entry) {
        if (param === "trim") {
            return [SC.normFilterStr(entry[0]), SC.normFilterStr(entry[1]), SC.normFilterStr(entry[2] || "")].join("\0");
        }
        if (param === "model") return [SC.normFilterStr(entry[0]), SC.normFilterStr(entry[1])].join("\0");
        return SC.normFilterStr(entry);
    }

    function lazyFacetEntries(param, facets) {
        if (param === "model") return Array.isArray(facets.model_rows) ? facets.model_rows : [];
        if (param === "trim") return Array.isArray(facets.trim_rows) ? facets.trim_rows : [];
        return Array.isArray(facets.all_package_names) ? facets.all_package_names : [];
    }

    // Build a make/model/trim/etc. row set restricted to cars within the
    // active ZIP+radius so that filter dropdowns only show options that
    // actually have inventory nearby.
    function buildCarRowsFromCars(cars) {
        const seen = new Set();
        const rows = [];
        for (const c of cars) {
            const key = [c.make, c.model, c.trim, c.fuel_type,
                         c.cylinders, c.drivetrain, c.body_style, c.forced_induction].join("\x00");
            if (seen.has(key)) continue;
            seen.add(key);
            rows.push({
                make:       c.make        || "",
                model:      c.model       || "",
                trim:       c.trim        || null,
                fuel:       c.fuel_type   || null,
                cyl:        c.cylinders != null ? Number(c.cylinders) : null,
                drive:      c.drivetrain  || null,
                body_style: c.body_style  || null,
                induction:  c.forced_induction || null,
            });
        }
        return rows;
    }

    DSL.filterCompatibleRows = filterCompatibleRows;
    DSL.visiblePackageNames = visiblePackageNames;
    DSL.cascadeRowKey = cascadeRowKey;
    DSL.cascadeOptionKey = cascadeOptionKey;
    DSL.lazyFacetEntryKey = lazyFacetEntryKey;
    DSL.lazyFacetEntries = lazyFacetEntries;
    DSL.buildCarRowsFromCars = buildCarRowsFromCars;
})(window);
