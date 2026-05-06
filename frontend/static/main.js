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
        window.ZIP_COORDS = readJsonScript("ds-listings-zip-coords", {});
        window.DEALER_COORDS = readJsonScript("ds-listings-dealer-coords", {});
        window.INITIAL_GRID_CARS = readJsonScript("ds-listings-initial-grid", []);
        window.PACKAGE_ROWS = readJsonScript("ds-listings-package-rows", []);
    })();

    // Haversine formula: calculate distance in miles between two lat/lon points
    window.haversineJS = function(lat1, lon1, lat2, lon2) {
        const R = 3958.8; // Earth's radius in miles
        const toRad = Math.PI / 180;
        const lat1Rad = lat1 * toRad;
        const lat2Rad = lat2 * toRad;
        const dlat = (lat2 - lat1) * toRad;
        const dlon = (lon2 - lon1) * toRad;
        const a = Math.sin(dlat / 2) ** 2 + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.sin(dlon / 2) ** 2;
        return R * 2 * Math.asin(Math.sqrt(a));
    };

    // Look up coordinates for a ZIP code from preloaded data
    window.zipCoordsJS = function(zipCode) {
        if (!zipCode || typeof ZIP_COORDS !== "object") return null;
        const coords = ZIP_COORDS[String(zipCode).trim()];
        return Array.isArray(coords) && coords.length === 2 ? coords : null;
    };

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

    /** Listings filters use paint-family bucket ids (e.g. red); car rows expose *_color_families arrays. */
    function carMatchesPaintFamilyBuckets(car, param, selected) {
        if (!selected.length) return true;
        const key = param === "exterior_color" ? "exterior_color_families" : "interior_color_families";
        const fams = Array.isArray(car[key]) ? car[key] : [];
        return selected.some((s) => fams.includes(s));
    }

    // Collect unique checked values (pill + accordion share names, deduplicate)
    function checked(name) {
        const seen = new Set();
        return [...document.querySelectorAll(`input[name="${name}"]:checked`)]
            .map(cb => cb.value)
            .filter(v => seen.has(v) ? false : seen.add(v));
    }

    let RADIUS_CAR_ROWS = null; // non-null when ZIP+radius are active; cascade uses this subset

    function compatibleRows(excluding, alsoExclude = []) {
        const skip = v => v === excluding || alsoExclude.includes(v);
        const makes  = skip("make")       ? [] : checked("make");
        const models = skip("model")      ? [] : checked("model");
        const trims  = skip("trim")       ? [] : checked("trim");
        const fuels  = skip("fuel_type")  ? [] : checked("fuel_type");
        const drives = skip("drivetrain") ? [] : checked("drivetrain");
        const bodies = skip("body_style") ? [] : checked("body_style");
        const cyls   = skip("cylinders")  ? [] : checked("cylinders");

        return (RADIUS_CAR_ROWS || CAR_ROWS).filter(r => {
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
        // Exclude model from fuel_type compat so selecting an electric model doesn't hide gas options
        cascadeParam("fuel_type",   r => r.fuel,        ["options-fuel_type",   "acc-options-fuel_type"], ["model"]);
        cascadeParam("drivetrain",  r => r.drive,       ["options-drivetrain",  "acc-options-drivetrain"]);
        cascadeParam(
            "body_style",
            r => (r.body_style != null && String(r.body_style).trim() !== "" ? String(r.body_style) : ""),
            ["options-body_style", "acc-options-body_style"]
        );
        cascadeParam("cylinders",   r => String(r.cyl), ["options-cylinders",   "acc-options-cylinders"]);
        cascadePackages();
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

    function cascadePackages() {
        const activeMakes  = checked("make").map(s => s.toLowerCase());
        const activeModels = checked("model").map(s => s.toLowerCase());
        // Packages to show: those belonging to any selected make AND model (or all if none selected)
        let visibleNames;
        if (activeMakes.length || activeModels.length) {
            visibleNames = new Set(
                PACKAGE_ROWS
                    .filter(r =>
                        (!activeMakes.length  || activeMakes.includes(r.make.toLowerCase())) &&
                        (!activeModels.length || activeModels.includes(r.model.toLowerCase()))
                    )
                    .map(r => r.name.toLowerCase())
            );
        } else {
            visibleNames = new Set(PACKAGE_ROWS.map(r => r.name.toLowerCase()));
        }
        for (const cid of ["options-package", "acc-options-package"]) {
            const container = document.getElementById(cid);
            if (!container) continue;
            for (const label of container.querySelectorAll("label.filter-option")) {
                const input = label.querySelector("input");
                if (!input) continue;
                const hidden = visibleNames.size > 0 && !visibleNames.has(input.value.toLowerCase());
                label.style.display = hidden ? "none" : "";
                if (hidden && input.checked) { input.checked = false; }
            }
        }
    }

    function cascadeParam(param, rowKey, containerIds, alsoExclude = []) {
        const compatible = new Set(compatibleRows(param, alsoExclude).map(rowKey));
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
        // Only show the "Electric" collapsed state when no model is explicitly chosen;
        // if the user picked a specific electric model they can still browse other fuel types.
        const noModelsSelected = checked("model").length === 0;
        const showElectricMode = allElectric && noModelsSelected;

        // Update both pill trigger and accordion trigger
        ["trigger-cylinders", "acc-trigger-cylinders"].forEach(id => {
            const trigger = document.getElementById(id);
            const labelEl = document.getElementById(
                id === "trigger-cylinders" ? "label-cylinders" : "acc-label-cylinders"
            );
            const chevron = trigger ? trigger.querySelector(".pill-chevron, .acc-chevron") : null;

            if (!trigger || !labelEl) return;

            if (showElectricMode) {
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

        // Auto-check the 0-cyl box only when at make level (no model selected) and all-electric
        document.querySelectorAll("input[name='cylinders']").forEach(cb => {
            if (showElectricMode) cb.checked = (cb.value === "0");
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
         "transmission", "drivetrain", "body_style", "exterior_color", "interior_color", "package"]
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
        const isGeo = el.name === "zip_code" || el.name === "radius";
        el.addEventListener("change", isGeo ? refreshRadiusAndRender : renderResults);
        el.addEventListener("input",  isGeo ? refreshRadiusAndRender : renderResults);
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
            const incompletePill = c.public_incomplete
                ? `<span class="result-incomplete-pill" title="Missing some public-listing fields">Incomplete</span>`
                : "";
            return `
            <a href="/car/${idStr}" class="result-card${c.public_incomplete ? " result-card--incomplete" : ""}">
                <div class="result-image-wrap">
                    <div class="result-image" style="background-image:url('${imgSrcQuoted}')"></div>
                    ${photoLabel ? `<span class="result-photo-count">${escapeHtml(photoLabel)}</span>` : ""}
                </div>
                <div class="result-content">
                    <div class="result-title-row">
                        <h2>${escapeHtml(c.title)}</h2>
                        ${incompletePill}
                    </div>
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

    let _listingsGeoPersistTimer = null;
    let _listingsGeoLastSent = null;
    function schedulePersistListingsGeoSession() {
        const zip = scalarVal("zip_code");
        const radius = scalarVal("radius");
        if (!zip || !radius) return;
        const r = parseFloat(radius);
        if (!Number.isFinite(r) || r <= 0) return;
        const payload = `${zip.trim()}|${radius}`;
        if (payload === _listingsGeoLastSent) return;
        clearTimeout(_listingsGeoPersistTimer);
        _listingsGeoPersistTimer = setTimeout(() => {
            const m = document.querySelector('meta[name="csrf-token"]');
            const csrf = m && m.content ? m.content : "";
            fetch("/api/session/listings-geo", {
                method: "POST",
                credentials: "same-origin",
                headers: {
                    "Content-Type": "application/json",
                    ...(csrf ? { "X-CSRF-Token": csrf } : {}),
                },
                body: JSON.stringify({ zip_code: zip.trim(), radius }),
            })
                .then((res) => (res.ok ? res.json() : null))
                .then((data) => {
                    if (data && data.ok) _listingsGeoLastSent = payload;
                })
                .catch(() => {});
        }, 500);
    }

    function syncUrl() {
        const params = new URLSearchParams();
        const multiParams = ["make", "model", "trim", "fuel_type", "cylinders", "transmission",
                             "drivetrain", "body_style", "exterior_color", "interior_color", "country", "package"];
        for (const name of multiParams) {
            const seen = new Set();
            document.querySelectorAll(`input[name="${name}"]:checked`).forEach(cb => {
                const opt = cb.closest(".filter-option");
                if (opt && opt.style.display === "none") return;
                if (!seen.has(cb.value)) {
                    seen.add(cb.value);
                    params.append(name, cb.value);
                }
            });
        }
        for (const name of ["zip_code", "radius", "max_price", "max_mileage", "engine_l_min", "engine_l_max"]) {
            const val = scalarVal(name);
            if (val) params.set(name, val);
        }
        const smartIn = document.getElementById("smart-search-input");
        const q = smartIn ? (smartIn.value || "").trim() : "";
        if (q) params.set("q", q);
        const qs = params.toString();
        history.replaceState(null, "", window.location.pathname + (qs ? "?" + qs : ""));
        schedulePersistListingsGeoSession();
    }

    function renderResults() {
        if (!resultsGrid) return;

        const smartIn = document.getElementById("smart-search-input");
        if (smartIn && (smartIn.value || "").trim()) return;

        syncUrl();

        const makes       = checked("make");
        const models      = checked("model");
        const zipCode     = scalarVal("zip_code");
        const radiusMi    = parseFloat(scalarVal("radius"))      || null;

        // Require: ZIP, positive radius, and at least one make
        const hasAllRequired = zipCode && radiusMi && radiusMi > 0 && makes.length > 0;

        if (!hasAllRequired) {
            resultsGrid.innerHTML = "";
            if (emptyState) {
                emptyState.style.display = "";
                emptyState.querySelector(".no-results").textContent = "Enter a ZIP code, radius, and make to search";
                emptyState.querySelector(".no-results-sub").textContent = "";
            }
            if (resultsCount) resultsCount.textContent = "";
            return;
        }
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
        const engLMin     = parseFloat(scalarVal("engine_l_min")) || null;
        const engLMax     = parseFloat(scalarVal("engine_l_max")) || null;
        const pkgs        = checked("package");

        function parseEngineLiters(c) {
            const raw = c.engine_l;
            if (raw != null && String(raw).trim()) {
                const s = String(raw).trim().toLowerCase();
                if (s === "electric" || s === "phev") return null;
                const v = parseFloat(s.replace(/l/gi, ""));
                if (Number.isFinite(v) && v > 0) return v;
            }
            const ed = c.engine_description;
            if (typeof ed === "string" && ed.trim()) {
                const m = ed.match(/(\d+\.\d+|\d+)\s*[lL]\b/);
                if (m) {
                    const v = parseFloat(m[1]);
                    if (Number.isFinite(v) && v > 0) return v;
                }
            }
            return null;
        }

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
            if (fuels.length      && !valueInListCI(fuels, c.fuel_type))       return false;
            if (cyls.length       && !cyls.includes(String(c.cylinders)))      return false;
            if (trans.length      && !valueInListCI(trans, c.transmission))    return false;
            if (drives.length     && !valueInListCI(drives, c.drivetrain))     return false;
            if (bodies.length     && !valueInListCI(bodies, c.body_style)) return false;
            if (extColors.length  && !carMatchesPaintFamilyBuckets(c, "exterior_color", extColors)) return false;
            if (intColors.length  && !carMatchesPaintFamilyBuckets(c, "interior_color", intColors)) return false;
            if (pkgs.length) {
                const carPkgs = (c.package_names || []).map(n => n.toLowerCase());
                if (!pkgs.some(p => carPkgs.includes(p.toLowerCase()))) return false;
            }
            if (maxPrice    && c.price   > maxPrice)   return false;
            if (maxMileage  && c.mileage > maxMileage) return false;
            if (engLMin != null || engLMax != null) {
                const disp = parseEngineLiters(c);
                if (disp == null) return false;
                if (engLMin != null && disp < engLMin) return false;
                if (engLMax != null && disp > engLMax) return false;
            }
            return true;
        });

        if (zipCode && radiusMi && typeof haversineJS === "function") {
            const applyRadius = (origin) => {
                if (!origin) {
                    resultsGrid.innerHTML = "";
                    if (emptyState) {
                        emptyState.style.display = "";
                        emptyState.querySelector(".no-results").textContent = "ZIP code not found — no results shown.";
                        emptyState.querySelector(".no-results-sub").textContent = "Check the ZIP and try again.";
                    }
                    if (resultsCount) resultsCount.textContent = "";
                    return;
                }
                const filtered = cars.filter(c => {
                    // Primary: dealer's geocoded lat/lon from dealer_geopoints
                    let coords = (typeof DEALER_COORDS === "object" && c.dealer_url)
                        ? (DEALER_COORDS[String(c.dealer_url).trim()] || null) : null;
                    // Fallback: car's own zip_code (for dealers not yet geocoded)
                    if (!coords && c.zip_code && typeof ZIP_COORDS === "object") {
                        coords = ZIP_COORDS[String(c.zip_code).trim()] || null;
                    }
                    if (!coords) return false;
                    return haversineJS(origin[0], origin[1], coords[0], coords[1]) <= radiusMi;
                });
                renderCarGrid(filtered);
            };

            // Try preloaded ZIP_COORDS first (fast), else fetch from API
            let origin = zipCoordsJS(zipCode);
            if (origin) {
                applyRadius(origin);
            } else {
                fetch(`/api/zip-coords?zip=${encodeURIComponent(zipCode)}`, { credentials: "same-origin" })
                    .then(r => r.ok ? r.json() : null)
                    .then(data => applyRadius(data && data.lat != null ? [data.lat, data.lon] : null))
                    .catch(() => applyRadius(null));
                return; // renderCarGrid called inside applyRadius
            }
            return;
        }

        renderCarGrid(cars);
    }

    window.__DS_renderCarGrid = renderCarGrid;
    window.__DS_runFilterRender = renderResults;

    // ── Radius-aware cascade ───────────────────────────────────────────
    // Build a make/model/trim/etc. row set restricted to cars within the
    // active ZIP+radius so that filter dropdowns only show options that
    // actually have inventory nearby.

    function _buildCarRowsFromCars(cars) {
        const seen = new Set();
        const rows = [];
        for (const c of cars) {
            const key = [c.make, c.model, c.trim, c.fuel_type,
                         c.cylinders, c.drivetrain, c.body_style].join("\x00");
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
            });
        }
        return rows;
    }

    function _setRadiusCarRows(origin, radiusMi) {
        if (!origin || !radiusMi) {
            RADIUS_CAR_ROWS = null;
            return;
        }
        const nearby = ALL_CARS.filter(c => {
            let coords = (typeof DEALER_COORDS === "object" && c.dealer_url)
                ? (DEALER_COORDS[String(c.dealer_url).trim()] || null) : null;
            if (!coords && c.zip_code && typeof ZIP_COORDS === "object") {
                coords = ZIP_COORDS[String(c.zip_code).trim()] || null;
            }
            if (!coords) return false;
            return haversineJS(origin[0], origin[1], coords[0], coords[1]) <= radiusMi;
        });
        RADIUS_CAR_ROWS = _buildCarRowsFromCars(nearby);
    }

    function refreshRadiusAndRender() {
        const zipCode  = scalarVal("zip_code");
        const radiusMi = parseFloat(scalarVal("radius")) || null;

        if (!zipCode || !radiusMi) {
            RADIUS_CAR_ROWS = null;
            runCascade();
            renderResults();
            return;
        }

        const origin = zipCoordsJS(zipCode);
        if (origin) {
            _setRadiusCarRows(origin, radiusMi);
            runCascade();
            renderResults();
        } else {
            fetch(`/api/zip-coords?zip=${encodeURIComponent(zipCode)}`, { credentials: "same-origin" })
                .then(r => r.ok ? r.json() : null)
                .then(data => {
                    const o = data && data.lat != null ? [data.lat, data.lon] : null;
                    _setRadiusCarRows(o, radiusMi);
                    runCascade();
                    renderResults();
                })
                .catch(() => {
                    _setRadiusCarRows(null, radiusMi);
                    runCascade();
                    renderResults();
                });
        }
    }

    if (typeof INITIAL_GRID_CARS !== "undefined" && Array.isArray(INITIAL_GRID_CARS) && INITIAL_GRID_CARS.length) {
        runCascade();
        renderCarGrid(INITIAL_GRID_CARS);
    } else {
        // refreshRadiusAndRender builds the radius-filtered cascade row set first,
        // then runs cascade + renderResults — picks up URL-param pre-checked filters too.
        refreshRadiusAndRender();
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

    // Poll inventory JSON while a scan writes to inventory.db (LISTINGS_CLIENT_POLL_MS, e.g. 8000).
    const pollAttr = document.body && document.body.getAttribute("data-listings-poll-ms");
    const pollMs = pollAttr != null ? parseInt(pollAttr, 10) : 0;
    if (Number.isFinite(pollMs) && pollMs > 0) {
        setInterval(() => {
            fetch("/api/listings/cars", { credentials: "same-origin" })
                .then((r) => (r.ok ? r.json() : Promise.reject()))
                .then((data) => {
                    if (!data || !data.ok || !Array.isArray(data.cars)) return;
                    window.ALL_CARS = data.cars;
                    const smartIn = document.getElementById("smart-search-input");
                    if (smartIn && (smartIn.value || "").trim()) return;
                    if (typeof window.__DS_runFilterRender === "function") {
                        window.__DS_runFilterRender();
                    }
                })
                .catch(() => {});
        }, pollMs);
    }

});
