document.addEventListener("DOMContentLoaded", () => {
    // The listings boot (JSON blob unpack), market-intel cohort helpers and
    // geo helpers were extracted to listings_boot.js, market_intel.js and
    // geo.js — loaded before this file (see listings.html / dealership.html).
    // They share the window.SC namespace from sc-helpers.js; this closure
    // publishes SC.scalarVal and SC.clearListingsRadiusCache back to them
    // (see the bridge block below the listings-page guard).

    function startListingsAssetPrefetch() {
        if (!document.getElementById("ds-listings-car-rows")) return;
        if (window.__DS_listingsAssetPrefetchStarted) return;
        window.__DS_listingsAssetPrefetchStarted = true;

        if (!SC.listingsDealerCoordsReady()) {
            if (window.__DS_listingsGeoPrefetchPromise) {
                window.__DS_listingsGeoPrefetchPromise = window.__DS_listingsGeoPrefetchPromise.then(() => {
                    if (SC.listingsDealerCoordsReady()) return;
                    return fetch("/api/listings/geo-coords", { credentials: "same-origin" })
                        .then((r) => (r.ok ? r.json() : null))
                        .then((data) => SC.mergeListingsGeoCoordsPayload(data));
                }).catch(() => {});
            } else {
                window.__DS_listingsGeoPrefetchPromise = fetch("/api/listings/geo-coords", {
                    credentials: "same-origin",
                })
                    .then((r) => (r.ok ? r.json() : null))
                    .then((data) => {
                        SC.mergeListingsGeoCoordsPayload(data);
                    })
                    .catch(() => {});
            }
        } else {
            window.__DS_listingsGeoCoordsReady = true;
        }

        if (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length) {
            if (Array.isArray(window.__DS_prefetchCars) && window.__DS_prefetchCars.length) {
                applyListingsCarsPayload({ ok: true, cars: window.__DS_prefetchCars });
            } else if (window.__DS_listingsCarsPrefetchPromise) {
                window.__DS_listingsCarsPrefetchPromise = window.__DS_listingsCarsPrefetchPromise
                    .then(() => {
                        if (
                            Array.isArray(window.__DS_prefetchCars)
                            && window.__DS_prefetchCars.length
                            && (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length)
                        ) {
                            applyListingsCarsPayload({ ok: true, cars: window.__DS_prefetchCars });
                        }
                    })
                    .catch(() => {});
            } else {
            const headers = {};
            if (window.__DS_listingsCarsEtag) {
                headers["If-None-Match"] = window.__DS_listingsCarsEtag;
            }
            window.__DS_listingsCarsPrefetchPromise = fetch("/api/listings/cars", {
                credentials: "same-origin",
                headers,
            })
                .then((r) => {
                    if (r.status === 304) {
                        if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
                            return { ok: true, cars: window.ALL_CARS, unchanged: true };
                        }
                        return fetch("/api/listings/cars", { credentials: "same-origin" }).then((r2) => {
                            if (!r2.ok) throw new Error("cars prefetch failed");
                            const etag = r2.headers.get("ETag");
                            if (etag) window.__DS_listingsCarsEtag = etag;
                            return r2.json();
                        });
                    }
                    if (!r.ok) throw new Error("cars prefetch failed");
                    const etag = r.headers.get("ETag");
                    if (etag) window.__DS_listingsCarsEtag = etag;
                    return r.json();
                })
                .then((data) => {
                    if (!applyListingsCarsPayload(data)) {
                        throw new Error("cars prefetch invalid");
                    }
                })
                .catch(() => {});
            }
        }
    }

    const _dsListingsPage = !!document.getElementById("ds-listings-car-rows");

    // Hard dependency check: this file cannot run on a listings-style page
    // without the extracted modules. Fail loudly instead of half-rendering.
    if (
        _dsListingsPage &&
        (typeof SC === "undefined" ||
            typeof SC.enrichCarWithMarket !== "function" ||
            typeof SC.filterCarsInRadius !== "function")
    ) {
        console.error(
            "main.js: missing extracted modules — load listings_boot.js, " +
            "market_intel.js and geo.js before main.js (see listings.html)."
        );
        return;
    }

    if (!_dsListingsPage || typeof CAR_ROWS === "undefined") return;

    // Bridge closure functions to the extracted geo.js / market_intel.js files
    // (their call sites stay typeof-guarded). Function declarations hoist, so
    // both are initialized even though they are defined further down.
    SC.scalarVal = scalarVal;
    SC.clearListingsRadiusCache = clearListingsRadiusCache;

    startListingsAssetPrefetch();

    const dashContent = document.getElementById("dash-content");
    const filterTopRow = document.getElementById("filter-top-row");

    // ── Listings: smooth compact header (same pills/search UI — no sidebar swap / layout jump) ──
    const COMPACT_AT = 52;
    const EXPAND_AT = 28;
    let listingsCompact = false;

    function setListingsCompact(on) {
        if (listingsCompact === on) return;
        listingsCompact = on;
        if (filterTopRow) filterTopRow.classList.toggle("listings-filters-compact", on);
    }

    /** Sidebar layout: spacing comes from CSS — clear any legacy inline padding from older builds. */
    function syncListingsMainPadding() {
        if (!dashContent || !document.body.classList.contains("listings-page")) return;
        dashContent.style.marginLeft = "";
        dashContent.style.paddingTop = "";
    }

    function onListingsScrollCompact() {
        if (!filterTopRow || !document.body.classList.contains("listings-page")) return;
        const y = window.scrollY;
        if (!listingsCompact && y > COMPACT_AT) setListingsCompact(true);
        if (listingsCompact && y < EXPAND_AT) setListingsCompact(false);
    }

    if (document.body.classList.contains("listings-page") && filterTopRow && dashContent) {
        window.addEventListener(
            "scroll",
            () => {
                onListingsScrollCompact();
            },
            { passive: true },
        );
        requestAnimationFrame(() => {
            onListingsScrollCompact();
            syncListingsMainPadding();
        });
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /** Listings filters use paint-family bucket ids (e.g. red); car rows expose *_color_families arrays. */
    function carMatchesPaintFamilyBuckets(car, param, selected) {
        if (!selected.length) return true;
        const key = param === "exterior_color" ? "exterior_color_families" : "interior_color_families";
        const fams = Array.isArray(car[key]) ? car[key] : [];
        return selected.some((s) => fams.includes(s));
    }

    function carListingCondition(car) {
        const raw = car && car.condition != null ? String(car.condition).trim() : "";
        if (raw && raw !== "—" && raw !== "-") return raw.toLowerCase();
        const mi = car && car.mileage != null && car.mileage !== "" ? Number(car.mileage) : null;
        if (Number.isFinite(mi) && mi > 0) return "used";
        if (Number.isFinite(mi) && mi === 0) return "new";
        const yr = car && car.year != null ? Number(car.year) : null;
        if (Number.isFinite(yr) && yr < 2024) return "pre-owned";
        return "";
    }

    function passesInventoryConditionFilter(car, inventoryCondition) {
        if (!inventoryCondition) return true;
        const cond = carListingCondition(car);
        if (inventoryCondition === "new") return cond === "new";
        if (inventoryCondition === "pre_owned") {
            if (!cond) return false;
            return cond !== "new";
        }
        if (inventoryCondition === "cpo") return !!car.is_cpo;
        return true;
    }

    // Collect unique checked values (pill + accordion share names, deduplicate)
    let _checkedCache = null;
    let _compatRowCache = null;

    function invalidateCheckedCache() {
        _checkedCache = null;
    }

    function checked(name) {
        if (!_checkedCache) {
            _checkedCache = new Map();
            const seenByName = {};
            document.querySelectorAll('input[type=checkbox][name]').forEach((cb) => {
                if (!cb.checked || !cb.name) return;
                if (!seenByName[cb.name]) seenByName[cb.name] = new Set();
                if (seenByName[cb.name].has(cb.value)) return;
                seenByName[cb.name].add(cb.value);
                if (!_checkedCache.has(cb.name)) _checkedCache.set(cb.name, []);
                _checkedCache.get(cb.name).push(cb.value);
            });
        }
        return _checkedCache.get(name) || [];
    }

    let RADIUS_CAR_ROWS = null; // non-null when ZIP+radius are active; cascade uses this subset

    function compatibleRows(excluding, alsoExclude = []) {
        const cacheKey = `${excluding}|${alsoExclude.join(",")}`;
        if (_compatRowCache && _compatRowCache.has(cacheKey)) {
            return _compatRowCache.get(cacheKey);
        }
        const skip = v => v === excluding || alsoExclude.includes(v);
        const makes  = skip("make")       ? [] : checked("make");
        const models = skip("model")      ? [] : checked("model");
        const trims  = skip("trim")       ? [] : checked("trim");
        const fuels  = skip("fuel_type")  ? [] : checked("fuel_type");
        const drives = skip("drivetrain") ? [] : checked("drivetrain");
        const inductions = skip("forced_induction") ? [] : checked("forced_induction");
        const bodies = skip("body_style") ? [] : checked("body_style");
        const cyls   = skip("cylinders")  ? [] : checked("cylinders");

        // null => radius inactive (use all cars). An EMPTY array => radius is
        // active but nothing is in range — must show 0, NOT silently fall back
        // to the full national inventory (that produced the "flash of unrelated
        // results" while coords were still loading).
        const rows = (RADIUS_CAR_ROWS !== null ? RADIUS_CAR_ROWS : CAR_ROWS).filter(r => {
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
        if (_compatRowCache) _compatRowCache.set(cacheKey, rows);
        return rows;
    }

    // ── Cascade engine ─────────────────────────────────────────────────
    // Both pill dropdowns and accordion bodies share the same input names
    // so checking one automatically syncs the other — we just need to
    // cascade visibility across all containers with matching option ids.
    // Cascade runs when opening a filter panel, not on every checkbox tick.

    function runCascade() {
        invalidateCheckedCache();
        _compatRowCache = new Map();
        cascadeParam("make",        r => r.make,        ["options-make",        "acc-options-make"]);
        cascadeMakeByCountry();
        cascadeParam("model",       r => r.model,       ["options-model",       "acc-options-model"]);
        cascadeParam("trim",        r => r.trim,        ["options-trim",        "acc-options-trim"]);
        // Exclude model from fuel_type compat so selecting an electric model doesn't hide gas options
        cascadeParam("fuel_type",   r => r.fuel,        ["options-fuel_type",   "acc-options-fuel_type"], ["model"]);
        cascadeParam("drivetrain",  r => r.drive,       ["options-drivetrain",  "acc-options-drivetrain"]);
        cascadeParam("forced_induction", r => r.induction, ["options-forced_induction", "acc-options-forced_induction"]);
        cascadeParam(
            "body_style",
            r => (r.body_style != null && String(r.body_style).trim() !== "" ? String(r.body_style) : ""),
            ["options-body_style", "acc-options-body_style"]
        );
        cascadeParam("cylinders",   r => String(r.cyl), ["options-cylinders",   "acc-options-cylinders"]);
        cascadePackages();
        updateCylinders();
        updateAllCounts();
        _compatRowCache = null;
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

    function cascadeRowKey(param, r) {
        if (param === "trim") {
            return [SC.normFilterStr(r.make), SC.normFilterStr(r.model), SC.normFilterStr(r.trim)].join("\0");
        }
        if (param === "model") {
            return [SC.normFilterStr(r.make), SC.normFilterStr(r.model)].join("\0");
        }
        return SC.normFilterStr(r.make ?? r.model ?? r.trim ?? r.fuel ?? r.drive ?? r.induction ?? r.body_style ?? r.cyl ?? "");
    }

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

    function cascadeParam(param, rowKey, containerIds, alsoExclude = []) {
        const composite = param === "trim" || param === "model";
        const compatible = new Set(
            compatibleRows(param, alsoExclude).map(r =>
                composite ? cascadeRowKey(param, r) : SC.normFilterStr(rowKey(r))
            )
        );
        containerIds.forEach(id => {
            const container = document.getElementById(id);
            if (!container) return;
            container.querySelectorAll(".filter-option").forEach(label => {
                const cb = label.querySelector("input");
                if (!cb) return;
                const key = composite ? cascadeOptionKey(param, label, cb) : SC.normFilterStr(cb.value);
                const visible = compatible.has(key);
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
         "transmission", "drivetrain", "forced_induction", "body_style", "exterior_color", "interior_color", "package"]
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

    // ── Wire all checkboxes → sync twin + live render (cascade on panel open) ───────

    function wireFilterOptionCheckbox(cb) {
        if (cb.__dsWired) return;
        cb.__dsWired = true;
        cb.addEventListener("change", () => {
            // Mirror state to the twin checkbox (pill ↔ accordion)
            document.querySelectorAll(`input[type=checkbox][name="${cb.name}"]`).forEach(twin => {
                if (twin !== cb && twin.value === cb.value) twin.checked = cb.checked;
            });
            invalidateCheckedCache();
            scheduleFilterRender();
        });
    }

    document.querySelectorAll(".filter-option input[type=checkbox]").forEach(wireFilterOptionCheckbox);

    // ── Lazy facet options (model / trim / package) ────────────────────
    // These three lists are ~7,300 entries. Server-rendering them cost ~5 MB of
    // uncompressed HTML and pushed main.js past the 5-second mark on a throttled
    // connection. The template now ships only the options the URL has checked —
    // so `checked()` and the first render are already correct — and the full
    // lists arrive from /api/listings/filter-options and are grafted in here.
    //
    // Hydration is idempotent and preserves checked state, because a section can
    // be forced open (accordion click) before the background pass has finished.

    const LAZY_FACET_PARAMS = ["model", "trim", "package"];
    const _lazyFacetHydrated = new Set();
    let _facetOptionsPromise = null;

    function loadFacetOptions() {
        // Only a SUCCESSFUL fetch may be memoised. Caching the failure (as a
        // resolved null) made one 500 permanent: ensureLazyFacet() reused the
        // cached null forever and Model/Trim stayed empty for the whole session
        // with nothing shown to the user. Rejecting + clearing the slot means
        // the next accordion open retries.
        if (!_facetOptionsPromise) {
            const attempt = fetch("/api/listings/filter-options", { credentials: "same-origin" })
                .then((r) => {
                    if (!r.ok) throw new Error(`filter-options HTTP ${r.status}`);
                    return r.json();
                })
                .then((d) => {
                    if (!d || !d.ok) throw new Error("filter-options payload not ok");
                    return d;
                })
                .catch((err) => {
                    if (_facetOptionsPromise === attempt) _facetOptionsPromise = null;
                    throw err;
                });
            _facetOptionsPromise = attempt;
        }
        return _facetOptionsPromise;
    }

    /** Visible, retryable failure state — an empty accordion is indistinguishable from "no options". */
    function setLazyFacetError(param, show) {
        const containers = [`options-${param}`, `acc-options-${param}`]
            .map((id) => document.getElementById(id))
            .filter(Boolean);
        for (const container of containers) {
            const existing = container.querySelector(".filter-facet-error");
            if (!show) {
                if (existing) existing.remove();
                continue;
            }
            if (existing) continue;
            const box = document.createElement("div");
            box.className = "filter-facet-error";
            box.setAttribute("role", "alert");
            // Styled inline: this lives in main.js and the listings CSS bundle is
            // owned elsewhere. CSSOM writes are CSP-safe (a style="" attribute is not).
            box.style.padding = "8px 4px";
            box.style.fontSize = "13px";
            box.style.lineHeight = "1.4";
            box.style.opacity = "0.85";
            const msg = document.createElement("span");
            msg.textContent = "Couldn’t load these options. ";
            const retry = document.createElement("button");
            retry.type = "button";
            retry.className = "filter-facet-error__retry";
            retry.textContent = "Retry";
            retry.style.background = "none";
            retry.style.border = "0";
            retry.style.padding = "0";
            retry.style.font = "inherit";
            retry.style.cursor = "pointer";
            retry.style.textDecoration = "underline";
            retry.addEventListener("click", (e) => {
                e.preventDefault();
                e.stopPropagation();
                setLazyFacetError(param, false);
                ensureLazyFacet(param);
            });
            box.appendChild(msg);
            box.appendChild(retry);
            container.appendChild(box);
        }
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

    function buildLazyFacetLabel(param, entry, isChecked) {
        const label = document.createElement("label");
        label.className = "filter-option";
        const input = document.createElement("input");
        input.type = "checkbox";
        input.name = param;
        const span = document.createElement("span");
        if (param === "model") {
            label.dataset.make = entry[0] == null ? "" : String(entry[0]);
            input.dataset.make = label.dataset.make;
            input.value = entry[1] == null ? "" : String(entry[1]);
            span.textContent = input.value;
        } else if (param === "trim") {
            label.dataset.make = entry[0] == null ? "" : String(entry[0]);
            label.dataset.model = entry[1] == null ? "" : String(entry[1]);
            input.dataset.make = label.dataset.make;
            input.dataset.model = label.dataset.model;
            input.value = entry[2] == null ? "" : String(entry[2]);
            span.textContent = input.value || "—";
        } else {
            input.value = entry == null ? "" : String(entry);
            span.textContent = input.value;
        }
        input.checked = !!isChecked;
        label.appendChild(input);
        label.appendChild(span);
        return label;
    }

    function hydrateLazyFacet(param, facets) {
        if (_lazyFacetHydrated.has(param)) return false;
        const containers = [`options-${param}`, `acc-options-${param}`]
            .map((id) => document.getElementById(id))
            .filter(Boolean);
        if (!containers.length || !facets) return false;
        const entries = lazyFacetEntries(param, facets);
        if (!entries.length) return false;
        _lazyFacetHydrated.add(param);

        for (const container of containers) {
            // The server-rendered subset is exactly the checked options; carry
            // that state over so a URL like ?model=Sierra survives the rebuild.
            const preChecked = new Set();
            container.querySelectorAll(".filter-option input:checked").forEach((cb) => {
                const label = cb.closest(".filter-option");
                const ds = label && label.dataset ? label.dataset : {};
                preChecked.add(
                    param === "trim"
                        ? lazyFacetEntryKey(param, [ds.make || "", ds.model || "", cb.value])
                        : param === "model"
                            ? lazyFacetEntryKey(param, [ds.make || "", cb.value])
                            : lazyFacetEntryKey(param, cb.value)
                );
            });
            const frag = document.createDocumentFragment();
            for (const entry of entries) {
                const label = buildLazyFacetLabel(param, entry, preChecked.has(lazyFacetEntryKey(param, entry)));
                wireFilterOptionCheckbox(label.querySelector("input"));
                frag.appendChild(label);
            }
            container.replaceChildren(frag);
        }
        invalidateCheckedCache();
        return true;
    }

    /** Fill in a section's options right now if the facet payload has landed; otherwise when it does. */
    function ensureLazyFacet(param) {
        if (!LAZY_FACET_PARAMS.includes(param) || _lazyFacetHydrated.has(param)) return;
        setLazyFacetError(param, false);
        loadFacetOptions().then(
            (facets) => {
                if (hydrateLazyFacet(param, facets)) runCascade();
            },
            () => setLazyFacetError(param, true)
        );
    }

    function hydrateAllLazyFacets() {
        loadFacetOptions().then(
            (facets) => {
                if (!facets) return;
                let any = false;
                for (const param of LAZY_FACET_PARAMS) {
                    if (hydrateLazyFacet(param, facets)) any = true;
                }
                if (any) runCascade();
            },
            () => {
                // Speculative prefetch — stay silent. loadFacetOptions() has already
                // dropped its cached promise, so opening a section retries and, if
                // that fails too, shows the retryable error in place.
            }
        );
    }

    // Wire scalar filters (price, mileage, zip, radius) → live render
    // Scope to #search-form so nav/header ZIP fields do not leak into listings chips.
    function scalarVal(name) {
        if (name === "zip_code") {
            const pill = document.getElementById("listings-zip-input");
            if (pill && pill.value.trim()) return pill.value.trim();
        }
        if (name === "radius") {
            const pill = document.getElementById("listings-radius-select");
            if (pill && pill.value.trim()) return pill.value.trim();
        }
        const form = document.getElementById("search-form");
        const root = form || document;
        const vals = [...root.querySelectorAll(`[name="${name}"]`)]
            .map((el) => el.value.trim())
            .filter(Boolean);
        return vals[0] || "";
    }

    function syncSearchFormZipInputs(value) {
        const form = document.getElementById("search-form");
        if (!form) return;
        const z = String(value || "").replace(/\D/g, "").slice(0, 5);
        form.querySelectorAll('[name="zip_code"]').forEach((el) => {
            el.value = z;
        });
    }

    function listingsHasValidZip() {
        return SC.isValidUsZip(scalarVal("zip_code"));
    }

    function showListingsZipCallout() {
        const el = document.getElementById("listings-zip-callout");
        if (el) el.hidden = false;
        const zipInput = document.getElementById("listings-zip-input");
        if (zipInput) zipInput.focus({ preventScroll: true });
    }

    function hideListingsZipCallout() {
        const el = document.getElementById("listings-zip-callout");
        if (el) el.hidden = true;
    }

    // ── ZIP prompt banner (listings without a location) ────────────────
    // Shown above the results when no valid ZIP is set, so the blocked
    // filter state (maybeBlockFilterWithoutZip) is visible and fixable.
    // Dismissal and the ZIP itself both persist in localStorage, so once
    // a ZIP is set the banner never comes back.

    const LISTINGS_ZIP_PROMPT_DISMISS_KEY = "ds_listings_zip_prompt_dismissed";

    function listingsZipPromptDismissed() {
        try {
            return localStorage.getItem(LISTINGS_ZIP_PROMPT_DISMISS_KEY) === "1";
        } catch (_) {
            return false;
        }
    }

    function hideListingsZipPromptBanner() {
        const el = document.getElementById("listings-zip-banner");
        if (el) el.hidden = true;
    }

    function maybeShowListingsZipPromptBanner() {
        const el = document.getElementById("listings-zip-banner");
        if (!el) return;
        el.hidden = listingsHasValidZip() || listingsZipPromptDismissed();
    }

    (function initListingsZipPromptBanner() {
        const banner = document.getElementById("listings-zip-banner");
        if (!banner) return;
        const input = document.getElementById("listings-zip-banner-input");
        const submitBtn = document.getElementById("listings-zip-banner-submit");
        const dismissBtn = document.getElementById("listings-zip-banner-dismiss");

        function commitBannerZip() {
            if (!input) return;
            if (!SC.isValidUsZip(input.value)) {
                input.focus();
                return;
            }
            // Route through the real sidebar zip input (id=listings-zip-input),
            // which is bound to the full pipeline (sync, persist, hide banner,
            // chips, radius refresh). The banner's own input isn't in
            // #search-form, so dispatching on it alone would hide the banner
            // without ever applying the ZIP.
            const z = input.value.trim();
            const mainZip = document.getElementById("listings-zip-input");
            if (mainZip) {
                mainZip.value = z;
                mainZip.dispatchEvent(new Event("input", { bubbles: true }));
            }
            hideListingsZipPromptBanner();
        }

        if (submitBtn) submitBtn.addEventListener("click", commitBannerZip);
        if (input) {
            input.addEventListener("keydown", (e) => {
                if (e.key === "Enter") {
                    e.preventDefault();
                    commitBannerZip();
                }
            });
        }
        if (dismissBtn) {
            dismissBtn.addEventListener("click", () => {
                try {
                    localStorage.setItem(LISTINGS_ZIP_PROMPT_DISMISS_KEY, "1");
                } catch (_) {}
                hideListingsZipPromptBanner();
            });
        }
    })();

    function maybeBlockFilterWithoutZip(e) {
        if (!document.getElementById("ds-listings-car-rows")) return false;
        if (listingsHasValidZip()) {
            hideListingsZipCallout();
            return false;
        }
        const t = e.target;
        if (!t || !t.closest) return false;
        if (t.closest(".listings-geo-bar, .listings-zip-callout, .listings-zip-banner")) return false;
        if (t.closest(".smart-search-wrap, #smart-search-input")) return false;
        if (t.closest(".listings-active-chips-wrap, #listings-pagination, #listings-toolbar")) return false;
        if (!t.closest(
            "#filter-controls-row, #listings-sidebar-panel, .pill-trigger, .pill-dropdown, "
            + ".filter-option, .acc-trigger, .listings-filters-sheet-done, #listings-filters-open"
        )) {
            return false;
        }
        e.preventDefault();
        e.stopPropagation();
        showListingsZipCallout();
        return true;
    }

    document.addEventListener("click", maybeBlockFilterWithoutZip, true);
    document.addEventListener("change", maybeBlockFilterWithoutZip, true);

    document.querySelectorAll('#search-form [name="zip_code"]').forEach((el) => {
        el.addEventListener("input", () => {
            if (_syncingZipInputs) return;
            _syncingZipInputs = true;
            syncSearchFormZipInputs(el.value);
            _syncingZipInputs = false;
            if (listingsHasValidZip()) {
                const zipNow = scalarVal("zip_code");
                persistListingsZipLocal(zipNow);
                hideListingsZipPromptBanner();
                patchListingCarLinkZips(zipNow);
                scheduleDebouncedZipChips();
                scheduleListingsZipRefresh();
            } else {
                clearListingsZipLocal();
                clearTimeout(_listingsZipInputTimer);
                _listingsZipInputTimer = null;
                _listingsGeoRenderGen += 1;
                SC.abortPendingZipOriginFetches();
                clearListingsRadiusCache();
                _listingsGeoLastSent = null;
                scheduleDebouncedListingsUrlSync();
                scheduleListingsZipIncomplete();
            }
        });
    });

    document.querySelectorAll(".pill-select, .sidebar-select, .pill-zip, .sidebar-input").forEach(el => {
        if (el.name === "zip_code") return;
        const isGeo = el.name === "radius";
        el.addEventListener("change", isGeo ? refreshRadiusAndRender : renderResults);
        el.addEventListener("input",  isGeo ? refreshRadiusAndRender : renderResults);
    });

    // ── Live results renderer ──────────────────────────────────────────

    const resultsGrid  = document.getElementById("results-grid");
    const resultsCount = document.getElementById("results-count");
    const emptyState   = document.getElementById("empty-state");
    const listingsSortEl = document.getElementById("listings-sort");
    const activeChipsWrap = document.getElementById("listings-active-chips-wrap");
    const activeChipsEl = document.getElementById("listings-active-chips");
    const chipsClearBtn = document.getElementById("listings-chips-clear");
    const zeroHintEl = document.getElementById("listings-zero-hint");
    const undoFilterBtn = document.getElementById("listings-undo-filter");
    const filtersOpenBtn = document.getElementById("listings-filters-open");
    const filterBackdrop = document.getElementById("listings-filter-backdrop");
    const filtersMobileCount = document.getElementById("listings-filters-mobile-count");
    const sidebarPanel = document.getElementById("listings-sidebar-panel");
    const listingsPagination = document.getElementById("listings-pagination");
    const listingsRange = document.getElementById("listings-range");
    const listingsPagePrev = document.getElementById("listings-page-prev");
    const listingsPageNext = document.getElementById("listings-page-next");
    const listingsPaginationPages = document.getElementById("listings-pagination-pages");
    const listingsPerPageEl = document.getElementById("listings-per-page");

    const LISTINGS_LOGGED_IN = document.body && document.body.getAttribute("data-logged-in") === "1";
    let _lastFilterAction = null;
    let _listingsAllCars = [];
    let _listingsPage = 1;
    let _listingsGeoRenderGen = 0;
    let _listingsZipInputTimer = null;
    let _listingsZipChipsTimer = null;
    let _listingsZipUrlTimer = null;
    let _listingsZipIncompleteTimer = null;
    let _listingsUiSyncRaf = null;
    let _syncingZipInputs = false;
    let _radiusFilteredCars = null;
    let _radiusFilterKey = "";

    function listingsZipRenderStale(gen) {
        return gen !== _listingsGeoRenderGen || !listingsHasValidZip();
    }

    function listingsRadiusFilterKey() {
        const zip = scalarVal("zip_code");
        const radiusMi = parseFloat(scalarVal("radius")) || null;
        if (!SC.isValidUsZip(zip) || !radiusMi) return "";
        return `${zip.trim()}|${radiusMi}`;
    }

    function clearListingsRadiusCache() {
        _radiusFilteredCars = null;
        _radiusFilterKey = "";
        RADIUS_CAR_ROWS = null;
    }

    function cancelListingsZipWork() {
        clearTimeout(_listingsZipInputTimer);
        _listingsZipInputTimer = null;
        clearTimeout(_listingsZipIncompleteTimer);
        _listingsZipIncompleteTimer = null;
        clearTimeout(_listingsZipChipsTimer);
        _listingsZipChipsTimer = null;
        if (_filterRenderFrame) {
            cancelAnimationFrame(_filterRenderFrame);
            _filterRenderFrame = null;
        }
        _pendingFilterRenderOpts = null;
        if (_radiusRenderRaf) {
            cancelAnimationFrame(_radiusRenderRaf);
            _radiusRenderRaf = null;
        }
        SC.abortPendingZipOriginFetches();
        clearListingsRadiusCache();
    }

    function bumpListingsGeoRenderGen() {
        _listingsGeoRenderGen += 1;
        SC.abortPendingZipOriginFetches();
        clearListingsRadiusCache();
        return _listingsGeoRenderGen;
    }

    function scheduleDebouncedZipChips() {
        scheduleListingsUiSync();
    }

    function scheduleDebouncedListingsUrlSync() {
        scheduleListingsUiSync();
    }

    function scheduleListingsUiSync() {
        if (_listingsUiSyncRaf) cancelAnimationFrame(_listingsUiSyncRaf);
        _listingsUiSyncRaf = requestAnimationFrame(() => {
            _listingsUiSyncRaf = null;
            syncActiveFilterChips();
            syncUrl();
        });
    }

    function scheduleListingsZipIncomplete() {
        clearTimeout(_listingsZipIncompleteTimer);
        _listingsZipIncompleteTimer = setTimeout(() => {
            _listingsZipIncompleteTimer = null;
            if (listingsHasValidZip()) return;
            const gen = bumpListingsGeoRenderGen();
            _listingsGeoLastSent = null;
            scheduleListingsUiSync();
            refreshRadiusAndRenderNow(gen);
        }, 40);
    }

    function scheduleListingsZipRefresh() {
        clearTimeout(_listingsZipInputTimer);
        clearTimeout(_listingsZipIncompleteTimer);
        _listingsZipIncompleteTimer = null;
        const zipNow = scalarVal("zip_code");
        if (!SC.isValidUsZip(zipNow)) return;

        SC.resolveListingsZipOrigin(zipNow);
        hideListingsZipCallout();
        if (!window.__DS_listingsGeoState.ready) {
            markListingsGeoReady();
        }

        const runRefresh = () => {
            _listingsZipInputTimer = null;
            if (!listingsHasValidZip()) return;
            const gen = bumpListingsGeoRenderGen();
            if (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length) {
                showInventoryLoading();
            }
            refreshRadiusAndRenderNow(gen);
            if (typeof window.__DS_scheduleReloadNearbyDealers === "function") {
                window.__DS_scheduleReloadNearbyDealers();
            }
        };

        // Instant when ZIP coords are already known; short debounce while fetching.
        if (zipCoordsJS(zipNow)) {
            runRefresh();
            return;
        }
        _listingsZipInputTimer = setTimeout(runRefresh, 30);
    }

    function deferListingsIdleWork(fn, timeoutMs) {
        if (typeof requestIdleCallback === "function") {
            requestIdleCallback(fn, { timeout: timeoutMs || 800 });
        } else {
            setTimeout(fn, 0);
        }
    }

    const SORT_STORAGE_KEY = "ds_listings_sort";
    const PER_PAGE_STORAGE_KEY = "ds_listings_per_page";
    const DEFAULT_PER_PAGE = 24;
    if (listingsSortEl) {
        try {
            const savedSort = localStorage.getItem(SORT_STORAGE_KEY);
            if (savedSort && listingsSortEl.querySelector(`option[value="${savedSort}"]`)) {
                listingsSortEl.value = savedSort;
            }
        } catch (_) {}
        listingsSortEl.addEventListener("change", () => {
            try {
                localStorage.setItem(SORT_STORAGE_KEY, listingsSortEl.value);
            } catch (_) {}
            resetListingsPage();
            if (typeof window.__DS_runFilterRender === "function") {
                window.__DS_runFilterRender();
            } else {
                renderResults();
            }
        });
    }

    function getListingsPerPage() {
        const raw = listingsPerPageEl ? parseInt(listingsPerPageEl.value, 10) : DEFAULT_PER_PAGE;
        return [12, 24, 48].includes(raw) ? raw : DEFAULT_PER_PAGE;
    }

    if (listingsPerPageEl) {
        try {
            const urlPp = parseInt(new URLSearchParams(window.location.search).get("per_page"), 10);
            const saved = parseInt(localStorage.getItem(PER_PAGE_STORAGE_KEY), 10);
            const pick = [12, 24, 48].includes(urlPp) ? urlPp : ([12, 24, 48].includes(saved) ? saved : DEFAULT_PER_PAGE);
            listingsPerPageEl.value = String(pick);
        } catch (_) {}
        listingsPerPageEl.addEventListener("change", () => {
            try {
                localStorage.setItem(PER_PAGE_STORAGE_KEY, listingsPerPageEl.value);
            } catch (_) {}
            if (!_listingsAllCars.length) return;
            resetListingsPage();
            renderListingsPage();
        });
    }

    function getListingsSortMode() {
        return listingsSortEl ? listingsSortEl.value : "relevance";
    }

    function resetListingsPage() {
        _listingsPage = 1;
    }
    window.__DS_resetListingsPage = resetListingsPage;

    function readListingsPageFromUrl() {
        const p = parseInt(new URLSearchParams(window.location.search).get("page"), 10);
        return Number.isFinite(p) && p > 0 ? p : 1;
    }

    function scrollListingsResultsIntoView() {
        const anchor = document.getElementById("listings-toolbar") || document.getElementById("listings-results");
        if (anchor && anchor.scrollIntoView) {
            anchor.scrollIntoView({ behavior: "smooth", block: "start" });
        }
    }

    function updatePaginationUI(total, page, perPage) {
        const totalPages = Math.max(1, Math.ceil(total / perPage));
        if (page > totalPages) {
            _listingsPage = totalPages;
            page = totalPages;
        }
        if (listingsPagination) {
            listingsPagination.hidden = totalPages <= 1 && total <= perPage;
        }
        const start = total === 0 ? 0 : (page - 1) * perPage + 1;
        const end = Math.min(page * perPage, total);
        if (resultsCount) {
            resultsCount.textContent = total === 0
                ? "0 vehicles"
                : (totalPages > 1
                    ? `${start.toLocaleString()}–${end.toLocaleString()} of ${total.toLocaleString()}`
                    : `${total.toLocaleString()} vehicle${total !== 1 ? "s" : ""}`);
        }
        if (listingsRange) {
            listingsRange.textContent = totalPages > 1
                ? `Page ${page} of ${totalPages} · ${perPage} per page`
                : "";
        }
        if (listingsPagePrev) listingsPagePrev.disabled = page <= 1;
        if (listingsPageNext) listingsPageNext.disabled = page >= totalPages;
        if (listingsPaginationPages) {
            listingsPaginationPages.innerHTML = SC.buildPageNumberWindow(page, totalPages).map((item) => {
                if (item === "…") {
                    return `<span class="listings-page-ellipsis" aria-hidden="true">…</span>`;
                }
                const active = item === page ? " listings-page-num--active" : "";
                return `<button type="button" class="listings-page-num${active}" data-page="${item}" aria-label="Page ${item}"${item === page ? ' aria-current="page"' : ""}>${item}</button>`;
            }).join("");
            listingsPaginationPages.querySelectorAll(".listings-page-num").forEach((btn) => {
                btn.addEventListener("click", () => {
                    const n = parseInt(btn.dataset.page, 10);
                    if (!Number.isFinite(n)) return;
                    _listingsPage = n;
                    renderListingsPage({ scroll: true });
                });
            });
        }
    }

    function goListingsPage(delta) {
        const perPage = getListingsPerPage();
        const total = _listingsAllCars.length;
        const totalPages = Math.max(1, Math.ceil(total / perPage));
        _listingsPage = Math.min(totalPages, Math.max(1, _listingsPage + delta));
        renderListingsPage({ scroll: true });
    }

    if (listingsPagePrev) listingsPagePrev.addEventListener("click", () => goListingsPage(-1));
    if (listingsPageNext) listingsPageNext.addEventListener("click", () => goListingsPage(1));

    function renderCardHtml(c, savedSet, compareIds) {
        const gallery = Array.isArray(c.gallery) ? c.gallery : [];
        const imgRaw = (gallery.length && gallery[0]) ? gallery[0] : (c.image_url || "") || "/static/placeholder.svg";
        const imgSrc = SC.safeImageSrc(imgRaw);
        const imgSrcAttr = SC.escapeHtml(imgSrc);
        const photoCount = Number(c.photo_count) > 0 ? Number(c.photo_count) : gallery.length;
        const photoLabel = photoCount > 1 ? `${photoCount} photos` : "";
        const idNum = Number(c.id);
        const idStr = Number.isFinite(idNum) && idNum > 0 ? String(Math.floor(idNum)) : "0";
        const dashLike = (v) => {
            const s = String(v || "").trim();
            return !s || s === "\u2014" || s === "-" || s === "--";
        };
        const specBits = [];
        if (!dashLike(c.body_style)) specBits.push(SC.escapeHtml(c.body_style));
        const specLine = specBits.length
            ? `<p class="result-meta result-meta--specs">${specBits.join(" &middot; ")}</p>`
            : "";
        const incompletePill = c.public_incomplete
            ? `<span class="result-incomplete-pill" title="Missing some public-listing fields">Incomplete</span>`
            : "";
        const cpoBadge = SC.cpoBadgeHtml(c);
        const mkt = c.market;
        // A payment-shaped "price" (either intel tier flagged it) renders as
        // an advertised payment, never as the sale price with a deal badge.
        const paymentListed = (mkt && mkt.vs_market === "payment_listed")
            || (c.deal_score && c.deal_score.label === "payment_listed");
        // Premium trim-avg badge takes precedence; otherwise fall back to the
        // free coarse market deal score attached during serialization.
        const dealBadge = paymentListed ? "" : (SC.dealBadgeHtml(mkt) || SC.dealScoreBadgeHtml(c.deal_score));
        const priceDropBadge = SC.priceDropBadgeHtml(c);
        let marketLine = "";
        if (mkt && mkt.avg_price_display) {
            marketLine = `<p class="result-market-sub">Trim avg ${SC.escapeHtml(mkt.avg_price_display)}</p>`;
        }
        const distMi = carDistanceMiles(c);
        const distLine = (distMi != null && Number.isFinite(distMi))
            ? `<span class="result-distance">${distMi < 10 ? distMi.toFixed(1) : Math.round(distMi)} mi away</span>`
            : "";
        const isSaved = savedSet.has(idNum);
        const inCompare = compareIds.includes(idNum);
        const saveBtn = LISTINGS_LOGGED_IN
            ? `<button type="button" class="result-save-btn${isSaved ? " result-save-btn--saved" : ""}" data-car-id="${idStr}" aria-label="${isSaved ? "Saved" : "Save this car"}" title="${isSaved ? "Remove from saved" : "Save"}">`
                + `<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" stroke-width="1.8" fill="${isSaved ? "currentColor" : "none"}" aria-hidden="true">`
                + `<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>`
                + `</svg></button>`
            : "";
        const compareCb = `<label class="result-compare-label" title="Add to compare (max 4)">`
            + `<input type="checkbox" class="result-compare-cb" data-car-id="${idStr}"${inCompare ? " checked" : ""}>`
            + `<span>Compare</span></label>`;
        const zipForUrl = typeof scalarVal === "function" ? scalarVal("zip_code") : "";
        const carHref = zipForUrl && /^\d{5}$/.test(String(zipForUrl).trim())
            ? `/car/${idStr}?zip_code=${encodeURIComponent(String(zipForUrl).trim())}`
            : `/car/${idStr}`;
        // Dealer name -> /dealership/<dealer_id>, the same research page the car page
        // links to. Same key shape the route validates (_DEALER_KEY_RE), so a card
        // carrying a junk dealer_id renders plain text instead of a link to a 404.
        const dealerName = String(c.dealer_name || "").trim();
        const dealerKey = String(c.dealer_id || "").trim();
        const dealerNameHtml = SC.escapeHtml(dealerName);
        const dealerHtml = (dealerName && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(dealerKey))
            ? `<a href="/dealership/${encodeURIComponent(dealerKey)}" class="result-dealer result-dealer-link">${dealerNameHtml}</a>`
            : `<span class="result-dealer">${dealerNameHtml}</span>`;
        // The backend sets location_confirmed=false only when this listing's own photos
        // (or a proven group-wide feed) contradict the dealership it is filed under —
        // see backend/db/repositories/cars_repo.py. The card keeps the dealer name (that
        // is who published it) and stops implying the car is on that lot.
        const locUnconfirmed = c.location_confirmed === false
            ? `<span class="result-dealer-unconfirmed" title="${SC.escapeHtml(c.location_note || "")}">Location unconfirmed</span>`
            : "";
        // The dealer row sits OUTSIDE .result-card-link: an <a> nested inside an <a> is
        // invalid HTML, and browsers recover by unnesting it, which breaks the card link.
        return `
            <article class="result-card${c.public_incomplete ? " result-card--incomplete" : ""}">
                <a href="${carHref}" class="result-card-link">
                    <div class="result-image-wrap">
                        <img class="result-image" src="${imgSrcAttr}" alt="" loading="lazy" decoding="async" onerror="this.onerror=null;this.src='/static/placeholder.svg';">
                        ${dealBadge ? `<div class="result-deal-badge-wrap">${dealBadge}</div>` : ""}
                        ${photoLabel ? `<span class="result-photo-count">${SC.escapeHtml(photoLabel)}</span>` : ""}
                    </div>
                    <div class="result-content">
                        <div class="result-title-row">
                            <h2>${SC.escapeHtml(c.title)}</h2>
                            ${cpoBadge}
                            ${incompletePill}
                        </div>
                        <p class="result-trim">${SC.escapeHtml(c.trim || "")}</p>
                        ${paymentListed
                            ? `<p class="result-price result-price--payment">${SC.fmtUSD(c.price)}<span class="result-price-payment-note">advertised payment &mdash; price not listed</span></p>`
                            : `<p class="result-price">${SC.fmtUSD(c.price)}${priceDropBadge}</p>`}
                        ${marketLine}
                        <p class="result-meta">
                            ${SC.fmt(c.mileage)} mi
                            &middot; ${SC.escapeHtml(c.fuel_type || "")}
                            &middot; ${SC.escapeHtml(c.drivetrain || "")}
                        </p>
                        ${specLine}
                    </div>
                </a>
                <p class="result-dealer-row">
                    ${distLine}
                    ${dealerHtml}
                    ${locUnconfirmed}
                </p>
                <div class="result-card-actions">
                    ${compareCb}
                    ${saveBtn}
                </div>
            </article>`;
    }

    function renderListingsPage(opts) {
        if (!resultsGrid) return;
        const perPage = getListingsPerPage();
        const total = _listingsAllCars.length;
        const totalPages = Math.max(1, Math.ceil(Math.max(total, 1) / perPage));
        if (_listingsPage > totalPages) _listingsPage = totalPages;
        if (_listingsPage < 1) _listingsPage = 1;

        const start = (_listingsPage - 1) * perPage;
        let pageCars = _listingsAllCars.slice(start, start + perPage);
        if (!pageCars.length && total > 0 && _listingsPage > 1) {
            _listingsPage = 1;
            pageCars = _listingsAllCars.slice(0, perPage);
        }
        const savedSet = window.SAVED_CAR_IDS instanceof Set ? window.SAVED_CAR_IDS : new Set();
        const compareIds = typeof window.__DS_compareReadIds === "function"
            ? window.__DS_compareReadIds()
            : [];
        const enrichPage = window.__DS_MARKET_STATS && getListingsSortMode() !== "deal";

        resultsGrid.innerHTML = pageCars.map((c) => {
            const row = enrichPage ? SC.enrichCarWithMarket(c) : c;
            return renderCardHtml(row, savedSet, compareIds);
        }).join("");
        updatePaginationUI(total, _listingsPage, perPage);
        scheduleListingsUiSync();
        wireResultSaveButtons();
        if (typeof window.__DS_compareSyncTray === "function") {
            window.__DS_compareSyncTray();
        }
        if (typeof window.__DS_wirePrefetchLinks === "function") {
            window.__DS_wirePrefetchLinks(resultsGrid);
        }
        if (typeof window.__DS_prefetchVisibleCarLinks === "function") {
            window.__DS_prefetchVisibleCarLinks(resultsGrid, 8);
        }
        if (opts && opts.scroll) scrollListingsResultsIntoView();
    }

    function carDistanceMiles(car) {
        const preset = Number(car.distance_miles);
        if (Number.isFinite(preset) && preset >= 0) return preset;
        const zipCode = scalarVal("zip_code");
        const origin = zipCoordsJS(zipCode);
        if (!origin || typeof haversineJS !== "function") return null;
        const coords = SC.carGeoCoords(car);
        if (!coords) return null;
        return haversineJS(origin[0], origin[1], coords[0], coords[1]);
    }

    function listingCallForPrice(c) {
        const p = Number(c.price);
        return !Number.isFinite(p) || p <= 0;
    }

    function listingPhotoCount(c) {
        const pc = Number(c.photo_count);
        if (Number.isFinite(pc) && pc >= 0) return pc;
        const gallery = Array.isArray(c.gallery) ? c.gallery : [];
        if (gallery.length) return gallery.length;
        return c.image_url ? 1 : 0;
    }

    // True when the car has a genuine dealer photo (not the placeholder). A
    // truthy image_url can be "/static/placeholder.svg", so string-truthiness
    // isn't enough — require an http image on image_url or in the gallery.
    function listingHasRealImage(c) {
        // Real photo = remote http(s) OR a locally-cached /car-images/ path
        // (served by Flask), not the /static/placeholder.svg fallback.
        const isReal = (u) => typeof u === "string" && (u.indexOf("http") === 0 || u.indexOf("/car-images/") === 0);
        if (isReal(c.image_url)) return true;
        const gallery = Array.isArray(c.gallery) ? c.gallery : [];
        return gallery.some(isReal);
    }

    function listingDepriorityCompare(a, b) {
        // Strongest tier: placeholder-image cars (no real dealer photo — mostly
        // unphotographed new inventory) sink below everything, in every sort.
        const aNoImg = listingHasRealImage(a) ? 0 : 1;
        const bNoImg = listingHasRealImage(b) ? 0 : 1;
        if (aNoImg !== bNoImg) return aNoImg - bNoImg;
        const aCall = listingCallForPrice(a) ? 1 : 0;
        const bCall = listingCallForPrice(b) ? 1 : 0;
        if (aCall !== bCall) return aCall - bCall;
        const aSingle = listingPhotoCount(a) <= 1 ? 1 : 0;
        const bSingle = listingPhotoCount(b) <= 1 ? 1 : 0;
        return aSingle - bSingle;
    }

    function sortListingsCars(cars, mode, preserveOrder) {
        if (preserveOrder && mode === "relevance") {
            return cars.slice().sort((a, b) => listingDepriorityCompare(a, b));
        }
        const arr = cars.slice();
        const priceKey = (c) => {
            const p = Number(c.price);
            return Number.isFinite(p) && p > 0 ? p : Infinity;
        };
        const mileageKey = (c) => {
            const m = Number(c.mileage);
            return Number.isFinite(m) && m >= 0 ? m : Infinity;
        };
        const yearKey = (c) => {
            const y = parseInt(c.year, 10);
            return Number.isFinite(y) ? y : 0;
        };
        const dealKey = (c) => {
            const m = c.market;
            if (!m || m.delta_pct == null) return 999;
            return Number(m.delta_pct);
        };
        const distKey = (c) => {
            const d = carDistanceMiles(c);
            return d == null ? Infinity : d;
        };
        const withDepriority = (cmp) => (a, b) => listingDepriorityCompare(a, b) || cmp(a, b);

        if (mode === "price_asc") {
            arr.sort(withDepriority((a, b) => priceKey(a) - priceKey(b) || distKey(a) - distKey(b)));
        } else if (mode === "price_desc") {
            arr.sort(withDepriority((a, b) => priceKey(b) - priceKey(a) || distKey(a) - distKey(b)));
        } else if (mode === "mileage_asc") {
            arr.sort(withDepriority((a, b) => mileageKey(a) - mileageKey(b) || priceKey(a) - priceKey(b)));
        } else if (mode === "year_desc") {
            arr.sort(withDepriority((a, b) => yearKey(b) - yearKey(a) || priceKey(a) - priceKey(b)));
        } else if (mode === "deal") {
            arr.sort(withDepriority((a, b) => dealKey(a) - dealKey(b) || priceKey(a) - priceKey(b)));
        } else if (!preserveOrder) {
            arr.sort(withDepriority((a, b) => priceKey(a) - priceKey(b)));
        }
        return arr;
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

    function renderCarGrid(cars, opts) {
        if (!resultsGrid) return;
        window.__DS_lastResultCount = Array.isArray(cars) ? cars.length : 0;
        window.__DS_lastResults = Array.isArray(cars) ? cars.slice(0, 12) : [];
        const preserveOrder = opts && opts.preserveOrder;
        if (opts && opts.resetPage) {
            resetListingsPage();
        } else if (!opts || !opts.keepPage) {
            _listingsPage = readListingsPageFromUrl();
        }

        const sortMode = getListingsSortMode();

        if (cars.length === 0) {
            _listingsAllCars = [];
            _listingsPage = 1;
            resultsGrid.innerHTML = "";
            if (listingsPagination) listingsPagination.hidden = true;
            if (listingsRange) listingsRange.textContent = "";
            if (listingsPaginationPages) listingsPaginationPages.innerHTML = "";
            if (listingsPagePrev) listingsPagePrev.disabled = true;
            if (listingsPageNext) listingsPageNext.disabled = true;
            const customEmpty = opts && opts.emptyMessage;
            const zipCode = typeof scalarVal === "function" ? scalarVal("zip_code") : "";
            const radiusMi = typeof scalarVal === "function" ? parseFloat(scalarVal("radius")) : null;
            const geoSearchActive =
                listingsHasValidZip() && radiusMi && Number.isFinite(radiusMi);
            const noResultsEl = emptyState && emptyState.querySelector(".no-results");
            const noResultsSubEl = emptyState && emptyState.querySelector(".no-results-sub");
            const activeMakes = typeof checked === "function" ? checked("make") : [];
            if (geoSearchActive && !customEmpty) {
                let geoMsg =
                    "No listings within " + radiusMi + " mi of " + zipCode + " yet.";
                if (activeMakes.length) {
                    geoMsg =
                        "No " +
                        activeMakes.slice(0, 4).join(", ") +
                        (activeMakes.length > 4 ? "…" : "") +
                        " within " +
                        radiusMi +
                        " mi of " +
                        zipCode +
                        ". Try a larger radius or different makes.";
                }
                setListingsGeoHint(geoMsg);
                if (emptyState) emptyState.style.display = "none";
            } else if (customEmpty) {
                setListingsGeoHint("");
                if (emptyState) {
                    emptyState.style.display = "";
                    if (noResultsEl) noResultsEl.textContent = customEmpty;
                    if (noResultsSubEl) {
                        noResultsSubEl.textContent = "Try different keywords or clear filters.";
                    }
                }
            } else {
                setListingsGeoHint("");
                if (emptyState) emptyState.style.display = "none";
            }
            if (resultsCount) {
                resultsCount.textContent = geoSearchActive
                    ? "0 vehicles near " + zipCode
                    : "0 vehicles";
            }
            if (zeroHintEl) zeroHintEl.hidden = geoSearchActive || !_lastFilterAction;
            scheduleListingsUiSync();
            return;
        }

        const needsMarketForSort = sortMode === "deal" && window.__DS_MARKET_STATS;
        if (needsMarketForSort) {
            cars = SC.enrichCarsWithMarket(cars);
        }

        cars = sortListingsCars(cars, sortMode, preserveOrder);

        if (emptyState) emptyState.style.display = "none";
        if (zeroHintEl) zeroHintEl.hidden = true;

        const paintGrid = (rows) => {
            _listingsAllCars = rows;
            window.__DS_listingsAllCars = rows;
            renderListingsPage();
        };

        paintGrid(cars);
    }

    let _listingsGeoPersistTimer = null;
    let _listingsGeoLastSent = null;
    function setListingsGeoHint(message) {
        const hint = document.getElementById("listings-geo-hint");
        if (!hint) return;
        const text = (message || "").trim();
        if (!text) {
            hint.textContent = "";
            hint.hidden = true;
            return;
        }
        hint.textContent = text;
        hint.hidden = false;
    }

    function schedulePersistListingsGeoSession() {
        const zip = scalarVal("zip_code");
        const radius = scalarVal("radius");
        if (!zip || !radius) {
            setListingsGeoHint("");
            return;
        }
        const z = zip.trim();
        if (!SC.isValidUsZip(z)) {
            setListingsGeoHint("Enter a valid 5-digit US ZIP code.");
            return;
        }
        const r = parseFloat(radius);
        if (!Number.isFinite(r) || r <= 0) return;
        setListingsGeoHint("");
        const payload = `${z}|${radius}`;
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
                body: JSON.stringify({ zip_code: z, radius }),
            })
                .then(async (res) => {
                    let data = null;
                    try {
                        data = await res.json();
                    } catch (_) {}
                    if (res.ok && data && data.ok) {
                        _listingsGeoLastSent = payload;
                        setListingsGeoHint("");
                        SC.scheduleReloadMarketStats();
                        return;
                    }
                    if (data && data.error === "invalid_zip_or_radius") {
                        setListingsGeoHint("That ZIP could not be found. Check the number and try again.");
                    } else if (data && data.error === "bad_radius") {
                        setListingsGeoHint("Choose a valid search radius.");
                    }
                })
                .catch(() => {});
        }, 500);
    }

    function dealerRegistryCheckboxState() {
        const form = document.getElementById("search-form");
        const root = form || document;
        const boxes = [...root.querySelectorAll('input[name="dealer_registry_id"]')];
        const checkedBoxes = boxes.filter((cb) => cb.checked);
        return {
            boxes,
            total: boxes.length,
            checkedCount: checkedBoxes.length,
            checkedBoxes,
        };
    }

    /** Premium dealership filter: non-empty only when a strict subset is selected (iOS parity). */
    function buildDealerFilterIdSet() {
        const radiusMi = parseFloat(scalarVal("radius")) || null;
        if (!listingsHasValidZip() || !radiusMi || radiusMi > 50) return null;
        const { total, checkedCount, checkedBoxes } = dealerRegistryCheckboxState();
        if (!total || checkedCount === 0 || checkedCount === total) return null;
        const set = new Set();
        for (const cb of checkedBoxes) {
            const n = parseInt(cb.value, 10);
            if (Number.isFinite(n) && n > 0) set.add(n);
        }
        return set.size ? set : null;
    }

    function selectedDealerRegistryIds() {
        const set = buildDealerFilterIdSet();
        return set ? [...set] : [];
    }

    function carRegistryIdCached(car) {
        if (!car || typeof car !== "object") return 0;
        if (car._dsRegId !== undefined) return car._dsRegId;
        const reg = SC.carDealershipRegistryId(car);
        car._dsRegId = reg;
        return reg;
    }

    function carMatchesDealerFilter(c, dealerFilterSet) {
        if (!dealerFilterSet) return true;
        const reg = carRegistryIdCached(c);
        return reg > 0 && dealerFilterSet.has(reg);
    }

    function passesDealerFilter(c, dealerFilterSet) {
        const set = dealerFilterSet !== undefined ? dealerFilterSet : buildDealerFilterIdSet();
        return carMatchesDealerFilter(c, set);
    }

    function syncUrl() {
        const params = new URLSearchParams();
        const multiParams = ["make", "model", "trim", "fuel_type", "cylinders", "transmission",
                             "drivetrain", "forced_induction", "body_style", "exterior_color", "interior_color", "country", "package", "cpo_only"];
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
        const zipForUrl = scalarVal("zip_code");
        if (zipForUrl && SC.isValidUsZip(zipForUrl)) {
            params.set("zip_code", zipForUrl);
            const radiusForUrl = scalarVal("radius");
            if (radiusForUrl) params.set("radius", radiusForUrl);
        }
        for (const name of ["max_price", "max_mileage", "inventory_condition"]) {
            const val = scalarVal(name);
            if (val) params.set(name, val);
        }
        const radiusForDealers = parseFloat(scalarVal("radius")) || null;
        if (listingsHasValidZip() && radiusForDealers && radiusForDealers <= 50) {
            const narrowedIds = selectedDealerRegistryIds();
            const seenDealers = new Set();
            narrowedIds.forEach((id) => {
                const s = String(id);
                if (!seenDealers.has(s)) {
                    seenDealers.add(s);
                    params.append("dealer_registry_id", s);
                }
            });
        }
        const smartIn = document.getElementById("smart-search-input");
        const q = smartIn ? (smartIn.value || "").trim() : "";
        if (q) params.set("q", q);
        if (_listingsPage > 1) params.set("page", String(_listingsPage));
        const perPage = getListingsPerPage();
        if (perPage !== DEFAULT_PER_PAGE) params.set("per_page", String(perPage));
        const qs = params.toString();
        history.replaceState(null, "", window.location.pathname + (qs ? "?" + qs : ""));
        schedulePersistListingsGeoSession();
    }

    let _geoCoordsLoadPromise = null;
    function ensureListingsGeoCoordsLoaded() {
        if (SC.listingsDealerCoordsReady()) {
            return Promise.resolve();
        }
        if (_geoCoordsLoadPromise) return _geoCoordsLoadPromise;
        if (window.__DS_listingsGeoPrefetchPromise) {
            _geoCoordsLoadPromise = window.__DS_listingsGeoPrefetchPromise.catch(() => {});
            return _geoCoordsLoadPromise;
        }
        _geoCoordsLoadPromise = fetch("/api/listings/geo-coords", { credentials: "same-origin" })
            .then((r) => (r.ok ? r.json() : null))
            .then((data) => {
                SC.mergeListingsGeoCoordsPayload(data);
            })
            .catch(() => {});
        return _geoCoordsLoadPromise;
    }
    window.__DS_ensureListingsGeoCoordsLoaded = ensureListingsGeoCoordsLoaded;

    function collectFacetFilterState() {
        const makes = checked("make");
        const models = checked("model");
        const trims = checked("trim");
        const fuels = checked("fuel_type");
        const cyls = checked("cylinders");
        const trans = checked("transmission");
        const drives = checked("drivetrain");
        const inductions = checked("forced_induction");
        const bodies = checked("body_style");
        const extColors = checked("exterior_color");
        const intColors = checked("interior_color");
        const countries = checked("country");
        const maxPriceRaw = scalarVal("max_price");
        const maxPrice = maxPriceRaw !== "" ? parseFloat(maxPriceRaw) : null;
        const maxMileageRaw = scalarVal("max_mileage");
        const maxMileage = maxMileageRaw !== "" ? parseInt(maxMileageRaw, 10) : null;
        const inventoryCondition = scalarVal("inventory_condition");
        const cpoOnly = checked("cpo_only").length > 0;
        const pkgs = checked("package");

        let makesFilter = makes.slice();
        if (countries.length && typeof COUNTRY_TO_MAKES === "object") {
            const fromCountries = countries.flatMap(c => COUNTRY_TO_MAKES[c] || []);
            makesFilter = makesFilter.length
                ? makesFilter.filter(m => SC.valueInListCI(fromCountries, m))
                : fromCountries;
        }

        return {
            makesFilter,
            models,
            trims,
            fuels,
            cyls,
            trans,
            drives,
            inductions,
            bodies,
            extColors,
            intColors,
            maxPrice,
            maxMileage,
            inventoryCondition,
            cpoOnly,
            pkgs,
        };
    }

    function carMatchesFacetFilters(c, state, dealerFilterSet) {
        if (state.makesFilter.length && !SC.valueInListCI(state.makesFilter, c.make)) return false;
        if (state.models.length && !SC.valueInListCI(state.models, c.model)) return false;
        if (state.trims.length && !SC.valueInListCI(state.trims, c.trim)) return false;
        if (state.fuels.length && !SC.valueInListCI(state.fuels, c.fuel_type)) return false;
        if (state.cyls.length && !state.cyls.includes(String(c.cylinders))) return false;
        if (state.trans.length && !SC.valueInListCI(state.trans, c.transmission)) return false;
        if (state.drives.length && !SC.valueInListCI(state.drives, c.drivetrain)) return false;
        if (state.inductions.length && !SC.valueInListCI(state.inductions, c.forced_induction)) return false;
        if (state.bodies.length && !SC.valueInListCI(state.bodies, c.body_style)) return false;
        if (state.extColors.length && !carMatchesPaintFamilyBuckets(c, "exterior_color", state.extColors)) return false;
        if (state.intColors.length && !carMatchesPaintFamilyBuckets(c, "interior_color", state.intColors)) return false;
        if (state.pkgs.length) {
            const carPkgs = (c.package_names || []).map(n => n.toLowerCase());
            if (!state.pkgs.some(p => carPkgs.includes(p.toLowerCase()))) return false;
        }
        if (state.maxPrice != null && Number.isFinite(state.maxPrice) && c.price > state.maxPrice) return false;
        if (state.maxMileage != null && Number.isFinite(state.maxMileage) && c.mileage > state.maxMileage) return false;
        if (!passesInventoryConditionFilter(c, state.inventoryCondition)) return false;
        if (state.cpoOnly && !c.is_cpo) return false;
        if (!carMatchesDealerFilter(c, dealerFilterSet)) return false;
        return true;
    }

    function renderResultsNowCore(opts) {
        if (!resultsGrid) return;

        const renderGen = opts && opts.renderGen != null ? opts.renderGen : _listingsGeoRenderGen;
        if (renderGen != null && listingsZipRenderStale(renderGen)) return;

        resetListingsPage();
        invalidateCheckedCache();

        const facetState = collectFacetFilterState();
        const dealerFilterSet = buildDealerFilterIdSet();
        const zipCode = scalarVal("zip_code");
        const radiusMi = parseFloat(scalarVal("radius")) || null;

        let inventorySource = (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length)
            ? window.ALL_CARS
            : ((typeof BOOTSTRAP_GRID_CARS !== "undefined" && Array.isArray(BOOTSTRAP_GRID_CARS))
                ? BOOTSTRAP_GRID_CARS
                : []);

        const radiusCacheKey = listingsRadiusFilterKey();
        const hasRadiusCache = !!(
            radiusCacheKey
            && _radiusFilterKey === radiusCacheKey
            && Array.isArray(_radiusFilteredCars)
        );
        if (hasRadiusCache) {
            inventorySource = _radiusFilteredCars;
        }

        let cars = inventorySource.filter(c => carMatchesFacetFilters(c, facetState, dealerFilterSet));

        if (hasRadiusCache && listingsHasValidZip() && radiusMi) {
            renderCarGrid(cars, { resetPage: true });
            return;
        }

        if (zipCode && radiusMi && listingsHasValidZip() && typeof haversineJS === "function") {
            const applyRadius = (origin, gen) => {
                if (gen != null && listingsZipRenderStale(gen)) return;
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
                const radiusCars = applyListingsRadiusFilter(origin, radiusMi, zipCode);
                const filtered = radiusCars.filter(c => carMatchesFacetFilters(c, facetState, dealerFilterSet));
                renderCarGrid(filtered);
            };

            if (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length) {
                showInventoryLoading();
                const gen = _listingsGeoRenderGen;
                loadAllCarsFromApi()
                    .then(() => {
                        if (listingsZipRenderStale(gen)) return;
                        renderResultsNow();
                    })
                    .catch(() => applyRadius(null, gen));
                return;
            }

            ensureListingsGeoCoordsLoaded();
            const cachedOrigin = zipCoordsJS(zipCode);
            if (cachedOrigin) {
                applyRadius(cachedOrigin, renderGen);
                return;
            }
            SC.resolveListingsZipOrigin(zipCode).then((origin) => applyRadius(origin, renderGen));
            return;
        }

        renderCarGrid(cars, { resetPage: true });
    }

    let _filterRenderFrame = null;
    let _pendingFilterRenderOpts = null;

    function scheduleFilterRender(opts) {
        _pendingFilterRenderOpts = opts;
        if (_filterRenderFrame) return;
        _filterRenderFrame = requestAnimationFrame(() => {
            _filterRenderFrame = null;
            const pending = _pendingFilterRenderOpts;
            _pendingFilterRenderOpts = null;
            updateAllCounts();
            renderResultsNowCore(pending);
        });
    }

    function renderResultsNow(opts) {
        if (_filterRenderFrame) {
            cancelAnimationFrame(_filterRenderFrame);
            _filterRenderFrame = null;
        }
        _pendingFilterRenderOpts = null;
        updateAllCounts();
        renderResultsNowCore(opts);
    }

    window.__DS_refreshListingsMarketBadges = function refreshListingsMarketBadges() {
        if (!_listingsAllCars.length) return;
        renderListingsPage();
    };

    function renderResults() {
        scheduleFilterRender();
    }

    window.__DS_renderCarGrid = renderCarGrid;
    window.__DS_runFilterRender = renderResults;
    window.__DS_runFilterRenderInstant = renderResultsNow;

    function carMatchesSmartFilters(c, filters) {
        if (!filters || typeof filters !== "object") return true;

        const vehicleOr = filters.vehicle_or;
        if (Array.isArray(vehicleOr) && vehicleOr.length) {
            const branchHit = vehicleOr.some((vf) => {
                if (!vf || typeof vf !== "object") return false;
                if (vf.make && !SC.valueInListCISmart([vf.make], c.make)) return false;
                if (vf.model) {
                    const md = String(c.model || "").toLowerCase();
                    const want = String(vf.model).toLowerCase();
                    if (md !== want && !md.startsWith(want + " ") && !md.startsWith(want + "-")) {
                        return false;
                    }
                }
                if (vf.trim_contains) {
                    const blob = `${c.trim || ""} ${c.title || ""}`.toLowerCase();
                    if (!blob.includes(String(vf.trim_contains).toLowerCase())) return false;
                }
                return true;
            });
            if (!branchHit) return false;
        } else {
            const makes = filters.make;
            const makeList = Array.isArray(makes) ? makes : makes ? [makes] : [];
            if (makeList.length && !SC.valueInListCISmart(makeList, c.make)) return false;
            const models = filters.model;
            const modelList = Array.isArray(models) ? models : models ? [models] : [];
            if (modelList.length) {
                const md = String(c.model || "").toLowerCase();
                const ok = modelList.some((m) => {
                    const want = String(m).toLowerCase();
                    return md === want || md.startsWith(want + " ") || md.startsWith(want + "-");
                });
                if (!ok) return false;
            }
        }

        const trimNeedles = filters.trim_contains;
        const trimList = Array.isArray(trimNeedles) ? trimNeedles : trimNeedles ? [trimNeedles] : [];
        if (trimList.length) {
            const blob = `${c.trim || ""} ${c.title || ""}`.toLowerCase();
            if (!trimList.some((t) => blob.includes(String(t).toLowerCase()))) return false;
        }

        const drives = filters.drivetrain;
        const driveList = Array.isArray(drives) ? drives : drives ? [drives] : [];
        if (driveList.length && !SC.valueInListCISmart(driveList, c.drivetrain)) return false;

        if (filters.fuel_type && !SC.valueInListCISmart([filters.fuel_type], c.fuel_type)) return false;

        if (filters.forced_induction && !SC.valueInListCISmart([filters.forced_induction], c.forced_induction)) return false;

        if (filters.cpo_only && !c.is_cpo) return false;

        if (filters.cylinders != null && String(c.cylinders) !== String(filters.cylinders)) return false;

        const bodies = filters.body_style;
        const bodyList = Array.isArray(bodies) ? bodies : bodies ? [bodies] : [];
        if (bodyList.length && !SC.valueInListCISmart(bodyList, c.body_style)) return false;

        const ext = filters.exterior_color;
        const extList = Array.isArray(ext) ? ext : ext ? [ext] : [];
        if (extList.length && !carMatchesPaintFamilyBuckets(c, "exterior_color", extList)) return false;

        const intc = filters.interior_color;
        const intList = Array.isArray(intc) ? intc : intc ? [intc] : [];
        if (intList.length && !carMatchesPaintFamilyBuckets(c, "interior_color", intList)) return false;

        if (filters.max_price != null) {
            const cap = Number(filters.max_price);
            if (Number.isFinite(cap) && Number(c.price) > cap) return false;
        }
        if (filters.max_mileage != null) {
            const cap = Number(filters.max_mileage);
            if (Number.isFinite(cap) && Number(c.mileage) > cap) return false;
        }
        if (filters.min_year != null) {
            const y = Number(c.year);
            if (!Number.isFinite(y) || y < Number(filters.min_year)) return false;
        }
        if (filters.max_year != null) {
            const y = Number(c.year);
            if (!Number.isFinite(y) || y > Number(filters.max_year)) return false;
        }

        const hay = SC.carSmartEquipmentHaystack(c);
        const pkgAll = filters.packages_json_contains_all;
        if (Array.isArray(pkgAll) && pkgAll.length) {
            if (!pkgAll.every((needle) => hay.includes(String(needle).toLowerCase()))) return false;
        } else {
            const pkgOne = filters.packages_json_contains;
            const pkgList = filters.packages_json_contains_list;
            const needles = [];
            if (pkgOne) needles.push(String(pkgOne));
            if (Array.isArray(pkgList)) needles.push(...pkgList.map(String));
            if (needles.length) {
                const lows = needles.map((n) => n.toLowerCase());
                if (!lows.some((needle) => hay.includes(needle))) return false;
            }
        }

        return true;
    }

    window.__DS_getListingsInventorySource = function getListingsInventorySource() {
        const radiusCacheKey = listingsRadiusFilterKey();
        if (
            radiusCacheKey
            && _radiusFilterKey === radiusCacheKey
            && Array.isArray(_radiusFilteredCars)
        ) {
            return _radiusFilteredCars;
        }
        if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) return window.ALL_CARS;
        if (Array.isArray(window.INITIAL_GRID_CARS) && window.INITIAL_GRID_CARS.length) {
            return window.INITIAL_GRID_CARS;
        }
        if (Array.isArray(window.BOOTSTRAP_GRID_CARS) && window.BOOTSTRAP_GRID_CARS.length) {
            return window.BOOTSTRAP_GRID_CARS;
        }
        return [];
    };

    window.__DS_renderFromSmartFilters = function renderFromSmartFilters(filters, opts) {
        const source = window.__DS_getListingsInventorySource();
        const dealerFilterSet = buildDealerFilterIdSet();
        const filtered = source.filter(
            (c) => carMatchesSmartFilters(c, filters) && passesDealerFilter(c, dealerFilterSet)
        );
        renderCarGrid(filtered, {
            preserveOrder: false,
            resetPage: true,
            emptyMessage: (opts && opts.emptyMessage) || null,
        });
    };

    window.__DS_applySmartFilters = function(filters, opts) {
        if (!filters) return;
        const skipCascade = !!(opts && opts.skipCascade);
        // Uncheck all filter checkboxes without triggering change events
        document.querySelectorAll(".filter-option input[type=checkbox]").forEach(cb => {
            cb.checked = false;
        });
        function checkFilter(name, value) {
            if (value == null || value === "") return;
            const normVal = String(value).trim().toLowerCase();
            document.querySelectorAll(`input[type=checkbox][name="${name}"]`).forEach(cb => {
                const cbVal = String(cb.value).trim().toLowerCase();
                if (
                    cbVal === normVal
                    || cbVal.startsWith(normVal + " ")
                    || cbVal.startsWith(normVal + "-")
                    || (name === "model" && normVal.length >= 2 && cbVal.startsWith(normVal))
                ) {
                    cb.checked = true;
                }
            });
        }
        function checkFilters(name, values) {
            const list = Array.isArray(values) ? values : [values];
            list.forEach((v) => checkFilter(name, v));
        }
        if (filters.vehicle_or && Array.isArray(filters.vehicle_or)) {
            filters.vehicle_or.forEach((vf) => {
                if (vf && vf.make) checkFilter("make", vf.make);
                if (vf && vf.model) checkFilter("model", vf.model);
            });
        } else {
            if (filters.make) checkFilters("make", filters.make);
            if (filters.model) checkFilters("model", filters.model);
        }
        if (filters.drivetrain) checkFilter("drivetrain", filters.drivetrain);
        if (filters.fuel_type) checkFilter("fuel_type", filters.fuel_type);
        if (filters.forced_induction) checkFilter("forced_induction", filters.forced_induction);
        if (filters.cpo_only) checkFilter("cpo_only", "1");
        if (filters.cylinders != null) checkFilter("cylinders", String(filters.cylinders));
        if (filters.body_style) {
            const bs = filters.body_style;
            const vals = Array.isArray(bs) ? bs : [bs];
            vals.forEach(v => checkFilter("body_style", v));
        }
        const ext = filters.exterior_color;
        if (ext) {
            const vals = Array.isArray(ext) ? ext : [ext];
            vals.forEach(v => checkFilter("exterior_color", v));
        }
        const intc = filters.interior_color;
        if (intc) {
            const vals = Array.isArray(intc) ? intc : [intc];
            vals.forEach(v => checkFilter("interior_color", v));
        }
        function setScalarSelect(name, value) {
            if (value == null || value === "") return;
            const v = String(value);
            const num = Number(value);
            document.querySelectorAll(`#search-form [name="${name}"]`).forEach(el => {
                const opts = Array.from(el.options || []);
                if (opts.some(o => o.value === v) || !Number.isFinite(num)) {
                    el.value = v;  // exact option, or a non-numeric select (condition)
                    return;
                }
                // Numeric bracket select (price/mileage): the exact value isn't an
                // option, so snap to the smallest bracket that still covers it (so an
                // "under $X" constraint isn't silently dropped). Above the top bracket
                // -> Any (no upper bound).
                const geq = opts
                    .map(o => ({ v: o.value, n: Number(o.value) }))
                    .filter(o => Number.isFinite(o.n) && o.n >= num)
                    .sort((a, b) => a.n - b.n);
                el.value = geq.length ? geq[0].v : "";
            });
        }
        if (filters.max_price != null) setScalarSelect("max_price", filters.max_price);
        if (filters.max_mileage != null) setScalarSelect("max_mileage", filters.max_mileage);
        if (filters.inventory_condition) setScalarSelect("inventory_condition", filters.inventory_condition);
        if (filters.packages_json_contains) {
            checkFilter("package", filters.packages_json_contains);
        }
        const pkgAll = filters.packages_json_contains_all;
        if (Array.isArray(pkgAll)) {
            pkgAll.forEach((p) => checkFilter("package", p));
        }
        const pkgList = filters.packages_json_contains_list;
        if (Array.isArray(pkgList)) {
            pkgList.forEach((p) => checkFilter("package", p));
        }
        if (skipCascade) {
            updateAllCounts();
            return;
        }
        runCascade();
        updateAllCounts();
    };

    // ── Radius-aware cascade ───────────────────────────────────────────
    // Build a make/model/trim/etc. row set restricted to cars within the
    // active ZIP+radius so that filter dropdowns only show options that
    // actually have inventory nearby.

    function _buildCarRowsFromCars(cars) {
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
    window.__DS_buildCarRowsFromCars = _buildCarRowsFromCars;

    function applyListingsRadiusFilter(origin, radiusMi, zipCode) {
        const key = `${String(zipCode || "").trim()}|${radiusMi}`;
        if (_radiusFilterKey === key && Array.isArray(_radiusFilteredCars)) {
            return _radiusFilteredCars;
        }
        const source = Array.isArray(window.ALL_CARS) && window.ALL_CARS.length
            ? window.ALL_CARS
            : [];
        const nearby = SC.filterCarsInRadius(source, origin, radiusMi);
        _radiusFilterKey = key;
        _radiusFilteredCars = nearby;
        // Always an array when radius is active (empty when nothing is in range)
        // so the cascade shows 0 instead of falling back to all national cars.
        // null is reserved for "radius inactive" (see clearListingsRadiusCache).
        RADIUS_CAR_ROWS = _buildCarRowsFromCars(nearby);
        return nearby;
    }

    let _radiusRenderRaf = null;
    function refreshRadiusAndRenderNow(renderGen) {
        const zipCode  = scalarVal("zip_code");
        const radiusMi = parseFloat(scalarVal("radius")) || null;
        const gen = renderGen != null ? renderGen : _listingsGeoRenderGen;

        if (!listingsHasValidZip() || !radiusMi) {
            clearListingsRadiusCache();
            if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
                runCascade();
                renderResultsNow();
            } else if (
                typeof BOOTSTRAP_GRID_CARS !== "undefined"
                && Array.isArray(BOOTSTRAP_GRID_CARS)
                && BOOTSTRAP_GRID_CARS.length
            ) {
                runCascade();
                renderCarGrid(BOOTSTRAP_GRID_CARS, { resetPage: true });
            }
            return;
        }

        schedulePersistListingsGeoSession();

        const runRadius = () => {
            if (listingsZipRenderStale(gen)) return;

        const finish = (origin) => {
            if (listingsZipRenderStale(gen)) return;
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
            applyListingsRadiusFilter(origin, radiusMi, zipCode);
            renderResultsNow({ radiusPrefiltered: true, renderGen: gen });
            deferListingsIdleWork(() => {
                if (listingsZipRenderStale(gen)) return;
                runCascade();
            });
            SC.scheduleReloadMarketStats();
            deferListingsIdleWork(() => {
                if (typeof window.__DS_scheduleReloadNearbyDealers === "function") {
                    window.__DS_scheduleReloadNearbyDealers();
                }
            }, 250);
        };

        const runWithOrigin = () => {
            const cached = zipCoordsJS(zipCode);
            if (cached) {
                finish(cached);
                return;
            }
            SC.resolveListingsZipOrigin(zipCode).then((origin) => finish(origin));
        };

        if (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length) {
            showInventoryLoading();
            loadAllCarsFromApi()
                .then(() => {
                    if (listingsZipRenderStale(gen)) return;
                    runWithOrigin();
                })
                .catch(() => finish(null));
            return;
        }

        runWithOrigin();
        };

        if (!SC.listingsDealerCoordsReady()) {
            showInventoryLoading();
            ensureListingsGeoCoordsLoaded()
                .then(() => runRadius())
                .catch(() => runRadius());
            return;
        }

        runRadius();
    }

    function refreshRadiusAndRender() {
        const cacheKey = listingsRadiusFilterKey();
        if (cacheKey && _radiusFilterKey === cacheKey && Array.isArray(_radiusFilteredCars)) {
            refreshRadiusAndRenderNow();
            return;
        }
        if (_radiusRenderRaf) cancelAnimationFrame(_radiusRenderRaf);
        _radiusRenderRaf = requestAnimationFrame(() => {
            _radiusRenderRaf = null;
            refreshRadiusAndRenderNow();
        });
    }

    function showInventoryLoading() {
        if (!resultsGrid) return;
        if (resultsCount) resultsCount.textContent = "Loading inventory…";
        resultsGrid.innerHTML = SC.skeletonCardsHtml(8);
        if (emptyState) emptyState.style.display = "none";
        if (zeroHintEl) zeroHintEl.hidden = true;
    }

    function fetchListingsCarsJson(skipEtag) {
        const headers = {};
        if (!skipEtag && window.__DS_listingsCarsEtag) {
            headers["If-None-Match"] = window.__DS_listingsCarsEtag;
        }
        return fetch("/api/listings/cars", { credentials: "same-origin", headers })
            .then((r) => {
                if (r.status === 304) {
                    if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
                        return { ok: true, cars: window.ALL_CARS, unchanged: true };
                    }
                    return fetchListingsCarsJson(true);
                }
                if (!r.ok) return Promise.reject(new Error("cars fetch failed"));
                const etag = r.headers.get("ETag");
                if (etag) window.__DS_listingsCarsEtag = etag;
                return r.json();
            });
    }

    function applyListingsCarsPayload(data) {
        if (!data || !data.ok) return false;
        if (data.unchanged) return true;
        if (!Array.isArray(data.cars)) return false;
        window.ALL_CARS = data.cars;
        SC.invalidateCarGeoIndex();
        for (const car of data.cars) {
            if (car && typeof car === "object") delete car._dsRegId;
        }
        if (typeof window.__DS_buildCarRowsFromCars === "function") {
            window.CAR_ROWS = window.__DS_buildCarRowsFromCars(data.cars);
        }
        return true;
    }

    function afterListingsCarsLoaded() {
        if (listingsHasValidZip()) {
            const gen = bumpListingsGeoRenderGen();
            refreshRadiusAndRenderNow(gen);
        } else if (typeof window.__DS_runFilterRender === "function") {
            window.__DS_runFilterRender();
        } else if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
            renderCarGrid(window.ALL_CARS, { resetPage: true });
        }
    }

    let _listingsCarsLoadPromise = null;
    function loadAllCarsFromApi(afterPrefetchAttempt) {
        if (_listingsCarsLoadPromise) return _listingsCarsLoadPromise;
        if (
            !afterPrefetchAttempt
            && (!Array.isArray(window.ALL_CARS) || !window.ALL_CARS.length)
            && window.__DS_listingsCarsPrefetchPromise
        ) {
            _listingsCarsLoadPromise = window.__DS_listingsCarsPrefetchPromise
                .then(() => {
                    if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
                        afterListingsCarsLoaded();
                        return;
                    }
                    _listingsCarsLoadPromise = null;
                    return loadAllCarsFromApi(true);
                })
                .catch((err) => {
                    _listingsCarsLoadPromise = null;
                    throw err;
                });
            return _listingsCarsLoadPromise;
        }
        _listingsCarsLoadPromise = fetchListingsCarsJson(false)
            .then((data) => {
                if (!applyListingsCarsPayload(data)) {
                    throw new Error("invalid cars payload");
                }
            })
            .then(() => {
                afterListingsCarsLoaded();
            })
            .catch((err) => {
                _listingsCarsLoadPromise = null;
                throw err;
            });
        return _listingsCarsLoadPromise;
    }

    function scheduleBackgroundInventoryLoad(run) {
        run();
    }

    window.__DS_ensureListingsCarsLoaded = function ensureListingsCarsLoaded() {
        if (Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
            return Promise.resolve(window.ALL_CARS);
        }
        return loadAllCarsFromApi().then(() => window.ALL_CARS || []);
    };

    function bootApplyListingsFilters() {
        _listingsPage = readListingsPageFromUrl();
        refreshRadiusAndRender();
    }

    function bootListingsGrid() {
        if (typeof INITIAL_GRID_CARS !== "undefined" && Array.isArray(INITIAL_GRID_CARS) && INITIAL_GRID_CARS.length) {
            runCascade();
            const smartIn = document.getElementById("smart-search-input");
            const hasQ = smartIn && (smartIn.value || "").trim();
            renderCarGrid(INITIAL_GRID_CARS, { preserveOrder: !!hasQ, resetPage: !hasQ });
            return;
        }
        if (typeof ALL_CARS !== "undefined" && Array.isArray(ALL_CARS) && ALL_CARS.length) {
            runCascade();
            bootApplyListingsFilters();
            return;
        }
        if (Array.isArray(window.__DS_prefetchCars) && window.__DS_prefetchCars.length) {
            window.ALL_CARS = window.__DS_prefetchCars;
            if (typeof window.__DS_buildCarRowsFromCars === "function") {
                window.CAR_ROWS = window.__DS_buildCarRowsFromCars(window.ALL_CARS);
            }
            runCascade();
            bootApplyListingsFilters();
            return;
        }
        if (typeof BOOTSTRAP_GRID_CARS !== "undefined" && Array.isArray(BOOTSTRAP_GRID_CARS) && BOOTSTRAP_GRID_CARS.length) {
            runCascade();
            if (listingsHasValidZip()) {
                bootApplyListingsFilters();
            } else {
                renderCarGrid(BOOTSTRAP_GRID_CARS, { resetPage: true });
            }
            scheduleBackgroundInventoryLoad(() => {
                loadAllCarsFromApi()
                    .then(() => {
                        runCascade();
                        bootApplyListingsFilters();
                    })
                    .catch(() => {});
            });
            return;
        }
        showInventoryLoading();
        scheduleBackgroundInventoryLoad(() => {
            loadAllCarsFromApi()
                .then(() => {
                    runCascade();
                    bootApplyListingsFilters();
                })
                .catch(() => {
                if (resultsCount) resultsCount.textContent = "";
                if (resultsGrid) {
                    resultsGrid.innerHTML = "";
                }
                if (emptyState) {
                    emptyState.style.display = "";
                    const msg = emptyState.querySelector(".no-results");
                    const sub = emptyState.querySelector(".no-results-sub");
                    if (msg) msg.textContent = "Could not load inventory.";
                    if (sub) sub.textContent = "Refresh the page or try again in a moment.";
                }
            });
        });
    }

    function recordFilterAction(target) {
        if (!target || !target.name) return;
        if (target.type === "checkbox") {
            _lastFilterAction = {
                type: "checkbox",
                name: target.name,
                value: target.value,
                restoreChecked: !target.checked,
            };
        } else if (target.tagName === "SELECT") {
            _lastFilterAction = {
                type: "select",
                name: target.name,
                restoreValue: target.dataset.dsPrev != null ? target.dataset.dsPrev : "",
            };
        }
    }

    function undoLastFilter() {
        const a = _lastFilterAction;
        if (!a) return;
        if (a.type === "checkbox") {
            document.querySelectorAll(`input[name="${a.name}"]`).forEach((cb) => {
                if (String(cb.value) === String(a.value)) cb.checked = a.restoreChecked;
            });
        } else if (a.type === "select") {
            document.querySelectorAll(`#search-form [name="${a.name}"]`).forEach((el) => {
                el.value = a.restoreValue;
            });
        }
        _lastFilterAction = null;
        if (zeroHintEl) zeroHintEl.hidden = true;
        resetListingsPage();
        runCascade();
        updateAllCounts();
        renderResults();
    }

    if (undoFilterBtn) {
        undoFilterBtn.addEventListener("click", undoLastFilter);
    }

    const searchForm = document.getElementById("search-form");
    if (searchForm) {
        searchForm.addEventListener("focusin", (e) => {
            const t = e.target;
            if (t && t.tagName === "SELECT") {
                t.dataset.dsPrev = t.value;
            }
        });
        searchForm.addEventListener("change", (e) => {
            recordFilterAction(e.target);
        });
    }

    const FILTER_CHIP_LABELS = {
        make: "Make", model: "Model", trim: "Trim", fuel_type: "Fuel",
        cylinders: "Cylinders", transmission: "Trans.", drivetrain: "Drive",
        forced_induction: "Engine", body_style: "Body", exterior_color: "Exterior", interior_color: "Interior",
        country: "Country", package: "Package",
        max_price: "Price", max_mileage: "Mileage", inventory_condition: "Condition",
        zip_code: "ZIP", radius: "Radius", q: "Search",
    };

    function scalarChipLabel(name, val) {
        if (name === "max_price" && val) {
            const n = Number(val);
            return Number.isFinite(n) ? `Under $${n >= 1000 ? Math.round(n / 1000) + "k" : n}` : val;
        }
        if (name === "max_mileage" && val) {
            const n = Number(val);
            return Number.isFinite(n) ? `Under ${n.toLocaleString()} mi` : val;
        }
        if (name === "inventory_condition" && val) {
            if (val === "new") return "New";
            if (val === "pre_owned") return "Pre-owned";
            if (val === "cpo") return "Certified Pre-Owned";
        }
        if (name === "radius" && val) return `${val} mi radius`;
        if (name === "zip_code" && val) return val;
        return val;
    }

    function filterOptionVisible(cb) {
        const opt = cb.closest(".filter-option");
        if (!opt) return true;
        if (opt.closest("#filter-docked-inner")) return false;
        if (opt.style.display === "none") return false;
        return true;
    }

    function syncActiveFilterChips() {
        if (!activeChipsEl) return;
        const chips = [];
        const seen = new Set();
        const multiParams = ["make", "model", "trim", "fuel_type", "cylinders", "transmission",
            "drivetrain", "forced_induction", "body_style", "exterior_color", "interior_color", "country", "package", "cpo_only"];
        for (const name of multiParams) {
            document.querySelectorAll(`input[name="${name}"]:checked`).forEach((cb) => {
                if (!filterOptionVisible(cb)) return;
                const key = `${name}|${String(cb.value).trim().toLowerCase()}`;
                if (seen.has(key)) return;
                seen.add(key);
                const opt = cb.closest(".filter-option");
                const span = opt && opt.querySelector("span");
                chips.push({
                    param: name,
                    value: cb.value,
                    label: (span && span.textContent.trim()) || cb.value,
                });
            });
        }
        const zipChip = scalarVal("zip_code");
        if (zipChip && SC.isValidUsZip(zipChip)) {
            chips.push({ param: "zip_code", value: zipChip, label: zipChip });
            const radiusChip = scalarVal("radius");
            if (radiusChip) {
                chips.push({ param: "radius", value: radiusChip, label: scalarChipLabel("radius", radiusChip) });
            }
        }
        for (const name of ["max_price", "max_mileage", "inventory_condition"]) {
            const val = scalarVal(name);
            if (!val) continue;
            chips.push({ param: name, value: val, label: scalarChipLabel(name, val) });
        }
        const smartIn = document.getElementById("smart-search-input");
        const q = smartIn ? (smartIn.value || "").trim() : "";
        if (q) chips.push({ param: "q", value: q, label: q.length > 28 ? q.slice(0, 28) + "…" : q });

        const narrowedDealerIds = selectedDealerRegistryIds();
        if (narrowedDealerIds.length) {
            const form = document.getElementById("search-form");
            const root = form || document;
            narrowedDealerIds.forEach((id) => {
                const key = `dealer_registry_id|${id}`;
                if (seen.has(key)) return;
                seen.add(key);
                const cb = root.querySelector(`input[name="dealer_registry_id"][value="${id}"]`);
                const opt = cb && cb.closest(".dealer-filter-option");
                const span = opt && opt.querySelector("span");
                const label = (span && span.textContent.trim()) || `Dealer #${id}`;
                chips.push({ param: "dealer_registry_id", value: String(id), label });
            });
        }

        activeChipsEl.innerHTML = chips.map((c) => (
            `<button type="button" class="listings-filter-chip" data-chip-param="${SC.escapeHtml(c.param)}" data-chip-value="${SC.escapeHtml(c.value)}">`
            + `<span class="listings-filter-chip-label">${SC.escapeHtml(c.label)}</span>`
            + `<span class="listings-filter-chip-x" aria-hidden="true">×</span>`
            + `</button>`
        )).join("");

        if (activeChipsWrap) activeChipsWrap.hidden = chips.length === 0;
        if (filtersMobileCount) {
            filtersMobileCount.textContent = String(chips.length);
            filtersMobileCount.hidden = chips.length === 0;
        }
    }

    function removeFilterChip(param, value) {
        if (param === "q") {
            const smartIn = document.getElementById("smart-search-input");
            if (smartIn) smartIn.value = "";
            const sync = document.getElementById("form-q-sync");
            if (sync) sync.value = "";
        } else if (param === "zip_code") {
            syncSearchFormZipInputs("");
            _listingsGeoLastSent = null;
            cancelListingsZipWork();
            bumpListingsGeoRenderGen();
            syncUrl();
            syncActiveFilterChips();
            refreshRadiusAndRender();
            return;
        } else if (param === "dealer_registry_id") {
            const form = document.getElementById("search-form");
            const root = form || document;
            root.querySelectorAll('input[name="dealer_registry_id"]').forEach((cb) => {
                if (String(cb.value) === String(value)) cb.checked = false;
            });
            const { total, checkedCount } = dealerRegistryCheckboxState();
            if (total && checkedCount === 0) {
                root.querySelectorAll('input[name="dealer_registry_id"]').forEach((cb) => {
                    cb.checked = true;
                });
            }
            if (typeof window.__DS_reloadNearbyDealers === "function") {
                window.__DS_syncActiveDealerIdsFromDom();
            }
        } else if (["max_price", "max_mileage", "inventory_condition", "radius"].includes(param)) {
            document.querySelectorAll(`#search-form [name="${param}"]`).forEach((el) => { el.value = ""; });
        } else {
            document.querySelectorAll(`input[name="${param}"]`).forEach((cb) => {
                if (String(cb.value) === String(value)) cb.checked = false;
            });
        }
        resetListingsPage();
        updateAllCounts();
        renderResults();
    }

    if (activeChipsEl) {
        activeChipsEl.addEventListener("click", (e) => {
            const btn = e.target.closest(".listings-filter-chip");
            if (!btn) return;
            removeFilterChip(btn.dataset.chipParam, btn.dataset.chipValue || "");
        });
    }
    if (chipsClearBtn) {
        chipsClearBtn.addEventListener("click", () => {
            window.location.href = window.location.pathname;
        });
    }

    function setFiltersSheetOpen(open) {
        document.body.classList.toggle("listings-filters-open", open);
        if (filterBackdrop) {
            filterBackdrop.hidden = !open;
            filterBackdrop.setAttribute("aria-hidden", open ? "false" : "true");
        }
        if (filtersOpenBtn) filtersOpenBtn.setAttribute("aria-expanded", open ? "true" : "false");
        document.body.style.overflow = open ? "hidden" : "";
    }

    if (filtersOpenBtn) {
        filtersOpenBtn.addEventListener("click", () => {
            setFiltersSheetOpen(!document.body.classList.contains("listings-filters-open"));
        });
    }
    if (filterBackdrop) {
        filterBackdrop.addEventListener("click", () => setFiltersSheetOpen(false));
    }
    if (sidebarPanel) {
        sidebarPanel.addEventListener("click", (e) => {
            if (e.target.closest(".pill-clear-btn, .listings-filters-sheet-done")) {
                setFiltersSheetOpen(false);
            }
        });
    }

    function wireResultSaveButtons() {
        if (!resultsGrid || !LISTINGS_LOGGED_IN) return;
        resultsGrid.querySelectorAll(".result-save-btn").forEach((btn) => {
            if (btn.dataset.wired === "1") return;
            btn.dataset.wired = "1";
            btn.addEventListener("click", (e) => {
                e.preventDefault();
                e.stopPropagation();
                const carId = btn.dataset.carId;
                const m = document.querySelector('meta[name="csrf-token"]');
                const csrf = m && m.content ? m.content : "";
                btn.disabled = true;
                fetch(`/api/cars/${carId}/save`, {
                    method: "POST",
                    headers: { "X-CSRF-Token": csrf, "Content-Type": "application/json" },
                    credentials: "same-origin",
                })
                    .then((r) => (r.ok ? r.json() : Promise.reject()))
                    .then((data) => {
                        if (!data || !data.ok) {
                            btn.setAttribute("title", "Could not save — try again");
                            return;
                        }
                        btn.removeAttribute("title");
                        const saved = !!data.saved;
                        const idNum = Number(carId);
                        if (window.SAVED_CAR_IDS instanceof Set) {
                            if (saved) window.SAVED_CAR_IDS.add(idNum);
                            else window.SAVED_CAR_IDS.delete(idNum);
                        }
                        btn.classList.toggle("result-save-btn--saved", saved);
                        btn.setAttribute("aria-label", saved ? "Saved" : "Save this car");
                        btn.querySelector("svg").setAttribute("fill", saved ? "currentColor" : "none");
                    })
                    .catch(() => {
                        btn.setAttribute("title", "Could not save — try again");
                    })
                    .finally(() => { btn.disabled = false; });
            });
        });
    }

    let _listingsGridBooted = false;
    let _listingsGeoReadyCallbacks = [];
    window.__DS_listingsGeoState = { ready: false, blocked: false };

    const LISTINGS_ZIP_STORAGE_KEY = "ds_listings_geo_zip";

    function persistListingsZipLocal(zip) {
        const z = String(zip || "").trim();
        if (!SC.isValidUsZip(z)) return;
        try {
            localStorage.setItem(LISTINGS_ZIP_STORAGE_KEY, z);
        } catch (_) {}
    }

    function readListingsZipLocal() {
        try {
            const z = localStorage.getItem(LISTINGS_ZIP_STORAGE_KEY);
            return SC.isValidUsZip(z) ? String(z).trim() : "";
        } catch (_) {
            return "";
        }
    }

    function clearListingsZipLocal() {
        try {
            localStorage.removeItem(LISTINGS_ZIP_STORAGE_KEY);
        } catch (_) {}
    }

    function patchListingCarLinkZips(zip) {
        const z = String(zip || "").trim();
        if (!SC.isValidUsZip(z)) return;
        document.querySelectorAll(".result-card-link").forEach((a) => {
            const href = a.getAttribute("href") || "";
            const m = href.match(/^\/car\/(\d+)/);
            if (!m) return;
            a.setAttribute("href", `/car/${m[1]}?zip_code=${encodeURIComponent(z)}`);
        });
    }

    function setListingsZipCode(zip) {
        const z = String(zip || "").trim();
        if (!SC.isValidUsZip(z)) return;
        document.querySelectorAll('[name="zip_code"]').forEach((el) => {
            el.value = z;
        });
        persistListingsZipLocal(z);
        hideListingsZipPromptBanner();
        patchListingCarLinkZips(z);
        syncUrl();
    }

    function showListingsGeoPrompt(kind) {
        setListingsGeoHint(
            kind === "loading"
                ? "Getting your location… Allow access or enter your ZIP below."
                : "Enter your ZIP to filter inventory near you."
        );
        if (kind !== "loading") {
            showListingsZipCallout();
            maybeShowListingsZipPromptBanner();
        }
    }

    function hideListingsGeoPrompt() {
        /* no-op: ZIP callout handles filter-without-ZIP UX */
    }

    function flushListingsGeoReadyCallbacks() {
        const pending = _listingsGeoReadyCallbacks.slice();
        _listingsGeoReadyCallbacks = [];
        pending.forEach((fn) => {
            try { fn(); } catch (_) {}
        });
    }

    function markListingsGeoReady() {
        window.__DS_listingsGeoState.ready = true;
        window.__DS_listingsGeoState.blocked = false;
        hideListingsGeoPrompt();
        bootListingsGridOnce();
        flushListingsGeoReadyCallbacks();
    }

    window.__DS_whenListingsGeoReady = function whenListingsGeoReady(fn) {
        if (typeof fn !== "function") return;
        if (window.__DS_listingsGeoState.ready) fn();
        else _listingsGeoReadyCallbacks.push(fn);
    };

    function requestListingsGeolocation() {
        return new Promise((resolve, reject) => {
            if (!navigator.geolocation) {
                reject(new Error("unsupported"));
                return;
            }
            navigator.geolocation.getCurrentPosition(
                (pos) => resolve({ lat: pos.coords.latitude, lon: pos.coords.longitude }),
                (err) => reject(err),
                { enableHighAccuracy: false, timeout: 10000, maximumAge: 300000 }
            );
        });
    }

    function bootListingsGridOnce() {
        if (_listingsGridBooted) return;
        _listingsGridBooted = true;
        const smartIn = document.getElementById("smart-search-input");
        const hasQ = smartIn && (smartIn.value || "").trim();
        if (hasQ) {
            if (typeof ALL_CARS !== "undefined" && Array.isArray(ALL_CARS) && ALL_CARS.length) {
                runCascade();
            } else if (typeof BOOTSTRAP_GRID_CARS !== "undefined" && Array.isArray(BOOTSTRAP_GRID_CARS) && BOOTSTRAP_GRID_CARS.length) {
                loadAllCarsFromApi()
                    .then(() => runCascade())
                    .catch(() => {});
            }
            return;
        }
        bootListingsGrid();
    }

    function resolveListingsGeo() {
        if (SC.isValidUsZip(scalarVal("zip_code"))) {
            persistListingsZipLocal(scalarVal("zip_code"));
            markListingsGeoReady();
            return;
        }
        // Restore the last ZIP saved on this device before falling back to
        // geolocation; the input event runs the full existing zip pipeline
        // (sync, persist, chips, radius refresh, dealer picker reload).
        const savedZip = readListingsZipLocal();
        const zipEl = document.getElementById("listings-zip-input");
        if (savedZip && zipEl) {
            zipEl.value = savedZip;
            zipEl.dispatchEvent(new Event("input", { bubbles: true }));
            markListingsGeoReady();
            return;
        }
        maybeShowListingsZipPromptBanner();
        showListingsGeoPrompt("loading");
        requestListingsGeolocation()
            .then(({ lat, lon }) => fetch(
                `/api/coords-to-zip?lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}`,
                { credentials: "same-origin" }
            ))
            .then((r) => (r.ok ? r.json() : null))
            .then((data) => {
                if (data && SC.isValidUsZip(data.zip_code)) {
                    setListingsZipCode(data.zip_code);
                    markListingsGeoReady();
                    return;
                }
                window.__DS_listingsGeoState.blocked = true;
                showListingsGeoPrompt("denied");
                if (!window.__DS_listingsGeoState.ready) markListingsGeoReady();
            })
            .catch(() => {
                window.__DS_listingsGeoState.blocked = true;
                showListingsGeoPrompt("denied");
                if (!window.__DS_listingsGeoState.ready) markListingsGeoReady();
            });
    }

    document.querySelectorAll('[name="zip_code"]').forEach((el) => {
        el.addEventListener("input", () => {
            const z = (el.value || "").trim();
            if (!SC.isValidUsZip(z)) return;
            if (window.__DS_listingsGeoState.ready) return;
            markListingsGeoReady();
        });
    });

    markListingsGeoReady();
    resolveListingsGeo();
    syncActiveFilterChips();

    ensureListingsGeoCoordsLoaded();
    loadAllCarsFromApi().catch(() => {});

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
                ensureLazyFacet(param);
                runCascade();
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
            if (!isOpen) {
                ensureLazyFacet(param);
                runCascade();
            }
        });
    });

    // Background fill so smart search, chips and the make→model cascade see the
    // full option lists without the user having to open the section first.
    hydrateAllLazyFacets();

    // Poll inventory JSON while a scan writes to inventory.db (LISTINGS_CLIENT_POLL_MS, e.g. 8000).
    const pollAttr = document.body && document.body.getAttribute("data-listings-poll-ms");
    const pollMs = pollAttr != null ? parseInt(pollAttr, 10) : 0;
    if (Number.isFinite(pollMs) && pollMs > 0) {
        setInterval(() => {
            fetchListingsCarsJson(false)
                .then((data) => {
                    if (!applyListingsCarsPayload(data) || data.unchanged) return;
                    const smartIn = document.getElementById("smart-search-input");
                    if (smartIn && (smartIn.value || "").trim()) return;
                    if (typeof runCascade === "function") runCascade();
                    if (typeof window.__DS_runFilterRender === "function") {
                        window.__DS_runFilterRender();
                    }
                })
                .catch(() => {});
        }, pollMs);
    }

    document.querySelectorAll("form.nav-search").forEach((navForm) => {
        navForm.addEventListener("submit", (e) => {
            const zipInput = navForm.querySelector('input[name="zip_code"]');
            if (zipInput) {
                const z = (zipInput.value || "").trim();
                if (z && !/^\d{5}$/.test(z)) zipInput.value = "";
            }
            const qInput = navForm.querySelector('input[name="q"]');
            if (qInput && !(qInput.value || "").trim()) {
                e.preventDefault();
            }
        });
    });

    function syncListingsCompareFromStorage() {
        if (typeof window.__DS_compareSyncTray === "function") {
            window.__DS_compareSyncTray();
        }
    }

    window.addEventListener("pageshow", syncListingsCompareFromStorage);
    window.addEventListener("ds-compare-changed", syncListingsCompareFromStorage);

});
