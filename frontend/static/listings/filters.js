/**
 * Listings filter predicates: condition, paint family, hidden/selected dealers,
 * the facet (checkbox/select) matcher and the smart-search matcher.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL and read only their arguments plus the SC helpers from
 * sc-helpers.js / geo.js (resolved at call time). main.js gathers the page
 * state (checked boxes, hidden-dealer set, dealer subset) and passes it in.
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});

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
        // 0 mi is NOT evidence of "new": it is the feed's "not listed" sentinel on
        // 752 active Used rows (visual review 2026-09-28, DC-6 / IH-03).
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

    /** `hidden` is window.HIDDEN_DEALER_IDS: a Set of lower-cased dealer_id strings. */
    function carIsFromHiddenDealer(c, hidden) {
        if (!(hidden instanceof Set) || !hidden.size) return false;
        return hidden.has(String(c.dealer_id || "").trim().toLowerCase());
    }

    // Every client-side render path (token preview, parse preview) draws from this
    // source, so the signed-in user's hidden dealerships are removed here as well as
    // in the facet filter: a hidden store must never flash into the grid while the
    // server response (already filtered) is in flight.
    function withoutHiddenDealers(cars, hidden) {
        if (!(hidden instanceof Set) || !hidden.size) return cars;
        return cars.filter((c) => !carIsFromHiddenDealer(c, hidden));
    }

    /** Memoised on the row as _dsRegId (cleared when a new payload lands). */
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

    /** Country checkboxes narrow (or supply) the make list (COUNTRY_TO_MAKES). */
    function facetMakesFilter(makes, countries, countryToMakes) {
        let makesFilter = makes.slice();
        if (countries.length && typeof countryToMakes === "object") {
            const fromCountries = countries.flatMap(c => countryToMakes[c] || []);
            makesFilter = makesFilter.length
                ? makesFilter.filter(m => SC.valueInListCI(fromCountries, m))
                : fromCountries;
        }
        return makesFilter;
    }

    /**
     * Facet filter: `state` is main.js collectFacetFilterState(), `dealerFilterSet`
     * the premium dealer subset (null = all), `hidden` window.HIDDEN_DEALER_IDS.
     */
    function carMatchesFacetFilters(c, state, dealerFilterSet, hidden) {
        if (carIsFromHiddenDealer(c, hidden)) return false;
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

    /** Smart-search filters (the parsed `q` payload) against one car. */
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
            // A payment-shaped figure is not the price, so it can never satisfy a
            // price cap; such cards are dropped from any "under $X" result.
            if (c.payment_listed === true) return false;
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

    /**
     * Does a filter checkbox value match a smart-filter value? (applySmartFilters
     * checks every box this returns true for.) `normVal` is already trimmed+lowered.
     */
    function smartFilterOptionMatches(name, normVal, optionValue) {
        const cbVal = String(optionValue).trim().toLowerCase();
        return (
            cbVal === normVal
            || cbVal.startsWith(normVal + " ")
            || cbVal.startsWith(normVal + "-")
            || (name === "model" && normVal.length >= 2 && cbVal.startsWith(normVal))
        );
    }

    /**
     * Value to put in a scalar <select> (price/mileage/condition) for a smart
     * filter: the exact option, or a non-numeric value as-is; otherwise snap to
     * the smallest numeric bracket that still covers it ("" = Any above the top).
     */
    function scalarSelectValue(optionValues, value) {
        const v = String(value);
        const num = Number(value);
        if (optionValues.some(o => o === v) || !Number.isFinite(num)) {
            return v;  // exact option, or a non-numeric select (condition)
        }
        // Numeric bracket select (price/mileage): the exact value isn't an
        // option, so snap to the smallest bracket that still covers it (so an
        // "under $X" constraint isn't silently dropped). Above the top bracket
        // -> Any (no upper bound).
        const geq = optionValues
            .map(o => ({ v: o, n: Number(o) }))
            .filter(o => Number.isFinite(o.n) && o.n >= num)
            .sort((a, b) => a.n - b.n);
        return geq.length ? geq[0].v : "";
    }

    DSL.carMatchesPaintFamilyBuckets = carMatchesPaintFamilyBuckets;
    DSL.carListingCondition = carListingCondition;
    DSL.passesInventoryConditionFilter = passesInventoryConditionFilter;
    DSL.carIsFromHiddenDealer = carIsFromHiddenDealer;
    DSL.withoutHiddenDealers = withoutHiddenDealers;
    DSL.carRegistryIdCached = carRegistryIdCached;
    DSL.carMatchesDealerFilter = carMatchesDealerFilter;
    DSL.facetMakesFilter = facetMakesFilter;
    DSL.carMatchesFacetFilters = carMatchesFacetFilters;
    DSL.carMatchesSmartFilters = carMatchesSmartFilters;
    DSL.smartFilterOptionMatches = smartFilterOptionMatches;
    DSL.scalarSelectValue = scalarSelectValue;
})(window);
