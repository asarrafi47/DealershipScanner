/**
 * Listings sort order: the "deprioritise" tiers (no real photo, call for
 * price, single photo) and the sort-mode comparators.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL; the one page-dependent input (distance from the
 * shopper's ZIP) comes in as a callback. Uses DSL.cardMileageNotListed from
 * listings/card.js (resolved at call time).
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});
    // listings/card.js owns the mileage rule; look it up at call time.
    const cardMileageNotListed = (c) => DSL.cardMileageNotListed(c);

    function listingCallForPrice(c) {
        // A payment-shaped "price" ($122/mo scraped into the price field) is
        // no sale price: it sinks with call-for-price in every sort mode.
        if (c && c.payment_listed === true) return true;
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

    /**
     * Sorted copy of `cars` for a sort mode. `distanceOf(car)` returns miles from
     * the shopper's ZIP or null (main.js carDistanceMiles).
     */
    function sortListingsCars(cars, mode, preserveOrder, distanceOf) {
        if (preserveOrder && mode === "relevance") {
            return cars.slice().sort((a, b) => listingDepriorityCompare(a, b));
        }
        const arr = cars.slice();
        const priceKey = (c) => {
            if (c.payment_listed === true) return Infinity;
            const p = Number(c.price);
            return Number.isFinite(p) && p > 0 ? p : Infinity;
        };
        const mileageKey = (c) => {
            if (cardMileageNotListed(c)) return Infinity;
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
            const d = distanceOf(c);
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

    DSL.listingCallForPrice = listingCallForPrice;
    DSL.listingPhotoCount = listingPhotoCount;
    DSL.listingHasRealImage = listingHasRealImage;
    DSL.listingDepriorityCompare = listingDepriorityCompare;
    DSL.sortListingsCars = sortListingsCars;
})(window);
