/* Geo helpers for the listings pages: haversine distance, ZIP/dealer
 * coordinate lookups, the car geo index, and radius filtering.
 *
 * Extracted verbatim from main.js. Loaded BEFORE main.js (see the
 * <script defer> order in listings.html and dealership.html). The
 * public lookups keep their historical window attachments (haversineJS,
 * zipCoordsJS, dealerCoordsJS, carDealershipRegistryId); everything
 * else attaches to the shared window.SC namespace (created by
 * sc-helpers.js, which loads first).
 *
 * Bridge back into main.js's closure: SC.clearListingsRadiusCache is
 * published by main.js at DOMContentLoaded. mergeListingsGeoCoordsPayload
 * only ever runs from fetch callbacks, which cannot fire before main.js's
 * closure has finished, so the typeof guard below never skips a real call.
 */
(function (SC) {
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
        if (!zipCode || typeof window.ZIP_COORDS !== "object") return null;
        const coords = window.ZIP_COORDS[String(zipCode).trim()];
        return Array.isArray(coords) && coords.length === 2 ? coords : null;
    };

    function dealerHostKey(dealerUrl) {
        try {
            const host = new URL(String(dealerUrl).trim()).hostname.toLowerCase();
            return host.startsWith("www.") ? host.slice(4) : host;
        } catch (_e) {
            return "";
        }
    }

    /** Dealer lat/lon: exact URL, then host: key from geo-coords API. */
    window.dealerCoordsJS = function(dealerUrl) {
        if (!dealerUrl || typeof window.DEALER_COORDS !== "object") return null;
        const u = String(dealerUrl).trim();
        let coords = window.DEALER_COORDS[u] || null;
        if (!coords) {
            const host = dealerHostKey(u);
            if (host) coords = window.DEALER_COORDS[`host:${host}`] || null;
        }
        return Array.isArray(coords) && coords.length === 2 ? coords : null;
    };

    const _zipOriginFetchPromises = Object.create(null);
    const _zipOriginAborters = Object.create(null);

    function abortPendingZipOriginFetches() {
        Object.keys(_zipOriginAborters).forEach((z) => {
            try {
                _zipOriginAborters[z].abort();
            } catch (_) {}
            delete _zipOriginAborters[z];
        });
        Object.keys(_zipOriginFetchPromises).forEach((z) => {
            delete _zipOriginFetchPromises[z];
        });
    }

    function cacheListingsZipOrigin(zipCode, lat, lon) {
        const z = String(zipCode || "").trim();
        if (!z || lat == null || lon == null) return null;
        const origin = [Number(lat), Number(lon)];
        if (!Number.isFinite(origin[0]) || !Number.isFinite(origin[1])) return null;
        if (typeof window.ZIP_COORDS !== "object" || window.ZIP_COORDS === null) {
            window.ZIP_COORDS = {};
        }
        window.ZIP_COORDS[z] = origin;
        return origin;
    }

    function resolveListingsZipOrigin(zipCode) {
        const z = String(zipCode || "").trim();
        if (!z) return Promise.resolve(null);
        const cached = window.zipCoordsJS(z);
        if (cached) return Promise.resolve(cached);
        if (_zipOriginFetchPromises[z]) return _zipOriginFetchPromises[z];
        const controller = new AbortController();
        _zipOriginAborters[z] = controller;
        _zipOriginFetchPromises[z] = fetch(
            `/api/zip-coords?zip=${encodeURIComponent(z)}`,
            { credentials: "same-origin", signal: controller.signal },
        )
            .then((r) => (r.ok ? r.json() : null))
            .then((data) => {
                if (data && data.lat != null && data.lon != null) {
                    return cacheListingsZipOrigin(z, data.lat, data.lon);
                }
                return null;
            })
            .catch((err) => (err && err.name === "AbortError" ? null : null))
            .finally(() => {
                delete _zipOriginFetchPromises[z];
                delete _zipOriginAborters[z];
            });
        return _zipOriginFetchPromises[z];
    }

    /** Lat/lon for radius filter: dealer URL first, then listing ZIP in ZIP_COORDS. */
    function carGeoCoords(car) {
        const regId = carDealershipRegistryId(car);
        if (
            regId &&
            typeof window.REGISTRY_COORDS === "object" &&
            window.REGISTRY_COORDS[String(regId)]
        ) {
            return window.REGISTRY_COORDS[String(regId)];
        }
        const coords = typeof window.dealerCoordsJS === "function"
            ? window.dealerCoordsJS(car.dealer_url)
            : null;
        return Array.isArray(coords) && coords.length === 2 ? coords : null;
    }

    let _carGeoIndex = null;

    function invalidateCarGeoIndex() {
        _carGeoIndex = null;
    }

    function ensureCarGeoIndex(cars) {
        const source = Array.isArray(cars) ? cars : [];
        if (_carGeoIndex && _carGeoIndex.source === source) return _carGeoIndex;
        const lats = new Float64Array(source.length);
        const lons = new Float64Array(source.length);
        const hasGeo = new Uint8Array(source.length);
        for (let i = 0; i < source.length; i += 1) {
            const coords = carGeoCoords(source[i]);
            if (!coords) continue;
            lats[i] = coords[0];
            lons[i] = coords[1];
            hasGeo[i] = 1;
        }
        _carGeoIndex = { source, lats, lons, hasGeo };
        return _carGeoIndex;
    }

    function filterCarsInRadius(cars, origin, radiusMi) {
        if (!origin || !radiusMi || !Array.isArray(cars) || typeof window.haversineJS !== "function") {
            return [];
        }
        const idx = ensureCarGeoIndex(cars);
        const oLat = origin[0];
        const oLon = origin[1];
        const latPad = radiusMi / 69.0;
        const lonPad = radiusMi / Math.max(
            0.2,
            69.0 * Math.cos((oLat * Math.PI) / 180),
        );
        const latMin = oLat - latPad;
        const latMax = oLat + latPad;
        const lonMin = oLon - lonPad;
        const lonMax = oLon + lonPad;
        const out = [];
        for (let i = 0; i < cars.length; i += 1) {
            if (!idx.hasGeo[i]) continue;
            const lat = idx.lats[i];
            const lon = idx.lons[i];
            if (lat < latMin || lat > latMax || lon < lonMin || lon > lonMax) continue;
            if (window.haversineJS(oLat, oLon, lat, lon) <= radiusMi) out.push(cars[i]);
        }
        return out;
    }

    function listingsDealerCoordsReady() {
        const dc = window.DEALER_COORDS;
        return !!(dc && typeof dc === "object" && Object.keys(dc).length);
    }

    function mergeListingsGeoCoordsPayload(data) {
        if (!data || !data.ok) return;
        window.ZIP_COORDS = { ...(window.ZIP_COORDS || {}), ...(data.zip_coords || {}) };
        window.DEALER_COORDS = { ...(window.DEALER_COORDS || {}), ...(data.dealer_coords || {}) };
        window.REGISTRY_COORDS = {
            ...(window.REGISTRY_COORDS || {}),
            ...(data.registry_coords || {}),
        };
        if (data.registry_id_by_host && typeof data.registry_id_by_host === "object") {
            window.REGISTRY_ID_BY_DEALER_HOST = {
                ...(window.REGISTRY_ID_BY_DEALER_HOST || {}),
                ...data.registry_id_by_host,
            };
        }
        invalidateCarGeoIndex();
        if (typeof SC.clearListingsRadiusCache === "function") {
            SC.clearListingsRadiusCache();
        }
        window.__DS_listingsGeoCoordsReady = true;
    }

    /** Registry id from column or dealer_url host (geo-coords host map). */
    function carDealershipRegistryId(car) {
        const reg = parseInt(car && car.dealership_registry_id, 10);
        if (Number.isFinite(reg) && reg > 0) return reg;
        const host = dealerHostKey(car && car.dealer_url);
        if (!host || typeof window.REGISTRY_ID_BY_DEALER_HOST !== "object") return 0;
        const mapped = parseInt(window.REGISTRY_ID_BY_DEALER_HOST[host], 10);
        return Number.isFinite(mapped) && mapped > 0 ? mapped : 0;
    }
    window.carDealershipRegistryId = carDealershipRegistryId;

    SC.dealerHostKey = dealerHostKey;
    SC.abortPendingZipOriginFetches = abortPendingZipOriginFetches;
    SC.cacheListingsZipOrigin = cacheListingsZipOrigin;
    SC.resolveListingsZipOrigin = resolveListingsZipOrigin;
    SC.carGeoCoords = carGeoCoords;
    SC.invalidateCarGeoIndex = invalidateCarGeoIndex;
    SC.ensureCarGeoIndex = ensureCarGeoIndex;
    SC.filterCarsInRadius = filterCarsInRadius;
    SC.listingsDealerCoordsReady = listingsDealerCoordsReady;
    SC.mergeListingsGeoCoordsPayload = mergeListingsGeoCoordsPayload;
    SC.carDealershipRegistryId = carDealershipRegistryId;
})(window.SC = window.SC || {});
