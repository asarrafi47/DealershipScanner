/* Listings boot: unpack the CSP-friendly JSON blobs rendered by
 * listings.html / dealership.html into the window globals every listings
 * script reads (CAR_ROWS, ZIP_COORDS, ...).
 *
 * Extracted verbatim from the top of main.js's DOMContentLoaded closure.
 * Must be loaded BEFORE main.js (see the <script defer> order in
 * listings.html and dealership.html). It runs in its own DOMContentLoaded
 * listener so it fires after dealership_page.js's handler and before
 * main.js's handler — the same relative order as when this code was the
 * first statement inside main.js's handler.
 */
document.addEventListener("DOMContentLoaded", () => {
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
        // The cascade table ships dictionary-encoded (see pack_car_rows in
        // backend/listings/routes.py): ~12,700 rows of repeated make/model/trim
        // strings plus repeated JSON keys were ~1.9 MB of uncompressed HTML.
        function unpackCarRows(packed) {
            if (Array.isArray(packed)) return packed; // legacy/plain form
            if (!packed || !Array.isArray(packed.r) || !Array.isArray(packed.c)) return [];
            const cols = packed.c;
            const vocabs = Array.isArray(packed.v) ? packed.v : [];
            return packed.r.map((row) => {
                const obj = {};
                for (let i = 0; i < cols.length; i++) {
                    const vocab = vocabs[i];
                    if (!Array.isArray(vocab)) {
                        obj[cols[i]] = row[i];
                        continue;
                    }
                    const code = row[i];
                    obj[cols[i]] = code >= 0 && code < vocab.length ? vocab[code] : null;
                }
                return obj;
            });
        }
        window.CAR_ROWS = unpackCarRows(readJsonScript("ds-listings-car-rows", []));
        // Cars are never embedded (owner decision 2026-09-28): main.js loads the
        // shopper's ZIP + radius from /api/listings/cars once a search begins.
        window.ALL_CARS = [];
        window.COUNTRY_TO_MAKES = readJsonScript("ds-listings-country-to-makes", {});
        window.ZIP_COORDS = readJsonScript("ds-listings-zip-coords", {});
        window.DEALER_COORDS = readJsonScript("ds-listings-dealer-coords", {});
        window.INITIAL_GRID_CARS = readJsonScript("ds-listings-initial-grid", []);
        // Dealership page only: first-paint cards for its one rooftop.
        window.BOOTSTRAP_GRID_CARS = readJsonScript("ds-listings-bootstrap-grid", []);
        window.PACKAGE_ROWS = readJsonScript("ds-listings-package-rows", []);
        const savedRaw = readJsonScript("ds-listings-saved-ids", []);
        window.SAVED_CAR_IDS = new Set(
            (Array.isArray(savedRaw) ? savedRaw : [])
                .map((id) => Number(id))
                .filter((n) => Number.isFinite(n) && n > 0)
        );
        // Signed-in user's hidden dealerships (profile -> Hidden dealerships).
        // Absent on pages that don't ship the blob (dealership page): empty set.
        const hiddenRaw = readJsonScript("ds-listings-hidden-dealers", []);
        window.HIDDEN_DEALER_IDS = new Set(
            (Array.isArray(hiddenRaw) ? hiddenRaw : [])
                .map((id) => String(id || "").trim().toLowerCase())
                .filter((id) => id.length > 0)
        );
    })();
});
