/**
 * Single-dealer Leaflet map on car detail (same tiles/markers as find dealers).
 */
(function () {
    "use strict";

    const mapEl = document.getElementById("car-dealer-map");
    const jsonEl = document.getElementById("car-dealer-map-json");
    if (!mapEl || !jsonEl || typeof L === "undefined") return;

    let payload;
    try {
        payload = JSON.parse(jsonEl.textContent || "null");
    } catch (_e) {
        return;
    }
    if (!payload || !payload.has_pin || payload.lat == null || payload.lon == null) return;

    const lat = Number(payload.lat);
    const lon = Number(payload.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

    const iconBase = window.__DS_LEAFLET_ICON_BASE || "/static/vendor/leaflet/images/";
    L.Icon.Default.prototype.options.iconUrl = iconBase + "marker-icon.png";
    L.Icon.Default.prototype.options.iconRetinaUrl = iconBase + "marker-icon-2x.png";
    L.Icon.Default.prototype.options.shadowUrl = iconBase + "marker-shadow.png";

    const map = L.map(mapEl, { scrollWheelZoom: false }).setView([lat, lon], 14);
    if (window.DSMapTiles) {
        DSMapTiles.addSatelliteTiles(map);
    } else {
        L.tileLayer(
            "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            {
                maxZoom: 19,
                attribution:
                    "Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS community",
            }
        ).addTo(map);
    }

    const pin = L.divIcon({
        className: "find-dealers-marker",
        html: '<span class="find-dealers-marker-pin" style="background:#1a5f4a"></span>',
        iconSize: [14, 14],
        iconAnchor: [7, 7],
    });

    const lines = ["<strong>" + escapeHtml(payload.name || "Dealership") + "</strong>"];
    if (payload.address_line) {
        lines.push(escapeHtml(payload.address_line));
    }
    L.marker([lat, lon], { icon: pin }).addTo(map).bindPopup(lines.join("<br>"));

    requestAnimationFrame(function () {
        map.invalidateSize();
    });

    // The map now lives in the Dealership tab panel, which starts hidden. Leaflet sizes
    // itself against the container at init, so it would come up as a grey box; car_page.js
    // calls this after revealing a panel.
    window.__DS_resizeDealerMap = function () {
        requestAnimationFrame(function () {
            map.invalidateSize();
        });
    };

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = String(s);
        return d.innerHTML;
    }
})();
