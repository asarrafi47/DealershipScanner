(function () {
    "use strict";
    // Resilient base-tile layer for every Leaflet map on the site.
    //
    // OSM's free tile host throttles individual clients under its usage
    // policy; seen live as an all-gray map whose controls, marker, and
    // attribution still render (the tiles are the only external images).
    // Curl and a fresh headless browser load the same tiles fine, so the
    // block is per-client and transient — the fix is a fallback host, not a
    // retry. After 3 tile errors the layer swaps to the Carto raster
    // basemap (free tier, OSM data, permissive for low-volume use).
    function addResilientTiles(map) {
        var errors = 0;
        var swapped = false;
        var osm = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
            maxZoom: 19,
            attribution:
                '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        }).addTo(map);
        osm.on("tileerror", function () {
            errors += 1;
            if (swapped || errors < 3) return;
            swapped = true;
            try { map.removeLayer(osm); } catch (e) { /* already gone */ }
            L.tileLayer(
                "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
                {
                    maxZoom: 19,
                    subdomains: "abcd",
                    attribution:
                        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                        + ' &copy; <a href="https://carto.com/attributions">CARTO</a>',
                }
            ).addTo(map);
        });
        return osm;
    }

    // Satellite imagery basemap (Esri World Imagery, no API key required) with a
    // transparent roads/labels overlay on top so streets and place names stay
    // legible — matches the "hybrid" satellite view users expect.
    function addSatelliteTiles(map) {
        var imagery = L.tileLayer(
            "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            {
                maxZoom: 19,
                attribution:
                    "Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS community",
            }
        ).addTo(map);
        L.tileLayer(
            "https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
            { maxZoom: 19 }
        ).addTo(map);
        return imagery;
    }

    window.DSMapTiles = { addResilientTiles: addResilientTiles, addSatelliteTiles: addSatelliteTiles };
})();
