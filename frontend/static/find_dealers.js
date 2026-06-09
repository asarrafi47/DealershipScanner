/**
 * Find dealers: map + list (registry + Google Places via /api/dealer-locator).
 */
(function () {
    "use strict";

    const form = document.getElementById("find-dealers-form");
    const statusEl = document.getElementById("find-dealers-status");
    const resultsWrap = document.getElementById("find-dealers-results");
    const legend = document.getElementById("find-dealers-legend");
    const listEl = document.getElementById("find-dealers-list");
    const countEl = document.getElementById("find-dealers-count");
    const mapEl = document.getElementById("find-dealers-map");
    const submitBtn = document.getElementById("find-dealers-submit");

    if (!form || !mapEl || typeof L === "undefined") return;

    let map = null;
    let markerLayer = null;
    let markersByKey = {};

    function setStatus(msg, isError) {
        if (!statusEl) return;
        statusEl.textContent = msg || "";
        statusEl.classList.toggle("find-dealers-status--error", !!isError);
    }

    function showPanel(mode) {
        ["geo", "zip", "city"].forEach((m) => {
            const panel = document.getElementById("find-dealers-panel-" + m);
            if (panel) panel.hidden = m !== mode;
        });
    }

    function selectedMode() {
        const checked = form.querySelector('input[name="loc_mode"]:checked');
        return checked ? checked.value : "geo";
    }

    form.querySelectorAll('input[name="loc_mode"]').forEach((radio) => {
        radio.addEventListener("change", () => showPanel(selectedMode()));
    });
    showPanel(selectedMode());

    function ensureMap(center) {
        if (!map) {
            map = L.map(mapEl, { scrollWheelZoom: true }).setView(
                [center.lat, center.lon],
                11
            );
            L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
                maxZoom: 19,
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
            }).addTo(map);
            markerLayer = L.layerGroup().addTo(map);
            const iconBase = window.__DS_LEAFLET_ICON_BASE || "/static/vendor/leaflet/images/";
            L.Icon.Default.prototype.options.iconUrl = iconBase + "marker-icon.png";
            L.Icon.Default.prototype.options.iconRetinaUrl = iconBase + "marker-icon-2x.png";
            L.Icon.Default.prototype.options.shadowUrl = iconBase + "marker-shadow.png";
        } else {
            map.setView([center.lat, center.lon], map.getZoom());
        }
    }

    function markerColor(dealer) {
        return dealer.in_database ? "#1a5f4a" : "#6b7280";
    }

    function buildPopupHtml(d) {
        const lines = [
            "<strong>" + escapeHtml(d.name) + "</strong>",
        ];
        const addr = formatAddress(d);
        if (addr) lines.push(escapeHtml(addr));
        if (d.distance_miles != null) {
            lines.push(escapeHtml(d.distance_miles.toFixed(1) + " mi away"));
        }
        if (d.in_database) {
            const n = d.listing_count || 0;
            lines.push('<span class="find-dealers-badge find-dealers-badge--db">In our database · ' + n + " listing" + (n === 1 ? "" : "s") + "</span>");
        } else {
            lines.push('<span class="find-dealers-badge find-dealers-badge--google">Found via Google</span>');
        }
        if (d.website_url) {
            const href = safeHttpHref(d.website_url);
            if (href) {
                lines.push('<a href="' + escapeAttr(href) + '" target="_blank" rel="noopener noreferrer">Website</a>');
            }
        }
        if (d.registry_id && d.listing_count > 0) {
            lines.push('<a href="/listings?dealer_registry_id=' + encodeURIComponent(d.registry_id) + '">View inventory</a>');
        }
        return lines.join("<br>");
    }

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = String(s);
        return d.innerHTML;
    }

    function escapeAttr(s) {
        return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
    }

    function safeHttpHref(url) {
        const u = String(url || "").trim();
        if (/^https?:\/\//i.test(u)) return u;
        return "";
    }

    function formatAddress(d) {
        const parts = [];
        if (d.street_address) parts.push(d.street_address);
        const cs = [d.city, d.state].filter(Boolean).join(", ");
        if (cs) parts.push(cs);
        if (d.zip_code) parts.push(d.zip_code);
        return parts.join(" · ");
    }

    function renderResults(data) {
        const dealers = data.dealers || [];
        const center = data.center;
        if (!center) return;

        resultsWrap.hidden = false;
        legend.hidden = false;
        countEl.textContent = "(" + dealers.length + ")";
        ensureMap(center);

        if (markerLayer) markerLayer.clearLayers();
        markersByKey = {};

        const bounds = L.latLngBounds([center.lat, center.lon]);

        dealers.forEach((d) => {
            const lat = d.latitude;
            const lon = d.longitude;
            if (lat == null || lon == null) return;
            const ll = L.latLng(lat, lon);
            bounds.extend(ll);

            const icon = L.divIcon({
                className: "find-dealers-marker",
                html: '<span class="find-dealers-marker-pin" style="background:' + markerColor(d) + '"></span>',
                iconSize: [14, 14],
                iconAnchor: [7, 7],
            });
            const marker = L.marker(ll, { icon }).bindPopup(buildPopupHtml(d));
            marker.addTo(markerLayer);
            markersByKey[d.key] = marker;
        });

        if (dealers.length) {
            map.fitBounds(bounds.pad(0.12));
        }

        listEl.innerHTML = dealers.map((d) => renderListItem(d)).join("");
        listEl.querySelectorAll(".find-dealers-list-item").forEach((li) => {
            li.addEventListener("click", () => focusDealer(li.dataset.dealerKey));
            li.addEventListener("keydown", (e) => {
                if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    focusDealer(li.dataset.dealerKey);
                }
            });
        });
    }

    function renderListItem(d) {
        const badge = d.in_database
            ? '<span class="find-dealers-list-badge find-dealers-list-badge--db">In database</span>'
            : '<span class="find-dealers-list-badge find-dealers-list-badge--google">Google</span>';
        const dist = d.distance_miles != null ? d.distance_miles.toFixed(1) + " mi" : "";
        const stock = d.in_database && d.listing_count > 0
            ? '<a class="find-dealers-list-link" href="/listings?dealer_registry_id=' + encodeURIComponent(d.registry_id) + '">' + d.listing_count + " listings</a>"
            : (d.in_database ? '<span class="find-dealers-list-muted">No live listings yet</span>' : "");
        const web = d.website_url
            ? '<a class="find-dealers-list-link" href="' + escapeAttr(d.website_url) + '" target="_blank" rel="noopener noreferrer">Website</a>'
            : "";
        return (
            '<li class="find-dealers-list-item" tabindex="0" role="button" data-dealer-key="' + escapeAttr(d.key) + '">' +
            '<div class="find-dealers-list-item__head">' +
            '<span class="find-dealers-list-item__name">' + escapeHtml(d.name) + "</span>" +
            badge +
            "</div>" +
            '<p class="find-dealers-list-item__addr">' + escapeHtml(formatAddress(d) || "Address unavailable") + "</p>" +
            '<p class="find-dealers-list-item__meta">' + escapeHtml(dist) + (stock ? " · " + stock : "") + (web ? " · " + web : "") + "</p>" +
            "</li>"
        );
    }

    function focusDealer(key) {
        const marker = markersByKey[key];
        if (!marker || !map) return;
        map.setView(marker.getLatLng(), Math.max(map.getZoom(), 13));
        marker.openPopup();
        listEl.querySelectorAll(".find-dealers-list-item").forEach((el) => {
            el.classList.toggle("find-dealers-list-item--active", el.dataset.dealerKey === key);
        });
    }

    function buildQueryParams(mode) {
        const radius = document.getElementById("find-dealers-radius").value || "25";
        const params = new URLSearchParams({ radius, include_google: "1" });
        if (mode === "zip") {
            const zip = (document.getElementById("find-dealers-zip").value || "").trim();
            if (!zip) throw new Error("Enter a ZIP code.");
            params.set("zip", zip);
            return params;
        }
        if (mode === "city") {
            const city = (document.getElementById("find-dealers-city").value || "").trim();
            const state = (document.getElementById("find-dealers-state").value || "").trim().toUpperCase();
            if (!city || state.length !== 2) throw new Error("Enter city and a 2-letter state.");
            params.set("city", city);
            params.set("state", state);
            return params;
        }
        return null;
    }

    function getGeolocation() {
        return new Promise((resolve, reject) => {
            if (!navigator.geolocation) {
                reject(new Error("Geolocation is not supported in this browser."));
                return;
            }
            navigator.geolocation.getCurrentPosition(
                (pos) => resolve({ lat: pos.coords.latitude, lon: pos.coords.longitude }),
                (err) => reject(new Error(err.message || "Could not get your location.")),
                { enableHighAccuracy: false, timeout: 15000, maximumAge: 60000 }
            );
        });
    }

    async function runSearch() {
        const mode = selectedMode();
        setStatus("Searching…");
        submitBtn.disabled = true;
        try {
            let params = buildQueryParams(mode);
            if (mode === "geo") {
                const coords = await getGeolocation();
                params = new URLSearchParams({
                    lat: String(coords.lat),
                    lon: String(coords.lon),
                    radius: document.getElementById("find-dealers-radius").value || "25",
                    include_google: "1",
                });
            }
            const resp = await fetch("/api/dealer-locator?" + params.toString(), {
                headers: { Accept: "application/json" },
            });
            const ct = (resp.headers.get("content-type") || "").toLowerCase();
            let data;
            if (ct.includes("application/json")) {
                data = await resp.json();
            } else {
                throw new Error(
                    resp.ok
                        ? "Unexpected server response. Try again or use ZIP / city search."
                        : "Dealer search failed on the server. Try again in a moment."
                );
            }
            if (!resp.ok || !data.ok) {
                const err = data.error || "Search failed.";
                if (err === "location_not_found") {
                    throw new Error("We could not find that location. Try another ZIP or city.");
                }
                if (err === "rate_limited") {
                    throw new Error("Too many searches. Please wait a minute and try again.");
                }
                if (err === "login_required") {
                    throw new Error("Sign in to search with Google Places results, or use ZIP / city search.");
                }
                throw new Error(err);
            }
            const inDb = data.in_database_count || 0;
            let msg = "Found " + data.total + " dealer" + (data.total === 1 ? "" : "s");
            msg += " (" + inDb + " in our database";
            if (data.google_included) msg += ", includes Google results";
            msg += ").";
            if (!data.google_available) {
                msg += " Google search is unavailable (no API key on server).";
            }
            setStatus(msg, false);
            renderResults(data);
        } catch (e) {
            setStatus(e.message || "Search failed.", true);
        } finally {
            submitBtn.disabled = false;
        }
    }

    form.addEventListener("submit", (e) => {
        e.preventDefault();
        runSearch();
    });
})();
