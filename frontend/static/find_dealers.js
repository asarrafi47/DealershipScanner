/**
 * Find dealers: map + list (registry + Google Places via /api/dealer-locator).
 */
(function () {
    "use strict";

    const form = document.getElementById("find-dealers-form");
    const statusEl = document.getElementById("find-dealers-status");
    const legend = document.getElementById("find-dealers-legend");
    const listEl = document.getElementById("find-dealers-list");
    const countEl = document.getElementById("find-dealers-count");
    const mapEl = document.getElementById("find-dealers-map");
    const submitBtn = document.getElementById("find-dealers-submit");
    const mapPlaceholderEl = document.getElementById("find-dealers-map-placeholder");
    const sidebarEmptyEl = document.getElementById("find-dealers-sidebar-empty");
    const sidebarResultsEl = document.getElementById("find-dealers-sidebar-results");

    if (!form || !mapEl || typeof L === "undefined") return;

    const adminCfg = (function () {
        const el = document.getElementById("ds-find-dealers-admin");
        if (!el || !el.textContent) return null;
        try {
            const cfg = JSON.parse(el.textContent);
            return cfg && cfg.site_admin ? cfg : null;
        } catch (_e) {
            return null;
        }
    })();

    function csrfToken() {
        const m = document.querySelector('meta[name="csrf-token"]');
        return m && m.content ? m.content : "";
    }

    function canRequestScrape(d) {
        return !!(adminCfg && adminCfg.onboard_enabled && d && !d.in_database && d.website_url);
    }

    function adminScrapeButtonHtml(d) {
        if (!adminCfg || !adminCfg.onboard_enabled) {
            if (adminCfg && !adminCfg.onboard_enabled) {
                return '<p class="find-dealers-admin-hint">Scrape queue needs Postgres (<code>./deploy/up.sh --full</code>).</p>';
            }
            return "";
        }
        if (d.in_database) {
            return "";
        }
        if (!d.website_url) {
            return '<p class="find-dealers-admin-hint">No website from Google — add manually at <a href="/admin/dealers">/admin/dealers</a>.</p>';
        }
        return (
            '<button type="button" class="find-dealers-admin-btn primary-button" data-dealer-key="' +
            escapeAttr(d.key) +
            '">Request scrape</button>'
        );
    }

    async function requestDealerScrape(d, btn) {
        if (!d || !d.website_url) return;
        const prev = btn.textContent;
        btn.disabled = true;
        btn.textContent = "Queuing…";
        try {
            const headers = { "Content-Type": "application/json", Accept: "application/json" };
            const t = csrfToken();
            if (t) headers["X-CSRF-Token"] = t;
            const resp = await fetch("/api/admin/dealer-onboard", {
                method: "POST",
                credentials: "same-origin",
                headers,
                body: JSON.stringify({
                    url: d.website_url,
                    name: d.name || "",
                }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.ok) {
                const err = data.error || "Could not queue scrape.";
                throw new Error(err === "postgres_required" ? "Postgres job queue not configured." : err);
            }
            setStatus(
                "Queued onboard job #" +
                    data.job_id +
                    " for " +
                    (data.dealer_id || "dealer") +
                    '. Track progress on <a href="/admin/dealers">Dealer jobs</a> or <a href="/admin/site">Site admin</a>.',
                false,
                true
            );
            btn.textContent = "Queued ✓";
            btn.classList.add("find-dealers-admin-btn--done");
        } catch (e) {
            setStatus(e.message || "Could not queue scrape.", true);
            btn.disabled = false;
            btn.textContent = prev;
        }
    }

    function bindAdminScrapeButtons(root) {
        if (!adminCfg || !root) return;
        root.querySelectorAll(".find-dealers-admin-btn").forEach((btn) => {
            btn.addEventListener("click", (ev) => {
                ev.preventDefault();
                ev.stopPropagation();
                const key = btn.getAttribute("data-dealer-key");
                const d = dealersByKey[key];
                if (d) requestDealerScrape(d, btn);
            });
        });
    }

    let dealersByKey = {};

    let map = null;
    let markerLayer = null;
    let markersByKey = {};

    function setStatus(msg, isError, asHtml) {
        if (!statusEl) return;
        if (asHtml) statusEl.innerHTML = msg || "";
        else statusEl.textContent = msg || "";
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

    function refreshMapLayout(bounds) {
        if (!map) return;
        window.requestAnimationFrame(() => {
            map.invalidateSize();
            if (bounds && bounds.isValid()) {
                map.fitBounds(bounds.pad(0.12));
            }
        });
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
        const scraped = scrapeStatusLabel(d);
        if (scraped) {
            lines.push('<span class="find-dealers-scraped">' + escapeHtml(scraped) + "</span>");
        }
        if (d.website_url) {
            const href = safeHttpHref(d.website_url);
            if (href) {
                lines.push('<a href="' + escapeAttr(href) + '" target="_blank" rel="noopener noreferrer">Website</a>');
            }
        }
        const inventoryHref = d.listing_count > 0 ? dealerInventoryHref(d) : "";
        if (inventoryHref) {
            lines.push('<a href="' + escapeAttr(inventoryHref) + '">View inventory</a>');
        }
        const adminBtn = adminScrapeButtonHtml(d);
        if (adminBtn) lines.push(adminBtn);
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

    /**
     * Link into /listings filtered to this dealer. main.js reads the
     * repeatable `dealer_registry_id` param, but only applies it alongside
     * a valid ZIP + radius <= 50 mi — so center the search on the dealer's
     * own ZIP to guarantee it falls inside the radius.
     */
    function dealerInventoryHref(d) {
        if (!d || !d.in_database || !d.registry_id) return "";
        const params = new URLSearchParams();
        const zip = String(d.zip_code || "").trim();
        if (/^\d{5}$/.test(zip)) {
            params.set("zip_code", zip);
            params.set("radius", "25");
        }
        params.set("dealer_registry_id", String(d.registry_id));
        return "/listings?" + params.toString();
    }

    function formatAddress(d) {
        const parts = [];
        if (d.street_address) parts.push(d.street_address);
        const cs = [d.city, d.state].filter(Boolean).join(", ");
        if (cs) parts.push(cs);
        if (d.zip_code) parts.push(d.zip_code);
        return parts.join(" · ");
    }

    function formatLastSynced(iso) {
        const raw = String(iso || "").trim();
        if (!raw) return "";
        const d = new Date(raw);
        if (Number.isNaN(d.getTime())) return "";
        return d.toLocaleDateString(undefined, {
            month: "short",
            day: "numeric",
            year: "numeric",
        });
    }

    function scrapeStatusLabel(d) {
        if (!d) return "";
        const date = formatLastSynced(d.last_synced_at);
        const n = Number(d.listing_count) || 0;
        if (date && n > 0) {
            return "Last scraped · " + date + " · " + n + " car" + (n === 1 ? "" : "s");
        }
        if (date) {
            return "Last scraped · " + date + " · no inventory captured";
        }
        if (n > 0) {
            return n + " car" + (n === 1 ? "" : "s") + " scraped";
        }
        return "";
    }

    function renderResults(data) {
        const dealers = data.dealers || [];
        const center = data.center;
        if (!center) return;

        dealersByKey = {};
        dealers.forEach((d) => {
            if (d && d.key) dealersByKey[d.key] = d;
        });

        if (mapPlaceholderEl) mapPlaceholderEl.hidden = true;
        if (sidebarEmptyEl) sidebarEmptyEl.hidden = true;
        if (sidebarResultsEl) sidebarResultsEl.hidden = false;
        if (legend) legend.hidden = false;
        if (countEl) countEl.textContent = "(" + dealers.length + ")";
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
            marker.on("popupopen", () => bindAdminScrapeButtons(marker.getPopup().getElement()));
        });

        refreshMapLayout(dealers.length ? bounds : null);

        listEl.innerHTML = dealers.map((d) => renderListItem(d)).join("");
        listEl.querySelectorAll(".find-dealers-list-item").forEach((li) => {
            li.addEventListener("click", (ev) => {
                if (ev.target.closest(".find-dealers-admin-btn")) return;
                focusDealer(li.dataset.dealerKey);
            });
            li.addEventListener("keydown", (e) => {
                if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    focusDealer(li.dataset.dealerKey);
                }
            });
        });
        bindAdminScrapeButtons(listEl);
    }

    function renderListItem(d) {
        const badge = d.in_database
            ? '<span class="find-dealers-list-badge find-dealers-list-badge--db">In database</span>'
            : '<span class="find-dealers-list-badge find-dealers-list-badge--google">Google</span>';
        const dist = d.distance_miles != null ? d.distance_miles.toFixed(1) + " mi" : "";
        const scrapeStatus = scrapeStatusLabel(d);
        const inventoryHref = d.listing_count > 0 ? dealerInventoryHref(d) : "";
        const stock = inventoryHref
            ? '<a class="find-dealers-list-link" href="' + escapeAttr(inventoryHref) + '">View ' + d.listing_count + " listings</a>"
            : (d.in_database ? '<span class="find-dealers-list-muted">No live listings yet</span>' : "");
        const scrapeMeta = scrapeStatus
            ? '<span class="find-dealers-list-scraped">' + escapeHtml(scrapeStatus) + "</span>"
            : "";
        const web = d.website_url
            ? '<a class="find-dealers-list-link" href="' + escapeAttr(d.website_url) + '" target="_blank" rel="noopener noreferrer">Website</a>'
            : "";
        const adminBtn = canRequestScrape(d)
            ? '<button type="button" class="find-dealers-admin-btn primary-button find-dealers-admin-btn--inline" data-dealer-key="' +
              escapeAttr(d.key) +
              '">Request scrape</button>'
            : "";
        return (
            '<li class="find-dealers-list-item" tabindex="0" role="button" data-dealer-key="' + escapeAttr(d.key) + '">' +
            '<div class="find-dealers-list-item__head">' +
            '<span class="find-dealers-list-item__name">' + escapeHtml(d.name) + "</span>" +
            badge +
            "</div>" +
            '<p class="find-dealers-list-item__addr">' + escapeHtml(formatAddress(d) || "Address unavailable") + "</p>" +
            '<p class="find-dealers-list-item__meta">' + escapeHtml(dist) + (stock ? " · " + stock : "") + (scrapeMeta ? " · " + scrapeMeta : "") + (web ? " · " + web : "") + "</p>" +
            (adminBtn ? '<p class="find-dealers-list-item__admin">' + adminBtn + "</p>" : "") +
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
            let resp = await fetch("/api/dealer-locator?" + params.toString(), {
                headers: { Accept: "application/json" },
                credentials: "same-origin",
            });
            let ct = (resp.headers.get("content-type") || "").toLowerCase();
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
            if (
                resp.status === 403
                && data.error === "login_required"
                && params.get("include_google") === "1"
            ) {
                params.set("include_google", "0");
                resp = await fetch("/api/dealer-locator?" + params.toString(), {
                    headers: { Accept: "application/json" },
                    credentials: "same-origin",
                });
                ct = (resp.headers.get("content-type") || "").toLowerCase();
                if (ct.includes("application/json")) {
                    data = await resp.json();
                }
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
                    throw new Error("Sign in to include Google Places results in this search.");
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
