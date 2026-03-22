/**
 * OSM map discovery: geolocation or zip, list dealers, write manifest for local scanner run.
 */
document.addEventListener("DOMContentLoaded", () => {
    const toggleBtn = document.getElementById("discovery-toggle-btn");
    const panel = document.getElementById("discovery-panel");
    const zipInput = document.getElementById("discovery-zip");
    const radiusSelect = document.getElementById("discovery-radius");
    const geoBtn = document.getElementById("discovery-geo-btn");
    const searchBtn = document.getElementById("discovery-search-btn");
    const nameFilter = document.getElementById("discovery-name-filter");
    const statusEl = document.getElementById("discovery-status");
    const resultsWrap = document.getElementById("discovery-results-wrap");
    const listEl = document.getElementById("discovery-list");
    const checkAll = document.getElementById("discovery-check-all");
    const scanBtn = document.getElementById("discovery-scan-btn");
    const hintEl = document.getElementById("discovery-hint");

    if (!toggleBtn || !panel) return;

    let lastDealers = [];
    let lastCoords = null;

    function setStatus(msg, isError) {
        if (!statusEl) return;
        statusEl.textContent = msg || "";
        statusEl.classList.toggle("error", !!isError);
    }

    function parseNameFilter() {
        const raw = (nameFilter && nameFilter.value) || "";
        return raw
            .split(",")
            .map((s) => s.trim().toLowerCase())
            .filter(Boolean);
    }

    function filterDealers(dealers) {
        const keys = parseNameFilter();
        if (!keys.length) return dealers;
        return dealers.filter((d) => {
            const n = (d.name || "").toLowerCase();
            return keys.some((k) => n.includes(k));
        });
    }

    function renderList(dealers) {
        if (!listEl || !resultsWrap) return;
        listEl.innerHTML = "";
        dealers.forEach((d, i) => {
            const li = document.createElement("li");
            li.className = "discovery-item";
            const dc = !!d.dealer_com;
            const id = `discovery-cb-${i}`;
            li.innerHTML = `
                <input type="checkbox" id="${id}" class="discovery-cb" data-idx="${i}" ${dc ? "checked" : ""}>
                <div class="discovery-item-meta">
                    <div class="discovery-item-name"></div>
                    <a class="discovery-item-url" href="" target="_blank" rel="noopener"></a>
                </div>
                <span class="discovery-badge"></span>
            `;
            li.querySelector(".discovery-item-name").textContent = d.name || "Unknown";
            const a = li.querySelector(".discovery-item-url");
            a.href = d.website || "#";
            a.textContent = d.website || "";
            const badge = li.querySelector(".discovery-badge");
            badge.textContent = dc ? "Dealer.com" : "Not DC";
            badge.classList.add(dc ? "ok" : "no");
            const cb = li.querySelector(".discovery-cb");
            cb.disabled = !dc;
            listEl.appendChild(li);
        });
        resultsWrap.hidden = dealers.length === 0;
    }

    toggleBtn.addEventListener("click", () => {
        const open = panel.hidden;
        panel.hidden = !open;
        toggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
        if (open && zipInput) {
            const pillZip = document.querySelector('input[name="zip_code"].pill-zip, input[name="zip_code"].sidebar-input');
            if (pillZip && !(zipInput.value || "").trim()) {
                zipInput.value = (pillZip.value || "").trim();
            }
        }
    });

    async function runDiscovery(body) {
        setStatus("Searching OpenStreetMap…");
        if (hintEl) hintEl.hidden = true;
        const res = await fetch("/api/discovery/nearby", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.ok) {
            setStatus(data.error || "Discovery failed.", true);
            lastDealers = [];
            renderList([]);
            return;
        }
        lastCoords = data.center || null;
        lastDealers = data.dealers || [];
        const filtered = filterDealers(lastDealers);
        setStatus(
            `${filtered.length} dealer(s) in map results` +
                (lastCoords ? ` near ${lastCoords.lat.toFixed(4)}, ${lastCoords.lon.toFixed(4)}` : "") +
                "."
        );
        renderList(filtered);
    }

    if (nameFilter) {
        nameFilter.addEventListener(
            "input",
            debounce(() => {
                if (!lastDealers.length) return;
                const filtered = filterDealers(lastDealers);
                setStatus(`${filtered.length} dealer(s) after name filter.`);
                renderList(filtered);
            }, 200)
        );
    }

    if (searchBtn) {
        searchBtn.addEventListener("click", () => {
            const zip = (zipInput && zipInput.value.trim()) || "";
            const radius_miles = parseFloat((radiusSelect && radiusSelect.value) || "25") || 25;
            if (!zip) {
                setStatus("Enter a zip code, or use “Use my location”.", true);
                return;
            }
            runDiscovery({ zip, radius_miles, check_dealer_com: true });
        });
    }

    if (geoBtn) {
        geoBtn.addEventListener("click", () => {
            if (!navigator.geolocation) {
                setStatus("Geolocation is not available in this browser.", true);
                return;
            }
            setStatus("Getting your location…");
            navigator.geolocation.getCurrentPosition(
                (pos) => {
                    const radius_miles = parseFloat((radiusSelect && radiusSelect.value) || "25") || 25;
                    runDiscovery({
                        lat: pos.coords.latitude,
                        lon: pos.coords.longitude,
                        radius_miles,
                        check_dealer_com: true,
                    });
                },
                () => {
                    setStatus("Location denied or unavailable. Enter a zip and try again.", true);
                },
                { enableHighAccuracy: false, timeout: 15000, maximumAge: 600000 }
            );
        });
    }

    if (checkAll) {
        checkAll.addEventListener("change", () => {
            document.querySelectorAll(".discovery-cb:not(:disabled)").forEach((cb) => {
                cb.checked = checkAll.checked;
            });
        });
    }

    if (scanBtn) {
        scanBtn.addEventListener("click", async () => {
            const filtered = filterDealers(lastDealers);
            const selected = [];
            document.querySelectorAll(".discovery-cb:checked").forEach((cb) => {
                const idx = parseInt(cb.getAttribute("data-idx"), 10);
                if (Number.isFinite(idx) && filtered[idx]) selected.push(filtered[idx]);
            });
            if (!selected.length) {
                setStatus("Select at least one Dealer.com row to scan (or run discovery again).", true);
                return;
            }
            setStatus("Writing dealers.discovery.json…");
            const res = await fetch("/api/discovery/scan", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ dealers: selected }),
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || !data.ok) {
                setStatus(data.error || "Could not write manifest.", true);
                return;
            }
            setStatus(`Wrote manifest (${data.dealer_com_count} Dealer.com site(s)).`);
            if (hintEl && data.hint) {
                hintEl.textContent = data.hint;
                hintEl.hidden = false;
            }
        });
    }

    function debounce(fn, ms) {
        let t;
        return (...args) => {
            clearTimeout(t);
            t = setTimeout(() => fn(...args), ms);
        };
    }
});
