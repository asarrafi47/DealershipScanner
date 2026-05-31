/**
 * Compare tray (listings + car detail) + localStorage sync. Max 4 vehicles.
 */
(function () {
    "use strict";

    const STORAGE_KEY = "ds_compare_ids";
    const MAX_COMPARE = 4;

    function isLoggedIn() {
        return document.body && document.body.getAttribute("data-logged-in") === "1";
    }

    function requireCompareLogin() {
        window.location.href = "/login";
        return false;
    }

    function readIds() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            const parsed = raw ? JSON.parse(raw) : [];
            if (!Array.isArray(parsed)) return [];
            return parsed
                .map((id) => parseInt(id, 10))
                .filter((n) => Number.isFinite(n) && n > 0)
                .slice(0, MAX_COMPARE);
        } catch (_) {
            return [];
        }
    }

    function writeIds(ids) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(ids.slice(0, MAX_COMPARE)));
        } catch (_) {}
    }

    function compareUrl(ids) {
        if (!ids.length) return "/compare";
        return "/compare?ids=" + ids.join(",");
    }

    function syncUrlWithIds(ids) {
        if (!document.body.classList.contains("compare-page")) return;
        const qs = new URLSearchParams(window.location.search);
        const current = (qs.get("ids") || "").trim();
        const next = ids.join(",");
        if (current === next) return;
        if (ids.length) {
            window.history.replaceState(null, "", compareUrl(ids));
        }
    }

    function carMetaById(id) {
        const n = parseInt(id, 10);
        if (!Number.isFinite(n)) return null;

        const jsonEl = document.getElementById("compare-cars-json");
        if (jsonEl && jsonEl.textContent) {
            try {
                const cars = JSON.parse(jsonEl.textContent);
                if (Array.isArray(cars)) {
                    const hit = cars.find((c) => Number(c.id) === n);
                    if (hit) {
                        return {
                            title: hit.title || ("Vehicle #" + n),
                            image: hit.compare_image_url || hit.image_url || "",
                        };
                    }
                }
            } catch (_) {}
        }

        if (Array.isArray(window.ALL_CARS)) {
            const hit = window.ALL_CARS.find((c) => Number(c.id) === n);
            if (hit) {
                const gallery = Array.isArray(hit.gallery) ? hit.gallery : [];
                return {
                    title: hit.title || ("Vehicle #" + n),
                    image: (gallery[0] || hit.image_url || ""),
                };
            }
        }

        const pageCar = document.body.getAttribute("data-car-id");
        if (pageCar && parseInt(pageCar, 10) === n) {
            const titleEl = document.querySelector(".car-detail-title");
            const imgEl = document.getElementById("car-gallery-main-img");
            return {
                title: titleEl ? titleEl.textContent.trim() : ("Vehicle #" + n),
                image: imgEl ? imgEl.getAttribute("src") || "" : "",
            };
        }

        return { title: "Vehicle #" + n, image: "" };
    }

    function syncCompareControls(ids) {
        const idList = Array.isArray(ids) ? ids : readIds();
        document.querySelectorAll(".result-compare-cb").forEach((cb) => {
            const cid = parseInt(cb.dataset.carId, 10);
            cb.checked = idList.includes(cid);
            const label = cb.closest(".result-compare-label");
            if (label) label.classList.toggle("result-compare-label--active", cb.checked);
        });

        document.querySelectorAll("[data-compare-toggle]").forEach((btn) => {
            const cid = parseInt(btn.getAttribute("data-car-id"), 10);
            const on = idList.includes(cid);
            btn.classList.toggle("compare-toggle-btn--active", on);
            btn.setAttribute("aria-pressed", on ? "true" : "false");
            btn.setAttribute("title", on ? "Remove from compare" : "Add to compare (up to 4 vehicles)");
            const label = btn.querySelector(".compare-toggle-btn__label");
            if (label) label.textContent = on ? "In compare" : "Compare";
        });
    }

    function syncTray() {
        const ids = readIds();
        syncCompareControls(ids);

        const tray = document.getElementById("compare-tray");
        const slots = document.getElementById("compare-tray-slots");
        const go = document.getElementById("compare-tray-go");
        const clearBtn = document.getElementById("compare-tray-clear");
        if (!tray || !slots) return;

        if (!ids.length) {
            tray.hidden = true;
            document.body.classList.remove("compare-tray-open");
            return;
        }

        tray.hidden = false;
        document.body.classList.add("compare-tray-open");
        slots.innerHTML = ids.map((id) => {
            const meta = carMetaById(id);
            const img = meta && meta.image
                ? ` style="background-image:url('${String(meta.image).replace(/'/g, "%27")}')"`
                : "";
            const title = meta ? meta.title : ("#" + id);
            return (
                `<span class="compare-tray__slot" data-car-id="${id}">`
                + `<span class="compare-tray__slot-thumb"${img} aria-hidden="true"></span>`
                + `<span class="compare-tray__slot-title">${escapeHtml(title)}</span>`
                + `<button type="button" class="compare-tray__slot-remove" data-compare-tray-remove="${id}" aria-label="Remove from compare">×</button>`
                + `</span>`
            );
        }).join("");
        if (go) {
            go.href = compareUrl(ids);
            go.textContent = `Compare ${ids.length} vehicle${ids.length !== 1 ? "s" : ""}`;
        }
        if (clearBtn) clearBtn.disabled = false;
    }

    function escapeHtml(text) {
        return String(text || "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function toggleId(carId, on) {
        if (on && !isLoggedIn()) {
            requireCompareLogin();
            return readIds();
        }
        const id = parseInt(carId, 10);
        if (!Number.isFinite(id) || id <= 0) return;
        let ids = readIds();
        if (on) {
            if (!ids.includes(id)) {
                if (ids.length >= MAX_COMPARE) {
                    ids = ids.slice(1);
                }
                ids.push(id);
            }
        } else {
            ids = ids.filter((x) => x !== id);
        }
        writeIds(ids);
        syncTray();
        syncUrlWithIds(ids);
        const tray = document.getElementById("compare-tray");
        if (on && tray && !tray.hidden) {
            tray.scrollIntoView({ behavior: "smooth", block: "nearest" });
        }
        return ids;
    }

    window.__DS_compareToggle = toggleId;
    window.__DS_compareSyncTray = syncTray;
    window.__DS_compareReadIds = readIds;
    window.__DS_compareUrl = compareUrl;

    function wireTray() {
        const clearBtn = document.getElementById("compare-tray-clear");
        if (clearBtn) {
            clearBtn.addEventListener("click", () => {
                writeIds([]);
                syncTray();
                syncUrlWithIds([]);
                if (document.body.classList.contains("compare-page")) {
                    window.location.href = "/compare";
                }
            });
        }
        document.addEventListener("click", (e) => {
            const rm = e.target.closest("[data-compare-tray-remove]");
            if (!rm) return;
            e.preventDefault();
            toggleId(rm.getAttribute("data-compare-tray-remove"), false);
        });
        syncTray();
    }

    function wireGridCheckboxes() {
        const grid = document.getElementById("results-grid");
        if (!grid) return;
        grid.addEventListener("click", (e) => {
            const label = e.target.closest(".result-compare-label");
            if (!label) return;
            e.preventDefault();
            e.stopPropagation();
            const cb = label.querySelector(".result-compare-cb");
            if (!cb) return;
            cb.checked = !cb.checked;
            toggleId(cb.dataset.carId, cb.checked);
        });
    }

    function wireCompareToggleButtons() {
        document.querySelectorAll("[data-compare-toggle]").forEach((btn) => {
            btn.addEventListener("click", () => {
                const id = btn.getAttribute("data-car-id");
                const ids = readIds();
                const on = !ids.includes(parseInt(id, 10));
                toggleId(id, on);
            });
        });
    }

    function wireComparePage() {
        if (!document.body.classList.contains("compare-page")) return;

        const urlIds = readIds();
        const qs = new URLSearchParams(window.location.search);
        if (!qs.get("ids") && urlIds.length) {
            window.location.replace(compareUrl(urlIds));
            return;
        }
        if (qs.get("ids")) {
            writeIds(parseCompareIdsFromQuery(qs.get("ids")));
            syncTray();
        }

        document.querySelectorAll("[data-compare-remove]").forEach((btn) => {
            btn.addEventListener("click", () => {
                const ids = toggleId(btn.getAttribute("data-compare-remove"), false);
                if (!ids.length) {
                    window.location.href = "/compare";
                    return;
                }
                window.location.href = compareUrl(ids);
            });
        });
    }

    function parseCompareIdsFromQuery(raw) {
        return String(raw || "")
            .split(",")
            .map((p) => parseInt(p.trim(), 10))
            .filter((n) => Number.isFinite(n) && n > 0)
            .slice(0, MAX_COMPARE);
    }

    function init() {
        if (!isLoggedIn() && !document.body.classList.contains("compare-page")) {
            writeIds([]);
            return;
        }
        wireTray();
        wireGridCheckboxes();
        wireCompareToggleButtons();
        wireComparePage();
        syncCompareControls(readIds());
    }

    window.addEventListener("pageshow", (e) => {
        if (e.persisted) syncCompareControls(readIds());
    });

    window.addEventListener("storage", (e) => {
        if (e.key === STORAGE_KEY) syncTray();
    });

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
