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

    function normalizeEntry(raw) {
        if (raw == null) return null;
        if (typeof raw === "number" || typeof raw === "string") {
            const id = parseInt(raw, 10);
            return Number.isFinite(id) && id > 0 ? { id: id, title: "", image: "" } : null;
        }
        if (typeof raw === "object") {
            const id = parseInt(raw.id, 10);
            if (!Number.isFinite(id) || id <= 0) return null;
            return {
                id: id,
                title: String(raw.title || "").trim(),
                image: String(raw.image || "").trim(),
            };
        }
        return null;
    }

    function readEntries() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            const parsed = raw ? JSON.parse(raw) : [];
            if (!Array.isArray(parsed)) return [];
            const seen = new Set();
            const out = [];
            parsed.forEach(function (item) {
                const entry = normalizeEntry(item);
                if (!entry || seen.has(entry.id)) return;
                seen.add(entry.id);
                out.push(entry);
            });
            return out.slice(0, MAX_COMPARE);
        } catch (_) {
            return [];
        }
    }

    function notifyCompareChanged() {
        try {
            window.dispatchEvent(
                new CustomEvent("ds-compare-changed", { detail: { ids: readIds() } })
            );
        } catch (_) {}
    }

    function writeEntries(entries) {
        try {
            localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify(
                    entries.slice(0, MAX_COMPARE).map(function (entry) {
                        return {
                            id: entry.id,
                            title: entry.title || "",
                            image: entry.image || "",
                        };
                    })
                )
            );
        } catch (_) {}
        notifyCompareChanged();
    }

    function readIds() {
        return readEntries().map(function (entry) {
            return entry.id;
        });
    }

    function cachedMetaById(id) {
        const n = parseInt(id, 10);
        if (!Number.isFinite(n)) return null;
        const hit = readEntries().find(function (entry) {
            return entry.id === n;
        });
        if (!hit) return null;
        return {
            title: hit.title || ("Vehicle #" + n),
            image: hit.image || "",
        };
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
        const cached = cachedMetaById(id);
        if (cached && cached.title && cached.title.indexOf("Vehicle #") !== 0) {
            return cached;
        }

        const n = parseInt(id, 10);
        if (!Number.isFinite(n)) return null;

        const jsonEl = document.getElementById("compare-cars-json");
        if (jsonEl && jsonEl.textContent) {
            try {
                const cars = JSON.parse(jsonEl.textContent);
                if (Array.isArray(cars)) {
                    const hit = cars.find(function (c) {
                        return Number(c.id) === n;
                    });
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
            const hit = window.ALL_CARS.find(function (c) {
                return Number(c.id) === n;
            });
            if (hit) {
                const gallery = Array.isArray(hit.gallery) ? hit.gallery : [];
                return {
                    title: hit.title || ("Vehicle #" + n),
                    image: gallery[0] || hit.image_url || "",
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

        if (cached) return cached;
        return { title: "Vehicle #" + n, image: "" };
    }

    function syncCompareControls(ids) {
        const idList = Array.isArray(ids) ? ids : readIds();
        document.querySelectorAll(".result-compare-cb").forEach(function (cb) {
            const cid = parseInt(cb.dataset.carId, 10);
            cb.checked = idList.includes(cid);
            const label = cb.closest(".result-compare-label");
            if (label) label.classList.toggle("result-compare-label--active", cb.checked);
        });

        document.querySelectorAll("[data-compare-toggle]").forEach(function (btn) {
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
        slots.innerHTML = ids
            .map(function (id) {
                const meta = carMetaById(id);
                const safeImg = meta && meta.image ? safeCssBackgroundUrl(meta.image) : "";
                const img = safeImg ? ' style="background-image:url(\'' + safeImg + "')" : "";
                const title = meta ? meta.title : "#" + id;
                return (
                    '<span class="compare-tray__slot" data-car-id="' +
                    id +
                    '">' +
                    '<span class="compare-tray__slot-thumb"' +
                    img +
                    ' aria-hidden="true"></span>' +
                    '<span class="compare-tray__slot-title">' +
                    escapeHtml(title) +
                    "</span>" +
                    '<button type="button" class="compare-tray__slot-remove" data-compare-tray-remove="' +
                    id +
                    '" aria-label="Remove from compare">×</button>' +
                    "</span>"
                );
            })
            .join("");
        if (go) {
            go.href = compareUrl(ids);
            go.textContent = "Compare " + ids.length + " vehicle" + (ids.length !== 1 ? "s" : "");
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

    /** http(s) only for CSS background-image (mitigates javascript: / data: in compare tray). */
    function safeCssBackgroundUrl(url) {
        const raw = String(url || "").trim();
        if (!raw) return "";
        try {
            const abs = new URL(raw, window.location.origin);
            if (abs.protocol !== "http:" && abs.protocol !== "https:") return "";
            return abs.href.replace(/'/g, "%27");
        } catch (_) {
            return "";
        }
    }

    function toggleId(carId, on) {
        if (on && !isLoggedIn()) {
            requireCompareLogin();
            return readIds();
        }
        const id = parseInt(carId, 10);
        if (!Number.isFinite(id) || id <= 0) return readIds();

        let entries = readEntries();
        const exists = entries.some(function (entry) {
            return entry.id === id;
        });

        if (on) {
            if (!exists) {
                const meta = carMetaById(id);
                if (entries.length >= MAX_COMPARE) {
                    entries = entries.slice(1);
                }
                entries.push({
                    id: id,
                    title: meta ? meta.title : "Vehicle #" + id,
                    image: meta ? meta.image : "",
                });
                writeEntries(entries);
            }
        } else {
            writeEntries(
                entries.filter(function (entry) {
                    return entry.id !== id;
                })
            );
        }

        syncTray();
        syncUrlWithIds(readIds());
        const tray = document.getElementById("compare-tray");
        if (on && tray && !tray.hidden) {
            tray.scrollIntoView({ behavior: "smooth", block: "nearest" });
        }
        return readIds();
    }

    window.__DS_compareToggle = toggleId;
    window.__DS_compareSyncTray = syncTray;
    window.__DS_compareReadIds = readIds;
    window.__DS_compareUrl = compareUrl;

    function wireTray() {
        const clearBtn = document.getElementById("compare-tray-clear");
        if (clearBtn) {
            clearBtn.addEventListener("click", function () {
                writeEntries([]);
                syncTray();
                syncUrlWithIds([]);
                if (document.body.classList.contains("compare-page")) {
                    window.location.href = "/compare";
                }
            });
        }
        document.addEventListener("click", function (e) {
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
        grid.addEventListener("click", function (e) {
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
        document.querySelectorAll("[data-compare-toggle]").forEach(function (btn) {
            btn.addEventListener("click", function () {
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
            const ids = parseCompareIdsFromQuery(qs.get("ids"));
            writeEntries(
                ids.map(function (id) {
                    const meta = carMetaById(id);
                    return {
                        id: id,
                        title: meta ? meta.title : "Vehicle #" + id,
                        image: meta ? meta.image : "",
                    };
                })
            );
            syncTray();
        }

        document.querySelectorAll("[data-compare-remove]").forEach(function (btn) {
            btn.addEventListener("click", function () {
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
            .map(function (p) {
                return parseInt(p.trim(), 10);
            })
            .filter(function (n) {
                return Number.isFinite(n) && n > 0;
            })
            .slice(0, MAX_COMPARE);
    }

    function refreshCompareFromStorage() {
        syncTray();
    }

    function init() {
        if (!isLoggedIn() && !document.body.classList.contains("compare-page")) {
            writeEntries([]);
            return;
        }
        wireTray();
        wireGridCheckboxes();
        wireCompareToggleButtons();
        wireComparePage();
        syncCompareControls(readIds());
    }

    window.addEventListener("pageshow", refreshCompareFromStorage);
    document.addEventListener("visibilitychange", function () {
        if (document.visibilityState === "visible") refreshCompareFromStorage();
    });

    window.addEventListener("storage", function (e) {
        if (e.key === STORAGE_KEY) syncTray();
    });

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
