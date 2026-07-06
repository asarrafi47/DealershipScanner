/**
 * Shared sidebar behavior (signed-in + guest): mobile toggle/backdrop/close,
 * plus the Compare shortcut that appears once the compare tray has vehicles.
 */
(function () {
    "use strict";

    const COMPARE_STORAGE_KEY = "ds_compare_ids";
    const MAX_COMPARE = 4;

    function readCompareIds() {
        try {
            const raw = localStorage.getItem(COMPARE_STORAGE_KEY);
            const parsed = raw ? JSON.parse(raw) : [];
            if (!Array.isArray(parsed)) return [];
            const seen = new Set();
            const out = [];
            parsed.forEach(function (item) {
                const id = parseInt(item && typeof item === "object" ? item.id : item, 10);
                if (!Number.isFinite(id) || id <= 0 || seen.has(id)) return;
                seen.add(id);
                out.push(id);
            });
            return out.slice(0, MAX_COMPARE);
        } catch (_) {
            return [];
        }
    }

    function syncCompareLink() {
        const link = document.getElementById("sidebar-compare-link");
        if (!link) return;
        const ids = readCompareIds();
        if (!ids.length) {
            link.hidden = true;
            return;
        }
        link.href = "/compare?ids=" + ids.join(",");
        link.hidden = false;
    }

    function wireSidebarToggle() {
        const sidebar = document.getElementById("app-sidebar");
        const toggle = document.getElementById("app-sidebar-toggle");
        const backdrop = document.getElementById("app-sidebar-backdrop");
        const closeBtn = document.getElementById("app-sidebar-close");
        if (!sidebar || !toggle) return;
        function openMenu() {
            document.body.classList.add("app-sidebar-open");
            toggle.setAttribute("aria-expanded", "true");
            if (backdrop) backdrop.hidden = false;
            if (closeBtn) closeBtn.hidden = false;
        }
        function closeMenu() {
            document.body.classList.remove("app-sidebar-open");
            toggle.setAttribute("aria-expanded", "false");
            if (backdrop) backdrop.hidden = true;
            if (closeBtn) closeBtn.hidden = true;
        }
        toggle.addEventListener("click", function () {
            if (document.body.classList.contains("app-sidebar-open")) closeMenu();
            else openMenu();
        });
        if (backdrop) backdrop.addEventListener("click", closeMenu);
        if (closeBtn) closeBtn.addEventListener("click", closeMenu);
    }

    function init() {
        wireSidebarToggle();
        syncCompareLink();
    }

    window.addEventListener("ds-compare-changed", syncCompareLink);
    window.addEventListener("pageshow", syncCompareLink);
    window.addEventListener("storage", function (e) {
        if (e.key === COMPARE_STORAGE_KEY) syncCompareLink();
    });

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
