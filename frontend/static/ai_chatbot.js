/* Sidebar AI assistant. Show/hide persists across page loads (localStorage), and
   when the current page is a car detail (body[data-car-id]) the assistant sends
   that id so answers are about the car in view. */
(function () {
    "use strict";
    var OPEN_KEY = "ds_ai_chat_open";
    var HIST_KEY = "ds_ai_chat_history";
    var HIST_MAX = 24;

    function ready(fn) {
        if (document.readyState !== "loading") fn();
        else document.addEventListener("DOMContentLoaded", fn);
    }

    ready(function () {
        // Apply filters handed off from the assistant on a previous page (once the
        // listings page's inventory has loaded).
        (function applyHandoff() {
            var raw;
            try { raw = sessionStorage.getItem("ds_assistant_search"); } catch (e) { return; }
            if (!raw) return;
            try { sessionStorage.removeItem("ds_assistant_search"); } catch (e) {}
            var payload;
            try { payload = JSON.parse(raw); } catch (e) { return; }
            if (!payload || !payload.filters) return;
            var tries = 0;
            (function tryApply() {
                if (typeof window.__DS_applySmartFilters === "function"
                    && Array.isArray(window.ALL_CARS) && window.ALL_CARS.length) {
                    var bar = document.getElementById("smart-search-input");
                    if (bar && payload.q) bar.value = payload.q;
                    try { window.__DS_applySmartFilters(payload.filters); } catch (e) {}
                } else if (tries++ < 25) {
                    setTimeout(tryApply, 200);
                }
            })();
        })();

        var toggle = document.getElementById("ai-chat-toggle");
        var panel = document.getElementById("ai-chat-panel");
        var hideBtn = document.getElementById("ai-chat-hide");
        var form = document.getElementById("ai-chat-form");
        var input = document.getElementById("ai-chat-input");
        var sendBtn = document.getElementById("ai-chat-send");
        var log = document.getElementById("ai-chat-log");
        var ctxEl = document.getElementById("ai-chat-context");
        if (!toggle || !panel || !form) return;

        // Sit the toggle just above the sidebar's account/logout footer (measured,
        // so it's robust to the footer's height). On mobile the sidebar is
        // off-canvas, so fall back to the CSS bottom.
        function positionToggle() {
            if (window.innerWidth <= 860) { toggle.style.bottom = ""; return; }
            var foot = document.querySelector(".app-sidebar__foot");
            // Clear the footer AND its top divider line with breathing room.
            toggle.style.bottom = foot
                ? (Math.round(foot.getBoundingClientRect().height) + 40) + "px"
                : "110px";
        }
        positionToggle();
        window.addEventListener("resize", positionToggle);

        // Current car context (car detail pages set body[data-car-id]).
        var carId = document.body ? document.body.getAttribute("data-car-id") : null;
        if (carId && ctxEl) {
            var title = (document.querySelector("h1") || {}).textContent || "";
            ctxEl.textContent = "This car" + (title ? ": " + title.trim().slice(0, 40) : "");
            ctxEl.hidden = false;
        }

        function setOpen(open) {
            panel.hidden = !open;
            toggle.classList.toggle("is-open", open);
            toggle.setAttribute("aria-expanded", open ? "true" : "false");
            try { localStorage.setItem(OPEN_KEY, open ? "1" : "0"); } catch (e) {}
            if (open && input) input.focus();
        }

        // Restore prior show/hide choice across navigation.
        var wasOpen = false;
        try { wasOpen = localStorage.getItem(OPEN_KEY) === "1"; } catch (e) {}
        setOpen(wasOpen);

        toggle.addEventListener("click", function () { setOpen(true); });
        if (hideBtn) hideBtn.addEventListener("click", function () { setOpen(false); });
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape" && !panel.hidden) setOpen(false);
        });

        // Conversation persists across page navigation (full-page loads).
        function saveHistory() {
            try {
                var msgs = [];
                log.querySelectorAll(".ai-chat-msg--user, .ai-chat-msg--bot").forEach(function (el) {
                    msgs.push({ t: el.textContent, u: el.classList.contains("ai-chat-msg--user") });
                });
                localStorage.setItem(HIST_KEY, JSON.stringify(msgs.slice(-HIST_MAX)));
            } catch (e) {}
        }

        function addMsg(text, cls, persist) {
            var d = document.createElement("div");
            d.className = "ai-chat-msg ai-chat-msg--" + cls;
            d.textContent = text;
            log.appendChild(d);
            log.scrollTop = log.scrollHeight;
            if (persist !== false && (cls === "user" || cls === "bot")) saveHistory();
            return d;
        }

        // Restore prior conversation (skip the seeded greeting if we have history).
        try {
            var hist = JSON.parse(localStorage.getItem(HIST_KEY) || "[]");
            if (hist.length) {
                log.innerHTML = "";
                hist.forEach(function (m) { addMsg(m.t, m.u ? "user" : "bot", false); });
            }
        } catch (e) {}

        function csrf() {
            var m = document.querySelector('meta[name="csrf-token"]');
            return m && m.content ? m.content : "";
        }

        var busy = false;
        form.addEventListener("submit", function (ev) {
            ev.preventDefault();
            var msg = (input.value || "").trim();
            if (!msg || busy) return;
            busy = true;
            sendBtn.disabled = true;
            input.value = "";
            addMsg(msg, "user");
            var pending = addMsg("Thinking…", "pending");

            fetch("/api/ai/chat", {
                method: "POST",
                credentials: "same-origin",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf(),
                },
                body: JSON.stringify({ message: msg, car_id: carId || null }),
            })
                .then(function (r) { return r.json().catch(function () { return { ok: false }; }); })
                .then(function (data) {
                    pending.remove();
                    if (data && data.ok && data.reply) {
                        addMsg(data.reply, "bot");
                        var s = data.search;
                        if (s && s.filters && typeof window.__DS_applySmartFilters === "function") {
                            // On the listings page: select the matching filter
                            // controls (and re-render results) as if typed in the bar.
                            try {
                                var bar = document.getElementById("smart-search-input");
                                if (bar && s.q) bar.value = s.q;
                                window.__DS_applySmartFilters(s.filters);
                            } catch (e) {}
                        } else if (s && typeof s.url === "string" && s.url.indexOf("/listings") === 0) {
                            // Elsewhere: link to listings; hand the filters off so the
                            // controls get selected on arrival too.
                            var d = document.createElement("div");
                            d.className = "ai-chat-msg ai-chat-msg--bot ai-chat-msg--action";
                            var a = document.createElement("a");
                            a.href = s.url;
                            a.className = "ai-chat-cta";
                            a.textContent = "View matching listings →";
                            a.addEventListener("click", function () {
                                try {
                                    sessionStorage.setItem("ds_assistant_search",
                                        JSON.stringify({ filters: s.filters, q: s.q }));
                                } catch (e) {}
                            });
                            d.appendChild(a);
                            log.appendChild(d);
                            log.scrollTop = log.scrollHeight;
                        }
                    } else {
                        var e = (data && data.error) || "unavailable";
                        addMsg(
                            e === "rate_limited" ? "You're sending messages too fast — give it a moment."
                            : e === "message_too_long" ? "That message is too long."
                            : "Sorry, the assistant is unavailable right now.",
                            "error"
                        );
                    }
                })
                .catch(function () {
                    pending.remove();
                    addMsg("Network error — please try again.", "error");
                })
                .finally(function () {
                    busy = false;
                    sendBtn.disabled = false;
                    if (input) input.focus();
                });
        });
    });
})();
