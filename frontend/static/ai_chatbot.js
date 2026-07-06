/* Sidebar AI assistant. Show/hide persists across page loads (localStorage), and
   when the current page is a car detail (body[data-car-id]) the assistant sends
   that id so answers are about the car in view. */
(function () {
    "use strict";
    var OPEN_KEY = "ds_ai_chat_open";

    function ready(fn) {
        if (document.readyState !== "loading") fn();
        else document.addEventListener("DOMContentLoaded", fn);
    }

    ready(function () {
        var toggle = document.getElementById("ai-chat-toggle");
        var panel = document.getElementById("ai-chat-panel");
        var hideBtn = document.getElementById("ai-chat-hide");
        var form = document.getElementById("ai-chat-form");
        var input = document.getElementById("ai-chat-input");
        var sendBtn = document.getElementById("ai-chat-send");
        var log = document.getElementById("ai-chat-log");
        var ctxEl = document.getElementById("ai-chat-context");
        if (!toggle || !panel || !form) return;

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

        function addMsg(text, cls) {
            var d = document.createElement("div");
            d.className = "ai-chat-msg ai-chat-msg--" + cls;
            d.textContent = text;
            log.appendChild(d);
            log.scrollTop = log.scrollHeight;
            return d;
        }

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
