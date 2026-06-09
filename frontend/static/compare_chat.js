/**
 * Compare page chat: POST /api/compare/chat
 */
(function () {
    "use strict";

    function readCsrfToken() {
        const m = document.querySelector('meta[name="csrf-token"]');
        return m ? (m.getAttribute("content") || "").trim() : "";
    }

    const root = document.getElementById("compare-chat-section");
    if (!root) return;

    const messagesEl = document.getElementById("compare-chat-messages");
    const input = document.getElementById("compare-chat-input");
    const sendBtn = document.getElementById("compare-chat-send");
    if (!messagesEl || !input || !sendBtn) return;

    function readCarIds() {
        const fromBody = (document.body.getAttribute("data-compare-ids") || "").trim();
        if (fromBody) {
            return fromBody.split(",").map((x) => parseInt(x, 10)).filter((n) => Number.isFinite(n) && n > 0);
        }
        if (typeof window.__DS_compareReadIds === "function") {
            return window.__DS_compareReadIds();
        }
        return [];
    }

    function plainChatText(raw) {
        return String(raw || "")
            .replace(/^#{1,6}\s+/gm, "")
            .replace(/\*\*(.+?)\*\*/g, "$1")
            .replace(/\*(.+?)\*/g, "$1")
            .replace(/^[-*]\s+/gm, "• ")
            .replace(/\n{3,}/g, "\n\n")
            .trim();
    }

    function appendBubble(text, role) {
        const div = document.createElement("div");
        div.className = "car-chat-bubble car-chat-bubble--" + role;
        const body = role === "assistant" ? plainChatText(text) : text;
        div.textContent = body;
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function send() {
        const text = (input.value || "").trim();
        if (!text) return;
        const carIds = readCarIds();
        if (!carIds.length) {
            appendBubble("Add at least one vehicle to compare first.", "error");
            return;
        }

        appendBubble(text, "user");
        input.value = "";
        sendBtn.disabled = true;

        const headers = { "Content-Type": "application/json" };
        const t = readCsrfToken();
        if (t) headers["X-CSRF-Token"] = t;

        fetch("/api/compare/chat", {
            method: "POST",
            headers,
            credentials: "same-origin",
            body: JSON.stringify({ message: text, car_ids: carIds }),
        })
            .then(function (r) {
                const status = r.status;
                return r.json().then(function (data) {
                    return { ok: r.ok, status: status, data: data || {} };
                }).catch(function () {
                    return { ok: r.ok, status: status, data: {} };
                });
            })
            .then(function (wrapped) {
                const data = wrapped.data || {};
                if (!wrapped.ok) {
                    if (data.error === "premium_required") {
                        appendBubble("Premium subscription required. Open /premium to unlock compare chat.", "error");
                    } else if (data.error === "login_required") {
                        appendBubble("Sign in to use compare chat.", "error");
                    } else if (data.error === "user_chat_limit_reached") {
                        appendBubble("Daily chat limit reached. Try again tomorrow.", "error");
                    } else {
                        appendBubble(data.error || "Request failed (" + (wrapped.status || "?") + ").", "error");
                    }
                    return;
                }
                const reply = data.reply || "";
                const err = data.error;
                if (reply) appendBubble(reply, "assistant");
                else if (err) appendBubble("Error: " + err, "error");
                else appendBubble("No response.", "error");
            })
            .catch(function () {
                appendBubble("Network error. Is the app running?", "error");
            })
            .finally(function () {
                sendBtn.disabled = false;
                input.focus();
            });
    }

    sendBtn.addEventListener("click", send);
    input.addEventListener("keydown", function (e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    });
})();
