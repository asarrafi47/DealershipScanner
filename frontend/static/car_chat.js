/**
 * Car detail page chat: POST /api/car/<id>/chat
 */
(function () {
    const root = document.getElementById("car-chat-section");
    if (!root) return;
    const carId = root.getAttribute("data-car-id");
    const messagesEl = document.getElementById("car-chat-messages");
    const input = document.getElementById("car-chat-input");
    const sendBtn = document.getElementById("car-chat-send");
    if (!carId || !messagesEl || !input || !sendBtn) return;

    function appendBubble(text, role) {
        const div = document.createElement("div");
        div.className = "car-chat-bubble car-chat-bubble--" + role;
        div.textContent = text;
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function send() {
        const text = (input.value || "").trim();
        if (!text) return;
        appendBubble(text, "user");
        input.value = "";
        sendBtn.disabled = true;
        fetch("/api/car/" + encodeURIComponent(carId) + "/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: text }),
        })
            .then(function (r) {
                var status = r.status;
                return r
                    .json()
                    .then(function (data) {
                        return { ok: r.ok, status: status, data: data || {} };
                    })
                    .catch(function () {
                        return { ok: r.ok, status: status, data: {} };
                    });
            })
            .then(function (wrapped) {
                var data = wrapped.data || {};
                if (!wrapped.ok) {
                    appendBubble(
                        data.error || "Request failed (" + (wrapped.status || "?") + ").",
                        "error"
                    );
                    return;
                }
                var reply = data.reply || "";
                var err = data.error;
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
