/**
 * Floating AI co-pilot (listings / dashboard). Car detail page uses car-chatbot.js + same API.
 * Theme: #1a1a1a + Carfax-style blue accents.
 */
(function () {
    const TOOLTIP =
        "AI detected a potential data discrepancy vs EPA/trim inference — verify with the dealer.";

    function applyDiscrepancyHighlights(flags) {
        if (!Array.isArray(flags) || !flags.length) return;
        document.querySelectorAll("[data-spec-field].ai-spec-warning").forEach((el) => {
            el.classList.remove("ai-spec-warning");
            el.removeAttribute("title");
        });
        const byField = {};
        flags.forEach((f) => {
            const k = f.spec_key || f.field;
            if (k) byField[k] = f.message || TOOLTIP;
        });
        const extra = {
            cylinders: ["cylinders", "engine"],
            drivetrain: ["drivetrain"],
        };
        Object.keys(byField).forEach((field) => {
            const keys = extra[field] || [field];
            keys.forEach((key) => {
                document.querySelectorAll(`[data-spec-field="${key}"]`).forEach((el) => {
                    el.classList.add("ai-spec-warning");
                    el.setAttribute("title", byField[field]);
                });
            });
        });
    }

    window.__DS_applyAiDiscrepancyFlags = applyDiscrepancyHighlights;

    function buildWidget() {
        const root = document.createElement("div");
        root.id = "ai-widget-root";
        root.className = "ai-widget";
        root.innerHTML = `
            <button type="button" class="ai-widget-fab" id="ai-widget-fab" aria-expanded="false" aria-controls="ai-widget-panel" title="Automotive co-pilot">
                <span class="ai-widget-fab-icon" aria-hidden="true">✦</span>
                <span class="ai-widget-fab-label">Co-Pilot</span>
            </button>
            <div class="ai-widget-panel" id="ai-widget-panel" hidden role="dialog" aria-label="AI co-pilot chat">
                <div class="ai-widget-header">
                    <span class="ai-widget-title">Automotive co-pilot</span>
                    <button type="button" class="ai-widget-close" id="ai-widget-close" aria-label="Close">×</button>
                </div>
                <p class="ai-widget-sub">Search assistant — ask about filters, makes, or how to read listings.</p>
                <div class="ai-widget-messages" id="ai-widget-messages"></div>
                <div class="ai-widget-input-row">
                    <input type="text" class="ai-widget-input" id="ai-widget-input" placeholder="Ask anything…" autocomplete="off" />
                    <button type="button" class="ai-widget-send" id="ai-widget-send">Send</button>
                </div>
            </div>`;
        document.body.appendChild(root);

        const fab = root.querySelector("#ai-widget-fab");
        const panel = root.querySelector("#ai-widget-panel");
        const close = root.querySelector("#ai-widget-close");
        const messagesEl = root.querySelector("#ai-widget-messages");
        const input = root.querySelector("#ai-widget-input");
        const send = root.querySelector("#ai-widget-send");

        function append(role, text) {
            const div = document.createElement("div");
            div.className = "ai-widget-msg " + role;
            div.textContent = text;
            messagesEl.appendChild(div);
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }

        function setOpen(open) {
            panel.hidden = !open;
            fab.setAttribute("aria-expanded", open ? "true" : "false");
        }

        fab.addEventListener("click", () => setOpen(panel.hidden));
        close.addEventListener("click", () => setOpen(false));

        function sendMessage() {
            const msg = (input.value || "").trim();
            if (!msg) return;
            input.value = "";
            append("user", msg);
            const busy = document.createElement("div");
            busy.className = "ai-widget-msg bot ai-widget-loading";
            busy.textContent = "Thinking…";
            messagesEl.appendChild(busy);

            const vin = (document.body.dataset.aiVin || "").trim();
            const page = (document.body.dataset.aiPage || "listings").trim();

            fetch("/api/ai/chat", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    user_message: msg,
                    current_vin: vin || undefined,
                    page: page || "listings",
                }),
            })
                .then((r) => r.json())
                .then((data) => {
                    busy.remove();
                    if (data.error && !data.reply) {
                        append("bot", data.reply || data.error || "Error.");
                    } else {
                        append("bot", data.reply || "No response.");
                    }
                    applyDiscrepancyHighlights(data.discrepancy_flags || []);
                })
                .catch(() => {
                    busy.remove();
                    append("bot", "Request failed. Try again.");
                });
        }

        send.addEventListener("click", sendMessage);
        input.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
                e.preventDefault();
                sendMessage();
            }
        });
    }

    if (document.body.dataset.aiPage === "car") return;

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", buildWidget);
    } else {
        buildWidget();
    }
})();
