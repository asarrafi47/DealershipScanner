import { animate } from "https://cdn.jsdelivr.net/npm/motion@latest/+esm";

const COLLAPSED_HEIGHT = "76px";
const EXPANDED_HEIGHT = "420px";
const CHAT_DURATION = 0.35;
const CHAT_EASING = [0.4, 0, 0.2, 1]; // cubic-bezier

document.addEventListener("DOMContentLoaded", () => {
    const container = document.getElementById("car-chatbot");
    if (!container) return;

    const vin = container.dataset.vin;
    const messagesEl = document.getElementById("car-chatbot-messages");
    const inputEl = document.getElementById("car-chatbot-input");
    const sendBtn = document.getElementById("car-chatbot-send");

    let isExpanded = false;

    function expand() {
        if (isExpanded) return;
        isExpanded = true;
        container.classList.add("expanded");
        animate(container, { height: EXPANDED_HEIGHT }, {
            duration: CHAT_DURATION,
            ease: CHAT_EASING,
        });
    }

    function collapse(e) {
        if (!container.contains(e.target)) {
            if (!isExpanded) return;
            isExpanded = false;
            container.classList.remove("expanded");
            animate(container, { height: COLLAPSED_HEIGHT }, {
                duration: CHAT_DURATION,
                ease: CHAT_EASING,
            });
        }
    }

    container.addEventListener("click", expand);
    inputEl.addEventListener("focus", expand);
    sendBtn.addEventListener("click", expand);
    document.addEventListener("click", collapse);

    function appendMessage(text, role) {
        const div = document.createElement("div");
        div.className = "car-chatbot-message " + role;
        div.textContent = text;
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function showLoading() {
        const div = document.createElement("div");
        div.className = "car-chatbot-message loading";
        div.id = "car-chatbot-loading";
        div.innerHTML = '<span class="car-chatbot-spinner"></span><span>Thinking...</span>';
        messagesEl.appendChild(div);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function hideLoading() {
        const el = document.getElementById("car-chatbot-loading");
        if (el) el.remove();
    }

    function setBusy(busy) {
        sendBtn.disabled = busy;
        inputEl.disabled = busy;
    }

    sendBtn.addEventListener("click", sendMessage);
    inputEl.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    function sendMessage() {
        const message = (inputEl.value || "").trim();
        if (!message) return;

        inputEl.value = "";
        appendMessage(message, "user");
        showLoading();
        setBusy(true);

        fetch("/api/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message, vin }),
        })
            .then((res) => res.json())
            .then((data) => {
                hideLoading();
                if (data.error && !data.reply) {
                    appendMessage("Sorry, the assistant is unavailable. " + (data.error || ""), "bot");
                } else {
                    appendMessage(data.reply || "No response.", "bot");
                }
            })
            .catch((err) => {
                hideLoading();
                appendMessage("Sorry, something went wrong. Try again.", "bot");
            })
            .finally(() => setBusy(false));
    }
});
