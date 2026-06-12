/**
 * Show/hide password toggles for .password-input-wrap fields.
 */
(function () {
    "use strict";

    function initWrap(wrap) {
        const input = wrap.querySelector('input[type="password"], input[type="text"][data-password-input]');
        const btn = wrap.querySelector(".password-toggle");
        if (!input || !btn) return;

        function setVisible(visible) {
            input.type = visible ? "text" : "password";
            if (!input.hasAttribute("data-password-input") && visible) {
                input.setAttribute("data-password-input", "1");
            }
            btn.setAttribute("aria-pressed", visible ? "true" : "false");
            btn.setAttribute("aria-label", visible ? "Hide password" : "Show password");
            btn.classList.toggle("password-toggle--revealed", visible);
        }

        btn.addEventListener("click", function () {
            setVisible(input.type === "password");
        });
    }

    document.querySelectorAll(".password-input-wrap").forEach(initWrap);
})();
