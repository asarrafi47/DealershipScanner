/* Phone sign-in pages (card S8): /login/phone and /login/phone/code.
   Progressive enhancement only: the server enforces consent, the wait between codes and the
   Turnstile check, so every page works without this file. No inline script (CSP: 'self' + nonce). */
(function () {
    "use strict";

    function byId(id) { return document.getElementById(id); }

    // ---- number page ----------------------------------------------------------------------
    var startForm = byId("ph-start-form");
    if (startForm) {
        var box = byId("ph-consent");
        var send = byId("ph-send");
        var hint = byId("ph-consent-hint");
        var idleLabel = send ? send.textContent : "";

        // The button is aria-disabled (not disabled) until the box is ticked, so it stays focusable
        // and a screen reader reads the reason ("Tick the box to continue") beside it.
        var syncConsent = function () {
            if (!box || !send) return;
            send.setAttribute("aria-disabled", box.checked ? "false" : "true");
            if (hint) {
                hint.hidden = box.checked;
                // A hidden hint still counts as a description, so drop the reference with the text.
                if (box.checked) send.removeAttribute("aria-describedby");
                else send.setAttribute("aria-describedby", hint.id);
            }
        };
        if (box) box.addEventListener("change", syncConsent);
        syncConsent();

        startForm.addEventListener("submit", function (event) {
            if (send && send.getAttribute("aria-disabled") === "true") {
                event.preventDefault();
                if (box && !box.checked) box.focus();
                return;
            }
            if (send) {
                send.setAttribute("aria-disabled", "true");
                send.textContent = "Sending…";
            }
        });

        // Back after a send restores this page from the back-forward cache mid-submit: reset it.
        window.addEventListener("pageshow", function () {
            if (send) send.textContent = idleLabel;
            syncConsent();
        });
    }

    // ---- the person-check did not load (both pages) -----------------------------------------
    // Cloudflare's script defines window.turnstile once it has loaded; the widget adds an iframe and a
    // hidden response field. Any of the three means the check is there. The notice shows after 8 seconds
    // without one and goes away if the script arrives late.
    var missing = byId("ph-check-missing");
    if (missing && document.querySelector(".cf-turnstile")) {
        var waited = 0;
        var checkLoaded = function () {
            return !!(window.turnstile ||
                document.querySelector(".cf-turnstile iframe") ||
                document.querySelector('input[name="cf-turnstile-response"]'));
        };
        var watch = window.setInterval(function () {
            waited += 1;
            if (checkLoaded()) {
                missing.hidden = true;
                window.clearInterval(watch);
            } else if (waited >= 8) {
                missing.hidden = false;
                if (waited >= 120) window.clearInterval(watch);
            }
        }, 1000);
    }

    // ---- code page ------------------------------------------------------------------------
    var code = byId("ph-code");
    if (code) {
        code.focus();
        var verifyForm = byId("ph-code-form");
        var verify = byId("ph-verify");
        var verifyLabel = verify ? verify.textContent : "";
        if (verifyForm && verify) {
            verifyForm.addEventListener("submit", function (event) {
                if (verify.getAttribute("aria-disabled") === "true") {
                    event.preventDefault();
                    return;
                }
                verify.setAttribute("aria-disabled", "true");
                verify.textContent = "Signing in…";
            });
            window.addEventListener("pageshow", function () {
                verify.setAttribute("aria-disabled", "false");
                verify.textContent = verifyLabel;
            });
        }
    }

    var resend = byId("ph-resend");
    if (resend) {
        var visible = byId("ph-wait");
        var live = byId("ph-wait-live");
        var left = parseInt(resend.getAttribute("data-wait") || "0", 10);
        if (isNaN(left) || left < 0) left = 0;

        var say = function (text) { if (live) live.textContent = text; };
        var paint = function () {
            if (!visible) return;
            visible.textContent = left > 0
                ? "You can ask for another code in " + left + " seconds."
                : "You can ask for another code now.";
        };
        var ready = function () {
            resend.setAttribute("aria-disabled", "false");
            paint();
            say("You can ask for another code now.");
        };

        resend.addEventListener("click", function (event) {
            if (resend.getAttribute("aria-disabled") === "true") event.preventDefault();
        });

        if (left > 0) {
            // Announce once at the start and once when ready; the visible count ticks silently.
            // The count comes from a deadline, not from counting ticks: a phone that is locked or in
            // another app (reading the text) stops the timer, and the wait must still end on time.
            say("You can ask for another code in " + left + " seconds.");
            var deadline = Date.now() + left * 1000;
            var timer = 0;
            var tick = function () {
                left = Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
                if (left <= 0) {
                    window.clearInterval(timer);
                    ready();
                } else {
                    paint();
                }
            };
            timer = window.setInterval(tick, 1000);
            document.addEventListener("visibilitychange", function () { if (!document.hidden) tick(); });
            window.addEventListener("pageshow", tick);
        }
    }
})();
