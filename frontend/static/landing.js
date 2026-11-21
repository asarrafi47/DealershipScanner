/* Landing page behaviour (moved out of landing.html's inline script, perf 2026-11-20):
 * the landing is cached at the edge, so it carries no inline script and no CSP nonce.
 * Loaded with defer, so the DOM is parsed when this runs. */
(function () {
    function sanitizeZipInput(input) {
        if (!input) return;
        var v = (input.value || "").trim();
        if (v && !/^\d{5}$/.test(v)) input.value = "";
    }

    document.querySelectorAll("form.lps-search, form.nav-search").forEach(function (form) {
        form.addEventListener("submit", function (e) {
            sanitizeZipInput(form.querySelector('input[name="zip_code"]'));
            if (form.classList.contains("nav-search")) {
                var q = form.querySelector('input[name="q"]');
                if (q && !(q.value || "").trim()) e.preventDefault();
            }
        });
    });

    var reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var targets = document.querySelectorAll(".lps-reveal");
    if (reduced || !("IntersectionObserver" in window)) {
        targets.forEach(function (el) { el.classList.add("lps-in"); });
        return;
    }
    var io = new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
            if (e.isIntersecting) { e.target.classList.add("lps-in"); io.unobserve(e.target); }
        });
    }, { threshold: 0.14 });
    targets.forEach(function (el) { io.observe(el); });
})();
