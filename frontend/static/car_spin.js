/**
 * 360 spin + interior pano viewers on car detail.
 * Dependency-free drag/swipe spin player; interior pano uses vendored
 * Pannellum (frontend/static/vendor/pannellum/) when present.
 * Loaded only from car.html (no inline script for CSP). Bails silently
 * when the mounts/data are absent so the plain gallery is unaffected.
 */
(function () {
    "use strict";

    var MODE_PHOTOS = "photos";
    var MODE_SPIN = "spin";
    var MODE_PANO = "pano";
    var PRELOAD_CONCURRENCY = 4;

    var switchEl = document.getElementById("car-media-switch");
    var spinEl = document.getElementById("car-spin-viewer");
    var panoEl = document.getElementById("car-pano-viewer");
    var heroEl = document.getElementById("car-gallery-hero");
    if (!switchEl || !heroEl || (!spinEl && !panoEl)) return;

    // --- Spin frame data -------------------------------------------------
    var spinFrames = [];
    var spinJsonEl = document.getElementById("car-spin-json");
    if (spinJsonEl && spinJsonEl.textContent) {
        try {
            spinFrames = JSON.parse(spinJsonEl.textContent);
        } catch (_e) {
            spinFrames = [];
        }
    }
    if (!Array.isArray(spinFrames)) spinFrames = [];
    spinFrames = spinFrames.filter(function (u) {
        return typeof u === "string" && /^https?:\/\//i.test(u);
    });

    var panoUrl = panoEl ? panoEl.getAttribute("data-pano-url") || "" : "";
    if (!/^https?:\/\//i.test(panoUrl)) panoUrl = "";

    var hasSpin = !!(spinEl && spinFrames.length >= 2);
    var hasPano = !!(panoEl && panoUrl);

    function hideModeButton(mode) {
        var btn = switchEl.querySelector('[data-media-mode="' + mode + '"]');
        if (btn) btn.hidden = true;
    }

    if (!hasSpin && spinEl) {
        spinEl.hidden = true;
        hideModeButton(MODE_SPIN);
    }
    if (!hasPano && panoEl) {
        panoEl.hidden = true;
        hideModeButton(MODE_PANO);
    }
    if (!hasSpin && !hasPano) {
        switchEl.hidden = true;
        return;
    }

    // --- Mode switching ---------------------------------------------------
    // car_page.js checks window.__DS_MEDIA_MODE before handling gallery
    // clicks / arrow keys so the viewers do not fight the photo gallery.
    var currentMode = MODE_PHOTOS;
    window.__DS_MEDIA_MODE = currentMode;

    function setMode(mode) {
        if (mode === MODE_SPIN && !hasSpin) mode = MODE_PHOTOS;
        if (mode === MODE_PANO && !hasPano) mode = MODE_PHOTOS;
        currentMode = mode;
        window.__DS_MEDIA_MODE = mode;
        if (spinEl) spinEl.hidden = mode !== MODE_SPIN;
        if (panoEl) panoEl.hidden = mode !== MODE_PANO;
        heroEl.classList.toggle("car-gallery-hero--alt-media", mode !== MODE_PHOTOS);
        switchEl.querySelectorAll("[data-media-mode]").forEach(function (btn) {
            var active = btn.getAttribute("data-media-mode") === mode;
            btn.classList.toggle("active", active);
            btn.setAttribute("aria-selected", active ? "true" : "false");
        });
        if (mode === MODE_SPIN) ensureSpin();
        if (mode === MODE_PANO) ensurePano();
    }

    switchEl.querySelectorAll("[data-media-mode]").forEach(function (btn) {
        btn.addEventListener("click", function () {
            setMode(btn.getAttribute("data-media-mode"));
        });
    });

    // --- Spin player -------------------------------------------------------
    var spinStarted = false;

    function ensureSpin() {
        if (spinStarted || !hasSpin) return;
        spinStarted = true;
        initSpinPlayer(spinEl, spinFrames);
    }

    function initSpinPlayer(root, frames) {
        var frameCount = frames.length;
        var loadedOk = new Array(frameCount).fill(false);
        var settled = 0;
        var firstFrameFailed = false;

        var img = document.createElement("img");
        img.className = "car-spin-frame";
        img.alt = "360 degree exterior view";
        img.draggable = false;
        root.appendChild(img);

        var badge = document.createElement("div");
        badge.className = "car-spin-affordance";
        badge.setAttribute("aria-hidden", "true");
        badge.innerHTML =
            '<span class="car-spin-affordance-arrows">&#8634;</span>' +
            '<span class="car-spin-affordance-label">360&deg; &mdash; drag to spin</span>';
        root.appendChild(badge);

        var progress = document.createElement("div");
        progress.className = "car-spin-progress";
        progress.setAttribute("role", "progressbar");
        progress.setAttribute("aria-valuemin", "0");
        progress.setAttribute("aria-valuemax", "100");
        var progressBar = document.createElement("div");
        progressBar.className = "car-spin-progress-bar";
        progress.appendChild(progressBar);
        root.appendChild(progress);

        root.setAttribute("tabindex", "0");
        root.setAttribute("role", "img");
        root.setAttribute(
            "aria-label",
            "Interactive 360 degree view. Drag or use arrow keys to rotate."
        );

        var frameIndex = 0;

        function wrap(i) {
            return ((i % frameCount) + frameCount) % frameCount;
        }

        function nearestLoaded(i) {
            if (loadedOk[i]) return i;
            for (var d = 1; d <= frameCount; d++) {
                if (loadedOk[wrap(i - d)]) return wrap(i - d);
                if (loadedOk[wrap(i + d)]) return wrap(i + d);
            }
            return i;
        }

        function showFrame(i) {
            frameIndex = wrap(i);
            var src = frames[nearestLoaded(frameIndex)];
            if (img.getAttribute("src") !== src) img.setAttribute("src", src);
        }

        // Preload with a small concurrency cap; frame 0 first so the viewer
        // paints immediately, then the rest in order.
        function onSettled() {
            settled += 1;
            var pct = Math.round((settled / frameCount) * 100);
            progressBar.style.width = pct + "%";
            progress.setAttribute("aria-valuenow", String(pct));
            if (settled >= frameCount) {
                progress.classList.add("car-spin-progress--done");
                window.setTimeout(function () {
                    if (progress.parentNode) progress.parentNode.removeChild(progress);
                }, 400);
            }
        }

        var nextToLoad = 0;
        function pump() {
            while (nextToLoad < frameCount) {
                var i = nextToLoad;
                nextToLoad += 1;
                loadFrame(i);
                if (inFlight >= PRELOAD_CONCURRENCY) break;
            }
        }
        var inFlight = 0;
        function loadFrame(i) {
            inFlight += 1;
            var pre = new Image();
            pre.onload = function () {
                inFlight -= 1;
                loadedOk[i] = true;
                onSettled();
                if (i === frameIndex || i === nearestLoaded(frameIndex)) showFrame(frameIndex);
                pump();
            };
            pre.onerror = function () {
                inFlight -= 1;
                onSettled();
                if (i === 0) firstFrameFailed = true;
                // Only give up once every frame settled and none loaded.
                if (settled >= frameCount && !loadedOk.some(Boolean)) degrade();
                pump();
            };
            pre.src = frames[i];
        }

        var degraded = false;
        function degrade() {
            // Nothing usable: fall back to the photo gallery and hide the pill.
            if (degraded) return;
            degraded = true;
            hasSpin = false;
            hideModeButton(MODE_SPIN);
            if (!hasPano) switchEl.hidden = true;
            setMode(MODE_PHOTOS);
        }

        img.addEventListener("error", function () {
            // Current display frame failed after the fact; step to a good one.
            if (loadedOk.some(Boolean)) showFrame(frameIndex + 1);
            else if (firstFrameFailed) degrade();
        });

        showFrame(0);
        pump();

        // --- Drag / swipe -> frame index ---------------------------------
        var dragging = false;
        var dragStartX = 0;
        var dragStartFrame = 0;
        var lastMoveX = 0;
        var lastMoveT = 0;
        var velocity = 0; // frames per ms
        var momentumRaf = 0;
        var interacted = false;

        function pxPerFrame() {
            var w = root.clientWidth || 600;
            // One full drag across the viewer = one full rotation.
            return Math.max(2, w / frameCount);
        }

        function markInteracted() {
            if (interacted) return;
            interacted = true;
            badge.classList.add("car-spin-affordance--fade");
        }

        root.addEventListener("pointerdown", function (e) {
            if (e.button !== undefined && e.button !== 0) return;
            dragging = true;
            dragStartX = e.clientX;
            dragStartFrame = frameIndex;
            lastMoveX = e.clientX;
            lastMoveT = e.timeStamp;
            velocity = 0;
            if (momentumRaf) {
                window.cancelAnimationFrame(momentumRaf);
                momentumRaf = 0;
            }
            markInteracted();
            root.classList.add("car-spin-viewer--dragging");
            if (root.setPointerCapture) {
                try {
                    root.setPointerCapture(e.pointerId);
                } catch (_err) {
                    /* ignore */
                }
            }
            e.preventDefault();
        });

        root.addEventListener("pointermove", function (e) {
            if (!dragging) return;
            var dx = e.clientX - dragStartX;
            // Drag right -> rotate forward through the sequence.
            showFrame(dragStartFrame + Math.round(dx / pxPerFrame()));
            var dt = e.timeStamp - lastMoveT;
            if (dt > 0) {
                velocity = (e.clientX - lastMoveX) / pxPerFrame() / dt;
                lastMoveX = e.clientX;
                lastMoveT = e.timeStamp;
            }
            e.preventDefault();
        });

        function endDrag() {
            if (!dragging) return;
            dragging = false;
            root.classList.remove("car-spin-viewer--dragging");
            startMomentum();
        }
        root.addEventListener("pointerup", endDrag);
        root.addEventListener("pointercancel", endDrag);

        // Light momentum: keep spinning briefly after a fast flick.
        function startMomentum() {
            var v = velocity;
            if (Math.abs(v) < 0.004) return;
            v = Math.max(-0.06, Math.min(0.06, v));
            var carried = 0;
            var lastT = 0;
            function step(t) {
                if (!lastT) lastT = t;
                var dt = t - lastT;
                lastT = t;
                carried += v * dt;
                var whole = carried > 0 ? Math.floor(carried) : Math.ceil(carried);
                if (whole !== 0) {
                    showFrame(frameIndex + whole);
                    carried -= whole;
                }
                v *= Math.pow(0.994, dt);
                if (Math.abs(v) >= 0.0015 && currentMode === MODE_SPIN) {
                    momentumRaf = window.requestAnimationFrame(step);
                } else {
                    momentumRaf = 0;
                }
            }
            momentumRaf = window.requestAnimationFrame(step);
        }

        // Keyboard rotation while spin mode is active.
        document.addEventListener("keydown", function (e) {
            if (currentMode !== MODE_SPIN) return;
            if (e.target.matches("input, textarea, select")) return;
            if (e.key === "ArrowLeft") {
                showFrame(frameIndex - 1);
                markInteracted();
                e.preventDefault();
            } else if (e.key === "ArrowRight") {
                showFrame(frameIndex + 1);
                markInteracted();
                e.preventDefault();
            }
        });

        // Swallow clicks so car_page.js hero click-zones never see them.
        root.addEventListener("click", function (e) {
            e.stopPropagation();
        });
    }

    // --- Interior pano (Pannellum, lazily initialised) ---------------------
    var panoStarted = false;

    function ensurePano() {
        if (panoStarted || !hasPano) return;
        panoStarted = true;

        if (typeof window.pannellum === "undefined") {
            panoFailed();
            return;
        }
        panoEl.addEventListener("click", function (e) {
            e.stopPropagation();
        });
        try {
            var viewer = window.pannellum.viewer(panoEl, {
                type: "equirectangular",
                panorama: panoUrl,
                autoLoad: true,
                autoRotate: -2,
                compass: false,
                showFullscreenCtrl: true,
                crossOrigin: "anonymous",
            });
            viewer.on("error", function () {
                panoFailed();
            });
        } catch (_e) {
            panoFailed();
        }
    }

    function panoFailed() {
        // Pano could not render (missing vendor lib, CORS-blocked texture,
        // bad image): drop back to photos and hide the Interior pill.
        hasPano = false;
        hideModeButton(MODE_PANO);
        if (panoEl) panoEl.hidden = true;
        if (!hasSpin) switchEl.hidden = true;
        setMode(MODE_PHOTOS);
    }
})();
