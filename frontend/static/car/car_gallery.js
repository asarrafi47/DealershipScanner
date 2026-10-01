/**
 * Car page photo gallery: hero image with prev/next, thumbnail strip, keyboard
 * arrows, and a full-viewport lightbox with wheel / pinch zoom, drag pan and
 * swipe navigation.
 *
 * Moved out of car_page.js (2026-10-01 split; it was one 346-line function).
 * CP.filterGalleryUrls is the pure URL filter; window.CarPage.initCarGallery
 * wires the DOM. car_spin.js sets window.__DS_MEDIA_MODE while the 360 spin /
 * interior pano viewers are active; the photo gallery yields then.
 */
window.CP = window.CP || {};
window.CarPage = window.CarPage || {};

(function (CP) {
    "use strict";

    const GALLERY_JUNK_FRAGMENTS = [
        "transferbadge",
        "directions-icon",
        "photoswipe",
        "default-skin",
        "gubagoo",
        "pureinfluencer",
        "idrove.it",
        "/customwork/",
        "coming soon",
    ];

    /** Badges, widget chrome, "coming soon" placeholders and non-http URLs. */
    function isGalleryJunkUrl(u) {
        const sl = String(u || "").toLowerCase();
        if (!sl.startsWith("http")) return true;
        return GALLERY_JUNK_FRAGMENTS.some(function (frag) {
            return sl.indexOf(frag) >= 0;
        });
    }

    /**
     * The photos to page through: the gallery JSON minus junk; when nothing is
     * left, the hero's own src (if it is not junk itself); else [].
     */
    function filterGalleryUrls(raw, fallbackSrc) {
        let gallery = Array.isArray(raw) ? raw : [];
        gallery = gallery.filter(function (u) {
            return u && typeof u === "string" && !isGalleryJunkUrl(u);
        });
        if (gallery.length === 0) {
            const fallback = fallbackSrc || "";
            if (fallback && !isGalleryJunkUrl(fallback)) gallery = [fallback];
        }
        return gallery;
    }

    CP.isGalleryJunkUrl = isGalleryJunkUrl;
    CP.filterGalleryUrls = filterGalleryUrls;
})(window.CP);

(function (CarPage, CP) {
    "use strict";

    function getGalleryElements() {
        return {
            jsonEl: document.getElementById("car-gallery-json"),
            imgEl: document.getElementById("car-gallery-main-img"),
            heroEl: document.getElementById("car-gallery-hero"),
            prevBtn: document.getElementById("car-gallery-prev"),
            nextBtn: document.getElementById("car-gallery-next"),
            prevHero: document.getElementById("car-gallery-prev-hero"),
            nextHero: document.getElementById("car-gallery-next-hero"),
            counterEl: document.getElementById("car-gallery-counter"),
            counterBadge: document.getElementById("car-gallery-counter-badge"),
            thumbsEl: document.getElementById("car-gallery-thumbs"),
        };
    }

    function readGalleryUrls(els) {
        let raw = [];
        try {
            if (els.jsonEl && els.jsonEl.textContent) raw = JSON.parse(els.jsonEl.textContent);
        } catch (e) {
            raw = [];
        }
        return CP.filterGalleryUrls(raw, els.imgEl.getAttribute("src"));
    }

    /**
     * Nothing real to show even after filtering out junk/placeholder URLs --
     * swap in a clear empty state instead of leaving the broken/junk src on
     * screen with no explanation.
     */
    function showEmptyGallery(els) {
        const emptyMsg = document.getElementById("car-gallery-empty-msg");
        const placeholderSrc = els.heroEl ? els.heroEl.getAttribute("data-placeholder-src") : "";
        if (placeholderSrc) els.imgEl.src = placeholderSrc;
        els.imgEl.setAttribute("alt", "No photos available yet");
        if (emptyMsg) emptyMsg.hidden = false;
        if (els.counterBadge) els.counterBadge.remove();
        if (els.prevHero) els.prevHero.remove();
        if (els.nextHero) els.nextHero.remove();
        const controls = document.querySelector(".car-gallery-controls");
        if (controls) controls.remove();
        const thumbsWrap = document.querySelector(".car-gallery-thumbs-wrap");
        if (thumbsWrap) thumbsWrap.remove();
    }

    function altMediaActive() {
        return !!(window.__DS_MEDIA_MODE && window.__DS_MEDIA_MODE !== "photos");
    }

    const LIGHTBOX_HTML =
        '<button type="button" class="car-lightbox-close" aria-label="Close photo viewer">' +
        '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round"><path d="M18 6L6 18M6 6l12 12"/></svg>' +
        "</button>" +
        '<button type="button" class="car-lightbox-nav car-lightbox-prev" aria-label="Previous image">' +
        '<svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M15 6l-6 6 6 6"/></svg>' +
        "</button>" +
        '<div class="car-lightbox-stage">' +
        '<img class="car-lightbox-img" alt="">' +
        "</div>" +
        '<button type="button" class="car-lightbox-nav car-lightbox-next" aria-label="Next image">' +
        '<svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"/></svg>' +
        "</button>" +
        '<span class="car-lightbox-counter"></span>' +
        '<p class="car-lightbox-hint">Scroll or pinch to zoom · drag to pan</p>';

    /**
     * The lightbox, built lazily on first open. nav = {current() -> {url, alt,
     * index, length}, prev(), next()}. Returns {open, close, sync, isOpen}.
     */
    function createGalleryLightbox(nav) {
        let lightboxEl = null;
        let lightboxImgEl = null;
        let lightboxCounterEl = null;
        let lightboxOpen = false;
        let lastFocusedEl = null;
        let lbScale = 1;
        let lbTx = 0;
        let lbTy = 0;

        function applyLightboxTransform() {
            if (lightboxImgEl) {
                lightboxImgEl.style.transform =
                    "translate(" + lbTx + "px, " + lbTy + "px) scale(" + lbScale + ")";
                lightboxImgEl.classList.toggle("car-lightbox-img--zoomed", lbScale > 1);
            }
        }

        function setLightboxZoom(scale) {
            const clamped = Math.max(1, Math.min(5, scale));
            lbScale = clamped;
            if (clamped <= 1) {
                lbTx = 0;
                lbTy = 0;
            }
            applyLightboxTransform();
        }

        function resetLightboxZoom() {
            lbScale = 1;
            lbTx = 0;
            lbTy = 0;
            applyLightboxTransform();
        }

        function syncLightbox() {
            if (!lightboxOpen || !lightboxImgEl) return;
            resetLightboxZoom();
            const cur = nav.current();
            lightboxImgEl.src = cur.url;
            lightboxImgEl.alt = cur.alt;
            if (lightboxCounterEl) {
                lightboxCounterEl.textContent = cur.length > 1 ? cur.index + 1 + " / " + cur.length : "";
            }
        }

        function closeLightbox() {
            if (!lightboxOpen || !lightboxEl) return;
            lightboxOpen = false;
            lightboxEl.hidden = true;
            document.body.classList.remove("car-lightbox-open");
            if (lastFocusedEl && typeof lastFocusedEl.focus === "function") {
                lastFocusedEl.focus();
            }
        }

        function openLightbox() {
            if (altMediaActive()) return;
            buildLightbox();
            lastFocusedEl = document.activeElement;
            lightboxOpen = true;
            lightboxEl.hidden = false;
            document.body.classList.add("car-lightbox-open");
            syncLightbox();
            const closeBtn = lightboxEl.querySelector(".car-lightbox-close");
            if (closeBtn) closeBtn.focus();
        }

        // Unified pointer handling: one finger pans when zoomed (or is a
        // swipe-to-navigate when not), two fingers pinch-zoom, and a mouse
        // drag pans the same way a touch drag does.
        function bindLightboxPointer() {
            const pointers = new Map();
            let pinchStartDist = null;
            let pinchStartScale = 1;
            let panStart = null;
            let swipeStart = null;

            function pointerDist() {
                const pts = Array.from(pointers.values());
                return Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y);
            }

            lightboxImgEl.addEventListener("pointerdown", function (e) {
                lightboxImgEl.setPointerCapture(e.pointerId);
                pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
                if (pointers.size === 2) {
                    pinchStartDist = pointerDist();
                    pinchStartScale = lbScale;
                    panStart = null;
                    swipeStart = null;
                } else if (pointers.size === 1) {
                    panStart = { x: e.clientX, y: e.clientY, tx: lbTx, ty: lbTy };
                    swipeStart = { x: e.clientX, y: e.clientY };
                }
            });

            lightboxImgEl.addEventListener("pointermove", function (e) {
                if (!pointers.has(e.pointerId)) return;
                pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
                if (pointers.size === 2 && pinchStartDist) {
                    setLightboxZoom(pinchStartScale * (pointerDist() / pinchStartDist));
                } else if (pointers.size === 1 && panStart && lbScale > 1) {
                    lbTx = panStart.tx + (e.clientX - panStart.x);
                    lbTy = panStart.ty + (e.clientY - panStart.y);
                    applyLightboxTransform();
                }
            });

            function onPointerUp(e) {
                if (pointers.size === 1 && swipeStart && lbScale <= 1) {
                    const dx = e.clientX - swipeStart.x;
                    const dy = e.clientY - swipeStart.y;
                    if (Math.abs(dx) > 60 && Math.abs(dx) > Math.abs(dy) * 1.5) {
                        if (dx > 0) nav.prev();
                        else nav.next();
                    }
                }
                pointers.delete(e.pointerId);
                if (pointers.size < 2) pinchStartDist = null;
                if (pointers.size === 0) {
                    panStart = null;
                    swipeStart = null;
                }
            }
            lightboxImgEl.addEventListener("pointerup", onPointerUp);
            lightboxImgEl.addEventListener("pointercancel", onPointerUp);
        }

        function buildLightbox() {
            if (lightboxEl) return;
            lightboxEl = document.createElement("div");
            lightboxEl.className = "car-lightbox";
            lightboxEl.id = "car-gallery-lightbox";
            lightboxEl.hidden = true;
            lightboxEl.setAttribute("role", "dialog");
            lightboxEl.setAttribute("aria-modal", "true");
            lightboxEl.setAttribute("aria-label", "Photo viewer");
            lightboxEl.innerHTML = LIGHTBOX_HTML;
            document.body.appendChild(lightboxEl);
            lightboxImgEl = lightboxEl.querySelector(".car-lightbox-img");
            lightboxCounterEl = lightboxEl.querySelector(".car-lightbox-counter");

            lightboxEl.querySelector(".car-lightbox-close").addEventListener("click", closeLightbox);
            lightboxEl.querySelector(".car-lightbox-prev").addEventListener("click", function (e) {
                e.stopPropagation();
                nav.prev();
            });
            lightboxEl.querySelector(".car-lightbox-next").addEventListener("click", function (e) {
                e.stopPropagation();
                nav.next();
            });
            // Click outside the photo itself (the backdrop / stage) closes.
            lightboxEl.addEventListener("click", function (e) {
                if (e.target === lightboxEl || e.target.classList.contains("car-lightbox-stage")) {
                    closeLightbox();
                }
            });

            // Scroll-wheel zoom (desktop).
            lightboxImgEl.addEventListener(
                "wheel",
                function (e) {
                    e.preventDefault();
                    setLightboxZoom(lbScale + (e.deltaY < 0 ? 0.2 : -0.2));
                },
                { passive: false }
            );
            // Double-click / double-tap toggles zoomed in vs. reset.
            lightboxImgEl.addEventListener("dblclick", function () {
                setLightboxZoom(lbScale > 1 ? 1 : 2.5);
            });

            bindLightboxPointer();
        }

        return {
            open: openLightbox,
            close: closeLightbox,
            sync: syncLightbox,
            isOpen: function () {
                return lightboxOpen;
            },
        };
    }

    /** Update the hero image, counters, nav buttons and active thumbnail. */
    function renderGallerySlide(els, gallery, index) {
        const url = gallery[index];
        if (url && els.imgEl) els.imgEl.src = url;
        const counterText = index + 1 + " / " + gallery.length;
        if (els.counterEl) els.counterEl.textContent = index + 1 + " of " + gallery.length;
        if (els.counterBadge) els.counterBadge.textContent = counterText;
        if (els.prevBtn) els.prevBtn.disabled = false;
        if (els.nextBtn) els.nextBtn.disabled = false;
        if (els.prevHero) els.prevHero.disabled = false;
        if (els.nextHero) els.nextHero.disabled = false;
        if (els.thumbsEl) {
            const tabs = els.thumbsEl.querySelectorAll(".car-gallery-thumb");
            tabs.forEach(function (t, i) {
                t.classList.toggle("active", i === index);
                t.setAttribute("aria-selected", i === index);
            });
            const activeThumb = els.thumbsEl.querySelector(".car-gallery-thumb.active");
            if (activeThumb)
                activeThumb.scrollIntoView({
                    behavior: "smooth",
                    block: "nearest",
                    inline: "nearest",
                });
        }
    }

    function buildGalleryThumbs(thumbsEl, gallery, onPick) {
        gallery.forEach(function (url, i) {
            const t = document.createElement("button");
            t.type = "button";
            t.className = "car-gallery-thumb" + (i === 0 ? " active" : "");
            t.setAttribute("role", "tab");
            t.setAttribute("aria-selected", i === 0);
            t.setAttribute("aria-label", "Image " + (i + 1) + " of " + gallery.length);
            t.style.setProperty("background-image", "url(" + JSON.stringify(url) + ")");
            t.addEventListener("click", function () {
                onPick(i);
            });
            thumbsEl.appendChild(t);
        });
    }

    /** Escape closes the lightbox; arrows page (outside text inputs). */
    function bindGalleryKeyboard(lightbox, galleryLength, goPrev, goNext) {
        document.addEventListener("keydown", function (e) {
            if (lightbox.isOpen() && e.key === "Escape") {
                lightbox.close();
                e.preventDefault();
                return;
            }
            if (altMediaActive()) return;
            if (!lightbox.isOpen() && e.target.matches("input, textarea")) return;
            if (galleryLength <= 1) return;
            if (e.key === "ArrowLeft") {
                goPrev(e);
                e.preventDefault();
            }
            if (e.key === "ArrowRight") {
                goNext(e);
                e.preventDefault();
            }
        });
    }

    function initCarGallery() {
        const els = getGalleryElements();
        if (!els.imgEl) return;

        const gallery = readGalleryUrls(els);
        if (gallery.length === 0) {
            showEmptyGallery(els);
            return;
        }

        let activeImageIndex = 0;
        const lightbox = createGalleryLightbox({
            current: function () {
                return {
                    url: gallery[activeImageIndex],
                    alt: els.imgEl.getAttribute("alt") || "",
                    index: activeImageIndex,
                    length: gallery.length,
                };
            },
            prev: function () {
                goPrev();
            },
            next: function () {
                goNext();
            },
        });

        function show() {
            renderGallerySlide(els, gallery, activeImageIndex);
            lightbox.sync();
        }
        function goPrev(e) {
            if (e) e.preventDefault();
            activeImageIndex = (activeImageIndex - 1 + gallery.length) % gallery.length;
            show();
        }
        function goNext(e) {
            if (e) e.preventDefault();
            activeImageIndex = (activeImageIndex + 1) % gallery.length;
            show();
        }

        if (els.heroEl) {
            els.heroEl.addEventListener("click", function (e) {
                if (altMediaActive()) return;
                if (e.target.closest("button")) return;
                lightbox.open();
            });
        }
        if (els.prevBtn) els.prevBtn.addEventListener("click", goPrev);
        if (els.nextBtn) els.nextBtn.addEventListener("click", goNext);
        if (els.prevHero) els.prevHero.addEventListener("click", goPrev);
        if (els.nextHero) els.nextHero.addEventListener("click", goNext);

        if (els.thumbsEl && gallery.length > 1) {
            buildGalleryThumbs(els.thumbsEl, gallery, function (i) {
                activeImageIndex = i;
                show();
            });
        }

        bindGalleryKeyboard(lightbox, gallery.length, goPrev, goNext);
        show();
    }

    CarPage.initCarGallery = initCarGallery;
})(window.CarPage, window.CP);
