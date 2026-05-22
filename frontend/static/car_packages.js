/**
 * Premium car detail: download OEM window sticker, analyze options, show PNG preview.
 */
(function () {
    "use strict";

    const section = document.getElementById("car-packages-section");
    if (!section || section.querySelector(".car-packages-hint a[href*='premium']")) {
        return;
    }

    const carId = section.getAttribute("data-car-id");
    if (!carId) return;

    const loading = document.getElementById("car-packages-loading");
    const stickerLoading = document.getElementById("car-window-sticker-loading");
    const stickerAnalyzed = document.getElementById("car-window-sticker-analyzed");
    const stickerPreview = document.getElementById("car-window-sticker-preview");
    const stickerImg = document.getElementById("car-window-sticker-img");
    const panel = document.getElementById("car-packages-panel");
    const empty = document.getElementById("car-packages-empty");
    const skipEnsure = section.getAttribute("data-skip-ensure") === "1";
    const stickerPreviewApi =
        (stickerPreview && stickerPreview.getAttribute("data-sticker-preview-api")) || "";

    function readCsrf() {
        const m = document.querySelector('meta[name="csrf-token"]');
        return m ? (m.getAttribute("content") || "").trim() : "";
    }

    function hideStickerLoading() {
        if (stickerLoading) stickerLoading.hidden = true;
    }

    function showStickerPreview(previewUrl) {
        const url = (previewUrl || stickerPreviewApi || "").trim();
        if (!url || !stickerImg || !stickerPreview) {
            return Promise.resolve(false);
        }
        if (stickerImg.complete && stickerImg.naturalWidth > 0) {
            stickerPreview.hidden = false;
            hideStickerLoading();
            return Promise.resolve(true);
        }
        if (stickerLoading) stickerLoading.hidden = false;
        return new Promise(function (resolve) {
            function done(ok) {
                stickerPreview.hidden = !ok;
                hideStickerLoading();
                resolve(ok);
            }
            stickerImg.onload = function () {
                done(stickerImg.naturalWidth > 0);
            };
            stickerImg.onerror = function () {
                done(false);
            };
            const current = (stickerImg.getAttribute("src") || "").split("?")[0];
            const base = url.split("?")[0];
            if (current !== base || !stickerImg.getAttribute("src")) {
                stickerImg.src =
                    url + (url.indexOf("?") >= 0 ? "&" : "?") + "_ts=" + Date.now();
            }
            stickerImg.hidden = false;
            if (stickerImg.complete) {
                done(stickerImg.naturalWidth > 0);
            }
        });
    }

    function stickerIsVisible() {
        return !!(
            stickerPreview &&
            !stickerPreview.hidden &&
            stickerImg &&
            stickerImg.complete &&
            stickerImg.naturalWidth > 0
        );
    }

    function esc(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function renderStickerInBox(data) {
        if (!stickerAnalyzed || !data) return;
        const opts = data.listing_sticker_options || [];
        const specLines = data.sticker_spec_lines || [];
        const msrp = data.sticker_msrp;
        const title = data.vehicle_title || "";

        if (!opts.length && !specLines.length && !msrp && !title) {
            stickerAnalyzed.hidden = true;
            return;
        }

        let html = "";
        if (title) {
            html += '<p class="car-window-sticker-ymm">' + esc(title) + "</p>";
        }
        if (specLines.length) {
            html += '<dl class="car-window-sticker-specs">';
            specLines.forEach(function (row) {
                if (!row || !row.label || !row.value) return;
                html +=
                    "<div><dt>" +
                    esc(row.label) +
                    "</dt><dd>" +
                    esc(row.value) +
                    "</dd></div>";
            });
            html += "</dl>";
        }
        if (msrp) {
            html +=
                '<p class="car-window-sticker-msrp"><span class="car-sticker-color-label">Sticker price:</span> $' +
                esc(Number(msrp).toLocaleString()) +
                "</p>";
        }
        if (opts.length) {
            html += '<h4 class="car-window-sticker-sub">Optional equipment</h4><ul class="car-window-sticker-opt-list">';
            opts.forEach(function (name) {
                html += "<li>" + esc(name) + "</li>";
            });
            html += "</ul>";
        }
        stickerAnalyzed.innerHTML = html;
        stickerAnalyzed.hidden = false;
    }

    function updatePackagesPanel(data) {
        if (!data || !data.packages_panel_has_content || !panel) return;
        panel.hidden = false;
        const opts = data.listing_sticker_options || [];
        if (!opts.length) return;
        let list = panel.querySelector(".listing-package-inline-list");
        if (!list) {
            const h3 = document.createElement("h3");
            h3.className = "listing-packages-sub-heading";
            h3.textContent = "Window sticker options";
            list = document.createElement("ul");
            list.className = "listing-package-inline-list";
            panel.insertBefore(h3, panel.firstChild);
            panel.insertBefore(list, h3.nextSibling);
        }
        list.innerHTML = "";
        opts.forEach(function (name) {
            const li = document.createElement("li");
            li.textContent = name;
            list.appendChild(li);
        });
    }

    function finish(data, httpOk) {
        if (loading) loading.hidden = true;

        const oemUrl = (section.getAttribute("data-oem-sticker-url") || "").trim();
        let previewPromise = Promise.resolve(stickerIsVisible());

        if (data && data.window_sticker_available) {
            previewPromise = showStickerPreview(data.window_sticker_preview_url).then(function (shown) {
                return shown || stickerIsVisible();
            });
            renderStickerInBox(data);
        } else if (stickerIsVisible()) {
            renderStickerInBox(data);
            hideStickerLoading();
        } else if (data && ((data.listing_sticker_options || []).length || (data.sticker_spec_lines || []).length)) {
            renderStickerInBox(data);
            hideStickerLoading();
        } else {
            hideStickerLoading();
            if (stickerPreview && !stickerIsVisible()) stickerPreview.hidden = true;
        }

        updatePackagesPanel(data);

        previewPromise.then(function (shown) {
            let hasContent = shown || stickerIsVisible();
            if (data && data.packages_panel_has_content && panel) {
                panel.hidden = false;
                hasContent = true;
            }
            if (data && (data.listing_sticker_options || []).length) {
                hasContent = true;
            }
            if (data && (data.sticker_spec_lines || []).length) {
                hasContent = true;
            }
            if (data && data.window_sticker_available && !shown && oemUrl) {
                hasContent = true;
            }

            if (empty) {
                empty.hidden = hasContent;
            }
            if (data && data.fetch_error && empty && !hasContent) {
                empty.textContent =
                    "Could not download this VIN’s OEM window sticker. Use “Open OEM PDF” above for the full file.";
                empty.hidden = false;
            }
            if (!httpOk && empty && !hasContent) {
                const err = data && data.error;
                if (err === "csrf_required") {
                    empty.textContent = "Session expired. Refresh the page and try again.";
                } else if (err === "premium_required") {
                    empty.textContent = "Premium subscription required for window sticker analysis.";
                } else {
                    empty.textContent = "Could not load package data. Try refreshing the page.";
                }
                empty.hidden = false;
            }
        });
    }

    function runEnsure() {
        const headers = {
            "X-CSRF-Token": readCsrf(),
            "Content-Type": "application/json",
        };
        return fetch("/api/cars/" + encodeURIComponent(carId) + "/packages/ensure?vision=0", {
            method: "POST",
            headers: headers,
            credentials: "same-origin",
        }).then(function (r) {
            return r
                .json()
                .catch(function () {
                    return {};
                })
                .then(function (data) {
                    return { ok: r.ok, data: data || {} };
                });
        });
    }

    if (skipEnsure && stickerPreviewApi) {
        showStickerPreview(stickerPreviewApi);
    }

    if (skipEnsure) {
        runEnsure()
            .then(function (wrapped) {
                finish(wrapped.data, wrapped.ok);
            })
            .catch(function () {
                if (loading) loading.hidden = true;
                hideStickerLoading();
            });
        return;
    }

    runEnsure()
        .then(function (wrapped) {
            finish(wrapped.data, wrapped.ok);
        })
        .catch(function () {
            if (loading) loading.hidden = true;
            hideStickerLoading();
            const oemUrl = (section.getAttribute("data-oem-sticker-url") || "").trim();
            if (empty && !oemUrl) {
                empty.textContent = "Could not load package data. Try refreshing the page.";
                empty.hidden = false;
            }
        });
})();
