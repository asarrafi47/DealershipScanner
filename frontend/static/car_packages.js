/**
 * Premium car detail: download OEM window sticker, analyze options, show PNG preview.
 */
(function () {
    "use strict";

    const section = document.getElementById("car-packages-section");
    if (!section) {
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
    const stickerUnavailable = document.getElementById("car-window-sticker-unavailable");
    const stickerPdfEmbed = document.getElementById("car-window-sticker-pdf-embed");
    const showStickerUi = section.getAttribute("data-show-sticker") !== "0";
    const stickerLocal = section.getAttribute("data-sticker-local") === "1";
    const panelHasContent = section.getAttribute("data-panel-has-content") === "1";
    const stickerPreviewApi =
        (stickerPreview && stickerPreview.getAttribute("data-sticker-preview-api")) || "";
    const stickerPdfApi =
        (stickerPdfEmbed && stickerPdfEmbed.getAttribute("data-sticker-pdf-api")) || "";

    function serverRenderedPanelHasContent() {
        if (panelHasContent) {
            return true;
        }
        if (!panel) {
            return false;
        }
        if (!panel.hidden) {
            return true;
        }
        return !!panel.querySelector(
            ".listing-packages-sub-heading, .listing-package-plain, .listing-package-inline-list li"
        );
    }

    function ensurePanelVisible() {
        if (panel && serverRenderedPanelHasContent()) {
            panel.hidden = false;
        }
    }

    function resolveStickerPreviewUrl(explicitUrl) {
        const fromArg = (explicitUrl || "").trim();
        if (fromArg) {
            return fromArg;
        }
        const imgSrc =
            stickerImg && stickerImg.getAttribute("src")
                ? stickerImg.getAttribute("src").split("?")[0]
                : "";
        if (imgSrc) {
            return imgSrc;
        }
        if (stickerLocal && stickerPreviewApi) {
            return stickerPreviewApi;
        }
        return "";
    }

    function hideStickerPreview() {
        if (stickerPreview) {
            stickerPreview.hidden = true;
        }
        if (stickerImg) {
            stickerImg.removeAttribute("src");
        }
    }

    function readCsrf() {
        const m = document.querySelector('meta[name="csrf-token"]');
        return m ? (m.getAttribute("content") || "").trim() : "";
    }

    function hideStickerLoading() {
        if (stickerLoading) stickerLoading.hidden = true;
    }

    function showStickerPreview(previewUrl) {
        const url = resolveStickerPreviewUrl(previewUrl);
        if (!url || !stickerImg || !stickerPreview) {
            hideStickerPreview();
            hideStickerLoading();
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
                if (ok) {
                    stickerPreview.hidden = false;
                } else {
                    hideStickerPreview();
                }
                hideStickerLoading();
                resolve(ok);
            }
            stickerImg.onload = function () {
                done(stickerImg.naturalWidth > 0);
            };
            stickerImg.onerror = function () {
                hideWindowStickerBlock();
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

    function showPdfEmbed(pdfUrl) {
        const url = (pdfUrl || "").trim();
        if (!url || !stickerPdfEmbed) {
            return false;
        }
        const obj = stickerPdfEmbed.querySelector("object");
        if (obj) {
            obj.setAttribute("data", url);
        }
        stickerPdfEmbed.hidden = false;
        if (stickerPreview) {
            stickerPreview.hidden = true;
        }
        return true;
    }

    function hideWindowStickerBlock() {
        const wrap = document.getElementById("car-window-sticker-wrap");
        if (wrap) {
            wrap.hidden = true;
        }
        hideStickerPreview();
        hideStickerLoading();
    }

    function stickerHasDisplayableContent(data) {
        if (stickerIsVisible()) {
            return true;
        }
        if (!data) {
            return false;
        }
        const opts = data.listing_sticker_options || [];
        const groups = data.listing_sticker_option_groups || [];
        const sections = data.listing_sticker_option_sections || null;
        const specLines = data.sticker_spec_lines || [];
        const hasRealOpts =
            opts.length > 0 &&
            !opts.every(function (o) {
                const t = String((o && (o.name || o.label)) || o || "").toLowerCase();
                return t.includes("unreadable") || t.includes("visible on windshield");
            });
        if (hasRealOpts || groups.length || specLines.length) {
            return true;
        }
        if (
            sections &&
            ((sections.packages || []).length ||
                (sections.options || []).length ||
                sections.base)
        ) {
            return true;
        }
        return !!(data.window_sticker_visual_available && data.window_sticker_preview_url);
    }

    function esc(s) {
        const el = document.createElement("div");
        el.textContent = s == null ? "" : String(s);
        return el.innerHTML;
    }

    function ensureStickerWrapVisible() {
        let wrap = document.getElementById("car-window-sticker-wrap");
        if (wrap) return wrap;
        wrap = document.createElement("div");
        wrap.id = "car-window-sticker-wrap";
        wrap.className = "car-window-sticker-embed-wrap";
        const headDiv = document.createElement("div");
        headDiv.className = "car-window-sticker-head";
        const titleEl = document.createElement("h3");
        titleEl.className = "car-window-sticker-title";
        titleEl.textContent = "Window sticker";
        headDiv.appendChild(titleEl);
        const analyzed = document.createElement("div");
        analyzed.id = "car-window-sticker-analyzed";
        analyzed.className = "car-window-sticker-analyzed";
        wrap.appendChild(headDiv);
        wrap.appendChild(analyzed);
        const hint = section.querySelector(".car-packages-hint");
        if (hint && hint.nextSibling) {
            section.insertBefore(wrap, hint.nextSibling);
        } else {
            section.appendChild(wrap);
        }
        return wrap;
    }

    function formatOptionPrice(price) {
        if (price == null || price === "") return "";
        const p = Number(price);
        if (!isFinite(p)) return "";
        if (p < 0) return "$" + Math.abs(p).toLocaleString() + " credit";
        return "$" + p.toLocaleString();
    }

    function renderOptionRow(opt) {
        if (opt && typeof opt === "object" && (opt.name || opt.label)) {
            const name = opt.name || opt.label;
            const priceHtml = formatOptionPrice(opt.price);
            return (
                '<div class="car-sticker-opt-row"><span class="car-sticker-opt-name">' +
                esc(name) +
                "</span>" +
                (priceHtml
                    ? '<span class="car-sticker-opt-price">' + esc(priceHtml) + "</span>"
                    : "") +
                "</div>"
            );
        }
        return '<div class="car-sticker-opt-row"><span class="car-sticker-opt-name">' + esc(opt) + "</span></div>";
    }

    function renderPackageEntryHtml(entry) {
        if (!entry || !entry.name) return "";
        const priceHtml = formatOptionPrice(entry.price);
        if (entry.kind === "package" && entry.features && entry.features.length) {
            let html =
                '<details class="car-sticker-package-disclosure"><summary class="car-sticker-package-summary">' +
                '<span class="car-sticker-opt-name">' +
                esc(entry.name) +
                "</span>" +
                (priceHtml
                    ? '<span class="car-sticker-opt-price">' + esc(priceHtml) + "</span>"
                    : "") +
                '</summary><ul class="car-sticker-package-features">';
            entry.features.forEach(function (feat) {
                if (!feat || !feat.name) return;
                html += "<li>" + esc(feat.name) + "</li>";
            });
            html += "</ul></details>";
            return html;
        }
        return renderOptionRow(entry);
    }

    function renderStickerOptionGroupsHtml(groups) {
        if (!groups || !groups.length) return "";
        let html = "";
        groups.forEach(function (entry) {
            html += renderPackageEntryHtml(entry);
        });
        return html;
    }

    function renderStickerPackagesAndOptionsHtml(packages, options) {
        let html = "";
        if (packages && packages.length) {
            html += '<h4 class="car-window-sticker-sub">Packages</h4>';
            html += '<div class="car-window-sticker-opt-list car-window-sticker-opt-list--packages">';
            packages.forEach(function (entry) {
                html += renderPackageEntryHtml(entry);
            });
            html += "</div>";
        }
        if (options && options.length) {
            html += '<h4 class="car-window-sticker-sub">Options</h4>';
            html += '<div class="car-window-sticker-opt-list car-window-sticker-opt-list--options">';
            options.forEach(function (entry) {
                html += renderOptionRow(entry);
            });
            html += "</div>";
        }
        return html;
    }

    function renderStickerOptionSectionsHtml(sections) {
        if (!sections || typeof sections !== "object") return "";
        let html = "";
        const packages = sections.packages || [];
        const options = sections.options || [];
        const base = sections.base || null;
        html += renderStickerPackagesAndOptionsHtml(packages, options);
        if (base && base.name) {
            const priceHtml = formatOptionPrice(base.price);
            html += '<h4 class="car-window-sticker-sub">Base price & included equipment</h4>';
            html += '<div class="car-window-sticker-opt-list car-window-sticker-opt-list--base">';
            html +=
                '<div class="car-sticker-opt-row"><span class="car-sticker-opt-name">' +
                esc(base.name) +
                "</span>" +
                (priceHtml
                    ? '<span class="car-sticker-opt-price">' + esc(priceHtml) + "</span>"
                    : "") +
                "</div>";
            if (base.features && base.features.length) {
                base.features.forEach(function (feat) {
                    if (!feat || !feat.name) return;
                    html +=
                        '<div class="car-sticker-opt-row car-sticker-opt-row--included"><span class="car-sticker-opt-name">' +
                        esc(feat.name) +
                        "</span></div>";
                });
            }
            html += "</div>";
        }
        return html;
    }

    function renderStickerInBox(data) {
        if (!data) return;
        const sections = data.listing_sticker_option_sections || null;
        const groups = data.listing_sticker_option_groups || [];
        const opts = data.listing_sticker_options || [];
        const specLines = data.sticker_spec_lines || [];
        const msrp = data.sticker_msrp;
        const title = data.vehicle_title || "";
        const hasSections =
            sections &&
            ((sections.packages || []).length ||
                (sections.options || []).length ||
                sections.base);
        if (!hasSections && !groups.length && !opts.length && !specLines.length && !msrp && !title) {
            const existing = document.getElementById("car-window-sticker-analyzed");
            if (existing) existing.hidden = true;
            return;
        }
        ensureStickerWrapVisible();
        const analyzedEl = document.getElementById("car-window-sticker-analyzed");
        if (!analyzedEl) return;

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
        if (hasSections) {
            html += renderStickerOptionSectionsHtml(sections);
        } else if (groups.length || opts.length) {
            const pkgRows = groups.filter(function (g) {
                return g && g.kind === "package";
            });
            const optRows = groups.filter(function (g) {
                return g && g.kind !== "package";
            });
            html += renderStickerPackagesAndOptionsHtml(pkgRows, optRows);
            if (!groups.length) {
                html += '<h4 class="car-window-sticker-sub">Options</h4><div class="car-window-sticker-opt-list car-window-sticker-opt-list--options">';
                opts.forEach(function (opt) {
                    html += renderOptionRow(opt);
                });
                html += "</div>";
            }
        }
        analyzedEl.innerHTML = html;
        analyzedEl.hidden = false;
    }

    function renderListingPackagesSections(sections) {
        if (!panel || !sections || !sections.length) {
            return;
        }
        const listingSections = sections.filter(function (pkg) {
            return pkg && pkg.name && !pkg.from_vision;
        });
        if (!listingSections.length) {
            return;
        }
        let heading = panel.querySelector(".listing-packages-sub-heading--listing");
        let host = panel.querySelector(".listing-packages-listing-host");
        if (!host) {
            heading = document.createElement("h3");
            heading.className =
                "listing-packages-sub-heading listing-packages-sub-heading--listing";
            heading.textContent = "Packages from listing";
            host = document.createElement("div");
            host.className = "listing-packages-listing-host";
            panel.appendChild(heading);
            panel.appendChild(host);
        }
        host.innerHTML = "";
        listingSections.forEach(function (pkg) {
            if (!pkg || !pkg.name) {
                return;
            }
            const feats = pkg.features || [];
            const evidence = pkg.evidence || [];
            if (feats.length || evidence.length) {
                const details = document.createElement("details");
                details.className = "listing-package-disclosure";
                const summary = document.createElement("summary");
                summary.className = "listing-package-summary";
                summary.textContent = pkg.name;
                details.appendChild(summary);
                if (feats.length) {
                    const list = document.createElement("ul");
                    list.className = "listing-package-inline-list listing-package-features";
                    feats.forEach(function (f) {
                        const li = document.createElement("li");
                        li.textContent = f;
                        list.appendChild(li);
                    });
                    details.appendChild(list);
                } else if (evidence.length) {
                    const list = document.createElement("ul");
                    list.className = "listing-package-inline-list listing-package-evidence";
                    evidence.forEach(function (f) {
                        const li = document.createElement("li");
                        li.textContent = f;
                        list.appendChild(li);
                    });
                    details.appendChild(list);
                }
                host.appendChild(details);
            } else {
                const p = document.createElement("p");
                p.className = "listing-package-plain";
                p.textContent = pkg.name;
                host.appendChild(p);
            }
        });
    }

    function updatePackagesPanel(data) {
        if (!data || !panel) return;
        const photoItems =
            data.listing_photo_detected_equipment ||
            [].concat(data.listing_observed_features || [], data.listing_possible_packages || []);
        const opts = data.listing_sticker_options || [];
        const sections = data.listing_packages_sections || [];
        if (!data.packages_panel_has_content && !photoItems.length && !opts.length && !sections.length) {
            return;
        }
        panel.hidden = false;

        renderListingPackagesSections(sections);

        if (photoItems.length) {
            let heading = panel.querySelector(".listing-packages-sub-heading--photos");
            let list = panel.querySelector(".listing-package-inline-list--photos");
            if (!heading) {
                heading = document.createElement("h3");
                heading.className =
                    "listing-packages-sub-heading listing-packages-sub-heading--photos";
                heading.textContent = "Visible in photos (estimated)";
                list = document.createElement("ul");
                list.className = "listing-package-inline-list listing-package-inline-list--photos";
                panel.appendChild(heading);
                panel.appendChild(list);
            }
            list.innerHTML = "";
            photoItems.forEach(function (name) {
                const li = document.createElement("li");
                li.textContent = name;
                list.appendChild(li);
            });
        }

        if (showStickerUi) {
            const staleHeading = panel.querySelector(".listing-packages-sub-heading--sticker");
            const staleList = panel.querySelector(".listing-package-inline-list--sticker");
            if (staleHeading) staleHeading.remove();
            if (staleList) staleList.remove();
            panel.querySelectorAll(".listing-packages-sub-heading").forEach(function (heading) {
                const label = (heading.textContent || "").trim().toLowerCase();
                if (label === "factory options" || label === "standard equipment") {
                    const next = heading.nextElementSibling;
                    heading.remove();
                    if (
                        next &&
                        (next.classList.contains("listing-package-inline-list") ||
                            next.tagName === "UL")
                    ) {
                        next.remove();
                    }
                }
            });
            return;
        }

        if (!opts.length && !(data.listing_sticker_option_groups || []).length) return;
        const groups = data.listing_sticker_option_groups || [];
        let stickerHeading = panel.querySelector(".listing-packages-sub-heading--sticker");
        let list = panel.querySelector(".listing-package-inline-list--sticker");
        if (!list) {
            stickerHeading = document.createElement("h3");
            stickerHeading.className = "listing-packages-sub-heading listing-packages-sub-heading--sticker";
            stickerHeading.textContent = "Window sticker options";
            list = document.createElement("div");
            list.className =
                "listing-package-inline-list listing-package-inline-list--sticker listing-package-inline-list--priced car-window-sticker-opt-list";
            panel.insertBefore(stickerHeading, panel.firstChild);
            panel.insertBefore(list, stickerHeading.nextSibling);
        }
        list.innerHTML = groups.length
            ? renderStickerOptionGroupsHtml(groups)
            : "";
        if (!groups.length) {
            opts.forEach(function (opt) {
                list.insertAdjacentHTML("beforeend", renderOptionRow(opt));
            });
        }
    }

    function finish(data, httpOk) {
        if (loading) loading.hidden = true;
        ensurePanelVisible();

        const oemUrl = (section.getAttribute("data-oem-sticker-url") || "").trim();
        let previewPromise = Promise.resolve(stickerIsVisible());

        if (data && data.window_sticker_visual_available && showStickerUi) {
            previewPromise = showStickerPreview(data.window_sticker_preview_url).then(function (shown) {
                return shown || stickerIsVisible();
            });
        } else if (showStickerUi && stickerImg && stickerImg.getAttribute("src")) {
            previewPromise = showStickerPreview("").then(function (shown) {
                return shown || stickerIsVisible();
            });
        }

        if (
            data &&
            ((data.listing_sticker_options || []).length ||
                (data.listing_sticker_option_groups || []).length ||
                (data.listing_sticker_option_sections &&
                    (((data.listing_sticker_option_sections.packages || []).length ||
                        (data.listing_sticker_option_sections.options || []).length) ||
                        data.listing_sticker_option_sections.base)) ||
                (data.sticker_spec_lines || []).length ||
                data.sticker_msrp)
        ) {
            renderStickerInBox(data);
            hideStickerLoading();
        } else if (stickerIsVisible()) {
            renderStickerInBox(data);
            hideStickerLoading();
        } else {
            hideStickerLoading();
            if (stickerPreview && !stickerIsVisible()) stickerPreview.hidden = true;
        }

        if (stickerUnavailable) {
            const hasStickerData =
                stickerIsVisible() ||
                (data &&
                    ((data.listing_sticker_options || []).length ||
                        (data.listing_sticker_option_groups || []).length ||
                        (data.sticker_spec_lines || []).length ||
                        data.sticker_msrp ||
                        data.window_sticker_available));
            stickerUnavailable.hidden = !showStickerUi || !!hasStickerData;
        }

        updatePackagesPanel(data);
        ensurePanelVisible();

        previewPromise.then(function (shown) {
            ensurePanelVisible();
            if (showStickerUi && !stickerHasDisplayableContent(data)) {
                hideWindowStickerBlock();
                if (
                    section &&
                    !serverRenderedPanelHasContent() &&
                    !(data && data.packages_panel_has_content)
                ) {
                    section.hidden = true;
                }
            }
            let hasContent =
                shown || stickerIsVisible() || serverRenderedPanelHasContent();
            if (data && data.packages_panel_has_content && panel) {
                panel.hidden = false;
                hasContent = true;
            }
            if (data && ((data.listing_sticker_options || []).length || (data.listing_sticker_option_groups || []).length)) {
                hasContent = true;
            }
            if (data && (data.listing_photo_detected_equipment || []).length) {
                hasContent = true;
            }
            if (data && (data.sticker_spec_lines || []).length) {
                hasContent = true;
            }
            if (data && stickerHasDisplayableContent(data)) {
                hasContent = true;
            }
            if (data && (data.listing_packages_sections || []).length) {
                hasContent = true;
            }
            if (data && data.listing_description_parsed) {
                hasContent = true;
            }

            if (empty) {
                empty.hidden = hasContent;
            }
            if (data && data.fetch_error && empty && !hasContent) {
                empty.textContent =
                    "Could not download a window sticker for this listing yet. If the dealer provides one, try again later or open the listing page.";
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

    if (panelHasContent) {
        ensurePanelVisible();
    }

    if (showStickerUi && stickerLoading) {
        stickerLoading.hidden = false;
    }

    runEnsure()
        .then(function (wrapped) {
            finish(wrapped.data, wrapped.ok);
        })
        .catch(function () {
            if (loading) loading.hidden = true;
            hideStickerLoading();
            ensurePanelVisible();
            const oemUrl = (section.getAttribute("data-oem-sticker-url") || "").trim();
            if (empty && !oemUrl && !serverRenderedPanelHasContent()) {
                empty.textContent = "Could not load package data. Try refreshing the page.";
                empty.hidden = false;
            }
        });
})();
