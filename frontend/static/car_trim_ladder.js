/**
 * Interactive trim ladder on the car detail Specs tab.
 * Renders from SSR when available; otherwise hydrates from GET /api/cars/:id.
 */
(function () {
    "use strict";

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function renderTrimLadderHtml(ladder) {
        const steps = Array.isArray(ladder.steps) ? ladder.steps : [];
        let stepsHtml = "";
        steps.forEach(function (step, i) {
            const cls = ["car-trim-ladder__step", "car-trim-ladder__step--" + (step.side || (i % 2 === 0 ? "left" : "right"))];
            if (step.is_current) cls.push("car-trim-ladder__step--current");
            if (step.is_passed) cls.push("car-trim-ladder__step--passed");

            let addsHtml = "";
            const adds = Array.isArray(step.adds) ? step.adds : [];
            if (adds.length) {
                addsHtml =
                    '<div class="car-trim-ladder__panel" id="car-trim-ladder-panel-' +
                    i +
                    '">' +
                    '<p class="car-trim-ladder__panel-label">What this trim adds</p>' +
                    '<ul class="car-trim-ladder__adds">' +
                    adds
                        .map(function (item) {
                            return "<li>" + escapeHtml(item) + "</li>";
                        })
                        .join("") +
                    "</ul></div>";
            }

            stepsHtml +=
                '<li class="' +
                cls.join(" ") +
                '" role="listitem" data-trim-index="' +
                i +
                '" data-trim-name="' +
                escapeHtml(step.name || "") +
                '">' +
                '<button type="button" class="car-trim-ladder__node" aria-label="Select ' +
                escapeHtml(step.name || "") +
                ' trim">' +
                '<span class="car-trim-ladder__dot" aria-hidden="true"></span>' +
                "</button>" +
                '<div class="car-trim-ladder__card" role="button" tabindex="0" aria-expanded="false" aria-controls="car-trim-ladder-panel-' +
                i +
                '">' +
                '<div class="car-trim-ladder__card-head">' +
                '<span class="car-trim-ladder__name">' +
                escapeHtml(step.name || "") +
                "</span>" +
                (step.is_current
                    ? '<span class="car-trim-ladder__badge car-trim-ladder__badge--vehicle">This vehicle</span>'
                    : "") +
                "</div>" +
                addsHtml +
                "</div></li>";
        });

        const lead = ladder.matched
            ? "This listing matches <strong>" +
              escapeHtml(ladder.listing_trim || "") +
              "</strong> on the " +
              escapeHtml(ladder.make || "") +
              " " +
              escapeHtml(ladder.model || "") +
              " trim ladder."
            : "Trim ladder for " +
              escapeHtml(ladder.make || "") +
              " " +
              escapeHtml(ladder.model || "") +
              '. We could not confidently match &ldquo;' +
              escapeHtml(ladder.listing_trim || "") +
              "&rdquo; to a rung.";

        let confidenceHtml = "";
        if (ladder.quality === "high") {
            confidenceHtml = '<p class="car-trim-ladder__confidence">Curated OEM trim order</p>';
        } else if (!ladder.matched) {
            confidenceHtml =
                '<p class="car-trim-ladder__confidence car-trim-ladder__confidence--low">Trim match uncertain — ladder shown for reference.</p>';
        }

        return (
            '<div class="car-trim-ladder" id="car-trim-ladder" aria-label="' +
            escapeHtml(ladder.label || "Trim lineup") +
            '" data-make="' +
            escapeHtml(ladder.make || "") +
            '" data-model="' +
            escapeHtml(ladder.model || "") +
            '" data-listing-trim="' +
            escapeHtml(ladder.listing_trim || "") +
            '">' +
            '<h3 class="car-trim-ladder__title">Trim lineup</h3>' +
            '<p class="car-trim-ladder__lead" id="car-trim-ladder-lead">' +
            lead +
            "</p>" +
            confidenceHtml +
            '<ol class="car-trim-ladder__timeline" role="list">' +
            stepsHtml +
            "</ol></div>"
        );
    }

    function setSplitLayout(hasLadder) {
        const split = document.querySelector(".car-specs-split");
        const mount = document.querySelector(".car-specs-split__col--ladder");
        if (!split) return;
        split.classList.toggle("car-specs-split--single", !hasLadder);
        if (mount) {
            if (hasLadder) {
                mount.removeAttribute("hidden");
            } else {
                mount.setAttribute("hidden", "");
            }
        }
    }

    function initTrimLadder() {
        const root = document.getElementById("car-trim-ladder");
        if (!root) return;

        const steps = [...root.querySelectorAll(".car-trim-ladder__step")];
        if (!steps.length) return;

        setSplitLayout(true);

        const leadEl = root.querySelector(".car-trim-ladder__lead");
        const make = root.getAttribute("data-make") || "";
        const model = root.getAttribute("data-model") || "";
        const listingTrim = root.getAttribute("data-listing-trim") || "";

        function leadHtml(trimName) {
            const name = trimName || listingTrim || "this trim";
            const strong = "<strong>" + escapeHtml(name) + "</strong>";
            if (make && model) {
                return "Viewing " + strong + " on the " + escapeHtml(make) + " " + escapeHtml(model) + " trim ladder.";
            }
            return "Viewing " + strong + " on the trim ladder.";
        }

        function setActive(index, options) {
            const opts = options || {};
            const scrollIntoView = opts.scrollIntoView !== false;

            steps.forEach(function (step, i) {
                const active = i === index;
                step.classList.toggle("car-trim-ladder__step--active", active);
                const card = step.querySelector(".car-trim-ladder__card");
                const panel = step.querySelector(".car-trim-ladder__panel");
                if (card) {
                    const hasPanel = !!(panel && panel.querySelector(".car-trim-ladder__adds"));
                    card.setAttribute("aria-expanded", active && hasPanel ? "true" : "false");
                }
                const nodeBtn = step.querySelector(".car-trim-ladder__node");
                if (nodeBtn) {
                    nodeBtn.setAttribute("aria-pressed", active ? "true" : "false");
                }
            });

            const activeStep = steps[index];
            if (!activeStep) return;

            const trimName = activeStep.getAttribute("data-trim-name") || "";
            if (leadEl) {
                leadEl.innerHTML = leadHtml(trimName);
            }

            if (scrollIntoView) {
                try {
                    activeStep.scrollIntoView({ block: "nearest", behavior: "smooth" });
                } catch (_) {
                    activeStep.scrollIntoView(true);
                }
            }
        }

        let initial = steps.findIndex(function (s) {
            return s.classList.contains("car-trim-ladder__step--current");
        });
        if (initial >= 0) {
            setActive(initial, { scrollIntoView: false });
        } else {
            steps.forEach(function (step) {
                step.classList.remove("car-trim-ladder__step--active");
                const card = step.querySelector(".car-trim-ladder__card");
                const panel = step.querySelector(".car-trim-ladder__panel");
                if (card) {
                    const hasPanel = !!(panel && panel.querySelector(".car-trim-ladder__adds"));
                    card.setAttribute("aria-expanded", "false");
                }
                const nodeBtn = step.querySelector(".car-trim-ladder__node");
                if (nodeBtn) {
                    nodeBtn.setAttribute("aria-pressed", "false");
                }
            });
        }

        steps.forEach(function (step, index) {
            const nodeBtn = step.querySelector(".car-trim-ladder__node");
            const card = step.querySelector(".car-trim-ladder__card");

            function activateFromUi() {
                setActive(index);
            }

            if (nodeBtn) {
                nodeBtn.addEventListener("click", function (e) {
                    e.preventDefault();
                    e.stopPropagation();
                    activateFromUi();
                });
            }

            if (card) {
                card.addEventListener("click", function (e) {
                    if (e.target.closest(".car-trim-ladder__node")) return;
                    activateFromUi();
                });
                card.addEventListener("keydown", function (e) {
                    if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        activateFromUi();
                    } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
                        e.preventDefault();
                        const delta = e.key === "ArrowDown" ? 1 : -1;
                        const next = Math.max(0, Math.min(steps.length - 1, index + delta));
                        const nextCard = steps[next].querySelector(".car-trim-ladder__card");
                        if (nextCard) nextCard.focus();
                    }
                });
            }
        });
    }

    function hydrateTrimLadderFromApi() {
        if (document.getElementById("car-trim-ladder")) {
            initTrimLadder();
            return Promise.resolve(true);
        }

        const mount = document.querySelector(".car-specs-split__col--ladder");
        if (!mount || !document.getElementById("car-panel-specs")) {
            return Promise.resolve(false);
        }

        const carId = document.body.getAttribute("data-car-id");
        if (!carId) {
            setSplitLayout(false);
            return Promise.resolve(false);
        }

        return fetch("/api/cars/" + encodeURIComponent(carId), { credentials: "same-origin" })
            .then(function (res) {
                if (!res.ok) throw new Error("trim ladder fetch failed");
                return res.json();
            })
            .then(function (data) {
                const ladder = data && data.trim_ladder;
                const stepCount = ladder && Array.isArray(ladder.steps) ? ladder.steps.length : 0;
                if (!data || !data.ok || !ladder || stepCount < 2) {
                    setSplitLayout(false);
                    return false;
                }
                mount.innerHTML = renderTrimLadderHtml(ladder);
                initTrimLadder();
                return true;
            })
            .catch(function () {
                setSplitLayout(false);
                return false;
            });
    }

    function bootTrimLadder() {
        if (document.getElementById("car-trim-ladder")) {
            initTrimLadder();
            return;
        }
        hydrateTrimLadderFromApi();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", bootTrimLadder);
    } else {
        bootTrimLadder();
    }
})();
