/**
 * Listings result card: condition/mileage tokens and the card HTML renderer.
 *
 * Pure helpers extracted from main.js (2026-10-01 monolith audit, F1). They
 * attach to window.DSL, read only their arguments plus the SC helpers from
 * sc-helpers.js, and never touch the DOM. Load order (listings.html,
 * dealership.html): sc-helpers.js -> listings/*.js -> main.js.
 * Unit tests: backend/tests/js/listings_*.test.js.
 */
(function (root) {
    const DSL = (root.DSL = root.DSL || {});

    /** Card condition token: New / Pre-owned / CPO (IH-02). Reads the row, never infers from mileage. */
    function cardConditionToken(car) {
        if (!car) return "Condition not listed";
        if (car.is_cpo === true || car.is_cpo === 1 || car.is_cpo === "1") return "CPO";
        const raw = car.condition != null ? String(car.condition).trim().toLowerCase() : "";
        if (!raw || raw === "—" || raw === "-") return "Condition not listed";
        if (raw.includes("certified") || /\bcpo\b/.test(raw)) return "CPO";
        if (raw === "new") return "New";
        return "Pre-owned";
    }

    /** Mirrors backend/utils/mileage_display.mileage_not_listed (DC-6 / IH-03). */
    function cardMileageNotListed(car) {
        if (!car) return true;
        if (typeof car.mileage_not_listed === "boolean") return car.mileage_not_listed;
        const raw = car.mileage;
        if (raw == null || String(raw).trim() === "") return true;
        const mi = Number(String(raw).replace(/,/g, ""));
        if (!Number.isFinite(mi)) return true;
        if (mi > 0) return false;
        if (cardConditionToken(car) !== "New") return true;
        const yr = parseInt(car.year, 10);
        return Number.isFinite(yr) && yr < new Date().getFullYear() - 1;
    }

    /**
     * One result card's markup.
     *
     * @param c          car row (API shape)
     * @param savedSet   Set of saved car ids (numbers)
     * @param compareIds array of car ids in the compare tray
     * @param ctx        page state the card depends on, resolved by the caller:
     *                   {loggedIn: bool, zipForUrl: string, distanceMiles: number|null}
     */
    function renderCardHtml(c, savedSet, compareIds, ctx) {
        const loggedIn = !!(ctx && ctx.loggedIn);
        const gallery = Array.isArray(c.gallery) ? c.gallery : [];
        const imgRaw = (gallery.length && gallery[0]) ? gallery[0] : (c.image_url || "") || "/static/placeholder.svg";
        const imgSrc = SC.safeImageSrc(imgRaw);
        const imgSrcAttr = SC.escapeHtml(imgSrc);
        const photoCount = Number(c.photo_count) > 0 ? Number(c.photo_count) : gallery.length;
        const photoLabel = photoCount > 1 ? `${photoCount} photos` : "";
        const idNum = Number(c.id);
        const idStr = Number.isFinite(idNum) && idNum > 0 ? String(Math.floor(idNum)) : "0";
        const dashLike = (v) => {
            const s = String(v || "").trim();
            return !s || s === "—" || s === "-" || s === "--";
        };
        // Condition leads the meta line (IH-02); a 0/blank odometer on a used or
        // older car is "Mileage not listed", never "0 mi" (DC-6 / IH-03).
        const metaBits = [
            `<span class="result-condition">${SC.escapeHtml(cardConditionToken(c))}</span>`,
            cardMileageNotListed(c) ? "Mileage not listed" : `${SC.fmt(c.mileage)} mi`,
        ];
        if (!dashLike(c.fuel_type)) metaBits.push(SC.escapeHtml(c.fuel_type));
        if (!dashLike(c.drivetrain)) metaBits.push(SC.escapeHtml(c.drivetrain));
        const specBits = [];
        if (!dashLike(c.body_style)) specBits.push(SC.escapeHtml(c.body_style));
        const specLine = specBits.length
            ? `<p class="result-meta result-meta--specs">${specBits.join(" &middot; ")}</p>`
            : "";
        const incompletePill = c.public_incomplete
            ? `<span class="result-incomplete-note" title="Missing some public-listing fields">Incomplete</span>`
            : "";
        const cpoBadge = SC.cpoBadgeHtml(c);
        const mkt = c.market;
        // A payment-shaped "price" (either intel tier flagged it) renders as
        // an advertised payment, never as the sale price with a deal badge.
        const paymentListed = c.payment_listed === true
            || (mkt && mkt.vs_market === "payment_listed")
            || (c.deal_score && c.deal_score.label === "payment_listed");
        // Premium trim-avg badge takes precedence; otherwise fall back to the
        // free coarse market deal score attached during serialization.
        const dealBadge = paymentListed ? "" : (SC.dealBadgeHtml(mkt) || SC.dealScoreBadgeHtml(c.deal_score));
        const priceDropBadge = paymentListed ? "" : SC.priceDropBadgeHtml(c);
        let marketLine = "";
        if (!paymentListed && mkt && mkt.avg_price_display) {
            marketLine = `<p class="result-market-sub">Trim avg ${SC.escapeHtml(mkt.avg_price_display)}</p>`;
        }
        const distMi = ctx ? ctx.distanceMiles : null;
        const distLine = (distMi != null && Number.isFinite(distMi))
            ? `<span class="result-distance">${distMi < 10 ? distMi.toFixed(1) : Math.round(distMi)} mi away</span>`
            : "";
        const isSaved = savedSet.has(idNum);
        const inCompare = compareIds.includes(idNum);
        const saveBtn = loggedIn
            ? `<button type="button" class="result-save-btn${isSaved ? " result-save-btn--saved" : ""}" data-car-id="${idStr}" aria-label="${isSaved ? "Saved" : "Save this car"}" title="${isSaved ? "Remove from saved" : "Save"}">`
                + `<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" stroke-width="1.8" fill="${isSaved ? "currentColor" : "none"}" aria-hidden="true">`
                + `<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>`
                + `</svg></button>`
            // Guests get the same heart affordance; clicking it sends them to the
            // existing sign-in page (the same prompt ds_comments.js uses for guests)
            // instead of silently doing nothing.
            : `<button type="button" class="result-save-btn" data-guest="1" aria-label="Sign in to save this car" title="Sign in to save">`
                + `<svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" stroke-width="1.8" fill="none" aria-hidden="true">`
                + `<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>`
                + `</svg></button>`;
        const compareCb = `<label class="result-compare-label" title="Add to compare (max 4)">`
            + `<input type="checkbox" class="result-compare-cb" data-car-id="${idStr}"${inCompare ? " checked" : ""}>`
            + `<span>Compare</span></label>`;
        const zipForUrl = ctx && ctx.zipForUrl ? ctx.zipForUrl : "";
        const carHref = zipForUrl && /^\d{5}$/.test(String(zipForUrl).trim())
            ? `/car/${idStr}?zip_code=${encodeURIComponent(String(zipForUrl).trim())}`
            : `/car/${idStr}`;
        // Dealer name -> /dealership/<dealer_id>, the same research page the car page
        // links to. Same key shape the route validates (_DEALER_KEY_RE), so a card
        // carrying a junk dealer_id renders plain text instead of a link to a 404.
        const dealerName = String(c.dealer_name || "").trim();
        const dealerKey = String(c.dealer_id || "").trim();
        const dealerNameHtml = SC.escapeHtml(dealerName);
        const dealerHtml = (dealerName && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(dealerKey))
            ? `<a href="/dealership/${encodeURIComponent(dealerKey)}" class="result-dealer result-dealer-link">${dealerNameHtml}</a>`
            : `<span class="result-dealer">${dealerNameHtml}</span>`;
        // The backend sets location_confirmed=false only when this listing's own photos
        // (or a proven group-wide feed) contradict the dealership it is filed under —
        // see backend/db/repositories/cars_repo.py. The card keeps the dealer name (that
        // is who published it) and stops implying the car is on that lot.
        const locUnconfirmed = c.location_confirmed === false
            ? `<span class="result-dealer-unconfirmed" title="${SC.escapeHtml(c.location_note || "")}">Location unconfirmed</span>`
            : "";
        // The dealer row sits OUTSIDE .result-card-link: an <a> nested inside an <a> is
        // invalid HTML, and browsers recover by unnesting it, which breaks the card link.
        return `
            <article class="result-card${c.public_incomplete ? " result-card--incomplete" : ""}">
                <a href="${carHref}" class="result-card-link">
                    <div class="result-image-wrap">
                        <img class="result-image" src="${imgSrcAttr}" alt="" loading="lazy" decoding="async" onerror="this.onerror=null;this.src='/static/placeholder.svg';">
                    </div>
                    <div class="result-content">
                        <div class="result-title-row">
                            <h2>${SC.escapeHtml(c.title)}</h2>
                            ${cpoBadge}
                            ${incompletePill}
                        </div>
                        <p class="result-trim">${SC.escapeHtml(c.trim || "")}</p>
                        ${paymentListed
                            ? `<p class="result-price result-price--payment">${SC.fmtUSD(c.price)}/mo advertised<span class="result-price-payment-note">&mdash; see dealer for the price</span></p>`
                            : `<p class="result-price">${SC.fmtUSD(c.price)}${priceDropBadge}</p>`}
                        ${dealBadge ? `<p class="result-deal-line">${dealBadge}</p>` : ""}
                        ${marketLine}
                        <p class="result-meta">${metaBits.join(" &middot; ")}</p>
                        ${specLine}
                    </div>
                </a>
                <p class="result-dealer-row">
                    ${distLine}
                    ${dealerHtml}
                    ${locUnconfirmed}
                </p>
                <div class="result-card-actions">
                    ${compareCb}
                    ${photoLabel ? `<span class="result-photo-count">${SC.escapeHtml(photoLabel)}</span>` : ""}
                    ${saveBtn}
                </div>
            </article>`;
    }

    /**
     * Resolve URL and allow only http(s) for CSS background-image (mitigates
     * javascript: / data: in listings). Was defined in main.js with no caller;
     * kept here as the listings copy of dev.js's helper.
     */
    function cssSingleQuotedUrl(url, origin) {
        const raw = String(url || "").trim();
        if (!raw) return "/static/placeholder.svg";
        try {
            const abs = new URL(raw, origin || root.location.origin);
            if (abs.protocol !== "http:" && abs.protocol !== "https:") {
                return "/static/placeholder.svg";
            }
            return abs.href.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        } catch (_) {
            return "/static/placeholder.svg";
        }
    }

    DSL.cardConditionToken = cardConditionToken;
    DSL.cardMileageNotListed = cardMileageNotListed;
    DSL.renderCardHtml = renderCardHtml;
    DSL.cssSingleQuotedUrl = cssSingleQuotedUrl;
})(window);
