/* Market intel: premium trim-average price cohorts for listings cards.
 *
 * Extracted verbatim from main.js. Loaded BEFORE main.js (see the
 * <script defer> order in listings.html and dealership.html); helpers
 * attach to the shared window.SC namespace (created by sc-helpers.js,
 * which loads first). Cohort data lives on window.__DS_MARKET_STATS.
 *
 * Bridge back into main.js's closure: SC.scalarVal is published by
 * main.js at DOMContentLoaded so reloadMarketStats can read the live
 * zip/radius filter inputs. All calls into it stay typeof-guarded, the
 * same guard the code carried when it lived inside the closure.
 */
(function (SC) {
    window.__DS_MARKET_STATS = null;

    const _MILEAGE_BANDS = ["0-25k", "25-50k", "50-75k", "75-100k", "100k+", "unknown"];

    function marketIntelForCar(car) {
        const meta = window.__DS_MARKET_STATS;
        if (!meta || !meta.cohorts) return null;

        const cohorts = meta.cohorts;
        const minSamples = Number(meta.min_samples) > 0 ? Number(meta.min_samples) : 3;
        const yearWindow = Number(meta.year_window) >= 0 ? Number(meta.year_window) : 1;
        const [mk, md, tr] = SC.marketTrimParts(car);
        if (!mk || !md) return null;

        let year = parseInt(car.year, 10);
        year = Number.isFinite(year) ? year : null;
        const mb = SC.mileageBand(car.mileage);

        const attempts = [];
        if (year != null && mb !== "unknown") {
            attempts.push({ years: [year], bands: [mb] });
            const widen = [year];
            for (let d = 1; d <= yearWindow; d++) {
                widen.push(year - d, year + d);
            }
            attempts.push({ years: widen, bands: [mb] });
        }
        if (year != null) {
            attempts.push({ years: [year], bands: _MILEAGE_BANDS });
            const widen = [year];
            for (let d = 1; d <= yearWindow; d++) {
                widen.push(year - d, year + d);
            }
            attempts.push({ years: widen, bands: _MILEAGE_BANDS });
        }
        if (mb !== "unknown") {
            const years = [];
            for (let y = 2010; y <= 2030; y++) years.push(y);
            attempts.push({ years, bands: [mb] });
        }
        {
            const prefix = `${mk}|${md}|${tr}|`;
            const entries = Object.entries(cohorts)
                .filter(([k]) => k.startsWith(prefix))
                .map(([, v]) => v);
            attempts.push({ entries });
        }

        for (const att of attempts) {
            const stats = att.entries
                ? SC.weightedCohortStats(att.entries, minSamples)
                : SC.weightedCohortStats(
                    SC.cohortEntries(cohorts, mk, md, tr, att.years, att.bands),
                    minSamples
                );
            if (!stats) continue;

            const price = Number(car.price);
            const avg = Number(stats.avg_price);
            if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(avg) || avg <= 0) {
                continue;
            }
            const deltaPct = Math.round(((price - avg) / avg) * 1000) / 10;
            return {
                avg_price_display: "$" + Math.round(avg).toLocaleString(),
                delta_pct: deltaPct,
                vs_market: deltaPct <= -3 ? "below_market" : deltaPct >= 3 ? "above_market" : "near_market",
                sample_count: stats.sample_count,
            };
        }
        return null;
    }

    function enrichCarWithMarket(car) {
        if (!window.__DS_MARKET_STATS || !car || typeof car !== "object") return car;
        if (car.market) return car;
        const market = marketIntelForCar(car);
        if (market) car.market = market;
        return car;
    }

    function enrichCarsWithMarket(cars) {
        if (!window.__DS_MARKET_STATS) return cars;
        return cars.map((c) => enrichCarWithMarket(c));
    }

    let _marketStatsReloadTimer = null;
    window.__DS_reloadMarketStats = function reloadMarketStats() {
        const el = document.getElementById("ds-listings-premium");
        if (!el) return Promise.resolve();
        let premium = false;
        try {
            premium = JSON.parse(el.textContent || "false");
        } catch (_) {}
        if (!premium) return Promise.resolve();

        const qs = new URLSearchParams();
        const zip = typeof SC.scalarVal === "function" ? SC.scalarVal("zip_code") : "";
        const radius = typeof SC.scalarVal === "function" ? SC.scalarVal("radius") : "";
        if (zip) qs.set("zip_code", zip.trim());
        if (radius) qs.set("radius", radius);

        const url = "/api/listings/market-stats" + (qs.toString() ? "?" + qs.toString() : "");
        return fetch(url, { credentials: "same-origin" })
            .then((r) => (r.ok ? r.json() : null))
            .then((data) => {
                if (!data || !data.ok || !data.cohorts) return;
                window.__DS_MARKET_STATS = {
                    cohorts: data.cohorts,
                    geo_label: data.geo_label || "",
                    min_samples: data.min_samples,
                    year_window: data.year_window,
                };
                if (typeof window.__DS_refreshListingsMarketBadges === "function") {
                    window.__DS_refreshListingsMarketBadges();
                }
            })
            .catch(() => {});
    };

    function scheduleReloadMarketStats() {
        clearTimeout(_marketStatsReloadTimer);
        _marketStatsReloadTimer = setTimeout(() => {
            const run = () => {
                if (typeof window.__DS_reloadMarketStats === "function") {
                    window.__DS_reloadMarketStats();
                }
            };
            if (typeof requestIdleCallback === "function") {
                requestIdleCallback(run, { timeout: 600 });
            } else {
                run();
            }
        }, 150);
    }

    SC.marketIntelForCar = marketIntelForCar;
    SC.enrichCarWithMarket = enrichCarWithMarket;
    SC.enrichCarsWithMarket = enrichCarsWithMarket;
    SC.scheduleReloadMarketStats = scheduleReloadMarketStats;
})(window.SC = window.SC || {});
