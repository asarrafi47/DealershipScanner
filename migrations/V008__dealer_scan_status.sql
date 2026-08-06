-- V008__dealer_scan_status.sql
--
-- Why each registered rooftop we cannot scan is not scannable, and what to do about it.
--
-- 593 rooftops are registered; 432 have live inventory. The other 161 are silent, and
-- until now "silent" was a single undifferentiated bucket -- which is why the same
-- dealers get re-scanned every sweep with the same nil result. They are not all the same
-- problem:
--
--   * 130 have a dealer_recipes row containing ZERO recipes: capture ran and found no
--     inventory endpoint. That is usually an unrecognised platform, or a site that
--     blocked the capture, or a domain that now redirects to a group site.
--   *  44 were never captured at all.
--   *   8 have working recipes that stopped returning anything.
--
-- Each of those wants a different remedy, so the diagnosis is recorded per dealer along
-- with a `remediation` recipe naming the concrete next step. The point is that a human
-- (or a later job) can work the list by class instead of re-running a scan that has
-- already failed the same way a dozen times.
--
-- One deliberate nuance in `reason`: an HTTP 403 is NOT recorded as "blocked". Dealer
-- Inspire's Cloudflare rejects bare HTTP clients by TLS fingerprint on every dealer,
-- including ones we have never touched, so a 403 says nothing about whether we are
-- banned or whether the dealer is reachable. Those are classed `needs_browser_probe`
-- and stay pending until something with a real browser fingerprint checks them.

CREATE TABLE IF NOT EXISTS dealer_scan_status (
    dealer_key        TEXT PRIMARY KEY,       -- hostname in dealer_id form
    dealer_name       TEXT,
    website_url       TEXT,

    -- Diagnosis, one of:
    --   dns_fail             domain does not resolve -- dealership likely gone
    --   redirect_offsite     resolves, but redirects to a different host (acquired/merged)
    --   http_error           reachable but returns 4xx/5xx (not 403)
    --   needs_browser_probe  403 / TLS-fingerprint rejection; verdict unknown by design
    --   reachable_no_recipe  site loads, no capture has ever been run
    --   reachable_empty_recipe  site loads, capture ran and found no endpoint
    --   recipe_decayed       had working recipes, now returns nothing
    reason            TEXT NOT NULL,

    -- The concrete next step for this class; see audit_unscannable_dealers.py.
    remediation       TEXT,

    http_status       INTEGER,
    final_url         TEXT,                   -- after redirects; reveals acquisitions
    platform_guess    TEXT,
    recipe_count      INTEGER NOT NULL DEFAULT 0,
    last_ok_at        TEXT,

    notes             TEXT,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The list is worked by class, so that is what gets filtered on.
CREATE INDEX IF NOT EXISTS idx_dealer_scan_status_reason
    ON dealer_scan_status (reason);
