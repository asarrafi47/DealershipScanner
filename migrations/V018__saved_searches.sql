-- V018__saved_searches.sql
--
-- Persistence for FEATURE_SAVED_SEARCHES (backend/billing/catalog.py). A saved
-- search is the listings page's current filter state (the same query-string
-- shape main.js already builds for syncUrl()/shareable links), stored so a
-- signed-in user can come back to it without re-entering filters.
--
-- last_notified_at exists for a future alerting cron (new/changed listings
-- matching a saved search) but nothing writes it yet -- that cron is out of
-- scope for this migration; the column just avoids a second migration later.
--
-- No FK to users(id): every cross-database user_id column in this schema
-- (saved_cars.user_id, users.org_id, ...) is unenforced by design, since
-- users.db is a separate SQLite store from the inventory Postgres database.

CREATE TABLE IF NOT EXISTS saved_searches (
    id                BIGSERIAL PRIMARY KEY,
    user_id           BIGINT NOT NULL,
    filters_json      TEXT NOT NULL,
    created_at        TEXT DEFAULT (CURRENT_TIMESTAMP::text),
    last_notified_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_saved_searches_user
    ON saved_searches (user_id, created_at);
