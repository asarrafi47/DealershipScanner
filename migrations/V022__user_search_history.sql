-- V022__user_search_history.sql
--
-- Per-user search history (account profile -> Recent searches). One row per
-- search a signed-in user ran on /listings (query text and/or filters) or via
-- POST /api/search/smart. filters_json is the same cleaned filter object the
-- saved_searches table stores (listings query-param shape), so a history row can
-- be re-run as a /listings URL or promoted to a saved search without conversion.
-- query_text duplicates filters_json.q for cheap display; result_count is NULL
-- when the grid was filtered client-side and the server never counted.
--
-- The repo dedupes identical filters within a 10-minute window and prunes each
-- user to the newest 100 rows, so the table stays bounded without a cron.
--
-- created_at is written by the application as an ISO-8601 UTC string (not the
-- engine default) so the dedupe window compares the same format on SQLite and
-- Postgres; the DEFAULT is only a safety net for hand-inserted rows.
--
-- No FK to users(id): every cross-database user_id column in this schema
-- (saved_cars.user_id, saved_searches.user_id, user_hidden_dealers.user_id) is
-- unenforced by design, since users.db is a separate SQLite store.

CREATE TABLE IF NOT EXISTS user_search_history (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    filters_json  TEXT NOT NULL,
    query_text    TEXT,
    result_count  INTEGER,
    created_at    TEXT DEFAULT (CURRENT_TIMESTAMP::text)
);

CREATE INDEX IF NOT EXISTS idx_user_search_history_user
    ON user_search_history (user_id, created_at DESC, id DESC);
