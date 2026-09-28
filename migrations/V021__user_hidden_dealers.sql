-- V021__user_hidden_dealers.sql
--
-- Per-user "never show me this dealership" list (account profile -> Hidden
-- dealerships). dealer_id is the hostname-derived ``cars.dealer_id`` key
-- (``longotoyota-com``), the same value the dealership research page routes on,
-- so exclusion is a plain ``dealer_id NOT IN (...)`` against cars. dealer_name
-- is a display snapshot taken at hide time so the profile list renders without
-- a join back to cars/dealerships.
--
-- No FK to users(id): every cross-database user_id column in this schema
-- (saved_cars.user_id, saved_searches.user_id, ...) is unenforced by design,
-- since users.db is a separate SQLite store from the inventory Postgres database.

CREATE TABLE IF NOT EXISTS user_hidden_dealers (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    dealer_id   TEXT NOT NULL,
    dealer_name TEXT,
    created_at  TEXT DEFAULT (CURRENT_TIMESTAMP::text),
    UNIQUE (user_id, dealer_id)
);

CREATE INDEX IF NOT EXISTS idx_user_hidden_dealers_user
    ON user_hidden_dealers (user_id, created_at);
