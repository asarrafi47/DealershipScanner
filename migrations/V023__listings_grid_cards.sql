-- V023__listings_grid_cards.sql
--
-- Persisted listings-grid cards (owner decision 2026-09-28: the listings page asks
-- for the shopper's ZIP + radius only). A card is serialize_car_for_listings_grid()
-- output for one car, stored as JSON so GET /api/listings/cars?zip=&radius= is SQL
-- plus string concatenation instead of ~0.9 ms of Python per car. See
-- backend/db/repositories/grid_cards_repo.py for the freshness rules:
--
--   row_ver      cars.xmin when the card was built (fast "row unchanged" check)
--   content_key  digest of the grid columns the card was built from
--   aux_key      serializer revision + attribution verdict + public-incomplete flag
--   built_day    UTC day number; cards older than a day are refreshed in background
--   day_dep      1 when the card carries a date-relative field (price_drop_days_ago)
--   sort_bucket / sort_price   listing_sort_key_by_price, so the grid order needs no parse
--
-- No FK to cars(id): cards of sold/removed cars are never served (the scoped query
-- starts from active cars) and cost one row each until the builder prunes them.
-- The web process also creates this table lazily (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS listings_grid_cards (
    car_id       BIGINT PRIMARY KEY,
    row_ver      TEXT,
    content_key  TEXT NOT NULL,
    aux_key      TEXT NOT NULL,
    built_day    INTEGER NOT NULL,
    day_dep      INTEGER NOT NULL DEFAULT 0,
    sort_bucket  INTEGER NOT NULL DEFAULT 0,
    sort_price   DOUBLE PRECISION,
    card_json    TEXT NOT NULL
);
