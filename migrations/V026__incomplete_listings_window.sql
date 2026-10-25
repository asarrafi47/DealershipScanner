-- V026__incomplete_listings_window.sql
--
-- N6.4 (D-N13): the incomplete index carries the car's first-seen date, its latest
-- scrape, how many scans the gap has survived, and a `pending` flag for a new
-- arrival (price/photo gap only, inside the window). Runtime copy:
-- backend/db/incomplete_listings_db.py::_ensure_window_columns.

ALTER TABLE public.incomplete_listings ADD COLUMN IF NOT EXISTS first_seen_at text;
ALTER TABLE public.incomplete_listings ADD COLUMN IF NOT EXISTS last_scraped_at text;
ALTER TABLE public.incomplete_listings ADD COLUMN IF NOT EXISTS scans_survived integer NOT NULL DEFAULT 1;
ALTER TABLE public.incomplete_listings ADD COLUMN IF NOT EXISTS pending integer NOT NULL DEFAULT 0;
