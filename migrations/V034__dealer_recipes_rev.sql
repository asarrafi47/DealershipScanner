-- V034__dealer_recipes_rev.sql
--
-- N13 P7.1 (D-RS3): dealer_recipes becomes compare-and-set. Every write goes through
-- backend/scanner/recipe_store.py (db_mutate / mutate_scan_hints) as
--   UPDATE ... SET ..., rev = rev + 1 WHERE dealer_id = ? AND rev = ?
-- and counts the rows it changed, so two hosts (the MBP lane, Railway) can no longer
-- overwrite each other's recipes silently.
--
--   rev                 bumped by every write (recipes or scan_hints)
--   recipes_written_at  epoch seconds, bumped ONLY by a recipe write; 0 = never. A row is
--                       authoritative for its recipes only when this is > 0 (hints-only
--                       rows hold '[]' and are not)
--   superseded_json     archive of replaced recipe sets (P7.5 fills it)
--
-- The plan assigned V031; the next free number on the integration head is V034.
-- Runtime copy: none. recipe_store checks information_schema and never ALTERs.
--
-- Rows that already hold recipes are stamped from their own max_saved_at so they are
-- authoritative from the start (a ~700-row UPDATE).

ALTER TABLE public.dealer_recipes ADD COLUMN IF NOT EXISTS rev bigint NOT NULL DEFAULT 0;
ALTER TABLE public.dealer_recipes ADD COLUMN IF NOT EXISTS recipes_written_at double precision NOT NULL DEFAULT 0;
ALTER TABLE public.dealer_recipes ADD COLUMN IF NOT EXISTS superseded_json text NOT NULL DEFAULT '[]';

UPDATE public.dealer_recipes
   SET recipes_written_at = max_saved_at
 WHERE recipes_written_at = 0 AND recipe_count > 0 AND max_saved_at > 0;
