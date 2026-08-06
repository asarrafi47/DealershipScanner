-- V003__dealership_address_provenance.sql
--
-- Provenance for dealerships.street_address / zip_code.
--
-- The rooftop attribution gate (backend/parsers/__init__.py) tells a store from
-- its siblings in a group feed by street address. On 2026-08-03 only 28 of the
-- 178 dealerships with active inventory carried a street_address, so the
-- street/zip tiers were inert and any group feed whose rooftops are unnamed
-- address blocks resolved to "target_rooftop_unidentified" -- a refusal to
-- store the whole payload.
--
-- Backfilling those addresses is only safe if a later reader can tell WHERE an
-- address came from. A wrong street address is worse than a blank one: blank
-- leaves the gate undecided (nothing is un-listed), wrong makes it confidently
-- attribute a payload to the wrong rooftop. These two columns record the tier
-- that produced the value and when it was confirmed, so a value written by a
-- weaker source can be re-checked or withdrawn without guessing.
--
-- Written by backend/scripts/backfill_dealership_addresses.py. Values used so
-- far: 'site_jsonld', 'site_jsonld_browser', 'site_text', 'site_text_browser',
-- 'osm_website'. Rows placed before this migration keep NULL, which reads as
-- "unknown origin", not as "verified".
--
-- Additive and idempotent.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_min_messages = warning;

ALTER TABLE dealerships ADD COLUMN IF NOT EXISTS street_address_source TEXT;
ALTER TABLE dealerships ADD COLUMN IF NOT EXISTS street_address_verified_at TEXT;
