-- V002__community.sql
--
-- Community layer: user-authored comments on a car and on a dealership, plus
-- a per-dealership Google Places rating cache.
--
-- Why new tables instead of widening ``dealer_reviews``: a review is one
-- rated, structured opinion per (dealer, user), edited in place. A comment is
-- free text, many-per-user, ordered by time, and exists for cars too --
-- squeezing both into one table would mean a nullable rating, a nullable
-- car_id, and a UNIQUE constraint that only applies to half the rows.
--
-- Why ``dealer_ratings`` when ``dealerships`` already carries google_rating /
-- google_review_count: those are an ad-hoc cache with no provenance ("which
-- API answered, and did it answer at all?"). This table records ``source`` and
-- ``fetched_at`` per row, so a stale or failed import is distinguishable from a
-- dealer that genuinely has no reviews. The importer keeps the legacy
-- ``dealerships`` columns mirrored so existing readers (the dealership
-- research page) do not have to change.
--
-- Every statement here is idempotent. The runtime also creates these tables on
-- demand (backend/db/comments_db.py) for a database that has not been migrated
-- yet, so this file must not fail against a database where they already exist.
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: car_comments; Type: TABLE; Schema: public; Owner: -
--
-- ``user_id`` has no FK: app users live in a separate SQLite database
-- (backend/db/users_db/), not in this Postgres instance.
--
-- ``ip_hash`` is a salted hash, never a raw address. It exists so abuse can be
-- traced across sessions and logins; the in-process limiter in
-- backend/utils/ip_rate_limit.py is per-worker memory that forgets everything
-- on restart, so it cannot be the only defence on a user-writable table.
--
-- Deletes are soft (``deleted_at``): a hard DELETE would let an author remove
-- a comment after it was flagged, destroying the moderation trail.
--

CREATE TABLE IF NOT EXISTS public.car_comments (
    id bigserial NOT NULL PRIMARY KEY,
    car_id bigint NOT NULL REFERENCES public.cars(id) ON DELETE CASCADE,
    user_id bigint NOT NULL,
    body text NOT NULL,
    ip_hash text,
    is_hidden boolean DEFAULT false NOT NULL,
    flag_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT car_comments_body_not_blank CHECK ((btrim(body) <> ''::text)),
    CONSTRAINT car_comments_body_length CHECK ((char_length(body) <= 4000)),
    CONSTRAINT car_comments_flag_count_nonneg CHECK ((flag_count >= 0))
);


--
-- Name: dealer_comments; Type: TABLE; Schema: public; Owner: -
--
-- Keyed to ``dealerships.id`` (the registry id), NOT to the hostname-derived
-- ``cars.dealer_id`` that ``dealer_reviews`` uses. The registry row is the
-- stable identity: a dealer that changes domains keeps its registry id but
-- gets a new dealer_id, which would orphan every comment written about it.
--

CREATE TABLE IF NOT EXISTS public.dealer_comments (
    id bigserial NOT NULL PRIMARY KEY,
    dealership_id bigint NOT NULL REFERENCES public.dealerships(id) ON DELETE CASCADE,
    user_id bigint NOT NULL,
    body text NOT NULL,
    ip_hash text,
    is_hidden boolean DEFAULT false NOT NULL,
    flag_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT dealer_comments_body_not_blank CHECK ((btrim(body) <> ''::text)),
    CONSTRAINT dealer_comments_body_length CHECK ((char_length(body) <= 4000)),
    CONSTRAINT dealer_comments_flag_count_nonneg CHECK ((flag_count >= 0))
);


--
-- Name: dealer_ratings; Type: TABLE; Schema: public; Owner: -
--
-- One current row per dealership (upserted), not an append-only history: the
-- importer is idempotent and re-running it must not grow the table.
--
-- A row with NULL rating and a non-NULL ``fetched_at`` is meaningful -- it
-- records "we asked Google and it had nothing", which is what stops the
-- importer from re-asking about the same dealer on every run.
--

CREATE TABLE IF NOT EXISTS public.dealer_ratings (
    dealership_id bigint NOT NULL PRIMARY KEY REFERENCES public.dealerships(id) ON DELETE CASCADE,
    google_place_id text,
    google_rating numeric(2,1),
    google_review_count integer,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL,
    source text DEFAULT 'google_places'::text NOT NULL,
    CONSTRAINT dealer_ratings_rating_range CHECK (((google_rating IS NULL) OR ((google_rating >= (0)::numeric) AND (google_rating <= (5)::numeric)))),
    CONSTRAINT dealer_ratings_review_count_nonneg CHECK (((google_review_count IS NULL) OR (google_review_count >= 0)))
);


--
-- Name: idx_car_comments_thread; Type: INDEX; Schema: public; Owner: -
--
-- The read path: visible comments for one car, newest first. Partial, so the
-- index never carries deleted or moderated-away rows.
--

CREATE INDEX IF NOT EXISTS idx_car_comments_thread ON public.car_comments USING btree (car_id, created_at DESC) WHERE ((deleted_at IS NULL) AND (is_hidden = false));


--
-- Name: idx_car_comments_user_recent; Type: INDEX; Schema: public; Owner: -
--
-- Ownership checks on delete, and the per-user posting-volume cap.
--

CREATE INDEX IF NOT EXISTS idx_car_comments_user_recent ON public.car_comments USING btree (user_id, created_at DESC);


--
-- Name: idx_car_comments_moderation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_car_comments_moderation ON public.car_comments USING btree (flag_count DESC, created_at DESC) WHERE (flag_count > 0);


--
-- Name: idx_dealer_comments_thread; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_dealer_comments_thread ON public.dealer_comments USING btree (dealership_id, created_at DESC) WHERE ((deleted_at IS NULL) AND (is_hidden = false));


--
-- Name: idx_dealer_comments_user_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_dealer_comments_user_recent ON public.dealer_comments USING btree (user_id, created_at DESC);


--
-- Name: idx_dealer_comments_moderation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_dealer_comments_moderation ON public.dealer_comments USING btree (flag_count DESC, created_at DESC) WHERE (flag_count > 0);


--
-- Name: idx_dealer_ratings_place; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS idx_dealer_ratings_place ON public.dealer_ratings USING btree (google_place_id);


--
-- Name: idx_dealer_ratings_fetched; Type: INDEX; Schema: public; Owner: -
--
-- "What is stale enough to refresh" -- the importer's work queue.
--

CREATE INDEX IF NOT EXISTS idx_dealer_ratings_fetched ON public.dealer_ratings USING btree (fetched_at);


--
-- V002 complete
--
