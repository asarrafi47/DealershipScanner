-- V005__comment_attachments.sql
--
-- Photo attachments for car and dealership comments.
--
-- V002 gave car_comments/dealer_comments a text-only body and
-- backend/routes/community_api.py exposed it at
-- POST /api/{cars,dealerships}/<id>/comments. This table is the durable half of
-- the upload path added alongside it: the bytes live on disk under
-- <repo_root>/comment_uploads (backend/utils/comment_images.py), and one row
-- here per stored file is what survives a restart and what the serving route
-- resolves an id against.
--
-- Scope-polymorphic on purpose, the same arrangement V004 uses for
-- comment_flags: car_comments.id and dealer_comments.id are independent
-- sequences, so (scope, comment_id) is the only key that identifies a thread
-- entry, and no single foreign key can point at both parents. Deletion is
-- handled by backend/db/comments_db.delete_attachments_for_comment, which the
-- DELETE endpoint calls in the same request that soft-deletes the comment.
--
-- storage_key is the ONLY path-ish value stored, and it is generated
-- server-side: "<2 hex>/<32 hex>.<jpg|png|webp>" from secrets.token_hex. The
-- user-supplied filename is never stored and never reaches the filesystem. The
-- UNIQUE constraint is what makes a collision a failed INSERT rather than one
-- comment's photo silently overwriting another's.
--
-- width/height/byte_size describe the RE-ENCODED file, not the upload. Uploads
-- are decoded with Pillow and written back out from raw pixel data, which is
-- what strips EXIF (including GPS) and neutralizes polyglot payloads; the
-- original bytes are never stored, so these columns describe what is actually
-- on disk and let the client reserve layout space without reading the file.
--
-- sort_order (not "position": that is a SQL function name and reads badly in
-- both engines) preserves the order the composer sent, capped at
-- comment_images.MAX_ATTACHMENTS_PER_COMMENT.
--
-- Additive and idempotent.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS comment_attachments (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL,
    comment_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    mime_type TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT,
    deleted_at TEXT,
    CONSTRAINT comment_attachments_scope_known CHECK (scope IN ('car', 'dealer')),
    CONSTRAINT comment_attachments_mime_allowed
        CHECK (mime_type IN ('image/jpeg', 'image/png', 'image/webp')),
    CONSTRAINT comment_attachments_dims_positive CHECK (width > 0 AND height > 0),
    CONSTRAINT comment_attachments_size_positive CHECK (byte_size > 0),
    CONSTRAINT comment_attachments_sort_order_nonneg CHECK (sort_order >= 0)
);

-- The read path: every live attachment for one thread entry, in composer order.
CREATE INDEX IF NOT EXISTS idx_comment_attachments_comment
    ON comment_attachments (scope, comment_id, sort_order)
    WHERE (deleted_at IS NULL);

-- Ownership checks and any future per-user upload-volume cap.
CREATE INDEX IF NOT EXISTS idx_comment_attachments_user
    ON comment_attachments (user_id, created_at DESC);
