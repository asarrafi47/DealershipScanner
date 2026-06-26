-- Normalize empty JSON array sentinels on cars (TEXT columns) to SQL NULL.
--
-- Run against Postgres inventory (dealership_scanner):
--   psql -d dealership_scanner -f backend/scripts/sql/normalize_empty_json_arrays_to_null.sql
--
-- Safe to re-run: only rows with literal '[]' (after trim) are updated.

BEGIN;

UPDATE cars
SET gallery = NULL
WHERE TRIM(COALESCE(gallery, '')) = '[]';

UPDATE cars
SET history_highlights = NULL
WHERE TRIM(COALESCE(history_highlights, '')) = '[]';

UPDATE cars
SET interior_color_buckets = NULL
WHERE TRIM(COALESCE(interior_color_buckets, '')) = '[]';

UPDATE cars
SET packages = NULL
WHERE TRIM(COALESCE(packages, '')) = '[]';

COMMIT;

-- Verification (expect 0 for each column after migration)
SELECT 'gallery' AS column_name, COUNT(*) AS empty_array_rows
FROM cars WHERE TRIM(COALESCE(gallery, '')) = '[]'
UNION ALL
SELECT 'history_highlights', COUNT(*)
FROM cars WHERE TRIM(COALESCE(history_highlights, '')) = '[]'
UNION ALL
SELECT 'interior_color_buckets', COUNT(*)
FROM cars WHERE TRIM(COALESCE(interior_color_buckets, '')) = '[]'
UNION ALL
SELECT 'packages', COUNT(*)
FROM cars WHERE TRIM(COALESCE(packages, '')) = '[]';
