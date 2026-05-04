-- Multi-brand vehicle reference (normalized). BMW is the first populated brand.
-- Enable FK checks in application code: PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ref_brand (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_source (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL,
    url TEXT,
    source_group_key TEXT,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- One row per U.S.-market (unless noted) configuration row you would flatten to CSV.
CREATE TABLE IF NOT EXISTS ref_vehicle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id INTEGER NOT NULL REFERENCES ref_brand (id),
    model_year INTEGER NOT NULL,
    market TEXT NOT NULL DEFAULT 'US',
    series_name TEXT NOT NULL,
    variant_name TEXT,
    trim_line TEXT,
    body_style TEXT,
    engine TEXT,
    transmission TEXT,
    drivetrain TEXT,
    fuel_type TEXT,
    mpg_text TEXT,
    passenger_seating TEXT,
    uncertainty_notes TEXT,
    source_id INTEGER NOT NULL REFERENCES ref_source (id),
    external_source TEXT,
    external_record_id TEXT,
    internal_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_ref_vehicle_brand_year ON ref_vehicle (brand_id, model_year);
CREATE INDEX IF NOT EXISTS idx_ref_vehicle_series ON ref_vehicle (brand_id, series_name);

-- One row per upstream record (e.g. EPA vehicle id) when external_record_id is set.
CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_vehicle_brand_external
ON ref_vehicle (brand_id, external_source, external_record_id)
WHERE external_record_id IS NOT NULL AND external_source IS NOT NULL;

CREATE TABLE IF NOT EXISTS ref_package (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_code TEXT,
    package_name TEXT NOT NULL,
    package_details TEXT,
    source_id INTEGER NOT NULL REFERENCES ref_source (id)
);

CREATE TABLE IF NOT EXISTS ref_vehicle_package (
    vehicle_id INTEGER NOT NULL REFERENCES ref_vehicle (id) ON DELETE CASCADE,
    package_id INTEGER NOT NULL REFERENCES ref_package (id) ON DELETE CASCADE,
    sort_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (vehicle_id, package_id)
);

CREATE TABLE IF NOT EXISTS ref_exterior_color (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    color_name TEXT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES ref_source (id)
);

CREATE TABLE IF NOT EXISTS ref_vehicle_exterior (
    vehicle_id INTEGER NOT NULL REFERENCES ref_vehicle (id) ON DELETE CASCADE,
    exterior_color_id INTEGER NOT NULL REFERENCES ref_exterior_color (id) ON DELETE CASCADE,
    PRIMARY KEY (vehicle_id, exterior_color_id)
);

CREATE TABLE IF NOT EXISTS ref_interior_color (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    color_name TEXT NOT NULL,
    source_id INTEGER NOT NULL REFERENCES ref_source (id)
);

CREATE TABLE IF NOT EXISTS ref_vehicle_interior (
    vehicle_id INTEGER NOT NULL REFERENCES ref_vehicle (id) ON DELETE CASCADE,
    interior_color_id INTEGER NOT NULL REFERENCES ref_interior_color (id) ON DELETE CASCADE,
    PRIMARY KEY (vehicle_id, interior_color_id)
);

-- Tracks which product lines exist for coverage / gap reporting (no fabricated specs).
CREATE TABLE IF NOT EXISTS ref_coverage_line (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id INTEGER NOT NULL REFERENCES ref_brand (id),
    series_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    line_category TEXT,
    first_model_year_us INTEGER,
    last_model_year_us INTEGER,
    year_range_uncertain INTEGER NOT NULL DEFAULT 1,
    coverage_status TEXT NOT NULL DEFAULT 'pending',
    notes TEXT,
    source_id INTEGER REFERENCES ref_source (id),
    UNIQUE (brand_id, series_key)
);

CREATE INDEX IF NOT EXISTS idx_ref_coverage_line_brand ON ref_coverage_line (brand_id);
