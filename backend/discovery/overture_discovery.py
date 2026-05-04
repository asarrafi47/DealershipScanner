"""
US car dealerships from Overture Maps **Places** (GeoParquet on S3), queried with DuckDB.

Release discovery uses the public STAC root catalog (``latest`` field), not hardcoded paths.

Schema notes (as of 2026-04 Overture Places):
  - ``categories`` is a struct with ``primary`` and ``alternate`` (there is no ``main`` key).
    We treat ``primary = 'car_dealer'`` or ``'car_dealer'`` in ``alternate`` as a match.
  - ``geometry`` is already a ``GEOMETRY`` column; use ``ST_X`` / ``ST_Y`` directly. If a future
    release stores WKB blobs instead, wrap with ``ST_GeomFromWKB(CAST(geometry AS BLOB))``.
  - DuckDB struct/list fields use **1-based** indexing (e.g. ``addresses[1]``, ``websites[1]``).
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import duckdb

logger = logging.getLogger(__name__)

STAC_CATALOG_URL_DEFAULT = "https://stac.overturemaps.org/catalog.json"
OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT = "s3://overturemaps-us-west-2/release"

# Major OEM / franchise brand tokens for manifest merge (word-boundary matching in :func:`is_franchised_dealer`).
OEM_BRANDS = [
    "Acura",
    "Alfa Romeo",
    "Aston Martin",
    "Audi",
    "Bentley",
    "BMW",
    "Buick",
    "Cadillac",
    "Chevrolet",
    "Chevy",
    "Chrysler",
    "Dodge",
    "Ferrari",
    "Fiat",
    "Ford",
    "Genesis",
    "GMC",
    "Honda",
    "Hyundai",
    "Infiniti",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lotus",
    "Lucid",
    "Maserati",
    "Mazda",
    "McLaren",
    "Mercedes",
    "Mercedes-Benz",
    "Mini",
    "Mitsubishi",
    "Nissan",
    "Polestar",
    "Porsche",
    "Ram",
    "Rivian",
    "Rolls-Royce",
    "Subaru",
    "Tesla",
    "Toyota",
    "Volkswagen",
    "VW",
    "Volvo",
]


def _compile_oem_brand_pattern() -> re.Pattern[str]:
    """Longest brands first so e.g. ``Mercedes-Benz`` wins over ``Mercedes``."""
    escaped = [re.escape(b) for b in sorted(OEM_BRANDS, key=len, reverse=True)]
    return re.compile(r"\b(?:" + "|".join(escaped) + r")\b", re.IGNORECASE)


_OEM_BRAND_WORD_RE = _compile_oem_brand_pattern()


def is_franchised_dealer(dealer_name: str | None) -> bool:
    """
    Return True if ``dealer_name`` contains a known OEM/franchise brand as a whole word.

    Uses case-insensitive regex with ``\\b`` boundaries so substrings like ``Oxford`` do not match ``Ford``,
    and ``Macchia`` does not match ``Kia``.
    """
    if dealer_name is None:
        return False
    s = str(dealer_name).strip()
    if not s:
        return False
    return _OEM_BRAND_WORD_RE.search(s) is not None


# Overture release folder names, e.g. "2026-04-15.0"
_RELEASE_ID_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.\d+$")


def _validate_release_id(release_id: str) -> str:
    rid = (release_id or "").strip()
    if not _RELEASE_ID_RE.match(rid):
        raise ValueError(f"Invalid Overture release id (expected YYYY-MM-DD.N): {release_id!r}")
    return rid


def connect_overture_duckdb() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with ``httpfs`` and ``spatial`` loaded (S3 + GeoParquet geometry)."""
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    # Public bucket lives in us-west-2; anonymous reads work without credentials.
    con.execute("SET s3_region='us-west-2';")
    return con


def fetch_latest_release_id(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog_url: str = STAC_CATALOG_URL_DEFAULT,
) -> str:
    """Read the ``latest`` release folder name from the STAC root catalog JSON."""
    row = con.execute(
        "SELECT latest FROM read_json_auto(?)",
        [catalog_url.strip()],
    ).fetchone()
    if not row or row[0] is None or not str(row[0]).strip():
        raise RuntimeError(f"STAC catalog has no usable 'latest' field: {catalog_url!r}")
    return _validate_release_id(str(row[0]).strip())


def places_type_place_glob(release_id: str, *, s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT) -> str:
    """Glob for all Places ``type=place`` Parquet parts for a release."""
    rid = _validate_release_id(release_id)
    base = s3_release_prefix.rstrip("/")
    return f"{base}/{rid}/theme=places/type=place/*.parquet"


def sql_us_car_dealers_select(parquet_glob: str) -> str:
    """
    Baseline query aligned with Overture Places + DuckDB conventions.

    ``parquet_glob`` must be a trusted literal (validated release id is interpolated only via
    :func:`places_type_place_glob`).
    """
    # parquet_glob is inserted only after path validation above — keep as single-quoted literal.
    escaped = parquet_glob.replace("'", "''")
    return f"""
SELECT
    id AS overture_id,
    names.primary AS name,
    websites[1] AS website,
    addresses[1].freeform AS address,
    addresses[1].locality AS city,
    addresses[1].region AS state,
    addresses[1].postcode AS zip_code,
    ST_X(geometry) AS longitude,
    ST_Y(geometry) AS latitude
FROM read_parquet('{escaped}', hive_partitioning = true)
WHERE (
    categories.primary = 'car_dealer'
    OR (
      categories.alternate IS NOT NULL
      AND list_contains(categories.alternate, 'car_dealer')
    )
  )
  AND addresses[1].country = 'US'
"""


@dataclass(frozen=True)
class OvertureCarDealerRow:
    """One US car-dealer place row from Overture."""

    overture_id: str
    name: str | None
    website: str | None
    address: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    longitude: float | None
    latitude: float | None

    @classmethod
    def from_mapping(cls, m: Mapping[str, Any]) -> OvertureCarDealerRow:
        return cls(
            overture_id=str(m.get("overture_id") or ""),
            name=m.get("name"),
            website=m.get("website"),
            address=m.get("address"),
            city=m.get("city"),
            state=m.get("state"),
            zip_code=m.get("zip_code"),
            longitude=_maybe_float(m.get("longitude")),
            latitude=_maybe_float(m.get("latitude")),
        )


def _maybe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _execute_to_mapping_rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    result = con.execute(sql)
    columns = [c[0] for c in result.description]
    out: list[dict[str, Any]] = []
    for tup in result.fetchall():
        out.append(dict(zip(columns, tup)))
    return out


def fetch_us_car_dealers_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    release_id: str | None = None,
    catalog_url: str = STAC_CATALOG_URL_DEFAULT,
    s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
    limit: int | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Load US ``car_dealer`` rows from the latest (or pinned) Overture Places release.

    Returns ``(resolved_release_id, rows_as_dicts)``.

    **Memory:** This materializes all matching rows in Python. For the full national extract,
    prefer :func:`copy_us_car_dealers_to_parquet` / ``COPY`` via the CLI so DuckDB streams to disk.
    """
    if release_id:
        rid = _validate_release_id(release_id.strip())
    else:
        rid = fetch_latest_release_id(con, catalog_url=catalog_url)
    glob_path = places_type_place_glob(rid, s3_release_prefix=s3_release_prefix)
    sql = sql_us_car_dealers_select(glob_path)
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be >= 0")
        sql = f"{sql}\nLIMIT {int(limit)}"
    logger.info("Querying Overture Places car_dealer US rows release=%s glob=%s", rid, glob_path)
    rows = _execute_to_mapping_rows(con, sql)
    return rid, rows


def _sql_single_quoted_literal(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


def export_us_car_dealers_sql(
    *,
    release_id: str,
    s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
    limit: int | None = None,
) -> str:
    """Build the full ``SELECT`` (optional ``LIMIT``) for COPY / external tooling."""
    rid = _validate_release_id(release_id)
    glob_path = places_type_place_glob(rid, s3_release_prefix=s3_release_prefix)
    sql = sql_us_car_dealers_select(glob_path)
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be >= 0")
        sql = f"{sql}\nLIMIT {int(limit)}"
    return sql


def copy_us_car_dealers_to_parquet(
    con: duckdb.DuckDBPyConnection,
    out_path: str,
    *,
    release_id: str | None = None,
    catalog_url: str = STAC_CATALOG_URL_DEFAULT,
    s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
    limit: int | None = None,
) -> str:
    """
    Run ``COPY (SELECT ...) TO out_path (FORMAT PARQUET)`` — streams via DuckDB (scalable).

    Returns resolved ``release_id``.
    """
    rid = _validate_release_id(release_id.strip()) if release_id else fetch_latest_release_id(con, catalog_url=catalog_url)
    inner = export_us_car_dealers_sql(release_id=rid, s3_release_prefix=s3_release_prefix, limit=limit)
    dest = _sql_single_quoted_literal(out_path)
    con.execute(f"COPY ({inner.strip()}) TO {dest} (FORMAT PARQUET)")
    return rid


def copy_us_car_dealers_to_csv(
    con: duckdb.DuckDBPyConnection,
    out_path: str,
    *,
    release_id: str | None = None,
    catalog_url: str = STAC_CATALOG_URL_DEFAULT,
    s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
    limit: int | None = None,
) -> str:
    """``COPY`` dealers to CSV (HEADER). Returns resolved ``release_id``."""
    rid = _validate_release_id(release_id.strip()) if release_id else fetch_latest_release_id(con, catalog_url=catalog_url)
    inner = export_us_car_dealers_sql(release_id=rid, s3_release_prefix=s3_release_prefix, limit=limit)
    dest = _sql_single_quoted_literal(out_path)
    con.execute(f"COPY ({inner.strip()}) TO {dest} (FORMAT CSV, HEADER, DELIMITER ',')")
    return rid


class OverturePlacesDealersQuery:
    """Stateful helper (catalog URL + S3 prefix) around :func:`connect_overture_duckdb`."""

    def __init__(
        self,
        *,
        catalog_url: str = STAC_CATALOG_URL_DEFAULT,
        s3_release_prefix: str = OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
    ) -> None:
        self.catalog_url = catalog_url
        self.s3_release_prefix = s3_release_prefix

    def fetch_all(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        release_id: str | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        return fetch_us_car_dealers_rows(
            con,
            release_id=release_id,
            catalog_url=self.catalog_url,
            s3_release_prefix=self.s3_release_prefix,
        )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export US car dealers from Overture Places via DuckDB.")
    p.add_argument(
        "--catalog-url",
        default=STAC_CATALOG_URL_DEFAULT,
        help="STAC root catalog URL (default: Overture public catalog)",
    )
    p.add_argument(
        "--release",
        default=None,
        help="Pin Overture release folder (e.g. 2026-04-15.0). Default: read `latest` from STAC.",
    )
    p.add_argument(
        "--s3-prefix",
        default=OVERTURE_S3_BUCKET_RELEASE_PREFIX_DEFAULT,
        help="S3 release prefix (default: Overture us-west-2 release bucket path)",
    )
    p.add_argument(
        "--output",
        "-o",
        default=None,
        help="Write Parquet to this path (e.g. ./data/us_car_dealers.parquet)",
    )
    p.add_argument(
        "--csv",
        default=None,
        help="Write CSV to this path instead of/in addition to Parquet",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit rows (for smoke tests only)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")
    con = connect_overture_duckdb()

    if args.output:
        rid = copy_us_car_dealers_to_parquet(
            con,
            args.output,
            release_id=args.release,
            catalog_url=args.catalog_url,
            s3_release_prefix=args.s3_prefix,
            limit=args.limit,
        )
        logger.info("Wrote Parquet %s (release %s)", args.output, rid)

    if args.csv:
        rid_csv = copy_us_car_dealers_to_csv(
            con,
            args.csv,
            release_id=args.release,
            catalog_url=args.catalog_url,
            s3_release_prefix=args.s3_prefix,
            limit=args.limit,
        )
        logger.info("Wrote CSV %s (release %s)", args.csv, rid_csv)

    if not args.output and not args.csv:
        lim = args.limit if args.limit is not None else 10
        rid, preview = fetch_us_car_dealers_rows(
            con,
            release_id=args.release,
            catalog_url=args.catalog_url,
            s3_release_prefix=args.s3_prefix,
            limit=lim,
        )
        logger.info("Resolved release %s — preview up to %s rows", rid, lim)
        for r in preview:
            print(r)
        print(
            "Tip: use --output file.parquet or --csv file.csv for the full national extract (streams via DuckDB).",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
