"""
Permanent key for ``epa_master`` rows (Phase 10A, D-ER2). Pure: no IO.

``natural_key(row)`` is ``vid:<epaId>`` when the EPA vehicle id is known
(vehicles.csv ``id`` is unique per row). Rows without one get a normalized
content key over (year, make, model, trim, engine_description, trany, drive,
displacement, cylinders, fuel_type) plus an ordinal ``#n`` that is assigned
ONCE (at insert or backfill), stored, and never recomputed. engine_description
is part of the key, so it is never an updatable column.

Rows come in two shapes, both accepted: CSV-typed (vehicles.csv headers: ``id``,
``displ``, ``fuelType``, ``eng_dscr``, ``VClass``) and DB-typed (``epa_master``
columns: ``epa_vehicle_id``, ``displacement``, ``fuel_type``,
``engine_description``, ``body_style``).

Duplicate groups (same base key) are paired CSV-to-DB by value with
``match_rows``, never by ordinal position: 236 groups are ambiguous on
(body_style, city08, highway08) alone.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

# canonical field -> accepted spellings, first present wins
_ALIASES: dict[str, tuple[str, ...]] = {
    "vid": ("epa_vehicle_id", "epaId", "epa_id", "id"),
    "year": ("year",),
    "make": ("make",),
    "model": ("model",),
    "trim": ("trim",),
    "engine_description": ("engine_description", "eng_dscr", "engDscr"),
    "trany": ("trany", "transmission"),
    "drive": ("drive", "drivetrain"),
    "displacement": ("displacement", "displ"),
    "cylinders": ("cylinders", "cyl"),
    "fuel_type": ("fuel_type", "fuelType"),
    "body_style": ("body_style", "VClass"),
    "city08": ("city08",),
    "highway08": ("highway08",),
    "engine_display": ("engine_display", "engineDisplay"),
    "forced_induction": ("forced_induction", "forcedInduction"),
}

BASE_KEY_FIELDS = (
    "year",
    "make",
    "model",
    "trim",
    "engine_description",
    "trany",
    "drive",
    "displacement",
    "cylinders",
    "fuel_type",
)
# exact-match tuple, strongest first (value-based pairing)
MATCH_FIELDS = ("body_style", "city08", "highway08", "engine_display", "forced_induction")
_NUMERIC_MATCH = ("city08", "highway08")


def _get(row: Mapping[str, Any], field: str) -> Any:
    for name in _ALIASES[field]:
        if name in row and row[name] is not None:
            return row[name]
    return None


def _norm_part(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (int, float)):
        return "%g" % value
    s = str(value).strip()
    if not s:
        return ""
    try:  # CSV strings of numbers must equal DB numbers: "2.0" == 2.0
        return "%g" % float(s)
    except ValueError:
        return " ".join(s.lower().split())


def epa_vehicle_id(row: Mapping[str, Any]) -> int | None:
    v = _get(row, "vid")
    if v is None or str(v).strip() == "":
        return None
    try:
        n = int(float(str(v).strip()))
    except ValueError:
        return None
    return n if n > 0 else None


def base_key(row: Mapping[str, Any]) -> str:
    """Normalized content key without the ordinal; same for CSV- and DB-typed rows."""
    return "|".join(_norm_part(_get(row, f)) for f in BASE_KEY_FIELDS)


def format_key(base: str, ordinal: int) -> str:
    return f"{base}#{ordinal}"


def split_key(key: str) -> tuple[str, int | None]:
    """``(base, ordinal)``; ordinal is None for ``vid:`` keys."""
    if key.startswith("vid:"):
        return key, None
    base, sep, n = key.rpartition("#")
    if sep and n.isdigit():
        return base, int(n)
    return key, None


def natural_key(row: Mapping[str, Any], ordinal: int = 1) -> str:
    """Key for one row: ``vid:<id>``, else ``<base key>#<ordinal>``.

    ``ordinal`` matters only without an EPA id, and is the stored value from the
    first assignment (see ``assign_natural_keys``), never a recomputed position.
    """
    vid = epa_vehicle_id(row)
    if vid is not None:
        return f"vid:{vid}"
    return format_key(base_key(row), ordinal)


def assign_natural_keys(
    rows: Sequence[Mapping[str, Any]],
    taken: Iterable[str] = (),
) -> list[str]:
    """Keys for *rows* in order; the one place ordinals are minted.

    A row that already carries a non-empty ``natural_key`` keeps it. A new row in a
    base-key group gets one above the highest ordinal ever used in that group
    (``taken`` plus keys minted here), so a deleted row's ordinal is not reused.
    """
    top: dict[str, int] = {}
    used: set[str] = set()

    def note(key: str) -> None:
        used.add(key)
        base, n = split_key(key)
        if n is not None:
            top[base] = max(top.get(base, 0), n)

    for key in taken:
        note(key)
    for row in rows:
        existing = str(row.get("natural_key") or "").strip()
        if existing:
            note(existing)
    out: list[str] = []
    for row in rows:
        existing = str(row.get("natural_key") or "").strip()
        if existing:
            out.append(existing)
            continue
        vid = epa_vehicle_id(row)
        if vid is not None:
            key = f"vid:{vid}"
        else:
            base = base_key(row)
            key = format_key(base, top.get(base, 0) + 1)
        note(key)
        out.append(key)
    return out


def _match_tuple(row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_norm_part(_get(row, f)) for f in MATCH_FIELDS)


def _distance(a: Mapping[str, Any], b: Mapping[str, Any]) -> float:
    d = 0.0
    for f in MATCH_FIELDS:
        x, y = _get(a, f), _get(b, f)
        if f in _NUMERIC_MATCH:
            try:
                d += abs(float(x) - float(y))
                continue
            except (TypeError, ValueError):
                pass
        d += 0.0 if _norm_part(x) == _norm_part(y) else 1.0
    return d


def match_rows(
    csv_rows: Sequence[Mapping[str, Any]],
    db_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    """Pair CSV rows with DB rows by value. Pure and deterministic.

    Returns ``(pairs, csv_unmatched, db_unmatched)`` of indexes. Only rows with the
    same ``base_key`` pair. Inside a group, exact ``MATCH_FIELDS`` equality pairs
    first (in input order among equals), then the rest are assigned by smallest
    value distance (ties by index). Position never decides a pairing.
    """
    groups_db: dict[str, list[int]] = {}
    for j, r in enumerate(db_rows):
        groups_db.setdefault(base_key(r), []).append(j)
    groups_csv: dict[str, list[int]] = {}
    for i, r in enumerate(csv_rows):
        groups_csv.setdefault(base_key(r), []).append(i)

    pairs: list[tuple[int, int]] = []
    csv_left: list[int] = []
    db_left: list[int] = []
    for base in sorted(set(groups_csv) | set(groups_db)):
        ci = list(groups_csv.get(base, []))
        dj = list(groups_db.get(base, []))
        # pass 1: exact tuple
        by_tuple: dict[tuple[str, ...], list[int]] = {}
        for j in dj:
            by_tuple.setdefault(_match_tuple(db_rows[j]), []).append(j)
        rest_c: list[int] = []
        for i in ci:
            bucket = by_tuple.get(_match_tuple(csv_rows[i]))
            if bucket:
                pairs.append((i, bucket.pop(0)))
            else:
                rest_c.append(i)
        rest_d = sorted(j for b in by_tuple.values() for j in b)
        # pass 2: nearest values, globally smallest distance first
        cand = sorted(
            (_distance(csv_rows[i], db_rows[j]), i, j) for i in rest_c for j in rest_d
        )
        done_c: set[int] = set()
        done_d: set[int] = set()
        for _, i, j in cand:
            if i in done_c or j in done_d:
                continue
            pairs.append((i, j))
            done_c.add(i)
            done_d.add(j)
        csv_left += [i for i in rest_c if i not in done_c]
        db_left += [j for j in rest_d if j not in done_d]
    pairs.sort()
    return pairs, sorted(csv_left), sorted(db_left)
