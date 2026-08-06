"""Model years a named rung actually existed for."""
from __future__ import annotations

import re

from .naming import (
    _resolve_trim_model_key,
)

def _trim_year_window_key(trim_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (trim_name or "").lower())


# Listing year must fall in one window for the trim to appear on the ladder.
# Single (year_min, year_max) or multiple eras: ((y0, y1), (y2, y3)).
TRIM_YEAR_WINDOWS: dict[tuple[str, str, str], tuple[int, ...]] = {
    # --- Dodge Charger LD (2015–2023) + FR (2024+) ---
    ("dodge", "charger", "srthellcatredeyewidebody"): (2020, 2023),
    ("dodge", "charger", "srthellcatredeye"): (2019, 2023),
    ("dodge", "charger", "srthellcat"): (2015, 2023),
    ("dodge", "charger", "hellcat"): (2015, 2023),
    ("dodge", "charger", "scatpackwidebody"): (2019, 2023),
    ("dodge", "charger", "scatpack"): (2015, 2023, 2024, 2030),
    ("dodge", "charger", "rtscatpack"): (2024, 2030),
    ("dodge", "charger", "daytonascatpack"): (2023, 2030),
    ("dodge", "charger", "daytona"): (2017, 2018, 2024, 2030),
    ("dodge", "charger", "gt"): (2017, 2023),
    ("dodge", "charger", "sxt"): (2015, 2023),
    ("dodge", "charger", "se"): (2015, 2019),
    # --- Dodge Challenger (shared performance trims) ---
    ("dodge", "challenger", "srthellcatredeyewidebody"): (2019, 2023),
    ("dodge", "challenger", "srthellcatredeye"): (2019, 2023),
    ("dodge", "challenger", "srthellcat"): (2015, 2023),
    ("dodge", "challenger", "hellcat"): (2015, 2023),
    ("dodge", "challenger", "scatpackwidebody"): (2019, 2023),
    ("dodge", "challenger", "scatpack"): (2015, 2023),
    ("dodge", "challenger", "rtscatpack"): (2015, 2016),
    ("dodge", "challenger", "srt392"): (2015, 2023),
    ("dodge", "challenger", "t/a"): (2017, 2018),
    ("dodge", "challenger", "gt"): (2017, 2023),
    ("dodge", "challenger", "sxt"): (2015, 2023),
    # --- Ram 1500 ---
    ("ram", "1500", "tungsten"): (2025, 2030),
    ("ram", "1500", "trx"): (2021, 2023),
    ("gmc", "yukon", "at4"): (2021, 2030),
    ("gmc", "yukon", "denaliultimate"): (2022, 2030),
    # --- Jeep Grand Cherokee WK2 / WL ---
    ("jeep", "grandcherokee", "trackhawk"): (2018, 2021),
    ("jeep", "grandcherokee", "srt"): (2012, 2021),
    ("jeep", "grandcherokee", "limitedx"): (2019, 2021),
    ("jeep", "grandcherokee", "summitreserve"): (2022, 2030),
    # --- BMW X5 (G05) ---
    ("bmw", "x5", "m60ixdrive"): (2024, 2030),
    ("bmw", "x5", "m60i"): (2024, 2030),
    ("bmw", "x5", "xdrive50i"): (2019, 2023),
    ("bmw", "x5", "m50i"): (2019, 2023),
    ("bmw", "x5", "xdrive45e"): (2021, 2030),
    ("bmw", "x5", "45e"): (2021, 2030),
    # --- BMW X6 (G06) ---
    ("bmw", "x6", "m60ixdrive"): (2024, 2030),
    ("bmw", "x6", "m60i"): (2024, 2030),
    ("bmw", "x6", "xdrive50i"): (2020, 2023),
    # --- Cadillac Escalade (5th gen) ---
    ("cadillac", "escalade", "vseries"): (2022, 2030),
}


def trim_step_year_windows(
    make: str,
    model: str | None,
    trim_name: str,
) -> list[tuple[int, int]]:
    """Return applicable model-year windows for a trim, or [] if unconstrained."""
    mk, mod = _resolve_trim_model_key(make, model)
    tok = _trim_year_window_key(trim_name)
    if not tok:
        return []
    raw = TRIM_YEAR_WINDOWS.get((mk, mod, tok))
    if not raw:
        return []
    if len(raw) == 2:
        return [(int(raw[0]), int(raw[1]))]
    out: list[tuple[int, int]] = []
    for i in range(0, len(raw) - 1, 2):
        out.append((int(raw[i]), int(raw[i + 1])))
    return out
