"""
Vehicle facts: one implementation per rule that used to have many copies
(monolith audit 2026-10-01, enrich.md P1 #3).

* :func:`normalize_drivetrain` -- canonical FWD / RWD / AWD / 4WD or None
  (4x2 / 2WD / ambiguous -> None).
* :func:`electrification` -- ICE / HEV / PHEV / BEV / FCEV or None; NHTSA vPIC
  outranks the dealer feed, the EPA catalog never outranks either.
* :func:`epa_model_candidates` -- listing model -> EPA catalog model names, one
  named strategy per consumer (the four alias maps live in one module).
* :mod:`backend.vehicle_facts.extended_specs` -- the one cached reader of
  ``epa_extended_specs``.
"""
from __future__ import annotations

from backend.vehicle_facts.drivetrain import (
    CANONICAL_DRIVETRAINS,
    drivetrain_storage_value,
    is_two_wheel_unknown,
    normalize_drivetrain,
    same_drive_wheels,
)
from backend.vehicle_facts.electrification import (
    BEV,
    ELECTRIFICATION_VALUES,
    FCEV,
    HEV,
    ICE,
    LEGACY_CODE,
    PHEV,
    can_plug_in,
    electrification,
    electrification_from_text,
    fuel_label_electrification,
    is_battery_electric,
    vpic_electrification,
)
from backend.vehicle_facts.epa_model import epa_model_candidates
from backend.vehicle_facts import extended_specs

__all__ = [
    "BEV", "CANONICAL_DRIVETRAINS", "ELECTRIFICATION_VALUES", "FCEV", "HEV", "ICE", "LEGACY_CODE",
    "PHEV", "can_plug_in", "drivetrain_storage_value", "electrification", "electrification_from_text",
    "epa_model_candidates", "extended_specs", "fuel_label_electrification",
    "is_battery_electric", "is_two_wheel_unknown", "normalize_drivetrain", "same_drive_wheels",
    "vpic_electrification",
]
