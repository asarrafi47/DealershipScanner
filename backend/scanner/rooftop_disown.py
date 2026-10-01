"""Shared rooftop-attribution reconcile policy for every scan path.

Canonical home since 2026-10-01: ``backend.attribution`` —
:mod:`backend.attribution.disown` (``EVIDENCE_BACKED_REJECTS``,
``split_refusals``, ``disown_foreign_rooftop_vins``) and
:mod:`backend.attribution.place` (``roster_place`` is ``registry_place``).
The names are re-exported here so existing imports keep working, and
``backend.attribution.place.store_place`` reads the registry through
``roster_place`` on THIS module, so stubbing it here still takes effect.
"""
from __future__ import annotations

from backend.attribution.disown import (
    EVIDENCE_BACKED_REJECTS,
    disown_foreign_rooftop_vins,
    split_refusals,
)
from backend.attribution.place import registry_place as roster_place

__all__ = [
    "EVIDENCE_BACKED_REJECTS",
    "disown_foreign_rooftop_vins",
    "roster_place",
    "split_refusals",
]
