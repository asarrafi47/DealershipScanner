"""Generic ladder steps when no make/model ladder is known."""
from __future__ import annotations


from .naming import (
    _model_trim_order,
    _resolve_trim_model_key,
)
from .tables import (
    GENERIC_TRIM_ORDER,
    MAKE_TRIM_ORDER,
)

_TRUCK_MODEL_TOKENS = frozenset(
    {
        "tundra",
        "tacoma",
        "sequoia",
        "4runner",
        "highlander",
        "sienna",
        "landcruiser",
    }
)

# Chevrolet make-wide order is truck/sports-only (ZR1, High Country, etc.).
_CHEVROLET_MAKE_FALLBACK_MODEL_TOKENS = frozenset(
    {
        "silverado",
        "tahoe",
        "suburban",
        "corvette",
        "camaro",
        "colorado",
        "blazer",
        "express",
    }
)

def _make_fallback_trim_order(make: str, model: str) -> tuple[str, ...]:
    """Make-wide trim order when model-specific data is unavailable."""
    mk, mod = _resolve_trim_model_key(make, model)
    if mk == "bmw":
        return ()
    if mk == "toyota" and not any(tok in mod for tok in _TRUCK_MODEL_TOKENS):
        return ()
    if mk == "chevrolet" and not any(tok in mod for tok in _CHEVROLET_MAKE_FALLBACK_MODEL_TOKENS):
        return ()
    return MAKE_TRIM_ORDER.get(mk, ())


def generic_fallback_steps(make: str, model: str) -> list[dict[str, object]]:
    """Last-resort ladder so the VDP never shows an empty trim panel."""
    names = list(_model_trim_order(make, model)[:8])
    if len(names) < 2:
        names = list(_make_fallback_trim_order(make, model)[:8])
    if len(names) < 2:
        names = list(GENERIC_TRIM_ORDER[:5])
    return [
        {
            "name": name,
            "aliases": [],
            "adds": [],
        }
        for name in names
    ]
