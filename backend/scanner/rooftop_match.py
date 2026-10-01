"""Deprecated alias — canonical module: backend.attribution.match.

The rooftop scorer moved into the attribution package (2026-10-01). Both import
paths must resolve to the SAME module object (shared state, monkeypatches hold),
so this file replaces itself in sys.modules with the canonical module.
"""
import sys

from backend.attribution import match as _impl

sys.modules[__name__] = _impl
