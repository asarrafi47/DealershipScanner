"""Deprecated alias — canonical module: backend.scanner.dealer.location.

The scanner restructure moved this module; both import paths must resolve to
the SAME module object (shared state, no copy drift), so this file replaces
itself in sys.modules with the canonical module.
"""
import sys

from backend.scanner.dealer import location as _impl

sys.modules[__name__] = _impl
