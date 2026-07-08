"""Deprecated alias — canonical module: backend.scanner.vdp.html_recovery.

The scanner restructure moved this module; both import paths must resolve to
the SAME module object (shared state, no copy drift), so this file replaces
itself in sys.modules with the canonical module.
"""
import sys

from backend.scanner.vdp import html_recovery as _impl

sys.modules[__name__] = _impl
