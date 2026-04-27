#!/usr/bin/env python3
"""
Backfill interior_color via Ollama LLaVA (wrapper).

Preferred invocation from **repository root**::

    python -m backend.vision.analyze_images backfill-interior --dry-run

This script fixes ``sys.path`` then delegates to the unified module.
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backend.vision.analyze_images import main

if __name__ == "__main__":
    # ``python scripts/backfill_interior_from_vision.py …`` → forward ``backfill-interior`` if missing
    argv = sys.argv[1:]
    if argv and argv[0] not in (
        "interior",
        "classify",
        "filter-gallery",
        "monroney",
        "backfill-interior",
        "-h",
        "--help",
    ):
        argv = ["backfill-interior", *argv]
    main(argv)
