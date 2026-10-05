"""Monkeypatch helper for the dealer pipeline (backend/scanner/pipeline/).

``backend/scripts/dealer_pipeline.py`` is a facade since the audit F11 split:
its names are re-exports, and the code that looks them up lives in the
``backend.scanner.pipeline.*`` modules, each with its OWN binding (``from .runner
import run_http_only_scan`` copies the reference). A ``monkeypatch.setattr(dp,
name, stub)`` on the facade alone therefore patches nothing the moved code
reads. :func:`patch_pipeline` patches the facade and every pipeline module that
binds *name*, so every lookup site sees the stub, exactly as the single
namespace did before the split. It fails when nothing binds the name, so a
renamed function cannot turn a patch into a silent no-op.
"""
from __future__ import annotations

import sys
from typing import Any


def pipeline_modules() -> list[Any]:
    from backend.scripts import dealer_pipeline as dp

    mods = [dp]
    for name, mod in sorted(sys.modules.items()):
        if mod is not None and (name == "backend.scanner.pipeline" or name.startswith("backend.scanner.pipeline.")):
            mods.append(mod)
    return mods


def patch_pipeline(monkeypatch, name: str, value: Any) -> int:
    """Set *name* to *value* on every pipeline module that binds it. Returns the count."""
    hits = 0
    for mod in pipeline_modules():
        if name in vars(mod):
            monkeypatch.setattr(mod, name, value)
            hits += 1
    assert hits, f"no dealer pipeline module binds {name!r}"
    return hits
