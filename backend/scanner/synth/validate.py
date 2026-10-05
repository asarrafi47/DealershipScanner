"""
Compatibility shim. :func:`validate_recipe` and its per-shape replay walkers
(``_validate_json_feed`` / ``_validate_dep`` / ``_validate_html_walk`` /
``_validate_cosmos``) moved verbatim into :mod:`backend.scanner.recipe_validation`
so there is one validation module. Patch names there (where the code looks them
up), not here.
"""
from __future__ import annotations

from backend.scanner.recipe_validation import (  # noqa: F401
    _VALIDATE_MAX_PAGES,
    _cosmos_get_json,
    _dep_fetch_html,
    _validate_cosmos,
    _validate_dep,
    _validate_html_walk,
    _validate_json_feed,
    validate_recipe,
)
