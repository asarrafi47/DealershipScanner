"""Helpers shared by the extracted route modules in ``backend/routes/``.

Route modules extracted from ``backend/main.py`` follow two rules so existing
tests and reload semantics keep working:

1. Any name that tests monkeypatch on ``backend.main`` (``get_car_by_id``,
   ``_session_has_paid_access``, ``_nhtsa_recalls_lookup_payload``, env-derived
   rate-limit globals, ...) is resolved through the ``backend.main`` module
   object at request time (``main_module().<name>``) instead of being imported
   by value. Patching ``backend.main.X`` therefore still affects moved views.
2. ``importlib.reload(backend.main)`` re-executes main's import-time pipeline;
   attribute lookups on the module object always see the freshly computed
   values, so the moved views stay reload-safe.
"""

from __future__ import annotations

from flask import request

from backend.utils.client_ip import client_ip as _client_ip_from_request


def main_module():
    """Return the (fully imported) ``backend.main`` module.

    Lazy on purpose: ``backend.main`` imports the route modules while it is
    itself being imported; resolving at call time avoids any circularity and
    keeps ``backend.main`` monkeypatch targets working.
    """
    import backend.main as _main

    return _main


def _client_ip() -> str:
    return _client_ip_from_request(request)
