"""Helpers shared by the extracted route modules in ``backend/routes/``.

Route modules extracted from ``backend/main.py`` import their helpers from the
owning modules (``backend.db.inventory_db``, ``backend.db.user_history_db``,
``backend.enrichment.knowledge_engine``, ...) and tests patch those names on the
route module that looks them up. The old ``main_module()`` indirection, which
resolved helpers through ``backend.main`` at request time, is gone (monolith
audit W1).
"""

from __future__ import annotations

from flask import request

from backend.utils.client_ip import client_ip as _client_ip_from_request


def _client_ip() -> str:
    return _client_ip_from_request(request)
