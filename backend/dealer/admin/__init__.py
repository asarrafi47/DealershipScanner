"""Authenticated store admin UI over scraped ``inventory.db`` (separate from ``/dev`` ops)."""

from backend.dealer.admin.routes import store_admin_bp

# Register site-admin dealer hub (/admin/dealers).
import backend.dealer.admin.dealers_hub  # noqa: F401,E402
import backend.dealer.admin.users_hub  # noqa: F401,E402
import backend.dealer.admin.data_quality_hub  # noqa: F401,E402
import backend.dealer.admin.scanner_ops_hub  # noqa: F401,E402

__all__ = ["store_admin_bp"]
