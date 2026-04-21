"""Authenticated store admin UI over scraped ``inventory.db`` (separate from ``/dev`` ops)."""

from backend.dealer_admin.routes import store_admin_bp

__all__ = ["store_admin_bp"]
