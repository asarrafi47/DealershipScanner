"""Shared policy for startup admin bootstrap (password sync)."""

from __future__ import annotations

import os


def bootstrap_force_admin_password() -> bool:
    """When true, startup scripts may reset env-listed admin passwords from ADMIN_PASSWORD."""
    return (os.environ.get("BOOTSTRAP_FORCE_ADMIN_PASSWORD") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
