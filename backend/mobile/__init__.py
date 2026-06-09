"""Mobile / native client API surface (iOS).

Route implementations live in ``backend.main`` today. This package holds the
contract registry and docs so iOS backend work stays scoped and testable.
"""

from backend.mobile.contract import MOBILE_API_ROUTES

__all__ = ["MOBILE_API_ROUTES"]
