"""Root pytest configuration — ensures ``backend/`` is on sys.path for bare-import modules."""
import sys
from pathlib import Path

import pytest

# backend/ must be on sys.path so bare imports like ``from scraping.xxx`` and
# ``from intelligence.llm.xxx`` resolve to the correct packages.
_backend = str(Path(__file__).resolve().parent / "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)


@pytest.fixture(autouse=True)
def _isolate_listings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent ``FLASK_ENV=production`` and strict listings flags leaking across tests."""
    monkeypatch.delenv("FLASK_ENV", raising=False)
    monkeypatch.delenv("LISTINGS_INCLUDE_INCOMPLETE_CARS", raising=False)
