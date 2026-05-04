"""Root pytest configuration — ensures ``backend/`` is on sys.path for bare-import modules."""
import sys
from pathlib import Path

# backend/ must be on sys.path so bare imports like ``from scraping.xxx`` and
# ``from intelligence.llm.xxx`` resolve to the correct packages.
_backend = str(Path(__file__).resolve().parent / "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)
