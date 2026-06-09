"""Gallery finalize keeps at least one image when blur would drop all."""
from __future__ import annotations

from unittest.mock import patch

from backend.vision import claude_vision as cv


def test_finalize_returns_best_when_all_blurry() -> None:
    kept = [(0, "https://cdn.example.com/a.jpg", "exterior")]
    with patch.object(cv, "_fetch_image_b64", return_value="e30="):
        with patch.object(cv, "_image_is_blurry", return_value=True):
            out = cv._finalize_kept_gallery_urls(kept, None)
    assert out == ["https://cdn.example.com/a.jpg"]
