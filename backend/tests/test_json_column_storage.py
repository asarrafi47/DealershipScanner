"""Tests for nullable JSON array TEXT storage."""
from __future__ import annotations

from backend.utils.json_column_storage import nullable_json_array_text


def test_nullable_json_array_text_empty_list_is_null() -> None:
    assert nullable_json_array_text([]) is None
    assert nullable_json_array_text("[]") is None
    assert nullable_json_array_text("  []  ") is None
    assert nullable_json_array_text(None) is None


def test_nullable_json_array_text_nonempty_list() -> None:
    assert nullable_json_array_text(["a"]) == '["a"]'
    assert nullable_json_array_text('["Clean CARFAX"]') == '["Clean CARFAX"]'
