"""Pins the public surface of ``comments_db`` across the attachments/ratings split."""
from __future__ import annotations

import importlib
import inspect

import pytest

_SURFACE = {
    "backend.db.comment_attachments_db": {
        "list_attachments": "(scope: 'str', comment_ids: 'list[int]') -> 'dict[int, list[dict[str, Any]]]'",
        "attach_attachments": "(scope: 'str', comments: 'list[dict[str, Any]]') -> 'list[dict[str, Any]]'",
        "get_attachment": "(attachment_id: 'int') -> 'dict[str, Any] | None'",
        "delete_attachments_for_comment": "(scope: 'str', comment_id: 'int') -> 'list[str]'",
    },
    "backend.db.dealer_ratings_db": {
        "get_dealer_rating": "(dealership_id: 'int') -> 'dict[str, Any] | None'",
        "list_active_dealership_websites": "() -> 'list[dict[str, Any]]'",
    },
}
_OTHER = ("add_attachments", "upsert_dealer_rating", "list_dealerships_needing_rating")


@pytest.mark.parametrize("mod", sorted(_SURFACE))
def test_reexports_are_the_moved_objects_with_pinned_signatures(mod):
    from backend.db import comments_db

    new = importlib.import_module(mod)
    for name, sig in _SURFACE[mod].items():
        assert getattr(comments_db, name) is getattr(new, name)
        assert str(inspect.signature(getattr(new, name))) == sig


def test_other_names_resolve_and_either_import_order_works():
    from backend.db import comments_db

    for name in _OTHER:
        assert callable(getattr(comments_db, name))
    with pytest.raises(AttributeError):
        comments_db.no_such_name


def test_moved_modules_import_first_without_cycle():
    import subprocess
    import sys

    for mod in _SURFACE:
        r = subprocess.run([sys.executable, "-c", f"import {mod}"], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr


def test_kept_names_stay_in_comments_db():
    from backend.db import comments_db

    for name in ("SCOPE_CAR", "SCOPE_DEALER", "CommentError", "FLAG_HIDE_THRESHOLD",
                 "sanitize_body", "list_comments", "subject_exists", "car_exists"):
        assert hasattr(comments_db, name)
