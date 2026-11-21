"""One home for the embedding model id and the Hugging Face revision behind the stored vectors.

Stdlib only, so Dockerfile.web can load it by path at build time (before app code is copied)
to bake the exact snapshot into ``/app/models``.

Every ``SentenceTransformer(...)`` call goes through :func:`load_sentence_transformer`, which
passes ``revision=``. A different model can only be loaded in production when the operator also
names its revision (``LISTING_EMBEDDING_REVISION``), so a vector space never changes silently.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULT_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
# Commit hash of DEFAULT_MODEL_ID (hub head on 2026-11-03). The weights, tokenizer and config
# files are byte-identical to the 2025-03-06 head c9745ed1d9f2 (same git-lfs oids), and encoding
# the same sentences with both gave a max absolute difference of 0.0, so pinning this revision
# leaves the vector space of the stored embeddings unchanged. Empty would mean unpinned: the call
# passes ``revision=None`` (hub default branch).
DEFAULT_MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"

_SHORT_NAME = DEFAULT_MODEL_ID.rsplit("/", 1)[-1]


def _is_production() -> bool:
    return (os.environ.get("FLASK_ENV") or "").strip().lower() == "production"


def model_id() -> str:
    """Model to load: ``LISTING_EMBEDDING_MODEL`` override, else the pinned default."""
    return (os.environ.get("LISTING_EMBEDDING_MODEL") or "").strip() or DEFAULT_MODEL_ID


def _is_default(name: str) -> bool:
    return name in (DEFAULT_MODEL_ID, _SHORT_NAME)


def model_revision() -> str | None:
    """Pinned revision for :func:`model_id`, or ``None`` when none is pinned.

    The default model uses ``DEFAULT_MODEL_REVISION``. An override uses
    ``LISTING_EMBEDDING_REVISION``; in production an override without one raises.
    """
    name = model_id()
    if _is_default(name):
        return DEFAULT_MODEL_REVISION or None
    rev = (os.environ.get("LISTING_EMBEDDING_REVISION") or "").strip()
    if not rev and _is_production():
        raise RuntimeError(
            f"LISTING_EMBEDDING_MODEL={name!r} needs LISTING_EMBEDDING_REVISION in production "
            "(an unpinned model can change the vector space under the stored embeddings)."
        )
    return rev or None


def load_sentence_transformer() -> Any:
    """Build the SentenceTransformer for :func:`model_id`, pinned to :func:`model_revision`."""
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_id(), revision=model_revision())
