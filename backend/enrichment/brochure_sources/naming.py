"""Make/model normalisation used for gap-list grouping and host matching."""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse
from backend.enrichment.dictionary_catalog import (
    canonical_make,
    load_make_aliases,
)

logger = logging.getLogger(__name__)

from .hosts import (
    DISCOVERY_UNRESOLVED,
    OFFICIAL_HOSTS,
    UNSUPPORTED_MAKES,
    VERIFIED_MAKES,
)

def _make_token(make: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", canonical_make(make or "").strip().lower())


def _model_token(model: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (model or "").strip().lower())


# --------------------------------------------------------------------------
# Make/model normalisation (gap-list grouping)
# --------------------------------------------------------------------------

# Extra make tokens that appear as a model prefix in inventory but have no entry
# in OFFICIAL_HOSTS / UNSUPPORTED_MAKES / make_aliases.json.
_EXTRA_MAKE_TOKENS = frozenset(
    {
        "alfaromeo", "astonmartin", "audi", "bentley", "chevy", "fiat", "genesis",
        "jaguar", "landrover", "lincoln", "lucid", "maserati", "mclaren", "mini",
        "mitsubishi", "polestar", "porsche", "rivian", "rollsroyce", "scion",
        "smart", "tesla", "volvo",
    }
)


def _known_make_tokens() -> frozenset[str]:
    """Every token that may legitimately prefix a model name."""
    tokens = set(_EXTRA_MAKE_TOKENS)
    tokens.update(OFFICIAL_HOSTS)
    tokens.update(UNSUPPORTED_MAKES)
    tokens.update(DISCOVERY_UNRESOLVED)
    for alias, canonical in load_make_aliases().items():
        tokens.add(re.sub(r"[^a-z0-9]+", "", alias.lower()))
        tokens.add(re.sub(r"[^a-z0-9]+", "", canonical.lower()))
    return frozenset(t for t in tokens if t)


def normalized_model_token(make: str, model: str) -> str:
    """
    Model token with the make prefix stripped and punctuation/case folded.

    Inventory spells the same vehicle several ways -- ``Mazda CX-50`` and
    ``Cx-50``, ``Mercedes-Benz GLE`` and ``GLE`` -- and :func:`catalog_key`
    keeps those apart, which inflates any gap count computed from it. Folding
    them here is what makes "how many vehicles have no brochure" a real number.

    Stripping only happens when something is left over, so make ``Jeep`` model
    ``Jeep`` stays ``jeep`` rather than becoming empty.

    This is a **grouping key only**. Stored filenames still come from
    :func:`catalog_key` of a real inventory spelling, so nothing on disk is
    renamed by a change here. (The existing corpus already follows the stripped
    convention: ``2010__mazda__3.json``, not ``2010__mazda__mazda3.json``.)
    """
    token = _model_token(model)
    if not token:
        return ""
    candidates = {_make_token(make)} | _known_make_tokens()
    # Longest prefix first: "mercedesbenz" must win over "mercedes".
    for prefix in sorted((c for c in candidates if c), key=len, reverse=True):
        if token.startswith(prefix) and len(token) > len(prefix):
            return token[len(prefix) :]
    return token


def gap_group_key(year: int | None, make: str, model: str) -> str:
    """
    Key that folds every inventory spelling of one vehicle into one gap.

    ``2026|mazda|cx50`` for both ``Mazda / Mazda CX-50`` and ``Mazda / Cx-50``.
    """
    y = str(year) if year is not None else "*"
    return f"{y}|{_make_token(make)}|{normalized_model_token(make, model)}"


def model_token_variants(make: str, model: str) -> set[str]:
    """
    Tokens a document filename or body may use for this model.

    Both the raw token and the make-stripped one, because inventory says
    ``Mazda CX-5`` where the document says ``CX-5``.

    **No single-character token is ever returned**, raw or stripped. One
    character identifies nothing: ``3`` occurs in the "2013" of every 2013
    document and as a standalone cell in every spec grid, so it would file the
    CX-5 deck as the Mazda3's. This bites both spellings inventory uses --
    ``Mazda3`` strips to ``3``, and ``Mazda`` / ``3`` *is* the raw token for 41
    files in the existing corpus (Mazda 3/5/6, Infiniti M/G, Nissan Z).

    When that leaves nothing, the make-prefixed form is used instead, which is
    what the manufacturer actually prints: ``Mazda3``, ``INFINITI M``,
    ``Nissan Z``. It is added only in that case, so a model with a usable token
    of its own is matched exactly as before.
    """
    raw = _model_token(model)
    stripped = normalized_model_token(make, model)
    out = {token for token in (raw, stripped) if len(token) > 1}
    if not out:
        prefixed = f"{_make_token(make)}{raw}"
        if len(prefixed) > 1 and raw:
            out.add(prefixed)
    return out


def official_hosts_for(make: str) -> frozenset[str]:
    """Hosts a document may be downloaded from for ``make`` (empty = none)."""
    return OFFICIAL_HOSTS.get(_make_token(make), frozenset())


def is_official_url(url: str, make: str) -> bool:
    """True only if ``url`` is https on an allowlisted host for ``make``."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    return (parsed.hostname or "").lower() in official_hosts_for(make)


def unsupported_reason(make: str) -> str | None:
    """
    Why this make yields no brochure **from the OEM tier**, or ``None`` if the
    OEM tier is known to work for it.

    A make with a registered host but an unresolved discovery step counts as
    unsupported here: the honest answer is that it produces nothing.

    This says nothing about the archive tier. A make can be unsupported at the
    OEM tier and still resolvable at ``tier="archive"`` -- that is the entire
    reason the archive tier exists. Callers that want the combined answer must
    ask both; ``fetch_oem_brochures.resolve_source`` does exactly that, OEM
    first.
    """
    token = _make_token(make)
    if token in VERIFIED_MAKES:
        return None
    if token in DISCOVERY_UNRESOLVED:
        return DISCOVERY_UNRESOLVED[token]
    return UNSUPPORTED_MAKES.get(token, "no official source registered for this make")
