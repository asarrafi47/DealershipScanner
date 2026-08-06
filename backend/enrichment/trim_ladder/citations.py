"""One citation register per rung; one choke point for every store."""
from __future__ import annotations

import csv
import logging
import re
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

from ._common import (
    _find_epa_csv,
    _norm_make,
    _norm_model,
    _norm_token,
    _normalize_listing_model,
)
from .epa import (
    _epa_models_match,
    _normalize_epa_trim,
)

# --- one citation register per rung, one choke point for every store ---------
#
# The provenance gate used to sit on the brochure overlays only, so a rung could
# still be filled from four stores that never carried a citation: the curated
# trim_ladders.json adds, the machine-written generated ladders, the
# Complete_Options CSV cells (directly, and again via the trim spec sheets), and
# the position-describing knowledge prose. A verifier sweeping the fleet found
# 46,722 of the 46,781 cars that still showed a bullet showing at least one that
# no admissible document could account for.
#
# The rule is now applied ONCE, at the end of the rung, against a register of
# what this rung can actually quote. A store earns entries in that register by
# naming a document and a location inside it; a store that cannot say where its
# text came from contributes nothing and its bullets fall out here no matter
# which producer put them in ``candidates``. Which stores may register is decided
# by ``brochure_extract.LADDER_BULLET_STORES`` and nowhere else.
#
# Blocking at the end rather than at each producer is deliberate: it also closes
# the refill hole, where emptying one store lets a lower-quality store fill the
# rung it used to be crowded out of.

_CITATION_KEY_RE = re.compile(r"[^a-z0-9]+")


def _citation_key(text: Any) -> str:
    """Bullet text reduced to space-separated alphanumeric tokens for comparison."""
    return _CITATION_KEY_RE.sub(" ", str(text or "").lower()).strip()


class _CitationRegister:
    """What one rung is able to quote, and the document location for each quote.

    ``add_quote`` registers a whole source line: a rendered bullet matches it
    either exactly or as a whole-token run inside it, because the display
    pipeline splits multi-value cells (``"A; B"`` → ``"A"``, ``"B"``) and a
    fragment of a quoted line is still that line's words. Containment is only
    ever checked in that direction — a bullet that adds words to a quote is not
    covered by it.
    """

    __slots__ = ("_exact", "_quotes")

    def __init__(self) -> None:
        self._exact: dict[str, dict[str, Any]] = {}
        self._quotes: list[tuple[str, dict[str, Any]]] = []

    @staticmethod
    def _store_allowed(citation: dict[str, Any]) -> bool:
        """``LADDER_BULLET_STORES`` decides, here, for every producer at once.

        Registering is what makes a bullet renderable, so the admissibility check
        belongs on the register rather than on each caller: a store that is
        revoked in that table stops putting citations in front of a shopper even
        if its producer still runs. A citation with no ``store`` is not
        admissible either.
        """
        from backend.enrichment.brochure_extract import ladder_bullet_store_admissible

        return ladder_bullet_store_admissible((citation or {}).get("store"))

    def add_quote(self, text: Any, citation: dict[str, Any]) -> None:
        key = _citation_key(text)
        if not key or not self._store_allowed(citation):
            return
        self._exact.setdefault(key, citation)
        self._quotes.append((key, citation))

    def add_exact(self, text: Any, citation: dict[str, Any]) -> None:
        key = _citation_key(text)
        if key and self._store_allowed(citation):
            self._exact.setdefault(key, citation)

    def citation_for(self, bullet: Any) -> dict[str, Any] | None:
        key = _citation_key(bullet)
        # Two tokens or fewer ("v6 turbo", "all wheel drive") appear inside
        # unrelated lines by accident, so a fragment has to be substantial
        # before containment counts as a quote.
        if not key:
            return None
        hit = self._exact.get(key)
        if hit is not None:
            return hit
        if len(key) < 8 or key.count(" ") < 1:
            return None
        padded = f" {key} "
        for hay, citation in self._quotes:
            if padded in f" {hay} ":
                return citation
        return None

    def __bool__(self) -> bool:
        return bool(self._exact or self._quotes)


@lru_cache(maxsize=256)
def _epa_engine_citations(make: str, model: str, year: Any) -> tuple[tuple[str, str, str, int], ...]:
    """
    ``(trim key, engineOptions text, EPA csv filename, csv row number)`` for this vehicle.

    Reachable only while ``epa_csv`` is admissible in
    ``brochure_extract.LADDER_BULLET_STORES``; it is not, as of 2026-07-31, so
    this returns to no caller today (``_register_epa_citations`` returns before
    calling it). The reason is recorded in full on that table: ``engineOptions``
    is a string ``import_epa_to_dictionary.build_engine_desc`` composed out of
    several EPA columns, so quoting it quotes us.
    """
    from backend.enrichment.trim_ladder_knowledge import epa_model_search_name

    path = _find_epa_csv(make, model, year)
    if not path:
        return ()
    model_label = epa_model_search_name(make, model)
    out: list[tuple[str, str, str, int]] = []
    try:
        with path.open(encoding="utf-8", newline="") as fh:
            # DictReader consumes the header, so the first data row is line 2.
            for row_no, row in enumerate(csv.DictReader(fh), start=2):
                trim_raw = (row.get("Trim") or "").strip()
                if not trim_raw:
                    continue
                if _norm_make((row.get("Make") or make or "").strip()) != _norm_make(make):
                    continue
                if not _epa_models_match(model_label, (row.get("Model") or model or "").strip()):
                    continue
                name = _normalize_epa_trim(trim_raw, make, model)
                if not name:
                    continue
                engine = (row.get("engineOptions") or "").strip()
                if not engine:
                    continue
                out.append((_norm_token(name), engine, path.name, row_no))
    except OSError:
        return ()
    return tuple(out)


def _register_epa_citations(
    register: _CitationRegister,
    *,
    name: str,
    aliases: list[str],
    make: str,
    model: str,
    year: Any,
) -> None:
    """Let this rung quote the engine text of the EPA rows filed under its trim.

    A no-op while ``epa_csv`` is revoked in ``LADDER_BULLET_STORES``: the
    register would refuse every citation this builds, and reading the CSV to
    build them is wasted work. Flipping that row back on is what turns this on
    again — the policy is not repeated here.
    """
    from backend.enrichment.brochure_extract import ladder_bullet_store_admissible

    if not ladder_bullet_store_admissible("epa_csv"):
        return
    keys = {_norm_token(name)} | {_norm_token(a) for a in aliases}
    keys.discard("")
    if not keys:
        return
    for trim_key, engine, file_name, row_no in _epa_engine_citations(make, model, year):
        if trim_key not in keys:
            continue
        register.add_quote(
            engine,
            {"store": "epa_csv", "source": file_name, "row": row_no, "column": "engineOptions"},
        )


def _register_overlay_citations(
    register: _CitationRegister,
    overlay: dict[str, Any] | None,
    trim_key: str | None,
    *,
    make: str,
    model: str,
    year: Any,
) -> None:
    """Replay the overlay's own per-bullet ``{source, page}`` entries onto this rung.

    THE CITATION HAS TO BE ABOUT THIS CAR. ``load_brochure_trim_overlay`` reaches
    ±2 model years to find a lineup, and ``_build_ladder_result`` is also called
    with overlays read straight off disk by scripts and tests, so the overlay in
    hand is not guaranteed to be this car's book. Year, make and model are
    compared here as well as in ``admissible_overlay_adds`` — the two checks are
    independent, and this one is the last thing standing between a 2019 brochure
    and a 2022 car if a future caller assembles an overlay some other way.

    Only entries the build-time verifier has confirmed are replayed: an entry
    with no ``verified: true`` is a citation nobody has checked against the
    document, and this function is not the place to check it (that is
    ``backend/scripts/verify_trim_citations.py``, which re-opens the PDF).
    """
    from backend.enrichment.brochure_extract import (
        CITATION_VERIFIED_KEY,
        VERIFIED_LADDER_BULLET_STORES,
        overlay_citable_for_year,
    )

    if not isinstance(overlay, dict) or not trim_key:
        return
    if not overlay_citable_for_year(overlay, year):
        return
    if _norm_make(str(overlay.get("make") or "")) != _norm_make(make):
        return
    if _norm_model(str(overlay.get("model") or "")) != _norm_model(
        _normalize_listing_model(make, model)
    ):
        return
    provenance = overlay.get("adds_provenance")
    entries = (provenance or {}).get(trim_key) if isinstance(provenance, dict) else None
    if not isinstance(entries, list):
        return
    require_verified = "brochure_text_quoted" in VERIFIED_LADDER_BULLET_STORES
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        source = str(entry.get("source") or "").strip()
        page = entry.get("page")
        if not text or not source or page is None:
            continue
        if require_verified and entry.get(CITATION_VERIFIED_KEY) is not True:
            continue
        register.add_quote(
            text,
            {"store": "brochure_text_quoted", "source": source, "page": page},
        )
