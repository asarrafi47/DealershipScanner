"""
Where the brochure acquisition lane writes: ledger, content index, artifacts.

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

from pathlib import Path

from backend.enrichment.dictionary_paths import DERIVED_DIR

#: Repo root (the script computed ``parents[2]`` from backend/scripts/).
_REPO = Path(__file__).resolve().parents[3]

#: Append-only provenance ledger: one line per stored document.
FETCH_LOG_PATH = DERIVED_DIR / "brochure_fetch_log.jsonl"

#: sha256 -> the single stored copy. Lives next to the ledger.
CONTENT_INDEX_PATH = DERIVED_DIR / "brochure_content_index.json"

#: Artifacts for other agents. Under workspace/ because they are run output,
#: not dictionary data.
ARTIFACT_DIR = _REPO / "workspace" / "brochure_acquisition"
