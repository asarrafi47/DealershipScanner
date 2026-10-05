"""
Surface pin for ``backend/scripts/fetch_oem_brochures.py``.

Written before the script's library code moved into
``backend/enrichment/brochure_acquisition/`` (audit datascripts.md F7) and kept
passing across the move: the importable names, the argparse surface (option
strings, dest, action, default, type, choices, metavar, help) and the mode
dispatch order are what operators and other code rely on.

Offline: nothing here fetches, connects to a database or writes a file. Mode
functions are replaced by recorders before ``main()`` runs.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

from backend.scripts import fetch_oem_brochures as fob

REPO = Path(__file__).resolve().parents[2]

#: Every name the script defined at module level before the split. Each must
#: still resolve as ``fob.<name>`` (tests import build_gap_list / resolve_source).
SCRIPT_NAMES = (
    "_REPO",
    "logger",
    "FETCH_LOG_PATH",
    "CONTENT_INDEX_PATH",
    "ARTIFACT_DIR",
    "_fetch_inventory_rows",
    "build_gap_list",
    "load_gap_list",
    "write_gap_artifact",
    "ArchiveResolver",
    "_resolve_from_archive",
    "resolve_source",
    "oem_pdf_for_catalog_key",
    "download_and_extract",
    "full_document_text",
    "page_texts",
    "_append_fetch_log",
    "open_content_index",
    "audit_corpus_text",
    "corpus_page_texts",
    "quarantine_unidentified",
    "quarantine_derived",
    "archive_verify_overlap",
    "archive_audit",
    "audit_corpus",
    "_UA_CHOICES",
    "probe_reachability",
    "write_reachability_artifact",
    "HTML_SPEC_LOG_PATH",
    "HTML_SPEC_OVERLAY_DIR",
    "LIVE_OVERLAY_DIR",
    "_html_spec_targets",
    "_honda_release_for",
    "_held_html_spec",
    "_append_html_spec_log",
    "fetch_html_specs",
    "rebuild_html_spec_overlays",
    "reverify_html_specs",
    "_overlay_source_of",
    "main",
)

_ART = REPO / "workspace" / "brochure_acquisition"

#: (option_strings, dest, action class, default, type, choices, metavar, help)
ARGPARSE_SURFACE = [
    (("-h", "--help"), "help", "_HelpAction", argparse.SUPPRESS, None, None, None,
     "show this help message and exit"),
    (("--download",), "download", "_StoreTrueAction", False, None, None, None,
     "Actually download. Without this the run is a dry run and writes nothing."),
    (("--limit",), "limit", "_StoreAction", 25, int, None, None, "Max gaps to consider."),
    (("--brand", "--make"), "brand", "_AppendAction", None, None, None, None,
     "Restrict to brand(s) so parallel agents take disjoint slices."),
    (("--year",), "year", "_AppendAction", None, int, None, None, "Restrict to year(s)."),
    (("--min-cars",), "min_cars", "_StoreAction", 1, int, None, None, "Skip smaller gaps."),
    (("--delay",), "delay", "_StoreAction", 4.0, float, None, None,
     "Seconds between requests (min 3)."),
    (("--user-agent",), "user_agent", "_StoreAction", "browser", None,
     ["browser", "identified", "both"], None,
     "Which user agent to send. 'both' is probe-only and reports the delta."),
    (("--supported-only",), "supported_only", "_StoreTrueAction", False, None, None, None,
     "Skip makes with no registered official source."),
    (("--probe-reachability",), "probe_reachability", "_StoreTrueAction", False, None, None,
     None, "Only measure per-brand reachability and exit."),
    (("--audit-corpus",), "audit_corpus", "_StoreTrueAction", False, None, None, None,
     "Only content-hash every stored PDF and report duplicates."),
    (("--allow-archive",), "allow_archive", "_StoreTrueAction", False, None, None, None,
     "Allow the archive tier (auto-brochures.com) as a FALLBACK for gaps the OEM tier "
     "cannot serve. OEM is always tried first. Paced at 12s, more conservatively than "
     "the OEM path."),
    (("--archive-verify-overlap",), "archive_verify_overlap", "_StoreAction", 0, int, None,
     "N",
     "Measurement mode: fetch the archive's copy of N vehicles we already hold an OEM "
     "original for and compare sha256, then text. Copies go to "
     "brochures_archive_comparison/, never into the corpus."),
    (("--archive-audit",), "archive_audit", "_StoreTrueAction", False, None, None, None,
     "Re-open every archive-tier PDF on disk and report its hash, metadata and text "
     "layer. Reads the documents, not the register."),
    (("--quarantine-unidentified",), "quarantine_unidentified", "_StoreTrueAction", False,
     None, None, None,
     "Re-run the identity, subject (derived-nameplate) and quality (decode / length / "
     "digit-density) checks over derived/brochure_text and move the failures to "
     "brochure_text_quarantine/. Dry run unless --apply."),
    (("--quarantine-derived",), "quarantine_derived", "_StoreTrueAction", False, None, None,
     None,
     "Only sweep the derived stores (brochure_text_slim, trim_candidates, "
     "trim_adds_by_year) for artifacts of an already-quarantined document. Dry run "
     "unless --apply."),
    (("--apply",), "apply", "_StoreTrueAction", False, None, None, None,
     "Perform the moves for --quarantine-unidentified / --quarantine-derived (default: "
     "dry run)."),
    (("--html-specs",), "html_specs", "_StoreTrueAction", False, None, None, None,
     "Acquire official HTML specification pages instead of PDFs, for the makes with a "
     "measured discovery chain. Honours --download."),
    (("--rebuild-html-overlays",), "rebuild_html_overlays", "_StoreTrueAction", False, None,
     None, None,
     "Re-derive HTML spec transcripts and overlays from the stored bytes. Fetches nothing."),
    (("--reverify-html-specs",), "reverify_html_specs", "_StoreTrueAction", False, None,
     None, None,
     "Re-derive every stored HTML citation from the stored bytes and report. Fetches "
     "nothing."),
    (("--show-gaps",), "show_gaps", "_StoreAction", 0, int, None, None, "Print top N gaps."),
    (("--artifact-dir",), "artifact_dir", "_StoreAction", _ART, Path, None, None,
     "Where the JSON artifacts are written."),
    (("--no-artifact",), "no_artifact", "_StoreTrueAction", False, None, None, None,
     "Do not write JSON artifacts."),
]


class _Captured(Exception):
    def __init__(self, parser: argparse.ArgumentParser) -> None:
        super().__init__("parser captured")
        self.parser = parser


def _capture_parser(monkeypatch) -> argparse.ArgumentParser:
    def _parse_args(self, *a, **k):
        raise _Captured(self)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", _parse_args)
    with pytest.raises(_Captured) as info:
        fob.main()
    return info.value.parser


def _patch_everywhere(monkeypatch, name: str, value) -> None:
    """Rebind ``name`` in the script and in every loaded module that binds the
    same object, so the patch lands wherever ``main`` looks it up."""
    original = getattr(fob, name)
    for module in list(sys.modules.values()):
        modname = getattr(module, "__name__", "") or ""
        if not (
            modname == fob.__name__
            or modname.startswith("backend.enrichment.brochure_acquisition")
        ):
            continue
        if getattr(module, name, None) is original:
            monkeypatch.setattr(module, name, value)


def test_script_names_still_importable():
    missing = [name for name in SCRIPT_NAMES if not hasattr(fob, name)]
    assert not missing, missing


def test_script_paths_unchanged():
    assert fob._REPO == REPO
    assert fob.ARTIFACT_DIR == _ART
    from backend.enrichment.dictionary_paths import DERIVED_DIR, TRIM_ADDS_BY_YEAR_DIR

    assert fob.FETCH_LOG_PATH == DERIVED_DIR / "brochure_fetch_log.jsonl"
    assert fob.CONTENT_INDEX_PATH == DERIVED_DIR / "brochure_content_index.json"
    assert fob.HTML_SPEC_LOG_PATH == DERIVED_DIR / "html_spec_fetch_log.jsonl"
    assert fob.HTML_SPEC_OVERLAY_DIR == DERIVED_DIR / "html_spec_overlays"
    assert fob.LIVE_OVERLAY_DIR == TRIM_ADDS_BY_YEAR_DIR
    assert set(fob._UA_CHOICES) == {"browser", "identified"}


def test_argparse_surface_is_pinned(monkeypatch):
    parser = _capture_parser(monkeypatch)
    assert parser.description == fob.__doc__
    got = [
        (
            tuple(a.option_strings),
            a.dest,
            type(a).__name__,
            a.default,
            a.type,
            a.choices,
            a.metavar,
            a.help,
        )
        for a in parser._actions
    ]
    assert got == ARGPARSE_SURFACE


_MODES = [
    (["--quarantine-unidentified", "--audit-corpus"], "quarantine_unidentified", (False,)),
    (["--quarantine-unidentified", "--apply"], "quarantine_unidentified", (True,)),
    (["--quarantine-derived", "--audit-corpus"], "quarantine_derived", (False,)),
    (["--audit-corpus", "--rebuild-html-overlays"], "audit_corpus", ()),
    (["--rebuild-html-overlays", "--reverify-html-specs"], "rebuild_html_spec_overlays", ()),
    (["--reverify-html-specs", "--archive-audit"], "reverify_html_specs", ()),
    (["--archive-audit", "--archive-verify-overlap", "3"], "archive_audit", ()),
    (
        ["--archive-verify-overlap", "3", "--delay", "1", "--user-agent", "both",
         "--brand", "Mazda", "--probe-reachability"],
        "archive_verify_overlap",
        (3, 3.0, fob._UA_CHOICES["browser"], {"mazda"}),
    ),
    (
        ["--archive-verify-overlap", "2", "--user-agent", "identified"],
        "archive_verify_overlap",
        (2, 4.0, fob._UA_CHOICES["identified"], None),
    ),
]


@pytest.mark.parametrize("argv,target,expected", _MODES)
def test_mode_dispatch_order(monkeypatch, argv, target, expected):
    calls: list[tuple] = []

    def _recorder(*args):
        calls.append(args)
        return 7

    _patch_everywhere(monkeypatch, target, _recorder)

    def _no_gaps():
        raise AssertionError("default mode reached")

    _patch_everywhere(monkeypatch, "load_gap_list", _no_gaps)
    monkeypatch.setattr(sys, "argv", ["fetch_oem_brochures", *argv])
    assert fob.main() == 7
    assert calls == [expected]


def test_probe_reachability_mode(monkeypatch, capsys):
    calls: list[tuple] = []

    def _probe(labels, delay, brand):
        calls.append((labels, delay, brand))
        return []

    _patch_everywhere(monkeypatch, "probe_reachability", _probe)

    def _no_write(*a, **k):
        raise AssertionError("artifact written despite --no-artifact")

    _patch_everywhere(monkeypatch, "write_reachability_artifact", _no_write)
    monkeypatch.setattr(
        sys,
        "argv",
        ["x", "--probe-reachability", "--user-agent", "both", "--brand", "Honda",
         "--no-artifact", "--html-specs"],
    )
    assert fob.main() == 0
    assert calls == [(["browser", "identified"], 4.0, "Honda")]
    out = capsys.readouterr().out
    assert "Per-brand reachability (paced 4s, sequential)" in out
    assert "Verdict counts:" in out and "Who declined:" in out


def test_default_mode_starts_with_the_gap_list(monkeypatch):
    class _Reached(Exception):
        pass

    def _gaps():
        raise _Reached

    _patch_everywhere(monkeypatch, "load_gap_list", _gaps)
    monkeypatch.setattr(sys, "argv", ["x", "--html-specs", "--download"])
    with pytest.raises(_Reached):
        fob.main()
