"""Filter facet deduplication for listings UI."""

from __future__ import annotations

from backend.db.inventory_db import _canonical_facet_label, get_filter_options


def test_canonical_facet_label_prefers_mixed_case() -> None:
    assert _canonical_facet_label("", variants=["LARIAT", "Lariat"]) == "Lariat"
    assert _canonical_facet_label("", variants=["CAMRY", "Camry"]) == "Camry"


def test_filter_options_merge_case_variant_trims() -> None:
    opts = get_filter_options()
    groups: dict[tuple[str, str, str], list[str]] = {}
    for make, model, trim in opts["trim_rows"]:
        key = (make.lower(), model.lower(), (trim or "").lower())
        groups.setdefault(key, []).append(trim or "")
    dupes = [k for k, vals in groups.items() if len(vals) > 1]
    assert dupes == []


def test_filter_options_merge_case_variant_models() -> None:
    opts = get_filter_options()
    groups: dict[tuple[str, str], list[str]] = {}
    for make, model in opts["model_rows"]:
        groups.setdefault((make.lower(), model.lower()), []).append(model)
    dupes = [k for k, vals in groups.items() if len(vals) > 1]
    assert ("toyota", "corolla") not in dupes
    assert ("toyota", "camry") not in dupes
