from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from SCRAPING.paths import ROOT
from SCRAPING.text_utils import normalize_root

from oem_intake.normalize import intake_dict_to_normalized
from oem_intake.paths import ensure_bmw_dirs
from oem_intake.raw_store import append_bmw_batch_jsonl
from oem_intake.sqlite_store import (
    clear_partial_staging,
    connect,
    count_stats,
    delete_all_normalized,
    init_schema,
    insert_raw_intake,
    list_normalized_for_enrichment,
    list_partial_staging,
    load_all_raw_extracted,
    upsert_partial_staging,
    update_enrichment_fields,
    upsert_normalized,
)
from scrapers.oem.bmw import BMWIntakeBundle, ingest_bmw_usa, load_fixture_records
from scrapers.oem.bmw_debug import write_debug_artifact

logger = logging.getLogger("oem_intake.bmw_pipeline")


def _load_selector_overrides(root: Path, input_selector: str | None, result_selector: str | None) -> dict[str, str]:
    out: dict[str, str] = {}
    cfg = root / "data" / "oem" / "bmw_dom_overrides.json"
    if cfg.is_file():
        try:
            data = json.loads(cfg.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                if isinstance(data.get("input_selector"), str):
                    out["input_selector"] = data["input_selector"].strip()
                if isinstance(data.get("result_selector"), str):
                    out["result_selector"] = data["result_selector"].strip()
        except Exception:
            pass
    if input_selector and input_selector.strip():
        out["input_selector"] = input_selector.strip()
    if result_selector and result_selector.strip():
        out["result_selector"] = result_selector.strip()
    return out


def _write_selector_hints(project_root: Path, debug_report: dict[str, Any]) -> Path:
    flow = (debug_report.get("interactive_flow") or {})
    obs = flow.get("zip_observations") or []
    best = None
    for o in obs:
        if o.get("dealer_cards_appeared") or o.get("result_container_accepted"):
            best = o
            break
    if best is None and obs:
        best = obs[0]
    hints = {
        "source_debug_artifact_mode": debug_report.get("mode"),
        "input_selector": (best or {}).get("input_selector_used") or "",
        "result_selector": (best or {}).get("result_container_selector") or "",
        "locator_mode_detected": (best or {}).get("locator_mode_detected") or "",
        "result_state": (best or {}).get("result_state") or "",
        "ai_selector_plan": flow.get("ai_selector_plan") or {},
    }
    p = project_root / "data" / "oem" / "bmw" / "bmw_selector_hints_latest.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(hints, indent=2, ensure_ascii=False), encoding="utf-8")
    return p


def _fingerprint_record(rec: dict[str, Any]) -> str:
    key = "|".join(
        str(rec.get(k) or "")
        for k in ("dealer_name", "zip", "phone", "website", "street", "city")
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _insert_raw_safe(
    conn: sqlite3.Connection,
    *,
    scraped_at: str,
    source_locator_url: str,
    intake_method: str,
    fingerprint: str,
    raw_payload: dict[str, Any],
    extracted_fields: dict[str, Any],
) -> tuple[int, bool]:
    """Returns (raw_id, was_duplicate_fingerprint)."""
    try:
        rid = insert_raw_intake(
            conn,
            scraped_at=scraped_at,
            source_locator_url=source_locator_url,
            intake_method=intake_method,
            fingerprint=fingerprint,
            raw_payload=raw_payload,
            extracted_fields=extracted_fields,
        )
        return rid, False
    except sqlite3.IntegrityError:
        conn.rollback()
        row = conn.execute(
            "SELECT id FROM bmw_raw_intake WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        if not row:
            raise
        return int(row[0]), True


@dataclass
class BMWIngestStats:
    raw_inserted: int = 0
    raw_duplicate_fingerprints: int = 0
    normalized_upserts: int = 0
    duplicates_merged: int = 0
    new_unique_normalized: int = 0
    partial_staged: int = 0


def run_bmw_ingest(
    *,
    project_root: Path | None = None,
    use_fixture: bool = False,
    prefer_playwright: bool = False,
    timeout: int = 60,
    verify_ssl: bool = True,
    intake_method: str | None = None,
    debug_live: bool = False,
    test_url: str | None = None,
    zip_seeds: tuple[str, ...] = ("92606", "90807"),
    manual_input_selector: str | None = None,
    manual_result_selector: str | None = None,
    ai_selector_assist: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> tuple[BMWIntakeBundle, BMWIngestStats]:
    root = project_root or ROOT
    ensure_bmw_dirs()
    conn = connect()
    init_schema(conn)

    if use_fixture:
        bundle = load_fixture_records(root)
        method = intake_method or "fixture"
    else:
        extra_urls: list[str] | None = None
        if test_url and test_url.strip():
            extra_urls = [test_url.strip()]
        bundle = ingest_bmw_usa(
            project_root=root,
            prefer_playwright=prefer_playwright,
            timeout=timeout,
            verify_ssl=verify_ssl,
            debug=debug_live,
            extra_test_urls=extra_urls,
            zip_seeds=zip_seeds,
            selector_overrides=_load_selector_overrides(root, manual_input_selector, manual_result_selector),
            ai_selector_assist=ai_selector_assist,
            llm_base_url=llm_base_url,
            llm_model=llm_model,
        )
        method = intake_method or ("playwright" if prefer_playwright else "requests+maybe_playwright")
        if debug_live and bundle.debug_report:
            dbg_path = write_debug_artifact(root, bundle.debug_report)
            bundle.notes.append(f"debug_artifact:{dbg_path}")
            try:
                hints_path = _write_selector_hints(root, bundle.debug_report)
                bundle.notes.append(f"selector_hints:{hints_path}")
            except Exception:
                pass

    append_bmw_batch_jsonl(
        {
            "scraped_at": bundle.scraped_at_iso,
            "source_locator_url": bundle.source_locator_url,
            "intake_method": method,
            "notes": bundle.notes,
            "raw_payloads_count": len(bundle.raw_payloads),
            "raw_payloads_kinds": [p.get("kind") for p in bundle.raw_payloads[:40] if isinstance(p, dict)],
            "record_count": len(bundle.records),
        }
    )

    stats = BMWIngestStats()
    validation_rows: list[dict[str, Any]] = []
    for rec in bundle.records:
        fp = _fingerprint_record(rec)
        raw_frag = rec.get("raw_source_payload")
        if not isinstance(raw_frag, dict):
            raw_frag = {}
        raw_payload = {"fragment": raw_frag, "intake_method": method}
        extracted = {k: v for k, v in rec.items() if k != "raw_source_payload"}

        rid, dup = _insert_raw_safe(
            conn,
            scraped_at=bundle.scraped_at_iso,
            source_locator_url=bundle.source_locator_url,
            intake_method=method,
            fingerprint=fp,
            raw_payload=raw_payload,
            extracted_fields=extracted,
        )
        if dup:
            stats.raw_duplicate_fingerprints += 1
        else:
            stats.raw_inserted += 1

        nd = intake_dict_to_normalized(rec, last_verified_at=bundle.scraped_at_iso)
        if nd.row_quality == "usable":
            _, merged = upsert_normalized(conn, nd, [rid])
            stats.normalized_upserts += 1
            if merged:
                stats.duplicates_merged += 1
        else:
            upsert_partial_staging(conn, rid, nd)
            stats.partial_staged += 1
        validation_rows.append(
            {
                "row_quality": nd.row_quality,
                "dealer_name": rec.get("dealer_name") or "",
                "website": nd.root_website,
                "phone": rec.get("phone") or "",
                "street": rec.get("street") or "",
                "city": rec.get("city") or "",
                "state": rec.get("state") or "",
                "zip": rec.get("zip") or "",
                "website_extracted": rec.get("website") or "",
                "map_reference_url": nd.map_reference_url,
                "rejection_reasons": ",".join(nd.row_rejection_reasons),
                "source_of_each_field": json.dumps(
                    rec.get("source_of_each_field") or {}, ensure_ascii=False
                ),
                "source_views": ",".join(
                    sorted(
                        {
                            str((rec.get("raw_source_payload") or {}).get("source") or ""),
                            str((rec.get("source_of_each_field") or {}).get("website") or ""),
                        }
                    )
                ),
                "enrichment_ready": 1 if nd.enrichment_ready else 0,
                "dedupe_key": nd.dedupe_key,
            }
        )

    _write_validation_artifacts(root, validation_rows)
    # Canonical path: always rebuild normalized from raw for idempotence.
    conn.close()
    norm_diag = run_bmw_normalize_dedupe_from_raw(root)
    stats.new_unique_normalized = max(
        0,
        int(norm_diag.get("normalized_count", 0))
        - int(norm_diag.get("normalized_before_run", 0)),
    )
    return bundle, stats


def run_bmw_locator_dom_inspect(
    *,
    project_root: Path | None = None,
    timeout: int = 60,
    verify_ssl: bool = True,
    zip_seeds: tuple[str, ...] = ("92606", "90807"),
    manual_input_selector: str | None = None,
    manual_result_selector: str | None = None,
    ai_selector_assist: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
) -> dict[str, Any]:
    """Run locator observability/debug pass without persisting intake rows."""
    root = project_root or ROOT
    overrides = _load_selector_overrides(root, manual_input_selector, manual_result_selector)
    bundle = ingest_bmw_usa(
        project_root=root,
        prefer_playwright=True,
        timeout=timeout,
        verify_ssl=verify_ssl,
        debug=True,
        zip_seeds=zip_seeds,
        selector_overrides=overrides,
        ai_selector_assist=ai_selector_assist,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
    )
    debug_path = write_debug_artifact(root, bundle.debug_report or {"error": "missing_debug_report"})
    try:
        hints_path = _write_selector_hints(root, bundle.debug_report or {})
    except Exception:
        hints_path = None
    return {
        "debug_artifact": str(debug_path),
        "selector_hints_path": str(hints_path) if hints_path else "",
        "rows_extracted": len(bundle.records),
        "notes": list(bundle.notes),
        "selector_overrides": overrides,
    }


def _write_validation_artifacts(project_root: Path, rows: list[dict[str, Any]]) -> None:
    import csv

    out_dir = project_root / "data" / "oem" / "bmw"
    out_dir.mkdir(parents=True, exist_ok=True)
    jpath = out_dir / "bmw_row_validation_latest.json"
    cpath = out_dir / "bmw_row_validation_latest.csv"
    jpath.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    if not rows:
        cpath.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with cpath.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_partial_staging_review(project_root: Path, rows: list[dict[str, Any]]) -> None:
    import csv

    out_dir = project_root / "data" / "oem" / "bmw"
    out_dir.mkdir(parents=True, exist_ok=True)
    jpath = out_dir / "bmw_partial_review_latest.json"
    cpath = out_dir / "bmw_partial_review_latest.csv"
    jpath.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    if not rows:
        cpath.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with cpath.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _missing_reason_codes_for_partial(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    website = (row.get("website") or "").strip()
    street = (row.get("street") or "").strip()
    city = (row.get("city") or "").strip()
    state = (row.get("state") or "").strip()
    phone = (row.get("phone") or "").strip()
    name = (row.get("dealer_name") or "").strip()
    source = (row.get("source_of_each_field") or "").lower()
    if not website:
        reasons.append("missing_website")
    if not street:
        reasons.append("missing_street")
    if not (city and state):
        reasons.append("missing_city_state_zip")
    if not phone:
        reasons.append("missing_phone")
    if name and not any((website, street, city, state, phone)):
        reasons.append("weak_name_only")
    if row.get("map_reference_url") and not website:
        reasons.append("map_card_only")
    if "detail_drawer" not in source and "detail_interaction" not in source:
        reasons.append("no_detail_panel_data")
    return reasons


def export_partial_rows_review(path: Path | None = None, project_root: Path | None = None) -> dict[str, Any]:
    """Export partial staging rows with explicit missing-field reason codes."""
    import csv

    root = project_root or ROOT
    conn = connect()
    init_schema(conn)
    rows = list_partial_staging(conn)
    conn.close()

    out_rows: list[dict[str, Any]] = []
    for r in rows:
        merged_raw = r.get("merged_raw_intake_ids_json") or "[]"
        try:
            merged_ids = json.loads(merged_raw)
        except Exception:
            merged_ids = []
        first_raw = merged_ids[0] if merged_ids else None
        rec = {
            "raw_row_id": first_raw,
            "partial_row_id": r.get("partial_group_key") or "",
            "dealer_name": r.get("dealer_name") or "",
            "website": r.get("root_website") or "",
            "phone": r.get("phone") or "",
            "street": r.get("street") or "",
            "city": r.get("city") or "",
            "state": r.get("state") or "",
            "zip": r.get("zip") or "",
            "row_quality": r.get("row_quality") or "",
            "why_partial": r.get("row_rejection_reasons_json") or "[]",
            "source_of_each_field": r.get("source_of_each_field_json") or "{}",
            "zip_seed_hint": r.get("zip_seed_hint") or "",
            "map_reference_url": r.get("map_reference_url") or "",
            "merged_raw_intake_ids": merged_raw,
        }
        rec["missing_reason_codes"] = ",".join(_missing_reason_codes_for_partial(rec))
        out_rows.append(rec)

    out_path = path or (root / "data" / "oem" / "bmw" / "bmw_partial_review_latest.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".json":
        out_path.write_text(json.dumps(out_rows, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        if not out_rows:
            out_path.write_text("", encoding="utf-8")
        else:
            fields = list(out_rows[0].keys())
            with out_path.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                for rr in out_rows:
                    w.writerow(rr)

    return {"path": str(out_path), "rows": len(out_rows)}


def export_zip_diagnostics_from_latest_debug(
    path: Path | None = None, project_root: Path | None = None
) -> dict[str, Any]:
    """Export per-ZIP diagnostics from latest debug artifact for quick review."""
    import csv
    from glob import glob

    root = project_root or ROOT
    dbg_files = sorted(glob(str(root / "data" / "oem" / "bmw" / "debug" / "bmw_ingest_debug_*.json")))
    if not dbg_files:
        return {"error": "no_debug_artifacts_found"}
    latest = Path(dbg_files[-1])
    data = json.loads(latest.read_text(encoding="utf-8"))
    obs = ((data.get("interactive_flow") or {}).get("zip_observations") or [])
    if not obs and isinstance(data.get("playwright_phase"), dict):
        obs = (((data.get("playwright_phase") or {}).get("interactive_flow") or {}).get("zip_observations") or [])
    flow = data.get("interactive_flow") or {}
    if not flow and isinstance(data.get("playwright_phase"), dict):
        flow = (data.get("playwright_phase") or {}).get("interactive_flow") or {}
    rows: list[dict[str, Any]] = []
    for o in obs:
        rows.append(
            {
                "zip": o.get("zip"),
                "zip_entered": o.get("zip_entered") or o.get("zip"),
                "search_triggered": o.get("search_triggered"),
                "input_selector_found": o.get("input_selector_found"),
                "input_selector_used": o.get("input_selector_used"),
                "typing_succeeded": o.get("typing_succeeded"),
                "enter_triggered": o.get("enter_triggered"),
                "search_button_clicked": o.get("search_button_clicked"),
                "locator_mode_detected": o.get("locator_mode_detected"),
                "search_by_location_selected": o.get("search_by_location_selected"),
                "see_list_results_clicked": o.get("see_list_results_clicked"),
                "locator_empty_state": o.get("locator_empty_state"),
                "empty_state_text": o.get("empty_state_text"),
                "dealer_cards_appeared": o.get("dealer_cards_appeared"),
                "result_state": o.get("result_state"),
                "result_container_appeared": o.get("result_container_appeared"),
                "wait_ms": o.get("wait_ms"),
                "visible_result_count": o.get("visible_result_count"),
                "visible_dealer_names": json.dumps(o.get("visible_dealer_names") or []),
                "suggestions_detected": o.get("suggestions_detected"),
                "suggestion_texts": json.dumps(o.get("suggestion_texts") or []),
                "commit_mode": o.get("commit_mode"),
                "usable_rows_found": o.get("usable_visible_rows_estimate"),
                "partial_rows_found": o.get("partial_visible_rows_estimate"),
                "results_changed_from_previous_zip": o.get("results_changed_from_previous_zip"),
                "zip_input_not_found": o.get("zip_input_not_found"),
                "search_submit_not_found": o.get("search_submit_not_found"),
                "no_ui_change_detected": o.get("no_ui_change_detected"),
                "same_result_set_as_previous": o.get("same_result_set_as_previous"),
                "detail_cards_not_detected": o.get("detail_cards_not_detected"),
                "interaction_errors": json.dumps(o.get("interaction_errors") or []),
            }
        )
    if not rows:
        rows.append(
            {
                "zip": "",
                "zip_entered": "",
                "search_triggered": False,
                "visible_result_count": 0,
                "visible_dealer_names": "[]",
                "usable_rows_found": 0,
                "partial_rows_found": 0,
                "results_changed_from_previous_zip": False,
                "zip_input_not_found": not bool(flow.get("zip_input_found")),
                "search_submit_not_found": True,
                "no_ui_change_detected": True,
                "same_result_set_as_previous": False,
                "detail_cards_not_detected": True,
                "interaction_errors": json.dumps(
                    ["no_zip_observations_in_debug_artifact"]
                    + list(flow.get("steps") or [])[:3]
                ),
            }
        )

    out_path = path or (root / "data" / "oem" / "bmw" / "bmw_zip_diagnostics_latest.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".json":
        out_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        if not rows:
            out_path.write_text("", encoding="utf-8")
        else:
            fields = list(rows[0].keys())
            with out_path.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                w.writeheader()
                for rr in rows:
                    w.writerow(rr)
    return {"path": str(out_path), "rows": len(rows), "source_debug_artifact": str(latest)}


def run_bmw_normalize_dedupe_from_raw(project_root: Path | None = None) -> dict[str, int]:
    """
    Rebuild normalized table from raw intake (applies dedupe keys and merges history).
    Use after manual raw edits or to recover consistency.
    """
    root = project_root or ROOT
    conn = connect()
    init_schema(conn)
    before_norm = conn.execute("SELECT COUNT(*) FROM bmw_normalized_dealer").fetchone()[0]
    before_partial = conn.execute("SELECT COUNT(*) FROM bmw_partial_staging").fetchone()[0]
    delete_all_normalized(conn)
    clear_partial_staging(conn)
    raw_rows = load_all_raw_extracted(conn)
    merged = 0
    created = 0
    updated_or_matched = 0
    staged_partial = 0
    dedupe_collisions = 0
    dedupe_seen: dict[str, int] = {}
    row_audit: list[dict[str, Any]] = []
    processed = 0
    for row in raw_rows:
        d = json.loads(row["extracted_fields_json"])
        nd = intake_dict_to_normalized(d, last_verified_at=row["scraped_at"])
        dedupe_seen[nd.dedupe_key] = dedupe_seen.get(nd.dedupe_key, 0) + 1
        if dedupe_seen[nd.dedupe_key] > 1:
            dedupe_collisions += 1
        if nd.row_quality == "usable":
            norm_id, was_merged = upsert_normalized(conn, nd, [int(row["id"])])
            if was_merged:
                merged += 1
                updated_or_matched += 1
                action = "merged_existing"
            else:
                created += 1
                action = "inserted_new"
        else:
            upsert_partial_staging(conn, int(row["id"]), nd)
            norm_id = None
            was_merged = False
            staged_partial += 1
            action = "staged_partial"
        processed += 1
        row_audit.append(
            {
                "raw_row_id": int(row["id"]),
                "dealer_name": d.get("dealer_name") or "",
                "website": d.get("website") or "",
                "row_quality": nd.row_quality,
                "dedupe_key": nd.dedupe_key,
                "normalized_id": norm_id,
                "action": action,
                "was_merged": bool(was_merged),
            }
        )
    _write_normalize_audit(root, row_audit)
    partial_rows = list_partial_staging(conn)
    _write_partial_staging_review(
        root,
        [
            {
                "partial_group_key": r.get("partial_group_key") or "",
                "dealer_name": r.get("dealer_name") or "",
                "website": r.get("root_website") or "",
                "phone": r.get("phone") or "",
                "street": r.get("street") or "",
                "city": r.get("city") or "",
                "state": r.get("state") or "",
                "zip": r.get("zip") or "",
                "source_of_each_field": r.get("source_of_each_field_json") or "{}",
                "row_quality": r.get("row_quality") or "",
                "rejection_reasons": r.get("row_rejection_reasons_json") or "[]",
                "zip_seed_hint": r.get("zip_seed_hint") or "",
                "merged_raw_intake_ids": r.get("merged_raw_intake_ids_json") or "[]",
            }
            for r in partial_rows
        ],
    )
    stats = count_stats(conn)
    n_norm = stats["bmw_normalized_dealers"]
    n_partial = stats.get("bmw_partial_staging_rows", 0)
    conn.close()
    return {
        "normalized_before_run": int(before_norm),
        "partial_before_run": int(before_partial),
        "normalized_deleted": int(before_norm),
        "partial_deleted": int(before_partial),
        "raw_rows_replayed": processed,
        "normalized_created": created,
        "normalized_updated_or_matched": updated_or_matched,
        "duplicate_key_collisions": dedupe_collisions,
        "merge_events": merged,
        "partial_rows_staged": staged_partial,
        "normalized_count": n_norm,
        "partial_staging_count": n_partial,
    }


def _write_normalize_audit(project_root: Path, rows: list[dict[str, Any]]) -> None:
    import csv

    out_dir = project_root / "data" / "oem" / "bmw"
    out_dir.mkdir(parents=True, exist_ok=True)
    jpath = out_dir / "bmw_normalize_audit_latest.json"
    cpath = out_dir / "bmw_normalize_audit_latest.csv"
    jpath.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    if not rows:
        cpath.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with cpath.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def normalized_rows_to_enrichment_manifest(rows: list[dict[str, Any]]) -> list[dict[str, str | None]]:
    out: list[dict[str, str | None]] = []
    for r in rows:
        url = normalize_root((r.get("root_website") or "").strip())
        if not url:
            continue
        did = str(r.get("id") or "")
        name = (r.get("dealer_name") or "").strip()
        out.append(
            {
                "url": url,
                "dealer_id": f"bmw-{did}",
                "dealer_name": name,
                "brand": "BMW",
            }
        )
    return out


def _site_enrichment_details(
    orch: Any,  # OrchestratorResult duck-typed
    manifest: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    browser_used: bool,
    use_requests_only: bool,
) -> list[dict[str, Any]]:
    """Per-site diagnostics aligned with SiteResult + pipeline output."""
    pairs = list(zip(manifest, rows))
    url_to_pair: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for m, r in pairs:
        url_to_pair[normalize_root(str(m["url"]))] = (m, r)

    out: list[dict[str, Any]] = []
    for site in orch.sites:
        sd = site.site_result_dict or {}
        u = normalize_root(sd.get("url") or "")
        pair = url_to_pair.get(u)
        if pair is None:
            fu = normalize_root(sd.get("final_url") or "")
            pair = url_to_pair.get(fu)
        m, _r = pair if pair else ({}, {})
        out.append(
            {
                "dealer_id": m.get("dealer_id"),
                "manifest_url": m.get("url"),
                "fetch_mode": sd.get("fetch_mode"),
                "homepage_loaded": sd.get("homepage_loaded"),
                "final_url": sd.get("final_url"),
                "final_domain": sd.get("final_domain"),
                "final_status": sd.get("final_status"),
                "fetch_error": sd.get("fetch_error"),
                "flags": list(sd.get("flags") or []),
                "merged_status": site.merged_status,
                "adjudication_skip_reason": site.adjudication_skip_reason,
                "playwright_used": browser_used and not use_requests_only,
                "use_requests_only": use_requests_only,
            }
        )
    return out


def run_bmw_enrichment(
    *,
    limit: int = 0,
    use_requests_only: bool = False,
    timeout: int = 35,
    max_extra_pages: int = 5,
    insecure_ssl: bool = False,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
    hybrid_out: Path | None = None,
) -> dict[str, Any]:
    """Run hybrid crawl + adjudication — same crawl behavior as SCRAPING.cli --adjudicate."""
    from llm.providers.ollama_client import OpenAICompatibleClient
    from pipeline.orchestrator import run_hybrid_batch, save_hybrid_run

    from SCRAPING.adjudicate_crawl import build_adjudicate_crawl_one
    from SCRAPING.fetch_requests import fetch_requests_session

    conn = connect()
    try:
        init_schema(conn)
        rows = list_normalized_for_enrichment(conn)
        if limit > 0:
            rows = rows[:limit]
        manifest = normalized_rows_to_enrichment_manifest(rows)
        if not manifest:
            return {"error": "no_dealers_with_websites", "sent": 0}

        session = fetch_requests_session(timeout, verify_ssl=not insecure_ssl)
        llm = OpenAICompatibleClient(base_url=llm_base_url)

        browser = None
        playwright = None
        browser_used = False

        try:
            if not use_requests_only:
                try:
                    from playwright.sync_api import sync_playwright

                    playwright = sync_playwright().start()
                    browser = playwright.chromium.launch(headless=True)
                    browser_used = True
                    logger.info("BMW enrich: Playwright Chromium (same default as SCRAPING --adjudicate)")
                except Exception as e:
                    logger.warning("Playwright unavailable (%s); using requests only", e)
                    browser = None
                    browser_used = False

            crawl_one = build_adjudicate_crawl_one(
                session=session,
                browser=browser,
                timeout_sec=timeout,
                max_extra_pages=max_extra_pages,
                use_requests_only=use_requests_only,
                insecure_ssl=insecure_ssl,
            )

            orch = run_hybrid_batch(
                crawl_one,
                manifest,
                use_adjudicator=True,
                llm_client=llm,
                llm_model=llm_model or "llama3.2",
            )
        finally:
            if browser:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright:
                try:
                    playwright.stop()
                except Exception:
                    pass

        out_path = hybrid_out or (ROOT / "data" / "oem" / "bmw" / "bmw_enrichment_hybrid.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        save_hybrid_run(orch, out_path)

        run_id = orch.summary.run_id
        pairs = list(zip(manifest, rows))
        url_to_id = {normalize_root(str(m["url"])): int(r["id"]) for m, r in pairs}
        for site in orch.sites:
            sd = site.site_result_dict or {}
            u = normalize_root(sd.get("url") or "")
            oid = url_to_id.get(u)
            if oid is None:
                fu = normalize_root(sd.get("final_url") or "")
                oid = url_to_id.get(fu)
            if oid is None:
                continue
            canon = site.merged_best_canonical or sd.get("best_candidate_canonical")
            conf = site.merged_confidence if site.merged_confidence is not None else sd.get("confidence_score")
            update_enrichment_fields(
                conn,
                int(oid),
                canonical=str(canon) if canon else None,
                confidence=float(conf) if conf is not None else None,
                status=site.merged_status,
                run_id=run_id,
            )

        summary = orch.summary.model_dump(mode="json")
        site_details = _site_enrichment_details(
            orch,
            manifest,
            rows,
            browser_used=browser_used,
            use_requests_only=use_requests_only,
        )
        return {
            "hybrid_path": str(out_path),
            "sent_to_enrichment": len(manifest),
            "summary": summary,
            "run_id": run_id,
            "crawl_config": {
                "timeout_sec": timeout,
                "max_extra_pages": max_extra_pages,
                "use_requests_only": use_requests_only,
                "insecure_ssl": insecure_ssl,
                "playwright_launched": browser_used,
                "shared_with": "SCRAPING.adjudicate_crawl.build_adjudicate_crawl_one",
            },
            "site_details": site_details,
        }
    finally:
        conn.close()


def build_report(project_root: Path | None = None) -> dict[str, Any]:
    root = project_root or ROOT
    conn = connect()
    init_schema(conn)
    base = count_stats(conn)
    conn.close()
    base["project_root"] = str(root)
    base["last_enrichment_summary"] = load_last_enrichment_summary()
    return base


def export_normalized_csv(path: Path, project_root: Path | None = None) -> int:
    import csv

    _ = project_root or ROOT
    conn = connect()
    init_schema(conn)
    rows = conn.execute("SELECT * FROM bmw_normalized_dealer ORDER BY id").fetchall()
    conn.close()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return 0
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fieldnames})
    return len(rows)


def load_last_enrichment_summary() -> dict[str, Any] | None:
    p = ROOT / "data" / "oem" / "bmw" / "bmw_enrichment_hybrid.json"
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data.get("summary") if isinstance(data, dict) else None


def export_summary_json(path: Path, report: dict[str, Any], ingest_extra: dict[str, Any] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = {"report": report, "ingest": ingest_extra or {}}
    path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
